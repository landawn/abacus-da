package com.landawn.abacus.da.mongodb.reactivestreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import com.landawn.abacus.da.MockitoTestBase;
import com.landawn.abacus.da.mongodb.MongoMappingFixtures;
import com.landawn.abacus.util.Dataset;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class MongoCollectionMapperTest extends MockitoTestBase {

    @Mock
    private MongoCollectionExecutor mockExecutor;

    private MongoCollectionMapper<TestEntity> mapper;

    @BeforeEach
    public void setUp() {
        mapper = new MongoCollectionMapper<>(mockExecutor, TestEntity.class);
    }

    @Test
    public void testDecodedValueConversionsOnMapperReadPaths() {
        // The reactive mapper reads through the executor with its entity class as row type, so the MongoDBBase read-side
        // conversions (typed container elements, java.time Local* in UTC, generic bean type arguments, records incl. _id into
        // the record's id component) must hold for mapped entities too. Fixtures are shared with the sync
        // com.landawn.abacus.da.mongodb.MongoCollectionExecutorTest (*); each read path is asserted on its own.
        final var entityType = com.landawn.abacus.da.mongodb.MongoMappingFixtures.MappedEntity.class;
        final var entityMapper = mapperReturning(com.landawn.abacus.da.mongodb.MongoMappingFixtures.entityDoc(), entityType);
        final var recordMapper = mapperReturning(com.landawn.abacus.da.mongodb.MongoMappingFixtures.recordDoc(),
                com.landawn.abacus.da.mongodb.MongoMappingFixtures.MappedRecord.class);
        final var boxMapper = mapperReturning(com.landawn.abacus.da.mongodb.MongoMappingFixtures.longBoxDoc(),
                com.landawn.abacus.da.mongodb.MongoMappingFixtures.LongBox.class);
        final Bson filter = new Document();
        final List<Document> pipeline = Arrays.asList(new Document("$match", new Document()));
        final ObjectId oid = new ObjectId(com.landawn.abacus.da.mongodb.MongoMappingFixtures.OID_HEX);
        final Object expectedRecord = com.landawn.abacus.da.mongodb.MongoMappingFixtures.expectedRecord();

        org.junit.jupiter.api.Assertions.assertAll(
                () -> com.landawn.abacus.da.mongodb.MongoMappingFixtures.assertEntity("list", entityMapper.list(filter).blockFirst()),
                () -> com.landawn.abacus.da.mongodb.MongoMappingFixtures.assertEntity("list(Collection)",
                        entityMapper.list(Arrays.asList("nums", "day"), filter).blockFirst()),
                () -> com.landawn.abacus.da.mongodb.MongoMappingFixtures.assertEntity("findFirst", entityMapper.findFirst(filter).block()),
                () -> com.landawn.abacus.da.mongodb.MongoMappingFixtures.assertEntity("get(ObjectId)", entityMapper.get(oid).block()),
                () -> com.landawn.abacus.da.mongodb.MongoMappingFixtures.assertEntity("aggregate", entityMapper.aggregate(pipeline).blockFirst()),
                () -> com.landawn.abacus.da.mongodb.MongoMappingFixtures.assertEntity("findOneAndDelete", entityMapper.findOneAndDelete(filter).block()),
                () -> com.landawn.abacus.da.mongodb.MongoMappingFixtures.assertEntityDataset("query(Bson)", entityMapper.query(filter).block()),
                () -> assertEquals(com.landawn.abacus.da.mongodb.MongoMappingFixtures.LOCAL_DAY,
                        entityMapper.queryForSingleValue("day", filter, java.time.LocalDate.class).block(), "queryForSingleValue LocalDate"),
                () -> assertEquals(expectedRecord, recordMapper.get(oid).block(), "record get(ObjectId)"),
                () -> assertEquals(expectedRecord, recordMapper.list(filter).blockFirst(), "record list"),
                () -> assertEquals((Object) 9L, boxMapper.list(filter).blockFirst().getValue(), "LongBox list"),
                () -> assertEquals((Object) 9L, boxMapper.findFirst(filter).block().getValue(), "LongBox findFirst"));
    }

    @Test
    public void testBulkInsert() {
        Collection<TestEntity> entities = Arrays.asList(new TestEntity(), new TestEntity());
        BulkWriteResult bulkResult = mock(BulkWriteResult.class);

        when(mockExecutor.bulkInsert(entities)).thenReturn(Mono.just(bulkResult));

        StepVerifier.create(mapper.bulkInsert(entities)).expectNext(bulkResult).verifyComplete();

        verify(mockExecutor).bulkInsert(entities);
    }

    @Test
    public void testBulkInsertWithOptions() {
        Collection<TestEntity> entities = Arrays.asList(new TestEntity(), new TestEntity());
        BulkWriteOptions options = new BulkWriteOptions().ordered(false);
        BulkWriteResult bulkResult = mock(BulkWriteResult.class);

        when(mockExecutor.bulkInsert(entities, options)).thenReturn(Mono.just(bulkResult));

        StepVerifier.create(mapper.bulkInsert(entities, options)).expectNext(bulkResult).verifyComplete();

        verify(mockExecutor).bulkInsert(entities, options);
    }

    @Test
    public void testInsertMany() {
        Collection<TestEntity> entities = Arrays.asList(new TestEntity(), new TestEntity());

        mapper.insertMany(entities);

        verify(mockExecutor).insertMany(entities);
    }

    @Test
    public void testInsertManyWithOptions() {
        Collection<TestEntity> entities = Arrays.asList(new TestEntity(), new TestEntity());
        InsertManyOptions options = new InsertManyOptions().ordered(false);

        mapper.insertMany(entities, options);

        verify(mockExecutor).insertMany(entities, options);
    }

    @Test
    public void testInsertOne() {
        TestEntity entity = new TestEntity();

        mapper.insertOne(entity);

        verify(mockExecutor).insertOne(entity);
    }

    @Test
    public void testInsertOneWithOptions() {
        TestEntity entity = new TestEntity();
        InsertOneOptions options = new InsertOneOptions();

        mapper.insertOne(entity, options);

        verify(mockExecutor).insertOne(entity, options);
    }

    @Test
    public void testDeleteMany() {
        Bson filter = new Document("obsolete", true);
        DeleteResult mockResult = mock(DeleteResult.class);

        when(mockExecutor.deleteMany(filter)).thenReturn(Mono.just(mockResult));

        StepVerifier.create(mapper.deleteMany(filter)).expectNext(mockResult).verifyComplete();

        verify(mockExecutor).deleteMany(filter);
    }

    @Test
    public void testDeleteManyWithOptions() {
        Bson filter = new Document("category", "archived");
        DeleteOptions options = new DeleteOptions();
        DeleteResult mockResult = mock(DeleteResult.class);

        when(mockExecutor.deleteMany(filter, options)).thenReturn(Mono.just(mockResult));

        StepVerifier.create(mapper.deleteMany(filter, options)).expectNext(mockResult).verifyComplete();

        verify(mockExecutor).deleteMany(filter, options);
    }

    @Test
    public void testDeleteOneWithFilter() {
        Bson filter = new Document("status", "deleted");
        DeleteResult mockResult = mock(DeleteResult.class);

        when(mockExecutor.deleteOne(filter)).thenReturn(Mono.just(mockResult));

        StepVerifier.create(mapper.deleteOne(filter)).expectNext(mockResult).verifyComplete();

        verify(mockExecutor).deleteOne(filter);
    }

    @Test
    public void testDeleteOneWithFilterAndOptions() {
        Bson filter = new Document("expired", true);
        DeleteOptions options = new DeleteOptions();
        DeleteResult mockResult = mock(DeleteResult.class);

        when(mockExecutor.deleteOne(filter, options)).thenReturn(Mono.just(mockResult));

        StepVerifier.create(mapper.deleteOne(filter, options)).expectNext(mockResult).verifyComplete();

        verify(mockExecutor).deleteOne(filter, options);
    }

    @Test
    public void testDeleteOneWithObjectId() {
        ObjectId objectId = new ObjectId();
        DeleteResult mockResult = mock(DeleteResult.class);

        when(mockExecutor.deleteOne(objectId)).thenReturn(Mono.just(mockResult));

        StepVerifier.create(mapper.deleteOne(objectId)).expectNext(mockResult).verifyComplete();

        verify(mockExecutor).deleteOne(objectId);
    }

    @Test
    public void testDeleteOneWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        DeleteResult mockResult = mock(DeleteResult.class);

        when(mockExecutor.deleteOne(objectId)).thenReturn(Mono.just(mockResult));

        StepVerifier.create(mapper.deleteOne(objectId)).expectNext(mockResult).verifyComplete();

        verify(mockExecutor).deleteOne(objectId);
    }

    @Test
    public void testFindOneAndDelete() {
        Bson filter = new Document("toDelete", true);
        TestEntity result = new TestEntity();

        when(mockExecutor.findOneAndDelete(filter, TestEntity.class)).thenReturn(Mono.just(result));

        StepVerifier.create(mapper.findOneAndDelete(filter)).expectNext(result).verifyComplete();

        verify(mockExecutor).findOneAndDelete(filter, TestEntity.class);
    }

    @Test
    public void testFindOneAndDeleteWithOptions() {
        Bson filter = new Document("expired", true);
        FindOneAndDeleteOptions options = new FindOneAndDeleteOptions();
        TestEntity result = new TestEntity();

        when(mockExecutor.findOneAndDelete(filter, options, TestEntity.class)).thenReturn(Mono.just(result));

        StepVerifier.create(mapper.findOneAndDelete(filter, options)).expectNext(result).verifyComplete();

        verify(mockExecutor).findOneAndDelete(filter, options, TestEntity.class);
    }

    @Test
    public void testFindOneAndUpdate() {
        Bson filter = new Document("id", 123);
        TestEntity update = new TestEntity();
        TestEntity result = new TestEntity();

        when(mockExecutor.findOneAndUpdate(filter, update, TestEntity.class)).thenReturn(Mono.just(result));

        StepVerifier.create(mapper.findOneAndUpdate(filter, update)).expectNext(result).verifyComplete();

        verify(mockExecutor).findOneAndUpdate(filter, update, TestEntity.class);
    }

    @Test
    public void testFindOneAndUpdateWithCollection() {
        Bson filter = new Document("name", "test");
        Collection<TestEntity> objList = Arrays.asList(new TestEntity());
        TestEntity result = new TestEntity();

        when(mockExecutor.findOneAndUpdate(filter, objList, TestEntity.class)).thenReturn(Mono.just(result));

        StepVerifier.create(mapper.findOneAndUpdate(filter, objList)).expectNext(result).verifyComplete();

        verify(mockExecutor).findOneAndUpdate(filter, objList, TestEntity.class);
    }

    @Test
    public void testFindOneAndUpdateWithCollectionAndOptions() {
        Bson filter = new Document("status", "pending");
        Collection<TestEntity> objList = Arrays.asList(new TestEntity());
        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
        TestEntity result = new TestEntity();

        when(mockExecutor.findOneAndUpdate(filter, objList, options, TestEntity.class)).thenReturn(Mono.just(result));

        StepVerifier.create(mapper.findOneAndUpdate(filter, objList, options)).expectNext(result).verifyComplete();

        verify(mockExecutor).findOneAndUpdate(filter, objList, options, TestEntity.class);
    }

    @Test
    public void testFindOneAndUpdateWithOptions() {
        Bson filter = new Document("key", "value");
        TestEntity update = new TestEntity();
        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions().upsert(true);
        TestEntity result = new TestEntity();

        when(mockExecutor.findOneAndUpdate(filter, update, options, TestEntity.class)).thenReturn(Mono.just(result));

        StepVerifier.create(mapper.findOneAndUpdate(filter, update, options)).expectNext(result).verifyComplete();

        verify(mockExecutor).findOneAndUpdate(filter, update, options, TestEntity.class);
    }

    @Test
    public void testListWithFilterOffsetAndCount() {
        Bson filter = new Document("active", true);
        int offset = 10;
        int count = 5;
        List<TestEntity> entities = Arrays.asList(new TestEntity());

        when(mockExecutor.list(filter, offset, count, TestEntity.class)).thenReturn(Flux.fromIterable(entities));

        StepVerifier.create(mapper.list(filter, offset, count)).expectNextSequence(entities).verifyComplete();

        verify(mockExecutor).list(filter, offset, count, TestEntity.class);
    }

    @Test
    public void testListWithProjectionFilterSortOffsetAndCount() {
        Bson projection = Projections.include("name", "value");
        Bson filter = new Document("active", true);
        Bson sort = Sorts.ascending("name");
        int offset = 0;
        int count = 100;
        List<TestEntity> entities = Arrays.asList(new TestEntity());

        when(mockExecutor.list(projection, filter, sort, offset, count, TestEntity.class)).thenReturn(Flux.fromIterable(entities));

        StepVerifier.create(mapper.list(projection, filter, sort, offset, count)).expectNextSequence(entities).verifyComplete();

        verify(mockExecutor).list(projection, filter, sort, offset, count, TestEntity.class);
    }

    @Test
    public void testListWithSelectPropNamesFilterOffsetAndCount() {
        Collection<String> selectPropNames = Arrays.asList("field1");
        Bson filter = new Document("status", "active");
        int offset = 5;
        int count = 10;
        List<TestEntity> entities = Arrays.asList(new TestEntity());

        when(mockExecutor.list(selectPropNames, filter, offset, count, TestEntity.class)).thenReturn(Flux.fromIterable(entities));

        StepVerifier.create(mapper.list(selectPropNames, filter, offset, count)).expectNextSequence(entities).verifyComplete();

        verify(mockExecutor).list(selectPropNames, filter, offset, count, TestEntity.class);
    }

    @Test
    public void testListWithSelectPropNamesFilterSortOffsetAndCount() {
        Collection<String> selectPropNames = Arrays.asList("id", "title");
        Bson filter = new Document("category", "books");
        Bson sort = new Document("rating", -1);
        int offset = 20;
        int count = 15;
        List<TestEntity> entities = Arrays.asList(new TestEntity());

        when(mockExecutor.list(selectPropNames, filter, sort, offset, count, TestEntity.class)).thenReturn(Flux.fromIterable(entities));

        StepVerifier.create(mapper.list(selectPropNames, filter, sort, offset, count)).expectNextSequence(entities).verifyComplete();

        verify(mockExecutor).list(selectPropNames, filter, sort, offset, count, TestEntity.class);
    }

    @Test
    public void testQueryWithFilterOffsetAndCount() {
        Bson filter = new Document("type", "premium");
        int offset = 10;
        int count = 20;
        Dataset mockDataset = Dataset.empty();

        when(mockExecutor.query(filter, offset, count, TestEntity.class)).thenReturn(Mono.just(mockDataset));

        StepVerifier.create(mapper.query(filter, offset, count)).expectNext(mockDataset).verifyComplete();

        verify(mockExecutor).query(filter, offset, count, TestEntity.class);
    }

    @Test
    public void testQueryWithProjectionFilterSortOffsetAndCount() {
        Bson projection = Projections.fields(Projections.include("field1"), Projections.excludeId());
        Bson filter = new Document("active", true);
        Bson sort = Sorts.descending("createdAt");
        int offset = 10;
        int count = 25;
        Dataset mockDataset = Dataset.empty();

        when(mockExecutor.query(projection, filter, sort, offset, count, TestEntity.class)).thenReturn(Mono.just(mockDataset));

        StepVerifier.create(mapper.query(projection, filter, sort, offset, count)).expectNext(mockDataset).verifyComplete();

        verify(mockExecutor).query(projection, filter, sort, offset, count, TestEntity.class);
    }

    @Test
    public void testQueryWithSelectPropNamesFilterOffsetAndCount() {
        Collection<String> selectPropNames = Arrays.asList("field1", "field2");
        Bson filter = new Document("category", "books");
        int offset = 5;
        int count = 15;
        Dataset mockDataset = Dataset.empty();

        when(mockExecutor.query(selectPropNames, filter, offset, count, TestEntity.class)).thenReturn(Mono.just(mockDataset));

        StepVerifier.create(mapper.query(selectPropNames, filter, offset, count)).expectNext(mockDataset).verifyComplete();

        verify(mockExecutor).query(selectPropNames, filter, offset, count, TestEntity.class);
    }

    @Test
    public void testQueryWithSelectPropNamesFilterSortOffsetAndCount() {
        Collection<String> selectPropNames = Arrays.asList("id", "value");
        Bson filter = new Document("type", "special");
        Bson sort = new Document("priority", 1);
        int offset = 0;
        int count = 50;
        Dataset mockDataset = Dataset.empty();

        when(mockExecutor.query(selectPropNames, filter, sort, offset, count, TestEntity.class)).thenReturn(Mono.just(mockDataset));

        StepVerifier.create(mapper.query(selectPropNames, filter, sort, offset, count)).expectNext(mockDataset).verifyComplete();

        verify(mockExecutor).query(selectPropNames, filter, sort, offset, count, TestEntity.class);
    }

    @Test
    public void testUpdateMany() {
        Bson filter = new Document("status", "old");
        TestEntity update = new TestEntity();
        UpdateResult mockResult = mock(UpdateResult.class);

        when(mockExecutor.updateMany(filter, update)).thenReturn(Mono.just(mockResult));

        StepVerifier.create(mapper.updateMany(filter, update)).expectNext(mockResult).verifyComplete();

        verify(mockExecutor).updateMany(filter, update);
    }

    @Test
    public void testUpdateManyWithCollection() {
        Bson filter = new Document("type", "batch");
        Collection<TestEntity> objList = Arrays.asList(new TestEntity(), new TestEntity());
        UpdateResult mockResult = mock(UpdateResult.class);

        when(mockExecutor.updateMany(filter, objList)).thenReturn(Mono.just(mockResult));

        StepVerifier.create(mapper.updateMany(filter, objList)).expectNext(mockResult).verifyComplete();

        verify(mockExecutor).updateMany(filter, objList);
    }

    @Test
    public void testUpdateManyWithCollectionAndOptions() {
        Bson filter = new Document("expired", true);
        Collection<TestEntity> objList = Arrays.asList(new TestEntity());
        UpdateOptions options = new UpdateOptions().upsert(false);
        UpdateResult mockResult = mock(UpdateResult.class);

        when(mockExecutor.updateMany(filter, objList, options)).thenReturn(Mono.just(mockResult));

        StepVerifier.create(mapper.updateMany(filter, objList, options)).expectNext(mockResult).verifyComplete();

        verify(mockExecutor).updateMany(filter, objList, options);
    }

    @Test
    public void testUpdateManyWithOptions() {
        Bson filter = new Document("category", "temp");
        TestEntity update = new TestEntity();
        UpdateOptions options = new UpdateOptions();
        UpdateResult mockResult = mock(UpdateResult.class);

        when(mockExecutor.updateMany(filter, update, options)).thenReturn(Mono.just(mockResult));

        StepVerifier.create(mapper.updateMany(filter, update, options)).expectNext(mockResult).verifyComplete();

        verify(mockExecutor).updateMany(filter, update, options);
    }

    @Test
    public void testUpdateOneWithFilter() {
        Bson filter = new Document("name", "test");
        TestEntity update = new TestEntity();
        UpdateResult mockResult = mock(UpdateResult.class);

        when(mockExecutor.updateOne(filter, update)).thenReturn(Mono.just(mockResult));

        StepVerifier.create(mapper.updateOne(filter, update)).expectNext(mockResult).verifyComplete();

        verify(mockExecutor).updateOne(filter, update);
    }

    @Test
    public void testUpdateOneWithFilterAndCollection() {
        Bson filter = new Document("key", "value");
        Collection<TestEntity> objList = Arrays.asList(new TestEntity());
        UpdateResult mockResult = mock(UpdateResult.class);

        when(mockExecutor.updateOne(filter, objList)).thenReturn(Mono.just(mockResult));

        StepVerifier.create(mapper.updateOne(filter, objList)).expectNext(mockResult).verifyComplete();

        verify(mockExecutor).updateOne(filter, objList);
    }

    @Test
    public void testUpdateOneWithFilterAndOptions() {
        Bson filter = new Document("id", 123);
        TestEntity update = new TestEntity();
        UpdateOptions options = new UpdateOptions().upsert(true);
        UpdateResult mockResult = mock(UpdateResult.class);

        when(mockExecutor.updateOne(filter, update, options)).thenReturn(Mono.just(mockResult));

        StepVerifier.create(mapper.updateOne(filter, update, options)).expectNext(mockResult).verifyComplete();

        verify(mockExecutor).updateOne(filter, update, options);
    }

    @Test
    public void testUpdateOneWithFilterCollectionAndOptions() {
        Bson filter = new Document("status", "pending");
        Collection<TestEntity> objList = Arrays.asList(new TestEntity());
        UpdateOptions options = new UpdateOptions();
        UpdateResult mockResult = mock(UpdateResult.class);

        when(mockExecutor.updateOne(filter, objList, options)).thenReturn(Mono.just(mockResult));

        StepVerifier.create(mapper.updateOne(filter, objList, options)).expectNext(mockResult).verifyComplete();

        verify(mockExecutor).updateOne(filter, objList, options);
    }

    @Test
    public void testUpdateOneWithObjectId() {
        ObjectId objectId = new ObjectId();
        TestEntity update = new TestEntity();
        UpdateResult mockResult = mock(UpdateResult.class);

        when(mockExecutor.updateOne(objectId, update)).thenReturn(Mono.just(mockResult));

        StepVerifier.create(mapper.updateOne(objectId, update)).expectNext(mockResult).verifyComplete();

        verify(mockExecutor).updateOne(objectId, update);
    }

    @Test
    public void testUpdateOneWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        TestEntity update = new TestEntity();
        UpdateResult mockResult = mock(UpdateResult.class);

        when(mockExecutor.updateOne(objectId, update)).thenReturn(Mono.just(mockResult));

        StepVerifier.create(mapper.updateOne(objectId, update)).expectNext(mockResult).verifyComplete();

        verify(mockExecutor).updateOne(objectId, update);
    }

    @Test
    public void testAggregate() {
        List<Bson> pipeline = Arrays.asList(new Document("$match", new Document("status", "active")));
        List<TestEntity> results = Arrays.asList(new TestEntity());

        when(mockExecutor.aggregate(pipeline, TestEntity.class)).thenReturn(Flux.fromIterable(results));

        StepVerifier.create(mapper.aggregate(pipeline)).expectNextSequence(results).verifyComplete();

        verify(mockExecutor).aggregate(pipeline, TestEntity.class);
    }

    @Test
    public void testCount() {
        when(mockExecutor.count()).thenReturn(Mono.just(100L));

        StepVerifier.create(mapper.count()).expectNext(100L).verifyComplete();

        verify(mockExecutor).count();
    }

    @Test
    public void testCountWithFilter() {
        Bson filter = new Document("type", "premium");
        when(mockExecutor.count(filter)).thenReturn(Mono.just(25L));

        StepVerifier.create(mapper.count(filter)).expectNext(25L).verifyComplete();

        verify(mockExecutor).count(filter);
    }

    @Test
    public void testCountWithFilterAndOptions() {
        Bson filter = new Document("active", true);
        CountOptions options = new CountOptions().limit(50);
        when(mockExecutor.count(filter, options)).thenReturn(Mono.just(50L));

        StepVerifier.create(mapper.count(filter, options)).expectNext(50L).verifyComplete();

        verify(mockExecutor).count(filter, options);
    }

    @Test
    public void testDistinct() {
        // distinct now routes through a $group/$project pipeline so distinct scalar values come back as
        // {fieldName: value} documents decodable into the mapped entity type (mirrors the blocking mapper;
        // the driver's native distinct(field, entityClass) throws BsonInvalidOperationException on scalars).
        String fieldName = "category";
        List<TestEntity> distinctValues = Arrays.asList(new TestEntity(), new TestEntity());
        List<Bson> expectedPipeline = Arrays.asList(new Document("$group", new Document("_id", "$" + fieldName)),
                new Document("$project", new Document("_id", 0).append(fieldName, "$_id")));

        when(mockExecutor.aggregate(expectedPipeline, TestEntity.class)).thenReturn(Flux.fromIterable(distinctValues));

        StepVerifier.create(mapper.distinct(fieldName)).expectNextSequence(distinctValues).verifyComplete();

        verify(mockExecutor).aggregate(expectedPipeline, TestEntity.class);
    }

    @Test
    public void testDistinct_nullOrEmptyFieldName_throwsSynchronously() {
        // The documented IAE is thrown at the call site, before any Flux is built or subscribed.
        assertThrows(IllegalArgumentException.class, () -> mapper.distinct(null));
        assertThrows(IllegalArgumentException.class, () -> mapper.distinct(""));
    }

    @Test
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testDistinctDottedFieldIntoScalarRowTypeSendsGroupOnlyPipelineAndEmitsValues() {
        // A single-value mapper type must read each distinct value from the {_id: value} group rows. A trailing
        // {$project: {"address.city": "$_id"}} makes the server nest the value ({address: {city: v}}), which a
        // String mapper then emitted as '{"city": "Paris"}' and an Integer mapper failed with NumberFormatException.
        final com.mongodb.reactivestreams.client.MongoCollection<Document> collection = mock(com.mongodb.reactivestreams.client.MongoCollection.class);
        final com.mongodb.reactivestreams.client.AggregatePublisher<Document> publisher = mock(com.mongodb.reactivestreams.client.AggregatePublisher.class,
                org.mockito.Mockito.RETURNS_SELF);
        when(collection.aggregate(org.mockito.ArgumentMatchers.anyList(), org.mockito.ArgumentMatchers.eq(Document.class))).thenReturn(publisher);
        org.mockito.Mockito.doAnswer(invocation -> {
            Flux.just(new Document("_id", "Paris"), new Document("_id", "Rome"))
                    .subscribe(invocation.<org.reactivestreams.Subscriber<? super Document>> getArgument(0));
            return null;
        }).when(publisher).subscribe(org.mockito.ArgumentMatchers.any());

        final MongoCollectionMapper<String> stringMapper = new MongoCollectionMapper<>(new MongoCollectionExecutor(collection), String.class);
        final Bson filter = new Document("active", true);

        StepVerifier.create(stringMapper.distinct("address.city")).expectNext("Paris", "Rome").verifyComplete();
        StepVerifier.create(stringMapper.distinct("address.city", filter)).expectNext("Paris", "Rome").verifyComplete();

        final org.mockito.ArgumentCaptor<List> pipelineCaptor = org.mockito.ArgumentCaptor.forClass(List.class);
        verify(collection, org.mockito.Mockito.times(2)).aggregate(pipelineCaptor.capture(), org.mockito.ArgumentMatchers.eq(Document.class));
        assertEquals(Arrays.asList(new Document("$group", new Document("_id", "$address.city"))), pipelineCaptor.getAllValues().get(0));
        final List<?> filtered = pipelineCaptor.getAllValues().get(1);
        assertEquals(2, filtered.size());
        assertEquals(new Document("$group", new Document("_id", "$address.city")), filtered.get(1));
    }

    @Test
    public void testDistinctWithFilter() {
        String fieldName = "type";
        Bson filter = new Document("active", true);
        List<TestEntity> distinctValues = Arrays.asList(new TestEntity());

        when(mockExecutor.aggregate(org.mockito.ArgumentMatchers.anyList(), org.mockito.ArgumentMatchers.eq(TestEntity.class)))
                .thenReturn(Flux.fromIterable(distinctValues));

        StepVerifier.create(mapper.distinct(fieldName, filter)).expectNextSequence(distinctValues).verifyComplete();

        @SuppressWarnings("rawtypes")
        org.mockito.ArgumentCaptor<List> pipelineCaptor = org.mockito.ArgumentCaptor.forClass(List.class);
        verify(mockExecutor).aggregate(pipelineCaptor.capture(), org.mockito.ArgumentMatchers.eq(TestEntity.class));

        List<?> pipeline = pipelineCaptor.getValue();
        assertEquals(3, pipeline.size());
        // First stage matches the filter, then $group on the field, then $project surfacing the value.
        assertEquals(new Document("$group", new Document("_id", "$" + fieldName)), pipeline.get(1));
        assertEquals(new Document("$project", new Document("_id", 0).append(fieldName, "$_id")), pipeline.get(2));
    }

    @Test
    public void testDistinctWithFilter_nullArgs_throwSynchronously() {
        assertThrows(IllegalArgumentException.class, () -> mapper.distinct(null, new Document("active", true)));
        assertThrows(IllegalArgumentException.class, () -> mapper.distinct("type", null));
    }

    @Test
    public void testExistsWithFilter() {
        Bson filter = new Document("status", "active");
        when(mockExecutor.exists(filter)).thenReturn(Mono.just(true));

        StepVerifier.create(mapper.exists(filter)).expectNext(true).verifyComplete();

        verify(mockExecutor).exists(filter);
    }

    @Test
    public void testExistsWithObjectId() {
        ObjectId objectId = new ObjectId();
        when(mockExecutor.exists(objectId)).thenReturn(Mono.just(false));

        StepVerifier.create(mapper.exists(objectId)).expectNext(false).verifyComplete();

        verify(mockExecutor).exists(objectId);
    }

    @Test
    public void testExistsWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        when(mockExecutor.exists(objectId)).thenReturn(Mono.just(true));

        StepVerifier.create(mapper.exists(objectId)).expectNext(true).verifyComplete();

        verify(mockExecutor).exists(objectId);
    }

    @Test
    public void testFindFirstWithFilter() {
        Bson filter = new Document("status", "active");
        TestEntity entity = new TestEntity();

        when(mockExecutor.findFirst(filter, TestEntity.class)).thenReturn(Mono.just(entity));

        StepVerifier.create(mapper.findFirst(filter)).expectNext(entity).verifyComplete();

        verify(mockExecutor).findFirst(filter, TestEntity.class);
    }

    @Test
    public void testFindFirstWithProjectionFilterAndSort() {
        Bson projection = Projections.include("field1", "field2");
        Bson filter = new Document("status", "pending");
        Bson sort = Sorts.ascending("priority");
        TestEntity entity = new TestEntity();

        when(mockExecutor.findFirst(projection, filter, sort, TestEntity.class)).thenReturn(Mono.just(entity));

        StepVerifier.create(mapper.findFirst(projection, filter, sort)).expectNext(entity).verifyComplete();

        verify(mockExecutor).findFirst(projection, filter, sort, TestEntity.class);
    }

    @Test
    public void testFindFirstWithSelectPropNamesAndFilter() {
        Collection<String> selectPropNames = Arrays.asList("name");
        Bson filter = new Document("type", "special");
        TestEntity entity = new TestEntity();

        when(mockExecutor.findFirst(selectPropNames, filter, TestEntity.class)).thenReturn(Mono.just(entity));

        StepVerifier.create(mapper.findFirst(selectPropNames, filter)).expectNext(entity).verifyComplete();

        verify(mockExecutor).findFirst(selectPropNames, filter, TestEntity.class);
    }

    @Test
    public void testFindFirstWithSelectPropNamesFilterAndSort() {
        Collection<String> selectPropNames = Arrays.asList("id", "name");
        Bson filter = new Document("active", true);
        Bson sort = new Document("createdAt", -1);
        TestEntity entity = new TestEntity();

        when(mockExecutor.findFirst(selectPropNames, filter, sort, TestEntity.class)).thenReturn(Mono.just(entity));

        StepVerifier.create(mapper.findFirst(selectPropNames, filter, sort)).expectNext(entity).verifyComplete();

        verify(mockExecutor).findFirst(selectPropNames, filter, sort, TestEntity.class);
    }

    @Test
    public void testFindOneAndReplace() {
        Bson filter = new Document("id", 789);
        TestEntity replacement = new TestEntity();
        TestEntity result = new TestEntity();

        when(mockExecutor.findOneAndReplace(filter, replacement, TestEntity.class)).thenReturn(Mono.just(result));

        StepVerifier.create(mapper.findOneAndReplace(filter, replacement)).expectNext(result).verifyComplete();

        verify(mockExecutor).findOneAndReplace(filter, replacement, TestEntity.class);
    }

    @Test
    public void testFindOneAndReplaceWithOptions() {
        Bson filter = new Document("key", "oldValue");
        TestEntity replacement = new TestEntity();
        FindOneAndReplaceOptions options = new FindOneAndReplaceOptions().upsert(true);
        TestEntity result = new TestEntity();

        when(mockExecutor.findOneAndReplace(filter, replacement, options, TestEntity.class)).thenReturn(Mono.just(result));

        StepVerifier.create(mapper.findOneAndReplace(filter, replacement, options)).expectNext(result).verifyComplete();

        verify(mockExecutor).findOneAndReplace(filter, replacement, options, TestEntity.class);
    }

    @Test
    public void testGetWithObjectId() {
        ObjectId objectId = new ObjectId();
        TestEntity entity = new TestEntity();
        entity.setId(objectId.toString());

        when(mockExecutor.get(objectId, TestEntity.class)).thenReturn(Mono.just(entity));

        StepVerifier.create(mapper.get(objectId)).expectNext(entity).verifyComplete();

        verify(mockExecutor).get(objectId, TestEntity.class);
    }

    @Test
    public void testGetWithObjectIdAndSelectPropNames() {
        ObjectId objectId = new ObjectId();
        Collection<String> selectPropNames = Arrays.asList("field1", "field2");
        TestEntity entity = new TestEntity();

        when(mockExecutor.get(objectId, selectPropNames, TestEntity.class)).thenReturn(Mono.just(entity));

        StepVerifier.create(mapper.get(objectId, selectPropNames)).expectNext(entity).verifyComplete();

        verify(mockExecutor).get(objectId, selectPropNames, TestEntity.class);
    }

    @Test
    public void testGetWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        TestEntity entity = new TestEntity();
        entity.setId(objectId);

        when(mockExecutor.get(objectId, TestEntity.class)).thenReturn(Mono.just(entity));

        StepVerifier.create(mapper.get(objectId)).expectNext(entity).verifyComplete();

        verify(mockExecutor).get(objectId, TestEntity.class);
    }

    @Test
    public void testGetWithStringObjectIdAndSelectPropNames() {
        String objectId = "507f1f77bcf86cd799439011";
        Collection<String> selectPropNames = Arrays.asList("name", "value");
        TestEntity entity = new TestEntity();

        when(mockExecutor.get(objectId, selectPropNames, TestEntity.class)).thenReturn(Mono.just(entity));

        StepVerifier.create(mapper.get(objectId, selectPropNames)).expectNext(entity).verifyComplete();

        verify(mockExecutor).get(objectId, selectPropNames, TestEntity.class);
    }

    @Test
    public void testGroupBy() {
        String fieldName = "department";
        List<TestEntity> results = Arrays.asList(new TestEntity());

        when(mockExecutor.groupBy(fieldName, TestEntity.class)).thenReturn(Flux.fromIterable(results));

        StepVerifier.create(mapper.groupBy(fieldName)).expectNextSequence(results).verifyComplete();

        verify(mockExecutor).groupBy(fieldName, TestEntity.class);
    }

    @Test
    public void testGroupByAndCount() {
        String fieldName = "status";
        List<TestEntity> results = Arrays.asList(new TestEntity());

        when(mockExecutor.groupByAndCount(fieldName, TestEntity.class)).thenReturn(Flux.fromIterable(results));

        StepVerifier.create(mapper.groupByAndCount(fieldName)).expectNextSequence(results).verifyComplete();

        verify(mockExecutor).groupByAndCount(fieldName, TestEntity.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGroupByAndCountRejectsGroupFieldNamedCountForEntityMapper_sliceI() {
        // The mapper passes its entity class as rowType, so a group field named "count" would be projected as
        // {count: key} and then overwritten by the {count: n} column: the executor rejects it eagerly instead.
        final com.mongodb.reactivestreams.client.MongoCollection<Document> collection = mock(com.mongodb.reactivestreams.client.MongoCollection.class);
        final com.mongodb.reactivestreams.client.AggregatePublisher<Document> publisher = mock(com.mongodb.reactivestreams.client.AggregatePublisher.class);
        when(collection.aggregate(org.mockito.ArgumentMatchers.anyList(), org.mockito.ArgumentMatchers.eq(Document.class))).thenReturn(publisher);

        final MongoCollectionExecutor executor = new MongoCollectionExecutor(collection);
        final MongoCollectionMapper<TestEntity> entityMapper = new MongoCollectionMapper<>(executor, TestEntity.class);

        final IllegalArgumentException single = assertThrows(IllegalArgumentException.class, () -> entityMapper.groupByAndCount("count"));
        assertEquals("Group field name 'count' conflicts with the count column of groupByAndCount; use Document as the row type", single.getMessage());
        assertThrows(IllegalArgumentException.class, () -> entityMapper.groupByAndCount(Arrays.asList("name", "count")));
        org.mockito.Mockito.verifyNoInteractions(collection);

        // A Document mapper keeps the key under _id, so the same field name is accepted.
        final MongoCollectionMapper<Document> documentMapper = new MongoCollectionMapper<>(executor, Document.class);
        documentMapper.groupByAndCount("count");
        documentMapper.groupByAndCount(Arrays.asList("name", "count"));
        verify(collection, org.mockito.Mockito.times(2)).aggregate(org.mockito.ArgumentMatchers.anyList(), org.mockito.ArgumentMatchers.eq(Document.class));
    }

    @Test
    public void testGroupByAndCountMultipleFields() {
        Collection<String> fieldNames = Arrays.asList("type", "region");
        List<TestEntity> results = Arrays.asList(new TestEntity());

        when(mockExecutor.groupByAndCount(fieldNames, TestEntity.class)).thenReturn(Flux.fromIterable(results));

        StepVerifier.create(mapper.groupByAndCount(fieldNames)).expectNextSequence(results).verifyComplete();

        verify(mockExecutor).groupByAndCount(fieldNames, TestEntity.class);
    }

    @Test
    public void testGroupByMultipleFields() {
        Collection<String> fieldNames = Arrays.asList("category", "status");
        List<TestEntity> results = Arrays.asList(new TestEntity());

        when(mockExecutor.groupBy(fieldNames, TestEntity.class)).thenReturn(Flux.fromIterable(results));

        StepVerifier.create(mapper.groupBy(fieldNames)).expectNextSequence(results).verifyComplete();

        verify(mockExecutor).groupBy(fieldNames, TestEntity.class);
    }

    @Test
    public void testListWithFilter() {
        Bson filter = new Document("category", "electronics");
        List<TestEntity> entities = Arrays.asList(new TestEntity(), new TestEntity());

        when(mockExecutor.list(filter, TestEntity.class)).thenReturn(Flux.fromIterable(entities));

        StepVerifier.create(mapper.list(filter)).expectNextSequence(entities).verifyComplete();

        verify(mockExecutor).list(filter, TestEntity.class);
    }

    @Test
    public void testListWithProjectionFilterAndSort() {
        Bson projection = Projections.exclude("_id");
        Bson filter = new Document("visible", true);
        Bson sort = Sorts.descending("createdAt");
        List<TestEntity> entities = Arrays.asList(new TestEntity());

        when(mockExecutor.list(projection, filter, sort, TestEntity.class)).thenReturn(Flux.fromIterable(entities));

        StepVerifier.create(mapper.list(projection, filter, sort)).expectNextSequence(entities).verifyComplete();

        verify(mockExecutor).list(projection, filter, sort, TestEntity.class);
    }

    @Test
    public void testListWithSelectPropNamesAndFilter() {
        Collection<String> selectPropNames = Arrays.asList("id", "name");
        Bson filter = new Document("type", "user");
        List<TestEntity> entities = Arrays.asList(new TestEntity());

        when(mockExecutor.list(selectPropNames, filter, TestEntity.class)).thenReturn(Flux.fromIterable(entities));

        StepVerifier.create(mapper.list(selectPropNames, filter)).expectNextSequence(entities).verifyComplete();

        verify(mockExecutor).list(selectPropNames, filter, TestEntity.class);
    }

    @Test
    public void testListWithSelectPropNamesFilterAndSort() {
        Collection<String> selectPropNames = Arrays.asList("name", "date");
        Bson filter = new Document("published", true);
        Bson sort = new Document("date", -1);
        List<TestEntity> entities = Arrays.asList(new TestEntity());

        when(mockExecutor.list(selectPropNames, filter, sort, TestEntity.class)).thenReturn(Flux.fromIterable(entities));

        StepVerifier.create(mapper.list(selectPropNames, filter, sort)).expectNextSequence(entities).verifyComplete();

        verify(mockExecutor).list(selectPropNames, filter, sort, TestEntity.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testQueryDottedSelectNameThroughMapperFillsNestedColumn() {
        // The mapper's Collection-projection Dataset overloads delegate with the entity class as row type, so a dotted
        // select name must come back as the nested value (it was an all-null column) and a path through an array fails.
        final com.mongodb.reactivestreams.client.MongoCollection<Document> collection = mock(com.mongodb.reactivestreams.client.MongoCollection.class);
        final com.mongodb.reactivestreams.client.FindPublisher<Document> findPublisher = mock(com.mongodb.reactivestreams.client.FindPublisher.class);
        when(collection.find(org.mockito.ArgumentMatchers.any(Bson.class))).thenReturn(findPublisher);
        when(findPublisher.projection(org.mockito.ArgumentMatchers.any())).thenReturn(findPublisher);
        when(findPublisher.sort(org.mockito.ArgumentMatchers.any())).thenReturn(findPublisher);
        when(findPublisher.skip(org.mockito.ArgumentMatchers.anyInt())).thenReturn(findPublisher);
        when(findPublisher.limit(org.mockito.ArgumentMatchers.anyInt())).thenReturn(findPublisher);
        final List<Document> rows = new java.util.ArrayList<>(Arrays.asList(
                new Document("_id", "1").append("name", "n1").append("address", new Document("city", "Paris")), new Document("_id", "2").append("name", "n2")));
        org.mockito.Mockito.doAnswer(invocation -> {
            Flux.fromIterable(new java.util.ArrayList<>(rows)).subscribe((org.reactivestreams.Subscriber<? super Document>) invocation.getArgument(0));
            return null;
        }).when(findPublisher).subscribe(org.mockito.ArgumentMatchers.any());

        final MongoCollectionMapper<TestEntity> entityMapper = new MongoCollectionMapper<>(new MongoCollectionExecutor(collection), TestEntity.class);
        final List<String> names = Arrays.asList("name", "address.city");
        final Bson filter = new Document();
        final Bson sort = new Document("_id", 1);

        for (final Mono<Dataset> mono : Arrays.asList(entityMapper.query(names, filter), entityMapper.query(names, filter, 1, 5),
                entityMapper.query(names, filter, sort), entityMapper.query(names, filter, sort, 1, 5))) {
            final Dataset ds = mono.block();
            assertEquals(names, ds.columnNames());
            assertEquals(Arrays.asList("n1", "n2"), ds.getColumn("name"));
            assertEquals(Arrays.asList("Paris", null), ds.getColumn("address.city"));
        }

        rows.add(new Document("_id", "3").append("name", "n3").append("address", Arrays.asList(new Document("city", "Rome"))));
        StepVerifier.create(entityMapper.query(names, filter)).expectError(ClassCastException.class).verify();
        StepVerifier.create(entityMapper.query(names, filter, sort, 1, 5)).expectError(ClassCastException.class).verify();
    }

    @Test
    public void testQueryForBoolean() {
        String propName = "isActive";
        Bson filter = new Document("id", 1);

        when(mockExecutor.queryForBoolean(propName, filter)).thenReturn(Mono.just(true));

        StepVerifier.create(mapper.queryForBoolean(propName, filter)).expectNext(true).verifyComplete();

        verify(mockExecutor).queryForBoolean(propName, filter);
    }

    @Test
    public void testQueryForByte() {
        String propName = "level";
        Bson filter = new Document("userId", 456);

        when(mockExecutor.queryForByte(propName, filter)).thenReturn(Mono.just((byte) 5));

        StepVerifier.create(mapper.queryForByte(propName, filter)).expectNext((byte) 5).verifyComplete();

        verify(mockExecutor).queryForByte(propName, filter);
    }

    @Test
    public void testQueryForChar() {
        String propName = "grade";
        Bson filter = new Document("studentId", 123);

        when(mockExecutor.queryForChar(propName, filter)).thenReturn(Mono.just('A'));

        StepVerifier.create(mapper.queryForChar(propName, filter)).expectNext('A').verifyComplete();

        verify(mockExecutor).queryForChar(propName, filter);
    }

    @Test
    public void testQueryForDate() {
        String propName = "createdAt";
        Bson filter = new Document("orderId", "order123");
        Date date = new Date();

        when(mockExecutor.queryForDate(propName, filter)).thenReturn(Mono.just(date));

        StepVerifier.create(mapper.queryForDate(propName, filter)).expectNext(date).verifyComplete();

        verify(mockExecutor).queryForDate(propName, filter);
    }

    @Test
    public void testQueryForDateWithValueType() {
        String propName = "modifiedAt";
        Bson filter = new Document("docId", "doc123");
        Date date = new Date();

        when(mockExecutor.queryForDate(propName, filter, Date.class)).thenReturn(Mono.just(date));

        StepVerifier.create(mapper.queryForDate(propName, filter, Date.class)).expectNext(date).verifyComplete();

        verify(mockExecutor).queryForDate(propName, filter, Date.class);
    }

    @Test
    public void testQueryForDouble() {
        String propName = "latitude";
        Bson filter = new Document("locationId", "loc123");

        when(mockExecutor.queryForDouble(propName, filter)).thenReturn(Mono.just(37.7749));

        StepVerifier.create(mapper.queryForDouble(propName, filter)).expectNext(37.7749).verifyComplete();

        verify(mockExecutor).queryForDouble(propName, filter);
    }

    @Test
    public void testQueryForFloat() {
        String propName = "price";
        Bson filter = new Document("productId", "prod123");

        when(mockExecutor.queryForFloat(propName, filter)).thenReturn(Mono.just(19.99f));

        StepVerifier.create(mapper.queryForFloat(propName, filter)).expectNext(19.99f).verifyComplete();

        verify(mockExecutor).queryForFloat(propName, filter);
    }

    @Test
    public void testQueryForInt() {
        String propName = "count";
        Bson filter = new Document("category", "electronics");

        when(mockExecutor.queryForInt(propName, filter)).thenReturn(Mono.just(42));

        StepVerifier.create(mapper.queryForInt(propName, filter)).expectNext(42).verifyComplete();

        verify(mockExecutor).queryForInt(propName, filter);
    }

    @Test
    public void testQueryForLong() {
        String propName = "timestamp";
        Bson filter = new Document("eventId", "evt123");

        when(mockExecutor.queryForLong(propName, filter)).thenReturn(Mono.just(1234567890L));

        StepVerifier.create(mapper.queryForLong(propName, filter)).expectNext(1234567890L).verifyComplete();

        verify(mockExecutor).queryForLong(propName, filter);
    }

    @Test
    public void testQueryForShort() {
        String propName = "score";
        Bson filter = new Document("gameId", 789);

        when(mockExecutor.queryForShort(propName, filter)).thenReturn(Mono.just((short) 100));

        StepVerifier.create(mapper.queryForShort(propName, filter)).expectNext((short) 100).verifyComplete();

        verify(mockExecutor).queryForShort(propName, filter);
    }

    @Test
    public void testqueryForSingleValue() {
        String propName = "customField";
        Bson filter = new Document("key", "value");
        String value = "custom value";

        when(mockExecutor.queryForSingleValue(propName, filter, String.class)).thenReturn(Mono.just(value));

        StepVerifier.create(mapper.queryForSingleValue(propName, filter, String.class)).expectNext(value).verifyComplete();

        verify(mockExecutor).queryForSingleValue(propName, filter, String.class);
    }

    @Test
    public void testQueryForString() {
        String propName = "name";
        Bson filter = new Document("userId", 123);

        when(mockExecutor.queryForString(propName, filter)).thenReturn(Mono.just("John Doe"));

        StepVerifier.create(mapper.queryForString(propName, filter)).expectNext("John Doe").verifyComplete();

        verify(mockExecutor).queryForString(propName, filter);
    }

    @Test
    public void testQueryWithFilter() {
        Bson filter = new Document("active", true);
        Dataset mockDataset = Dataset.empty();

        when(mockExecutor.query(filter, TestEntity.class)).thenReturn(Mono.just(mockDataset));

        StepVerifier.create(mapper.query(filter)).expectNext(mockDataset).verifyComplete();

        verify(mockExecutor).query(filter, TestEntity.class);
    }

    @Test
    public void testQueryWithProjectionFilterAndSort() {
        Bson projection = Projections.include("name", "description");
        Bson filter = new Document("visible", true);
        Bson sort = Sorts.ascending("name");
        Dataset mockDataset = Dataset.empty();

        when(mockExecutor.query(projection, filter, sort, TestEntity.class)).thenReturn(Mono.just(mockDataset));

        StepVerifier.create(mapper.query(projection, filter, sort)).expectNext(mockDataset).verifyComplete();

        verify(mockExecutor).query(projection, filter, sort, TestEntity.class);
    }

    @Test
    public void testQueryWithSelectPropNamesAndFilter() {
        Collection<String> selectPropNames = Arrays.asList("id", "name");
        Bson filter = new Document("status", "active");
        Dataset mockDataset = Dataset.empty();

        when(mockExecutor.query(selectPropNames, filter, TestEntity.class)).thenReturn(Mono.just(mockDataset));

        StepVerifier.create(mapper.query(selectPropNames, filter)).expectNext(mockDataset).verifyComplete();

        verify(mockExecutor).query(selectPropNames, filter, TestEntity.class);
    }

    @Test
    public void testQueryWithSelectPropNamesFilterAndSort() {
        Collection<String> selectPropNames = Arrays.asList("title", "author");
        Bson filter = new Document("published", true);
        Bson sort = new Document("date", -1);
        Dataset mockDataset = Dataset.empty();

        when(mockExecutor.query(selectPropNames, filter, sort, TestEntity.class)).thenReturn(Mono.just(mockDataset));

        StepVerifier.create(mapper.query(selectPropNames, filter, sort)).expectNext(mockDataset).verifyComplete();

        verify(mockExecutor).query(selectPropNames, filter, sort, TestEntity.class);
    }

    @Test
    public void testReplaceOneWithFilter() {
        Bson filter = new Document("id", 456);
        TestEntity replacement = new TestEntity();
        UpdateResult mockResult = mock(UpdateResult.class);

        when(mockExecutor.replaceOne(filter, replacement)).thenReturn(Mono.just(mockResult));

        StepVerifier.create(mapper.replaceOne(filter, replacement)).expectNext(mockResult).verifyComplete();

        verify(mockExecutor).replaceOne(filter, replacement);
    }

    @Test
    public void testReplaceOneWithFilterAndOptions() {
        Bson filter = new Document("key", "value");
        TestEntity replacement = new TestEntity();
        ReplaceOptions options = new ReplaceOptions().upsert(true);
        UpdateResult mockResult = mock(UpdateResult.class);

        when(mockExecutor.replaceOne(filter, replacement, options)).thenReturn(Mono.just(mockResult));

        StepVerifier.create(mapper.replaceOne(filter, replacement, options)).expectNext(mockResult).verifyComplete();

        verify(mockExecutor).replaceOne(filter, replacement, options);
    }

    @Test
    public void testBulkWrite() {
        List<WriteModel<Document>> requests = Arrays.asList(new InsertOneModel<>(new Document("id", 1)));
        BulkWriteResult mockResult = mock(BulkWriteResult.class);

        when(mockExecutor.bulkWrite(requests)).thenReturn(Mono.just(mockResult));

        StepVerifier.create(mapper.bulkWrite(requests)).expectNext(mockResult).verifyComplete();

        verify(mockExecutor).bulkWrite(requests);
    }

    @Test
    public void testBulkWriteWithOptions() {
        List<WriteModel<Document>> requests = Arrays.asList(new InsertOneModel<>(new Document("name", "test")));
        BulkWriteOptions options = new BulkWriteOptions();
        BulkWriteResult mockResult = mock(BulkWriteResult.class);

        when(mockExecutor.bulkWrite(requests, options)).thenReturn(Mono.just(mockResult));

        StepVerifier.create(mapper.bulkWrite(requests, options)).expectNext(mockResult).verifyComplete();

        verify(mockExecutor).bulkWrite(requests, options);
    }

    @Test
    public void testCollExecutor() {
        MongoCollectionExecutor result = mapper.collectionExecutor();
        assertEquals(mockExecutor, result);
    }

    @Test
    public void testMapReduce() {
        String mapFunction = "function() { emit(this.category, 1); }";
        String reduceFunction = "function(key, values) { return Array.sum(values); }";
        List<TestEntity> results = Arrays.asList(new TestEntity());

        when(mockExecutor.mapReduce(mapFunction, reduceFunction, TestEntity.class)).thenReturn(Flux.fromIterable(results));

        StepVerifier.create(mapper.mapReduce(mapFunction, reduceFunction)).expectNextSequence(results).verifyComplete();

        verify(mockExecutor).mapReduce(mapFunction, reduceFunction, TestEntity.class);
    }

    @Test
    public void testReplaceOneWithObjectId() {
        ObjectId objectId = new ObjectId();
        TestEntity replacement = new TestEntity();
        UpdateResult mockResult = mock(UpdateResult.class);

        when(mockExecutor.replaceOne(objectId, replacement)).thenReturn(Mono.just(mockResult));

        StepVerifier.create(mapper.replaceOne(objectId, replacement)).expectNext(mockResult).verifyComplete();

        verify(mockExecutor).replaceOne(objectId, replacement);
    }

    @Test
    public void testReplaceOneWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        TestEntity replacement = new TestEntity();
        UpdateResult mockResult = mock(UpdateResult.class);

        when(mockExecutor.replaceOne(objectId, replacement)).thenReturn(Mono.just(mockResult));

        StepVerifier.create(mapper.replaceOne(objectId, replacement)).expectNext(mockResult).verifyComplete();

        verify(mockExecutor).replaceOne(objectId, replacement);
    }

    private static <E> MongoCollectionMapper<E> mapperReturning(final Document row, final Class<E> entityClass) {
        return new MongoCollectionMapper<>(new MongoCollectionExecutor(MongoCollectionExecutorTestSupport.mockCollectionReturning(row)), entityClass);
    }

    // ---- 2026-10-24 r2 sliceV ----

    /** An entity whose text value looks like an aggregation field path. */
    public static class LiteralNoteEntity {
        private String note;

        public String getNote() {
            return note;
        }

        public void setNote(final String note) {
            this.note = note;
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPipelineUpdatesWriteEntityValuesLiterallyAndDocumentValuesAsExpressions_r2sliceV() {
        // Pins the mapper docs of updateOne/updateMany/findOneAndUpdate(Bson, Collection): a converted entity becomes a
        // {$set: {f: {$literal: v}}} stage (so "$other" is stored as text), while a Document element (T = Document) keeps
        // its values as aggregation expressions.
        final com.mongodb.reactivestreams.client.MongoCollection<Document> coll = mock(com.mongodb.reactivestreams.client.MongoCollection.class);
        when(coll.updateOne(org.mockito.ArgumentMatchers.any(Bson.class), org.mockito.ArgumentMatchers.anyList())).thenReturn(Mono.empty());
        when(coll.updateMany(org.mockito.ArgumentMatchers.any(Bson.class), org.mockito.ArgumentMatchers.anyList())).thenReturn(Mono.empty());
        when(coll.findOneAndUpdate(org.mockito.ArgumentMatchers.any(Bson.class), org.mockito.ArgumentMatchers.anyList())).thenReturn(Mono.empty());

        final MongoCollectionExecutor executor = new MongoCollectionExecutor(coll);
        final MongoCollectionMapper<LiteralNoteEntity> entityMapper = new MongoCollectionMapper<>(executor, LiteralNoteEntity.class);
        final LiteralNoteEntity entity = new LiteralNoteEntity();
        entity.setNote("$other");
        final Bson filter = new Document("k", 1);

        entityMapper.updateOne(filter, List.of(entity));
        entityMapper.updateMany(filter, List.of(entity));
        entityMapper.findOneAndUpdate(filter, List.of(entity));
        new MongoCollectionMapper<>(executor, Document.class).updateOne(filter, List.of(new Document("ref", "$other")));

        final org.mockito.ArgumentCaptor<List<Bson>> updateOneStages = org.mockito.ArgumentCaptor.forClass(List.class);
        final org.mockito.ArgumentCaptor<List<Bson>> updateManyStages = org.mockito.ArgumentCaptor.forClass(List.class);
        final org.mockito.ArgumentCaptor<List<Bson>> findOneAndUpdateStages = org.mockito.ArgumentCaptor.forClass(List.class);
        verify(coll, org.mockito.Mockito.times(2)).updateOne(org.mockito.ArgumentMatchers.any(Bson.class), updateOneStages.capture());
        verify(coll).updateMany(org.mockito.ArgumentMatchers.any(Bson.class), updateManyStages.capture());
        verify(coll).findOneAndUpdate(org.mockito.ArgumentMatchers.any(Bson.class), findOneAndUpdateStages.capture());

        final Document literalStage = new Document("$set", new Document("note", new Document("$literal", "$other")));

        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals(List.of(literalStage), updateOneStages.getAllValues().get(0), "updateOne"),
                () -> assertEquals(List.of(literalStage), updateManyStages.getValue(), "updateMany"),
                () -> assertEquals(List.of(literalStage), findOneAndUpdateStages.getValue(), "findOneAndUpdate"),
                () -> assertEquals(List.of(new Document("$set", new Document("ref", "$other"))), updateOneStages.getAllValues().get(1),
                        "a Document element keeps its values as expressions"));
    }

    // ---- 2026-10-27 sliceV ----

    @SuppressWarnings("unchecked")
    @Test
    public void testFindOneAndModifyConversionFailureIsSignalledAfterTheWriteAndRetryRunsItAgain_sliceV() {
        // Pins the findOneAndUpdate/findOneAndReplace/findOneAndDelete docs: the returned document is converted to the mapped type
        // only after the driver's write publisher has emitted it (the server has applied the write), so a conversion failure reaches
        // onError after the write, and every retry() resubscription runs the write again. The cold Mono below stands in for the
        // driver's find-and-modify publisher; each subscription counts as one write sent to the server.
        final com.mongodb.reactivestreams.client.MongoCollection<Document> coll = mock(com.mongodb.reactivestreams.client.MongoCollection.class);
        final java.util.concurrent.atomic.AtomicInteger writes = new java.util.concurrent.atomic.AtomicInteger();
        final Mono<Document> returnedDocument = Mono.fromCallable(() -> {
            writes.incrementAndGet();
            return new Document("name", "n").append("value", "notANumber"); // cannot be converted to TestEntity's int value
        });
        when(coll.findOneAndUpdate(org.mockito.ArgumentMatchers.any(Bson.class), org.mockito.ArgumentMatchers.any(Bson.class))).thenReturn(returnedDocument);
        when(coll.findOneAndReplace(org.mockito.ArgumentMatchers.any(Bson.class), org.mockito.ArgumentMatchers.any(Document.class)))
                .thenReturn(returnedDocument);
        when(coll.findOneAndDelete(org.mockito.ArgumentMatchers.any(Bson.class))).thenReturn(returnedDocument);

        final MongoCollectionMapper<TestEntity> entityMapper = new MongoCollectionMapper<>(new MongoCollectionExecutor(coll), TestEntity.class);
        final TestEntity entity = new TestEntity();
        entity.setName("n");
        final Bson filter = new Document("k", 1);

        final List<java.util.function.Supplier<Mono<TestEntity>>> calls = List.of(() -> entityMapper.findOneAndUpdate(filter, entity),
                () -> entityMapper.findOneAndReplace(filter, entity), () -> entityMapper.findOneAndDelete(filter));

        org.junit.jupiter.api.Assertions.assertAll(calls.stream().map(call -> (org.junit.jupiter.api.function.Executable) () -> {
            writes.set(0);
            StepVerifier.create(call.get().retry(1)).verifyError(IllegalArgumentException.class);
            assertEquals(2, writes.get(), "the write runs on the first subscription and again on the retry");
        }));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testDistinctReadsBsonValueRowTypesAsSingleValues_sliceV() {
        // readRow reads a Binary/Code/Symbol/BsonValue row type as the single value of the row (it is not mapped as a bean), so
        // distinct must route it like String: group rows {_id: value}, no $project. Through the $project the server returns a
        // dotted path as {n: {bin: value}} and a JavaScript Code as plain text (both observed live), which the single-value
        // conversion cannot turn back into the value. The mock answers each pipeline the way the server does.
        final List<List<? extends Bson>> pipelines = new java.util.ArrayList<>();
        final org.bson.types.Binary bin1 = new org.bson.types.Binary(new byte[] { 1 });
        final org.bson.types.Binary bin2 = new org.bson.types.Binary(new byte[] { 2 });
        final org.bson.types.Code code1 = new org.bson.types.Code("f1");
        final org.bson.types.Code code2 = new org.bson.types.Code("f2");
        final com.mongodb.reactivestreams.client.MongoCollection<Document> coll = mock(com.mongodb.reactivestreams.client.MongoCollection.class);
        when(coll.aggregate(org.mockito.ArgumentMatchers.anyList(), org.mockito.ArgumentMatchers.eq(Document.class))).thenAnswer(invocation -> {
            final List<Document> pipeline = invocation.getArgument(0);
            pipelines.add(pipeline);
            final String field = ((String) ((Document) pipeline.get(0).get("$group")).get("_id")).substring(1);
            final boolean projected = pipeline.stream().anyMatch(stage -> stage.containsKey("$project"));
            final List<Document> rows = "n.bin".equals(field)
                    ? (projected ? List.of(new Document("n", new Document("bin", bin1)), new Document("n", new Document("bin", bin2)))
                            : List.of(new Document("_id", bin1), new Document("_id", bin2)))
                    : (projected ? List.of(new Document("c", "f1"), new Document("c", "f2")) : List.of(new Document("_id", code1), new Document("_id", code2)));
            final com.mongodb.reactivestreams.client.AggregatePublisher<Document> publisher = mock(com.mongodb.reactivestreams.client.AggregatePublisher.class);
            org.mockito.Mockito.doAnswer(subscribe -> {
                Flux.fromIterable(rows).subscribe((org.reactivestreams.Subscriber<? super Document>) subscribe.getArgument(0));
                return null;
            }).when(publisher).subscribe(org.mockito.ArgumentMatchers.any());
            return publisher;
        });

        final MongoCollectionExecutor executor = new MongoCollectionExecutor(coll);
        final MongoCollectionMapper<org.bson.types.Binary> binaryMapper = new MongoCollectionMapper<>(executor, org.bson.types.Binary.class);
        final MongoCollectionMapper<org.bson.types.Code> codeMapper = new MongoCollectionMapper<>(executor, org.bson.types.Code.class);

        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertEquals(List.of(bin1, bin2), binaryMapper.distinct("n.bin").collectList().block(), "dotted path into Binary"),
                () -> assertEquals(List.of(code1, code2), codeMapper.distinct("c").collectList().block(), "Code values"),
                () -> assertEquals(List.of(new Document("$group", new Document("_id", "$c"))), pipelines.get(pipelines.size() - 1),
                        "no $project stage for a BSON value row type, as for String"));
    }

    // ---- 2026-10-27 r2S ----

    @Test
    public void testDistinctReadsEveryBsonValueRowTypeFromTheGroupRows_r2S() {
        // isSingleValueRowType follows MongoDBBase.isBsonValueClass (its local copy): Binary, Code (and its subclass CodeWithScope),
        // Symbol and a scalar BsonValue are read as the single value of each {_id: value} group row, for a top-level, a dotted and an
        // embedded-_id field, with and without a filter. The stub answers each pipeline as the server does (verified live): a $project
        // nests a dotted path ({n: {v: value}}, {_id: {part: value}}) and returns a Code or Symbol as plain text, which the single-value
        // read cannot turn back into the value.
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
                            new MongoCollectionExecutor(serverLikeDistinctCollection(c[1], pipelines)), (Class<?>) c[0]);

                    checks.add(() -> {
                        final List<?> values = (filtered ? distinctMapper.distinct(field, new Document("ok", true)) : distinctMapper.distinct(field))
                                .collectList()
                                .block();

                        assertEquals(Arrays.asList(c[1]), values, label);
                        assertEquals(new Document("$group", new Document("_id", "$" + field)), pipelines.get(0).get(pipelines.get(0).size() - 1), label);
                    });
                }
            }
        }

        org.junit.jupiter.api.Assertions.assertAll(checks);
    }

    @Test
    public void testDistinctPipelineShapeOfNonBsonValueRowTypesIsUnchanged_r2S() {
        // Guards the isSingleValueRowType rule against over-reach: String/Integer still read the group rows directly (int also drops the
        // null group), while a bean, Map, Document, Bson, Object and the BSON containers BsonDocument (a Map) and BsonArray (a List),
        // which isBsonValueClass excludes, still get the $project stage that surfaces the value under the field name.
        final Object[][] cases = { { String.class, "none" }, { Integer.class, "none" }, { int.class, "match" }, { TestEntity.class, "project" },
                { java.util.Map.class, "project" }, { Document.class, "project" }, { Bson.class, "project" }, { Object.class, "project" },
                { org.bson.BsonDocument.class, "project" }, { org.bson.BsonArray.class, "project" } };
        final List<org.junit.jupiter.api.function.Executable> checks = new java.util.ArrayList<>();

        for (final Object[] c : cases) {
            for (final String field : Arrays.asList("name", "address.city", "_id.part")) {
                final List<List<?>> pipelines = new java.util.ArrayList<>();
                final MongoCollectionMapper<?> distinctMapper = new MongoCollectionMapper<>(
                        new MongoCollectionExecutor(serverLikeDistinctCollection(null, pipelines)), (Class<?>) c[0]);

                checks.add(() -> {
                    distinctMapper.distinct(field).collectList().block();
                    distinctMapper.distinct(field, new Document("ok", true)).collectList().block();

                    for (final List<?> pipeline : pipelines) {
                        final Object last = pipeline.get(pipeline.size() - 1);
                        final String label = ((Class<?>) c[0]).getSimpleName() + " " + field + " " + pipeline;

                        switch ((String) c[1]) {
                            case "none" -> assertEquals(new Document("$group", new Document("_id", "$" + field)), last, label);
                            case "match" -> assertEquals(new Document("$match", new Document("_id", new Document("$ne", null))), last, label);
                            default -> org.junit.jupiter.api.Assertions.assertTrue(((Document) last).containsKey("$project"), label);
                        }
                    }

                    assertEquals(2, pipelines.size());
                });
            }
        }

        org.junit.jupiter.api.Assertions.assertAll(checks);
    }

    /**
     * A collection whose {@code aggregate} answers a distinct pipeline as the server does for a stored {@code value}: the group row
     * {@code {_id: value}} without a {@code $project}; with one, the value under the (nested) field name, a {@code Code} (not a
     * {@code CodeWithScope}) or {@code Symbol} as its text. A {@code null} value answers no rows.
     */
    @SuppressWarnings("unchecked")
    private static com.mongodb.reactivestreams.client.MongoCollection<Document> serverLikeDistinctCollection(final Object value,
            final List<List<?>> pipelines) {
        final com.mongodb.reactivestreams.client.MongoCollection<Document> coll = mock(com.mongodb.reactivestreams.client.MongoCollection.class);

        when(coll.aggregate(org.mockito.ArgumentMatchers.anyList(), org.mockito.ArgumentMatchers.eq(Document.class))).thenAnswer(invocation -> {
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

            final com.mongodb.reactivestreams.client.AggregatePublisher<Document> publisher = mock(com.mongodb.reactivestreams.client.AggregatePublisher.class);
            org.mockito.Mockito.doAnswer(subscribe -> {
                Flux.fromIterable(rows).subscribe((org.reactivestreams.Subscriber<? super Document>) subscribe.getArgument(0));
                return null;
            }).when(publisher).subscribe(org.mockito.ArgumentMatchers.any());
            return publisher;
        });

        return coll;
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testIdPropertyIsAddressedAsUnderscoreIdInPropNames_r2S() {
        // Pins the "BSON field names" bullet of the class doc (twin of the sync mapper's _sliceS pin): a String id property is stored
        // only as _id, so a matched document projected on "id" holds just its _id (as the server returns it - verified live) and
        // queryForString("id", ...) completes empty, while "_id" reads the id.
        final ObjectId oid = new ObjectId("507f1f77bcf86cd799439011");
        final com.mongodb.reactivestreams.client.MongoCollection<Document> collection = mock(com.mongodb.reactivestreams.client.MongoCollection.class);
        final com.mongodb.reactivestreams.client.FindPublisher<Document> findPublisher = mock(com.mongodb.reactivestreams.client.FindPublisher.class);
        when(collection.find(org.mockito.ArgumentMatchers.any(Bson.class))).thenReturn(findPublisher);
        when(findPublisher.projection(org.mockito.ArgumentMatchers.any())).thenReturn(findPublisher);
        when(findPublisher.sort(org.mockito.ArgumentMatchers.any())).thenReturn(findPublisher);
        when(findPublisher.skip(org.mockito.ArgumentMatchers.anyInt())).thenReturn(findPublisher);
        when(findPublisher.limit(org.mockito.ArgumentMatchers.anyInt())).thenReturn(findPublisher);
        org.mockito.Mockito.doAnswer(invocation -> {
            Flux.just(new Document("_id", oid)).subscribe((org.reactivestreams.Subscriber<? super Document>) invocation.getArgument(0));
            return null;
        }).when(findPublisher).subscribe(org.mockito.ArgumentMatchers.any());
        final MongoCollectionMapper<TestEntity> idMapper = new MongoCollectionMapper<>(new MongoCollectionExecutor(collection), TestEntity.class);

        StepVerifier.create(idMapper.queryForString("id", new Document())).verifyComplete();
        StepVerifier.create(idMapper.queryForString("_id", new Document())).expectNext(oid.toHexString()).verifyComplete();
    }

    // ---- end 2026-10-27 r2S ----

    // Helper class for testing
    static class TestEntity {
        private String id;
        private String name;
        private int value;

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

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }
    }
}
