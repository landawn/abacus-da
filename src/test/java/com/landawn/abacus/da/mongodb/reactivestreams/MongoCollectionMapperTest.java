package com.landawn.abacus.da.mongodb.reactivestreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import org.mockito.MockitoAnnotations;

import com.landawn.abacus.da.TestBase;
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

public class MongoCollectionMapperTest extends TestBase {

    @Mock
    private MongoCollectionExecutor mockExecutor;

    private MongoCollectionMapper<TestEntity> mapper;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        mapper = new MongoCollectionMapper<>(mockExecutor, TestEntity.class);
    }

    @Test
    public void testCollExecutor() {
        MongoCollectionExecutor result = mapper.collExecutor();
        assertEquals(mockExecutor, result);
    }

    @Test
    public void testExistsWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        when(mockExecutor.exists(objectId)).thenReturn(Mono.just(true));

        StepVerifier.create(mapper.exists(objectId)).expectNext(true).verifyComplete();

        verify(mockExecutor).exists(objectId);
    }

    @Test
    public void testExistsWithObjectId() {
        ObjectId objectId = new ObjectId();
        when(mockExecutor.exists(objectId)).thenReturn(Mono.just(false));

        StepVerifier.create(mapper.exists(objectId)).expectNext(false).verifyComplete();

        verify(mockExecutor).exists(objectId);
    }

    @Test
    public void testExistsWithFilter() {
        Bson filter = new Document("status", "active");
        when(mockExecutor.exists(filter)).thenReturn(Mono.just(true));

        StepVerifier.create(mapper.exists(filter)).expectNext(true).verifyComplete();

        verify(mockExecutor).exists(filter);
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
    public void testGetWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        TestEntity entity = new TestEntity();
        entity.setId(objectId);

        when(mockExecutor.get(objectId, TestEntity.class)).thenReturn(Mono.just(entity));

        StepVerifier.create(mapper.get(objectId)).expectNext(entity).verifyComplete();

        verify(mockExecutor).get(objectId, TestEntity.class);
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
    public void testGetWithStringObjectIdAndSelectPropNames() {
        String objectId = "507f1f77bcf86cd799439011";
        Collection<String> selectPropNames = Arrays.asList("name", "value");
        TestEntity entity = new TestEntity();

        when(mockExecutor.get(objectId, selectPropNames, TestEntity.class)).thenReturn(Mono.just(entity));

        StepVerifier.create(mapper.get(objectId, selectPropNames)).expectNext(entity).verifyComplete();

        verify(mockExecutor).get(objectId, selectPropNames, TestEntity.class);
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
    public void testFindFirstWithFilter() {
        Bson filter = new Document("status", "active");
        TestEntity entity = new TestEntity();

        when(mockExecutor.findFirst(filter, TestEntity.class)).thenReturn(Mono.just(entity));

        StepVerifier.create(mapper.findFirst(filter)).expectNext(entity).verifyComplete();

        verify(mockExecutor).findFirst(filter, TestEntity.class);
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
    public void testListWithFilter() {
        Bson filter = new Document("category", "electronics");
        List<TestEntity> entities = Arrays.asList(new TestEntity(), new TestEntity());

        when(mockExecutor.list(filter, TestEntity.class)).thenReturn(Flux.fromIterable(entities));

        StepVerifier.create(mapper.list(filter)).expectNextSequence(entities).verifyComplete();

        verify(mockExecutor).list(filter, TestEntity.class);
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
    public void testListWithSelectPropNamesAndFilter() {
        Collection<String> selectPropNames = Arrays.asList("id", "name");
        Bson filter = new Document("type", "user");
        List<TestEntity> entities = Arrays.asList(new TestEntity());

        when(mockExecutor.list(selectPropNames, filter, TestEntity.class)).thenReturn(Flux.fromIterable(entities));

        StepVerifier.create(mapper.list(selectPropNames, filter)).expectNextSequence(entities).verifyComplete();

        verify(mockExecutor).list(selectPropNames, filter, TestEntity.class);
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
    public void testQueryForBoolean() {
        String propName = "isActive";
        Bson filter = new Document("id", 1);

        when(mockExecutor.queryForBoolean(propName, filter)).thenReturn(Mono.just(true));

        StepVerifier.create(mapper.queryForBoolean(propName, filter)).expectNext(true).verifyComplete();

        verify(mockExecutor).queryForBoolean(propName, filter);
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
    public void testQueryForByte() {
        String propName = "level";
        Bson filter = new Document("userId", 456);

        when(mockExecutor.queryForByte(propName, filter)).thenReturn(Mono.just((byte) 5));

        StepVerifier.create(mapper.queryForByte(propName, filter)).expectNext((byte) 5).verifyComplete();

        verify(mockExecutor).queryForByte(propName, filter);
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
    public void testQueryForFloat() {
        String propName = "price";
        Bson filter = new Document("productId", "prod123");

        when(mockExecutor.queryForFloat(propName, filter)).thenReturn(Mono.just(19.99f));

        StepVerifier.create(mapper.queryForFloat(propName, filter)).expectNext(19.99f).verifyComplete();

        verify(mockExecutor).queryForFloat(propName, filter);
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
    public void testQueryForString() {
        String propName = "name";
        Bson filter = new Document("userId", 123);

        when(mockExecutor.queryForString(propName, filter)).thenReturn(Mono.just("John Doe"));

        StepVerifier.create(mapper.queryForString(propName, filter)).expectNext("John Doe").verifyComplete();

        verify(mockExecutor).queryForString(propName, filter);
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
    public void testQueryForSingleResult() {
        String propName = "customField";
        Bson filter = new Document("key", "value");
        String value = "custom value";

        when(mockExecutor.queryForSingleResult(propName, filter, String.class)).thenReturn(Mono.just(value));

        StepVerifier.create(mapper.queryForSingleResult(propName, filter, String.class)).expectNext(value).verifyComplete();

        verify(mockExecutor).queryForSingleResult(propName, filter, String.class);
    }

    @Test
    public void testQueryWithFilter() {
        Bson filter = new Document("active", true);
        Dataset mockDataset = mock(Dataset.class);

        when(mockExecutor.query(filter, TestEntity.class)).thenReturn(Mono.just(mockDataset));

        StepVerifier.create(mapper.query(filter)).expectNext(mockDataset).verifyComplete();

        verify(mockExecutor).query(filter, TestEntity.class);
    }

    @Test
    public void testQueryWithFilterOffsetAndCount() {
        Bson filter = new Document("type", "premium");
        int offset = 10;
        int count = 20;
        Dataset mockDataset = mock(Dataset.class);

        when(mockExecutor.query(filter, offset, count, TestEntity.class)).thenReturn(Mono.just(mockDataset));

        StepVerifier.create(mapper.query(filter, offset, count)).expectNext(mockDataset).verifyComplete();

        verify(mockExecutor).query(filter, offset, count, TestEntity.class);
    }

    @Test
    public void testQueryWithSelectPropNamesAndFilter() {
        Collection<String> selectPropNames = Arrays.asList("id", "name");
        Bson filter = new Document("status", "active");
        Dataset mockDataset = mock(Dataset.class);

        when(mockExecutor.query(selectPropNames, filter, TestEntity.class)).thenReturn(Mono.just(mockDataset));

        StepVerifier.create(mapper.query(selectPropNames, filter)).expectNext(mockDataset).verifyComplete();

        verify(mockExecutor).query(selectPropNames, filter, TestEntity.class);
    }

    @Test
    public void testQueryWithSelectPropNamesFilterOffsetAndCount() {
        Collection<String> selectPropNames = Arrays.asList("field1", "field2");
        Bson filter = new Document("category", "books");
        int offset = 5;
        int count = 15;
        Dataset mockDataset = mock(Dataset.class);

        when(mockExecutor.query(selectPropNames, filter, offset, count, TestEntity.class)).thenReturn(Mono.just(mockDataset));

        StepVerifier.create(mapper.query(selectPropNames, filter, offset, count)).expectNext(mockDataset).verifyComplete();

        verify(mockExecutor).query(selectPropNames, filter, offset, count, TestEntity.class);
    }

    @Test
    public void testQueryWithSelectPropNamesFilterAndSort() {
        Collection<String> selectPropNames = Arrays.asList("title", "author");
        Bson filter = new Document("published", true);
        Bson sort = new Document("date", -1);
        Dataset mockDataset = mock(Dataset.class);

        when(mockExecutor.query(selectPropNames, filter, sort, TestEntity.class)).thenReturn(Mono.just(mockDataset));

        StepVerifier.create(mapper.query(selectPropNames, filter, sort)).expectNext(mockDataset).verifyComplete();

        verify(mockExecutor).query(selectPropNames, filter, sort, TestEntity.class);
    }

    @Test
    public void testQueryWithSelectPropNamesFilterSortOffsetAndCount() {
        Collection<String> selectPropNames = Arrays.asList("id", "value");
        Bson filter = new Document("type", "special");
        Bson sort = new Document("priority", 1);
        int offset = 0;
        int count = 50;
        Dataset mockDataset = mock(Dataset.class);

        when(mockExecutor.query(selectPropNames, filter, sort, offset, count, TestEntity.class)).thenReturn(Mono.just(mockDataset));

        StepVerifier.create(mapper.query(selectPropNames, filter, sort, offset, count)).expectNext(mockDataset).verifyComplete();

        verify(mockExecutor).query(selectPropNames, filter, sort, offset, count, TestEntity.class);
    }

    @Test
    public void testQueryWithProjectionFilterAndSort() {
        Bson projection = Projections.include("name", "description");
        Bson filter = new Document("visible", true);
        Bson sort = Sorts.ascending("name");
        Dataset mockDataset = mock(Dataset.class);

        when(mockExecutor.query(projection, filter, sort, TestEntity.class)).thenReturn(Mono.just(mockDataset));

        StepVerifier.create(mapper.query(projection, filter, sort)).expectNext(mockDataset).verifyComplete();

        verify(mockExecutor).query(projection, filter, sort, TestEntity.class);
    }

    @Test
    public void testQueryWithProjectionFilterSortOffsetAndCount() {
        Bson projection = Projections.fields(Projections.include("field1"), Projections.excludeId());
        Bson filter = new Document("active", true);
        Bson sort = Sorts.descending("createdAt");
        int offset = 10;
        int count = 25;
        Dataset mockDataset = mock(Dataset.class);

        when(mockExecutor.query(projection, filter, sort, offset, count, TestEntity.class)).thenReturn(Mono.just(mockDataset));

        StepVerifier.create(mapper.query(projection, filter, sort, offset, count)).expectNext(mockDataset).verifyComplete();

        verify(mockExecutor).query(projection, filter, sort, offset, count, TestEntity.class);
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
    public void testUpdateOneWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        TestEntity update = new TestEntity();
        UpdateResult mockResult = mock(UpdateResult.class);

        when(mockExecutor.updateOne(objectId, update)).thenReturn(Mono.just(mockResult));

        StepVerifier.create(mapper.updateOne(objectId, update)).expectNext(mockResult).verifyComplete();

        verify(mockExecutor).updateOne(objectId, update);
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
    public void testUpdateOneWithFilter() {
        Bson filter = new Document("name", "test");
        TestEntity update = new TestEntity();
        UpdateResult mockResult = mock(UpdateResult.class);

        when(mockExecutor.updateOne(filter, update)).thenReturn(Mono.just(mockResult));

        StepVerifier.create(mapper.updateOne(filter, update)).expectNext(mockResult).verifyComplete();

        verify(mockExecutor).updateOne(filter, update);
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
    public void testUpdateOneWithFilterAndCollection() {
        Bson filter = new Document("key", "value");
        Collection<TestEntity> objList = Arrays.asList(new TestEntity());
        UpdateResult mockResult = mock(UpdateResult.class);

        when(mockExecutor.updateOne(filter, objList)).thenReturn(Mono.just(mockResult));

        StepVerifier.create(mapper.updateOne(filter, objList)).expectNext(mockResult).verifyComplete();

        verify(mockExecutor).updateOne(filter, objList);
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
    public void testUpdateMany() {
        Bson filter = new Document("status", "old");
        TestEntity update = new TestEntity();
        UpdateResult mockResult = mock(UpdateResult.class);

        when(mockExecutor.updateMany(filter, update)).thenReturn(Mono.just(mockResult));

        StepVerifier.create(mapper.updateMany(filter, update)).expectNext(mockResult).verifyComplete();

        verify(mockExecutor).updateMany(filter, update);
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
    public void testReplaceOneWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        TestEntity replacement = new TestEntity();
        UpdateResult mockResult = mock(UpdateResult.class);

        when(mockExecutor.replaceOne(objectId, replacement)).thenReturn(Mono.just(mockResult));

        StepVerifier.create(mapper.replaceOne(objectId, replacement)).expectNext(mockResult).verifyComplete();

        verify(mockExecutor).replaceOne(objectId, replacement);
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
    public void testDeleteOneWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        DeleteResult mockResult = mock(DeleteResult.class);

        when(mockExecutor.deleteOne(objectId)).thenReturn(Mono.just(mockResult));

        StepVerifier.create(mapper.deleteOne(objectId)).expectNext(mockResult).verifyComplete();

        verify(mockExecutor).deleteOne(objectId);
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
    public void testBulkInsert() {
        Collection<TestEntity> entities = Arrays.asList(new TestEntity(), new TestEntity());

        when(mockExecutor.bulkInsert(entities)).thenReturn(Mono.just(2));

        StepVerifier.create(mapper.bulkInsert(entities)).expectNext(2).verifyComplete();

        verify(mockExecutor).bulkInsert(entities);
    }

    @Test
    public void testBulkInsertWithOptions() {
        Collection<TestEntity> entities = Arrays.asList(new TestEntity(), new TestEntity());
        BulkWriteOptions options = new BulkWriteOptions().ordered(false);

        when(mockExecutor.bulkInsert(entities, options)).thenReturn(Mono.just(2));

        StepVerifier.create(mapper.bulkInsert(entities, options)).expectNext(2).verifyComplete();

        verify(mockExecutor).bulkInsert(entities, options);
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
    public void testFindOneAndUpdate() {
        Bson filter = new Document("id", 123);
        TestEntity update = new TestEntity();
        TestEntity result = new TestEntity();

        when(mockExecutor.findOneAndUpdate(filter, update, TestEntity.class)).thenReturn(Mono.just(result));

        StepVerifier.create(mapper.findOneAndUpdate(filter, update)).expectNext(result).verifyComplete();

        verify(mockExecutor).findOneAndUpdate(filter, update, TestEntity.class);
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
    public void testDistinct() {
        String fieldName = "category";
        List<TestEntity> distinctValues = Arrays.asList(new TestEntity(), new TestEntity());

        when(mockExecutor.distinct(fieldName, TestEntity.class)).thenReturn(Flux.fromIterable(distinctValues));

        StepVerifier.create(mapper.distinct(fieldName)).expectNextSequence(distinctValues).verifyComplete();

        verify(mockExecutor).distinct(fieldName, TestEntity.class);
    }

    @Test
    public void testDistinctWithFilter() {
        String fieldName = "type";
        Bson filter = new Document("active", true);
        List<TestEntity> distinctValues = Arrays.asList(new TestEntity());

        when(mockExecutor.distinct(fieldName, filter, TestEntity.class)).thenReturn(Flux.fromIterable(distinctValues));

        StepVerifier.create(mapper.distinct(fieldName, filter)).expectNextSequence(distinctValues).verifyComplete();

        verify(mockExecutor).distinct(fieldName, filter, TestEntity.class);
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
    public void testGroupBy() {
        String fieldName = "department";
        List<TestEntity> results = Arrays.asList(new TestEntity());

        when(mockExecutor.groupBy(fieldName, TestEntity.class)).thenReturn(Flux.fromIterable(results));

        StepVerifier.create(mapper.groupBy(fieldName)).expectNextSequence(results).verifyComplete();

        verify(mockExecutor).groupBy(fieldName, TestEntity.class);
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
    public void testGroupByAndCount() {
        String fieldName = "status";
        List<TestEntity> results = Arrays.asList(new TestEntity());

        when(mockExecutor.groupByAndCount(fieldName, TestEntity.class)).thenReturn(Flux.fromIterable(results));

        StepVerifier.create(mapper.groupByAndCount(fieldName)).expectNextSequence(results).verifyComplete();

        verify(mockExecutor).groupByAndCount(fieldName, TestEntity.class);
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
    public void testMapReduce() {
        String mapFunction = "function() { emit(this.category, 1); }";
        String reduceFunction = "function(key, values) { return Array.sum(values); }";
        List<TestEntity> results = Arrays.asList(new TestEntity());

        when(mockExecutor.mapReduce(mapFunction, reduceFunction, TestEntity.class)).thenReturn(Flux.fromIterable(results));

        StepVerifier.create(mapper.mapReduce(mapFunction, reduceFunction)).expectNextSequence(results).verifyComplete();

        verify(mockExecutor).mapReduce(mapFunction, reduceFunction, TestEntity.class);
    }

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