package com.landawn.abacus.da.mongodb;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;

public class AsyncMongoCollectionMutationTest extends AsyncMongoCollectionExecutorTestSupport {

    @Test
    public void testBulkInsertDelegatesArgumentsAndResult() throws Exception {
        List<TestEntity> entities = Arrays.asList(new TestEntity());
        BulkWriteResult bulkWriteResult = mock(BulkWriteResult.class);
        final var expected = bulkWriteResult;
        when(mockCollExecutor.bulkInsert(entities)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.bulkInsert(entities).get());

        verify(mockCollExecutor).bulkInsert(entities);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testBulkInsertWithOptionsDelegatesArgumentsAndResult() throws Exception {
        List<TestEntity> entities = Arrays.asList(new TestEntity());
        com.mongodb.client.model.BulkWriteOptions opts = new com.mongodb.client.model.BulkWriteOptions();
        BulkWriteResult bulkWriteResult = mock(BulkWriteResult.class);
        final var expected = bulkWriteResult;
        when(mockCollExecutor.bulkInsert(entities, opts)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.bulkInsert(entities, opts).get());

        verify(mockCollExecutor).bulkInsert(entities, opts);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testBulkWriteDelegatesArgumentsAndResult() throws Exception {
        List<WriteModel<? extends Document>> requests = Collections.singletonList(new InsertOneModel<>(new Document("k", "v")));
        BulkWriteResult br = mock(BulkWriteResult.class);
        final var expected = br;
        when(mockCollExecutor.bulkWrite(requests)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.bulkWrite(requests).get());

        verify(mockCollExecutor).bulkWrite(requests);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testBulkWriteWithOptionsDelegatesArgumentsAndResult() throws Exception {
        List<WriteModel<? extends Document>> requests = Collections.singletonList(new InsertOneModel<>(new Document("k", "v")));
        BulkWriteOptions opts = new BulkWriteOptions();
        BulkWriteResult br = mock(BulkWriteResult.class);
        final var expected = br;
        when(mockCollExecutor.bulkWrite(requests, opts)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.bulkWrite(requests, opts).get());

        verify(mockCollExecutor).bulkWrite(requests, opts);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testDeleteManyFilterDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("status", "active");
        DeleteResult dr = mock(DeleteResult.class);
        final var expected = dr;
        when(mockCollExecutor.deleteMany(filter)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.deleteMany(filter).get());

        verify(mockCollExecutor).deleteMany(filter);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testDeleteManyWithFilterAndOptionsDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("k", "v");
        DeleteOptions opts = new DeleteOptions();
        DeleteResult dr = mock(DeleteResult.class);
        final var expected = dr;
        when(mockCollExecutor.deleteMany(filter, opts)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.deleteMany(filter, opts).get());

        verify(mockCollExecutor).deleteMany(filter, opts);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testDeleteOneFilterDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("status", "active");
        DeleteResult dr = mock(DeleteResult.class);
        final var expected = dr;
        when(mockCollExecutor.deleteOne(filter)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.deleteOne(filter).get());

        verify(mockCollExecutor).deleteOne(filter);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testDeleteOneObjectIdDelegatesArgumentsAndResult() throws Exception {
        ObjectId oid = new ObjectId();
        DeleteResult dr = mock(DeleteResult.class);
        final var expected = dr;
        when(mockCollExecutor.deleteOne(oid)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.deleteOne(oid).get());

        verify(mockCollExecutor).deleteOne(oid);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testDeleteOneStringDelegatesArgumentsAndResult() throws Exception {
        DeleteResult dr = mock(DeleteResult.class);
        final var expected = dr;
        when(mockCollExecutor.deleteOne("id1")).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.deleteOne("id1").get());

        verify(mockCollExecutor).deleteOne("id1");
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testDeleteOneWithFilterAndOptionsDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("k", "v");
        DeleteOptions opts = new DeleteOptions();
        DeleteResult dr = mock(DeleteResult.class);
        final var expected = dr;
        when(mockCollExecutor.deleteOne(filter, opts)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.deleteOne(filter, opts).get());

        verify(mockCollExecutor).deleteOne(filter, opts);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testFindOneAndDeleteFilterDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("status", "active");
        Document doc = new Document();
        final var expected = doc;
        when(mockCollExecutor.findOneAndDelete(filter)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.findOneAndDelete(filter).get());

        verify(mockCollExecutor).findOneAndDelete(filter);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testFindOneAndDeleteFilterOptsDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("status", "active");
        com.mongodb.client.model.FindOneAndDeleteOptions opts = new com.mongodb.client.model.FindOneAndDeleteOptions();
        Document doc = new Document();
        final var expected = doc;
        when(mockCollExecutor.findOneAndDelete(filter, opts)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.findOneAndDelete(filter, opts).get());

        verify(mockCollExecutor).findOneAndDelete(filter, opts);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testFindOneAndDeleteFilterRowTypeDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("status", "active");
        TestEntity te = new TestEntity();
        final var expected = te;
        when(mockCollExecutor.findOneAndDelete(filter, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.findOneAndDelete(filter, TestEntity.class).get());

        verify(mockCollExecutor).findOneAndDelete(filter, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testFindOneAndDeleteOptionsRowTypeDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("k", "v");
        FindOneAndDeleteOptions opts = new FindOneAndDeleteOptions();
        TestEntity entity = new TestEntity();
        final var expected = entity;
        when(mockCollExecutor.findOneAndDelete(filter, opts, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.findOneAndDelete(filter, opts, TestEntity.class).get());

        verify(mockCollExecutor).findOneAndDelete(filter, opts, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testFindOneAndReplaceFilterReplDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("status", "active");
        Document repl = new Document("a", 1);
        Document doc = new Document();
        final var expected = doc;
        when(mockCollExecutor.findOneAndReplace(filter, repl)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.findOneAndReplace(filter, repl).get());

        verify(mockCollExecutor).findOneAndReplace(filter, repl);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testFindOneAndReplaceFilterReplOptsDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("status", "active");
        Document repl = new Document("a", 1);
        com.mongodb.client.model.FindOneAndReplaceOptions opts = new com.mongodb.client.model.FindOneAndReplaceOptions();
        Document doc = new Document();
        final var expected = doc;
        when(mockCollExecutor.findOneAndReplace(filter, repl, opts)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.findOneAndReplace(filter, repl, opts).get());

        verify(mockCollExecutor).findOneAndReplace(filter, repl, opts);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testFindOneAndReplaceOptionsRowTypeDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("k", "v");
        Document replacement = new Document("k", "x");
        FindOneAndReplaceOptions opts = new FindOneAndReplaceOptions();
        TestEntity entity = new TestEntity();
        final var expected = entity;
        when(mockCollExecutor.findOneAndReplace(filter, replacement, opts, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.findOneAndReplace(filter, replacement, opts, TestEntity.class).get());

        verify(mockCollExecutor).findOneAndReplace(filter, replacement, opts, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testFindOneAndReplaceRowTypeDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("k", "v");
        Document replacement = new Document("k", "x");
        TestEntity entity = new TestEntity();
        final var expected = entity;
        when(mockCollExecutor.findOneAndReplace(filter, replacement, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.findOneAndReplace(filter, replacement, TestEntity.class).get());

        verify(mockCollExecutor).findOneAndReplace(filter, replacement, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testFindOneAndUpdateFilterUpdateDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("status", "active");
        Document update = new Document("$set", new Document("a", 1));
        Document doc = new Document();
        final var expected = doc;
        when(mockCollExecutor.findOneAndUpdate(filter, update)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.findOneAndUpdate(filter, update).get());

        verify(mockCollExecutor).findOneAndUpdate(filter, update);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testFindOneAndUpdateFilterUpdateOptsDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("status", "active");
        Document update = new Document("$set", new Document("a", 1));
        com.mongodb.client.model.FindOneAndUpdateOptions opts = new com.mongodb.client.model.FindOneAndUpdateOptions();
        Document doc = new Document();
        final var expected = doc;
        when(mockCollExecutor.findOneAndUpdate(filter, update, opts)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.findOneAndUpdate(filter, update, opts).get());

        verify(mockCollExecutor).findOneAndUpdate(filter, update, opts);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testFindOneAndUpdateOptionsRowTypeDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("k", "v");
        Document update = new Document("$set", new Document("k", "w"));
        FindOneAndUpdateOptions opts = new FindOneAndUpdateOptions();
        TestEntity entity = new TestEntity();
        final var expected = entity;
        when(mockCollExecutor.findOneAndUpdate(filter, update, opts, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.findOneAndUpdate(filter, update, opts, TestEntity.class).get());

        verify(mockCollExecutor).findOneAndUpdate(filter, update, opts, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testFindOneAndUpdateRowTypeDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("k", "v");
        Document update = new Document("$set", new Document("k", "w"));
        TestEntity entity = new TestEntity();
        final var expected = entity;
        when(mockCollExecutor.findOneAndUpdate(filter, update, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.findOneAndUpdate(filter, update, TestEntity.class).get());

        verify(mockCollExecutor).findOneAndUpdate(filter, update, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testFindOneAndUpdateWithCollectionAndOptionsDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("k", "v");
        List<TestEntity> objList = Arrays.asList(new TestEntity());
        FindOneAndUpdateOptions opts = new FindOneAndUpdateOptions();
        Document doc = new Document();
        final var expected = doc;
        when(mockCollExecutor.findOneAndUpdate(filter, objList, opts)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.findOneAndUpdate(filter, objList, opts).get());

        verify(mockCollExecutor).findOneAndUpdate(filter, objList, opts);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testFindOneAndUpdateWithCollectionDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("k", "v");
        List<TestEntity> objList = Arrays.asList(new TestEntity());
        Document doc = new Document();
        final var expected = doc;
        when(mockCollExecutor.findOneAndUpdate(filter, objList)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.findOneAndUpdate(filter, objList).get());

        verify(mockCollExecutor).findOneAndUpdate(filter, objList);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testFindOneAndUpdateWithCollectionOptionsAndRowTypeDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("k", "v");
        List<TestEntity> objList = Arrays.asList(new TestEntity());
        FindOneAndUpdateOptions opts = new FindOneAndUpdateOptions();
        TestEntity entity = new TestEntity();
        final var expected = entity;
        when(mockCollExecutor.findOneAndUpdate(filter, objList, opts, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.findOneAndUpdate(filter, objList, opts, TestEntity.class).get());

        verify(mockCollExecutor).findOneAndUpdate(filter, objList, opts, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testFindOneAndUpdateWithCollectionRowTypeDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("k", "v");
        List<TestEntity> objList = Arrays.asList(new TestEntity());
        TestEntity entity = new TestEntity();
        final var expected = entity;
        when(mockCollExecutor.findOneAndUpdate(filter, objList, TestEntity.class)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.findOneAndUpdate(filter, objList, TestEntity.class).get());

        verify(mockCollExecutor).findOneAndUpdate(filter, objList, TestEntity.class);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testInsertManyDelegatesArgumentsAndResult() throws Exception {
        List<TestEntity> entities = Arrays.asList(new TestEntity());
        final var expected = mock(InsertManyResult.class);
        when(mockCollExecutor.insertMany(entities)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.insertMany(entities).get());

        verify(mockCollExecutor).insertMany(entities);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testInsertManyWithOptionsDelegatesArgumentsAndResult() throws Exception {
        List<TestEntity> entities = Arrays.asList(new TestEntity());
        InsertManyOptions opts = new InsertManyOptions();
        final var expected = mock(InsertManyResult.class);
        when(mockCollExecutor.insertMany(entities, opts)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.insertMany(entities, opts).get());

        verify(mockCollExecutor).insertMany(entities, opts);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testInsertOneDelegatesArgumentsAndResult() throws Exception {
        TestEntity entity = new TestEntity();
        final var expected = mock(InsertOneResult.class);
        when(mockCollExecutor.insertOne(entity)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.insertOne(entity).get());

        verify(mockCollExecutor).insertOne(entity);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testInsertOneWithOptionsDelegatesArgumentsAndResult() throws Exception {
        TestEntity entity = new TestEntity();
        InsertOneOptions opts = new InsertOneOptions();
        final var expected = mock(InsertOneResult.class);
        when(mockCollExecutor.insertOne(entity, opts)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.insertOne(entity, opts).get());

        verify(mockCollExecutor).insertOne(entity, opts);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testReplaceOneFilterDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("status", "active");
        Document repl = new Document("a", 1);
        UpdateResult ur = mock(UpdateResult.class);
        final var expected = ur;
        when(mockCollExecutor.replaceOne(filter, repl)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.replaceOne(filter, repl).get());

        verify(mockCollExecutor).replaceOne(filter, repl);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testReplaceOneStringDelegatesArgumentsAndResult() throws Exception {
        Document repl = new Document("a", 1);
        UpdateResult ur = mock(UpdateResult.class);
        final var expected = ur;
        when(mockCollExecutor.replaceOne("id1", repl)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.replaceOne("id1", repl).get());

        verify(mockCollExecutor).replaceOne("id1", repl);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testReplaceOneWithFilterAndOptionsDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("k", "v");
        Document replacement = new Document("k", "v");
        ReplaceOptions opts = new ReplaceOptions();
        UpdateResult ur = mock(UpdateResult.class);
        final var expected = ur;
        when(mockCollExecutor.replaceOne(filter, replacement, opts)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.replaceOne(filter, replacement, opts).get());

        verify(mockCollExecutor).replaceOne(filter, replacement, opts);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testReplaceOneWithObjectIdDelegatesArgumentsAndResult() throws Exception {
        ObjectId objectId = new ObjectId();
        Document replacement = new Document("k", "v");
        UpdateResult ur = mock(UpdateResult.class);
        final var expected = ur;
        when(mockCollExecutor.replaceOne(objectId, replacement)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.replaceOne(objectId, replacement).get());

        verify(mockCollExecutor).replaceOne(objectId, replacement);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testUpdateManyFilterDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("status", "active");
        Document update = new Document("$set", new Document("a", 1));
        UpdateResult ur = mock(UpdateResult.class);
        final var expected = ur;
        when(mockCollExecutor.updateMany(filter, update)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.updateMany(filter, update).get());

        verify(mockCollExecutor).updateMany(filter, update);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testUpdateManyWithCollectionAndOptionsDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("k", "v");
        List<TestEntity> objList = Arrays.asList(new TestEntity());
        UpdateOptions opts = new UpdateOptions();
        UpdateResult ur = mock(UpdateResult.class);
        final var expected = ur;
        when(mockCollExecutor.updateMany(filter, objList, opts)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.updateMany(filter, objList, opts).get());

        verify(mockCollExecutor).updateMany(filter, objList, opts);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testUpdateManyWithCollectionDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("k", "v");
        List<TestEntity> objList = Arrays.asList(new TestEntity());
        UpdateResult ur = mock(UpdateResult.class);
        final var expected = ur;
        when(mockCollExecutor.updateMany(filter, objList)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.updateMany(filter, objList).get());

        verify(mockCollExecutor).updateMany(filter, objList);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testUpdateManyWithOptionsDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("k", "v");
        Document update = new Document("$set", new Document("k", "w"));
        UpdateOptions opts = new UpdateOptions();
        UpdateResult ur = mock(UpdateResult.class);
        final var expected = ur;
        when(mockCollExecutor.updateMany(filter, update, opts)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.updateMany(filter, update, opts).get());

        verify(mockCollExecutor).updateMany(filter, update, opts);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testUpdateOneFilterDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("status", "active");
        Document update = new Document("$set", new Document("a", 1));
        UpdateResult ur = mock(UpdateResult.class);
        final var expected = ur;
        when(mockCollExecutor.updateOne(filter, update)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.updateOne(filter, update).get());

        verify(mockCollExecutor).updateOne(filter, update);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testUpdateOneObjectIdDelegatesArgumentsAndResult() throws Exception {
        ObjectId oid = new ObjectId();
        Document update = new Document("$set", new Document("a", 1));
        UpdateResult ur = mock(UpdateResult.class);
        final var expected = ur;
        when(mockCollExecutor.updateOne(oid, update)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.updateOne(oid, update).get());

        verify(mockCollExecutor).updateOne(oid, update);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testUpdateOneStringDelegatesArgumentsAndResult() throws Exception {
        Document update = new Document("$set", new Document("a", 1));
        UpdateResult ur = mock(UpdateResult.class);
        final var expected = ur;
        when(mockCollExecutor.updateOne("id1", update)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.updateOne("id1", update).get());

        verify(mockCollExecutor).updateOne("id1", update);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testUpdateOneWithFilterAndCollectionDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("k", "v");
        List<TestEntity> objList = Arrays.asList(new TestEntity());
        UpdateResult ur = mock(UpdateResult.class);
        final var expected = ur;
        when(mockCollExecutor.updateOne(filter, objList)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.updateOne(filter, objList).get());

        verify(mockCollExecutor).updateOne(filter, objList);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testUpdateOneWithFilterAndOptionsDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("k", "v");
        Document update = new Document("$set", new Document("k", "w"));
        UpdateOptions opts = new UpdateOptions();
        UpdateResult ur = mock(UpdateResult.class);
        final var expected = ur;
        when(mockCollExecutor.updateOne(filter, update, opts)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.updateOne(filter, update, opts).get());

        verify(mockCollExecutor).updateOne(filter, update, opts);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testUpdateOneWithFilterCollectionAndOptionsDelegatesArgumentsAndResult() throws Exception {
        Document filter = new Document("k", "v");
        List<TestEntity> objList = Arrays.asList(new TestEntity());
        UpdateOptions opts = new UpdateOptions();
        UpdateResult ur = mock(UpdateResult.class);
        final var expected = ur;
        when(mockCollExecutor.updateOne(filter, objList, opts)).thenReturn(expected);

        Assertions.assertSame(expected, asyncExecutor.updateOne(filter, objList, opts).get());

        verify(mockCollExecutor).updateOne(filter, objList, opts);
        verifyNoMoreInteractions(mockCollExecutor);
    }
}
