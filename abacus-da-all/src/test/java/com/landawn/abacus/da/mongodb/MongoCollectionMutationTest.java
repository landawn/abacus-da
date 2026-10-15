package com.landawn.abacus.da.mongodb;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;

public class MongoCollectionMutationTest extends MongoCollectionExecutorTestSupport {

    @Test
    public void testBulkInsert() {
        List<Document> docs = Arrays.asList(new Document("id", 1), new Document("id", 2));
        BulkWriteResult bulkWriteResult = mock(BulkWriteResult.class);
        when(bulkWriteResult.getInsertedCount()).thenReturn(2);
        when(mockCollection.bulkWrite(anyList())).thenReturn(bulkWriteResult);

        BulkWriteResult result = executor.bulkInsert(docs);
        Assertions.assertSame(bulkWriteResult, result);
        Assertions.assertEquals(2, result.getInsertedCount());
    }

    @Test
    public void testBulkInsertRejectsEmptyEntities() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.bulkInsert(List.of()));
    }

    @Test
    public void testBulkInsertRejectsNullEntities() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.bulkInsert(null));
    }

    @Test
    public void testBulkInsertWithDocumentsAndOptions() {
        List<Document> docs = Arrays.asList(new Document("a", 1), new Document("a", 2));
        com.mongodb.bulk.BulkWriteResult bwr = mock(com.mongodb.bulk.BulkWriteResult.class);
        when(bwr.getInsertedCount()).thenReturn(2);
        com.mongodb.client.model.BulkWriteOptions opts = new com.mongodb.client.model.BulkWriteOptions();
        when(mockCollection.bulkWrite(anyList(), any(com.mongodb.client.model.BulkWriteOptions.class))).thenReturn(bwr);
        BulkWriteResult result = executor.bulkInsert(docs, opts);
        Assertions.assertSame(bwr, result);
        Assertions.assertEquals(2, result.getInsertedCount());
    }

    @Test
    public void testBulkInsertWithEntities() {
        TestEntity e1 = new TestEntity();
        e1.setName("a");
        com.mongodb.bulk.BulkWriteResult bwr = mock(com.mongodb.bulk.BulkWriteResult.class);
        when(bwr.getInsertedCount()).thenReturn(1);
        when(mockCollection.bulkWrite(anyList())).thenReturn(bwr);
        BulkWriteResult result = executor.bulkInsert(Arrays.asList(e1));
        Assertions.assertSame(bwr, result);
        Assertions.assertEquals(1, result.getInsertedCount());
    }

    @Test
    public void testBulkWrite() {
        List<WriteModel<Document>> requests = Arrays.asList(mock(WriteModel.class));
        BulkWriteResult bulkWriteResult = mock(BulkWriteResult.class);
        when(mockCollection.bulkWrite(requests)).thenReturn(bulkWriteResult);

        BulkWriteResult result = executor.bulkWrite(requests);
        Assertions.assertSame(bulkWriteResult, result);
    }

    @Test
    public void testBulkWriteRejectsEmptyRequests() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.bulkWrite(List.of()));
    }

    @Test
    public void testBulkWriteRejectsNullRequests() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.bulkWrite(null));
    }

    @Test
    public void testBulkWriteWithRequestsAndOptions() {
        List<WriteModel<Document>> requests = Arrays.asList(new com.mongodb.client.model.InsertOneModel<>(new Document("a", 1)));
        BulkWriteResult bwr = mock(BulkWriteResult.class);
        com.mongodb.client.model.BulkWriteOptions opts = new com.mongodb.client.model.BulkWriteOptions();
        when(mockCollection.bulkWrite(anyList(), any(com.mongodb.client.model.BulkWriteOptions.class))).thenReturn(bwr);
        BulkWriteResult result = executor.bulkWrite(requests, opts);
        Assertions.assertSame(bwr, result);
    }

    @Test
    public void testDeleteMany() {
        Document filter = new Document("status", "inactive");
        DeleteResult deleteResult = mock(DeleteResult.class);
        when(mockCollection.deleteMany(filter)).thenReturn(deleteResult);

        DeleteResult result = executor.deleteMany(filter);
        Assertions.assertSame(deleteResult, result);
    }

    @Test
    public void testDeleteManyWithFilterAndOptions() {
        Document filter = new Document("status", "x");
        com.mongodb.client.model.DeleteOptions options = new com.mongodb.client.model.DeleteOptions();
        DeleteResult deleteResult = mock(DeleteResult.class);
        when(mockCollection.deleteMany(any(Bson.class), any(com.mongodb.client.model.DeleteOptions.class))).thenReturn(deleteResult);

        DeleteResult result = executor.deleteMany(filter, options);
        Assertions.assertSame(deleteResult, result);
    }

    @Test
    public void testDeleteManyWithNullOptionsUsesDefaultOverload() {
        Document filter = new Document("status", "x");
        DeleteResult deleteResult = mock(DeleteResult.class);
        when(mockCollection.deleteMany(filter)).thenReturn(deleteResult);

        DeleteResult result = executor.deleteMany(filter, (DeleteOptions) null);

        Assertions.assertSame(deleteResult, result);
        verify(mockCollection).deleteMany(filter);
        verify(mockCollection, times(0)).deleteMany(any(Bson.class), any(DeleteOptions.class));
    }

    @Test
    public void testDeleteOne() {
        String objectId = "507f1f77bcf86cd799439011";
        DeleteResult deleteResult = mock(DeleteResult.class);
        when(mockCollection.deleteOne(any())).thenReturn(deleteResult);

        DeleteResult result = executor.deleteOne(objectId);
        Assertions.assertSame(deleteResult, result);
    }

    @Test
    public void testDeleteOneWithFilterAndOptions() {
        Document filter = new Document("status", "x");
        com.mongodb.client.model.DeleteOptions options = new com.mongodb.client.model.DeleteOptions();
        DeleteResult deleteResult = mock(DeleteResult.class);
        when(mockCollection.deleteOne(any(Bson.class), any(com.mongodb.client.model.DeleteOptions.class))).thenReturn(deleteResult);

        DeleteResult result = executor.deleteOne(filter, options);
        Assertions.assertSame(deleteResult, result);
    }

    @Test
    public void testDeleteOneWithNullOptionsUsesDefaultOverload() {
        Document filter = new Document("status", "x");
        DeleteResult deleteResult = mock(DeleteResult.class);
        when(mockCollection.deleteOne(filter)).thenReturn(deleteResult);

        DeleteResult result = executor.deleteOne(filter, (DeleteOptions) null);

        Assertions.assertSame(deleteResult, result);
        verify(mockCollection).deleteOne(filter);
        verify(mockCollection, times(0)).deleteOne(any(Bson.class), any(DeleteOptions.class));
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
    public void testFindOneAndDeleteNullFilterThrows() {
        // Regression: a null filter must be rejected with IllegalArgumentException (as documented and as every
        // sibling deleteOne/deleteMany/findOneAndUpdate/findOneAndReplace does). Before the fix the null filter
        // was passed straight to the driver, which omits the query from the findAndModify command and silently
        // deletes an arbitrary document. The driver must NOT be invoked.
        final com.mongodb.client.model.FindOneAndDeleteOptions options = new com.mongodb.client.model.FindOneAndDeleteOptions();

        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.findOneAndDelete((Bson) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.findOneAndDelete((Bson) null, Document.class));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.findOneAndDelete((Bson) null, options));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.findOneAndDelete((Bson) null, options, Document.class));

        verify(mockCollection, never()).findOneAndDelete(any(Bson.class));
        verify(mockCollection, never()).findOneAndDelete(any(Bson.class), any(com.mongodb.client.model.FindOneAndDeleteOptions.class));
    }

    @Test
    public void testFindOneAndDeleteReturnsNullWhenNoDoc() {
        Document filter = new Document();
        when(mockCollection.findOneAndDelete(any(Bson.class))).thenReturn(null);
        Document r = executor.findOneAndDelete(filter, Document.class);
        Assertions.assertNull(r);
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

    @Test
    public void testFindOneAndDeleteWithRowType() {
        Document filter = new Document("id", 1);
        Document returnedDoc = new Document("id", 1);
        when(mockCollection.findOneAndDelete(any(Bson.class))).thenReturn(returnedDoc);

        Document result = executor.findOneAndDelete(filter, Document.class);
        Assertions.assertNotNull(result);
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
    public void testFindOneAndReplaceWithNullReplacementThrowsIAE() {
        Document filter = new Document("id", 1);
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.findOneAndReplace(filter, (Object) null));
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
    public void testFindOneAndReplaceWithRowType() {
        Document filter = new Document("id", 1);
        Document replacement = new Document("name", "x");
        Document returnedDoc = new Document("name", "x");
        when(mockCollection.findOneAndReplace(any(Bson.class), any(Document.class))).thenReturn(returnedDoc);

        Document result = executor.findOneAndReplace(filter, replacement, Document.class);
        Assertions.assertNotNull(result);
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
    public void testFindOneAndUpdateWithCollectionAndRowTypeReturnsNullWhenNoDoc() {
        Document filter = new Document("id", 1);
        List<Document> updates = Arrays.asList(new Document("$set", new Document("a", 1)));
        when(mockCollection.findOneAndUpdate(any(Bson.class), anyList())).thenReturn(null);
        Document result = executor.findOneAndUpdate(filter, updates, Document.class);
        Assertions.assertNull(result);
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
    public void testFindOneAndUpdateWithCollectionOptionsAndRowType_Specific() {
        Document filter = new Document("id", 1);
        List<Document> updates = Arrays.asList(new Document("$set", new Document("a", 1)));
        com.mongodb.client.model.FindOneAndUpdateOptions opts = new com.mongodb.client.model.FindOneAndUpdateOptions();
        Document returnedDoc = new Document("a", 1);
        when(mockCollection.findOneAndUpdate(any(Bson.class), anyList(), any(com.mongodb.client.model.FindOneAndUpdateOptions.class))).thenReturn(returnedDoc);
        Document result = executor.findOneAndUpdate(filter, updates, opts, Document.class);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testFindOneAndUpdateWithNullUpdateThrowsIAE() {
        Document filter = new Document("id", 1);
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.findOneAndUpdate(filter, (Object) null));
    }

    @Test
    public void testFindOneAndUpdateWithObjListRejectsEmptyUpdates() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.findOneAndUpdate(new Document("id", 1), List.of()));
    }

    @Test
    public void testFindOneAndUpdateWithObjListRejectsNullFilter() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.findOneAndUpdate(null, List.of(new Document("$set", new Document("a", 1)))));
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
    public void testFindOneAndUpdateWithRowType() {
        Document filter = new Document("id", 1);
        Document update = new Document("$set", new Document("a", 1));
        Document returnedDoc = new Document("a", 1);
        when(mockCollection.findOneAndUpdate(any(Bson.class), any(Bson.class))).thenReturn(returnedDoc);

        Document result = executor.findOneAndUpdate(filter, update, Document.class);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testInsertMany() {
        List<Document> docs = Arrays.asList(new Document("id", 1), new Document("id", 2));
        InsertManyResult insertResult = mock(InsertManyResult.class);
        when(mockCollection.insertMany(anyList())).thenReturn(insertResult);

        InsertManyResult result = executor.insertMany(docs);
        verify(mockCollection).insertMany(docs);
        Assertions.assertSame(insertResult, result);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testInsertManyPreservesDocumentInstancesInHeterogeneousBatch() {
        // The driver populates generated _id values into the supplied Document. A mixed batch used
        // to copy that Document while converting the non-Document rows, so the caller never saw it.
        Document callerDocument = new Document("name", "document-row");
        List<Object> rows = Arrays.asList(callerDocument, java.util.Map.of("name", "map-row"));
        InsertManyResult insertResult = mock(InsertManyResult.class);
        org.mockito.ArgumentCaptor<List> captor = org.mockito.ArgumentCaptor.forClass(List.class);
        when(mockCollection.insertMany(anyList())).thenReturn(insertResult);

        executor.insertMany(rows);

        verify(mockCollection).insertMany(captor.capture());
        Assertions.assertSame(callerDocument, captor.getValue().get(0));
        Assertions.assertTrue(captor.getValue().get(1) instanceof Document);
    }

    @Test
    public void testInsertManyWithEntities() {
        TestEntity e1 = new TestEntity();
        e1.setName("a");
        TestEntity e2 = new TestEntity();
        e2.setName("b");
        List<TestEntity> entities = Arrays.asList(e1, e2);
        executor.insertMany(entities);
        verify(mockCollection).insertMany(anyList());
    }

    @Test
    public void testInsertManyWithOptions() {
        List<Document> docs = Arrays.asList(new Document("id", 1), new Document("id", 2));
        InsertManyOptions options = new InsertManyOptions();
        InsertManyResult insertResult = mock(InsertManyResult.class);
        when(mockCollection.insertMany(anyList(), eq(options))).thenReturn(insertResult);

        InsertManyResult result = executor.insertMany(docs, options);
        verify(mockCollection).insertMany(docs, options);
        Assertions.assertSame(insertResult, result);
    }

    @Test
    public void testInsertManyWithSetOfDocuments() {
        java.util.Set<Document> docs = new java.util.LinkedHashSet<>();
        docs.add(new Document("a", 1));
        docs.add(new Document("a", 2));
        executor.insertMany(docs);
        verify(mockCollection).insertMany(anyList());
    }

    @Test
    public void testInsertNullElementIsIllegalArgument() {
        // A null element is rejected like the update family (updateOne/updateMany(filter, objList)):
        // IllegalArgumentException, before anything is sent to the driver.
        final List<Document> docs = Arrays.asList(new Document("id", 1), null);

        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.insertMany(docs));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.insertMany(docs, new InsertManyOptions()));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.bulkInsert(docs));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.bulkInsert(docs, null));
        verify(mockCollection, never()).insertMany(anyList());
        verify(mockCollection, never()).bulkWrite(anyList());
    }

    @Test
    public void testInsertOne() {
        Document doc = new Document("name", "test");
        InsertOneResult insertResult = mock(InsertOneResult.class);
        when(mockCollection.insertOne(doc)).thenReturn(insertResult);

        InsertOneResult result = executor.insertOne(doc);
        verify(mockCollection).insertOne(doc);
        Assertions.assertSame(insertResult, result);
    }

    @Test
    public void testInsertOneWithOptions() {
        Document doc = new Document("name", "test");
        InsertOneOptions options = new InsertOneOptions();
        InsertOneResult insertResult = mock(InsertOneResult.class);
        when(mockCollection.insertOne(doc, options)).thenReturn(insertResult);

        InsertOneResult result = executor.insertOne(doc, options);
        verify(mockCollection).insertOne(doc, options);
        Assertions.assertSame(insertResult, result);
    }

    @Test
    public void testReplaceOne() {
        String objectId = "507f1f77bcf86cd799439011";
        Document replacement = new Document("name", "replaced");
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockCollection.replaceOne(any(), any())).thenReturn(updateResult);

        UpdateResult result = executor.replaceOne(objectId, replacement);
        Assertions.assertSame(updateResult, result);
    }

    @Test
    public void testReplaceOneWithFilterAndDocumentAndOptions() {
        Document filter = new Document("id", 1);
        Document repl = new Document("name", "x");
        com.mongodb.client.model.ReplaceOptions opts = new com.mongodb.client.model.ReplaceOptions();
        UpdateResult ur = mock(UpdateResult.class);
        when(mockCollection.replaceOne(any(Bson.class), any(Document.class), any(com.mongodb.client.model.ReplaceOptions.class))).thenReturn(ur);
        UpdateResult result = executor.replaceOne(filter, repl, opts);
        Assertions.assertSame(ur, result);
        verify(mockCollection).replaceOne(any(Bson.class), any(Document.class), eq(opts));
    }

    @Test
    public void testReplaceOneWithNullReplacementThrowsIAE() {
        Document filter = new Document("id", 1);
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.replaceOne(filter, (Object) null));
    }

    @Test
    public void testUpdateMany() {
        Document filter = new Document("status", "active");
        Document update = new Document("status", "inactive");
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockCollection.updateMany(any(Bson.class), any(Bson.class))).thenReturn(updateResult);

        UpdateResult result = executor.updateMany(filter, update);
        Assertions.assertSame(updateResult, result);
    }

    @Test
    public void testUpdateManyWithCollection() {
        Document filter = new Document("id", 1);
        List<Document> updates = Arrays.asList(new Document("$set", new Document("a", 1)));
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockCollection.updateMany(any(Bson.class), anyList())).thenReturn(updateResult);

        UpdateResult result = executor.updateMany(filter, updates);
        Assertions.assertSame(updateResult, result);
    }

    @Test
    public void testUpdateManyWithCollectionAndOptions() {
        Document filter = new Document("id", 1);
        List<Document> updates = Arrays.asList(new Document("$set", new Document("a", 1)));
        com.mongodb.client.model.UpdateOptions options = new com.mongodb.client.model.UpdateOptions();
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockCollection.updateMany(any(Bson.class), anyList(), any(com.mongodb.client.model.UpdateOptions.class))).thenReturn(updateResult);

        UpdateResult result = executor.updateMany(filter, updates, options);
        Assertions.assertSame(updateResult, result);
    }

    @Test
    public void testUpdateManyWithEntityOnlyIdPayloadThrowsIAE() {
        // An entity whose only non-null property is the id converts to {_id: ...}; once the immutable
        // _id is stripped, the payload is empty and must be rejected before reaching the server.
        Document filter = new Document("status", "active");
        TestEntity entity = new TestEntity();
        entity.setId("507f1f77bcf86cd799439011");

        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class, () -> executor.updateMany(filter, entity));

        Assertions.assertTrue(ex.getMessage().contains("empty"));
        verify(mockCollection, never()).updateMany(any(Bson.class), any(Bson.class));
    }

    @Test
    public void testUpdateManyWithEntityPayloadStripsIdFromSet() {
        // Regression: the immutable _id is stripped from the converted update document. Previously the
        // generated {$set: {_id: ..., ...}} failed server-side for every matched document whose _id differs.
        Document filter = new Document("status", "active");
        TestEntity entity = new TestEntity();
        entity.setId("507f1f77bcf86cd799439011");
        entity.setName("alice");
        UpdateResult updateResult = mock(UpdateResult.class);
        org.mockito.ArgumentCaptor<Bson> updateCaptor = org.mockito.ArgumentCaptor.forClass(Bson.class);
        when(mockCollection.updateMany(any(Bson.class), any(Bson.class))).thenReturn(updateResult);

        executor.updateMany(filter, entity);

        verify(mockCollection).updateMany(any(Bson.class), updateCaptor.capture());
        Document captured = (Document) updateCaptor.getValue();
        Assertions.assertTrue(captured.containsKey("$set"));
        Document setDoc = (Document) captured.get("$set");
        Assertions.assertFalse(setDoc.containsKey("_id"));
        Assertions.assertEquals("alice", setDoc.get("name"));
    }

    @Test
    public void testUpdateManyWithFilterAndDocumentAndOptions() {
        Document filter = new Document("id", 1);
        Document update = new Document("$set", new Document("a", 1));
        com.mongodb.client.model.UpdateOptions opts = new com.mongodb.client.model.UpdateOptions();
        UpdateResult ur = mock(UpdateResult.class);
        when(mockCollection.updateMany(any(Bson.class), any(Bson.class), any(com.mongodb.client.model.UpdateOptions.class))).thenReturn(ur);
        UpdateResult result = executor.updateMany(filter, update, opts);
        Assertions.assertSame(ur, result);
        verify(mockCollection).updateMany(any(Bson.class), any(Bson.class), eq(opts));
    }

    @Test
    public void testUpdateManyWithFilterAndEntityAndOptions() {
        Document filter = new Document();
        TestEntity e = new TestEntity();
        e.setName("x");
        com.mongodb.client.model.UpdateOptions opts = new com.mongodb.client.model.UpdateOptions();
        UpdateResult ur = mock(UpdateResult.class);
        when(mockCollection.updateMany(any(Bson.class), any(Bson.class), any(com.mongodb.client.model.UpdateOptions.class))).thenReturn(ur);
        UpdateResult r = executor.updateMany(filter, e, opts);
        Assertions.assertSame(ur, r);
    }

    @Test
    public void testUpdateManyWithNullUpdateThrowsIAE() {
        Document filter = new Document("id", 1);
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.updateMany(filter, (Object) null));
    }

    @Test
    public void testUpdateManyWithSetOfBson() {
        Document filter = new Document("id", 1);
        java.util.Set<Document> updates = new java.util.LinkedHashSet<>();
        updates.add(new Document("$set", new Document("a", 1)));
        UpdateResult ur = mock(UpdateResult.class);
        when(mockCollection.updateMany(any(Bson.class), anyList())).thenReturn(ur);
        UpdateResult result = executor.updateMany(filter, updates);
        Assertions.assertSame(ur, result);
    }

    @Test
    public void testUpdateOneDoesNotMutateCallerDocument() {
        // Regression: toBson stripped _id from the CALLER's Document in place on the
        // non-$-operator path; the $set payload must be built from a copy.
        Document filter = new Document("name", "test");
        ObjectId id = new ObjectId();
        Document callerUpdate = new Document("_id", id).append("name", "updated");
        UpdateResult updateResult = mock(UpdateResult.class);
        org.mockito.ArgumentCaptor<Bson> updateCaptor = org.mockito.ArgumentCaptor.forClass(Bson.class);
        when(mockCollection.updateOne(any(Bson.class), any(Bson.class))).thenReturn(updateResult);

        executor.updateOne(filter, callerUpdate);

        // The caller's Document must be untouched.
        Assertions.assertEquals(id, callerUpdate.get("_id"));
        Assertions.assertEquals(2, callerUpdate.size());
        // The sent update is {$set: {...}} without _id.
        verify(mockCollection).updateOne(any(Bson.class), updateCaptor.capture());
        Document sent = (Document) updateCaptor.getValue();
        Document setPayload = (Document) sent.get("$set");
        Assertions.assertFalse(setPayload.containsKey("_id"));
        Assertions.assertEquals("updated", setPayload.get("name"));
    }

    @Test
    public void testUpdateOneNormalizesPlainBsonDocumentInPipeline() {
        Document filter = new Document("id", 1);
        Document plainUpdate = new Document("status", "active");
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockCollection.updateOne(eq(filter), anyList())).thenReturn(updateResult);

        Assertions.assertSame(updateResult, executor.updateOne(filter, Arrays.asList(plainUpdate)));

        @SuppressWarnings("rawtypes")
        org.mockito.ArgumentCaptor<List> updatesCaptor = org.mockito.ArgumentCaptor.forClass(List.class);
        verify(mockCollection).updateOne(eq(filter), updatesCaptor.capture());
        Assertions.assertEquals(Arrays.asList(new Document("$set", new Document("status", "active"))), updatesCaptor.getValue());
        Assertions.assertEquals(new Document("status", "active"), plainUpdate, "The caller's update document must remain unchanged");
    }

    @Test
    public void testUpdateOneWithCollection() {
        Document filter = new Document("id", 1);
        List<Document> updates = Arrays.asList(new Document("$set", new Document("a", 1)));
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockCollection.updateOne(any(Bson.class), anyList())).thenReturn(updateResult);

        UpdateResult result = executor.updateOne(filter, updates);
        Assertions.assertSame(updateResult, result);
    }

    @Test
    public void testUpdateOneWithCollectionAndOptions() {
        Document filter = new Document("id", 1);
        List<Document> updates = Arrays.asList(new Document("$set", new Document("a", 1)));
        com.mongodb.client.model.UpdateOptions options = new com.mongodb.client.model.UpdateOptions();
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockCollection.updateOne(any(Bson.class), anyList(), any(com.mongodb.client.model.UpdateOptions.class))).thenReturn(updateResult);

        UpdateResult result = executor.updateOne(filter, updates, options);
        Assertions.assertSame(updateResult, result);
    }

    @Test
    public void testUpdateOneWithCollectionOfEntities() {
        Document filter = new Document("id", 1);
        TestEntity e1 = new TestEntity();
        e1.setName("a");
        TestEntity e2 = new TestEntity();
        e2.setName("b");
        List<TestEntity> updates = Arrays.asList(e1, e2);
        UpdateResult ur = mock(UpdateResult.class);
        when(mockCollection.updateOne(any(Bson.class), anyList())).thenReturn(ur);
        UpdateResult result = executor.updateOne(filter, updates);
        Assertions.assertSame(ur, result);
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
    public void testUpdateOneWithEmptyMapPayloadThrowsIAE() {
        // This executor rejects an empty plain-object update before calling the driver, even though
        // MongoDB 5.0+ accepts an empty $set operand as a no-op.
        Document filter = new Document("status", "active");

        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class, () -> executor.updateOne(filter, new java.util.HashMap<>()));

        Assertions.assertTrue(ex.getMessage().contains("empty"));
        verify(mockCollection, never()).updateOne(any(Bson.class), any(Bson.class));
    }

    @Test
    public void testUpdateOneWithFilterAndDocumentAndOptions() {
        Document filter = new Document("id", 1);
        Document update = new Document("$set", new Document("a", 1));
        com.mongodb.client.model.UpdateOptions opts = new com.mongodb.client.model.UpdateOptions();
        UpdateResult ur = mock(UpdateResult.class);
        when(mockCollection.updateOne(any(Bson.class), any(Bson.class), any(com.mongodb.client.model.UpdateOptions.class))).thenReturn(ur);
        UpdateResult result = executor.updateOne(filter, update, opts);
        Assertions.assertSame(ur, result);
        verify(mockCollection).updateOne(any(Bson.class), any(Bson.class), eq(opts));
    }

    @Test
    public void testUpdateOneWithFilterAndEntityAndOptions() {
        Document filter = new Document();
        TestEntity e = new TestEntity();
        e.setName("x");
        com.mongodb.client.model.UpdateOptions opts = new com.mongodb.client.model.UpdateOptions();
        UpdateResult ur = mock(UpdateResult.class);
        when(mockCollection.updateOne(any(Bson.class), any(Bson.class), any(com.mongodb.client.model.UpdateOptions.class))).thenReturn(ur);
        UpdateResult r = executor.updateOne(filter, e, opts);
        Assertions.assertSame(ur, r);
    }

    @Test
    public void testUpdateOneWithMapPayloadStripsIdFromSet() {
        Document filter = new Document("status", "active");
        java.util.Map<String, Object> update = new java.util.HashMap<>();
        update.put("_id", new ObjectId());
        update.put("name", "bob");
        UpdateResult updateResult = mock(UpdateResult.class);
        org.mockito.ArgumentCaptor<Bson> updateCaptor = org.mockito.ArgumentCaptor.forClass(Bson.class);
        when(mockCollection.updateOne(any(Bson.class), any(Bson.class))).thenReturn(updateResult);

        executor.updateOne(filter, update);

        verify(mockCollection).updateOne(any(Bson.class), updateCaptor.capture());
        Document captured = (Document) updateCaptor.getValue();
        Assertions.assertTrue(captured.containsKey("$set"));
        Document setDoc = (Document) captured.get("$set");
        Assertions.assertFalse(setDoc.containsKey("_id"));
        Assertions.assertEquals("bob", setDoc.get("name"));
    }

    @Test
    public void testUpdateOneWithNullUpdateThrowsIAE() {
        Document filter = new Document("id", 1);
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.updateOne(filter, (Object) null));
    }

    @Test
    public void testUpdateOneWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        Document update = new Document("name", "updated");
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockCollection.updateOne(any(Bson.class), any(Bson.class))).thenReturn(updateResult);

        UpdateResult result = executor.updateOne(objectId, update);
        Assertions.assertSame(updateResult, result);
    }

    @Test
    public void testUpdateOneWrapsEntityUpdateInSet() {
        Document filter = new Document("id", 1);
        TestEntity entity = new TestEntity();
        entity.setName("x");
        UpdateResult updateResult = mock(UpdateResult.class);
        org.mockito.ArgumentCaptor<Bson> updateCaptor = org.mockito.ArgumentCaptor.forClass(Bson.class);
        when(mockCollection.updateOne(any(Bson.class), any(Bson.class))).thenReturn(updateResult);
        executor.updateOne(filter, entity);
        verify(mockCollection).updateOne(any(Bson.class), updateCaptor.capture());
        Assertions.assertTrue(updateCaptor.getValue() instanceof Document);
        Assertions.assertTrue(((Document) updateCaptor.getValue()).containsKey("$set"));
    }

    @Test
    public void testUpdateRejectsMixedOperatorAndOrdinaryFieldsBeforeDriverCall() {
        Document filter = new Document("active", true);
        Document invalidUpdate = new Document("$set", new Document("name", "alice")).append("status", "active");
        com.mongodb.BasicDBObject reversedInvalidUpdate = new com.mongodb.BasicDBObject("status", "active").append("$set",
                new com.mongodb.BasicDBObject("name", "alice"));

        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class, () -> executor.updateOne(filter, invalidUpdate));

        Assertions.assertTrue(ex.getMessage().contains("cannot mix"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.updateOne(filter, reversedInvalidUpdate));
        verify(mockCollection, never()).updateOne(any(Bson.class), any(Bson.class));
    }
}
