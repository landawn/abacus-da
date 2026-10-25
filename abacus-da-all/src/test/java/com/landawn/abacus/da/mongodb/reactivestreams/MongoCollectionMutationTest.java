package com.landawn.abacus.da.mongodb.reactivestreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import com.mongodb.BasicDBObject;
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

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class MongoCollectionMutationTest extends MongoCollectionExecutorTestSupport {

    @Test
    public void testBulkInsert() {
        List<Document> entities = Arrays.asList(new Document("name", "doc1"), new Document("name", "doc2"));
        BulkWriteResult bulkResult = mock(BulkWriteResult.class);
        Publisher<BulkWriteResult> publisher = Mono.just(bulkResult);
        when(mockCollection.bulkWrite(anyList())).thenReturn(publisher);

        Mono<BulkWriteResult> result = executor.bulkInsert(entities);

        StepVerifier.create(result).expectNext(bulkResult).verifyComplete();
    }

    @Test
    public void testBulkInsertRejectsEmptyEntities() {
        assertThrows(IllegalArgumentException.class, () -> executor.bulkInsert(List.of()));
    }

    @Test
    public void testBulkInsertRejectsNullEntities() {
        assertThrows(IllegalArgumentException.class, () -> executor.bulkInsert(null));
    }

    @Test
    public void testBulkInsertWithNonDocumentEntities() {
        List<Map<String, Object>> entities = new ArrayList<>();
        Map<String, Object> entity1 = new HashMap<>();
        entity1.put("name", "entity1");
        Map<String, Object> entity2 = new HashMap<>();
        entity2.put("name", "entity2");
        entities.add(entity1);
        entities.add(entity2);

        BulkWriteResult bulkResult = mock(BulkWriteResult.class);
        Publisher<BulkWriteResult> publisher = Mono.just(bulkResult);
        when(mockCollection.bulkWrite(anyList())).thenReturn(publisher);

        Mono<BulkWriteResult> result = executor.bulkInsert(entities);

        StepVerifier.create(result).expectNext(bulkResult).verifyComplete();
    }

    @Test
    public void testBulkInsertWithOptions() {
        List<Document> entities = Arrays.asList(new Document("name", "doc1"), new Document("name", "doc2"));
        BulkWriteOptions options = new BulkWriteOptions();
        BulkWriteResult bulkResult = mock(BulkWriteResult.class);
        Publisher<BulkWriteResult> publisher = Mono.just(bulkResult);
        when(mockCollection.bulkWrite(anyList(), eq(options))).thenReturn(publisher);

        Mono<BulkWriteResult> result = executor.bulkInsert(entities, options);

        StepVerifier.create(result).expectNext(bulkResult).verifyComplete();
    }

    @Test
    public void testBulkWrite() {
        List<WriteModel<Document>> requests = Arrays.asList(new InsertOneModel<>(new Document("name", "doc1")));
        BulkWriteResult bulkResult = mock(BulkWriteResult.class);
        Publisher<BulkWriteResult> publisher = Mono.just(bulkResult);
        when(mockCollection.bulkWrite(requests)).thenReturn(publisher);

        Mono<BulkWriteResult> result = executor.bulkWrite(requests);

        StepVerifier.create(result).expectNext(bulkResult).verifyComplete();
    }

    @Test
    public void testBulkWriteRejectsEmptyRequests() {
        assertThrows(IllegalArgumentException.class, () -> executor.bulkWrite(List.of()));
    }

    @Test
    public void testBulkWriteRejectsNullRequests() {
        assertThrows(IllegalArgumentException.class, () -> executor.bulkWrite(null));
    }

    @Test
    public void testBulkWriteWithOptions() {
        List<WriteModel<Document>> requests = Arrays.asList(new InsertOneModel<>(new Document("name", "doc1")));
        BulkWriteOptions options = new BulkWriteOptions();
        BulkWriteResult bulkResult = mock(BulkWriteResult.class);
        Publisher<BulkWriteResult> publisher = Mono.just(bulkResult);
        when(mockCollection.bulkWrite(requests, options)).thenReturn(publisher);

        Mono<BulkWriteResult> result = executor.bulkWrite(requests, options);

        StepVerifier.create(result).expectNext(bulkResult).verifyComplete();
    }

    @Test
    public void testDeleteMany() {
        Bson filter = new Document("status", "deleted");
        DeleteResult deleteResult = mock(DeleteResult.class);
        Publisher<DeleteResult> publisher = Mono.just(deleteResult);
        when(mockCollection.deleteMany(filter)).thenReturn(publisher);

        Mono<DeleteResult> result = executor.deleteMany(filter);

        StepVerifier.create(result).expectNext(deleteResult).verifyComplete();
    }

    @Test
    public void testDeleteManyWithNullOptionsUsesDefaultOverload() {
        Bson filter = new Document("status", "deleted");
        DeleteResult deleteResult = mock(DeleteResult.class);
        Publisher<DeleteResult> publisher = Mono.just(deleteResult);
        when(mockCollection.deleteMany(filter)).thenReturn(publisher);

        Mono<DeleteResult> result = executor.deleteMany(filter, null);

        StepVerifier.create(result).expectNext(deleteResult).verifyComplete();
        verify(mockCollection).deleteMany(filter);
    }

    @Test
    public void testDeleteManyWithOptions() {
        Bson filter = new Document("status", "deleted");
        DeleteOptions options = new DeleteOptions();
        DeleteResult deleteResult = mock(DeleteResult.class);
        Publisher<DeleteResult> publisher = Mono.just(deleteResult);
        when(mockCollection.deleteMany(filter, options)).thenReturn(publisher);

        Mono<DeleteResult> result = executor.deleteMany(filter, options);

        StepVerifier.create(result).expectNext(deleteResult).verifyComplete();
    }

    @Test
    public void testDeleteOneWithFilter() {
        Bson filter = new Document("name", "test");
        DeleteResult deleteResult = mock(DeleteResult.class);
        Publisher<DeleteResult> publisher = Mono.just(deleteResult);
        when(mockCollection.deleteOne(filter)).thenReturn(publisher);

        Mono<DeleteResult> result = executor.deleteOne(filter);

        StepVerifier.create(result).expectNext(deleteResult).verifyComplete();
    }

    @Test
    public void testDeleteOneWithFilterAndOptions() {
        Bson filter = new Document("name", "test");
        DeleteOptions options = new DeleteOptions();
        DeleteResult deleteResult = mock(DeleteResult.class);
        Publisher<DeleteResult> publisher = Mono.just(deleteResult);
        when(mockCollection.deleteOne(filter, options)).thenReturn(publisher);

        Mono<DeleteResult> result = executor.deleteOne(filter, options);

        StepVerifier.create(result).expectNext(deleteResult).verifyComplete();
    }

    @Test
    public void testDeleteOneWithNullOptionsUsesDefaultOverload() {
        Bson filter = new Document("name", "test");
        DeleteResult deleteResult = mock(DeleteResult.class);
        Publisher<DeleteResult> publisher = Mono.just(deleteResult);
        when(mockCollection.deleteOne(filter)).thenReturn(publisher);

        Mono<DeleteResult> result = executor.deleteOne(filter, null);

        StepVerifier.create(result).expectNext(deleteResult).verifyComplete();
        verify(mockCollection).deleteOne(filter);
    }

    @Test
    public void testDeleteOneWithObjectId() {
        ObjectId objectId = new ObjectId();
        DeleteResult deleteResult = mock(DeleteResult.class);
        Publisher<DeleteResult> publisher = Mono.just(deleteResult);
        when(mockCollection.deleteOne(any(Bson.class))).thenReturn(publisher);

        Mono<DeleteResult> result = executor.deleteOne(objectId);

        StepVerifier.create(result).expectNext(deleteResult).verifyComplete();
    }

    @Test
    public void testDeleteOneWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        DeleteResult deleteResult = mock(DeleteResult.class);
        Publisher<DeleteResult> publisher = Mono.just(deleteResult);
        when(mockCollection.deleteOne(any(Bson.class))).thenReturn(publisher);

        Mono<DeleteResult> result = executor.deleteOne(objectId);

        StepVerifier.create(result).expectNext(deleteResult).verifyComplete();
    }

    @Test
    public void testFindOneAndDelete() {
        Bson filter = new Document("name", "test");
        Document deletedDoc = new Document("name", "test").append("_id", new ObjectId());
        Publisher<Document> publisher = Mono.just(deletedDoc);
        when(mockCollection.findOneAndDelete(filter)).thenReturn(publisher);

        Mono<Document> result = executor.findOneAndDelete(filter);

        StepVerifier.create(result).expectNext(deletedDoc).verifyComplete();
    }

    @Test
    public void testFindOneAndDeleteWithNullFilterThrowsIAE() {
        // Regression: a null filter must be rejected eagerly with IllegalArgumentException at the call site (before a
        // Mono is built), matching every sibling deleteOne/findOneAndUpdate/findOneAndReplace. Before the fix the null
        // filter reached the driver, which silently deletes an arbitrary document.
        final com.mongodb.client.model.FindOneAndDeleteOptions options = new com.mongodb.client.model.FindOneAndDeleteOptions();

        assertThrows(IllegalArgumentException.class, () -> executor.findOneAndDelete((Bson) null));
        assertThrows(IllegalArgumentException.class, () -> executor.findOneAndDelete((Bson) null, Integer.class));
        assertThrows(IllegalArgumentException.class, () -> executor.findOneAndDelete((Bson) null, options));
        assertThrows(IllegalArgumentException.class, () -> executor.findOneAndDelete((Bson) null, options, Document.class));
    }

    @Test
    public void testFindOneAndDeleteWithOptions() {
        Bson filter = new Document("name", "test");
        FindOneAndDeleteOptions options = new FindOneAndDeleteOptions();
        Document deletedDoc = new Document("name", "test");
        Publisher<Document> publisher = Mono.just(deletedDoc);
        when(mockCollection.findOneAndDelete(filter, options)).thenReturn(publisher);

        Mono<Document> result = executor.findOneAndDelete(filter, options);

        StepVerifier.create(result).expectNext(deletedDoc).verifyComplete();
    }

    @Test
    public void testFindOneAndDeleteWithOptionsAndRowType() {
        Bson filter = new Document("name", "test");
        FindOneAndDeleteOptions options = new FindOneAndDeleteOptions();
        Document deletedDoc = new Document("name", "test");
        Publisher<Document> publisher = Mono.just(deletedDoc);
        when(mockCollection.findOneAndDelete(filter, options)).thenReturn(publisher);

        Mono<Document> result = executor.findOneAndDelete(filter, options, Document.class);

        StepVerifier.create(result).expectNext(deletedDoc).verifyComplete();
    }

    @Test
    public void testFindOneAndDeleteWithRowType() {
        Bson filter = new Document("value", 123);
        Document deletedDoc = new Document("value", 123);
        Publisher<Document> publisher = Mono.just(deletedDoc);
        when(mockCollection.findOneAndDelete(filter)).thenReturn(publisher);

        Mono<Integer> result = executor.findOneAndDelete(filter, Integer.class);

        StepVerifier.create(result).expectNext(123).verifyComplete();
    }

    @Test
    public void testFindOneAndReplace() {
        Bson filter = new Document("name", "oldName");
        Document replacement = new Document("name", "newName");
        Publisher<Document> publisher = Mono.just(replacement);
        when(mockCollection.findOneAndReplace(filter, replacement)).thenReturn(publisher);

        Mono<Document> result = executor.findOneAndReplace(filter, replacement);

        StepVerifier.create(result).expectNext(replacement).verifyComplete();
    }

    @Test
    public void testFindOneAndReplaceWithNullReplacementThrowsIAE() {
        Document filter = new Document("id", 1);
        assertThrows(IllegalArgumentException.class, () -> executor.findOneAndReplace(filter, (Object) null));
    }

    @Test
    public void testFindOneAndReplaceWithOptions() {
        Bson filter = new Document("name", "oldName");
        Document replacement = new Document("name", "newName");
        FindOneAndReplaceOptions options = new FindOneAndReplaceOptions();
        Publisher<Document> publisher = Mono.just(replacement);
        when(mockCollection.findOneAndReplace(filter, replacement, options)).thenReturn(publisher);

        Mono<Document> result = executor.findOneAndReplace(filter, replacement, options);

        StepVerifier.create(result).expectNext(replacement).verifyComplete();
    }

    @Test
    public void testFindOneAndReplaceWithOptionsAndRowType() {
        Bson filter = new Document("name", "oldName");
        Document replacement = new Document("name", "newName");
        FindOneAndReplaceOptions options = new FindOneAndReplaceOptions();
        Publisher<Document> publisher = Mono.just(replacement);
        when(mockCollection.findOneAndReplace(filter, replacement, options)).thenReturn(publisher);

        Mono<Document> result = executor.findOneAndReplace(filter, replacement, options, Document.class);

        StepVerifier.create(result).expectNext(replacement).verifyComplete();
    }

    @Test
    public void testFindOneAndReplaceWithRowType() {
        Bson filter = new Document("name", "oldName");
        Document replacement = new Document("name", "newName");
        Publisher<Document> publisher = Mono.just(replacement);
        when(mockCollection.findOneAndReplace(filter, replacement)).thenReturn(publisher);

        Mono<Document> result = executor.findOneAndReplace(filter, replacement, Document.class);

        StepVerifier.create(result).expectNext(replacement).verifyComplete();
    }

    @Test
    public void testFindOneAndUpdateWithAllParameters() {
        Bson filter = new Document("name", "test");
        Document update = new Document("status", "updated");
        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
        Document resultDoc = new Document("name", "test").append("status", "updated");
        Publisher<Document> publisher = Mono.just(resultDoc);
        when(mockCollection.findOneAndUpdate(eq(filter), any(Bson.class), eq(options))).thenReturn(publisher);

        Mono<Document> result = executor.findOneAndUpdate(filter, update, options, Document.class);

        StepVerifier.create(result).expectNext(resultDoc).verifyComplete();
    }

    @Test
    public void testFindOneAndUpdateWithFilter() {
        Bson filter = new Document("name", "test");
        Document update = new Document("status", "updated");
        Document resultDoc = new Document("name", "test").append("status", "updated");
        Publisher<Document> publisher = Mono.just(resultDoc);
        when(mockCollection.findOneAndUpdate(eq(filter), any(Bson.class))).thenReturn(publisher);

        Mono<Document> result = executor.findOneAndUpdate(filter, update);

        StepVerifier.create(result).expectNext(resultDoc).verifyComplete();
    }

    @Test
    public void testFindOneAndUpdateWithFilterAndList() {
        Bson filter = new Document("name", "test");
        List<Bson> updates = Arrays.asList(new Document("$set", new Document("status", "active")));
        Document resultDoc = new Document("name", "test").append("status", "active");
        Publisher<Document> publisher = Mono.just(resultDoc);
        when(mockCollection.findOneAndUpdate(eq(filter), eq(updates))).thenReturn(publisher);

        Mono<Document> result = executor.findOneAndUpdate(filter, updates);

        StepVerifier.create(result).expectNext(resultDoc).verifyComplete();
    }

    @Test
    public void testFindOneAndUpdateWithFilterAndListRejectsEmptyUpdates() {
        Bson filter = new Document("name", "test");

        assertThrows(IllegalArgumentException.class, () -> executor.findOneAndUpdate(filter, List.of()));
    }

    @Test
    public void testFindOneAndUpdateWithFilterAndListRejectsNullFilter() {
        List<Bson> updates = Arrays.asList(new Document("$set", new Document("status", "active")));

        assertThrows(IllegalArgumentException.class, () -> executor.findOneAndUpdate(null, updates));
    }

    @Test
    public void testFindOneAndUpdateWithFilterAndRowType() {
        Bson filter = new Document("name", "test");
        Document update = new Document("value", 123);
        Document resultDoc = new Document("value", 123);
        Publisher<Document> publisher = Mono.just(resultDoc);
        when(mockCollection.findOneAndUpdate(eq(filter), any(Bson.class))).thenReturn(publisher);

        Mono<Integer> result = executor.findOneAndUpdate(filter, update, Integer.class);

        StepVerifier.create(result).expectNext(123).verifyComplete();
    }

    @Test
    public void testFindOneAndUpdateWithFilterListAndOptions() {
        Bson filter = new Document("name", "test");
        List<Bson> updates = Arrays.asList(new Document("$set", new Document("status", "active")));
        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
        Document resultDoc = new Document("name", "test").append("status", "active");
        Publisher<Document> publisher = Mono.just(resultDoc);
        when(mockCollection.findOneAndUpdate(eq(filter), eq(updates), eq(options))).thenReturn(publisher);

        Mono<Document> result = executor.findOneAndUpdate(filter, updates, options);

        StepVerifier.create(result).expectNext(resultDoc).verifyComplete();
    }

    @Test
    public void testFindOneAndUpdateWithFilterListAndRowType() {
        Bson filter = new Document("name", "test");
        List<Bson> updates = Arrays.asList(new Document("$set", new Document("status", "active")));
        Document resultDoc = new Document("name", "test").append("status", "active");
        Publisher<Document> publisher = Mono.just(resultDoc);
        when(mockCollection.findOneAndUpdate(eq(filter), eq(updates))).thenReturn(publisher);

        Mono<Document> result = executor.findOneAndUpdate(filter, updates, Document.class);

        StepVerifier.create(result).expectNext(resultDoc).verifyComplete();
    }

    @Test
    public void testFindOneAndUpdateWithFilterListOptionsAndRowType() {
        Bson filter = new Document("name", "test");
        List<Bson> updates = Arrays.asList(new Document("$set", new Document("status", "active")));
        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
        Document resultDoc = new Document("name", "test").append("status", "active");
        Publisher<Document> publisher = Mono.just(resultDoc);
        when(mockCollection.findOneAndUpdate(eq(filter), eq(updates), eq(options))).thenReturn(publisher);

        Mono<Document> result = executor.findOneAndUpdate(filter, updates, options, Document.class);

        StepVerifier.create(result).expectNext(resultDoc).verifyComplete();
    }

    @Test
    public void testFindOneAndUpdateWithFilterUpdateAndOptions() {
        Bson filter = new Document("name", "test");
        Document update = new Document("status", "updated");
        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
        Document resultDoc = new Document("name", "test").append("status", "updated");
        Publisher<Document> publisher = Mono.just(resultDoc);
        when(mockCollection.findOneAndUpdate(eq(filter), any(Bson.class), eq(options))).thenReturn(publisher);

        Mono<Document> result = executor.findOneAndUpdate(filter, update, options);

        StepVerifier.create(result).expectNext(resultDoc).verifyComplete();
    }

    @Test
    public void testFindOneAndUpdateWithNonBsonCollection() {
        Bson filter = new Document("name", "test");
        List<Map<String, Object>> updates = new ArrayList<>();
        Map<String, Object> update = new HashMap<>();
        update.put("$set", new Document("status", "active"));
        updates.add(update);

        Document resultDoc = new Document("name", "test").append("status", "active");
        Publisher<Document> publisher = Mono.just(resultDoc);
        when(mockCollection.findOneAndUpdate(eq(filter), anyList())).thenReturn(publisher);

        Mono<Document> result = executor.findOneAndUpdate(filter, updates);

        StepVerifier.create(result).expectNext(resultDoc).verifyComplete();
    }

    @Test
    public void testFindOneAndUpdateWithNullUpdateThrowsIAE() {
        Document filter = new Document("id", 1);
        assertThrows(IllegalArgumentException.class, () -> executor.findOneAndUpdate(filter, (Object) null));
    }

    @Test
    public void testFindOneAndXxxTypedWithNullRowTypeThrowsIAE() {
        // Regression: the typed findOneAndUpdate/findOneAndReplace/findOneAndDelete overloads all
        // document "@throws IAE ... rowType is null" but had no guard — a null rowType silently
        // emitted Object[] rows (same defect class as aggregate(List, Class) above). The guard at
        // the four typed chokepoints now throws eagerly at the call site.
        final Bson filter = new Document("_id", "x");
        final Document update = new Document("$set", new Document("a", 1));

        assertThrows(IllegalArgumentException.class, () -> executor.findOneAndUpdate(filter, update, (FindOneAndUpdateOptions) null, null));
        assertThrows(IllegalArgumentException.class, () -> executor.findOneAndUpdate(filter, Arrays.asList(update), (FindOneAndUpdateOptions) null, null));
        assertThrows(IllegalArgumentException.class, () -> executor.findOneAndReplace(filter, new Document("a", 1), (FindOneAndReplaceOptions) null, null));
        assertThrows(IllegalArgumentException.class, () -> executor.findOneAndDelete(filter, (FindOneAndDeleteOptions) null, null));

        // The delegating no-options typed overloads inherit the guard.
        assertThrows(IllegalArgumentException.class, () -> executor.findOneAndDelete(filter, (Class<Document>) null));
    }

    @Test
    public void testInsertMany() {
        List<Document> docs = Arrays.asList(new Document("name", "doc1"), new Document("name", "doc2"));
        InsertManyResult insertResult = mock(InsertManyResult.class);
        Publisher<InsertManyResult> publisher = Mono.just(insertResult);
        when(mockCollection.insertMany(docs)).thenReturn(publisher);

        Mono<InsertManyResult> result = executor.insertMany(docs);

        StepVerifier.create(result).expectNext(insertResult).verifyComplete();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testInsertManyPreservesDocumentInstancesInHeterogeneousBatch() {
        Document callerDocument = new Document("name", "document-row");
        List<Object> rows = Arrays.asList(callerDocument, Map.of("name", "map-row"));
        InsertManyResult insertResult = mock(InsertManyResult.class);
        org.mockito.ArgumentCaptor<List> captor = org.mockito.ArgumentCaptor.forClass(List.class);
        when(mockCollection.insertMany(anyList())).thenReturn(Mono.just(insertResult));

        executor.insertMany(rows);

        verify(mockCollection).insertMany(captor.capture());
        assertSame(callerDocument, captor.getValue().get(0));
        assertTrue(captor.getValue().get(1) instanceof Document);
    }

    // Covers toDocument(Collection) all-Document path for a non-List collection.
    @Test
    public void testInsertManyWithAllDocumentsNonListCollection() {
        Collection<Document> docs = new java.util.LinkedHashSet<>(Arrays.asList(new Document("name", "a"), new Document("name", "b")));
        InsertManyResult insertResult = mock(InsertManyResult.class);
        Publisher<InsertManyResult> publisher = Mono.just(insertResult);
        when(mockCollection.insertMany(anyList())).thenReturn(publisher);

        Mono<InsertManyResult> result = executor.insertMany(docs);

        StepVerifier.create(result).expectNext(insertResult).verifyComplete();
    }

    @Test
    public void testInsertManyWithNonDocumentObjects() {
        List<Map<String, Object>> mapList = new ArrayList<>();
        Map<String, Object> map1 = new HashMap<>();
        map1.put("name", "test1");
        Map<String, Object> map2 = new HashMap<>();
        map2.put("name", "test2");
        mapList.add(map1);
        mapList.add(map2);

        InsertManyResult insertResult = mock(InsertManyResult.class);
        Publisher<InsertManyResult> publisher = Mono.just(insertResult);
        when(mockCollection.insertMany(anyList())).thenReturn(publisher);

        Mono<InsertManyResult> result = executor.insertMany(mapList);

        StepVerifier.create(result).expectNext(insertResult).verifyComplete();
    }

    @Test
    public void testInsertManyWithOptions() {
        List<Document> docs = Arrays.asList(new Document("name", "doc1"), new Document("name", "doc2"));
        InsertManyOptions options = new InsertManyOptions();
        InsertManyResult insertResult = mock(InsertManyResult.class);
        Publisher<InsertManyResult> publisher = Mono.just(insertResult);
        when(mockCollection.insertMany(docs, options)).thenReturn(publisher);

        Mono<InsertManyResult> result = executor.insertMany(docs, options);

        StepVerifier.create(result).expectNext(insertResult).verifyComplete();
    }

    @Test
    public void testInsertNullElementIsIllegalArgument() {
        // A null element is rejected eagerly (MongoDBBase.toDocument), before the driver is called.
        final List<Document> docs = Arrays.asList(new Document("id", 1), null);

        assertThrows(IllegalArgumentException.class, () -> executor.insertMany(docs));
        assertThrows(IllegalArgumentException.class, () -> executor.insertMany(docs, new InsertManyOptions()));
        assertThrows(IllegalArgumentException.class, () -> executor.bulkInsert(docs));
        assertThrows(IllegalArgumentException.class, () -> executor.bulkInsert(docs, new BulkWriteOptions()));
        verify(mockCollection, org.mockito.Mockito.never()).insertMany(anyList());
        verify(mockCollection, org.mockito.Mockito.never()).bulkWrite(anyList());
    }

    @Test
    public void testInsertOne() {
        Document doc = new Document("name", "test");
        InsertOneResult insertResult = mock(InsertOneResult.class);
        Publisher<InsertOneResult> publisher = Mono.just(insertResult);
        when(mockCollection.insertOne(doc)).thenReturn(publisher);

        Mono<InsertOneResult> result = executor.insertOne(doc);

        StepVerifier.create(result).expectNext(insertResult).verifyComplete();
    }

    // every subscription re-submits.
    @Test
    public void testInsertOneConvertsEntityOnceAtCallTimeNotPerSubscription() {
        final CountingBean bean = new CountingBean();
        final InsertOneResult insertResult = mock(InsertOneResult.class);
        when(mockCollection.insertOne(any(Document.class))).thenReturn(Mono.just(insertResult));

        final Mono<InsertOneResult> result = executor.insertOne(bean);

        assertTrue(bean.readCount > 0);
        final int readsAfterBuild = bean.readCount;

        StepVerifier.create(result).expectNext(insertResult).verifyComplete();
        StepVerifier.create(result).expectNext(insertResult).verifyComplete();

        assertEquals(readsAfterBuild, bean.readCount);
        final org.mockito.ArgumentCaptor<Document> captor = org.mockito.ArgumentCaptor.forClass(Document.class);
        verify(mockCollection).insertOne(captor.capture());
        assertEquals("counted", captor.getValue().getString("name"));
    }

    // Document instance rather than building a fresh write.
    @Test
    public void testInsertOneResubscriptionReissuesInsertWithSameDocument() {
        final Document doc = new Document("name", "dup");
        final InsertOneResult insertResult = mock(InsertOneResult.class);
        final java.util.concurrent.atomic.AtomicInteger executions = new java.util.concurrent.atomic.AtomicInteger();
        final Publisher<InsertOneResult> publisher = Mono.fromSupplier(() -> {
            executions.incrementAndGet();
            return insertResult;
        });
        when(mockCollection.insertOne(doc)).thenReturn(publisher);

        final Mono<InsertOneResult> result = executor.insertOne(doc);

        verify(mockCollection).insertOne(org.mockito.ArgumentMatchers.same(doc));
        assertEquals(0, executions.get());

        StepVerifier.create(result).expectNext(insertResult).verifyComplete();
        StepVerifier.create(result).expectNext(insertResult).verifyComplete();

        assertEquals(2, executions.get());
        verify(mockCollection, org.mockito.Mockito.times(1)).insertOne(any(Document.class));
    }

    @Test
    public void testInsertOneWithNonDocumentObject() {
        Map<String, Object> map = new HashMap<>();
        map.put("name", "test");
        map.put("value", 123);

        InsertOneResult insertResult = mock(InsertOneResult.class);
        Publisher<InsertOneResult> publisher = Mono.just(insertResult);
        when(mockCollection.insertOne(any(Document.class))).thenReturn(publisher);

        Mono<InsertOneResult> result = executor.insertOne(map);

        StepVerifier.create(result).expectNext(insertResult).verifyComplete();
    }

    @Test
    public void testInsertOneWithNullObjectAndOptionsThrowsIAESynchronously() {
        InsertOneOptions options = new InsertOneOptions();

        assertThrows(IllegalArgumentException.class, () -> executor.insertOne(null, options));

        verify(mockCollection, org.mockito.Mockito.never()).insertOne(any(Document.class), any(InsertOneOptions.class));
    }

    @Test
    public void testInsertOneWithNullObjectThrowsIAESynchronously() {
        // Regression: insertOne(null) converted the payload eagerly and surfaced a raw NPE. It must
        // throw IllegalArgumentException synchronously, before any Mono is built or the driver touched.
        assertThrows(IllegalArgumentException.class, () -> executor.insertOne((Object) null));

        verify(mockCollection, org.mockito.Mockito.never()).insertOne(any(Document.class));
    }

    @Test
    public void testInsertOneWithOptions() {
        Document doc = new Document("name", "test");
        InsertOneOptions options = new InsertOneOptions();
        InsertOneResult insertResult = mock(InsertOneResult.class);
        Publisher<InsertOneResult> publisher = Mono.just(insertResult);
        when(mockCollection.insertOne(doc, options)).thenReturn(publisher);

        Mono<InsertOneResult> result = executor.insertOne(doc, options);

        StepVerifier.create(result).expectNext(insertResult).verifyComplete();
    }

    @Test
    public void testReplaceOneWithFilter() {
        Bson filter = new Document("name", "old name");
        Document replacement = new Document("name", "new name").append("status", "active");
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.replaceOne(eq(filter), eq(replacement))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.replaceOne(filter, replacement);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testReplaceOneWithFilterAndOptions() {
        Bson filter = new Document("name", "old name");
        Document replacement = new Document("name", "new name").append("status", "active");
        ReplaceOptions options = new ReplaceOptions();
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.replaceOne(eq(filter), eq(replacement), eq(options))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.replaceOne(filter, replacement, options);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testReplaceOneWithNonDocumentObject() {
        Bson filter = new Document("name", "oldName");
        Map<String, Object> replacement = new HashMap<>();
        replacement.put("name", "newName");
        replacement.put("status", "active");

        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.replaceOne(eq(filter), any(Document.class))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.replaceOne(filter, replacement);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testReplaceOneWithNullReplacementThrowsIAE() {
        Document filter = new Document("id", 1);
        assertThrows(IllegalArgumentException.class, () -> executor.replaceOne(filter, (Object) null));
    }

    @Test
    public void testReplaceOneWithObjectId() {
        ObjectId objectId = new ObjectId();
        Document replacement = new Document("name", "new name").append("status", "active");
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.replaceOne(any(Bson.class), eq(replacement))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.replaceOne(objectId, replacement);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testReplaceOneWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        Document replacement = new Document("name", "new name").append("status", "active");
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.replaceOne(any(Bson.class), eq(replacement))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.replaceOne(objectId, replacement);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testUpdateMany() {
        Bson filter = new Document("status", "pending");
        Document update = new Document("status", "processed");
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateMany(eq(filter), any(Bson.class))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateMany(filter, update);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testUpdateManyWithList() {
        Bson filter = new Document("status", "pending");
        List<Bson> updates = Arrays.asList(new Document("$set", new Document("status", "processed")));
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateMany(eq(filter), eq(updates))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateMany(filter, updates);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testUpdateManyWithListAndOptions() {
        Bson filter = new Document("status", "pending");
        List<Bson> updates = Arrays.asList(new Document("$set", new Document("status", "processed")));
        UpdateOptions options = new UpdateOptions();
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateMany(eq(filter), eq(updates), eq(options))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateMany(filter, updates, options);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testUpdateManyWithNonBsonCollection() {
        Bson filter = new Document("status", "pending");
        List<Map<String, Object>> updates = new ArrayList<>();
        Map<String, Object> update = new HashMap<>();
        update.put("$set", new Document("status", "active"));
        updates.add(update);

        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateMany(eq(filter), anyList())).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateMany(filter, updates);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testUpdateManyWithNullUpdateThrowsIAE() {
        Document filter = new Document("id", 1);
        assertThrows(IllegalArgumentException.class, () -> executor.updateMany(filter, (Object) null));
    }

    @Test
    public void testUpdateManyWithOptions() {
        Bson filter = new Document("status", "pending");
        Document update = new Document("status", "processed");
        UpdateOptions options = new UpdateOptions();
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateMany(eq(filter), any(Bson.class), eq(options))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateMany(filter, update, options);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testUpdateOneDoesNotMutateCallerDocument() {
        // Regression: toBson stripped _id from the CALLER's Document in place on the
        // non-$-operator path; the $set payload must be built from a copy.
        Bson filter = new Document("name", "test");
        ObjectId id = new ObjectId();
        Document callerUpdate = new Document("_id", id).append("name", "updated");
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        org.mockito.ArgumentCaptor<Bson> updateCaptor = org.mockito.ArgumentCaptor.forClass(Bson.class);
        when(mockCollection.updateOne(eq(filter), any(Bson.class))).thenReturn(publisher);

        StepVerifier.create(executor.updateOne(filter, callerUpdate)).expectNext(updateResult).verifyComplete();

        // The caller's Document must be untouched.
        assertEquals(id, callerUpdate.get("_id"));
        assertEquals(2, callerUpdate.size());
        // The sent update is {$set: {...}} without _id.
        verify(mockCollection).updateOne(eq(filter), updateCaptor.capture());
        Document sent = (Document) updateCaptor.getValue();
        Document setPayload = (Document) sent.get("$set");
        assertTrue(!setPayload.containsKey("_id"));
        assertEquals("updated", setPayload.get("name"));
    }

    @Test
    public void testUpdateOneNormalizesPlainBsonDocumentInPipeline() {
        Bson filter = new Document("name", "test");
        Document plainUpdate = new Document("status", "active");
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockCollection.updateOne(eq(filter), anyList())).thenReturn(Mono.just(updateResult));

        StepVerifier.create(executor.updateOne(filter, Arrays.asList(plainUpdate))).expectNext(updateResult).verifyComplete();

        @SuppressWarnings("rawtypes")
        org.mockito.ArgumentCaptor<List> updatesCaptor = org.mockito.ArgumentCaptor.forClass(List.class);
        verify(mockCollection).updateOne(eq(filter), updatesCaptor.capture());
        assertEquals(Arrays.asList(new Document("$set", new Document("status", "active"))), updatesCaptor.getValue());
        assertEquals(new Document("status", "active"), plainUpdate, "The caller's update document must remain unchanged");
    }

    // preserves their values even though the containing list is defensively rebuilt.
    @Test
    public void testUpdateOneWithAllBsonListPassthrough() {
        Bson filter = new Document("name", "test");
        List<Bson> updates = Arrays.asList(new Document("$set", new Document("a", 1)), new Document("$set", new Document("b", 2)));
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateOne(eq(filter), eq(updates))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateOne(filter, updates);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
        verify(mockCollection).updateOne(eq(filter), eq(updates));
    }

    // The same per-element normalization works for a non-List Bson collection.
    @Test
    public void testUpdateOneWithAllBsonNonListCollection() {
        Bson filter = new Document("name", "test");
        Collection<Bson> updates = new java.util.LinkedHashSet<>(
                Arrays.asList(new Document("$set", new Document("a", 1)), new Document("$set", new Document("b", 2))));
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateOne(eq(filter), anyList())).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateOne(filter, updates);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
        verify(mockCollection).updateOne(eq(filter), anyList());
    }

    @Test
    public void testUpdateOneWithBasicDBObject() {
        Bson filter = new Document("name", "test");
        BasicDBObject update = new BasicDBObject("$set", new BasicDBObject("status", "updated"));
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateOne(eq(filter), eq(update))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateOne(filter, update);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testUpdateOneWithBsonUpdate() {
        Bson filter = new Document("name", "test");
        Bson update = new Document("$set", new Document("status", "updated"));
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateOne(eq(filter), eq(update))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateOne(filter, update);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testUpdateOneWithCollectionAsList() {
        Bson filter = new Document("name", "test");
        List<Bson> updates = Arrays.asList(new Document("$set", new Document("status", "active")));
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateOne(eq(filter), eq(updates))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateOne(filter, updates);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testUpdateOneWithDriverBuiltBsonNotWrappedInSet() {
        // Regression: a driver-built Bson (Updates.set(...)) is neither Document nor BasicDBObject.
        // It must be passed through as-is, not re-wrapped in {$set: ...} which corrupts the update.
        Bson filter = new Document("name", "test");
        Bson update = com.mongodb.client.model.Updates.set("verified", true);
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateOne(eq(filter), eq(update))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateOne(filter, update);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    // These are not Document / BasicDBObject so must be returned as-is (no {$set: ...} wrapping).
    @Test
    public void testUpdateOneWithDriverBuiltUpdateBson() {
        Bson filter = new Document("name", "test");
        Bson update = com.mongodb.client.model.Updates.set("status", "active");
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateOne(eq(filter), eq(update))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateOne(filter, update);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    // Covers toBson(Collection) empty-collection guard: must throw IllegalArgumentException.
    @Test
    public void testUpdateOneWithEmptyCollection_EdgeCase() {
        Bson filter = new Document("name", "test");
        assertThrows(IllegalArgumentException.class, () -> {
            executor.updateOne(filter, Collections.emptyList());
        });
    }

    @Test
    public void testUpdateOneWithFilter() {
        Bson filter = new Document("name", "test");
        Document update = new Document("status", "updated");
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateOne(eq(filter), any(Bson.class))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateOne(filter, update);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testUpdateOneWithFilterAndList() {
        Bson filter = new Document("name", "test");
        List<Bson> updates = Arrays.asList(new Document("$set", new Document("status", "active")), new Document("$inc", new Document("count", 1)));
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateOne(eq(filter), eq(updates))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateOne(filter, updates);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testUpdateOneWithFilterAndOptions() {
        Bson filter = new Document("name", "test");
        Document update = new Document("status", "updated");
        UpdateOptions options = new UpdateOptions();
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateOne(eq(filter), any(Bson.class), eq(options))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateOne(filter, update, options);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testUpdateOneWithFilterListAndOptions() {
        Bson filter = new Document("name", "test");
        List<Bson> updates = Arrays.asList(new Document("$set", new Document("status", "active")));
        UpdateOptions options = new UpdateOptions();
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateOne(eq(filter), eq(updates), eq(options))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateOne(filter, updates, options);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testUpdateOneWithNullUpdateThrowsIAE() {
        Document filter = new Document("id", 1);
        assertThrows(IllegalArgumentException.class, () -> executor.updateOne(filter, (Object) null));
    }

    @Test
    public void testUpdateOneWithObjectId() {
        ObjectId objectId = new ObjectId();
        Document update = new Document("status", "updated");
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateOne(any(Bson.class), any(Bson.class))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateOne(objectId, update);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testUpdateOneWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        Document update = new Document("status", "updated");
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateOne(any(Bson.class), any(Bson.class))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateOne(objectId, update);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testUpdateRejectsMixedOperatorAndOrdinaryFieldsBeforePublisherCreation() {
        Document filter = new Document("active", true);
        Document invalidUpdate = new Document("$set", new Document("name", "alice")).append("status", "active");
        BasicDBObject reversedInvalidUpdate = new BasicDBObject("status", "active").append("$set", new BasicDBObject("name", "alice"));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> executor.updateOne(filter, invalidUpdate));

        assertTrue(ex.getMessage().contains("cannot mix"));
        assertThrows(IllegalArgumentException.class, () -> executor.updateOne(filter, reversedInvalidUpdate));
        verify(mockCollection, org.mockito.Mockito.never()).updateOne(any(Bson.class), any(Bson.class));
    }

    // ---- 2026-10-24 r2 sliceU ----

    public static class PipelineInner {
        private String city;

        public String getCity() {
            return city;
        }

        public void setCity(final String city) {
            this.city = city;
        }
    }

    public static class PipelineEntity {
        private String note;
        private String[] tags;
        private PipelineInner inner;
        private Map<String, Object> meta;

        public String getNote() {
            return note;
        }

        public void setNote(final String note) {
            this.note = note;
        }

        public String[] getTags() {
            return tags;
        }

        public void setTags(final String[] tags) {
            this.tags = tags;
        }

        public PipelineInner getInner() {
            return inner;
        }

        public void setInner(final PipelineInner inner) {
            this.inner = inner;
        }

        public Map<String, Object> getMeta() {
            return meta;
        }

        public void setMeta(final Map<String, Object> meta) {
            this.meta = meta;
        }
    }

    /** Reaches the framework codec registry (protected in MongoDBBase) to encode a stage as the server receives it. */
    private static final class CodecAccess extends com.landawn.abacus.da.mongodb.MongoDBBase {
        static org.bson.BsonDocument encoded(final Object stage) {
            return ((Bson) stage).toBsonDocument(org.bson.BsonDocument.class, codecRegistry);
        }
    }

    private static PipelineEntity pipelineEntity() {
        final PipelineInner inner = new PipelineInner();
        inner.setCity("$x");
        final Map<String, Object> meta = new java.util.LinkedHashMap<>();
        meta.put("k", "$other");
        final PipelineEntity entity = new PipelineEntity();
        entity.setNote("$other");
        entity.setTags(new String[] { "$x", "plain" });
        entity.setInner(inner);
        entity.setMeta(meta);
        return entity;
    }

    /**
     * r2 sliceU (reactive twin of the sync r2 sliceQ fix): the pipeline-update overloads wrapped a converted entity as
     * {@code {$set: doc}}, and the server evaluates every value of a pipeline {@code $set} as an aggregation expression (proven
     * live on the reactive executor: an entity's {@code "$other"} stored the value of field {@code other}, {@code "$x"} inside a
     * {@code String[]} was resolved, a Map's {@code "$missingField"} removed the field). Converted entity, non-Bson map and
     * name/value array values are now written as {@code {$literal: value}}.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testPipelineUpdatesWriteConvertedEntityValuesLiterally_r2sliceU() {
        final Document filter = new Document("name", "a");
        final org.bson.BsonDocument expectedEntityStage = org.bson.BsonDocument.parse("{$set: {note: {$literal: '$other'}, tags: {$literal: ['$x', 'plain']},"
                + " inner: {$literal: {city: '$x'}}, meta: {$literal: {k: '$other'}}}}");
        final Map<String, Object> map = new java.util.LinkedHashMap<>();
        map.put("_id", "id-1");
        map.put("status", "$other");
        map.put("codes", Arrays.asList("$x", 1));

        when(mockCollection.updateOne(any(Bson.class), anyList())).thenReturn(Mono.empty());
        when(mockCollection.updateMany(any(Bson.class), anyList(), any(UpdateOptions.class))).thenReturn(Mono.empty());
        when(mockCollection.findOneAndUpdate(any(Bson.class), anyList(), any(FindOneAndUpdateOptions.class))).thenReturn(Mono.empty());

        final org.mockito.ArgumentCaptor<List> updateOneCaptor = org.mockito.ArgumentCaptor.forClass(List.class);
        final org.mockito.ArgumentCaptor<List> updateManyCaptor = org.mockito.ArgumentCaptor.forClass(List.class);
        final org.mockito.ArgumentCaptor<List> findOneAndUpdateCaptor = org.mockito.ArgumentCaptor.forClass(List.class);

        executor.updateOne(filter, Arrays.asList(pipelineEntity()));
        executor.updateMany(filter, Arrays.asList(map), new UpdateOptions());
        executor.findOneAndUpdate(filter, Collections.singletonList(new Object[] { "note", "$missingField" }), new FindOneAndUpdateOptions(), Document.class);

        verify(mockCollection).updateOne(any(Bson.class), updateOneCaptor.capture());
        verify(mockCollection).updateMany(any(Bson.class), updateManyCaptor.capture(), any(UpdateOptions.class));
        verify(mockCollection).findOneAndUpdate(any(Bson.class), findOneAndUpdateCaptor.capture(), any(FindOneAndUpdateOptions.class));

        org.junit.jupiter.api.Assertions.assertAll( //
                () -> assertEquals(expectedEntityStage, CodecAccess.encoded(updateOneCaptor.getValue().get(0)), "entity stage"),
                () -> assertEquals(org.bson.BsonDocument.parse("{$set: {status: {$literal: '$other'}, codes: {$literal: ['$x', 1]}}}"),
                        CodecAccess.encoded(updateManyCaptor.getValue().get(0)), "map stage (_id stripped)"),
                () -> assertEquals(org.bson.BsonDocument.parse("{$set: {note: {$literal: '$missingField'}}}"),
                        CodecAccess.encoded(findOneAndUpdateCaptor.getValue().get(0)), "name/value array stage"),
                () -> assertEquals("$other", map.get("status"), "caller map unchanged"));
    }

    /** Pins what the literal wrapping must leave alone: caller-built Bson stages, operator maps and the single-update overloads. */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testPipelineUpdatesKeepBsonStagesAsExpressions_r2sliceU() {
        final Document filter = new Document("name", "a");
        final Document computedStage = new Document("$set", new Document("total", new Document("$add", Arrays.asList("$a", "$b"))));
        final Document plainDocument = new Document("total", new Document("$add", Arrays.asList("$a", "$b"))).append("_id", 1);
        final Map<String, Object> operatorMap = new java.util.LinkedHashMap<>();
        operatorMap.put("$set", new Document("copy", "$a"));

        when(mockCollection.updateOne(any(Bson.class), anyList())).thenReturn(Mono.empty());
        when(mockCollection.updateOne(any(Bson.class), any(Bson.class))).thenReturn(Mono.empty());

        final org.mockito.ArgumentCaptor<List> pipelineCaptor = org.mockito.ArgumentCaptor.forClass(List.class);
        final org.mockito.ArgumentCaptor<Bson> classicCaptor = org.mockito.ArgumentCaptor.forClass(Bson.class);

        executor.updateOne(filter, Arrays.asList(computedStage, plainDocument, operatorMap));
        executor.updateOne(filter, pipelineEntity());

        verify(mockCollection).updateOne(any(Bson.class), pipelineCaptor.capture());
        verify(mockCollection).updateOne(any(Bson.class), classicCaptor.capture());

        final List<Object> pipeline = pipelineCaptor.getValue();

        org.junit.jupiter.api.Assertions.assertAll( //
                () -> assertSame(computedStage, pipeline.get(0), "operator Document stage passed as is"),
                () -> assertEquals(org.bson.BsonDocument.parse("{$set: {total: {$add: ['$a', '$b']}}}"), CodecAccess.encoded(pipeline.get(1)),
                        "plain Document stays an expression ($set-wrapped, _id stripped)"),
                () -> assertEquals(org.bson.BsonDocument.parse("{$set: {copy: '$a'}}"), CodecAccess.encoded(pipeline.get(2)), "operator Map stage"),
                () -> assertEquals(org.bson.BsonDocument.parse("{$set: {note: '$other', tags: ['$x', 'plain'], inner: {city: '$x'}, meta: {k: '$other'}}}"),
                        CodecAccess.encoded(classicCaptor.getValue()), "classic single-entity update (already literal) unchanged"));
    }

    /**
     * r2 sliceU (same wording as the sync r2 sliceQ fix): a null element of a collection argument was reported under the wrong
     * name: "'requests' cannot be null" (the list itself was not null), "'obj'"/"'update'" (no such parameter) or "'entity'". The
     * message now names the argument and the element's index; the exception (IllegalArgumentException, thrown at call time) is
     * unchanged and nothing is sent.
     */
    @Test
    public void testNullCollectionElementsAreReportedByArgumentAndIndex_r2sliceU() {
        final Document filter = new Document("name", "a");
        final Document doc = new Document("name", "x");

        org.junit.jupiter.api.Assertions.assertAll( //
                () -> assertEquals("'requests' element at index 1 cannot be null",
                        assertThrows(IllegalArgumentException.class, () -> executor.bulkWrite(Arrays.asList(new InsertOneModel<>(doc), null))).getMessage()),
                () -> assertEquals("'entities' element at index 1 cannot be null",
                        assertThrows(IllegalArgumentException.class, () -> executor.bulkInsert(Arrays.asList(doc, null))).getMessage()),
                () -> assertEquals("'objList' element at index 2 cannot be null",
                        assertThrows(IllegalArgumentException.class, () -> executor.insertMany(Arrays.asList(doc, pipelineEntity(), null))).getMessage()),
                () -> assertEquals("'objList' element at index 1 cannot be null",
                        assertThrows(IllegalArgumentException.class, () -> executor.updateOne(filter, Arrays.asList(doc, null))).getMessage()),
                () -> assertEquals("'objList' element at index 0 cannot be null",
                        assertThrows(IllegalArgumentException.class, () -> executor.updateMany(filter, Arrays.asList((Object) null))).getMessage()),
                () -> assertEquals("'objList' element at index 1 cannot be null",
                        assertThrows(IllegalArgumentException.class, () -> executor.findOneAndUpdate(filter, Arrays.asList(doc, null))).getMessage()));

        verify(mockCollection, org.mockito.Mockito.never()).bulkWrite(anyList());
        verify(mockCollection, org.mockito.Mockito.never()).insertMany(anyList());
        verify(mockCollection, org.mockito.Mockito.never()).updateOne(any(Bson.class), anyList());
        verify(mockCollection, org.mockito.Mockito.never()).updateMany(any(Bson.class), anyList());
        verify(mockCollection, org.mockito.Mockito.never()).findOneAndUpdate(any(Bson.class), anyList());
    }
}
