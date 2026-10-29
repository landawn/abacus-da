package com.landawn.abacus.da.mongodb;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.ContinuableFuture;
import com.mongodb.client.MongoCollection;

public class AsyncMongoCollectionExecutorTest extends AsyncMongoCollectionExecutorTestSupport {

    @Test
    public void testConstructorRejectsNullAsyncExecutorEagerly() {
        final IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> new AsyncMongoCollectionExecutor(mockCollExecutor, null));
        Assertions.assertTrue(e.getMessage().contains("asyncExecutor"), e.getMessage());

        // MongoCollectionExecutor builds its async view eagerly, so the guard also fires there.
        final MongoCollection<Document> mockColl = mock(MongoCollection.class);
        Assertions.assertThrows(IllegalArgumentException.class, () -> new MongoCollectionExecutor(mockColl, null));

        verifyNoInteractions(mockCollExecutor);
        verifyNoInteractions(mockColl);
    }

    @Test
    public void testSync() {
        MongoCollectionExecutor result = asyncExecutor.sync();
        Assertions.assertSame(mockCollExecutor, result);
    }

    @Test
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testNullArgumentsRejectedEagerlyOnCallingThread() {
        // mockAsyncExecutor is deliberately NOT stubbed here: an unstubbed execute(Callable) returns null
        // without ever invoking the callable. Every assertion below therefore only passes if the argument
        // is validated on the calling thread, BEFORE the task is submitted.
        final AsyncExecutor mockAsyncExecutor = mock(AsyncExecutor.class);
        final AsyncMongoCollectionExecutor asyncExecutor = new AsyncMongoCollectionExecutor(mockCollExecutor, mockAsyncExecutor);
        final Bson filter = new Document();
        final String oid = "507f1f77bcf86cd799439011";

        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.exists((Bson) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.exists((ObjectId) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.exists((String) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.count((Bson) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.get((String) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.get((ObjectId) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.get(oid, (Class<Document>) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.gett((String) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.findFirst((Bson) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.findFirst(filter, (Class<Document>) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.list((Bson) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.query((Bson) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.stream((Bson) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.queryForSingleValue(null, filter, String.class));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.queryForSingleValue("p", filter, (Class<String>) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.watch((Class<Document>) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.watch((List<Bson>) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.insertOne(null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.insertMany(null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.insertMany(Collections.emptyList()));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.updateOne((Bson) null, (Object) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.updateOne(filter, (Object) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.updateOne(filter, (java.util.Collection<?>) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.updateMany(filter, (Object) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.replaceOne(filter, (Object) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.deleteOne((Bson) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.deleteMany((Bson) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.bulkInsert(null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.bulkWrite(null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.findOneAndUpdate(filter, (Object) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.findOneAndReplace(filter, (Object) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.findOneAndDelete((Bson) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.distinct((String) null, String.class));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.distinct("name", (Bson) null, String.class));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.aggregate((List<Bson>) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.groupBy((String) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.groupBy((java.util.Collection<String>) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.groupByAndCount((String) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> asyncExecutor.mapReduce(null, "reduce"));

        // Nothing was ever submitted to the executor and the sync executor was never touched.
        verify(mockAsyncExecutor, never()).execute(any(Callable.class));
        verifyNoInteractions(mockCollExecutor);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testReadMethodsRejectNullFilter() {
        final MongoCollection<Document> mockColl = mock(MongoCollection.class);
        final MongoCollectionExecutor syncExec = new MongoCollectionExecutor(mockColl, directExecutor);
        final AsyncMongoCollectionExecutor async = new AsyncMongoCollectionExecutor(syncExec, directExecutor);

        Assertions.assertThrows(IllegalArgumentException.class, () -> async.findFirst((Bson) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> async.findFirst((Bson) null, Document.class));
        Assertions.assertThrows(IllegalArgumentException.class, () -> async.list((Bson) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> async.list((Bson) null, Document.class));
        Assertions.assertThrows(IllegalArgumentException.class, () -> async.query((Bson) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> async.query((Bson) null, Document.class));

        // stream / queryForSingleValue / count / exists / distinct take a Bson filter too and must reject null.
        Assertions.assertThrows(IllegalArgumentException.class, () -> async.stream((Bson) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> async.queryForSingleValue("name", (Bson) null, String.class));
        Assertions.assertThrows(IllegalArgumentException.class, () -> async.count((Bson) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> async.exists((Bson) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> async.distinct("name", (Bson) null, String.class));

        verify(mockColl, never()).find();
        verify(mockColl, never()).find(any(Bson.class));
        verify(mockColl, never()).countDocuments(any(Bson.class));
    }

    @Test
    void submitsWorkWithoutCallingTheSyncExecutorUntilTheTaskRuns() throws Exception {
        AtomicReference<Runnable> submitted = new AtomicReference<>();
        AsyncExecutor scheduler = new AsyncExecutor(submitted::set);
        AsyncMongoCollectionExecutor deferred = new AsyncMongoCollectionExecutor(mockCollExecutor, scheduler);
        Document filter = new Document("status", "active");
        when(mockCollExecutor.count(filter)).thenReturn(7L);

        ContinuableFuture<Long> result = deferred.count(filter);

        Assertions.assertFalse(result.isDone());
        Assertions.assertNotNull(submitted.get());
        verifyNoInteractions(mockCollExecutor);
        submitted.get().run();
        Assertions.assertEquals(7L, result.get(1, TimeUnit.SECONDS));
        verify(mockCollExecutor).count(filter);
        verifyNoMoreInteractions(mockCollExecutor);
    }

    @Test
    public void testSyncFailureSurfacesAsFutureCompletionException() throws Exception {
        final AsyncExecutor realExecutor = new AsyncExecutor(1, 1, 0L, TimeUnit.SECONDS);

        try {
            final AsyncMongoCollectionExecutor async = new AsyncMongoCollectionExecutor(mockCollExecutor, realExecutor);
            final com.mongodb.MongoException failure = new com.mongodb.MongoException("boom");
            final Document filter = new Document("k", "v");
            when(mockCollExecutor.count(filter)).thenThrow(failure);

            final ContinuableFuture<Long> future = async.count(filter);

            // The call site returned normally; the failure is delivered through the future only.
            final ExecutionException e = Assertions.assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
            Assertions.assertSame(failure, e.getCause());
            verify(mockCollExecutor).count(filter);
        } finally {
            realExecutor.shutdown();
        }
    }

    // ---- 2026-10-27 sliceR ----

    @Test
    @SuppressWarnings("unchecked")
    public void testArgumentsAreReadOnlyWhenTheSubmittedTaskRuns_sliceR() throws Exception {
        // Pins the class-level "Arguments are not copied" note: the eager checks run at the call site, but the
        // task reads the caller's (mutable) arguments only when it runs.
        final AtomicReference<Runnable> submitted = new AtomicReference<>();
        final MongoCollection<Document> mockColl = mock(MongoCollection.class);
        final AsyncMongoCollectionExecutor deferred = new AsyncMongoCollectionExecutor(new MongoCollectionExecutor(mockColl, directExecutor),
                new AsyncExecutor(submitted::set));
        final List<Integer> sizesSeen = new java.util.ArrayList<>();
        when(mockColl.insertMany(org.mockito.ArgumentMatchers.anyList())).thenAnswer(inv -> {
            sizesSeen.add(((List<?>) inv.getArgument(0)).size());
            return null;
        });

        final List<Document> cleared = new java.util.ArrayList<>(List.of(new Document("a", 1)));
        final ContinuableFuture<?> clearedFuture = deferred.insertMany(cleared); // accepted by the eager non-empty check
        cleared.clear();
        submitted.get().run();

        final List<Document> grown = new java.util.ArrayList<>(List.of(new Document("b", 1)));
        final ContinuableFuture<?> grownFuture = deferred.insertMany(grown);
        grown.add(new Document("b", 2));
        submitted.get().run();

        Assertions.assertAll(() -> Assertions.assertInstanceOf(IllegalArgumentException.class,
                Assertions.assertThrows(ExecutionException.class, clearedFuture::get).getCause()), () -> {
                    grownFuture.get();
                    Assertions.assertEquals(List.of(2), sizesSeen);
                });
    }

    @Test
    public void testMapOverAStreamFutureReappliesTheFunctionOnEveryGet_sliceR() throws Exception {
        // Pins the class-level map note: ContinuableFuture.map re-runs its function per get(), so a function that
        // consumes the delivered stream fails on the second get().
        final Document filter = new Document("k", "v");
        when(mockCollExecutor.stream(filter)).thenReturn(com.landawn.abacus.util.stream.Stream.of(new Document("a", 1), new Document("a", 2)));

        final ContinuableFuture<List<Document>> mapped = asyncExecutor.stream(filter).map(s -> s.toList());

        Assertions.assertEquals(2, mapped.get().size());
        Assertions.assertInstanceOf(IllegalStateException.class, Assertions.assertThrows(ExecutionException.class, mapped::get).getCause());
        verify(mockCollExecutor).stream(filter);
    }

    @Test
    public void testWatchWithRowTypeDeliversTheDriverCodecLookupFailureThroughTheFuture_sliceR() throws Exception {
        // The driver resolves the codec for rowType when it creates the change-stream iterable, without contacting the
        // server, so no MongoDB is needed here (nothing listens on port 1; the client never connects).
        try (com.mongodb.client.MongoClient client = com.mongodb.client.MongoClients
                .create("mongodb://localhost:1/?serverSelectionTimeoutMS=100&connectTimeoutMS=100")) {
            // A collection passed in as-is keeps the client's plain registry, which has no codec for a bean class.
            final AsyncMongoCollectionExecutor plainRegistry = new MongoCollectionExecutor(client.getDatabase("sliceR").getCollection("c"), directExecutor)
                    .async();
            final AsyncMongoCollectionExecutor frameworkRegistry = new MongoCollectionExecutor(
                    new MongoDB(client.getDatabase("sliceR")).db().getCollection("c"), directExecutor).async();

            final ContinuableFuture<?> byClass = plainRegistry.watch(TestEntity.class); // returns normally
            final ContinuableFuture<?> byPipeline = plainRegistry.watch(Collections.<Bson> emptyList(), TestEntity.class);

            Assertions.assertAll(
                    () -> Assertions.assertInstanceOf(org.bson.codecs.configuration.CodecConfigurationException.class,
                            Assertions.assertThrows(ExecutionException.class, byClass::get).getCause()),
                    () -> Assertions.assertInstanceOf(org.bson.codecs.configuration.CodecConfigurationException.class,
                            Assertions.assertThrows(ExecutionException.class, byPipeline::get).getCause()),
                    () -> Assertions.assertNotNull(frameworkRegistry.watch(TestEntity.class).get()));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testTypedFindOneAndWriteFailsItsFutureOnlyAfterTheWriteWasSent_sliceR() {
        // Pins the typed findOneAndUpdate/Replace/Delete note: the returned document is converted to rowType after the driver
        // call (proven live: the document stayed deleted/updated/replaced), so a conversion failure does not mean "not written".
        final MongoCollection<Document> mockColl = mock(MongoCollection.class);
        final AsyncMongoCollectionExecutor async = new MongoCollectionExecutor(mockColl, directExecutor).async();
        final Document filter = new Document("k", 1);
        final Document update = new Document("$set", new Document("z", 7));
        final Document replacement = new Document("k", 1).append("r", 1);
        final Document twoFields = new Document("_id", 1).append("x", 1).append("y", 2); // no single-value Integer form
        when(mockColl.findOneAndDelete(filter)).thenReturn(twoFields);
        when(mockColl.findOneAndUpdate(filter, update)).thenReturn(twoFields);
        when(mockColl.findOneAndReplace(filter, replacement)).thenReturn(twoFields);

        final ContinuableFuture<Integer> deleted = async.findOneAndDelete(filter, Integer.class);
        final ContinuableFuture<Integer> updated = async.findOneAndUpdate(filter, update, Integer.class);
        final ContinuableFuture<Integer> replaced = async.findOneAndReplace(filter, replacement, Integer.class);

        Assertions.assertAll(
                () -> Assertions.assertInstanceOf(IllegalArgumentException.class, Assertions.assertThrows(ExecutionException.class, deleted::get).getCause()),
                () -> Assertions.assertInstanceOf(IllegalArgumentException.class, Assertions.assertThrows(ExecutionException.class, updated::get).getCause()),
                () -> Assertions.assertInstanceOf(IllegalArgumentException.class, Assertions.assertThrows(ExecutionException.class, replaced::get).getCause()),
                () -> verify(mockColl).findOneAndDelete(filter), () -> verify(mockColl).findOneAndUpdate(filter, update),
                () -> verify(mockColl).findOneAndReplace(filter, replacement));
    }

    @Test
    public void testAggregateNullStageFailsTheFutureNotTheCallSite_sliceR() throws Exception {
        // The driver rejects a null stage while it builds the aggregate operation, before any server contact, so no MongoDB is
        // needed (nothing listens on port 1). Only the null pipeline itself is checked eagerly on the calling thread.
        try (com.mongodb.client.MongoClient client = com.mongodb.client.MongoClients
                .create("mongodb://localhost:1/?serverSelectionTimeoutMS=100&connectTimeoutMS=100")) {
            final AsyncMongoCollectionExecutor async = new MongoCollectionExecutor(new MongoDB(client.getDatabase("sliceR")).db().getCollection("c"),
                    directExecutor).async();

            final ContinuableFuture<?> lastNull = async.aggregate(java.util.Arrays.asList(new Document("$match", new Document()), null));
            final ContinuableFuture<?> firstNull = async.aggregate(java.util.Arrays.asList(null, new Document("$match", new Document())), Document.class);

            Assertions.assertAll(
                    () -> Assertions.assertInstanceOf(IllegalArgumentException.class,
                            Assertions.assertThrows(ExecutionException.class, lastNull::get).getCause()),
                    () -> Assertions.assertInstanceOf(IllegalArgumentException.class,
                            Assertions.assertThrows(ExecutionException.class, firstNull::get).getCause()),
                    () -> Assertions.assertThrows(IllegalArgumentException.class, () -> async.aggregate((List<Bson>) null)));
        }
    }
}
