package com.landawn.abacus.da.mongodb;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.stream.Stream;
import com.mongodb.client.MongoCollection;

public class AsyncMongoCollectionMappingTest extends AsyncMongoCollectionExecutorTestSupport {

    @Test
    public void testDecodedValueConversionsOnAsyncReadPaths() throws Exception {
        // The async read methods run the sync ones, so the MongoDBBase read-side conversions (typed container elements,
        // java.time Local* in UTC, generic bean type arguments, records) must hold for the futures' results too. Fixtures are
        // shared with MongoCollectionExecutorTest (*); each read path is asserted on its own.
        final AsyncExecutor realExecutor = new AsyncExecutor(1, 1, 0L, TimeUnit.SECONDS);

        try {
            final AsyncMongoCollectionExecutor entityAsync = new MongoCollectionExecutor(
                    MongoMappingFixtures.mockCollectionReturning(MongoMappingFixtures.entityDoc()), realExecutor).async();
            final AsyncMongoCollectionExecutor recordAsync = new MongoCollectionExecutor(
                    MongoMappingFixtures.mockCollectionReturning(MongoMappingFixtures.recordDoc()), realExecutor).async();
            final AsyncMongoCollectionExecutor boxAsync = new MongoCollectionExecutor(
                    MongoMappingFixtures.mockCollectionReturning(MongoMappingFixtures.longBoxDoc()), realExecutor).async();
            final AsyncMongoCollectionExecutor dayAsync = new MongoCollectionExecutor(
                    MongoMappingFixtures.mockCollectionReturning(MongoMappingFixtures.dayDoc()), realExecutor).async();
            final Bson filter = new Document();
            final Bson update = new Document("$set", new Document("x", 1));
            final Class<MongoMappingFixtures.MappedEntity> type = MongoMappingFixtures.MappedEntity.class;
            final Class<MongoMappingFixtures.MappedRecord> recordType = MongoMappingFixtures.MappedRecord.class;
            final Class<MongoMappingFixtures.LongBox> boxType = MongoMappingFixtures.LongBox.class;
            final java.time.LocalDate day = MongoMappingFixtures.LOCAL_DAY;

            Assertions.assertAll(() -> MongoMappingFixtures.assertEntity("list", entityAsync.list(filter, type).get().get(0)),
                    () -> MongoMappingFixtures.assertEntity("findFirst", entityAsync.findFirst(filter, type).get().get()),
                    () -> MongoMappingFixtures.assertEntity("get(ObjectId)", entityAsync.get(new ObjectId(MongoMappingFixtures.OID_HEX), type).get().get()),
                    () -> {
                        try (Stream<MongoMappingFixtures.MappedEntity> stream = entityAsync.stream(filter, type).get()) {
                            MongoMappingFixtures.assertEntity("stream", stream.toList().get(0));
                        }
                    }, () -> MongoMappingFixtures.assertEntity("findOneAndUpdate", entityAsync.findOneAndUpdate(filter, update, type).get()),
                    () -> MongoMappingFixtures.assertEntityDataset("query(Bson, Class)", entityAsync.query(filter, type).get()),
                    () -> Assertions.assertEquals(MongoMappingFixtures.expectedRecord(), recordAsync.list(filter, recordType).get().get(0), "record list"),
                    () -> Assertions.assertEquals(MongoMappingFixtures.expectedRecord(), recordAsync.findFirst(filter, recordType).get().get(),
                            "record findFirst"),
                    () -> Assertions.assertEquals((Object) 9L, boxAsync.list(filter, boxType).get().get(0).getValue(), "LongBox list"),
                    () -> Assertions.assertEquals((Object) 9L, boxAsync.findFirst(filter, boxType).get().get().getValue(), "LongBox findFirst"),
                    () -> Assertions.assertEquals(day, entityAsync.queryForSingleValue("day", filter, java.time.LocalDate.class).get().get(),
                            "queryForSingleValue LocalDate"),
                    () -> Assertions.assertEquals(MongoMappingFixtures.LOCAL_TIME,
                            entityAsync.queryForSingleValue("time", filter, java.time.LocalTime.class).get().get(), "queryForSingleValue LocalTime"),
                    () -> Assertions.assertEquals(day, entityAsync.queryForSingleNonNull("day", filter, java.time.LocalDate.class).get().get(),
                            "queryForSingleNonNull LocalDate"),
                    () -> Assertions.assertEquals(Arrays.asList(day), dayAsync.list(filter, java.time.LocalDate.class).get(), "single-field row list"));
        } finally {
            realExecutor.shutdown();
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testQueryDottedSelectNameThroughAsyncFillsNestedColumn() throws Exception {
        // The async Collection-projection Dataset overloads run the sync query, so a dotted select name comes back as the
        // nested value (it was an all-null column) and a path through an array fails the future with ClassCastException.
        final MongoCollection<Document> collection = mock(MongoCollection.class);
        final com.mongodb.client.FindIterable<Document> findIterable = mock(com.mongodb.client.FindIterable.class);
        when(collection.find(any(Bson.class))).thenReturn(findIterable);
        when(findIterable.projection(any())).thenReturn(findIterable);
        when(findIterable.sort(any())).thenReturn(findIterable);
        when(findIterable.skip(org.mockito.ArgumentMatchers.anyInt())).thenReturn(findIterable);
        when(findIterable.limit(org.mockito.ArgumentMatchers.anyInt())).thenReturn(findIterable);
        final List<Document> rows = new java.util.concurrent.CopyOnWriteArrayList<>(Arrays.asList(
                new Document("_id", 1).append("name", "n1").append("address", new Document("city", "Paris")), new Document("_id", 2).append("name", "n2")));
        doAnswer(invocation -> {
            final java.util.Collection<Object> target = invocation.getArgument(0);
            target.addAll(rows);
            return target;
        }).when(findIterable).into(any());

        final AsyncExecutor realExecutor = new AsyncExecutor(1, 1, 0L, TimeUnit.SECONDS);

        try {
            final AsyncMongoCollectionExecutor async = new MongoCollectionExecutor(collection, realExecutor).async();
            final List<String> names = Arrays.asList("name", "address.city");
            final Bson filter = new Document();
            final Bson sort = new Document("_id", 1);

            for (final ContinuableFuture<Dataset> future : Arrays.asList(async.query(names, filter, java.util.Map.class),
                    async.query(names, filter, 1, 5, Document.class), async.query(names, filter, sort, java.util.Map.class),
                    async.query(names, filter, sort, 1, 5, java.util.Map.class))) {
                final Dataset ds = future.get();
                Assertions.assertEquals(names, ds.columnNames());
                Assertions.assertEquals(Arrays.asList("n1", "n2"), ds.getColumn("name"));
                Assertions.assertEquals(Arrays.asList("Paris", null), ds.getColumn("address.city"));
            }

            rows.add(new Document("_id", 3).append("name", "n3").append("address", Arrays.asList(new Document("city", "Rome"))));
            final ExecutionException e = Assertions.assertThrows(ExecutionException.class,
                    () -> async.query(names, filter, sort, 0, 5, java.util.Map.class).get());
            Assertions.assertInstanceOf(ClassCastException.class, e.getCause());
        } finally {
            realExecutor.shutdown();
        }
    }
}
