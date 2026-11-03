package com.landawn.abacus.da.mongodb.reactivestreams;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import com.landawn.abacus.da.mongodb.MongoLiveTestSupport;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.Updates;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Review 2026-11-02 (reactive streams executor, driver 5.14 / reactor-core 3.8.8). Live tests, skipped when no MongoDB is
 * reachable on localhost:27017; each uses its own database and drops it.
 */
public class MongoCollectionExecutorReview20261102Test {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private static String dbName() {
        return "rx_review_20261102_" + UUID.randomUUID().toString().replace("-", "");
    }

    private static void requireLiveMongo() {
        // Skips (assumption) when no server is reachable.
        MongoLiveTestSupport.documentsClient().close();
    }

    private static <T extends Throwable> T signalled(final Class<T> type, final Supplier<? extends Publisher<?>> call) {
        final Publisher<?> publisher = call.get(); // must not throw: the failure belongs to the subscription

        return assertThrows(type, () -> Flux.from(publisher).collectList().block(TIMEOUT));
    }

    /**
     * Pins the class-level contract on a closed {@code MongoClient} under driver 5.14: counts and writes build their driver
     * operation when called and throw there; find-based reads, distinct, aggregate, grouping, mapReduce and watch return their
     * publisher and signal on subscription.
     */
    @Test
    @SuppressWarnings("deprecation")
    public void testClosedClientThrowsAtCallForCountsAndWritesButSignalsReadsOnSubscription() {
        requireLiveMongo();

        final MongoClient client = MongoClients.create(MongoLiveTestSupport.clientSettings());
        final MongoCollectionExecutor executor = new MongoDB(client.getDatabase(dbName())).collectionExecutor("c");
        client.close();

        assertThrows(IllegalStateException.class, () -> executor.exists(Filters.empty()));
        assertThrows(IllegalStateException.class, () -> executor.count());
        assertThrows(IllegalStateException.class, () -> executor.count(Filters.empty()));
        assertThrows(IllegalStateException.class, () -> executor.estimatedDocumentCount());
        assertThrows(IllegalStateException.class, () -> executor.insertOne(new Document("a", 1)));
        assertThrows(IllegalStateException.class, () -> executor.insertMany(List.of(new Document("a", 1))));
        assertThrows(IllegalStateException.class, () -> executor.updateOne(Filters.empty(), Updates.set("a", 1)));
        assertThrows(IllegalStateException.class, () -> executor.updateMany(Filters.empty(), List.of(Updates.set("a", 1))));
        assertThrows(IllegalStateException.class, () -> executor.replaceOne(Filters.empty(), new Document("a", 1)));
        assertThrows(IllegalStateException.class, () -> executor.deleteOne(Filters.empty()));
        assertThrows(IllegalStateException.class, () -> executor.deleteMany(Filters.empty()));
        assertThrows(IllegalStateException.class, () -> executor.bulkInsert(List.of(new Document("a", 1))));
        assertThrows(IllegalStateException.class, () -> executor.bulkWrite(List.of(new InsertOneModel<>(new Document("a", 1)))));
        assertThrows(IllegalStateException.class, () -> executor.findOneAndUpdate(Filters.empty(), Updates.set("a", 1)));
        assertThrows(IllegalStateException.class, () -> executor.findOneAndReplace(Filters.empty(), new Document("a", 1)));
        assertThrows(IllegalStateException.class, () -> executor.findOneAndDelete(Filters.empty()));

        signalled(IllegalStateException.class, () -> executor.get(new ObjectId()));
        signalled(IllegalStateException.class, () -> executor.findFirst(Filters.empty()));
        signalled(IllegalStateException.class, () -> executor.list(Filters.empty()));
        signalled(IllegalStateException.class, () -> executor.query(Filters.empty()));
        signalled(IllegalStateException.class, () -> executor.queryForInt("a", Filters.empty()));
        signalled(IllegalStateException.class, () -> executor.distinct("a", Integer.class));
        signalled(IllegalStateException.class, () -> executor.aggregate(List.of()));
        signalled(IllegalStateException.class, () -> executor.groupBy("a"));
        signalled(IllegalStateException.class, () -> executor.groupByAndCount(List.of("a", "b")));
        signalled(IllegalStateException.class, () -> executor.mapReduce("function() { emit(1, 1); }", "function(k, v) { return 1; }"));
        signalled(IllegalStateException.class, () -> executor.watch());
    }

    /**
     * Pins the codec-timing contract of the class Javadoc and {@code MongoDB.collectionExecutor(MongoCollection)} on a collection
     * with the driver's default registry: a driver-built expression is encoded when the operation is built (thrown at call
     * time), while a converted entity and a caller-supplied {@link Document} are encoded on subscription.
     */
    @Test
    public void testCodecFailureTimingOnACollectionWithoutTheFrameworkRegistry() {
        requireLiveMongo();

        final String dbName = dbName();

        try (MongoClient client = MongoClients.create(MongoLiveTestSupport.clientSettings())) {
            try {
                final MongoDB mongoDB = new MongoDB(client.getDatabase(dbName));
                final MongoCollectionExecutor executor = mongoDB.collectionExecutor(client.getDatabase(dbName).getCollection("c"));
                final Item noCodec = new Item();

                assertThrows(CodecConfigurationException.class, () -> executor.count(Filters.eq("item", noCodec)));
                assertThrows(CodecConfigurationException.class, () -> executor.updateOne(Filters.empty(), Updates.set("item", noCodec)));

                signalled(CodecConfigurationException.class, () -> executor.count(new Document("item", noCodec)));
                signalled(CodecConfigurationException.class, () -> executor.list(Filters.eq("item", noCodec)));
                signalled(CodecConfigurationException.class, () -> executor.insertOne(new Holder()));
                signalled(CodecConfigurationException.class, () -> executor.updateOne(Filters.empty(), new Holder()));
            } finally {
                Mono.from(client.getDatabase(dbName).drop()).block(TIMEOUT);
            }
        }
    }

    /**
     * The reactive row conversion delegates to {@code MongoDBBase.readRow} through {@link MongoDB#readRow(Document, Class)}, so a
     * {@link BsonDocument} row type (which used to fail with {@code ClassCastException}, "ObjectId cannot be cast to BsonValue")
     * now works on every reactive path that converts rows: find-based reads, aggregate, grouping and findOneAndXxx.
     */
    @Test
    public void testBsonDocumentRowTypeOnEveryReactiveConversionPath() {
        requireLiveMongo();

        final String dbName = dbName();

        try (MongoClient client = MongoClients.create(MongoLiveTestSupport.clientSettings())) {
            try {
                final MongoCollectionExecutor executor = new MongoDB(client.getDatabase(dbName)).collectionExecutor("c");
                final ObjectId id = new ObjectId();
                executor.insertOne(new Document("_id", id).append("a", 1).append("b", new Document("c", "x"))).block(TIMEOUT);

                final BsonDocument listed = executor.list(Filters.empty(), BsonDocument.class).blockFirst(TIMEOUT);
                assertNotNull(listed);
                assertEquals(id, listed.getObjectId("_id").getValue());
                assertEquals(new BsonInt32(1), listed.get("a"));
                assertEquals("x", listed.getDocument("b").getString("c").getValue());

                assertEquals(listed, executor.findFirst(Filters.eq("a", 1), BsonDocument.class).block(TIMEOUT));
                assertEquals(listed, executor.get(id, BsonDocument.class).block(TIMEOUT));
                assertEquals(List.of(listed),
                        executor.aggregate(List.of(new Document("$match", new Document("a", 1))), BsonDocument.class).collectList().block(TIMEOUT));
                assertEquals(new BsonDocument("a", new BsonInt32(1)), executor.groupBy("a", BsonDocument.class).blockFirst(TIMEOUT));
                assertEquals(new BsonDocument("count", new BsonInt32(1)).append("a", new BsonInt32(1)),
                        executor.groupByAndCount("a", BsonDocument.class).blockFirst(TIMEOUT));
                assertEquals(listed, executor.findOneAndUpdate(Filters.eq("a", 1), Updates.set("d", 2), BsonDocument.class).block(TIMEOUT));
            } finally {
                Mono.from(client.getDatabase(dbName).drop()).block(TIMEOUT);
            }
        }
    }

    /**
     * Pins the Javadoc of the terminal {@code list} overloads: an object-array or collection row type receives the
     * document's values positionally, {@code _id} included (the blocking {@code list} reads the sole non-{@code _id} field).
     */
    @Test
    public void testListIntoObjectArrayOrCollectionIsPositional() {
        requireLiveMongo();

        final String dbName = dbName();

        try (MongoClient client = MongoClients.create(MongoLiveTestSupport.clientSettings())) {
            try {
                final MongoCollectionExecutor executor = new MongoDB(client.getDatabase(dbName)).collectionExecutor("c");
                executor.insertOne(new Document("_id", 7).append("a", 1)).block(TIMEOUT);

                assertArrayEquals(new Object[] { 7, 1 }, executor.list(List.of("a"), Filters.empty(), Object[].class).blockFirst(TIMEOUT));
                assertEquals(List.of(7, 1), executor.list(List.of("a"), Filters.empty(), List.class).blockFirst(TIMEOUT));
                // Not just a List: the sole-field reading would also give one ([1]), so pin the positional values.
                assertEquals(List.of(7, 1), executor.list(new Document("a", 1), Filters.empty(), null, List.class).blockFirst(TIMEOUT));
            } finally {
                Mono.from(client.getDatabase(dbName).drop()).block(TIMEOUT);
            }
        }
    }

    /** A bean the driver's default registry has no codec for. */
    public static class Item {
        private String x = "v";

        public String getX() {
            return x;
        }

        public void setX(final String x) {
            this.x = x;
        }
    }

    /** A bean whose {@code String[]} property the driver's default registry cannot encode. */
    public static class Holder {
        private String name = "n";
        private String[] tags = { "a", "b" };

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public String[] getTags() {
            return tags;
        }

        public void setTags(final String[] tags) {
            this.tags = tags;
        }
    }
}
