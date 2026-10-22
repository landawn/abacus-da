package com.landawn.abacus.da.mongodb.reactivestreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.mongodb.MongoLiveTestSupport;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.reactivestreams.client.MongoClients;

import reactor.core.publisher.Mono;

/**
 * Reactive counterpart of the generated-{@code _id} write-back contract, against a live MongoDB (skipped when none is
 * reachable): a caller-supplied {@link Document} receives the generated {@code _id} in place when the publisher is built
 * (at call time, before subscription), while a Map or a bean is converted to a new document and is left untouched.
 */
public class ReactiveMongoCollectionExecutorDeepReview2026GeneratedIdWriteBackLiveTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    public static class Account {
        private String id;
        private String name;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    @Test
    public void executorStampsDocumentsBeforeSubscriptionAndLeavesMapsAndBeansUntouched() {
        try (MongoClient syncClient = MongoLiveTestSupport.documentsClient();
             com.mongodb.reactivestreams.client.MongoClient reactiveClient = MongoClients.create(MongoLiveTestSupport.clientSettings())) {
            final String name = collectionName();
            final com.mongodb.client.MongoCollection<Document> syncColl = syncClient.getDatabase("test").getCollection(name);

            try {
                final MongoCollectionExecutor executor = new MongoCollectionExecutor(reactiveClient.getDatabase("test").getCollection(name));

                // insertOne
                Document doc = newDocument();
                Map<String, Object> map = newMap();
                Account bean = newBean();

                final Mono<InsertOneResult> one = executor.insertOne(doc);
                final Object idBeforeSubscription = doc.get("_id");
                assertNotNull(idBeforeSubscription, "the driver stamps the generated _id when the publisher is built, before subscription");
                assertEquals(ObjectId.class, idBeforeSubscription.getClass());
                assertEquals(idBeforeSubscription, one.block(TIMEOUT).getInsertedId().asObjectId().getValue());
                executor.insertOne(map).block(TIMEOUT);
                assertUntouched(map);
                executor.insertOne(bean).block(TIMEOUT);
                assertUntouched(bean);

                // insertMany: a heterogeneous batch still keeps the caller's Document instance.
                doc = newDocument();
                map = newMap();
                bean = newBean();
                final Mono<InsertManyResult> many = executor.insertMany(List.of(doc, map, bean));
                final Object manyIdBeforeSubscription = doc.get("_id");
                assertNotNull(manyIdBeforeSubscription);
                assertEquals(manyIdBeforeSubscription, many.block(TIMEOUT).getInsertedIds().get(0).asObjectId().getValue());
                assertUntouched(map);
                assertUntouched(bean);

                // bulkInsert
                doc = newDocument();
                map = newMap();
                bean = newBean();
                final Mono<BulkWriteResult> bulk = executor.bulkInsert(List.of(doc, map, bean));
                final Object bulkIdBeforeSubscription = doc.get("_id");
                assertNotNull(bulkIdBeforeSubscription);
                assertEquals(bulkIdBeforeSubscription, bulk.block(TIMEOUT).getInserts().get(0).getId().asObjectId().getValue());
                assertUntouched(map);
                assertUntouched(bean);

                assertEquals(9, syncColl.countDocuments());
            } finally {
                syncColl.drop();
            }
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void mapperStampsDocumentsBeforeSubscriptionAndLeavesMapsAndBeansUntouched() {
        try (MongoClient syncClient = MongoLiveTestSupport.documentsClient();
             com.mongodb.reactivestreams.client.MongoClient reactiveClient = MongoClients.create(MongoLiveTestSupport.clientSettings())) {
            final String name = collectionName();
            final com.mongodb.client.MongoCollection<Document> syncColl = syncClient.getDatabase("test").getCollection(name);

            try {
                final MongoCollectionExecutor executor = new MongoCollectionExecutor(reactiveClient.getDatabase("test").getCollection(name));
                final MongoCollectionMapper<Document> documentMapper = new MongoCollectionMapper<>(executor, Document.class);
                final MongoCollectionMapper<Map> mapMapper = new MongoCollectionMapper<>(executor, Map.class);
                final MongoCollectionMapper<Account> beanMapper = new MongoCollectionMapper<>(executor, Account.class);

                Document doc = newDocument();
                Map<String, Object> map = newMap();
                Account bean = newBean();
                final Mono<InsertOneResult> one = documentMapper.insertOne(doc);
                final Object idBeforeSubscription = doc.get("_id");
                assertNotNull(idBeforeSubscription);
                assertEquals(idBeforeSubscription, one.block(TIMEOUT).getInsertedId().asObjectId().getValue());
                mapMapper.insertOne(map).block(TIMEOUT);
                assertUntouched(map);
                beanMapper.insertOne(bean).block(TIMEOUT);
                assertUntouched(bean);

                doc = newDocument();
                map = newMap();
                bean = newBean();
                final Mono<InsertManyResult> many = documentMapper.insertMany(List.of(doc));
                assertNotNull(doc.get("_id"));
                assertEquals(doc.get("_id"), many.block(TIMEOUT).getInsertedIds().get(0).asObjectId().getValue());
                mapMapper.insertMany(List.of(map)).block(TIMEOUT);
                assertUntouched(map);
                beanMapper.insertMany(List.of(bean)).block(TIMEOUT);
                assertUntouched(bean);

                doc = newDocument();
                map = newMap();
                bean = newBean();
                final Mono<BulkWriteResult> bulk = documentMapper.bulkInsert(List.of(doc));
                assertNotNull(doc.get("_id"));
                assertEquals(doc.get("_id"), bulk.block(TIMEOUT).getInserts().get(0).getId().asObjectId().getValue());
                mapMapper.bulkInsert(List.of(map)).block(TIMEOUT);
                assertUntouched(map);
                beanMapper.bulkInsert(List.of(bean)).block(TIMEOUT);
                assertUntouched(bean);

                assertEquals(9, syncColl.countDocuments());
            } finally {
                syncColl.drop();
            }
        }
    }

    private static Document newDocument() {
        return new Document("name", "document");
    }

    private static Map<String, Object> newMap() {
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("name", "map");
        return map;
    }

    private static Account newBean() {
        final Account bean = new Account();
        bean.setName("bean");
        return bean;
    }

    private static void assertUntouched(final Map<String, Object> map) {
        assertFalse(map.containsKey("_id"), () -> "a converted Map must not receive the generated _id: " + map);
        assertEquals(Map.of("name", "map"), map);
    }

    private static void assertUntouched(final Account bean) {
        assertNull(bean.getId(), "a converted bean must not receive the generated _id");
    }

    private static String collectionName() {
        return "abacus_da_reactive_generated_id_write_back_" + UUID.randomUUID().toString().replace("-", "");
    }
}
