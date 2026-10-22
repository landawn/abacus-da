package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;

/**
 * Pins the generated-{@code _id} write-back contract documented on insertOne/insertMany/bulkInsert of the blocking executor,
 * the async executor and the blocking mapper, against a live MongoDB (skipped when none is reachable): a caller-supplied
 * {@link Document} is inserted as-is and receives the generated {@code _id} in place, while a Map or a bean is converted to a
 * new document and is left untouched (the id is only reported by the result).
 */
public class MongoCollectionExecutorDeepReview2026GeneratedIdWriteBackLiveTest {

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
    public void syncExecutorWritesGeneratedIdBackOnlyIntoDocuments() {
        try (MongoClient client = MongoLiveTestSupport.documentsClient()) {
            final MongoCollection<Document> coll = client.getDatabase("test").getCollection(collectionName());

            try {
                final MongoCollectionExecutor executor = new MongoCollectionExecutor(coll, MongoDBBase.DEFAULT_ASYNC_EXECUTOR);

                // insertOne
                Document doc = newDocument();
                Map<String, Object> map = newMap();
                Account bean = newBean();

                final InsertOneResult one = executor.insertOne(doc);
                assertEquals(one.getInsertedId().asObjectId().getValue(), doc.get("_id"));
                executor.insertOne(map);
                assertUntouched(map);
                executor.insertOne(bean);
                assertUntouched(bean);

                // insertMany: a heterogeneous batch still keeps the caller's Document instance.
                doc = newDocument();
                map = newMap();
                bean = newBean();
                final InsertManyResult many = executor.insertMany(List.of(doc, map, bean));
                assertEquals(3, many.getInsertedIds().size());
                assertEquals(many.getInsertedIds().get(0).asObjectId().getValue(), doc.get("_id"));
                assertUntouched(map);
                assertUntouched(bean);

                // bulkInsert
                doc = newDocument();
                map = newMap();
                bean = newBean();
                final BulkWriteResult bulk = executor.bulkInsert(List.of(doc, map, bean));
                assertEquals(3, bulk.getInserts().size());
                assertEquals(bulk.getInserts().get(0).getId().asObjectId().getValue(), doc.get("_id"));
                assertUntouched(map);
                assertUntouched(bean);

                assertEquals(9, coll.countDocuments());
            } finally {
                coll.drop();
            }
        }
    }

    @Test
    public void asyncExecutorWritesGeneratedIdBackOnlyIntoDocuments() throws Exception {
        try (MongoClient client = MongoLiveTestSupport.documentsClient()) {
            final MongoCollection<Document> coll = client.getDatabase("test").getCollection(collectionName());

            try {
                final MongoCollectionExecutor executor = new MongoCollectionExecutor(coll, MongoDBBase.DEFAULT_ASYNC_EXECUTOR);
                final AsyncMongoCollectionExecutor async = new AsyncMongoCollectionExecutor(executor, MongoDBBase.DEFAULT_ASYNC_EXECUTOR);

                Document doc = newDocument();
                Map<String, Object> map = newMap();
                Account bean = newBean();
                final InsertOneResult one = async.insertOne(doc).get();
                assertEquals(one.getInsertedId().asObjectId().getValue(), doc.get("_id"));
                async.insertOne(map).get();
                assertUntouched(map);
                async.insertOne(bean).get();
                assertUntouched(bean);

                doc = newDocument();
                map = newMap();
                bean = newBean();
                final InsertManyResult many = async.insertMany(List.of(doc, map, bean)).get();
                assertEquals(many.getInsertedIds().get(0).asObjectId().getValue(), doc.get("_id"));
                assertUntouched(map);
                assertUntouched(bean);

                doc = newDocument();
                map = newMap();
                bean = newBean();
                final BulkWriteResult bulk = async.bulkInsert(List.of(doc, map, bean)).get();
                assertEquals(bulk.getInserts().get(0).getId().asObjectId().getValue(), doc.get("_id"));
                assertUntouched(map);
                assertUntouched(bean);

                assertEquals(9, coll.countDocuments());
            } finally {
                coll.drop();
            }
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void mapperWritesGeneratedIdBackOnlyIntoDocuments() {
        try (MongoClient client = MongoLiveTestSupport.documentsClient()) {
            final MongoCollection<Document> coll = client.getDatabase("test").getCollection(collectionName());

            try {
                final MongoCollectionExecutor executor = new MongoCollectionExecutor(coll, MongoDBBase.DEFAULT_ASYNC_EXECUTOR);
                final MongoCollectionMapper<Document> documentMapper = new MongoCollectionMapper<>(executor, Document.class);
                final MongoCollectionMapper<Map> mapMapper = new MongoCollectionMapper<>(executor, Map.class);
                final MongoCollectionMapper<Account> beanMapper = new MongoCollectionMapper<>(executor, Account.class);

                Document doc = newDocument();
                Map<String, Object> map = newMap();
                Account bean = newBean();
                assertEquals(documentMapper.insertOne(doc).getInsertedId().asObjectId().getValue(), doc.get("_id"));
                mapMapper.insertOne(map);
                assertUntouched(map);
                beanMapper.insertOne(bean);
                assertUntouched(bean);

                doc = newDocument();
                map = newMap();
                bean = newBean();
                assertEquals(documentMapper.insertMany(List.of(doc)).getInsertedIds().get(0).asObjectId().getValue(), doc.get("_id"));
                mapMapper.insertMany(List.of(map));
                assertUntouched(map);
                beanMapper.insertMany(List.of(bean));
                assertUntouched(bean);

                doc = newDocument();
                map = newMap();
                bean = newBean();
                assertEquals(documentMapper.bulkInsert(List.of(doc)).getInserts().get(0).getId().asObjectId().getValue(), doc.get("_id"));
                mapMapper.bulkInsert(List.of(map));
                assertUntouched(map);
                beanMapper.bulkInsert(List.of(bean));
                assertUntouched(bean);

                assertEquals(9, coll.countDocuments());
            } finally {
                coll.drop();
            }
        }
    }

    static Document newDocument() {
        return new Document("name", "document");
    }

    static Map<String, Object> newMap() {
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("name", "map");
        return map;
    }

    static Account newBean() {
        final Account bean = new Account();
        bean.setName("bean");
        return bean;
    }

    static void assertUntouched(final Map<String, Object> map) {
        assertFalse(map.containsKey("_id"), () -> "a converted Map must not receive the generated _id: " + map);
        assertEquals(Map.of("name", "map"), map);
    }

    static void assertUntouched(final Account bean) {
        assertNull(bean.getId(), "a converted bean must not receive the generated _id");
    }

    private static String collectionName() {
        return "abacus_da_generated_id_write_back_" + UUID.randomUUID().toString().replace("-", "");
    }
}
