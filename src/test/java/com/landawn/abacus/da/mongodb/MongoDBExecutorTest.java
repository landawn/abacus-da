/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.mongodb;

import static com.landawn.abacus.da.mongodb.MongoDBBase._ID;
import static com.landawn.abacus.da.mongodb.MongoDBBase.fromJson;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.AbstractNoSQLTest;
import com.landawn.abacus.da.Account;
import com.landawn.abacus.util.Beans;
import com.landawn.abacus.util.Clazz;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.Fn;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Strings;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;

public class MongoDBExecutorTest extends AbstractNoSQLTest {
    static final MongoClient mongoClient = MongoClients.create(MongoClientSettings.builder().uuidRepresentation(UuidRepresentation.STANDARD).build());
    static final MongoDatabase mongoDB = mongoClient.getDatabase("test");
    static final String collectionName = "account";
    static final MongoDB dbExecutor = new MongoDB(mongoDB);
    static final MongoCollectionExecutor collExecutor = dbExecutor.collExecutor(collectionName);
    static final AsyncMongoCollectionExecutor asyncCollExecutor = collExecutor.async();

    @Test
    public void test_collection() {
        Account account = createAccount();
        collExecutor.insertOne(account);

        MongoCollection<Account> collection = dbExecutor.collection(collectionName, Account.class);

        FindIterable<Account> it = collection.find();

        it.forEach(Fn.println());
    }

    @Test
    public void test_util() {
        Account account = createAccount();
        collExecutor.insertOne(account);

        MongoCollection<Document> collection = collExecutor.coll();

        Bson filter = new Document("lastName", account.getLastName());
        FindIterable<Document> findIterable = collection.find(filter);

        Dataset dataset = MongoDB.extractData(findIterable);
        dataset.println();

        findIterable = collection.find(filter).projection(MongoDB.toBson(_ID, 0));
        dataset = MongoDB.extractData(findIterable, Account.class);
        dataset.println();

        findIterable = collection.find(filter).projection(MongoDB.toBson(_ID, 0));
        Account dbAccount = MongoDB.readRow(findIterable.first(), Account.class);
        N.println(dbAccount);

        Document doc = MongoDB.toDocument(dbAccount);
        N.println(doc);

        BSONObject bsonObject = MongoDB.toBSONObject(account);
        N.println(bsonObject);

        bsonObject = MongoDB.toBSONObject(Beans.deepBeanToMap(account));
        N.println(bsonObject);
    }

    @Test
    public void test_distinct() {
        collExecutor.coll().drop();

        Account account = createAccount();
        collExecutor.insertOne(account);
        collExecutor.insertOne(createAccount());
        account.setId(generateId());
        collExecutor.insertOne(account);

        List<String> firstNameList = collExecutor.distinct("firstName", String.class).toList();
        N.println(firstNameList);

        collExecutor.deleteMany(Filters.eq("firstName", account.getFirstName()));
    }

    @Test
    public void test_groupBy() {
        collExecutor.coll().drop();

        Account account = createAccount();
        collExecutor.insertOne(account);
        collExecutor.insertOne(createAccount());
        account.setId(generateId());
        collExecutor.insertOne(account);
        account.setId(generateId());
        account.setFirstName("firstName123");
        collExecutor.insertOne(account);

        collExecutor.groupBy("firstName").println();

        collExecutor.groupByAndCount("firstName").println();

        collExecutor.groupBy(N.asList("firstName")).println();

        collExecutor.groupByAndCount(N.asList("firstName")).println();

        collExecutor.groupBy(N.asList("firstName", "lastName")).println();

        collExecutor.groupByAndCount(N.asList("firstName", "lastName")).println();

        collExecutor.deleteMany(Filters.eq("firstName", account.getFirstName()));
    }

    @Test
    public void test_aggregate() {
        collExecutor.coll().drop();

        Account account = createAccount();
        collExecutor.insertOne(account);
        collExecutor.insertOne(createAccount());
        account.setId(generateId());
        collExecutor.insertOne(account);

        List<Bson> pipeline = N.toList();

        pipeline.add(fromJson("{$match : {firstName : '" + account.getFirstName() + "'}}", Bson.class));
        pipeline.add(fromJson("{$group : {_id : $firstName, total : {$sum : $status}}}", Bson.class));

        List<Document> resultList = collExecutor.aggregate(pipeline).toList();
        N.println(resultList);

        collExecutor.deleteMany(Filters.eq("firstName", account.getFirstName()));
    }

    @Test
    public void test_mapReduce() {
        collExecutor.coll().drop();

        Account account = createAccount();
        collExecutor.insertOne(account);
        collExecutor.insertOne(createAccount());
        account.setId(generateId());
        collExecutor.insertOne(account);

        List<Bson> pipeline = N.toList();
        pipeline.add(fromJson("{$match : {firstName : '" + account.getFirstName() + "'}}", Bson.class));
        pipeline.add(fromJson("{$group : {_id : $firstName, total : {$sum : $status}}}", Bson.class));

        String mapFunction = "function() {emit(this.firstName, this.status)}";
        String reduceFunction = "function(key, values) { return Array.sum(values)}";

        List<Document> resultList = collExecutor.mapReduce(mapFunction, reduceFunction).toList();
        N.println(resultList);

        List<Map<String, Object>> mapList = collExecutor.mapReduce(mapFunction, reduceFunction, Clazz.PROPS_MAP).toList();
        N.println(mapList);

        collExecutor.deleteMany(Filters.eq("firstName", account.getFirstName()));
    }

    @Test
    public void test_exist_count_get() {
        collExecutor.deleteMany(Filters.ne("lastName", Strings.uuid()));

        Account account = createAccount();
        collExecutor.insertOne(account);

        Document doc = collExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        ObjectId objectId = doc.getObjectId(_ID);

        assertTrue(collExecutor.exists(objectId.toString()));
        assertTrue(collExecutor.exists(objectId));

        assertTrue(collExecutor.exists(Filters.eq(_ID, objectId)));
        assertFalse(collExecutor.exists(Filters.ne(_ID, objectId)));
        assertTrue(collExecutor.exists(Filters.eq("lastName", account.getLastName())));

        assertEquals(1, collExecutor.count(Filters.eq(_ID, objectId)));
        assertEquals(0, collExecutor.count(Filters.ne(_ID, objectId)));
        assertEquals(1, collExecutor.count(Filters.eq("lastName", account.getLastName())));

        assertEquals(objectId, collExecutor.gett(objectId.toString()).getObjectId(_ID));
        assertEquals(objectId, collExecutor.gett(objectId).getObjectId(_ID));

        String firstName = account.getFirstName();
        assertEquals(firstName, collExecutor.gett(objectId.toString(), Account.class).getFirstName());
        assertEquals(firstName, collExecutor.gett(objectId, Account.class).getFirstName());

        List<Document> result = collExecutor.list(Filters.eq("lastName", account.getLastName()), Document.class);

        N.println(result);

        collExecutor.deleteOne(objectId);
    }

    @Test
    public void test_exist_count_get_2() {
        collExecutor.deleteMany(Filters.ne("lastName", Strings.uuid()));

        Account account = createAccount();
        collExecutor.insertOne(account);

        Document doc = collExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        ObjectId objectId = doc.getObjectId(_ID);

        assertTrue(collExecutor.exists(objectId.toString()));
        assertTrue(collExecutor.exists(objectId));

        assertTrue(collExecutor.exists(Filters.eq(_ID, objectId)));
        assertFalse(collExecutor.exists(Filters.ne(_ID, objectId)));
        assertTrue(collExecutor.exists(Filters.eq("lastName", account.getLastName())));

        assertEquals(1, collExecutor.count(Filters.eq(_ID, objectId)));
        assertEquals(0, collExecutor.count(Filters.ne(_ID, objectId)));
        assertEquals(1, collExecutor.count(Filters.eq("lastName", account.getLastName())));

        assertEquals(objectId, collExecutor.gett(objectId.toString()).getObjectId(_ID));
        assertEquals(objectId, collExecutor.gett(objectId).getObjectId(_ID));

        String firstName = account.getFirstName();
        assertEquals(firstName, collExecutor.gett(objectId.toString(), Account.class).getFirstName());
        assertEquals(firstName, collExecutor.gett(objectId, Account.class).getFirstName());

        List<Document> result = collExecutor.list(Filters.eq("lastName", account.getLastName()), Document.class);

        N.println(result);

        collExecutor.deleteOne(objectId);
    }

    @Test
    public void test_insertOne() {
        collExecutor.deleteMany(Filters.ne("lastName", Strings.uuid()));

        Map<String, Object> m = N.asMap("lastName", Strings.uuid(), "firstName", Strings.uuid());
        m.put("props", N.asMap("prop1", 1, "prop2", 2));

        collExecutor.insertOne(m);

        Document doc = collExecutor.findFirst(Filters.eq("lastName", m.get("lastName"))).orElse(null);
        N.println(doc);

        collExecutor.deleteMany(Filters.eq("lastName", m.get("lastName")));
    }

    @Test
    public void test_query() {
        collExecutor.deleteMany(Filters.ne("lastName", Strings.uuid()));

        Account account = createAccount();
        collExecutor.insertOne(account);

        Document doc = collExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        N.println(doc);

        ObjectId objectId = doc.getObjectId(MongoDB._ID);
        Bson filter = Filters.eq("firstName", account.getFirstName());

        assertEquals(objectId, collExecutor.findFirst(filter).orElse(null).getObjectId(_ID));
        assertEquals(account.getFirstName(), collExecutor.findFirst(filter).orElse(null).get("firstName"));
        assertEquals(account.getFirstName(), collExecutor.findFirst(filter, Account.class).orElse(null).getFirstName());

        List<Document> docList = collExecutor.list(filter);
        assertEquals(account.getFirstName(), docList.get(0).get("firstName"));

        docList = collExecutor.list(N.asList("lastName"), filter, Document.class);

        collExecutor.list(N.asList("lastName"), filter, String.class).forEach(Fn.println());

        assertNull(docList.get(0).get("firstName"));
        assertEquals(account.getLastName(), docList.get(0).get("lastName"));

        List<Account> accountList = collExecutor.list(filter, Account.class);
        assertEquals(account.getFirstName(), accountList.get(0).getFirstName());

        accountList = collExecutor.list(N.asList("lastName"), filter, Account.class);

        assertNull(accountList.get(0).getFirstName());
        assertEquals(account.getLastName(), accountList.get(0).getLastName());

        Dataset dataset = collExecutor.query(filter);
        assertEquals(account.getFirstName(), dataset.get("firstName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        dataset = collExecutor.query(N.asList("lastName", "birthDate"), filter, Document.class);

        assertFalse(dataset.containsColumn("firstName"));
        assertEquals(account.getLastName(), dataset.get("lastName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        dataset = collExecutor.query(filter, Account.class);
        assertEquals(account.getFirstName(), dataset.get("firstName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        dataset = collExecutor.query(N.asList("lastName", "birthDate"), filter, Account.class);

        assertFalse(dataset.containsColumn("firstName"));
        N.println(dataset);
        assertEquals(account.getLastName(), dataset.get("lastName"));

        // ########################################################################
        Bson projection = Projections.include("id", "firstName", "lastName");

        assertEquals(objectId, collExecutor.findFirst(filter).orElse(null).getObjectId(_ID));
        assertEquals(account.getFirstName(), collExecutor.findFirst(filter).orElse(null).get("firstName"));
        assertEquals(account.getFirstName(), collExecutor.findFirst(projection, filter, null, Account.class).orElse(null).getFirstName());

        docList = collExecutor.list(filter);
        assertEquals(account.getFirstName(), docList.get(0).get("firstName"));

        projection = Projections.include("id", "lastName");
        docList = collExecutor.list(projection, filter, null, Document.class);

        assertNull(docList.get(0).get("firstName"));
        assertEquals(account.getLastName(), docList.get(0).get("lastName"));

        accountList = collExecutor.list(filter, Account.class);
        assertEquals(account.getFirstName(), accountList.get(0).getFirstName());

        accountList = collExecutor.list(projection, filter, null, Account.class);

        assertNull(accountList.get(0).getFirstName());
        assertEquals(account.getLastName(), accountList.get(0).getLastName());

        dataset = collExecutor.query(filter);
        assertEquals(account.getFirstName(), dataset.get("firstName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        projection = Projections.include("id", "lastName", "birthDate");
        dataset = collExecutor.query(projection, filter, null, Document.class);

        assertFalse(dataset.containsColumn("firstName"));
        assertEquals(account.getLastName(), dataset.get("lastName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        dataset = collExecutor.query(filter, Account.class);
        assertEquals(account.getFirstName(), dataset.get("firstName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        dataset = collExecutor.query(projection, filter, null, Account.class);

        assertTrue(dataset.containsColumn("firstName"));
        N.println(dataset);
        assertEquals(account.getLastName(), dataset.get("lastName"));

        // ===================
        assertEquals(objectId, collExecutor.queryForSingleResult(_ID, filter, ObjectId.class).get());
    }

    /**
     *
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Test
    public void test_query_asyn() throws InterruptedException, ExecutionException {
        asyncCollExecutor.deleteMany(Filters.ne("lastName", Strings.uuid()));

        Account account = createAccount();
        asyncCollExecutor.insertOne(account).get();

        Document doc = asyncCollExecutor.findFirst(Filters.eq("lastName", account.getLastName())).get().orElse(null);
        N.println(doc);

        ObjectId objectId = doc.getObjectId(MongoDB._ID);
        Bson filter = Filters.eq("firstName", account.getFirstName());

        assertEquals(objectId, asyncCollExecutor.findFirst(filter).get().orElse(null).getObjectId(_ID));
        assertEquals(account.getFirstName(), asyncCollExecutor.findFirst(filter).get().orElse(null).get("firstName"));
        assertEquals(account.getFirstName(), asyncCollExecutor.findFirst(filter, Account.class).get().orElse(null).getFirstName());

        List<Document> docList = asyncCollExecutor.list(filter).get();
        assertEquals(account.getFirstName(), docList.get(0).get("firstName"));

        docList = asyncCollExecutor.list(N.asList("lastName"), filter, Document.class).get();

        asyncCollExecutor.list(N.asList("lastName"), filter, String.class).get().forEach(Fn.println());

        assertNull(docList.get(0).get("firstName"));
        assertEquals(account.getLastName(), docList.get(0).get("lastName"));

        List<Account> accountList = asyncCollExecutor.list(filter, Account.class).get();
        assertEquals(account.getFirstName(), accountList.get(0).getFirstName());

        accountList = asyncCollExecutor.list(N.asList("lastName"), filter, Account.class).get();

        assertNull(accountList.get(0).getFirstName());
        assertEquals(account.getLastName(), accountList.get(0).getLastName());

        Dataset dataset = asyncCollExecutor.query(filter).get();
        assertEquals(account.getFirstName(), dataset.get("firstName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        dataset = asyncCollExecutor.query(N.asList("lastName", "birthDate"), filter, Document.class).get();

        assertFalse(dataset.containsColumn("firstName"));
        assertEquals(account.getLastName(), dataset.get("lastName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        dataset = asyncCollExecutor.query(filter, Account.class).get();
        assertEquals(account.getFirstName(), dataset.get("firstName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        dataset = asyncCollExecutor.query(N.asList("lastName", "birthDate"), filter, Account.class).get();

        assertFalse(dataset.containsColumn("firstName"));
        N.println(dataset);
        assertEquals(account.getLastName(), dataset.get("lastName"));

        // ########################################################################
        Bson projection = Projections.include("id", "firstName", "lastName");

        assertEquals(objectId, asyncCollExecutor.findFirst(filter).get().orElse(null).getObjectId(_ID));
        assertEquals(account.getFirstName(), asyncCollExecutor.findFirst(filter).get().orElse(null).get("firstName"));
        assertEquals(account.getFirstName(), asyncCollExecutor.findFirst(projection, filter, null, Account.class).get().orElse(null).getFirstName());

        docList = asyncCollExecutor.list(filter).get();
        assertEquals(account.getFirstName(), docList.get(0).get("firstName"));

        projection = Projections.include("id", "lastName");
        docList = asyncCollExecutor.list(projection, filter, null, Document.class).get();

        assertNull(docList.get(0).get("firstName"));
        assertEquals(account.getLastName(), docList.get(0).get("lastName"));

        accountList = asyncCollExecutor.list(filter, Account.class).get();
        assertEquals(account.getFirstName(), accountList.get(0).getFirstName());

        accountList = asyncCollExecutor.list(projection, filter, null, Account.class).get();

        assertNull(accountList.get(0).getFirstName());
        assertEquals(account.getLastName(), accountList.get(0).getLastName());

        dataset = asyncCollExecutor.query(filter).get();
        assertEquals(account.getFirstName(), dataset.get("firstName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        projection = Projections.include("id", "lastName", "birthDate");
        dataset = asyncCollExecutor.query(projection, filter, null, Document.class).get();

        assertFalse(dataset.containsColumn("firstName"));
        assertEquals(account.getLastName(), dataset.get("lastName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        dataset = asyncCollExecutor.query(filter, Account.class).get();
        assertEquals(account.getFirstName(), dataset.get("firstName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        dataset = asyncCollExecutor.query(projection, filter, null, Account.class).get();

        assertTrue(dataset.containsColumn("firstName"));
        N.println(dataset);
        assertEquals(account.getLastName(), dataset.get("lastName"));

        // ===================
        assertEquals(objectId, asyncCollExecutor.queryForSingleResult(_ID, filter, ObjectId.class).get().get());
    }

    @Test
    public void test_query_2() {
        collExecutor.deleteMany(Filters.ne("lastName", Strings.uuid()));

        Account account = createAccount();
        collExecutor.insertOne(account);

        Document doc = collExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        N.println(doc);

        ObjectId objectId = doc.getObjectId(MongoDB._ID);
        Bson filter = Filters.eq("firstName", account.getFirstName());

        assertEquals(objectId, collExecutor.findFirst(filter).orElse(null).getObjectId(_ID));
        assertEquals(account.getFirstName(), collExecutor.findFirst(filter).orElse(null).get("firstName"));
        assertEquals(account.getFirstName(), collExecutor.findFirst(filter, Account.class).orElse(null).getFirstName());

        List<Document> docList = collExecutor.list(filter);
        assertEquals(account.getFirstName(), docList.get(0).get("firstName"));

        docList = collExecutor.list(N.asList("lastName"), filter, Document.class);

        assertNull(docList.get(0).get("firstName"));
        assertEquals(account.getLastName(), docList.get(0).get("lastName"));

        List<Account> accountList = collExecutor.list(filter, Account.class);
        assertEquals(account.getFirstName(), accountList.get(0).getFirstName());

        accountList = collExecutor.list(N.asList("lastName"), filter, Account.class);

        assertNull(accountList.get(0).getFirstName());
        assertEquals(account.getLastName(), accountList.get(0).getLastName());

        Dataset dataset = collExecutor.query(filter);
        assertEquals(account.getFirstName(), dataset.get("firstName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        dataset = collExecutor.query(N.asList("lastName", "birthDate"), filter, Document.class);

        assertFalse(dataset.containsColumn("firstName"));
        assertEquals(account.getLastName(), dataset.get("lastName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        dataset = collExecutor.query(filter, Account.class);
        assertEquals(account.getFirstName(), dataset.get("firstName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        dataset = collExecutor.query(N.asList("lastName", "birthDate"), filter, Account.class);

        assertFalse(dataset.containsColumn("firstName"));
        N.println(dataset);
        assertEquals(account.getLastName(), dataset.get("lastName"));

        // ########################################################################
        Bson projection = Projections.include("id", "firstName", "lastName");

        assertEquals(objectId, collExecutor.findFirst(filter).orElse(null).getObjectId(_ID));
        assertEquals(account.getFirstName(), collExecutor.findFirst(filter).orElse(null).get("firstName"));
        assertEquals(account.getFirstName(), collExecutor.findFirst(projection, filter, null, Account.class).orElse(null).getFirstName());

        docList = collExecutor.list(filter);
        assertEquals(account.getFirstName(), docList.get(0).get("firstName"));

        projection = Projections.include("id", "lastName");
        docList = collExecutor.list(projection, filter, null, Document.class);

        assertNull(docList.get(0).get("firstName"));
        assertEquals(account.getLastName(), docList.get(0).get("lastName"));

        accountList = collExecutor.list(filter, Account.class);
        assertEquals(account.getFirstName(), accountList.get(0).getFirstName());

        accountList = collExecutor.list(projection, filter, null, Account.class);

        assertNull(accountList.get(0).getFirstName());
        assertEquals(account.getLastName(), accountList.get(0).getLastName());

        dataset = collExecutor.query(filter);
        assertEquals(account.getFirstName(), dataset.get("firstName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        projection = Projections.include("id", "lastName", "birthDate");
        dataset = collExecutor.query(projection, filter, null, Document.class);

        assertFalse(dataset.containsColumn("firstName"));
        assertEquals(account.getLastName(), dataset.get("lastName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        dataset = collExecutor.query(filter, Account.class);
        assertEquals(account.getFirstName(), dataset.get("firstName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        dataset = collExecutor.query(projection, filter, null, Account.class);

        assertTrue(dataset.containsColumn("firstName"));
        N.println(dataset);
        assertEquals(account.getLastName(), dataset.get("lastName"));

        // ===================
        assertEquals(objectId, collExecutor.queryForSingleResult(_ID, filter, ObjectId.class).get());
    }

    /**
     *
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Test
    public void test_query_async_2() throws InterruptedException, ExecutionException {
        asyncCollExecutor.deleteMany(Filters.ne("lastName", Strings.uuid()));

        Account account = createAccount();
        asyncCollExecutor.insertOne(account).get();

        Document doc = asyncCollExecutor.findFirst(Filters.eq("lastName", account.getLastName())).get().orElse(null);
        N.println(doc);

        ObjectId objectId = doc.getObjectId(MongoDB._ID);
        Bson filter = Filters.eq("firstName", account.getFirstName());

        assertEquals(objectId, asyncCollExecutor.findFirst(filter).get().orElse(null).getObjectId(_ID));
        assertEquals(account.getFirstName(), asyncCollExecutor.findFirst(filter).get().orElse(null).get("firstName"));
        assertEquals(account.getFirstName(), asyncCollExecutor.findFirst(filter, Account.class).get().orElse(null).getFirstName());

        List<Document> docList = asyncCollExecutor.list(filter).get();
        assertEquals(account.getFirstName(), docList.get(0).get("firstName"));

        docList = asyncCollExecutor.list(N.asList("lastName"), filter, Document.class).get();

        assertNull(docList.get(0).get("firstName"));
        assertEquals(account.getLastName(), docList.get(0).get("lastName"));

        List<Account> accountList = asyncCollExecutor.list(filter, Account.class).get();
        assertEquals(account.getFirstName(), accountList.get(0).getFirstName());

        accountList = asyncCollExecutor.list(N.asList("lastName"), filter, Account.class).get();

        assertNull(accountList.get(0).getFirstName());
        assertEquals(account.getLastName(), accountList.get(0).getLastName());

        Dataset dataset = asyncCollExecutor.query(filter).get();
        assertEquals(account.getFirstName(), dataset.get("firstName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        dataset = asyncCollExecutor.query(N.asList("lastName", "birthDate"), filter, Document.class).get();

        assertFalse(dataset.containsColumn("firstName"));
        assertEquals(account.getLastName(), dataset.get("lastName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        dataset = asyncCollExecutor.query(filter, Account.class).get();
        assertEquals(account.getFirstName(), dataset.get("firstName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        dataset = asyncCollExecutor.query(N.asList("lastName", "birthDate"), filter, Account.class).get();

        assertFalse(dataset.containsColumn("firstName"));
        N.println(dataset);
        assertEquals(account.getLastName(), dataset.get("lastName"));

        // ########################################################################
        Bson projection = Projections.include("id", "firstName", "lastName");

        assertEquals(objectId, asyncCollExecutor.findFirst(filter).get().orElse(null).getObjectId(_ID));
        assertEquals(account.getFirstName(), asyncCollExecutor.findFirst(filter).get().orElse(null).get("firstName"));
        assertEquals(account.getFirstName(), asyncCollExecutor.findFirst(projection, filter, null, Account.class).get().orElse(null).getFirstName());

        docList = asyncCollExecutor.list(filter).get();
        assertEquals(account.getFirstName(), docList.get(0).get("firstName"));

        projection = Projections.include("id", "lastName");
        docList = asyncCollExecutor.list(projection, filter, null, Document.class).get();

        assertNull(docList.get(0).get("firstName"));
        assertEquals(account.getLastName(), docList.get(0).get("lastName"));

        accountList = asyncCollExecutor.list(filter, Account.class).get();
        assertEquals(account.getFirstName(), accountList.get(0).getFirstName());

        accountList = asyncCollExecutor.list(projection, filter, null, Account.class).get();

        assertNull(accountList.get(0).getFirstName());
        assertEquals(account.getLastName(), accountList.get(0).getLastName());

        dataset = asyncCollExecutor.query(filter).get();
        assertEquals(account.getFirstName(), dataset.get("firstName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        projection = Projections.include("id", "lastName", "birthDate");
        dataset = asyncCollExecutor.query(projection, filter, null, Document.class).get();

        assertFalse(dataset.containsColumn("firstName"));
        assertEquals(account.getLastName(), dataset.get("lastName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        dataset = asyncCollExecutor.query(filter, Account.class).get();
        assertEquals(account.getFirstName(), dataset.get("firstName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        dataset = asyncCollExecutor.query(projection, filter, null, Account.class).get();

        assertTrue(dataset.containsColumn("firstName"));
        N.println(dataset);
        assertEquals(account.getLastName(), dataset.get("lastName"));

        // ===================
        assertEquals(objectId, asyncCollExecutor.queryForSingleResult(_ID, filter, ObjectId.class).get().get());
    }

    @Test
    public void test_updateOne() {
        collExecutor.deleteMany(Filters.ne("lastName", Strings.uuid()));

        Account account = createAccount();
        collExecutor.insertOne(account);

        Document doc = collExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        N.println(doc);

        ObjectId objectId = doc.getObjectId(MongoDB._ID);

        // =======================================================================================
        String newFirstName = Strings.uuid();
        collExecutor.updateOne(objectId, N.asMap("firstName", newFirstName));
        Account dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.updateOne(objectId, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.updateOne(objectId, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.updateOne(objectId, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        Account tmp = new Account();
        tmp.setFirstName(newFirstName);
        collExecutor.updateOne(objectId, tmp);
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        newFirstName = Strings.uuid();
        collExecutor.updateOne(objectId.toString(), N.asMap("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.updateOne(objectId.toString(), MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.updateOne(objectId.toString(), MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.updateOne(objectId.toString(), MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collExecutor.updateOne(objectId.toString(), tmp);
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        Bson filter = Filters.eq(_ID, objectId);
        newFirstName = Strings.uuid();
        collExecutor.updateMany(filter, N.asMap("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.updateMany(filter, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.updateMany(filter, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.updateMany(filter, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collExecutor.updateMany(filter, tmp);
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        filter = Filters.eq("lastName", account.getLastName());
        newFirstName = Strings.uuid();
        collExecutor.updateMany(filter, N.asMap("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.updateMany(filter, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.updateMany(filter, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.updateMany(filter, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collExecutor.updateMany(filter, tmp);
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        filter = Filters.eq("lastName", account.getLastName());
        newFirstName = Strings.uuid();
        collExecutor.updateOne(filter, N.asMap("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.updateOne(filter, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.updateOne(filter, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.updateOne(filter, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.updateOne(filter, MongoDB.toDBObject("$set", MongoDB.toDocument("firstName", newFirstName)));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.updateOne(filter, MongoDB.toDocument("$set", MongoDB.toDBObject("firstName", newFirstName)));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collExecutor.updateOne(filter, tmp);
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        //++++++++++++++++++++++++++++++++++++++++++++++ replace.

        // =======================================================================================
        newFirstName = Strings.uuid();
        collExecutor.replaceOne(objectId, N.asMap("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.replaceOne(objectId, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.replaceOne(objectId, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.replaceOne(objectId, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collExecutor.replaceOne(objectId, tmp);
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        newFirstName = Strings.uuid();
        collExecutor.replaceOne(objectId.toString(), N.asMap("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.replaceOne(objectId.toString(), MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.replaceOne(objectId.toString(), MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.replaceOne(objectId.toString(), MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collExecutor.replaceOne(objectId.toString(), tmp);
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        filter = Filters.eq(_ID, objectId);
        newFirstName = Strings.uuid();
        collExecutor.replaceOne(filter, N.asMap("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.replaceOne(filter, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.replaceOne(filter, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.replaceOne(filter, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collExecutor.replaceOne(filter, tmp);
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());
        //++++++++++++++++++++++++++++++++++++++++++++++ delete.

        // =======================================================================================
        account.setId(generateId());
        collExecutor.insertOne(account);
        doc = collExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        collExecutor.deleteOne(objectId);
        assertNull(collExecutor.gett(objectId));

        collExecutor.insertOne(account);
        doc = collExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        collExecutor.deleteOne(objectId.toHexString());
        assertNull(collExecutor.gett(objectId));

        collExecutor.insertOne(account);
        doc = collExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        filter = Filters.eq(_ID, objectId);
        N.println(collExecutor.list(filter));
        collExecutor.deleteMany(filter);
        assertEquals(0, collExecutor.list(filter).size());

        collExecutor.insertOne(account);
        doc = collExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        filter = Filters.eq("lastName", account.getLastName());
        N.println(collExecutor.list(filter));
        collExecutor.deleteMany(filter);
        assertEquals(0, collExecutor.list(filter).size());

        collExecutor.insertOne(account);
        doc = collExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        filter = Filters.eq("lastName", account.getLastName());
        N.println(collExecutor.list(filter));
        collExecutor.deleteOne(filter);
        assertEquals(0, collExecutor.list(filter).size());
    }

    /**
     *
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Test
    public void test_update_async() throws InterruptedException, ExecutionException {
        asyncCollExecutor.deleteMany(Filters.ne("lastName", Strings.uuid()));

        Account account = createAccount();
        asyncCollExecutor.insertOne(account).get();

        Document doc = asyncCollExecutor.findFirst(Filters.eq("lastName", account.getLastName())).get().orElse(null);
        N.println(doc);

        ObjectId objectId = doc.getObjectId(MongoDB._ID);

        // =======================================================================================
        String newFirstName = Strings.uuid();
        asyncCollExecutor.updateOne(objectId, N.asMap("firstName", newFirstName)).get();
        Account dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateOne(objectId, MongoDB.toDocument("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateOne(objectId, MongoDB.toBSONObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateOne(objectId, MongoDB.toDBObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        Account tmp = new Account();
        tmp.setFirstName(newFirstName);
        asyncCollExecutor.updateOne(objectId, tmp).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        newFirstName = Strings.uuid();
        asyncCollExecutor.updateOne(objectId.toString(), N.asMap("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateOne(objectId.toString(), MongoDB.toDocument("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateOne(objectId.toString(), MongoDB.toBSONObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateOne(objectId.toString(), MongoDB.toDBObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        asyncCollExecutor.updateOne(objectId.toString(), tmp).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        Bson filter = Filters.eq(_ID, objectId);
        newFirstName = Strings.uuid();
        asyncCollExecutor.updateMany(filter, N.asMap("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateMany(filter, MongoDB.toDocument("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateMany(filter, MongoDB.toBSONObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateMany(filter, MongoDB.toDBObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        asyncCollExecutor.updateMany(filter, tmp).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        filter = Filters.eq("lastName", account.getLastName());
        newFirstName = Strings.uuid();
        asyncCollExecutor.updateMany(filter, N.asMap("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateMany(filter, MongoDB.toDocument("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateMany(filter, MongoDB.toBSONObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateMany(filter, MongoDB.toDBObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        asyncCollExecutor.updateMany(filter, tmp).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        filter = Filters.eq("lastName", account.getLastName());
        newFirstName = Strings.uuid();
        asyncCollExecutor.updateOne(filter, N.asMap("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateOne(filter, MongoDB.toDocument("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateOne(filter, MongoDB.toBSONObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateOne(filter, MongoDB.toDBObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateOne(filter, MongoDB.toDBObject("$set", MongoDB.toDocument("firstName", newFirstName))).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateOne(filter, MongoDB.toDocument("$set", MongoDB.toDBObject("firstName", newFirstName))).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        asyncCollExecutor.updateOne(filter, tmp).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        //++++++++++++++++++++++++++++++++++++++++++++++ replace.

        // =======================================================================================
        newFirstName = Strings.uuid();
        asyncCollExecutor.replaceOne(objectId, N.asMap("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.replaceOne(objectId, MongoDB.toDocument("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.replaceOne(objectId, MongoDB.toBSONObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.replaceOne(objectId, MongoDB.toDBObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        asyncCollExecutor.replaceOne(objectId, tmp).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        newFirstName = Strings.uuid();
        asyncCollExecutor.replaceOne(objectId.toString(), N.asMap("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.replaceOne(objectId.toString(), MongoDB.toDocument("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.replaceOne(objectId.toString(), MongoDB.toBSONObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.replaceOne(objectId.toString(), MongoDB.toDBObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        asyncCollExecutor.replaceOne(objectId.toString(), tmp).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        filter = Filters.eq(_ID, objectId);
        newFirstName = Strings.uuid();
        asyncCollExecutor.replaceOne(filter, N.asMap("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.replaceOne(filter, MongoDB.toDocument("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.replaceOne(filter, MongoDB.toBSONObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.replaceOne(filter, MongoDB.toDBObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        asyncCollExecutor.replaceOne(filter, tmp).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());
        //++++++++++++++++++++++++++++++++++++++++++++++ delete.

        // =======================================================================================
        account.setId(generateId());
        asyncCollExecutor.insertOne(account).get();
        doc = asyncCollExecutor.findFirst(Filters.eq("lastName", account.getLastName())).get().orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        asyncCollExecutor.deleteOne(objectId).get();
        assertNull(asyncCollExecutor.gett(objectId).get());

        asyncCollExecutor.insertOne(account).get();
        doc = asyncCollExecutor.findFirst(Filters.eq("lastName", account.getLastName())).get().orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        asyncCollExecutor.deleteOne(objectId.toHexString()).get();
        assertNull(asyncCollExecutor.gett(objectId).get());

        asyncCollExecutor.insertOne(account).get();
        doc = asyncCollExecutor.findFirst(Filters.eq("lastName", account.getLastName())).get().orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        filter = Filters.eq(_ID, objectId);
        N.println(asyncCollExecutor.list(filter).get());
        asyncCollExecutor.deleteMany(filter).get();
        assertEquals(0, asyncCollExecutor.list(filter).get().size());

        asyncCollExecutor.insertOne(account).get();
        doc = asyncCollExecutor.findFirst(Filters.eq("lastName", account.getLastName())).get().orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        filter = Filters.eq("lastName", account.getLastName());
        N.println(asyncCollExecutor.list(filter).get());
        asyncCollExecutor.deleteMany(filter).get();
        assertEquals(0, asyncCollExecutor.list(filter).get().size());

        asyncCollExecutor.insertOne(account).get();
        doc = asyncCollExecutor.findFirst(Filters.eq("lastName", account.getLastName())).get().orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        filter = Filters.eq("lastName", account.getLastName());
        N.println(asyncCollExecutor.list(filter).get());
        asyncCollExecutor.deleteOne(filter).get();
        assertEquals(0, asyncCollExecutor.list(filter).get().size());
    }

    @Test
    public void test_update_2() {
        collExecutor.deleteMany(Filters.ne("lastName", Strings.uuid()));

        Account account = createAccount();
        collExecutor.insertOne(account);

        Document doc = collExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        N.println(doc);

        ObjectId objectId = doc.getObjectId(MongoDB._ID);

        // =======================================================================================
        String newFirstName = Strings.uuid();
        collExecutor.updateOne(objectId, N.asMap("firstName", newFirstName));
        Account dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.updateOne(objectId, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.updateOne(objectId, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.updateOne(objectId, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        Account tmp = new Account();
        tmp.setFirstName(newFirstName);
        collExecutor.updateOne(objectId, tmp);
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        newFirstName = Strings.uuid();
        collExecutor.updateOne(objectId.toString(), N.asMap("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.updateOne(objectId.toString(), MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.updateOne(objectId.toString(), MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.updateOne(objectId.toString(), MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collExecutor.updateOne(objectId.toString(), tmp);
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        Bson filter = Filters.eq(_ID, objectId);
        newFirstName = Strings.uuid();
        collExecutor.updateMany(filter, N.asMap("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.updateMany(filter, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.updateMany(filter, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.updateMany(filter, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collExecutor.updateMany(filter, tmp);
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        filter = Filters.eq("lastName", account.getLastName());
        newFirstName = Strings.uuid();
        collExecutor.updateMany(filter, N.asMap("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.updateMany(filter, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.updateMany(filter, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.updateMany(filter, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collExecutor.updateMany(filter, tmp);
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        filter = Filters.eq("lastName", account.getLastName());
        newFirstName = Strings.uuid();
        collExecutor.updateOne(filter, N.asMap("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.updateOne(filter, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.updateOne(filter, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.updateOne(filter, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collExecutor.updateOne(filter, tmp);
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        //++++++++++++++++++++++++++++++++++++++++++++++ replace.

        // =======================================================================================
        newFirstName = Strings.uuid();
        collExecutor.replaceOne(objectId, N.asMap("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.replaceOne(objectId, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.replaceOne(objectId, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.replaceOne(objectId, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collExecutor.replaceOne(objectId, tmp);
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        newFirstName = Strings.uuid();
        collExecutor.replaceOne(objectId.toString(), N.asMap("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.replaceOne(objectId.toString(), MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.replaceOne(objectId.toString(), MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.replaceOne(objectId.toString(), MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collExecutor.replaceOne(objectId.toString(), tmp);
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        filter = Filters.eq(_ID, objectId);
        newFirstName = Strings.uuid();
        collExecutor.replaceOne(filter, N.asMap("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.replaceOne(filter, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.replaceOne(filter, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collExecutor.replaceOne(filter, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collExecutor.replaceOne(filter, tmp);
        dbAccount = collExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());
        //++++++++++++++++++++++++++++++++++++++++++++++ delete.

        // =======================================================================================
        account.setId(generateId());
        collExecutor.insertOne(account);
        doc = collExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        collExecutor.deleteOne(objectId);
        assertNull(collExecutor.gett(objectId));

        collExecutor.insertOne(account);
        doc = collExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        collExecutor.deleteOne(objectId.toHexString());
        assertNull(collExecutor.gett(objectId));

        collExecutor.insertOne(account);
        doc = collExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        filter = Filters.eq(_ID, objectId);
        N.println(collExecutor.list(filter));
        collExecutor.deleteMany(filter);
        assertEquals(0, collExecutor.list(filter).size());

        collExecutor.insertOne(account);
        doc = collExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        filter = Filters.eq("lastName", account.getLastName());
        N.println(collExecutor.list(filter));
        collExecutor.deleteMany(filter);
        assertEquals(0, collExecutor.list(filter).size());

        collExecutor.insertOne(account);
        doc = collExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        filter = Filters.eq("lastName", account.getLastName());
        N.println(collExecutor.list(filter));
        collExecutor.deleteOne(filter);
        assertEquals(0, collExecutor.list(filter).size());
    }

    /**
     *
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Test
    public void test_update_async_2() throws InterruptedException, ExecutionException {
        asyncCollExecutor.deleteMany(Filters.ne("lastName", Strings.uuid()));

        Account account = createAccount();
        asyncCollExecutor.insertOne(account).get();

        Document doc = asyncCollExecutor.findFirst(Filters.eq("lastName", account.getLastName())).get().orElse(null);
        N.println(doc);

        ObjectId objectId = doc.getObjectId(MongoDB._ID);

        // =======================================================================================
        String newFirstName = Strings.uuid();
        asyncCollExecutor.updateOne(objectId, N.asMap("firstName", newFirstName)).get();
        Account dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateOne(objectId, MongoDB.toDocument("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateOne(objectId, MongoDB.toBSONObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateOne(objectId, MongoDB.toDBObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        Account tmp = new Account();
        tmp.setFirstName(newFirstName);
        asyncCollExecutor.updateOne(objectId, tmp).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        newFirstName = Strings.uuid();
        asyncCollExecutor.updateOne(objectId.toString(), N.asMap("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateOne(objectId.toString(), MongoDB.toDocument("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateOne(objectId.toString(), MongoDB.toBSONObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateOne(objectId.toString(), MongoDB.toDBObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        asyncCollExecutor.updateOne(objectId.toString(), tmp).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        Bson filter = Filters.eq(_ID, objectId);
        newFirstName = Strings.uuid();
        asyncCollExecutor.updateMany(filter, N.asMap("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateMany(filter, MongoDB.toDocument("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateMany(filter, MongoDB.toBSONObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateMany(filter, MongoDB.toDBObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        asyncCollExecutor.updateMany(filter, tmp).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        filter = Filters.eq("lastName", account.getLastName());
        newFirstName = Strings.uuid();
        asyncCollExecutor.updateMany(filter, N.asMap("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateMany(filter, MongoDB.toDocument("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateMany(filter, MongoDB.toBSONObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateMany(filter, MongoDB.toDBObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        asyncCollExecutor.updateMany(filter, tmp).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        filter = Filters.eq("lastName", account.getLastName());
        newFirstName = Strings.uuid();
        asyncCollExecutor.updateOne(filter, N.asMap("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateOne(filter, MongoDB.toDocument("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateOne(filter, MongoDB.toBSONObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateOne(filter, MongoDB.toDBObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateOne(filter, MongoDB.toDBObject("$set", MongoDB.toDocument("firstName", newFirstName))).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.updateOne(filter, MongoDB.toDocument("$set", MongoDB.toDBObject("firstName", newFirstName))).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        asyncCollExecutor.updateOne(filter, tmp).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        //++++++++++++++++++++++++++++++++++++++++++++++ replace.

        // =======================================================================================
        newFirstName = Strings.uuid();
        asyncCollExecutor.replaceOne(objectId, N.asMap("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.replaceOne(objectId, MongoDB.toDocument("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.replaceOne(objectId, MongoDB.toBSONObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.replaceOne(objectId, MongoDB.toDBObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        asyncCollExecutor.replaceOne(objectId, tmp).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        newFirstName = Strings.uuid();
        asyncCollExecutor.replaceOne(objectId.toString(), N.asMap("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.replaceOne(objectId.toString(), MongoDB.toDocument("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.replaceOne(objectId.toString(), MongoDB.toBSONObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.replaceOne(objectId.toString(), MongoDB.toDBObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        asyncCollExecutor.replaceOne(objectId.toString(), tmp).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        filter = Filters.eq(_ID, objectId);
        newFirstName = Strings.uuid();
        asyncCollExecutor.replaceOne(filter, N.asMap("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.replaceOne(filter, MongoDB.toDocument("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.replaceOne(filter, MongoDB.toBSONObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        asyncCollExecutor.replaceOne(filter, MongoDB.toDBObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        asyncCollExecutor.replaceOne(filter, tmp).get();
        dbAccount = asyncCollExecutor.gett(objectId, Account.class).get();
        assertEquals(newFirstName, dbAccount.getFirstName());
        //++++++++++++++++++++++++++++++++++++++++++++++ delete.

        // =======================================================================================
        account.setId(generateId());
        asyncCollExecutor.insertOne(account).get();
        doc = asyncCollExecutor.findFirst(Filters.eq("lastName", account.getLastName())).get().orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        asyncCollExecutor.deleteOne(objectId).get();
        assertNull(asyncCollExecutor.gett(objectId).get());

        asyncCollExecutor.insertOne(account).get();
        doc = asyncCollExecutor.findFirst(Filters.eq("lastName", account.getLastName())).get().orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        asyncCollExecutor.deleteOne(objectId.toHexString()).get();
        assertNull(asyncCollExecutor.gett(objectId).get());

        asyncCollExecutor.insertOne(account).get();
        doc = asyncCollExecutor.findFirst(Filters.eq("lastName", account.getLastName())).get().orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        filter = Filters.eq(_ID, objectId);
        N.println(asyncCollExecutor.list(filter).get());
        asyncCollExecutor.deleteMany(filter).get();
        assertEquals(0, asyncCollExecutor.list(filter).get().size());

        asyncCollExecutor.insertOne(account).get();
        doc = asyncCollExecutor.findFirst(Filters.eq("lastName", account.getLastName())).get().orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        filter = Filters.eq("lastName", account.getLastName());
        N.println(asyncCollExecutor.list(filter).get());
        asyncCollExecutor.deleteMany(filter).get();
        assertEquals(0, asyncCollExecutor.list(filter).get().size());

        asyncCollExecutor.insertOne(account).get();
        doc = asyncCollExecutor.findFirst(Filters.eq("lastName", account.getLastName())).get().orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        filter = Filters.eq("lastName", account.getLastName());
        N.println(asyncCollExecutor.list(filter).get());
        asyncCollExecutor.deleteOne(filter).get();
        assertEquals(0, asyncCollExecutor.list(filter).get().size());
    }

    public void test_toDocument() {
        // MongoDBExecutor.registerIdProeprty(Account.class, MongoDBExecutor.ID);
        Account account = createAccount();
        // account.setId(ObjectId.get().toString());
        Document doc = MongoDB.toDocument(account);
        String json = MongoDB.toJson(doc);
        N.println(json);

        Document doc2 = MongoDB.fromJson(json, Document.class);

        Account account2 = MongoDB.readRow(doc2, Account.class);
        assertEquals(account, account2);
    }

    public void test_toBasicBSONObject() {
        // MongoDBExecutor.registerIdProeprty(Account.class, MongoDBExecutor.ID);
        Account account = createAccount();
        // account.setId(ObjectId.get().toString());
        BasicBSONObject bsonObject = MongoDB.toBSONObject(account);
        String json = MongoDB.toJson(bsonObject);
        N.println(json);

        BasicBSONObject bsonObject2 = MongoDB.fromJson(json, BasicBSONObject.class);

        String json2 = MongoDB.toJson(bsonObject2);
        Account account2 = N.fromJson(json2, Account.class);

        assertEquals(account, account2);
    }

    public void test_toDBObject() {
        // MongoDBExecutor.registerIdProeprty(Account.class, MongoDBExecutor.ID);
        Account account = createAccount();
        // account.setId(ObjectId.get().toString());
        BasicDBObject bsonObject = MongoDB.toDBObject(account);
        String json = MongoDB.toJson(bsonObject);
        N.println(json);

        BasicDBObject bsonObject2 = MongoDB.fromJson(json, BasicDBObject.class);

        String json2 = MongoDB.toJson(bsonObject2);
        Account account2 = N.fromJson(json2, Account.class);

        assertEquals(account, account2);
    }

    public void test_bulkInsert() {
        Account account = createAccount();
        Account account2 = createAccount();
        Account account3 = createAccount();
        Account account4 = createAccount();
        Account account5 = createAccount();

        assertEquals(5, collExecutor.bulkInsert(N.asList(account, account2, account3, MongoDB.toDocument(account4), MongoDB.toDocument(account5))));
    }

    public void test_01() {
        collExecutor.deleteMany(Filters.eq("title", "A blog post"));

        Map<String, Object> m = N.fromJson("{\"title\" : \"A blog post\",\n" + "\"content\" : \"...\",\n" + "\"comments\" : [\n" + "{\n"
                + "\"name\" : \"joe\",\n" + "\"email\" : \"joe@example.com\",\n" + "\"content\" : \"nice post.\"\n" + "}\n" + "]}", Map.class);
        collExecutor.insertOne(m);

        N.println(collExecutor.list(Filters.eq("title", "A blog post"), Map.class));
    }

}
