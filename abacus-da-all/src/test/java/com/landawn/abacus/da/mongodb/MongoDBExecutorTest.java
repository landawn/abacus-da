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
import java.util.HashMap;
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

import com.landawn.abacus.da.Account;
import com.landawn.abacus.da.TestBase;
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

public class MongoDBExecutorTest extends TestBase {
    static final MongoClient mongoClient = MongoClients.create(MongoClientSettings.builder().uuidRepresentation(UuidRepresentation.STANDARD).build());
    static final MongoDatabase mongoDB = mongoClient.getDatabase("test");
    static final String collectionName = "account";
    static final MongoDB dbExecutor = new MongoDB(mongoDB);
    static final MongoCollectionExecutor collectionExecutor = dbExecutor.collectionExecutor(collectionName);
    static final AsyncMongoCollectionExecutor asyncCollExecutor = collectionExecutor.async();

    @Test
    public void test_collection() {
        Account account = createAccount();
        collectionExecutor.insertOne(account);

        MongoCollection<Account> collection = dbExecutor.collection(collectionName, Account.class);

        FindIterable<Account> it = collection.find();

        it.forEach(Fn.println());
    }

    @Test
    public void test_util() {
        Account account = createAccount();
        collectionExecutor.insertOne(account);

        MongoCollection<Document> collection = collectionExecutor.coll();

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
        collectionExecutor.coll().drop();

        Account account = createAccount();
        collectionExecutor.insertOne(account);
        collectionExecutor.insertOne(createAccount());
        account.setId(generateId());
        collectionExecutor.insertOne(account);

        List<String> firstNameList = collectionExecutor.distinct("firstName", String.class).toList();
        N.println(firstNameList);

        collectionExecutor.deleteMany(Filters.eq("firstName", account.getFirstName()));
    }

    @Test
    public void test_groupBy() {
        collectionExecutor.coll().drop();

        Account account = createAccount();
        collectionExecutor.insertOne(account);
        collectionExecutor.insertOne(createAccount());
        account.setId(generateId());
        collectionExecutor.insertOne(account);
        account.setId(generateId());
        account.setFirstName("firstName123");
        collectionExecutor.insertOne(account);

        collectionExecutor.groupBy("firstName").println();

        collectionExecutor.groupByAndCount("firstName").println();

        collectionExecutor.groupBy(N.asList("firstName")).println();

        collectionExecutor.groupByAndCount(N.asList("firstName")).println();

        collectionExecutor.groupBy(N.asList("firstName", "lastName")).println();

        collectionExecutor.groupByAndCount(N.asList("firstName", "lastName")).println();

        collectionExecutor.deleteMany(Filters.eq("firstName", account.getFirstName()));
    }

    @Test
    public void test_aggregate() {
        collectionExecutor.coll().drop();

        Account account = createAccount();
        collectionExecutor.insertOne(account);
        collectionExecutor.insertOne(createAccount());
        account.setId(generateId());
        collectionExecutor.insertOne(account);

        List<Bson> pipeline = N.toList();

        pipeline.add(fromJson("{$match : {firstName : '" + account.getFirstName() + "'}}", Bson.class));
        pipeline.add(fromJson("{$group : {_id : $firstName, total : {$sum : $status}}}", Bson.class));

        List<Document> resultList = collectionExecutor.aggregate(pipeline).toList();
        N.println(resultList);

        collectionExecutor.deleteMany(Filters.eq("firstName", account.getFirstName()));
    }

    @Test
    public void test_mapReduce() {
        collectionExecutor.coll().drop();

        Account account = createAccount();
        collectionExecutor.insertOne(account);
        collectionExecutor.insertOne(createAccount());
        account.setId(generateId());
        collectionExecutor.insertOne(account);

        List<Bson> pipeline = N.toList();
        pipeline.add(fromJson("{$match : {firstName : '" + account.getFirstName() + "'}}", Bson.class));
        pipeline.add(fromJson("{$group : {_id : $firstName, total : {$sum : $status}}}", Bson.class));

        String mapFunction = "function() {emit(this.firstName, this.status)}";
        String reduceFunction = "function(key, values) { return Array.sum(values)}";

        List<Document> resultList = collectionExecutor.mapReduce(mapFunction, reduceFunction).toList();
        N.println(resultList);

        List<? extends Map<String, Object>> mapList = collectionExecutor.mapReduce(mapFunction, reduceFunction, Clazz.PROPS_MAP).toList();
        N.println(mapList);

        collectionExecutor.deleteMany(Filters.eq("firstName", account.getFirstName()));
    }

    @Test
    public void test_exist_count_get() {
        collectionExecutor.deleteMany(Filters.ne("lastName", Strings.uuid()));

        Account account = createAccount();
        collectionExecutor.insertOne(account);

        Document doc = collectionExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        ObjectId objectId = doc.getObjectId(_ID);

        assertTrue(collectionExecutor.exists(objectId.toString()));
        assertTrue(collectionExecutor.exists(objectId));

        assertTrue(collectionExecutor.exists(Filters.eq(_ID, objectId)));
        assertFalse(collectionExecutor.exists(Filters.ne(_ID, objectId)));
        assertTrue(collectionExecutor.exists(Filters.eq("lastName", account.getLastName())));

        assertEquals(1, collectionExecutor.count(Filters.eq(_ID, objectId)));
        assertEquals(0, collectionExecutor.count(Filters.ne(_ID, objectId)));
        assertEquals(1, collectionExecutor.count(Filters.eq("lastName", account.getLastName())));

        assertEquals(objectId, collectionExecutor.gett(objectId.toString()).getObjectId(_ID));
        assertEquals(objectId, collectionExecutor.gett(objectId).getObjectId(_ID));

        String firstName = account.getFirstName();
        assertEquals(firstName, collectionExecutor.gett(objectId.toString(), Account.class).getFirstName());
        assertEquals(firstName, collectionExecutor.gett(objectId, Account.class).getFirstName());

        List<Document> result = collectionExecutor.list(Filters.eq("lastName", account.getLastName()), Document.class);

        N.println(result);

        collectionExecutor.deleteOne(objectId);
    }

    @Test
    public void test_exist_count_get_2() {
        collectionExecutor.deleteMany(Filters.ne("lastName", Strings.uuid()));

        Account account = createAccount();
        collectionExecutor.insertOne(account);

        Document doc = collectionExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        ObjectId objectId = doc.getObjectId(_ID);

        assertTrue(collectionExecutor.exists(objectId.toString()));
        assertTrue(collectionExecutor.exists(objectId));

        assertTrue(collectionExecutor.exists(Filters.eq(_ID, objectId)));
        assertFalse(collectionExecutor.exists(Filters.ne(_ID, objectId)));
        assertTrue(collectionExecutor.exists(Filters.eq("lastName", account.getLastName())));

        assertEquals(1, collectionExecutor.count(Filters.eq(_ID, objectId)));
        assertEquals(0, collectionExecutor.count(Filters.ne(_ID, objectId)));
        assertEquals(1, collectionExecutor.count(Filters.eq("lastName", account.getLastName())));

        assertEquals(objectId, collectionExecutor.gett(objectId.toString()).getObjectId(_ID));
        assertEquals(objectId, collectionExecutor.gett(objectId).getObjectId(_ID));

        String firstName = account.getFirstName();
        assertEquals(firstName, collectionExecutor.gett(objectId.toString(), Account.class).getFirstName());
        assertEquals(firstName, collectionExecutor.gett(objectId, Account.class).getFirstName());

        List<Document> result = collectionExecutor.list(Filters.eq("lastName", account.getLastName()), Document.class);

        N.println(result);

        collectionExecutor.deleteOne(objectId);
    }

    @Test
    public void test_insertOne() {
        collectionExecutor.deleteMany(Filters.ne("lastName", Strings.uuid()));

        // N.asMap(...) returns an immutable map; wrap so we can add "props".
        Map<String, Object> m = new HashMap<>(N.asMap("lastName", Strings.uuid(), "firstName", Strings.uuid()));
        m.put("props", N.asMap("prop1", 1, "prop2", 2));

        collectionExecutor.insertOne(m);

        Document doc = collectionExecutor.findFirst(Filters.eq("lastName", m.get("lastName"))).orElse(null);
        N.println(doc);

        collectionExecutor.deleteMany(Filters.eq("lastName", m.get("lastName")));
    }

    @Test
    public void test_query() {
        collectionExecutor.deleteMany(Filters.ne("lastName", Strings.uuid()));

        Account account = createAccount();
        collectionExecutor.insertOne(account);

        Document doc = collectionExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        N.println(doc);

        ObjectId objectId = doc.getObjectId(MongoDB._ID);
        Bson filter = Filters.eq("firstName", account.getFirstName());

        assertEquals(objectId, collectionExecutor.findFirst(filter).orElse(null).getObjectId(_ID));
        assertEquals(account.getFirstName(), collectionExecutor.findFirst(filter).orElse(null).get("firstName"));
        assertEquals(account.getFirstName(), collectionExecutor.findFirst(filter, Account.class).orElse(null).getFirstName());

        List<Document> docList = collectionExecutor.list(filter);
        assertEquals(account.getFirstName(), docList.get(0).get("firstName"));

        docList = collectionExecutor.list(N.asList("lastName"), filter, Document.class);

        collectionExecutor.list(N.asList("lastName"), filter, String.class).forEach(Fn.println());

        assertNull(docList.get(0).get("firstName"));
        assertEquals(account.getLastName(), docList.get(0).get("lastName"));

        List<Account> accountList = collectionExecutor.list(filter, Account.class);
        assertEquals(account.getFirstName(), accountList.get(0).getFirstName());

        accountList = collectionExecutor.list(N.asList("lastName"), filter, Account.class);

        assertNull(accountList.get(0).getFirstName());
        assertEquals(account.getLastName(), accountList.get(0).getLastName());

        Dataset dataset = collectionExecutor.query(filter);
        assertEquals(account.getFirstName(), dataset.get("firstName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        dataset = collectionExecutor.query(N.asList("lastName", "birthDate"), filter, Document.class);

        assertFalse(dataset.containsColumn("firstName"));
        assertEquals(account.getLastName(), dataset.get("lastName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        dataset = collectionExecutor.query(filter, Account.class);
        assertEquals(account.getFirstName(), dataset.get("firstName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        dataset = collectionExecutor.query(N.asList("lastName", "birthDate"), filter, Account.class);

        assertFalse(dataset.containsColumn("firstName"));
        N.println(dataset);
        assertEquals(account.getLastName(), dataset.get("lastName"));

        // ########################################################################
        Bson projection = Projections.include("id", "firstName", "lastName");

        assertEquals(objectId, collectionExecutor.findFirst(filter).orElse(null).getObjectId(_ID));
        assertEquals(account.getFirstName(), collectionExecutor.findFirst(filter).orElse(null).get("firstName"));
        assertEquals(account.getFirstName(), collectionExecutor.findFirst(projection, filter, null, Account.class).orElse(null).getFirstName());

        docList = collectionExecutor.list(filter);
        assertEquals(account.getFirstName(), docList.get(0).get("firstName"));

        projection = Projections.include("id", "lastName");
        docList = collectionExecutor.list(projection, filter, null, Document.class);

        assertNull(docList.get(0).get("firstName"));
        assertEquals(account.getLastName(), docList.get(0).get("lastName"));

        accountList = collectionExecutor.list(filter, Account.class);
        assertEquals(account.getFirstName(), accountList.get(0).getFirstName());

        accountList = collectionExecutor.list(projection, filter, null, Account.class);

        assertNull(accountList.get(0).getFirstName());
        assertEquals(account.getLastName(), accountList.get(0).getLastName());

        dataset = collectionExecutor.query(filter);
        assertEquals(account.getFirstName(), dataset.get("firstName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        projection = Projections.include("id", "lastName", "birthDate");
        dataset = collectionExecutor.query(projection, filter, null, Document.class);

        assertFalse(dataset.containsColumn("firstName"));
        assertEquals(account.getLastName(), dataset.get("lastName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        dataset = collectionExecutor.query(filter, Account.class);
        assertEquals(account.getFirstName(), dataset.get("firstName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        dataset = collectionExecutor.query(projection, filter, null, Account.class);

        assertTrue(dataset.containsColumn("firstName"));
        N.println(dataset);
        assertEquals(account.getLastName(), dataset.get("lastName"));

        // ===================
        assertEquals(objectId, collectionExecutor.queryForSingleValue(_ID, filter, ObjectId.class).get());
    }

    /**
     *
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Test
    public void test_query_asyn() throws InterruptedException, ExecutionException {
        // Must await deleteMany; otherwise it can race with the insertOne below
        // and wipe the just-inserted account, leaving findFirst empty and NPE'ing.
        asyncCollExecutor.deleteMany(Filters.ne("lastName", Strings.uuid())).get();

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
        assertEquals(objectId, asyncCollExecutor.queryForSingleValue(_ID, filter, ObjectId.class).get().get());
    }

    @Test
    public void test_query_2() {
        collectionExecutor.deleteMany(Filters.ne("lastName", Strings.uuid()));

        Account account = createAccount();
        collectionExecutor.insertOne(account);

        Document doc = collectionExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        N.println(doc);

        ObjectId objectId = doc.getObjectId(MongoDB._ID);
        Bson filter = Filters.eq("firstName", account.getFirstName());

        assertEquals(objectId, collectionExecutor.findFirst(filter).orElse(null).getObjectId(_ID));
        assertEquals(account.getFirstName(), collectionExecutor.findFirst(filter).orElse(null).get("firstName"));
        assertEquals(account.getFirstName(), collectionExecutor.findFirst(filter, Account.class).orElse(null).getFirstName());

        List<Document> docList = collectionExecutor.list(filter);
        assertEquals(account.getFirstName(), docList.get(0).get("firstName"));

        docList = collectionExecutor.list(N.asList("lastName"), filter, Document.class);

        assertNull(docList.get(0).get("firstName"));
        assertEquals(account.getLastName(), docList.get(0).get("lastName"));

        List<Account> accountList = collectionExecutor.list(filter, Account.class);
        assertEquals(account.getFirstName(), accountList.get(0).getFirstName());

        accountList = collectionExecutor.list(N.asList("lastName"), filter, Account.class);

        assertNull(accountList.get(0).getFirstName());
        assertEquals(account.getLastName(), accountList.get(0).getLastName());

        Dataset dataset = collectionExecutor.query(filter);
        assertEquals(account.getFirstName(), dataset.get("firstName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        dataset = collectionExecutor.query(N.asList("lastName", "birthDate"), filter, Document.class);

        assertFalse(dataset.containsColumn("firstName"));
        assertEquals(account.getLastName(), dataset.get("lastName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        dataset = collectionExecutor.query(filter, Account.class);
        assertEquals(account.getFirstName(), dataset.get("firstName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        dataset = collectionExecutor.query(N.asList("lastName", "birthDate"), filter, Account.class);

        assertFalse(dataset.containsColumn("firstName"));
        N.println(dataset);
        assertEquals(account.getLastName(), dataset.get("lastName"));

        // ########################################################################
        Bson projection = Projections.include("id", "firstName", "lastName");

        assertEquals(objectId, collectionExecutor.findFirst(filter).orElse(null).getObjectId(_ID));
        assertEquals(account.getFirstName(), collectionExecutor.findFirst(filter).orElse(null).get("firstName"));
        assertEquals(account.getFirstName(), collectionExecutor.findFirst(projection, filter, null, Account.class).orElse(null).getFirstName());

        docList = collectionExecutor.list(filter);
        assertEquals(account.getFirstName(), docList.get(0).get("firstName"));

        projection = Projections.include("id", "lastName");
        docList = collectionExecutor.list(projection, filter, null, Document.class);

        assertNull(docList.get(0).get("firstName"));
        assertEquals(account.getLastName(), docList.get(0).get("lastName"));

        accountList = collectionExecutor.list(filter, Account.class);
        assertEquals(account.getFirstName(), accountList.get(0).getFirstName());

        accountList = collectionExecutor.list(projection, filter, null, Account.class);

        assertNull(accountList.get(0).getFirstName());
        assertEquals(account.getLastName(), accountList.get(0).getLastName());

        dataset = collectionExecutor.query(filter);
        assertEquals(account.getFirstName(), dataset.get("firstName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        projection = Projections.include("id", "lastName", "birthDate");
        dataset = collectionExecutor.query(projection, filter, null, Document.class);

        assertFalse(dataset.containsColumn("firstName"));
        assertEquals(account.getLastName(), dataset.get("lastName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        dataset = collectionExecutor.query(filter, Account.class);
        assertEquals(account.getFirstName(), dataset.get("firstName"));
        assertTrue(dataset.get("birthDate") instanceof Date);

        dataset = collectionExecutor.query(projection, filter, null, Account.class);

        assertTrue(dataset.containsColumn("firstName"));
        N.println(dataset);
        assertEquals(account.getLastName(), dataset.get("lastName"));

        // ===================
        assertEquals(objectId, collectionExecutor.queryForSingleValue(_ID, filter, ObjectId.class).get());
    }

    /**
     *
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Test
    public void test_query_async_2() throws InterruptedException, ExecutionException {
        // Await the cleanup: an un-awaited deleteMany can run after the insert below and delete the test document.
        asyncCollExecutor.deleteMany(Filters.ne("lastName", Strings.uuid())).get();

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
        assertEquals(objectId, asyncCollExecutor.queryForSingleValue(_ID, filter, ObjectId.class).get().get());
    }

    @Test
    public void test_updateOne() {
        collectionExecutor.deleteMany(Filters.ne("lastName", Strings.uuid()));

        Account account = createAccount();
        collectionExecutor.insertOne(account);

        Document doc = collectionExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        N.println(doc);

        ObjectId objectId = doc.getObjectId(MongoDB._ID);

        // =======================================================================================
        String newFirstName = Strings.uuid();
        collectionExecutor.updateOne(objectId, N.asMap("firstName", newFirstName));
        Account dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.updateOne(objectId, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.updateOne(objectId, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.updateOne(objectId, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        Account tmp = new Account();
        tmp.setFirstName(newFirstName);
        collectionExecutor.updateOne(objectId, tmp);
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        newFirstName = Strings.uuid();
        collectionExecutor.updateOne(objectId.toString(), N.asMap("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.updateOne(objectId.toString(), MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.updateOne(objectId.toString(), MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.updateOne(objectId.toString(), MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collectionExecutor.updateOne(objectId.toString(), tmp);
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        Bson filter = Filters.eq(_ID, objectId);
        newFirstName = Strings.uuid();
        collectionExecutor.updateMany(filter, N.asMap("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.updateMany(filter, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.updateMany(filter, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.updateMany(filter, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collectionExecutor.updateMany(filter, tmp);
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        filter = Filters.eq("lastName", account.getLastName());
        newFirstName = Strings.uuid();
        collectionExecutor.updateMany(filter, N.asMap("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.updateMany(filter, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.updateMany(filter, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.updateMany(filter, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collectionExecutor.updateMany(filter, tmp);
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        filter = Filters.eq("lastName", account.getLastName());
        newFirstName = Strings.uuid();
        collectionExecutor.updateOne(filter, N.asMap("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.updateOne(filter, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.updateOne(filter, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.updateOne(filter, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.updateOne(filter, MongoDB.toDBObject("$set", MongoDB.toDocument("firstName", newFirstName)));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.updateOne(filter, MongoDB.toDocument("$set", MongoDB.toDBObject("firstName", newFirstName)));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collectionExecutor.updateOne(filter, tmp);
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        //++++++++++++++++++++++++++++++++++++++++++++++ replace.

        // =======================================================================================
        newFirstName = Strings.uuid();
        collectionExecutor.replaceOne(objectId, N.asMap("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.replaceOne(objectId, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.replaceOne(objectId, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.replaceOne(objectId, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collectionExecutor.replaceOne(objectId, tmp);
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        newFirstName = Strings.uuid();
        collectionExecutor.replaceOne(objectId.toString(), N.asMap("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.replaceOne(objectId.toString(), MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.replaceOne(objectId.toString(), MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.replaceOne(objectId.toString(), MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collectionExecutor.replaceOne(objectId.toString(), tmp);
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        filter = Filters.eq(_ID, objectId);
        newFirstName = Strings.uuid();
        collectionExecutor.replaceOne(filter, N.asMap("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.replaceOne(filter, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.replaceOne(filter, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.replaceOne(filter, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collectionExecutor.replaceOne(filter, tmp);
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());
        //++++++++++++++++++++++++++++++++++++++++++++++ delete.

        // =======================================================================================
        account.setId(generateId());
        collectionExecutor.insertOne(account);
        doc = collectionExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        collectionExecutor.deleteOne(objectId);
        assertNull(collectionExecutor.gett(objectId));

        collectionExecutor.insertOne(account);
        doc = collectionExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        collectionExecutor.deleteOne(objectId.toHexString());
        assertNull(collectionExecutor.gett(objectId));

        collectionExecutor.insertOne(account);
        doc = collectionExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        filter = Filters.eq(_ID, objectId);
        N.println(collectionExecutor.list(filter));
        collectionExecutor.deleteMany(filter);
        assertEquals(0, collectionExecutor.list(filter).size());

        collectionExecutor.insertOne(account);
        doc = collectionExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        filter = Filters.eq("lastName", account.getLastName());
        N.println(collectionExecutor.list(filter));
        collectionExecutor.deleteMany(filter);
        assertEquals(0, collectionExecutor.list(filter).size());

        collectionExecutor.insertOne(account);
        doc = collectionExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        filter = Filters.eq("lastName", account.getLastName());
        N.println(collectionExecutor.list(filter));
        collectionExecutor.deleteOne(filter);
        assertEquals(0, collectionExecutor.list(filter).size());
    }

    /**
     *
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Test
    public void test_update_async() throws InterruptedException, ExecutionException {
        // Await the cleanup: an un-awaited deleteMany can run after the insert below and delete the test document
        // (the source of this test's historical flakiness).
        asyncCollExecutor.deleteMany(Filters.ne("lastName", Strings.uuid())).get();

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
        collectionExecutor.deleteMany(Filters.ne("lastName", Strings.uuid()));

        Account account = createAccount();
        collectionExecutor.insertOne(account);

        Document doc = collectionExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        N.println(doc);

        ObjectId objectId = doc.getObjectId(MongoDB._ID);

        // =======================================================================================
        String newFirstName = Strings.uuid();
        collectionExecutor.updateOne(objectId, N.asMap("firstName", newFirstName));
        Account dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.updateOne(objectId, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.updateOne(objectId, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.updateOne(objectId, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        Account tmp = new Account();
        tmp.setFirstName(newFirstName);
        collectionExecutor.updateOne(objectId, tmp);
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        newFirstName = Strings.uuid();
        collectionExecutor.updateOne(objectId.toString(), N.asMap("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.updateOne(objectId.toString(), MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.updateOne(objectId.toString(), MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.updateOne(objectId.toString(), MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collectionExecutor.updateOne(objectId.toString(), tmp);
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        Bson filter = Filters.eq(_ID, objectId);
        newFirstName = Strings.uuid();
        collectionExecutor.updateMany(filter, N.asMap("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.updateMany(filter, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.updateMany(filter, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.updateMany(filter, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collectionExecutor.updateMany(filter, tmp);
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        filter = Filters.eq("lastName", account.getLastName());
        newFirstName = Strings.uuid();
        collectionExecutor.updateMany(filter, N.asMap("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.updateMany(filter, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.updateMany(filter, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.updateMany(filter, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collectionExecutor.updateMany(filter, tmp);
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        filter = Filters.eq("lastName", account.getLastName());
        newFirstName = Strings.uuid();
        collectionExecutor.updateOne(filter, N.asMap("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.updateOne(filter, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.updateOne(filter, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.updateOne(filter, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collectionExecutor.updateOne(filter, tmp);
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        //++++++++++++++++++++++++++++++++++++++++++++++ replace.

        // =======================================================================================
        newFirstName = Strings.uuid();
        collectionExecutor.replaceOne(objectId, N.asMap("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.replaceOne(objectId, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.replaceOne(objectId, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.replaceOne(objectId, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collectionExecutor.replaceOne(objectId, tmp);
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        newFirstName = Strings.uuid();
        collectionExecutor.replaceOne(objectId.toString(), N.asMap("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.replaceOne(objectId.toString(), MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.replaceOne(objectId.toString(), MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.replaceOne(objectId.toString(), MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collectionExecutor.replaceOne(objectId.toString(), tmp);
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        filter = Filters.eq(_ID, objectId);
        newFirstName = Strings.uuid();
        collectionExecutor.replaceOne(filter, N.asMap("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.replaceOne(filter, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.replaceOne(filter, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        collectionExecutor.replaceOne(filter, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = Strings.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collectionExecutor.replaceOne(filter, tmp);
        dbAccount = collectionExecutor.gett(objectId, Account.class);
        assertEquals(newFirstName, dbAccount.getFirstName());
        //++++++++++++++++++++++++++++++++++++++++++++++ delete.

        // =======================================================================================
        account.setId(generateId());
        collectionExecutor.insertOne(account);
        doc = collectionExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        collectionExecutor.deleteOne(objectId);
        assertNull(collectionExecutor.gett(objectId));

        collectionExecutor.insertOne(account);
        doc = collectionExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        collectionExecutor.deleteOne(objectId.toHexString());
        assertNull(collectionExecutor.gett(objectId));

        collectionExecutor.insertOne(account);
        doc = collectionExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        filter = Filters.eq(_ID, objectId);
        N.println(collectionExecutor.list(filter));
        collectionExecutor.deleteMany(filter);
        assertEquals(0, collectionExecutor.list(filter).size());

        collectionExecutor.insertOne(account);
        doc = collectionExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        filter = Filters.eq("lastName", account.getLastName());
        N.println(collectionExecutor.list(filter));
        collectionExecutor.deleteMany(filter);
        assertEquals(0, collectionExecutor.list(filter).size());

        collectionExecutor.insertOne(account);
        doc = collectionExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        filter = Filters.eq("lastName", account.getLastName());
        N.println(collectionExecutor.list(filter));
        collectionExecutor.deleteOne(filter);
        assertEquals(0, collectionExecutor.list(filter).size());
    }

    /**
     *
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Test
    public void test_update_async_2() throws InterruptedException, ExecutionException {
        // Must await deleteMany; see test_query_asyn for the race-condition rationale.
        asyncCollExecutor.deleteMany(Filters.ne("lastName", Strings.uuid())).get();

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

        assertEquals(5, collectionExecutor.bulkInsert(N.asList(account, account2, account3, MongoDB.toDocument(account4), MongoDB.toDocument(account5))));
    }

    public void test_01() {
        collectionExecutor.deleteMany(Filters.eq("title", "A blog post"));

        Map<String, Object> m = N.fromJson("{\"title\" : \"A blog post\",\n" + "\"content\" : \"...\",\n" + "\"comments\" : [\n" + "{\n"
                + "\"name\" : \"joe\",\n" + "\"email\" : \"joe@example.com\",\n" + "\"content\" : \"nice post.\"\n" + "}\n" + "]}", Map.class);
        collectionExecutor.insertOne(m);

        N.println(collectionExecutor.list(Filters.eq("title", "A blog post"), Map.class));
    }

    // ---- 2026-10-02 verifyME ----

    @Test
    public void test_queryDottedSelectNameAndGroupByAndCountOnCountField_live_verifyME() throws Exception {
        // Live server shapes: a dotted projection comes back nested ({address: {city: ..}}), so the Dataset column must be
        // resolved from the nested document; a path through an array fails; groupByAndCount on a field named "count"
        // works only for Document rows (others would lose the key to the count column).
        final MongoCollectionExecutor executor = dbExecutor.collectionExecutor("verifyME_dotted");
        executor.coll().drop();

        try {
            executor.coll().insertOne(new Document("_id", 1).append("name", "n1").append("address", new Document("city", "Paris")).append("count", 7));
            executor.coll().insertOne(new Document("_id", 2).append("name", "n2").append("count", 7));
            executor.coll().insertOne(new Document("_id", 3).append("name", "n3").append("address", new Document("city", null)).append("count", 8));

            final List<String> names = N.asList("name", "address.city");
            final Bson sort = new Document("_id", 1);

            for (final Dataset ds : N.asList(executor.query(names, Filters.exists("name"), sort, Map.class),
                    executor.query(names, Filters.exists("name"), sort, 0, 10, Document.class), executor.async().query(names, Filters.exists("name"), sort, Map.class).get(),
                    dbExecutor.collectionMapper("verifyME_dotted", Map.class).query(names, Filters.exists("name"), sort))) {
                assertEquals(names, ds.columnNames());
                assertEquals(N.asList("n1", "n2", "n3"), ds.getColumn("name"));
                assertEquals(N.asList("Paris", null, null), ds.getColumn("address.city"));
            }

            final IllegalArgumentException e = org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                    () -> executor.groupByAndCount("count", Map.class));
            assertTrue(e.getMessage().contains("'count'"), e.getMessage());
            assertEquals(N.asList(new Document("_id", 7).append("count", 2), new Document("_id", 8).append("count", 1)),
                    executor.groupByAndCount("count").sortedBy(doc -> doc.getInteger("_id")).toList());

            executor.coll().insertOne(new Document("_id", 4).append("name", "n4").append("address", N.asList(new Document("city", "Rome"))));
            org.junit.jupiter.api.Assertions.assertThrows(ClassCastException.class, () -> executor.query(names, Filters.exists("name"), sort, Map.class));
        } finally {
            executor.coll().drop();
        }
    }

    // ---- end 2026-10-02 verifyME ----

    // ---- 2026-10-04 coverageME ----

    @Test
    public void test_decodedValueConversionsOnEveryExecutorKind_live_coverageME() throws Exception {
        // Live driver shapes for the MongoDBBase read-side changes through the sync, async, mapper and reactive read paths:
        // documents written by another client (int32 numbers, String map keys, embedded documents, java.time values that the
        // driver's codecs write in UTC, a record document without an "id" field) and an entity/record written through the
        // executor must read back with their declared types. Beans and per-path assertions are shared with
        // MongoCollectionExecutorTest (*_coverageME).
        final MongoCollectionExecutor entityExec = dbExecutor.collectionExecutor("coverageME_e2e_entity");
        final MongoCollectionExecutor recordExec = dbExecutor.collectionExecutor("coverageME_e2e_record");
        final MongoCollectionExecutor boxExec = dbExecutor.collectionExecutor("coverageME_e2e_box");
        final List<MongoCollectionExecutor> executors = N.asList(entityExec, recordExec, boxExec);
        executors.forEach(executor -> executor.coll().drop());

        try (com.mongodb.reactivestreams.client.MongoClient reactiveClient = com.mongodb.reactivestreams.client.MongoClients
                .create(MongoClientSettings.builder().uuidRepresentation(UuidRepresentation.STANDARD).build())) {
            final ObjectId oid = new ObjectId(MongoCollectionExecutorTest.OID_HEX_coverageME);
            final java.time.LocalDate day = MongoCollectionExecutorTest.LOCAL_DAY_coverageME;

            entityExec.coll()
                    .insertOne(new Document("_id", oid).append("nums", N.asList(1, 2))
                            .append("addresses", N.asList(new Document("city", "Paris")))
                            .append("addressByName", new Document("home", new Document("city", "Rome")))
                            .append("countByYear", new Document("2024", 5))
                            .append("day", day)
                            .append("at", MongoCollectionExecutorTest.LOCAL_AT_coverageME)
                            .append("time", MongoCollectionExecutorTest.LOCAL_TIME_coverageME)
                            .append("days", N.asList(day))
                            .append("box", new Document("value", 7))
                            .append("boxes", N.asList(new Document("value", 8))));
            recordExec.coll().insertOne(new Document("_id", oid).append("count", 3).append("nums", N.asList(4)).append("day", day));
            boxExec.coll().insertOne(new Document("_id", 1).append("value", 9));

            // Written through the executor: nested beans become embedded documents, java.time values go through the driver codecs.
            final MongoCollectionExecutorTest.E2eAddress_coverageME address = new MongoCollectionExecutorTest.E2eAddress_coverageME();
            address.setCity("Oslo");
            final MongoCollectionExecutorTest.E2eBox_coverageME<Long> box = new MongoCollectionExecutorTest.E2eBox_coverageME<>();
            box.setValue(11L);
            final MongoCollectionExecutorTest.E2eEntity_coverageME written = new MongoCollectionExecutorTest.E2eEntity_coverageME();
            written.setId(new ObjectId().toHexString());
            written.setAddresses(N.asList(address));
            written.setDay(day);
            written.setTime(MongoCollectionExecutorTest.LOCAL_TIME_coverageME);
            written.setBoxes(N.asList(box));
            entityExec.insertOne(written);
            final MongoCollectionExecutorTest.E2eRecord_coverageME writtenRecord = new MongoCollectionExecutorTest.E2eRecord_coverageME("written-1", 5L,
                    N.asList(6L), day);
            recordExec.insertOne(writtenRecord);

            final Bson byOid = Filters.eq(_ID, oid);
            final Class<MongoCollectionExecutorTest.E2eEntity_coverageME> type = MongoCollectionExecutorTest.E2eEntity_coverageME.class;
            final Class<MongoCollectionExecutorTest.E2eRecord_coverageME> recordType = MongoCollectionExecutorTest.E2eRecord_coverageME.class;
            final Class<MongoCollectionExecutorTest.E2eLongBox_coverageME> boxType = MongoCollectionExecutorTest.E2eLongBox_coverageME.class;
            final MongoCollectionExecutorTest.E2eRecord_coverageME expectedRecord = MongoCollectionExecutorTest.expectedRecord_coverageME();
            final MongoCollectionMapper<MongoCollectionExecutorTest.E2eEntity_coverageME> entityMapper = dbExecutor.collectionMapper("coverageME_e2e_entity",
                    type);
            final MongoCollectionMapper<MongoCollectionExecutorTest.E2eRecord_coverageME> recordMapper = dbExecutor.collectionMapper("coverageME_e2e_record",
                    recordType);
            final com.landawn.abacus.da.mongodb.reactivestreams.MongoDB reactiveDB = new com.landawn.abacus.da.mongodb.reactivestreams.MongoDB(
                    reactiveClient.getDatabase(mongoDB.getName()));
            final com.landawn.abacus.da.mongodb.reactivestreams.MongoCollectionExecutor reactiveEntityExec = reactiveDB.collectionExecutor("coverageME_e2e_entity");
            final com.landawn.abacus.da.mongodb.reactivestreams.MongoCollectionExecutor reactiveRecordExec = reactiveDB.collectionExecutor("coverageME_e2e_record");
            final com.landawn.abacus.da.mongodb.reactivestreams.MongoCollectionExecutor reactiveBoxExec = reactiveDB.collectionExecutor("coverageME_e2e_box");

            org.junit.jupiter.api.Assertions.assertAll(() -> MongoCollectionExecutorTest.assertEntity_coverageME("list", entityExec.list(byOid, type).get(0)),
                    () -> MongoCollectionExecutorTest.assertEntity_coverageME("findFirst", entityExec.findFirst(byOid, type).get()),
                    () -> {
                        try (com.landawn.abacus.util.stream.Stream<MongoCollectionExecutorTest.E2eEntity_coverageME> stream = entityExec.stream(byOid, type)) {
                            MongoCollectionExecutorTest.assertEntity_coverageME("stream", stream.toList().get(0));
                        }
                    }, () -> MongoCollectionExecutorTest.assertEntityDataset_coverageME("query(Bson, Class)", entityExec.query(byOid, type)),
                    () -> MongoCollectionExecutorTest.assertEntity_coverageME("async findFirst", entityExec.async().findFirst(byOid, type).get().get()),
                    () -> MongoCollectionExecutorTest.assertEntity_coverageME("mapper get(ObjectId)", entityMapper.get(oid).get()),
                    () -> MongoCollectionExecutorTest.assertEntity_coverageME("reactive findFirst", reactiveEntityExec.findFirst(byOid, type).block()),
                    () -> MongoCollectionExecutorTest.assertEntity_coverageME("reactive list", reactiveEntityExec.list(byOid, type).blockFirst()),
                    // A typed collection decodes the entity with the framework codec (GeneralCodec -> readRow -> toEntity).
                    () -> MongoCollectionExecutorTest.assertEntity_coverageME("typed collection",
                            dbExecutor.collection("coverageME_e2e_entity", type).find(byOid).first()),
                    () -> MongoCollectionExecutorTest.assertEntity_coverageME("reactive typed collection",
                            reactor.core.publisher.Mono.from(reactiveDB.collection("coverageME_e2e_entity", type).find(byOid).first()).block()),
                    () -> assertEquals(day, entityExec.queryForSingleValue("day", byOid, java.time.LocalDate.class).get(), "queryForSingleValue LocalDate"),
                    () -> assertEquals(MongoCollectionExecutorTest.LOCAL_TIME_coverageME,
                            entityExec.queryForSingleValue("time", byOid, java.time.LocalTime.class).get(), "queryForSingleValue LocalTime"),
                    () -> assertEquals(MongoCollectionExecutorTest.LOCAL_AT_coverageME,
                            entityExec.async().queryForSingleValue("at", byOid, java.time.LocalDateTime.class).get().get(), "async queryForSingleValue LocalDateTime"),
                    () -> assertEquals(day, reactiveEntityExec.queryForSingleValue("day", byOid, java.time.LocalDate.class).block(),
                            "reactive queryForSingleValue LocalDate"),
                    () -> assertEquals(expectedRecord, recordExec.findFirst(byOid, recordType).get(), "record findFirst"),
                    () -> assertEquals(expectedRecord, recordMapper.get(oid).get(), "record mapper get(ObjectId)"),
                    () -> assertEquals(expectedRecord, reactiveRecordExec.findFirst(byOid, recordType).block(), "reactive record findFirst"),
                    () -> assertEquals(writtenRecord, recordExec.findFirst(Filters.eq("id", "written-1"), recordType).get(), "record written by insertOne"),
                    () -> assertEquals((Object) 9L, boxExec.list(Filters.eq(_ID, 1), boxType).get(0).getValue(), "LongBox list"),
                    () -> assertEquals((Object) 9L, reactiveBoxExec.findFirst(Filters.eq(_ID, 1), boxType).block().getValue(), "reactive LongBox findFirst"),
                    () -> {
                        final MongoCollectionExecutorTest.E2eEntity_coverageME back = entityExec.findFirst(Filters.eq(_ID, new ObjectId(written.getId())), type)
                                .get();
                        org.junit.jupiter.api.Assertions.assertAll("entity written by insertOne",
                                () -> assertEquals("Oslo", back.getAddresses().get(0).getCity(), "List<Address>"),
                                () -> assertEquals(day, back.getDay(), "LocalDate"),
                                () -> assertEquals(MongoCollectionExecutorTest.LOCAL_TIME_coverageME, back.getTime(), "LocalTime"),
                                () -> assertEquals((Object) 11L, back.getBoxes().get(0).getValue(), "List<Box<Long>>"));
                    });
        } finally {
            executors.forEach(executor -> executor.coll().drop());
        }
    }

    @Test
    public void test_nestedBeanUuidAndValueTypePropertiesRoundTrip_live_coverageME() throws Exception {
        // Write side of the MongoDBBase codec changes, end to end through insertOne/findFirst on this STANDARD-uuid client:
        // a UUID inside a nested bean is encoded with the collection's uuidRepresentation (HEAD: "The uuidRepresentation has
        // not been specified"), and value types with bean accessors are stored as values, not as documents of their bean
        // properties (HEAD: GregorianCalendar as {timeZone, ...}, ByteBuffer as {position, limit}).
        final MongoCollectionExecutor executor = dbExecutor.collectionExecutor("coverageME_e2e_write");
        executor.coll().drop();

        try {
            final ObjectId uuidOid = new ObjectId();
            final ObjectId calendarOid = new ObjectId();
            final ObjectId bufferOid = new ObjectId();
            final java.util.UUID uuid = java.util.UUID.randomUUID();
            final java.util.GregorianCalendar calendar = new java.util.GregorianCalendar();
            calendar.setTimeInMillis(1_704_164_645_006L);

            org.junit.jupiter.api.Assertions.assertAll(() -> {
                final UuidHolder_coverageME holder = new UuidHolder_coverageME();
                holder.setId(uuidOid.toHexString());
                holder.setInner(new UuidBean_coverageME());
                holder.getInner().setUuid(uuid);
                executor.insertOne(holder);

                assertEquals(uuid, executor.findFirst(Filters.eq(_ID, uuidOid), UuidHolder_coverageME.class).get().getInner().getUuid(), "nested UUID");
            }, () -> {
                final ValueTypes_coverageME values = new ValueTypes_coverageME();
                values.setId(calendarOid.toHexString());
                values.setCalendar(calendar);
                executor.insertOne(values);

                assertEquals(calendar.getTimeInMillis(),
                        executor.findFirst(Filters.eq(_ID, calendarOid), ValueTypes_coverageME.class).get().getCalendar().getTimeInMillis(), "GregorianCalendar");
            }, () -> {
                final ValueTypes_coverageME values = new ValueTypes_coverageME();
                values.setId(bufferOid.toHexString());
                values.setBuffer(java.nio.ByteBuffer.wrap(new byte[] { 1, 2, 3 }));
                executor.insertOne(values);

                assertEquals(java.nio.ByteBuffer.wrap(new byte[] { 1, 2, 3 }),
                        executor.findFirst(Filters.eq(_ID, bufferOid), ValueTypes_coverageME.class).get().getBuffer(), "ByteBuffer");
            });
        } finally {
            executor.coll().drop();
        }
    }

    @Test
    public void test_reactiveQueryDottedSelectNameAndGroupByAndCountOnCountField_live_coverageME() {
        // Reactive twin of test_queryDottedSelectNameAndGroupByAndCountOnCountField_live_verifyME against the live server: a
        // dotted projection comes back nested, so the Dataset column is resolved from the nested document (it was all null);
        // a path through an array fails through the publisher; groupByAndCount on a field named "count" is rejected for
        // non-Document rows at call time and keeps working for Document rows.
        final String collName = "coverageME_reactive_dotted";
        final MongoCollectionExecutor syncExec = dbExecutor.collectionExecutor(collName);
        syncExec.coll().drop();

        try (com.mongodb.reactivestreams.client.MongoClient reactiveClient = com.mongodb.reactivestreams.client.MongoClients
                .create(MongoClientSettings.builder().uuidRepresentation(UuidRepresentation.STANDARD).build())) {
            syncExec.coll().insertOne(new Document("_id", 1).append("name", "n1").append("address", new Document("city", "Paris")).append("count", 7));
            syncExec.coll().insertOne(new Document("_id", 2).append("name", "n2").append("count", 7));
            syncExec.coll().insertOne(new Document("_id", 3).append("name", "n3").append("address", new Document("city", null)).append("count", 8));

            final com.landawn.abacus.da.mongodb.reactivestreams.MongoDB reactiveDB = new com.landawn.abacus.da.mongodb.reactivestreams.MongoDB(
                    reactiveClient.getDatabase(mongoDB.getName()));
            final com.landawn.abacus.da.mongodb.reactivestreams.MongoCollectionExecutor executor = reactiveDB.collectionExecutor(collName);
            final List<String> names = N.asList("name", "address.city");
            final Bson sort = new Document(_ID, 1);

            org.junit.jupiter.api.Assertions.assertAll(() -> {
                final IllegalArgumentException e = org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                        () -> executor.groupByAndCount("count", Map.class));
                assertEquals("Group field name 'count' conflicts with the count column of groupByAndCount; use Document as the row type", e.getMessage());
                assertEquals(N.asList(new Document(_ID, 7).append("count", 2), new Document(_ID, 8).append("count", 1)),
                        executor.groupByAndCount("count").sort(java.util.Comparator.comparing((Document doc) -> doc.getInteger(_ID))).collectList().block());
            }, () -> {
                for (final Dataset ds : N.asList(executor.query(names, Filters.exists("name"), sort, Map.class).block(),
                        executor.query(names, Filters.exists("name"), sort, 0, 10, Document.class).block(),
                        reactiveDB.collectionMapper(collName, Map.class).query(names, Filters.exists("name"), sort).block())) {
                    assertEquals(names, ds.columnNames());
                    assertEquals(N.asList("n1", "n2", "n3"), ds.getColumn("name"));
                    assertEquals(N.asList("Paris", null, null), ds.getColumn("address.city"));
                }
            }, () -> {
                syncExec.coll().insertOne(new Document("_id", 4).append("name", "n4").append("address", N.asList(new Document("city", "Rome"))));
                reactor.test.StepVerifier.create(executor.query(names, Filters.exists("name"), sort, Map.class)).expectError(ClassCastException.class).verify();
            });
        } finally {
            syncExec.coll().drop();
        }
    }

    @Test
    public void test_mapperDistinctCountsDocumentsLackingTheField_live_coverageME() {
        // Pins the mapper distinct(...) @return documented this round: unlike the native distinct command, documents that
        // lack the field contribute one more result, an entity whose field is null (or a null element for a single-value T).
        final MongoCollectionExecutor executor = dbExecutor.collectionExecutor("coverageME_e2e_distinct");
        executor.coll().drop();

        try {
            executor.coll().insertOne(new Document("city", "Paris").append("zip", 1));
            executor.coll().insertOne(new Document("city", "Rome").append("zip", 2));
            executor.coll().insertOne(new Document("zip", 3));

            final List<String> cities = dbExecutor.collectionMapper("coverageME_e2e_distinct", MongoCollectionExecutorTest.E2eAddress_coverageME.class)
                    .distinct("city")
                    .map(MongoCollectionExecutorTest.E2eAddress_coverageME::getCity)
                    .sortedBy(city -> city == null ? "" : city)
                    .toList();
            assertEquals(N.asList(null, "Paris", "Rome"), cities);

            final List<String> values = dbExecutor.collectionMapper("coverageME_e2e_distinct", String.class)
                    .distinct("city", Filters.gte("zip", 2))
                    .sortedBy(city -> city == null ? "" : city)
                    .toList();
            assertEquals(N.asList(null, "Rome"), values);
            assertEquals(N.asList("Paris", "Rome"), executor.distinct("city", String.class).sorted().toList());
        } finally {
            executor.coll().drop();
        }
    }

    public static class UuidBean_coverageME {
        private java.util.UUID uuid;

        public java.util.UUID getUuid() {
            return uuid;
        }

        public void setUuid(final java.util.UUID uuid) {
            this.uuid = uuid;
        }
    }

    public static class UuidHolder_coverageME {
        private String id;
        private UuidBean_coverageME inner;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public UuidBean_coverageME getInner() {
            return inner;
        }

        public void setInner(final UuidBean_coverageME inner) {
            this.inner = inner;
        }
    }

    public static class ValueTypes_coverageME {
        private String id;
        private java.util.GregorianCalendar calendar;
        private java.nio.ByteBuffer buffer;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public java.util.GregorianCalendar getCalendar() {
            return calendar;
        }

        public void setCalendar(final java.util.GregorianCalendar calendar) {
            this.calendar = calendar;
        }

        public java.nio.ByteBuffer getBuffer() {
            return buffer;
        }

        public void setBuffer(final java.nio.ByteBuffer buffer) {
            this.buffer = buffer;
        }
    }

    // ---- end 2026-10-04 coverageME ----

}
