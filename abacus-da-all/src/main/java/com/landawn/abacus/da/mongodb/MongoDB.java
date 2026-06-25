/*
 * Copyright (C) 2015 HaiYang Li
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.landawn.abacus.da.mongodb;

import org.bson.Document;

import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.N;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * Synchronous MongoDB database executor that provides a simplified interface for MongoDB operations.
 *
 * <p>This class extends {@link MongoDBBase} and wraps the <strong>synchronous</strong> MongoDB
 * Java driver ({@link com.mongodb.client.MongoDatabase}) to provide convenient methods for
 * database operations including collection management, document CRUD operations, and query
 * execution. Calls made through the executors and mappers returned by this class block the
 * calling thread until the MongoDB driver returns a result (or throws).</p>
 *
 * <p>For a non-blocking, Publisher-based variant built on the MongoDB reactive streams driver,
 * see {@link com.landawn.abacus.da.mongodb.reactivestreams.MongoDB}.</p>
 *
 * <p>An {@link AsyncExecutor} is held internally and is used by {@link MongoCollectionExecutor}
 * to expose convenience asynchronous wrappers around the otherwise-synchronous driver calls;
 * it does not change the blocking nature of the underlying driver itself.</p>
 *
 * <p><strong>Important:</strong> We recommend defining an "id" property in Java entities/beans to map to the MongoDB "_id"
 * field to keep things as simple as possible.</p>
 *
 * @see com.landawn.abacus.da.mongodb.reactivestreams.MongoDB
 * @see com.mongodb.client.model.Filters
 * @see com.mongodb.client.model.Projections
 * @see com.mongodb.client.model.Sorts
 * @see com.mongodb.client.model.Updates
 * @see com.mongodb.client.model.Aggregates
 * @see com.mongodb.client.model.Indexes
 * @see <a href="https://www.mongodb.com/docs/drivers/java/sync/current/fundamentals/builders/">Simplify your Code with Builders</a>
 * @see <a href="https://www.mongodb.com/docs/drivers/java/sync/current/">MongoDB Java Driver</a>
 */
public final class MongoDB extends MongoDBBase {

    //    private final Map<String, MongoCollectionExecutor> collectionExecutorPool = new ConcurrentHashMap<>();
    //
    //    private final Map<Class<?>, MongoCollectionMapper<?>> collectionMapperPool = new ConcurrentHashMap<>();

    private final MongoDatabase mongoDatabase;

    private final AsyncExecutor asyncExecutor;

    /**
     * Constructs a MongoDB executor with the specified database and the framework's default
     * async executor.
     *
     * <p>Equivalent to calling {@link #MongoDB(MongoDatabase, AsyncExecutor)} with the shared
     * default {@code AsyncExecutor}. The database is wrapped with the framework's custom codec
     * registry so that automatic POJO mapping and BSON type conversions are available on every
     * collection obtained from this instance.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoClient client = MongoClients.create("mongodb://localhost:27017");
     * MongoDatabase database = client.getDatabase("myapp");
     * MongoDB mongoDB = new MongoDB(database);                 // wraps "myapp", uses DEFAULT_ASYNC_EXECUTOR
     * MongoDatabase wrapped = mongoDB.db();                    // codec-configured database (not necessarily the same instance passed in)
     *
     * // Edge case: a null database is rejected eagerly.
     * MongoDB bad = new MongoDB((MongoDatabase) null);         // throws IllegalArgumentException
     * }</pre>
     *
     * @param mongoDB the MongoDB database instance to wrap
     * @throws IllegalArgumentException if {@code mongoDB} is {@code null}
     * @see #MongoDB(MongoDatabase, AsyncExecutor)
     * @see MongoDatabase
     * @see AsyncExecutor
     */
    public MongoDB(final MongoDatabase mongoDB) {
        this(mongoDB, DEFAULT_ASYNC_EXECUTOR);
    }

    /**
     * Constructs a MongoDB executor with the specified database and custom async executor.
     *
     * <p>This constructor allows specification of a custom AsyncExecutor for controlling
     * asynchronous operation characteristics such as thread pool size, timeout behavior,
     * and execution policies. The database is configured with POJO codec support.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncExecutor customExecutor = new AsyncExecutor(10, 50, 300L, TimeUnit.SECONDS);
     * MongoDB mongoDB = new MongoDB(database, customExecutor); // both args wired in; collection executors use customExecutor for async wrappers
     *
     * // Edge cases: both arguments must be non-null.
     * new MongoDB((MongoDatabase) null, customExecutor);       // throws IllegalArgumentException
     * new MongoDB(database, (AsyncExecutor) null);             // throws IllegalArgumentException
     * }</pre>
     *
     * @param mongoDB the MongoDB database instance to wrap
     * @param asyncExecutor the async executor used by {@link MongoCollectionExecutor} to expose
     *                      asynchronous wrappers around the synchronous driver calls
     * @throws IllegalArgumentException if {@code mongoDB} or {@code asyncExecutor} is {@code null}
     * @see MongoDatabase
     * @see AsyncExecutor
     */
    public MongoDB(final MongoDatabase mongoDB, final AsyncExecutor asyncExecutor) {
        super();
        N.checkArgNotNull(mongoDB, "mongoDB");
        N.checkArgNotNull(asyncExecutor, "asyncExecutor");
        mongoDatabase = mongoDB.withCodecRegistry(codecRegistry);
        this.asyncExecutor = asyncExecutor;
    }

    /**
     * Returns the underlying MongoDB database instance.
     *
     * <p>This method provides direct access to the MongoDB Java driver's {@code MongoDatabase} object,
     * allowing for advanced operations not directly exposed by this wrapper. The returned database
     * instance is pre-configured with the appropriate codec registry for POJO mapping.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoDatabase database = mongoDB.db();                  // never null; the codec-configured database backing this executor
     * database.createCollection("newCollection");             // direct driver call, bypassing this wrapper
     *
     * // Successive calls return the same configured instance.
     * boolean same = (mongoDB.db() == mongoDB.db());           // true
     * }</pre>
     *
     * @return the MongoDB database instance
     * @see MongoDatabase
     */
    public MongoDatabase db() {
        return mongoDatabase;
    }

    /**
     * Returns a MongoDB collection as a Document-based collection.
     *
     * <p>This method returns a collection that works with MongoDB's {@code Document} type,
     * which is a flexible representation of BSON documents. This is ideal for schema-less
     * operations or when working with dynamic document structures.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollection<Document> users = mongoDB.collection("users");  // never null; a handle even if the collection does not yet exist
     * users.insertOne(new Document("name", "John").append("age", 30));
     *
     * // The collection name is validated up front, consistent with collectionExecutor(String):
     * mongoDB.collection(null);                               // throws IllegalArgumentException ("collectionName")
     * }</pre>
     *
     * @param collectionName the name of the MongoDB collection to retrieve
     * @return a MongoCollection configured for Document operations
     * @throws IllegalArgumentException if collectionName is null
     * @see Document
     * @see MongoCollection
     */
    public MongoCollection<Document> collection(final String collectionName) {
        N.checkArgNotNull(collectionName, "collectionName");

        return mongoDatabase.getCollection(collectionName);
    }

    /**
     * Returns a MongoDB collection configured for a specific Java type with automatic POJO mapping.
     *
     * <p>This method returns a strongly-typed collection that automatically converts between
     * MongoDB documents and Java objects. The codec registry handles the serialization and
     * deserialization of POJOs, providing type safety and eliminating manual mapping code.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollection<User> userColl = mongoDB.collection("users", User.class); // never null; typed handle, POJO codec applied
     * userColl.insertOne(new User("Alice", "alice@example.com"));
     *
     * // Document is just the special case where rowType is Document.class.
     * MongoCollection<Document> raw = mongoDB.collection("users", Document.class); // equivalent to collection("users")
     *
     * // Both arguments are validated up front, consistent with collectionMapper(String, Class):
     * mongoDB.collection(null, User.class);                  // throws IllegalArgumentException ("collectionName")
     * mongoDB.collection("users", (Class<User>) null);       // throws IllegalArgumentException ("rowType")
     * }</pre>
     *
     * @param <T> the Java type for documents in this collection
     * @param collectionName the name of the MongoDB collection to retrieve
     * @param rowType the Class object representing the document type
     * @return a MongoCollection configured for the specified type
     * @throws IllegalArgumentException if collectionName or rowType is null
     * @see MongoCollection
     */
    public <T> MongoCollection<T> collection(final String collectionName, final Class<T> rowType) {
        N.checkArgNotNull(collectionName, "collectionName");
        N.checkArgNotNull(rowType, "rowType");

        return mongoDatabase.getCollection(collectionName, rowType);
    }

    /**
     * Creates a MongoCollectionExecutor for performing operations on the specified collection.
     *
     * <p>The MongoCollectionExecutor provides a higher-level API for common MongoDB operations
     * including CRUD operations, aggregation pipelines, and bulk operations. It includes both
     * synchronous and asynchronous operation support.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionExecutor executor = mongoDB.collectionExecutor("users"); // never null; a fresh executor each call (not pooled)
     * long count = executor.count(Filters.eq("status", "active"));
     *
     * // Unlike collection(String), this overload validates its argument up front.
     * mongoDB.collectionExecutor((String) null);             // throws IllegalArgumentException ("collectionName")
     * }</pre>
     *
     * @param collectionName the name of the MongoDB collection
     * @return a MongoCollectionExecutor for the specified collection
     * @throws IllegalArgumentException if collectionName is null
     * @see MongoCollectionExecutor
     */
    public MongoCollectionExecutor collectionExecutor(final String collectionName) {
        N.checkArgNotNull(collectionName, "collectionName");

        //    MongoCollectionExecutor collectionExecutor = collectionExecutorPool.get(collectionName);
        //
        //    if (collectionExecutor == null) {
        //        collectionExecutor = new MongoCollectionExecutor(mongoDatabase.getCollection(collectionName), asyncExecutor);
        //        collectionExecutorPool.put(collectionName, collectionExecutor);
        //    }
        //
        //    return collectionExecutor;

        return new MongoCollectionExecutor(mongoDatabase.getCollection(collectionName), asyncExecutor);
    }

    /**
     * Creates a MongoCollectionExecutor for performing operations on the provided collection instance.
     *
     * <p>This overload allows you to pass an already configured MongoCollection instance,
     * which is useful when you need specific collection-level configurations like read preferences,
     * write concerns, or custom codec registries.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollection<Document> coll = database.getCollection("users").withWriteConcern(WriteConcern.MAJORITY);
     * MongoCollectionExecutor executor = mongoDB.collectionExecutor(coll); // never null; wraps the supplied collection as-is
     *
     * // Edge case: a null collection is rejected.
     * mongoDB.collectionExecutor((MongoCollection<Document>) null); // throws IllegalArgumentException ("collection")
     * }</pre>
     *
     * @param collection the MongoDB collection instance to wrap
     * @return a MongoCollectionExecutor for the provided collection
     * @throws IllegalArgumentException if collection is null
     * @see MongoCollectionExecutor
     * @see MongoCollection
     */
    public MongoCollectionExecutor collectionExecutor(final MongoCollection<Document> collection) {
        N.checkArgNotNull(collection, "collection");

        //    MongoCollectionExecutor collectionExecutor = collectionExecutorPool.get(collectionName);
        //
        //    if (collectionExecutor == null) {
        //        collectionExecutor = new MongoCollectionExecutor(mongoDatabase.getCollection(collectionName), asyncExecutor);
        //        collectionExecutorPool.put(collectionName, collectionExecutor);
        //    }
        //
        //    return collectionExecutor;

        return new MongoCollectionExecutor(collection, asyncExecutor);
    }

    /**
     * Creates a MongoCollectionMapper for object-document mapping with automatic collection naming.
     *
     * <p>This convenience method creates a mapper using the simple class name as the collection name.
     * The mapper provides object-document mapping capabilities, automatically converting between
     * Java objects and MongoDB documents with proper type handling.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collectionMapper(User.class);       // collection name = simple class name "User"
     * Optional<User> user = mapper.findFirst(Filters.eq("email", "john@example.com")); // abacus u.Optional; empty when no match
     *
     * // Equivalent explicit-name form:
     * mongoDB.collectionMapper(User.class);                  // same as mongoDB.collectionMapper("User", User.class)
     *
     * // Edge case: a null type is rejected eagerly, consistent with collectionMapper(String, Class):
     * mongoDB.collectionMapper((Class<User>) null);          // throws IllegalArgumentException ("rowType")
     * }</pre>
     *
     * @param <T> the entity type for mapping
     * @param rowType the Class object representing the entity type
     * @return a MongoCollectionMapper for the specified entity type
     * @throws IllegalArgumentException if rowType is null
     * @see MongoCollectionMapper
     */
    public <T> MongoCollectionMapper<T> collectionMapper(final Class<T> rowType) {
        N.checkArgNotNull(rowType, "rowType");

        return collectionMapper(ClassUtil.getSimpleClassName(rowType), rowType);
    }

    /**
     * Creates a MongoCollectionMapper for the specified collection and entity type.
     *
     * <p>This method creates a strongly-typed collection mapper that provides object-document
     * mapping capabilities for MongoDB collections. The mapper handles automatic conversion
     * between Java entities and BSON documents, including ID field mapping and type conversions.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> userMapper = mongoDB.collectionMapper("users", User.class); // never null
     *
     * // Use mapper for type-safe operations:
     * Optional<User> user = userMapper.findFirst(Filters.eq("active", true)); // abacus u.Optional; empty when no match
     * List<User> users = userMapper.stream(Filters.gte("age", 18)).toList();  // abacus Stream -> List (empty list, not null, when none)
     *
     * // Edge cases: both arguments are validated.
     * mongoDB.collectionMapper((String) null, User.class);   // throws IllegalArgumentException ("collectionName")
     * mongoDB.collectionMapper("users", (Class<User>) null); // throws IllegalArgumentException ("rowType")
     * }</pre>
     *
     * @param <T> the entity type for object-document mapping
     * @param collectionName the name of the MongoDB collection to map
     * @param rowType the Class representing the entity type
     * @return a MongoCollectionMapper configured for the specified type and collection
     * @throws IllegalArgumentException if collectionName or rowType is null
     * @see MongoCollectionMapper
     * @see #collectionMapper(Class)
     */
    @SuppressWarnings("rawtypes")
    public <T> MongoCollectionMapper<T> collectionMapper(final String collectionName, final Class<T> rowType) {
        N.checkArgNotNull(collectionName, "collectionName");
        N.checkArgNotNull(rowType, "rowType");

        //    MongoCollectionMapper collectionMapper = collectionMapperPool.get(rowType);
        //
        //    if (collectionMapper == null) {
        //        collectionMapper = new MongoCollectionMapper(collectionExecutor(collectionName), rowType);
        //
        //        collectionMapperPool.put(rowType, collectionMapper);
        //    }
        //
        //    return collectionMapper;

        return new MongoCollectionMapper(collectionExecutor(collectionName), rowType);
    }

    /**
     * Creates a MongoCollectionMapper for an existing MongoCollection with custom configuration.
     *
     * <p>This method allows creation of a mapper using a pre-configured MongoCollection instance,
     * which is useful when you need specific collection-level settings like custom read preferences,
     * write concerns, or codec registries while maintaining the benefits of object-document mapping.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollection<Document> customCollection = database
     *     .getCollection("users")
     *     .withReadPreference(ReadPreference.secondaryPreferred())
     *     .withWriteConcern(WriteConcern.MAJORITY);
     *
     * MongoCollectionMapper<User> mapper = mongoDB.collectionMapper(customCollection, User.class); // never null; honors the collection's read/write settings
     *
     * // Edge cases: both arguments are validated.
     * mongoDB.collectionMapper((MongoCollection<Document>) null, User.class); // throws IllegalArgumentException ("collection")
     * mongoDB.collectionMapper(customCollection, (Class<User>) null);         // throws IllegalArgumentException ("rowType")
     * }</pre>
     *
     * @param <T> the entity type for object-document mapping
     * @param collection the pre-configured MongoDB collection instance
     * @param rowType the Class representing the entity type for mapping
     * @return a MongoCollectionMapper for the provided collection and type
     * @throws IllegalArgumentException if collection or rowType is null
     * @see MongoCollectionMapper
     * @see MongoCollection
     */
    @SuppressWarnings("rawtypes")
    public <T> MongoCollectionMapper<T> collectionMapper(final MongoCollection<Document> collection, final Class<T> rowType) {
        N.checkArgNotNull(collection, "collection");
        N.checkArgNotNull(rowType, "rowType");

        //    MongoCollectionMapper collectionMapper = collectionMapperPool.get(rowType);
        //
        //    if (collectionMapper == null) {
        //        collectionMapper = new MongoCollectionMapper(collectionExecutor(collectionName), rowType);
        //
        //        collectionMapperPool.put(rowType, collectionMapper);
        //    }
        //
        //    return collectionMapper;

        return new MongoCollectionMapper(collectionExecutor(collection), rowType);
    }
}
