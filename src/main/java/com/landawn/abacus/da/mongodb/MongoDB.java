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
 * <p>This class extends {@link MongoDBBase} and wraps the MongoDB Java client to provide convenient methods for
 * database operations including collection management, document CRUD operations, and query execution.</p>
 * 
 * <p><strong>Important:</strong> We recommend defining an "id" property in Java entities/beans to map to the MongoDB "_id" 
 * field to keep things as simple as possible.</p>
 *
 * @see com.mongodb.client.model.Filters
 * @see com.mongodb.client.model.Projections  
 * @see com.mongodb.client.model.Sorts
 * @see com.mongodb.client.model.Updates
 * @see com.mongodb.client.model.Aggregates
 * @see com.mongodb.client.model.Indexes
 * @see <a href="https://www.mongodb.com/docs/drivers/java/sync/current/fundamentals/builders/">Simplify your Code with Builders</a>
 * @see <a href="https://www.mongodb.com/docs/drivers/java/sync/current/">MongoDB Java Driver</a>
 */
@SuppressWarnings("java:S1192")
public final class MongoDB extends MongoDBBase {

    //    private final Map<String, MongoCollectionExecutor> collExecutorPool = new ConcurrentHashMap<>();
    //
    //    private final Map<Class<?>, MongoCollectionMapper<?>> collMapperPool = new ConcurrentHashMap<>();

    private final MongoDatabase mongoDatabase;

    private final AsyncExecutor asyncExecutor;

    /**
     * Constructs a MongoDB executor with the specified database and default async executor.
     *
     * <p>This constructor initializes the MongoDB wrapper with a default asynchronous executor
     * for non-blocking operations. The database is configured with the framework's custom codec
     * registry to support automatic POJO mapping and BSON type conversions.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoClient client = MongoClients.create("mongodb://localhost:27017");
     * MongoDatabase database = client.getDatabase("myapp");
     * MongoDB mongoDB = new MongoDB(database);
     * }</pre>
     *
     * @param mongoDB the MongoDB database instance to wrap
     * @throws IllegalArgumentException if mongoDB is null
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
     * MongoDB mongoDB = new MongoDB(database, customExecutor);
     * }</pre>
     *
     * @param mongoDB the MongoDB database instance to wrap
     * @param asyncExecutor the async executor for non-blocking operations
     * @throws IllegalArgumentException if mongoDB or asyncExecutor is null
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
     * MongoDatabase database = mongoDB.db();
     * database.createCollection("newCollection");
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
     * MongoCollection<Document> users = mongoDB.collection("users");
     * users.insertOne(new Document("name", "John").append("age", 30));
     * }</pre>
     *
     * @param collectionName the name of the MongoDB collection to retrieve
     * @return a MongoCollection configured for Document operations
     * @see Document
     * @see MongoCollection
     */
    public MongoCollection<Document> collection(final String collectionName) {
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
     * MongoCollection<User> userColl = mongoDB.collection("users", User.class);
     * userColl.insertOne(new User("Alice", "alice@example.com"));
     * }</pre>
     *
     * @param <T> the Java type for documents in this collection
     * @param collectionName the name of the MongoDB collection to retrieve
     * @param rowType the Class object representing the document type
     * @return a MongoCollection configured for the specified type
     * @see MongoCollection
     */
    public <T> MongoCollection<T> collection(final String collectionName, final Class<T> rowType) {
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
     * MongoCollectionExecutor executor = mongoDB.collExecutor("users");
     * long count = executor.count(Filters.eq("status", "active"));
     * }</pre>
     *
     * @param collectionName the name of the MongoDB collection
     * @return a MongoCollectionExecutor for the specified collection
     * @see MongoCollectionExecutor
     */
    public MongoCollectionExecutor collExecutor(final String collectionName) {
        N.checkArgNotNull(collectionName, "collectionName");

        //    MongoCollectionExecutor collExecutor = collExecutorPool.get(collectionName);
        //
        //    if (collExecutor == null) {
        //        collExecutor = new MongoCollectionExecutor(mongoDatabase.getCollection(collectionName), asyncExecutor);
        //        collExecutorPool.put(collectionName, collExecutor);
        //    }
        //
        //    return collExecutor;

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
     * MongoCollectionExecutor executor = mongoDB.collExecutor(coll);
     * }</pre>
     *
     * @param collection the MongoDB collection instance to wrap
     * @return a MongoCollectionExecutor for the provided collection
     * @see MongoCollectionExecutor
     * @see MongoCollection
     */
    public MongoCollectionExecutor collExecutor(final MongoCollection<Document> collection) {
        N.checkArgNotNull(collection, "collection");

        //    MongoCollectionExecutor collExecutor = collExecutorPool.get(collectionName);
        //
        //    if (collExecutor == null) {
        //        collExecutor = new MongoCollectionExecutor(mongoDatabase.getCollection(collectionName), asyncExecutor);
        //        collExecutorPool.put(collectionName, collExecutor);
        //    }
        //
        //    return collExecutor;

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
     * MongoCollectionMapper<User> mapper = mongoDB.collMapper(User.class);
     * Optional<User> user = mapper.findFirst(Filters.eq("email", "john@example.com"));
     * }</pre>
     *
     * @param <T> the entity type for mapping
     * @param rowType the Class object representing the entity type
     * @return a MongoCollectionMapper for the specified entity type
     * @see MongoCollectionMapper
     */
    public <T> MongoCollectionMapper<T> collMapper(final Class<T> rowType) {
        return collMapper(ClassUtil.getSimpleClassName(rowType), rowType);
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
     * MongoCollectionMapper<User> userMapper = mongoDB.collMapper("users", User.class);
     *
     * // Use mapper for type-safe operations:
     * Optional<User> user = userMapper.findFirst(Filters.eq("active", true));
     * List<User> users = userMapper.stream(Filters.gte("age", 18)).toList();
     * }</pre>
     *
     * @param <T> the entity type for object-document mapping
     * @param collectionName the name of the MongoDB collection to map
     * @param rowType the Class representing the entity type
     * @return a MongoCollectionMapper configured for the specified type and collection
     * @throws IllegalArgumentException if collectionName or rowType is null
     * @see MongoCollectionMapper
     * @see #collMapper(Class)
     */
    @SuppressWarnings("rawtypes")
    public <T> MongoCollectionMapper<T> collMapper(final String collectionName, final Class<T> rowType) {
        N.checkArgNotNull(collectionName, "collectionName");
        N.checkArgNotNull(rowType, "rowType");

        //    MongoCollectionMapper collMapper = collMapperPool.get(rowType);
        //
        //    if (collMapper == null) {
        //        collMapper = new MongoCollectionMapper(collExecutor(collectionName), rowType);
        //
        //        collMapperPool.put(rowType, collMapper);
        //    }
        //
        //    return collMapper;

        return new MongoCollectionMapper(collExecutor(collectionName), rowType);
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
     * MongoCollectionMapper<User> mapper = mongoDB.collMapper(customCollection, User.class);
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
    public <T> MongoCollectionMapper<T> collMapper(final MongoCollection<Document> collection, final Class<T> rowType) {
        N.checkArgNotNull(collection, "collection");
        N.checkArgNotNull(rowType, "rowType");

        //    MongoCollectionMapper collMapper = collMapperPool.get(rowType);
        //
        //    if (collMapper == null) {
        //        collMapper = new MongoCollectionMapper(collExecutor(collectionName), rowType);
        //
        //        collMapperPool.put(rowType, collMapper);
        //    }
        //
        //    return collMapper;

        return new MongoCollectionMapper(collExecutor(collection), rowType);
    }
}
