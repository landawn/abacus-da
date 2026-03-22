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

package com.landawn.abacus.da.mongodb.reactivestreams;

import org.bson.Document;

import com.landawn.abacus.da.mongodb.MongoDBBase;
import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.N;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;

/**
 * Reactive MongoDB database executor providing reactive streams support for non-blocking database operations.
 *
 * <p>This class extends {@link MongoDBBase} and integrates with MongoDB's reactive streams driver to provide
 * Publisher-Subscriber based database operations. It supports reactive programming patterns, backpressure
 * handling, and seamless integration with reactive frameworks like Project Reactor, RxJava, and Akka Streams.</p>
 *
 * <h2>Key Features</h2>
 * <h3>Core Capabilities:</h3>
 * <ul>
 *   <li><strong>Reactive Streams:</strong> Full Publisher-Subscriber pattern support with backpressure</li>
 *   <li><strong>Non-blocking I/O:</strong> All operations return Publishers for asynchronous execution</li>
 *   <li><strong>Framework Integration:</strong> Compatible with Reactor, RxJava, and other reactive libraries</li>
 *   <li><strong>Stream Processing:</strong> Natural integration with stream processing pipelines</li>
 *   <li><strong>Resource Management:</strong> Automatic connection and cursor lifecycle management</li>
 *   <li><strong>Error Propagation:</strong> Reactive error handling through Publisher error signals</li>
 * </ul>
 *
 * <h3>Reactive Patterns:</h3>
 * <p>This executor supports various reactive patterns:</p>
 * <ul>
 *   <li><strong>Cold Publishers:</strong> Operations that start when subscribed to</li>
 *   <li><strong>Backpressure:</strong> Subscriber-controlled flow of data</li>
 *   <li><strong>Error Handling:</strong> Exception propagation through reactive streams</li>
 *   <li><strong>Completion Signals:</strong> Proper stream completion notification</li>
 * </ul>
 *
 * <h3>Thread Safety:</h3>
 * <p>This class is thread-safe. All operations can be called concurrently from multiple threads.
 * The underlying reactive streams implementation handles thread safety and concurrency.</p>
 *
 * <h3>Performance Considerations:</h3>
 * <ul>
 *   <li>Publishers are lazy - no work is done until subscription</li>
 *   <li>Backpressure prevents memory overflow with large result sets</li>
 *   <li>Connection pooling is managed by the MongoDB reactive driver</li>
 *   <li>Use proper Scheduler for computational work to avoid blocking I/O threads</li>
 * </ul>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Basic reactive setup:
 * MongoDB reactiveMongoDB = new MongoDB(reactiveDatabase);
 * MongoCollectionExecutor executor = reactiveMongoDB.collExecutor("users");
 * 
 * // With Project Reactor:
 * Flux<Document> flux = executor.list(Filters.eq("status", "active"))
 *     .filter(doc -> doc.getInteger("age") > 18)
 *     .map(doc -> doc.getString("name"))
 *     .doOnNext(System.out::println);
 *
 * // Subscribe to start processing:
 * Disposable subscription = flux.subscribe(
 *     name -> System.out.println("User: " + name),
 *     error -> System.err.println("Error: " + error),
 *     () -> System.out.println("Processing complete")
 * );
 *
 * // Type-safe reactive mapping:
 * MongoCollectionMapper<User> mapper = reactiveMongoDB.collMapper(User.class);
 * Flux<User> users = mapper.list(Filters.eq("department", "Engineering"))
 *     .take(100)  // Limit processing
 *     .buffer(10) // Process in batches
 *     .flatMap(batch -> processBatch(batch));
 * }</pre>
 *
 * <p><strong>Important:</strong> Define an "id" property in Java entities to map to MongoDB's "_id" 
 * field for optimal integration.</p>
 *
 * @see MongoDBBase
 * @see com.mongodb.reactivestreams.client.MongoDatabase
 * @see org.reactivestreams.Publisher
 * @see org.reactivestreams.Subscriber
 * @see com.mongodb.client.model.Filters
 * @see com.mongodb.client.model.Projections
 * @see com.mongodb.client.model.Sorts
 * @see com.mongodb.client.model.Updates
 * @see com.mongodb.client.model.Aggregates
 * @see com.mongodb.client.model.Indexes
 * @see <a href="https://www.mongodb.com/docs/drivers/java/reactive-streams/">MongoDB Reactive Streams Driver</a>
 * @see <a href="https://www.reactive-streams.org/">Reactive Streams Specification</a>
 */
@SuppressWarnings("java:S1192")
public final class MongoDB extends MongoDBBase {

    //    private final Map<String, MongoCollectionExecutor> collExecutorPool = new ConcurrentHashMap<>();
    //
    //    private final Map<Class<?>, MongoCollectionMapper<?>> collMapperPool = new ConcurrentHashMap<>();

    private final MongoDatabase mongoDatabase;

    /**
     * Constructs a reactive MongoDB executor with the specified reactive database instance.
     *
     * <p>This constructor initializes the reactive MongoDB wrapper with the MongoDB reactive streams
     * database. The database is configured with the framework's custom codec registry to support
     * automatic POJO mapping and reactive BSON type conversions.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoClient reactiveClient = MongoClients.create("mongodb://localhost:27017");
     * com.mongodb.reactivestreams.client.MongoDatabase reactiveDb =
     *     reactiveClient.getDatabase("myapp");
     * MongoDB reactiveMongoDB = new MongoDB(reactiveDb);
     * }</pre>
     *
     * @param mongoDB the reactive MongoDB database instance to wrap
     * @throws IllegalArgumentException if mongoDB is null
     * @see com.mongodb.reactivestreams.client.MongoDatabase
     */
    public MongoDB(final MongoDatabase mongoDB) {
        N.checkArgNotNull(mongoDB, "mongoDB");
        mongoDatabase = mongoDB.withCodecRegistry(codecRegistry);
    }

    /**
     * Returns the underlying reactive MongoDB database instance for advanced operations.
     *
     * <p>This method provides direct access to the MongoDB reactive streams driver's {@code MongoDatabase}
     * object, allowing for advanced reactive operations not directly exposed by this wrapper. The returned
     * database instance is pre-configured with the appropriate codec registry for POJO mapping.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoDB reactiveMongoDB = new MongoDB(reactiveDatabase);
     * MongoDatabase db = reactiveMongoDB.db();
     *
     * // Use reactive driver features directly:
     * Publisher<String> collectionNames = db.listCollectionNames();
     * }</pre>
     *
     * @return the reactive MongoDB database instance
     * @see com.mongodb.reactivestreams.client.MongoDatabase
     */
    public MongoDatabase db() {
        return mongoDatabase;
    }

    /**
     * Returns a reactive MongoDB collection as a Document-based Publisher collection.
     *
     * <p>This method returns a reactive collection that works with MongoDB's {@code Document} type
     * and returns Publishers for all operations. This provides maximum flexibility for dynamic
     * document structures and reactive processing pipelines.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollection<Document> userCollection = reactiveMongoDB.collection("users");
     * Publisher<Document> findPublisher = userCollection.find();
     *
     * // Process with reactive streams:
     * Flux.from(findPublisher)
     *     .subscribe(doc -> System.out.println(doc.toJson()));
     * }</pre>
     *
     * @param collectionName the name of the MongoDB collection to retrieve
     * @return a reactive MongoCollection configured for Document operations
     * @see Document
     * @see com.mongodb.reactivestreams.client.MongoCollection
     */
    public MongoCollection<Document> collection(final String collectionName) {
        return mongoDatabase.getCollection(collectionName);
    }

    /**
     * Returns a reactive MongoDB collection configured for a specific Java type with automatic POJO mapping.
     *
     * <p>This method returns a strongly-typed reactive collection that automatically converts between
     * MongoDB documents and Java objects using Publishers. The codec registry handles reactive
     * serialization and deserialization of POJOs with backpressure support.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollection<User> userCollection = reactiveMongoDB.collection("users", User.class);
     * Publisher<User> userPublisher = userCollection.find();
     *
     * // Type-safe reactive processing:
     * Flux.from(userPublisher)
     *     .filter(User::isActive)
     *     .map(User::getName)
     *     .subscribe(System.out::println);
     * }</pre>
     *
     * @param <T> the Java type for documents in this reactive collection
     * @param collectionName the name of the MongoDB collection to retrieve
     * @param rowType the Class object representing the document type
     * @return a reactive MongoCollection configured for the specified type
     * @see com.mongodb.reactivestreams.client.MongoCollection
     */
    public <T> MongoCollection<T> collection(final String collectionName, final Class<T> rowType) {
        return mongoDatabase.getCollection(collectionName, rowType);
    }

    /**
     * Creates a reactive MongoCollectionExecutor for performing Publisher-based operations on the specified collection.
     *
     * <p>The reactive MongoCollectionExecutor provides a higher-level API for common MongoDB reactive operations
     * including CRUD operations, aggregation pipelines, and bulk operations. All methods return Publishers
     * that integrate seamlessly with reactive streams frameworks.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionExecutor executor = reactiveMongoDB.collExecutor("users");
     *
     * // Reactive operations return Flux/Mono:
     * Flux<Document> findPublisher = executor.list(Filters.eq("status", "active"));
     * Mono<InsertOneResult> insertPublisher = executor.insertOne(newUser);
     *
     * // Chain reactive operations:
     * executor.list(Filters.eq("priority", "high"))
     *     .take(10)
     *     .subscribe(doc -> processHighPriorityItem(doc));
     * }</pre>
     *
     * @param collectionName the name of the MongoDB collection
     * @return a reactive MongoCollectionExecutor for the specified collection
     * @throws IllegalArgumentException if collectionName is null
     * @see MongoCollectionExecutor
     * @see org.reactivestreams.Publisher
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

        return new MongoCollectionExecutor(mongoDatabase.getCollection(collectionName));
    }

    /**
     * Creates a reactive MongoCollectionExecutor for performing Publisher-based operations on the provided collection instance.
     *
     * <p>This overload allows you to pass a pre-configured reactive MongoCollection instance,
     * which is useful when you need specific collection-level configurations like read preferences,
     * write concerns, or custom codec registries while maintaining reactive capabilities.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollection<Document> customCollection = reactiveDatabase
     *     .getCollection("users")
     *     .withReadPreference(ReadPreference.secondaryPreferred());
     *
     * MongoCollectionExecutor executor = reactiveMongoDB.collExecutor(customCollection);
     *
     * // All operations will use the custom collection settings:
     * Publisher<Long> countPublisher = executor.count();
     * }</pre>
     *
     * @param collection the reactive MongoDB collection instance to wrap
     * @return a reactive MongoCollectionExecutor for the provided collection
     * @throws IllegalArgumentException if collection is null
     * @see MongoCollectionExecutor
     * @see com.mongodb.reactivestreams.client.MongoCollection
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

        return new MongoCollectionExecutor(collection);
    }

    /**
     * Creates a reactive MongoCollectionMapper for object-document mapping with automatic collection naming.
     *
     * <p>This convenience method creates a reactive mapper using the simple class name as the collection name.
     * The mapper provides reactive object-document mapping capabilities, automatically converting between
     * Java objects and MongoDB documents with Publisher-based operations and proper backpressure handling.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> userMapper = reactiveMongoDB.collMapper(User.class);
     * // Uses "User" as collection name
     *
     * Publisher<User> userPublisher = userMapper.stream(Filters.eq("active", true));
     * Flux.from(userPublisher)
     *     .doOnNext(user -> System.out.println("Active user: " + user.getName()))
     *     .subscribe();
     * }</pre>
     *
     * @param <T> the entity type for reactive mapping
     * @param rowType the Class object representing the entity type
     * @return a reactive MongoCollectionMapper for the specified entity type
     * @throws IllegalArgumentException if rowType is null
     * @see org.reactivestreams.Publisher
     */
    public <T> MongoCollectionMapper<T> collMapper(final Class<T> rowType) {
        return collMapper(ClassUtil.getSimpleClassName(rowType), rowType);
    }

    /**
     * Creates a reactive MongoCollectionMapper for the specified collection and entity type.
     *
     * <p>This method creates a strongly-typed reactive collection mapper that provides object-document
     * mapping (ODM) capabilities for MongoDB collections with reactive streams support. The mapper handles
     * automatic conversion between Java entities and BSON documents with Publisher-based operations.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> userMapper = reactiveMongoDB.collMapper("users", User.class);
     *
     * // Reactive type-safe operations:
     * Publisher<User> activeUsers = userMapper.stream(Filters.eq("status", "active"));
     * Publisher<InsertOneResult> insertResult = userMapper.insertOne(newUser);
     *
     * // Process with backpressure:
     * Flux.from(activeUsers)
     *     .buffer(100)  // Process in batches
     *     .subscribe(batch -> processBatch(batch));
     * }</pre>
     *
     * @param <T> the entity type for reactive object-document mapping
     * @param collectionName the name of the MongoDB collection to map
     * @param rowType the Class representing the entity type
     * @return a reactive MongoCollectionMapper configured for the specified type and collection
     * @throws IllegalArgumentException if collectionName or rowType is null
     * @see org.reactivestreams.Publisher
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
     * Creates a reactive MongoCollectionMapper for an existing reactive MongoCollection with custom configuration.
     *
     * <p>This method allows creation of a reactive mapper using a pre-configured reactive MongoCollection instance,
     * which is useful when you need specific collection-level settings like custom read preferences,
     * write concerns, or codec registries while maintaining reactive object-document mapping capabilities.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollection<Document> customCollection = reactiveDatabase
     *     .getCollection("users")
     *     .withWriteConcern(WriteConcern.MAJORITY)
     *     .withReadPreference(ReadPreference.primaryPreferred());
     *
     * MongoCollectionMapper<User> mapper = reactiveMongoDB.collMapper(customCollection, User.class);
     *
     * // All operations use custom collection settings with reactive streams:
     * Flux<User> users = mapper.list(Filters.eq("department", "Engineering"));
     * }</pre>
     *
     * @param <T> the entity type for reactive object-document mapping
     * @param collection the pre-configured reactive MongoDB collection instance
     * @param rowType the Class representing the entity type for mapping
     * @return a reactive MongoCollectionMapper for the provided collection and type
     * @throws IllegalArgumentException if collection or rowType is null
     * @see com.mongodb.reactivestreams.client.MongoCollection
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

    protected static <T> T readRow(final Document row, final Class<T> rowType) {
        return MongoDBBase.readRow(row, rowType);
    }

}
