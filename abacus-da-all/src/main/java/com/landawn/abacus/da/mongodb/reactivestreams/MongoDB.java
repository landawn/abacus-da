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
 * <p>This class extends {@link MongoDBBase} and integrates with MongoDB's
 * <strong>reactive streams</strong> driver
 * ({@link com.mongodb.reactivestreams.client.MongoDatabase}) to provide
 * Publisher-Subscriber based database operations. Every collection, executor, and mapper produced
 * by this class returns {@link org.reactivestreams.Publisher Publisher}-valued results rather than
 * blocking on the calling thread, which makes it suitable for reactive programming patterns,
 * backpressure handling, and integration with frameworks such as Project Reactor, RxJava, and
 * Akka Streams.</p>
 *
 * <p>For a blocking, synchronous variant built on the standard MongoDB driver, see
 * {@link com.landawn.abacus.da.mongodb.MongoDB}.</p>
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
 * MongoCollectionExecutor executor = reactiveMongoDB.collectionExecutor("users");
 * 
 * // With Project Reactor:
 * Flux<String> flux = Flux.from(executor.list(Filters.eq("status", "active")))
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
 * MongoCollectionMapper<User> mapper = reactiveMongoDB.collectionMapper(User.class);
 * Flux<User> users = Flux.from(mapper.list(Filters.eq("department", "Engineering")))
 *     .take(100)  // Limit processing
 *     .buffer(10) // Process in batches
 *     .flatMap(batch -> processBatch(batch));
 * }</pre>
 *
 * <p><strong>Important:</strong> Define an "id" property in Java entities to map to MongoDB's "_id" 
 * field for optimal integration.</p>
 *
 * @see com.landawn.abacus.da.mongodb.MongoDB
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
public final class MongoDB extends MongoDBBase {

    //    private final Map<String, MongoCollectionExecutor> collectionExecutorPool = new ConcurrentHashMap<>();
    //
    //    private final Map<Class<?>, MongoCollectionMapper<?>> collectionMapperPool = new ConcurrentHashMap<>();

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
     *
     * MongoDB reactiveMongoDB = new MongoDB(reactiveDb);          // non-null wrapper; wraps reactiveDb with the framework codec registry
     *
     * // Edge case - null database is rejected eagerly:
     * new MongoDB(null);                                         // throws IllegalArgumentException: 'mongoDB' cannot be null
     * }</pre>
     *
     * @param mongoDB the reactive MongoDB database instance to wrap
     * @throws IllegalArgumentException if {@code mongoDB} is {@code null}
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
     *
     * MongoDatabase db = reactiveMongoDB.db();                   // returns the codec-configured reactive MongoDatabase (never null)
     *
     * // Use reactive driver features directly (results are Reactive Streams Publishers, not abacus Stream):
     * Publisher<String> collectionNames = db.listCollectionNames();  // a cold Publisher; no I/O until subscribed
     *
     * // Repeated calls return the same wrapped instance:
     * boolean same = (db == reactiveMongoDB.db());              // returns true
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
     * MongoDB reactiveMongoDB = new MongoDB(reactiveDatabase);
     *
     * MongoCollection<Document> userCollection = reactiveMongoDB.collection("users");  // returns a reactive MongoCollection<Document> (never null)
     * Publisher<Document> findPublisher = userCollection.find();                       // a cold Publisher; no query runs until subscribed
     *
     * // Process with reactive streams (a Reactive Streams Publisher, not abacus Stream):
     * Flux.from(findPublisher)
     *     .subscribe(doc -> System.out.println(doc.toJson()));
     *
     * // The name need not pre-exist: MongoDB creates the collection lazily on first write.
     * MongoCollection<Document> brandNew = reactiveMongoDB.collection("not_yet_created");  // returns a handle; no server round-trip here
     *
     * // The collection name is validated up front, consistent with collectionExecutor(String):
     * reactiveMongoDB.collection((String) null);                                       // throws IllegalArgumentException ("collectionName")
     * }</pre>
     *
     * @param collectionName the name of the MongoDB collection to retrieve
     * @return a reactive MongoCollection configured for Document operations
     * @throws IllegalArgumentException if collectionName is null
     * @see Document
     * @see com.mongodb.reactivestreams.client.MongoCollection
     */
    public MongoCollection<Document> collection(final String collectionName) {
        N.checkArgNotNull(collectionName, "collectionName");

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
     * MongoDB reactiveMongoDB = new MongoDB(reactiveDatabase);
     *
     * MongoCollection<User> userCollection = reactiveMongoDB.collection("users", User.class);  // returns a reactive MongoCollection<User> (never null)
     * Publisher<User> userPublisher = userCollection.find();                                   // a cold Publisher; decoding uses the codec registry on subscription
     *
     * // Type-safe reactive processing (a Reactive Streams Publisher, not abacus Stream):
     * Flux.from(userPublisher)
     *     .filter(User::isActive)
     *     .map(User::getName)
     *     .subscribe(System.out::println);
     *
     * // Document.class is also valid and yields the same untyped view as collection(name):
     * MongoCollection<Document> raw = reactiveMongoDB.collection("users", Document.class);     // returns a Document-typed reactive collection
     *
     * // Both arguments are validated up front, consistent with collectionMapper(String, Class):
     * reactiveMongoDB.collection(null, User.class);                                           // throws IllegalArgumentException ("collectionName")
     * reactiveMongoDB.collection("users", (Class<User>) null);                                // throws IllegalArgumentException ("rowType")
     * }</pre>
     *
     * @param <T> the Java type for documents in this reactive collection
     * @param collectionName the name of the MongoDB collection to retrieve
     * @param rowType the Class object representing the document type
     * @return a reactive MongoCollection configured for the specified type
     * @throws IllegalArgumentException if collectionName or rowType is null
     * @see com.mongodb.reactivestreams.client.MongoCollection
     */
    public <T> MongoCollection<T> collection(final String collectionName, final Class<T> rowType) {
        N.checkArgNotNull(collectionName, "collectionName");
        N.checkArgNotNull(rowType, "rowType");

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
     * MongoDB reactiveMongoDB = new MongoDB(reactiveDatabase);
     *
     * MongoCollectionExecutor executor = reactiveMongoDB.collectionExecutor("users");  // returns a new executor wrapping the "users" collection (never null)
     *
     * // Reactive operations return Reactive Streams Publishers (Flux/Mono), not abacus Stream:
     * Flux<Document> findPublisher = Flux.from(executor.list(Filters.eq("status", "active")));    // cold Flux; query runs on subscription
     * Mono<InsertOneResult> insertPublisher = Mono.from(executor.insertOne(newUser));             // cold Mono; insert runs on subscription
     *
     * // Chain reactive operations:
     * executor.list(Filters.eq("priority", "high"))
     *     .take(10)
     *     .subscribe(doc -> processHighPriorityItem(doc));
     *
     * // Each call returns a distinct executor instance (no pooling):
     * boolean shared = (executor == reactiveMongoDB.collectionExecutor("users"));      // returns false
     *
     * // Edge case - null name is rejected eagerly:
     * reactiveMongoDB.collectionExecutor((String) null);                               // throws IllegalArgumentException: 'collectionName' cannot be null
     * }</pre>
     *
     * @param collectionName the name of the MongoDB collection
     * @return a reactive MongoCollectionExecutor for the specified collection
     * @throws IllegalArgumentException if collectionName is null
     * @see MongoCollectionExecutor
     * @see org.reactivestreams.Publisher
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
     * MongoDB reactiveMongoDB = new MongoDB(reactiveDatabase);
     * MongoCollection<Document> customCollection = reactiveDatabase
     *     .getCollection("users")
     *     .withReadPreference(ReadPreference.secondaryPreferred());
     *
     * MongoCollectionExecutor executor = reactiveMongoDB.collectionExecutor(customCollection);  // returns a new executor wrapping the supplied collection (never null)
     *
     * // The provided collection is wrapped as-is, so its custom settings are preserved:
     * boolean same = (executor.coll() == customCollection);                                    // returns true
     *
     * // All operations will use the custom collection settings; results are Reactive Streams Publishers:
     * Publisher<Long> countPublisher = executor.count();                                       // a cold Publisher; counts on subscription
     *
     * // Edge case - null collection is rejected eagerly:
     * reactiveMongoDB.collectionExecutor((MongoCollection<Document>) null);                    // throws IllegalArgumentException: 'collection' cannot be null
     * }</pre>
     *
     * @param collection the reactive MongoDB collection instance to wrap
     * @return a reactive MongoCollectionExecutor for the provided collection
     * @throws IllegalArgumentException if collection is null
     * @see MongoCollectionExecutor
     * @see com.mongodb.reactivestreams.client.MongoCollection
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
     * MongoDB reactiveMongoDB = new MongoDB(reactiveDatabase);
     *
     * MongoCollectionMapper<User> userMapper = reactiveMongoDB.collectionMapper(User.class);  // returns a mapper bound to the "User" collection (simple class name)
     *
     * // Reactive operations return a Reactive Streams Publisher (Flux/Mono), not abacus Stream:
     * Flux<User> userFlux = Flux.from(userMapper.list(Filters.eq("active", true)));            // cold Flux; query runs on subscription
     * userFlux
     *     .doOnNext(user -> System.out.println("Active user: " + user.getName()))
     *     .subscribe();
     *
     * // The collection name is derived from the SIMPLE class name, not the fully-qualified name:
     * MongoCollectionMapper<Order> orderMapper = reactiveMongoDB.collectionMapper(Order.class);  // uses "Order", not "com.example.Order"
     *
     * // Edge case - null rowType is rejected eagerly, consistent with collectionMapper(String, Class):
     * reactiveMongoDB.collectionMapper((Class<User>) null);                                  // throws IllegalArgumentException ("rowType")
     * }</pre>
     *
     * @param <T> the entity type for reactive mapping
     * @param rowType the Class object representing the entity type
     * @return a reactive MongoCollectionMapper for the specified entity type
     * @throws IllegalArgumentException if rowType is null
     * @see org.reactivestreams.Publisher
     */
    public <T> MongoCollectionMapper<T> collectionMapper(final Class<T> rowType) {
        N.checkArgNotNull(rowType, "rowType");

        return collectionMapper(ClassUtil.getSimpleClassName(rowType), rowType);
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
     * MongoDB reactiveMongoDB = new MongoDB(reactiveDatabase);
     *
     * MongoCollectionMapper<User> userMapper = reactiveMongoDB.collectionMapper("users", User.class);  // returns a mapper bound to the "users" collection (never null)
     *
     * // Reactive type-safe operations return Reactive Streams Publishers (Flux/Mono), not abacus Stream:
     * Flux<User> activeUsers = Flux.from(userMapper.list(Filters.eq("status", "active")));  // cold Flux; query runs on subscription
     * Mono<InsertOneResult> insertResult = Mono.from(userMapper.insertOne(newUser));        // cold Mono; insert runs on subscription
     *
     * // Process with backpressure:
     * activeUsers
     *     .buffer(100)  // Process in batches
     *     .subscribe(batch -> processBatch(batch));
     *
     * // Edge case - null collection name is rejected eagerly:
     * reactiveMongoDB.collectionMapper((String) null, User.class);                        // throws IllegalArgumentException: 'collectionName' cannot be null
     *
     * // Edge case - null rowType is rejected eagerly:
     * reactiveMongoDB.collectionMapper("users", (Class<User>) null);                      // throws IllegalArgumentException: 'rowType' cannot be null
     * }</pre>
     *
     * @param <T> the entity type for reactive object-document mapping
     * @param collectionName the name of the MongoDB collection to map
     * @param rowType the Class representing the entity type
     * @return a reactive MongoCollectionMapper configured for the specified type and collection
     * @throws IllegalArgumentException if collectionName or rowType is null
     * @see org.reactivestreams.Publisher
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
     * Creates a reactive MongoCollectionMapper for an existing reactive MongoCollection with custom configuration.
     *
     * <p>This method allows creation of a reactive mapper using a pre-configured reactive MongoCollection instance,
     * which is useful when you need specific collection-level settings like custom read preferences,
     * write concerns, or codec registries while maintaining reactive object-document mapping capabilities.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoDB reactiveMongoDB = new MongoDB(reactiveDatabase);
     * MongoCollection<Document> customCollection = reactiveDatabase
     *     .getCollection("users")
     *     .withWriteConcern(WriteConcern.MAJORITY)
     *     .withReadPreference(ReadPreference.primaryPreferred());
     *
     * MongoCollectionMapper<User> mapper = reactiveMongoDB.collectionMapper(customCollection, User.class);  // returns a mapper wrapping the supplied collection (never null)
     *
     * // The provided collection is wrapped as-is, so its custom settings are preserved:
     * boolean same = (mapper.collectionExecutor().coll() == customCollection);                            // returns true
     *
     * // All operations use custom collection settings; results are Reactive Streams Publishers, not abacus Stream:
     * Flux<User> users = Flux.from(mapper.list(Filters.eq("department", "Engineering")));                  // cold Flux; query runs on subscription
     *
     * // Edge case - null collection is rejected eagerly:
     * reactiveMongoDB.collectionMapper((MongoCollection<Document>) null, User.class);                    // throws IllegalArgumentException: 'collection' cannot be null
     *
     * // Edge case - null rowType is rejected eagerly:
     * reactiveMongoDB.collectionMapper(customCollection, (Class<User>) null);                            // throws IllegalArgumentException: 'rowType' cannot be null
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

    /**
     * Converts a BSON {@link Document} into an instance of {@code rowType} using the same mapping
     * rules as the synchronous variant. This {@code protected static} method is a bridge that
     * delegates to {@link MongoDBBase#readRow(Document, Class)}; it exists so reactive
     * collaborators in this package and subclasses can decode rows without referencing the
     * protected helper on the base class directly.
     *
     * @param <T> the target row type
     * @param row the BSON document to convert; if {@code null}, the helper's null-handling
     *            contract applies (the default value of {@code rowType} is returned, or
     *            {@code null} when {@code rowType} is {@code null})
     * @param rowType the target Java class to map the document to
     * @return an instance of {@code rowType} populated from {@code row}
     * @see MongoDBBase#readRow(Document, Class)
     */
    protected static <T> T readRow(final Document row, final Class<T> rowType) {
        return MongoDBBase.readRow(row, rowType);
    }

}
