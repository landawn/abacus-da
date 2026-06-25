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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.landawn.abacus.annotation.Beta;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.N;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Reactive, type-safe MongoDB collection mapper providing object-document mapping over the MongoDB
 * Reactive Streams driver.
 *
 * <p>This class is the reactive counterpart of
 * {@link com.landawn.abacus.da.mongodb.MongoCollectionMapper} (in the parent package). It wraps a
 * reactive {@link MongoCollectionExecutor} and binds a fixed entity type {@code T} so callers never
 * have to repeat the target class on each call. Every operation returns a Project Reactor
 * {@link Mono} or {@link Flux} (both implementations of the Reactive Streams {@code Publisher}); the
 * returned publishers are <b>cold</b> — no database I/O is performed until they are subscribed to,
 * and each subscription triggers a fresh request honouring downstream demand (back-pressure) per the
 * Reactive Streams contract.</p>
 *
 * <h2>Key Features</h2>
 * <ul>
 *   <li><strong>Reactive type safety:</strong> Every method returns a {@code Mono<...>} or
 *       {@code Flux<T>} typed against the mapper's entity type, so no per-call {@code Class<T>}
 *       parameter is needed.</li>
 *   <li><strong>Automatic conversion:</strong> Entities are encoded/decoded by the driver's
 *       configured codec registry; ID fields annotated with {@code @Id} (or named {@code id}) are
 *       mapped to MongoDB's {@code _id}.</li>
 *   <li><strong>Back-pressure support:</strong> Streaming {@code Flux} operations propagate
 *       Reactive Streams {@code Subscription.request(n)} demand to the driver.</li>
 *   <li><strong>Error propagation:</strong> Database errors are signalled via
 *       {@code Subscriber.onError} rather than thrown synchronously.</li>
 *   <li><strong>Framework integration:</strong> Returned types are interoperable with any
 *       Reactive Streams consumer (Project Reactor, RxJava 3, SmallRye Mutiny, &hellip;).</li>
 * </ul>
 *
 * <h3>Subscription &amp; lifecycle</h3>
 * <p>The methods of this class merely build a publisher; the MongoDB call only begins on
 * subscription. Subscribing a returned publisher twice will issue the underlying MongoDB request
 * twice. Use {@link Mono#cache()}/{@link Flux#cache()} or {@code share()} to multicast a single
 * execution when needed.</p>
 *
 * <h3>Reactive Entity Requirements:</h3>
 * <p>Entity classes used with this reactive mapper should follow these conventions:</p>
 * <ul>
 *   <li>Have a default (no-argument) constructor for reactive instantiation</li>
 *   <li>Use proper getter/setter methods for reactive property access</li>
 *   <li>Mark ID fields with @Id annotation or use "id" property name</li>
 *   <li>Use MongoDB-compatible data types for reactive serialization</li>
 *   <li>Be thread-safe if used across reactive streams boundaries</li>
 * </ul>
 *
 * <h3>Thread Safety:</h3>
 * <p>This class is thread-safe. All operations can be called concurrently from multiple threads.
 * The underlying reactive executor handles thread safety and resource management.</p>
 *
 * <h3>Performance Considerations:</h3>
 * <ul>
 *   <li>Publishers are lazy - object conversion happens only on subscription</li>
 *   <li>Use reactive backpressure to control object creation rate</li>
 *   <li>Consider using projection to limit fields for large objects</li>
 *   <li>Batch operations are more efficient than individual reactive entity operations</li>
 *   <li>Use appropriate schedulers to avoid blocking reactive threads</li>
 * </ul>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Entity class definition:
 * public class User {
 *     @Id
 *     private String id;
 *     private String name;
 *     private String email;
 *     private Date createdAt;
 *     // getters and setters...
 * }
 * 
 * // Create reactive mapper:
 * MongoCollectionMapper<User> userMapper = reactiveMongoDB.collectionMapper("users", User.class);
 * 
 * // Reactive type-safe operations with Project Reactor:
 * // (insertOne returns a cold Mono; the insert only runs once it is subscribed)
 * userMapper.insertOne(new User("John", "john@example.com")).subscribe();
 *
 * Flux<User> activeUsers = userMapper.list(Filters.eq("status", "active"))
 *     .filter(user -> user.getCreatedAt().after(cutoffDate))
 *     .take(100)  // Backpressure control
 *     .doOnNext(user -> System.out.println("Processing user: " + user.getName()));
 * 
 * // Reactive aggregation with type conversion:
 * Mono<Long> userCount = userMapper.count(Filters.eq("department", "Engineering"))
 *     .doOnSuccess(count -> System.out.println("Engineering users: " + count));
 * 
 * // Error handling with reactive streams:
 * Flux<User> users = userMapper.list(Filters.empty())
 *     .onErrorResume(MongoException.class, error -> {
 *         System.err.println("Database error: " + error.getMessage());
 *         return Flux.empty();   // Graceful degradation
 *     });
 * 
 * // Subscribe to execute reactive pipeline:
 * users.subscribe(
 *     user -> processUser(user),
 *     error -> handleError(error),
 *     () -> System.out.println("All users processed")
 * );
 * }</pre>
 *
 * @param <T> the entity type for reactive object-document mapping operations
 * @see MongoCollectionExecutor
 * @see com.landawn.abacus.annotation.Id
 * @see org.reactivestreams.Publisher
 * @see org.reactivestreams.Subscriber
 * @see reactor.core.publisher.Mono
 * @see reactor.core.publisher.Flux
 * @see com.mongodb.client.model.Filters
 * @see com.mongodb.client.model.Projections
 * @see com.mongodb.client.model.Sorts
 * @see com.mongodb.client.model.Updates
 * @see com.mongodb.client.model.Aggregates
 * @see com.mongodb.client.model.Indexes
 * @see <a href="https://www.mongodb.com/docs/drivers/java/reactive-streams/">MongoDB Reactive Streams Driver</a>
 * @see <a href="https://www.reactive-streams.org/">Reactive Streams Specification</a>
 */
public final class MongoCollectionMapper<T> {

    private final MongoCollectionExecutor collectionExecutor;

    private final Class<T> rowType;

    /**
     * Package-private constructor; instances are obtained via
     * {@link MongoDB#collectionMapper(Class)} (and its overloads such as
     * {@code collectionMapper(String, Class)}) rather than direct instantiation.
     *
     * <p>Binds this mapper to a specific reactive collection executor and entity type. The
     * {@code resultClass} becomes the implicit decode target for every {@code Mono}/{@code Flux}
     * returned by this mapper.</p>
     *
     * @param collectionExecutor the reactive collection executor to use for operations
     * @param resultClass the {@link Class} representing the entity type {@code T} for mapping
     */
    MongoCollectionMapper(final MongoCollectionExecutor collectionExecutor, final Class<T> resultClass) {
        this.collectionExecutor = collectionExecutor;
        rowType = resultClass;
    }

    /**
     * Returns the underlying reactive MongoCollectionExecutor for advanced operations.
     *
     * <p>This method provides access to the lower-level reactive executor, allowing for advanced
     * operations not directly exposed by this mapper while maintaining reactive capabilities.
     * This is a pure accessor: it performs no database I/O and simply returns the executor instance
     * that was supplied when this mapper was created.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> userMapper = reactiveMongoDB.collectionMapper("users", User.class);
     *
     * // Typical: reach the executor for an overload not exposed on the mapper, e.g. a typed
     * // distinct that decodes scalar values rather than the mapper's entity type T.
     * MongoCollectionExecutor exec = userMapper.collectionExecutor();   // returns the backing executor (never null)
     * exec.distinct("country", String.class)
     *     .collectList()
     *     .subscribe(countries -> System.out.println("Countries: " + countries));
     *
     * // Typical: the accessor itself triggers no I/O — only the chained publisher does, on subscription.
     * Flux<String> names = userMapper.collectionExecutor().distinct("name", String.class);   // cold, nothing run yet
     *
     * // Edge: repeated calls always return the SAME instance (identity-stable accessor).
     * boolean same = userMapper.collectionExecutor() == userMapper.collectionExecutor();   // returns true
     *
     * // Edge/negative: it is never null, so no null-guard is needed before chaining.
     * Mono<Long> total = userMapper.collectionExecutor().count();   // safe: collectionExecutor() != null
     * }</pre>
     *
     * @return the reactive {@link MongoCollectionExecutor} instance backing this mapper; never null
     *         and identity-stable across calls
     * @see MongoCollectionExecutor
     */
    public MongoCollectionExecutor collectionExecutor() {
        return collectionExecutor;
    }

    /**
     * Checks if a document exists with the specified ObjectId string reactively.
     *
     * <p>This method provides a reactive way to verify document existence without retrieving the full document,
     * making it efficient for existence checks in a type-safe manner.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> userMapper = reactiveMongoDB.collectionMapper("users", User.class);
     *
     * // Typical: existing id -> emits true.
     * Mono<Boolean> found = userMapper.exists("507f1f77bcf86cd799439011");   // cold; emits true on subscribe if present
     * found.subscribe(exists -> System.out.println(exists ? "User found" : "User not found"));
     *
     * // Edge: a well-formed but unused id -> emits false (always exactly one value, never empty).
     * Mono<Boolean> missing = userMapper.exists("000000000000000000000000");   // emits false
     *
     * // Edge: the Mono is cold — building it runs no query until subscribed.
     * Mono<Boolean> notRunYet = userMapper.exists("507f1f77bcf86cd799439011");   // nothing executed yet
     *
     * // Negative: a non-hex / wrong-length id is parsed eagerly, so it throws
     * // IllegalArgumentException synchronously at the call site (before any Mono is built).
     * userMapper.exists("not-a-valid-hex");   // throws IllegalArgumentException
     * }</pre>
     *
     * @param objectId the string representation of the ObjectId to check for existence
     * @return a Mono that emits {@code true} if a document with the specified ObjectId exists, {@code false} otherwise
     * @throws IllegalArgumentException if objectId is null or empty, or if it is not a valid ObjectId hex string
     * @see ObjectId
     */
    public Mono<Boolean> exists(final String objectId) {
        return collectionExecutor.exists(objectId);
    }

    /**
     * Checks if a document exists with the specified ObjectId reactively.
     *
     * <p>This method provides a reactive way to verify document existence using a typed ObjectId,
     * making it efficient for existence checks in a type-safe context.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: check a known ObjectId.
     * ObjectId userId = new ObjectId("507f1f77bcf86cd799439011");
     * Mono<Boolean> exists = userMapper.exists(userId);   // cold; emits true/false on subscribe
     * exists.subscribe(e -> System.out.println("Document exists: " + e));
     *
     * // Edge: a freshly generated, never-stored ObjectId -> emits false.
     * Mono<Boolean> missing = userMapper.exists(new ObjectId());   // emits false
     *
     * // Edge: always emits exactly one boolean (never completes empty), so defaultIfEmpty is unnecessary.
     * boolean present = userMapper.exists(userId).block();   // present == true or false, never null from empty
     *
     * // Negative: null id is rejected with IllegalArgumentException, thrown synchronously at the
     * // call site (before any Mono is returned).
     * userMapper.exists((ObjectId) null);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param objectId the ObjectId to check for existence
     * @return a Mono that emits {@code true} if a document with the specified ObjectId exists, {@code false} otherwise
     * @throws IllegalArgumentException if objectId is null
     * @see ObjectId
     */
    public Mono<Boolean> exists(final ObjectId objectId) {
        return collectionExecutor.exists(objectId);
    }

    /**
     * Checks if any documents exist matching the specified filter reactively.
     *
     * <p>This method provides a reactive way to verify if documents matching the given filter exist
     * without retrieving the actual documents, optimized for the mapped entity type.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: at least one matching document -> emits true.
     * Bson activeUserFilter = Filters.eq("status", "active");
     * Mono<Boolean> any = userMapper.exists(activeUserFilter);   // cold; emits true on subscribe if any match
     * any.subscribe(e -> System.out.println("Active users exist: " + e));
     *
     * // Edge: a filter matching nothing -> emits false (not empty).
     * Mono<Boolean> none = userMapper.exists(Filters.eq("status", "no-such-status"));   // emits false
     *
     * // Edge: empty filter matches every document -> emits true unless the collection is empty.
     * Mono<Boolean> anyAtAll = userMapper.exists(Filters.empty());   // emits true if collection is non-empty
     *
     * // Negative: null filter is rejected with IllegalArgumentException, thrown synchronously at the
     * // call site (before any Mono is returned).
     * userMapper.exists((Bson) null);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param filter the query filter to match documents against; must not be null
     * @return a Mono that emits {@code true} if any documents match the filter, {@code false} otherwise
     * @throws IllegalArgumentException if filter is null (thrown synchronously at the call site)
     * @see Bson
     * @see com.mongodb.client.model.Filters
     */
    public Mono<Boolean> exists(final Bson filter) {
        return collectionExecutor.exists(filter);
    }

    /**
     * Counts all documents in the mapped collection reactively.
     *
     * <p>This method provides a reactive way to count all documents in the collection
     * mapped to the specified entity type. The operation is performed asynchronously.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: count every document; emits a single Long on subscribe.
     * Mono<Long> total = userMapper.count();   // cold; e.g. emits 100L
     * total.subscribe(count -> System.out.println("Total users: " + count));
     *
     * // Typical: combine with the reactive pipeline.
     * userMapper.count()
     *     .map(c -> "There are " + c + " users")
     *     .subscribe(System.out::println);
     *
     * // Edge: an empty collection still emits exactly one value -> 0L (never completes empty).
     * long n = userMapper.count().block();   // n == 0L for an empty collection
     *
     * // Edge: the Mono is cold — building it issues no count command until subscribed.
     * Mono<Long> notRunYet = userMapper.count();   // nothing executed yet
     * }</pre>
     *
     * @return a Mono that emits the total count of documents in the collection
     */
    public Mono<Long> count() {
        return collectionExecutor.count();
    }

    /**
     * Counts documents matching the specified filter reactively.
     *
     * <p>This method provides a reactive way to count documents that match the given filter criteria
     * in the context of the mapped entity type. The operation is performed asynchronously.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: count documents matching a filter.
     * Bson activeFilter = Filters.eq("status", "active");
     * Mono<Long> active = userMapper.count(activeFilter);   // cold; e.g. emits 25L
     * active.subscribe(count -> System.out.println("Active users: " + count));
     *
     * // Edge: a filter matching nothing -> emits 0L (one value, never empty).
     * long zero = userMapper.count(Filters.eq("status", "no-such-status")).block();   // zero == 0L
     *
     * // Edge: an empty filter counts the whole collection (same result as count()).
     * Mono<Long> all = userMapper.count(Filters.empty());   // emits total document count
     *
     * // Negative: null filter is rejected with IllegalArgumentException, thrown synchronously at the
     * // call site (before any Mono is returned).
     * userMapper.count((Bson) null);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param filter the query filter to match documents; must not be null
     * @return a Mono that emits the count of documents matching the filter
     * @throws IllegalArgumentException if filter is null (thrown synchronously at the call site)
     * @see Bson
     * @see com.mongodb.client.model.Filters
     */
    public Mono<Long> count(final Bson filter) {
        return collectionExecutor.count(filter);
    }

    /**
     * Counts documents matching the specified filter with custom options reactively.
     *
     * <p>This method provides a reactive way to count documents with additional counting options
     * such as limit, skip, maxTime, and collation for the mapped entity type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: cap the count via the limit option (result never exceeds the limit).
     * CountOptions options = new CountOptions().limit(1000).maxTime(30, TimeUnit.SECONDS);
     * Mono<Long> capped = userMapper.count(Filters.gte("age", 18), options);   // emits min(matches, 1000)
     * capped.subscribe(count -> System.out.println("Adult users (max 1000): " + count));
     *
     * // Edge: skip option offsets the count, so count(filter, skip(n)) == max(0, matches - n).
     * Mono<Long> afterSkip = userMapper.count(Filters.empty(), new CountOptions().skip(10));   // emits max(0, total - 10)
     *
     * // Edge: null options behaves like the no-options overload (default counting).
     * Mono<Long> defaults = userMapper.count(Filters.eq("status", "active"), (CountOptions) null);   // same as count(filter)
     *
     * // Negative: null filter is rejected with IllegalArgumentException, thrown synchronously at the
     * // call site (before any Mono is returned).
     * userMapper.count((Bson) null, options);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param filter the query filter to match documents; must not be null
     * @param options the count options to apply (can be null)
     * @return a Mono that emits the count of documents matching the filter with applied options
     * @throws IllegalArgumentException if filter is null (thrown synchronously at the call site)
     * @see Bson
     * @see CountOptions
     * @see com.mongodb.client.model.Filters
     */
    public Mono<Long> count(final Bson filter, final CountOptions options) {
        return collectionExecutor.count(filter, options);
    }

    /**
     * Retrieves a single entity by its ObjectId string reactively.
     *
     * <p>This method provides a reactive way to retrieve a document using its string ObjectId representation
     * and automatically convert it to the mapped entity type using the configured codec registry.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: existing id -> emits the decoded entity, then completes.
     * Mono<User> userMono = userMapper.get("507f1f77bcf86cd799439011");   // cold; emits one User on subscribe
     * userMono.subscribe(
     *     user -> System.out.println("Found user: " + user.getName()),
     *     error -> System.err.println("Error: " + error),
     *     () -> System.out.println("Completed (empty == not found)")
     * );
     *
     * // Edge: no document with that id -> Mono completes EMPTY (onComplete with no onNext).
     * userMapper.get("000000000000000000000000")
     *     .defaultIfEmpty(User.GUEST)   // supply a fallback when not found
     *     .subscribe(u -> System.out.println("User or guest: " + u));
     *
     * // Edge: cold publisher — building it runs no query until subscribed.
     * Mono<User> notRunYet = userMapper.get("507f1f77bcf86cd799439011");   // nothing executed yet
     *
     * // Negative: a malformed hex id is parsed eagerly, so it throws
     * // IllegalArgumentException synchronously at the call site (before any Mono is built).
     * userMapper.get("xyz");   // throws IllegalArgumentException
     * }</pre>
     *
     * @param objectId the string representation of the ObjectId to search for
     * @return a Mono that emits the found entity, or empty if no document matches the ObjectId
     * @throws IllegalArgumentException if objectId is null or empty, or if it is not a valid ObjectId hex string
     * @see ObjectId
     */
    public Mono<T> get(final String objectId) {
        return collectionExecutor.get(objectId, rowType);
    }

    /**
     * Retrieves a single entity by its ObjectId reactively.
     *
     * <p>This method provides a reactive way to retrieve a document using a typed ObjectId
     * and automatically convert it to the mapped entity type using the configured codec registry.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: existing id -> emits one decoded entity, then completes.
     * ObjectId userId = new ObjectId("507f1f77bcf86cd799439011");
     * Mono<User> userMono = userMapper.get(userId);   // cold; emits one User on subscribe
     * userMono.subscribe(user -> processUser(user), error -> handleError(error));
     *
     * // Edge: an unused ObjectId -> Mono completes EMPTY (no onNext).
     * userMapper.get(new ObjectId())
     *     .switchIfEmpty(Mono.error(new NoSuchElementException("user missing")))
     *     .subscribe(u -> {}, err -> System.err.println(err.getMessage()));   // onError on empty
     *
     * // Edge: cold publisher — nothing runs until subscription.
     * Mono<User> notRunYet = userMapper.get(userId);   // no query issued yet
     *
     * // Negative: null id is rejected with IllegalArgumentException, thrown synchronously at the
     * // call site (before any Mono is returned).
     * userMapper.get((ObjectId) null);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param objectId the ObjectId to search for
     * @return a Mono that emits the found entity, or empty if no document matches the ObjectId
     * @throws IllegalArgumentException if objectId is null
     * @see ObjectId
     */
    public Mono<T> get(final ObjectId objectId) {
        return collectionExecutor.get(objectId, rowType);
    }

    /**
     * Retrieves a single entity by its ObjectId string with field projection reactively.
     *
     * <p>This method provides a reactive way to retrieve specific fields from a document identified by
     * its ObjectId string, then convert the result to the mapped entity type. Field projection helps
     * reduce network traffic and memory usage by only retrieving necessary fields.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: fetch only the listed fields; unselected fields stay at their default values.
     * Collection<String> fields = Arrays.asList("name", "email", "status");
     * Mono<User> userMono = userMapper.get("507f1f77bcf86cd799439011", fields);   // cold; partially populated User
     * userMono.subscribe(user -> System.out.println("User basic info: " + user.getName()));
     *
     * // Edge: no matching id -> Mono completes EMPTY regardless of projection.
     * userMapper.get("000000000000000000000000", fields)
     *     .hasElement()
     *     .subscribe(present -> System.out.println("found? " + present));   // emits false
     *
     * // Edge: a null or empty projection list selects ALL fields (no projection is applied).
     * Mono<User> fullUser = userMapper.get("507f1f77bcf86cd799439011", Collections.emptyList());
     *
     * // Negative: a malformed id is parsed eagerly, so it throws
     * // IllegalArgumentException synchronously at the call site (before any Mono is built).
     * userMapper.get("nope", fields);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param objectId the string representation of the ObjectId to search for
     * @param selectPropNames the collection of field names to include in the projection (null or empty selects all fields)
     * @return a Mono that emits the projected entity, or empty if no document matches the ObjectId
     * @throws IllegalArgumentException if objectId is null/empty, or objectId is not a valid ObjectId hex string
     * @see ObjectId
     * @see com.mongodb.client.model.Projections
     */
    public Mono<T> get(final String objectId, final Collection<String> selectPropNames) {
        return collectionExecutor.get(objectId, selectPropNames, rowType);
    }

    /**
     * Retrieves a single entity by its ObjectId with field projection reactively.
     *
     * <p>This method provides a reactive way to retrieve specific fields from a document identified by
     * its ObjectId, then convert the result to the mapped entity type. Field projection reduces
     * network traffic and memory usage by only retrieving necessary fields.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: typed id + projection -> partially populated entity.
     * ObjectId userId = new ObjectId("507f1f77bcf86cd799439011");
     * Collection<String> fields = Arrays.asList("name", "email");
     * Mono<User> userMono = userMapper.get(userId, fields);   // cold; only name/email populated
     * userMono.subscribe(user -> processUserBasicInfo(user));
     *
     * // Edge: unused id -> Mono completes EMPTY.
     * userMapper.get(new ObjectId(), fields)
     *     .defaultIfEmpty(User.GUEST)
     *     .subscribe(u -> System.out.println("User or guest: " + u));
     *
     * // Edge: cold publisher — building it issues no query until subscribed.
     * Mono<User> notRunYet = userMapper.get(userId, fields);   // nothing executed yet
     *
     * // Negative: null id is rejected with IllegalArgumentException, thrown synchronously at the
     * // call site (before any Mono is returned).
     * userMapper.get((ObjectId) null, fields);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param objectId the ObjectId to search for
     * @param selectPropNames the collection of field names to include in the projection (null or empty selects all fields)
     * @return a Mono that emits the projected entity, or empty if no document matches the ObjectId
     * @throws IllegalArgumentException if objectId is null
     * @see ObjectId
     * @see com.mongodb.client.model.Projections
     */
    public Mono<T> get(final ObjectId objectId, final Collection<String> selectPropNames) {
        return collectionExecutor.get(objectId, selectPropNames, rowType);
    }

    /**
     * Finds the first entity matching the specified filter in a reactive manner.
     *
     * <p>Retrieves the first document that matches the given filter criteria and automatically
     * converts it to the mapped entity type {@code T}.</p>
     *
     * <p><b>Empty vs. present semantics:</b> on subscription, the returned {@code Mono} emits the
     * first matching document decoded as {@code T} and then completes, or completes <i>empty</i>
     * when no document matches the filter.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: at least one match -> emits the first decoded entity, then completes.
     * Bson filter = Filters.eq("status", "active");
     * Mono<User> userMono = userMapper.findFirst(filter);   // cold; emits one User on subscribe
     * userMono.subscribe(
     *     user -> System.out.println("First active user: " + user.getName()),
     *     error -> System.err.println("Error: " + error)
     * );
     *
     * // Edge: no match -> Mono completes EMPTY (onComplete with no onNext).
     * userMapper.findFirst(Filters.eq("status", "no-such-status"))
     *     .switchIfEmpty(Mono.error(new NoSuchElementException("none")))
     *     .subscribe(u -> {}, err -> System.err.println(err.getMessage()));   // onError on empty
     *
     * // Edge: empty filter returns the natural-order first document of the collection.
     * Mono<User> anyOne = userMapper.findFirst(Filters.empty());   // first document, or empty if none
     *
     * // Negative: a null filter is rejected with IllegalArgumentException, thrown synchronously
     * // at the call site (before any Mono is built).
     * userMapper.findFirst((Bson) null);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param filter the query filter to match documents
     * @return a {@code Mono} that emits the first matching entity decoded as {@code T}, or
     *         completes empty when no document matches the filter
     * @throws IllegalArgumentException if filter is null (thrown synchronously at the call site)
     * @see Bson
     * @see com.mongodb.client.model.Filters
     * @see MongoCollectionExecutor#findFirst(Bson, Class)
     */
    public Mono<T> findFirst(final Bson filter) {
        return collectionExecutor.findFirst(filter, rowType);
    }

    /**
     * Finds the first entity matching the specified filter with field projection reactively.
     *
     * <p>This method provides a reactive way to retrieve specific fields from the first document
     * that matches the given filter criteria, then convert the result to the mapped entity type.
     * Field projection reduces network traffic and memory usage.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: first match, only the listed fields populated.
     * Collection<String> fields = Arrays.asList("name", "email", "createdAt");
     * Bson filter = Filters.eq("department", "Engineering");
     * Mono<User> userMono = userMapper.findFirst(fields, filter);   // cold; partially populated User
     * userMono.subscribe(user -> System.out.println("Engineer: " + user.getName()));
     *
     * // Edge: no match -> Mono completes EMPTY (no onNext).
     * userMapper.findFirst(fields, Filters.eq("department", "no-such-dept"))
     *     .hasElement()
     *     .subscribe(present -> System.out.println("found? " + present));   // emits false
     *
     * // Edge: cold publisher — building it issues no query until subscribed.
     * Mono<User> notRunYet = userMapper.findFirst(fields, filter);   // nothing executed yet
     *
     * // Negative: a null filter is rejected with IllegalArgumentException, thrown synchronously
     * // at the call site (before any Mono is built).
     * userMapper.findFirst(fields, (Bson) null);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param selectPropNames the collection of field names to include in the projection (null or empty selects all fields)
     * @param filter the query filter to match documents
     * @return a Mono that emits the first matching projected entity, or empty if no documents match
     * @throws IllegalArgumentException if filter is null
     * @see Bson
     * @see com.mongodb.client.model.Filters
     * @see com.mongodb.client.model.Projections
     */
    public Mono<T> findFirst(final Collection<String> selectPropNames, final Bson filter) {
        return collectionExecutor.findFirst(selectPropNames, filter, rowType);
    }

    /**
     * Finds the first entity matching the specified filter with field projection and sorting reactively.
     *
     * <p>This method provides a reactive way to retrieve specific fields from the first document
     * that matches the given filter criteria with custom sorting, then convert the result to the
     * mapped entity type. Field projection and sorting help optimize query performance.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: sort picks WHICH document becomes "first" — descending score yields the top scorer.
     * Collection<String> fields = Arrays.asList("name", "score", "updatedAt");
     * Bson filter = Filters.gte("score", 90);
     * Bson sort = Sorts.descending("score");
     * Mono<User> top = userMapper.findFirst(fields, filter, sort);   // cold; highest-score match
     * top.subscribe(user -> System.out.println("Top scorer: " + user.getName()));
     *
     * // Typical: reversing the sort selects the opposite extreme (lowest qualifying score).
     * Mono<User> lowest = userMapper.findFirst(fields, filter, Sorts.ascending("score"));   // lowest match
     *
     * // Edge: no match -> Mono completes EMPTY regardless of sort.
     * userMapper.findFirst(fields, Filters.gte("score", 1000), sort)
     *     .hasElement().subscribe(present -> System.out.println("found? " + present));   // emits false
     *
     * // Edge: a null sort is tolerated and simply applies no ordering (natural/index order).
     * Mono<User> unsorted = userMapper.findFirst(fields, filter, (Bson) null);   // no sort applied; not an error
     * }</pre>
     *
     * @param selectPropNames the collection of field names to include in the projection (null or empty selects all fields)
     * @param filter the query filter to match documents
     * @param sort the sort specification for ordering results (can be null for natural order)
     * @return a Mono that emits the first matching projected entity with applied sorting, or empty if no documents match
     * @throws IllegalArgumentException if filter is null
     * @see Bson
     * @see com.mongodb.client.model.Filters
     * @see com.mongodb.client.model.Projections
     * @see com.mongodb.client.model.Sorts
     */
    public Mono<T> findFirst(final Collection<String> selectPropNames, final Bson filter, final Bson sort) {
        return collectionExecutor.findFirst(selectPropNames, filter, sort, rowType);
    }

    /**
     * Finds the first entity matching the specified filter with BSON projection and sorting reactively.
     *
     * <p>This method provides a reactive way to retrieve documents with advanced BSON projection
     * that matches the given filter criteria with custom sorting, then convert the result to the
     * mapped entity type. This overload allows for complex projection expressions.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson projection = Projections.fields(
     *     Projections.include("name", "email"),
     *     Projections.computed("fullName", Projections.concat("$firstName", " ", "$lastName"))
     * );
     * Bson filter = Filters.eq("active", true);
     * Bson sort = Sorts.ascending("name");
     *
     * Mono<User> userMono = userMapper.findFirst(projection, filter, sort);   // cold; first active user
     * userMono.subscribe(user -> System.out.println("Active user: " + user.getName()));
     *
     * // Edge: no match -> Mono completes EMPTY (no onNext).
     * userMapper.findFirst(projection, Filters.eq("active", false), sort)
     *     .hasElement().subscribe(present -> System.out.println("found? " + present));   // emits false (if none inactive)
     *
     * // Edge: cold publisher — nothing runs until subscription.
     * Mono<User> notRunYet = userMapper.findFirst(projection, filter, sort);   // no query issued yet
     *
     * // Edge: a null projection is tolerated and simply selects all fields.
     * Mono<User> allFields = userMapper.findFirst((Bson) null, filter, sort);   // no projection applied; not an error
     * }</pre>
     *
     * @param projection the BSON projection specification for field selection (can be null to select all fields)
     * @param filter the query filter to match documents
     * @param sort the sort specification for ordering results (can be null for natural order)
     * @return a Mono that emits the first matching projected entity with applied sorting, or empty if no documents match
     * @throws IllegalArgumentException if filter is null
     * @see Bson
     * @see com.mongodb.client.model.Filters
     * @see com.mongodb.client.model.Projections
     * @see com.mongodb.client.model.Sorts
     */
    public Mono<T> findFirst(final Bson projection, final Bson filter, final Bson sort) {
        return collectionExecutor.findFirst(projection, filter, sort, rowType);
    }

    /**
     * Lists all entities matching the specified filter as a reactive {@code Flux<T>}.
     *
     * <p>On subscription, the matching documents are fetched (page by page per the driver's cursor
     * batch size) and decoded one at a time to the mapper's entity type. The returned {@code Flux}
     * is cold and respects downstream demand: the driver fetches further documents only as the
     * downstream subscriber requests them.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: stream each matching entity; take(100) bounds the demand (backpressure).
     * Bson filter = Filters.eq("status", "active");
     * Flux<User> userFlux = userMapper.list(filter);   // cold; emits one User per matching doc
     * userFlux
     *     .take(100)  // Limit to 100 users for backpressure control
     *     .subscribe(
     *         user -> System.out.println("Active user: " + user.getName()),
     *         error -> System.err.println("Error: " + error),
     *         () -> System.out.println("All active users processed")
     *     );
     *
     * // Typical: aggregate the stream — count without materialising every entity in user code.
     * Mono<Long> activeCount = userMapper.list(filter).count();   // emits number of matches
     *
     * // Edge: no match -> Flux completes with ZERO emissions (immediate onComplete).
     * userMapper.list(Filters.eq("status", "no-such-status"))
     *     .switchIfEmpty(Flux.just(User.GUEST))
     *     .subscribe(u -> System.out.println("User or guest: " + u));
     *
     * // Negative: a null filter is rejected with IllegalArgumentException, thrown synchronously
     * // at the call site (before any Flux is built).
     * userMapper.list((Bson) null);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param filter the query filter to match documents
     * @return a cold {@code Flux} that, on subscription, emits each matching entity decoded as
     *         {@code T} (one per emission, honouring downstream demand), then completes
     * @throws IllegalArgumentException if filter is null (thrown synchronously at the call site)
     * @see Bson
     * @see com.mongodb.client.model.Filters
     */
    public Flux<T> list(final Bson filter) {
        return collectionExecutor.list(filter, rowType);
    }

    /**
     * Lists entities matching the specified filter with offset-based pagination.
     *
     * <p>Applies {@code skip(offset)} and {@code limit(count)} on the server-side query and decodes
     * each returned document to the mapper's entity type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: a single page — skip 20, take up to 10 (emits at most 10 entities).
     * Bson filter = Filters.gt("age", 18);
     * Flux<User> page = userMapper.list(filter, 20, 10);   // cold; skip 20, take 10
     * page.subscribe(user -> processUser(user));
     *
     * // Typical: offset 0 takes the first page.
     * Flux<User> firstPage = userMapper.list(filter, 0, 10);   // first 10 matches
     *
     * // Edge: offset beyond the number of matches -> Flux completes with ZERO emissions.
     * userMapper.list(filter, 1_000_000, 10)
     *     .count().subscribe(n -> System.out.println("rows: " + n));   // emits 0
     *
     * // Negative: a negative offset is rejected with IllegalArgumentException.
     * userMapper.list(filter, -1, 10);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param filter the query filter to match documents against
     * @param offset the number of documents to skip (must be &gt;= 0)
     * @param count the maximum number of documents to return (must be &gt;= 0; {@code 0} yields an empty result)
     * @return a cold {@code Flux} that, on subscription, emits up to {@code count} matching entities
     *         decoded as {@code T} (honouring downstream demand), then completes
     * @throws IllegalArgumentException if filter is null, offset is negative, or count is negative
     */
    public Flux<T> list(final Bson filter, final int offset, final int count) {
        return collectionExecutor.list(filter, offset, count, rowType);
    }

    /**
     * Lists entities with specified fields matching the filter.
     *
     * <p>Retrieves documents matching the filter criteria, but only includes the specified properties
     * in the result. This is useful for optimizing network traffic and memory usage when only
     * certain fields are needed.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: stream entities with only name/email populated.
     * List<String> fields = Arrays.asList("name", "email");
     * Bson filter = Filters.eq("department", "IT");
     * Flux<User> itUsers = userMapper.list(fields, filter);   // cold; partially populated Users
     * itUsers.subscribe(u -> System.out.println(u.getName() + " <" + u.getEmail() + ">"));
     *
     * // Edge: no match -> Flux completes with ZERO emissions.
     * userMapper.list(fields, Filters.eq("department", "no-such-dept"))
     *     .count().subscribe(n -> System.out.println("rows: " + n));   // emits 0
     *
     * // Edge: cold publisher — nothing runs until subscription.
     * Flux<User> notRunYet = userMapper.list(fields, filter);   // no query issued yet
     *
     * // Negative: a null filter is rejected with IllegalArgumentException, thrown synchronously
     * // at the call site (before any Flux is built).
     * userMapper.list(fields, (Bson) null);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param selectPropNames the collection of property names to include in the results (null or empty selects all fields)
     * @param filter the query filter to match documents
     * @return a Flux that emits matching entities with only the specified fields populated
     * @throws IllegalArgumentException if filter is null
     */
    public Flux<T> list(final Collection<String> selectPropNames, final Bson filter) {
        return collectionExecutor.list(selectPropNames, filter, rowType);
    }

    /**
     * Lists entities with specified fields and pagination.
     *
     * <p>Combines field projection with pagination to retrieve a subset of documents with only
     * specific fields populated. This provides maximum control over the query results for
     * efficient data retrieval.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: projected first page — skip 0, take up to 50.
     * List<String> fields = Arrays.asList("id", "name", "status");
     * Bson filter = Filters.eq("active", true);
     * Flux<User> users = userMapper.list(fields, filter, 0, 50);   // cold; up to 50 partial Users
     * users.subscribe(u -> processUser(u));
     *
     * // Edge: offset past the end -> Flux completes with ZERO emissions.
     * userMapper.list(fields, filter, 1_000_000, 50)
     *     .count().subscribe(n -> System.out.println("rows: " + n));   // emits 0
     *
     * // Edge: cold publisher — building it issues no query until subscribed.
     * Flux<User> notRunYet = userMapper.list(fields, filter, 0, 50);   // nothing executed yet
     *
     * // Negative: a negative count is rejected with IllegalArgumentException.
     * userMapper.list(fields, filter, 0, -1);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param selectPropNames the collection of property names to include in the results
     * @param filter the query filter to match documents against
     * @param offset the number of documents to skip (must be >= 0)
     * @param count the maximum number of documents to return (must be >= 0; {@code 0} yields an empty result)
     * @return a Flux that emits the paginated matching entities with specified fields
     * @throws IllegalArgumentException if filter is null, offset is negative, or count is negative
     */
    public Flux<T> list(final Collection<String> selectPropNames, final Bson filter, final int offset, final int count) {
        return collectionExecutor.list(selectPropNames, filter, offset, count, rowType);
    }

    /**
     * Lists entities with specified fields and sorting.
     *
     * <p>Retrieves documents matching the filter criteria with specified field projection and
     * sorting order. The results are streamed in the order defined by the sort specification.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: newest-first ordering via a descending sort on createdAt.
     * List<String> fields = Arrays.asList("name", "createdAt");
     * Bson filter = Filters.eq("status", "pending");
     * Bson sort = Sorts.descending("createdAt");
     * Flux<User> recentPending = userMapper.list(fields, filter, sort);   // cold; newest pending first
     * recentPending.subscribe(u -> System.out.println(u.getName()));
     *
     * // Typical: reverse the sort to stream oldest-first instead.
     * Flux<User> oldestFirst = userMapper.list(fields, filter, Sorts.ascending("createdAt"));   // oldest pending first
     *
     * // Edge: no match -> Flux completes with ZERO emissions.
     * userMapper.list(fields, Filters.eq("status", "no-such-status"), sort)
     *     .count().subscribe(n -> System.out.println("rows: " + n));   // emits 0
     *
     * // Edge: a null sort is tolerated and simply applies no ordering (natural/index order).
     * Flux<User> unsorted = userMapper.list(fields, filter, (Bson) null);   // no sort applied; not an error
     * }</pre>
     *
     * @param selectPropNames the collection of property names to include in the results
     * @param filter the query filter to match documents against
     * @param sort the sort specification for ordering results
     * @return a Flux that emits sorted matching entities with specified fields
     * @throws IllegalArgumentException if filter is null
     * @see com.mongodb.client.model.Sorts
     */
    public Flux<T> list(final Collection<String> selectPropNames, final Bson filter, final Bson sort) {
        return collectionExecutor.list(selectPropNames, filter, sort, rowType);
    }

    /**
     * Lists entities with full control over projection, filtering, sorting, and pagination.
     *
     * <p>Provides complete control over the query execution including field selection, filtering,
     * sorting, and pagination. This is the most flexible list method for complex query requirements.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: top-10 leaderboard — descending score, first page.
     * List<String> fields = Arrays.asList("id", "name", "score");
     * Bson filter = Filters.gte("score", 80);
     * Bson sort = Sorts.descending("score");
     * Flux<User> topScorers = userMapper.list(fields, filter, sort, 0, 10);   // cold; up to 10 highest scorers
     * topScorers.subscribe(u -> System.out.println(u.getName() + ": " + u.getScore()));
     *
     * // Typical: next page of 10.
     * Flux<User> nextTen = userMapper.list(fields, filter, sort, 10, 10);   // ranks 11..20
     *
     * // Edge: offset past the end -> Flux completes with ZERO emissions.
     * userMapper.list(fields, filter, sort, 1_000_000, 10)
     *     .count().subscribe(n -> System.out.println("rows: " + n));   // emits 0
     *
     * // Negative: a negative offset is rejected with IllegalArgumentException.
     * userMapper.list(fields, filter, sort, -1, 10);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param selectPropNames the collection of property names to include in the results
     * @param filter the query filter to match documents against
     * @param sort the sort specification for ordering results
     * @param offset the number of documents to skip (must be >= 0)
     * @param count the maximum number of documents to return (must be >= 0; {@code 0} yields an empty result)
     * @return a Flux that emits the fully controlled query results
     * @throws IllegalArgumentException if filter is null, offset is negative, or count is negative
     */
    public Flux<T> list(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final int offset, final int count) {
        return collectionExecutor.list(selectPropNames, filter, sort, offset, count, rowType);
    }

    /**
     * Lists entities using a BSON projection document for field selection.
     *
     * <p>Uses a MongoDB projection document to control which fields are included or excluded
     * in the results. This provides fine-grained control over field inclusion/exclusion patterns.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: include name/email, drop _id; null sort means natural order (sort is optional here).
     * Bson projection = Projections.fields(
     *     Projections.include("name", "email"),
     *     Projections.excludeId()
     * );
     * Flux<User> users = userMapper.list(projection, Filters.empty(), null);   // cold; all docs, no _id, unsorted
     * users.subscribe(u -> System.out.println(u.getName()));
     *
     * // Typical: supply a sort to order the stream.
     * Flux<User> sorted = userMapper.list(projection, Filters.empty(), Sorts.ascending("name"));   // name-ordered
     *
     * // Edge: filter matching nothing -> Flux completes with ZERO emissions.
     * userMapper.list(projection, Filters.eq("status", "no-such-status"), null)
     *     .count().subscribe(n -> System.out.println("rows: " + n));   // emits 0
     *
     * // Edge: cold publisher — nothing runs until subscription.
     * Flux<User> notRunYet = userMapper.list(projection, Filters.empty(), null);   // no query issued yet
     * }</pre>
     *
     * @param projection the BSON projection document for field selection
     * @param filter the query filter to match documents against
     * @param sort the sort specification for ordering results (can be null)
     * @return a Flux that emits matching entities with projection applied
     * @throws IllegalArgumentException if filter is null
     * @see com.mongodb.client.model.Projections
     */
    public Flux<T> list(final Bson projection, final Bson filter, final Bson sort) {
        return collectionExecutor.list(projection, filter, sort, rowType);
    }

    /**
     * Lists entities with BSON projection and full pagination control.
     *
     * <p>Combines BSON projection with filtering, sorting, and pagination for maximum query
     * flexibility. This method is ideal for complex queries requiring precise control over
     * all aspects of the result set.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: projected, sorted first page of up to 100.
     * Bson projection = Projections.include("id", "name", "tags");
     * Bson filter = Filters.in("tags", "premium", "verified");
     * Bson sort = Sorts.ascending("name");
     * Flux<User> premiumUsers = userMapper.list(projection, filter, sort, 0, 100);   // cold; up to 100 projected Users
     * premiumUsers.subscribe(u -> System.out.println(u.getName()));
     *
     * // Edge: offset past the end -> Flux completes with ZERO emissions.
     * userMapper.list(projection, filter, sort, 1_000_000, 100)
     *     .count().subscribe(n -> System.out.println("rows: " + n));   // emits 0
     *
     * // Edge: null sort is tolerated -> natural order, still paginated.
     * Flux<User> unsortedPage = userMapper.list(projection, filter, (Bson) null, 0, 100);   // no sort applied
     *
     * // Negative: a negative offset is rejected with IllegalArgumentException.
     * userMapper.list(projection, filter, sort, -1, 100);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param projection the BSON projection document for field selection
     * @param filter the query filter to match documents against
     * @param sort the sort specification for ordering results (can be null)
     * @param offset the number of documents to skip (must be >= 0)
     * @param count the maximum number of documents to return (must be >= 0; {@code 0} yields an empty result)
     * @return a Flux that emits the fully controlled query results with projection
     * @throws IllegalArgumentException if filter is null
     */
    public Flux<T> list(final Bson projection, final Bson filter, final Bson sort, final int offset, final int count) {
        return collectionExecutor.list(projection, filter, sort, offset, count, rowType);
    }

    /**
     * Returns a {@code Mono} that, on subscription, queries for the given property of the first
     * matching document in this mapper's collection as a {@code Boolean} value.
     *
     * <p>Only the named property of the first matched document is read; any remaining documents or
     * fields are ignored. Delegates to the underlying
     * {@link MongoCollectionExecutor#queryForBoolean(String, Bson)}.</p>
     *
     * <p><b>Empty vs. present semantics:</b> the returned {@code Mono} completes <i>empty</i> when no
     * document matches the filter, or when a matching document's named field is absent or BSON null.
     * Otherwise it emits the converted {@code Boolean} value and then completes. Because this
     * overload is driven by the wrapper type {@code Boolean.class}, missing/null fields surface as
     * Mono completion (not as the primitive default {@code false}).</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: field present on the first match -> emits its Boolean value.
     * Mono<Boolean> isActive = userMapper.queryForBoolean(
     *     "isActive", Filters.eq("username", "admin"));
     * isActive.subscribe(a -> System.out.println("Admin active: " + a));   // emits true/false
     *
     * // Edge: no document matches OR the field is missing/null -> Mono completes EMPTY.
     * userMapper.queryForBoolean("isActive", Filters.eq("username", "ghost"))
     *     .defaultIfEmpty(false)   // wrapper type: missing surfaces as empty, NOT primitive false
     *     .subscribe(a -> System.out.println("active (default false): " + a));
     *
     * // Edge: cold publisher — building it issues no query until subscribed.
     * Mono<Boolean> notRunYet = userMapper.queryForBoolean("isActive", Filters.eq("username", "admin"));
     *
     * // Negative: a null/empty propName throws IllegalArgumentException synchronously (at the call
     * // site, before any Mono is returned), because propName is validated eagerly.
     * userMapper.queryForBoolean("", Filters.empty());   // throws IllegalArgumentException
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @return a {@code Mono} that emits the {@code Boolean} field value on subscription, or completes
     *         empty when no document matches or the field is missing/null
     * @throws IllegalArgumentException if {@code propName} is null or empty, or if {@code filter} is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     * @see #queryForSingleValue(String, Bson, Class)
     * @see MongoCollectionExecutor#queryForBoolean(String, Bson)
     */
    @Beta
    public Mono<Boolean> queryForBoolean(final String propName, final Bson filter) {
        return collectionExecutor.queryForBoolean(propName, filter);
    }

    /**
     * Returns a {@code Mono} that, on subscription, queries for the given property of the first
     * matching document in this mapper's collection as a {@code Character} value.
     *
     * <p>Only the named property of the first matched document is read; any remaining documents or
     * fields are ignored. Delegates to the underlying
     * {@link MongoCollectionExecutor#queryForChar(String, Bson)}.</p>
     *
     * <p><b>Empty vs. present semantics:</b> the returned {@code Mono} completes <i>empty</i> when no
     * document matches the filter, or when a matching document's named field is absent or BSON null.
     * Otherwise it emits the converted {@code Character} value and then completes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: field present -> emits its Character value.
     * Mono<Character> grade = userMapper.queryForChar("grade", Filters.eq("id", userId));
     * grade.subscribe(g -> System.out.println("Grade: " + g));   // e.g. emits 'A'
     *
     * // Edge: no match OR missing/null field -> Mono completes EMPTY.
     * userMapper.queryForChar("grade", Filters.eq("id", "missing"))
     *     .hasElement().subscribe(present -> System.out.println("present? " + present));   // emits false
     *
     * // Edge: cold publisher — building it issues no query until subscribed.
     * Mono<Character> notRunYet = userMapper.queryForChar("grade", Filters.empty());
     *
     * // Negative: a null/empty propName throws IllegalArgumentException synchronously (at the call
     * // site, before any Mono is returned), because propName is validated eagerly.
     * userMapper.queryForChar("", Filters.empty());   // throws IllegalArgumentException
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @return a {@code Mono} that emits the {@code Character} field value on subscription, or
     *         completes empty when no document matches or the field is missing/null
     * @throws IllegalArgumentException if {@code propName} is null or empty, or if {@code filter} is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     * @see #queryForSingleValue(String, Bson, Class)
     * @see MongoCollectionExecutor#queryForChar(String, Bson)
     */
    @Beta
    public Mono<Character> queryForChar(final String propName, final Bson filter) {
        return collectionExecutor.queryForChar(propName, filter);
    }

    /**
     * Returns a {@code Mono} that, on subscription, queries for the given property of the first
     * matching document in this mapper's collection as a {@code Byte} value.
     *
     * <p>Only the named property of the first matched document is read; any remaining documents or
     * fields are ignored. Delegates to the underlying
     * {@link MongoCollectionExecutor#queryForByte(String, Bson)}.</p>
     *
     * <p><b>Empty vs. present semantics:</b> the returned {@code Mono} completes <i>empty</i> when no
     * document matches the filter, or when a matching document's named field is absent or BSON null.
     * Otherwise it emits the converted {@code Byte} value and then completes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: field present -> emits its Byte value.
     * Mono<Byte> status = userMapper.queryForByte(
     *     "statusCode", Filters.eq("productId", productId));
     * status.subscribe(code -> processStatusCode(code));   // e.g. emits (byte) 5
     *
     * // Edge: no match OR missing/null field -> Mono completes EMPTY (not the primitive default 0).
     * userMapper.queryForByte("statusCode", Filters.eq("productId", "missing"))
     *     .defaultIfEmpty((byte) -1)
     *     .subscribe(code -> System.out.println("status (default -1): " + code));   // emits -1 when absent
     *
     * // Edge: cold publisher — building it issues no query until subscribed.
     * Mono<Byte> notRunYet = userMapper.queryForByte("statusCode", Filters.empty());
     *
     * // Negative: a null/empty propName throws IllegalArgumentException synchronously (at the call
     * // site, before any Mono is returned), because propName is validated eagerly.
     * userMapper.queryForByte("", Filters.empty());   // throws IllegalArgumentException
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @return a {@code Mono} that emits the {@code Byte} field value on subscription, or completes
     *         empty when no document matches or the field is missing/null
     * @throws IllegalArgumentException if {@code propName} is null or empty, or if {@code filter} is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     * @see #queryForSingleValue(String, Bson, Class)
     * @see MongoCollectionExecutor#queryForByte(String, Bson)
     */
    @Beta
    public Mono<Byte> queryForByte(final String propName, final Bson filter) {
        return collectionExecutor.queryForByte(propName, filter);
    }

    /**
     * Returns a {@code Mono} that, on subscription, queries for the given property of the first
     * matching document in this mapper's collection as a {@code Short} value.
     *
     * <p>Only the named property of the first matched document is read; any remaining documents or
     * fields are ignored. Delegates to the underlying
     * {@link MongoCollectionExecutor#queryForShort(String, Bson)}.</p>
     *
     * <p><b>Empty vs. present semantics:</b> the returned {@code Mono} completes <i>empty</i> when no
     * document matches the filter, or when a matching document's named field is absent or BSON null.
     * Otherwise it emits the converted {@code Short} value and then completes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: field present -> emits its Short value.
     * Mono<Short> quantity = userMapper.queryForShort(
     *     "quantity", Filters.eq("orderId", orderId));
     * quantity.subscribe(q -> System.out.println("Quantity: " + q));   // e.g. emits (short) 100
     *
     * // Edge: no match OR missing/null field -> Mono completes EMPTY (not the primitive default 0).
     * userMapper.queryForShort("quantity", Filters.eq("orderId", "missing"))
     *     .defaultIfEmpty((short) -1)
     *     .subscribe(q -> System.out.println("qty (default -1): " + q));   // emits -1 when absent
     *
     * // Edge: cold publisher — building it issues no query until subscribed.
     * Mono<Short> notRunYet = userMapper.queryForShort("quantity", Filters.empty());
     *
     * // Negative: a null/empty propName throws IllegalArgumentException synchronously (at the call
     * // site, before any Mono is returned), because propName is validated eagerly.
     * userMapper.queryForShort("", Filters.empty());   // throws IllegalArgumentException
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @return a {@code Mono} that emits the {@code Short} field value on subscription, or completes
     *         empty when no document matches or the field is missing/null
     * @throws IllegalArgumentException if {@code propName} is null or empty, or if {@code filter} is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     * @see #queryForSingleValue(String, Bson, Class)
     * @see MongoCollectionExecutor#queryForShort(String, Bson)
     */
    @Beta
    public Mono<Short> queryForShort(final String propName, final Bson filter) {
        return collectionExecutor.queryForShort(propName, filter);
    }

    /**
     * Returns a {@code Mono} that, on subscription, queries for the given property of the first
     * matching document in this mapper's collection as an {@code Integer} value.
     *
     * <p>Only the named property of the first matched document is read; any remaining documents or
     * fields are ignored. Delegates to the underlying
     * {@link MongoCollectionExecutor#queryForInt(String, Bson)}.</p>
     *
     * <p><b>Empty vs. present semantics:</b> the returned {@code Mono} completes <i>empty</i> when no
     * document matches the filter, or when a matching document's named field is absent or BSON null.
     * Otherwise it emits the converted {@code Integer} value and then completes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: field present -> emits its Integer value.
     * Mono<Integer> age = userMapper.queryForInt(
     *     "age", Filters.eq("email", "user@example.com"));
     * age.subscribe(a -> System.out.println("User age: " + a));   // e.g. emits 42
     *
     * // Edge: no match OR missing/null field -> Mono completes EMPTY (not the primitive default 0).
     * userMapper.queryForInt("age", Filters.eq("email", "ghost@example.com"))
     *     .defaultIfEmpty(-1)
     *     .subscribe(a -> System.out.println("age (default -1): " + a));   // emits -1 when absent
     *
     * // Edge: cold publisher — building it issues no query until subscribed.
     * Mono<Integer> notRunYet = userMapper.queryForInt("age", Filters.empty());
     *
     * // Negative: a null/empty propName throws IllegalArgumentException synchronously (at the call
     * // site, before any Mono is returned), because propName is validated eagerly.
     * userMapper.queryForInt("", Filters.empty());   // throws IllegalArgumentException
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @return a {@code Mono} that emits the {@code Integer} field value on subscription, or completes
     *         empty when no document matches or the field is missing/null
     * @throws IllegalArgumentException if {@code propName} is null or empty, or if {@code filter} is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     * @see #queryForSingleValue(String, Bson, Class)
     * @see MongoCollectionExecutor#queryForInt(String, Bson)
     */
    @Beta
    public Mono<Integer> queryForInt(final String propName, final Bson filter) {
        return collectionExecutor.queryForInt(propName, filter);
    }

    /**
     * Returns a {@code Mono} that, on subscription, queries for the given property of the first
     * matching document in this mapper's collection as a {@code Long} value.
     *
     * <p>Only the named property of the first matched document is read; any remaining documents or
     * fields are ignored. Delegates to the underlying
     * {@link MongoCollectionExecutor#queryForLong(String, Bson)}.</p>
     *
     * <p><b>Empty vs. present semantics:</b> the returned {@code Mono} completes <i>empty</i> when no
     * document matches the filter, or when a matching document's named field is absent or BSON null.
     * Otherwise it emits the converted {@code Long} value and then completes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: field present -> emits its Long value.
     * Mono<Long> timestamp = userMapper.queryForLong(
     *     "lastAccessTime", Filters.eq("sessionId", sessionId));
     * timestamp.subscribe(t -> updateLastAccess(t));   // e.g. emits 1234567890L
     *
     * // Edge: no match OR missing/null field -> Mono completes EMPTY (not the primitive default 0).
     * userMapper.queryForLong("lastAccessTime", Filters.eq("sessionId", "missing"))
     *     .defaultIfEmpty(0L)
     *     .subscribe(t -> System.out.println("ts (default 0): " + t));   // emits 0 when absent
     *
     * // Edge: cold publisher — building it issues no query until subscribed.
     * Mono<Long> notRunYet = userMapper.queryForLong("lastAccessTime", Filters.empty());
     *
     * // Negative: a null/empty propName throws IllegalArgumentException synchronously (at the call
     * // site, before any Mono is returned), because propName is validated eagerly.
     * userMapper.queryForLong("", Filters.empty());   // throws IllegalArgumentException
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @return a {@code Mono} that emits the {@code Long} field value on subscription, or completes
     *         empty when no document matches or the field is missing/null
     * @throws IllegalArgumentException if {@code propName} is null or empty, or if {@code filter} is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     * @see #queryForSingleValue(String, Bson, Class)
     * @see MongoCollectionExecutor#queryForLong(String, Bson)
     */
    @Beta
    public Mono<Long> queryForLong(final String propName, final Bson filter) {
        return collectionExecutor.queryForLong(propName, filter);
    }

    /**
     * Returns a {@code Mono} that, on subscription, queries for the given property of the first
     * matching document in this mapper's collection as a {@code Float} value.
     *
     * <p>Only the named property of the first matched document is read; any remaining documents or
     * fields are ignored. Delegates to the underlying
     * {@link MongoCollectionExecutor#queryForFloat(String, Bson)}.</p>
     *
     * <p><b>Empty vs. present semantics:</b> the returned {@code Mono} completes <i>empty</i> when no
     * document matches the filter, or when a matching document's named field is absent or BSON null.
     * Otherwise it emits the converted {@code Float} value and then completes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: field present -> emits its Float value.
     * Mono<Float> price = userMapper.queryForFloat(
     *     "price", Filters.eq("productId", productId));
     * price.subscribe(p -> System.out.println("Price: $" + p));   // e.g. emits 19.99f
     *
     * // Edge: no match OR missing/null field -> Mono completes EMPTY (not the primitive default 0).
     * userMapper.queryForFloat("price", Filters.eq("productId", "missing"))
     *     .defaultIfEmpty(0.0f)
     *     .subscribe(p -> System.out.println("price (default 0): " + p));   // emits 0.0 when absent
     *
     * // Edge: cold publisher — building it issues no query until subscribed.
     * Mono<Float> notRunYet = userMapper.queryForFloat("price", Filters.empty());
     *
     * // Negative: a null/empty propName throws IllegalArgumentException synchronously (at the call
     * // site, before any Mono is returned), because propName is validated eagerly.
     * userMapper.queryForFloat("", Filters.empty());   // throws IllegalArgumentException
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @return a {@code Mono} that emits the {@code Float} field value on subscription, or completes
     *         empty when no document matches or the field is missing/null
     * @throws IllegalArgumentException if {@code propName} is null or empty, or if {@code filter} is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     * @see #queryForDouble(String, Bson)
     * @see #queryForSingleValue(String, Bson, Class)
     * @see MongoCollectionExecutor#queryForFloat(String, Bson)
     */
    @Beta
    public Mono<Float> queryForFloat(final String propName, final Bson filter) {
        return collectionExecutor.queryForFloat(propName, filter);
    }

    /**
     * Returns a {@code Mono} that, on subscription, queries for the given property of the first
     * matching document in this mapper's collection as a {@code Double} value.
     *
     * <p>Only the named property of the first matched document is read; any remaining documents or
     * fields are ignored. Delegates to the underlying
     * {@link MongoCollectionExecutor#queryForDouble(String, Bson)}.</p>
     *
     * <p><b>Empty vs. present semantics:</b> the returned {@code Mono} completes <i>empty</i> when no
     * document matches the filter, or when a matching document's named field is absent or BSON null.
     * Otherwise it emits the converted {@code Double} value and then completes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: field present -> emits its Double value.
     * Mono<Double> balance = userMapper.queryForDouble(
     *     "balance", Filters.eq("accountId", accountId));
     * balance.subscribe(b -> System.out.println("Balance: " + b));   // e.g. emits 37.7749
     *
     * // Edge: no match OR missing/null field -> Mono completes EMPTY (not the primitive default 0).
     * userMapper.queryForDouble("balance", Filters.eq("accountId", "missing"))
     *     .defaultIfEmpty(0.0)
     *     .subscribe(b -> System.out.println("balance (default 0): " + b));   // emits 0.0 when absent
     *
     * // Edge: cold publisher — building it issues no query until subscribed.
     * Mono<Double> notRunYet = userMapper.queryForDouble("balance", Filters.empty());
     *
     * // Negative: a null/empty propName throws IllegalArgumentException synchronously (at the call
     * // site, before any Mono is returned), because propName is validated eagerly.
     * userMapper.queryForDouble("", Filters.empty());   // throws IllegalArgumentException
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @return a {@code Mono} that emits the {@code Double} field value on subscription, or completes
     *         empty when no document matches or the field is missing/null
     * @throws IllegalArgumentException if {@code propName} is null or empty, or if {@code filter} is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     * @see #queryForFloat(String, Bson)
     * @see #queryForSingleValue(String, Bson, Class)
     * @see MongoCollectionExecutor#queryForDouble(String, Bson)
     */
    @Beta
    public Mono<Double> queryForDouble(final String propName, final Bson filter) {
        return collectionExecutor.queryForDouble(propName, filter);
    }

    /**
     * Returns a {@code Mono} that, on subscription, queries for the given property of the first
     * matching document in this mapper's collection as a {@code String} value.
     *
     * <p>Only the named property of the first matched document is read; any remaining documents or
     * fields are ignored. Delegates to the underlying
     * {@link MongoCollectionExecutor#queryForString(String, Bson)}.</p>
     *
     * <p><b>Empty vs. present semantics:</b> the returned {@code Mono} completes <i>empty</i> when no
     * document matches the filter, or when a matching document's named field is absent or BSON null.
     * Otherwise it emits the converted {@code String} value and then completes. Subscribers cannot
     * distinguish "no document matched" from "document matched but value is null" purely from the
     * reactive signal — use {@code defaultIfEmpty(...)} or the blocking sync API when that
     * distinction is required.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: field present -> emits its String value.
     * Mono<String> username = userMapper.queryForString(
     *     "username", Filters.eq("userId", userId));
     * username.subscribe(n -> System.out.println("Username: " + n));   // emits e.g. "John Doe"
     *
     * // Edge: no match OR missing/null field -> Mono completes EMPTY; cannot distinguish the two from the signal.
     * userMapper.queryForString("username", Filters.eq("userId", "missing"))
     *     .defaultIfEmpty("<unknown>")
     *     .subscribe(n -> System.out.println("Username: " + n));   // emits "<unknown>" when absent
     *
     * // Edge: cold publisher — building it issues no query until subscribed.
     * Mono<String> notRunYet = userMapper.queryForString("username", Filters.empty());
     *
     * // Negative: a null/empty propName throws IllegalArgumentException synchronously (at the call
     * // site, before any Mono is returned), because propName is validated eagerly.
     * userMapper.queryForString("", Filters.empty());   // throws IllegalArgumentException
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @return a {@code Mono} that emits the {@code String} field value on subscription, or completes
     *         empty when no document matches or the field is missing/null
     * @throws IllegalArgumentException if {@code propName} is null or empty, or if {@code filter} is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     * @see #queryForSingleValue(String, Bson, Class)
     * @see MongoCollectionExecutor#queryForString(String, Bson)
     */
    @Beta
    public Mono<String> queryForString(final String propName, final Bson filter) {
        return collectionExecutor.queryForString(propName, filter);
    }

    /**
     * Returns a {@code Mono} that, on subscription, queries for the given property of the first
     * matching document in this mapper's collection as a {@link Date} value.
     *
     * <p>Only the named property of the first matched document is read; any remaining documents or
     * fields are ignored. Commonly used for timestamp fields such as {@code createdAt} or
     * {@code updatedAt}. Delegates to the underlying
     * {@link MongoCollectionExecutor#queryForDate(String, Bson)}.</p>
     *
     * <p><b>Empty vs. present semantics:</b> the returned {@code Mono} completes <i>empty</i> when no
     * document matches the filter, or when a matching document's named field is absent or BSON null.
     * Otherwise it emits the converted {@code Date} value and then completes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: timestamp field present -> emits its Date value.
     * Mono<Date> orderDate = userMapper.queryForDate(
     *     "createdAt", Filters.eq("orderId", orderId));
     * orderDate.subscribe(d -> System.out.println("Order date: " + d));   // emits a java.util.Date
     *
     * // Edge: no match OR missing/null field -> Mono completes EMPTY.
     * userMapper.queryForDate("createdAt", Filters.eq("orderId", "missing"))
     *     .hasElement().subscribe(present -> System.out.println("present? " + present));   // emits false
     *
     * // Edge: cold publisher — building it issues no query until subscribed.
     * Mono<Date> notRunYet = userMapper.queryForDate("createdAt", Filters.empty());
     *
     * // Negative: a null/empty propName throws IllegalArgumentException synchronously (at the call
     * // site, before any Mono is returned), because propName is validated eagerly.
     * userMapper.queryForDate("", Filters.empty());   // throws IllegalArgumentException
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @return a {@code Mono} that emits the {@code Date} field value on subscription, or completes
     *         empty when no document matches or the field is missing/null
     * @throws IllegalArgumentException if {@code propName} is null or empty, or if {@code filter} is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     * @see #queryForDate(String, Bson, Class)
     * @see #queryForSingleValue(String, Bson, Class)
     * @see MongoCollectionExecutor#queryForDate(String, Bson)
     */
    @Beta
    public Mono<Date> queryForDate(final String propName, final Bson filter) {
        return collectionExecutor.queryForDate(propName, filter);
    }

    /**
     * Returns a {@code Mono} that, on subscription, queries for the given property of the first
     * matching document in this mapper's collection as a specific {@link Date} subclass value
     * (e.g. {@link java.sql.Timestamp}).
     *
     * <p>Only the named property of the first matched document is read; any remaining documents or
     * fields are ignored. The value is converted to the requested {@code Date} subclass. Delegates
     * to {@link MongoCollectionExecutor#queryForDate(String, Bson, Class)}.</p>
     *
     * <p><b>Empty vs. present semantics:</b> the returned {@code Mono} completes <i>empty</i> when no
     * document matches the filter, or when a matching document's named field is absent or BSON null.
     * Otherwise it emits the converted typed value and then completes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: field present -> emits the value converted to the requested Date subclass.
     * Mono<Timestamp> processedAt = userMapper.queryForDate(
     *     "processedAt", Filters.eq("transactionId", txId), Timestamp.class);
     * processedAt.subscribe(ts -> logTransaction(ts));   // emits a java.sql.Timestamp
     *
     * // Edge: no match OR missing/null field -> Mono completes EMPTY.
     * userMapper.queryForDate("processedAt", Filters.eq("transactionId", "missing"), Timestamp.class)
     *     .hasElement().subscribe(present -> System.out.println("present? " + present));   // emits false
     *
     * // Edge: cold publisher — building it issues no query until subscribed.
     * Mono<Timestamp> notRunYet = userMapper.queryForDate("processedAt", Filters.empty(), Timestamp.class);
     *
     * // Negative: null valueType -> error signal (IllegalArgumentException) on subscription.
     * userMapper.queryForDate("processedAt", Filters.empty(), (Class<Timestamp>) null)
     *     .subscribe(ts -> {}, err -> System.err.println("Null valueType: " + err));   // onError(IllegalArgumentException)
     * }</pre>
     *
     * @param <P> the specific Date subclass type
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @param valueType the class of the Date subclass to convert to
     * @return a {@code Mono} that emits the typed {@code Date} value on subscription, or completes
     *         empty when no document matches or the field is missing/null
     * @throws IllegalArgumentException if {@code propName} is null or empty, if {@code filter} is null, or if {@code valueType} is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     * @see #queryForDate(String, Bson)
     * @see #queryForSingleValue(String, Bson, Class)
     * @see MongoCollectionExecutor#queryForDate(String, Bson, Class)
     */
    public <P extends Date> Mono<P> queryForDate(final String propName, final Bson filter, final Class<P> valueType) {
        return collectionExecutor.queryForDate(propName, filter, valueType);
    }

    /**
     * Returns a {@code Mono} that, on subscription, queries for the given property of the first
     * matching document in this mapper's collection, converted to the specified type.
     *
     * <p>Only the named property of the first matched document is read; any remaining documents or
     * fields are ignored. This is the underlying method delegated to by the primitive-wrapper
     * convenience overloads ({@link #queryForBoolean}, {@link #queryForInt}, etc.). Delegates to
     * {@link MongoCollectionExecutor#queryForSingleValue(String, Bson, Class)}.</p>
     *
     * <p><b>Empty vs. present semantics:</b> the returned {@code Mono} completes <i>empty</i> when no
     * document matches the filter, or when a matching document's named field is absent or BSON null.
     * Otherwise it emits the converted value and then completes. Subscribers cannot distinguish "no
     * document matched" from "document matched but value is null" purely from the reactive signal —
     * use the blocking sync API when that distinction is required.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: convert the field to an arbitrary target type.
     * Mono<BigDecimal> amount = userMapper.queryForSingleValue(
     *     "amount", Filters.eq("id", entityId), BigDecimal.class);
     * amount.subscribe(a -> processPayment(a));   // emits the converted BigDecimal
     *
     * // Edge: no match OR missing/null field -> Mono completes EMPTY.
     * userMapper.queryForSingleValue("amount", Filters.eq("id", "missing"), BigDecimal.class)
     *     .defaultIfEmpty(BigDecimal.ZERO)
     *     .subscribe(a -> System.out.println("amount: " + a));   // emits 0 when absent
     *
     * // Edge: cold publisher — building it issues no query until subscribed.
     * Mono<BigDecimal> notRunYet = userMapper.queryForSingleValue("amount", Filters.empty(), BigDecimal.class);
     *
     * // Negative: null valueType -> error signal (IllegalArgumentException) on subscription.
     * userMapper.queryForSingleValue("amount", Filters.empty(), (Class<BigDecimal>) null)
     *     .subscribe(a -> {}, err -> System.err.println("Null valueType: " + err));   // onError(IllegalArgumentException)
     * }</pre>
     *
     * @param <V> the type of value to retrieve
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @param valueType the class of the value type to convert to
     * @return a {@code Mono} that emits the converted field value on subscription, or completes empty
     *         when no document matches or the field is missing/null
     * @throws IllegalArgumentException if {@code propName} is null or empty, if {@code filter} is null, or if {@code valueType} is null (thrown synchronously at the call site)
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Mono})
     * @see MongoCollectionExecutor#queryForSingleValue(String, Bson, Class)
     */
    public <V> Mono<V> queryForSingleValue(final String propName, final Bson filter, final Class<V> valueType) {
        return collectionExecutor.queryForSingleValue(propName, filter, valueType);
    }

    /**
     * Queries all matching documents and returns them as a single {@link Dataset}.
     *
     * <p>On subscription, the matching documents are collected into one {@code Dataset} whose
     * column layout is derived from the mapper's row type {@code T}, emitted once, and then the
     * {@code Mono} completes. Even when no documents match, the {@code Mono} emits an empty
     * {@code Dataset} rather than completing empty.</p>
     *
     * <p><b>Note:</b> all matching documents are materialised into memory before the {@code
     * Dataset} is emitted, so this is not suitable for very large result sets — use
     * {@link #list(Bson)} (streaming {@code Flux}) instead in that case.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: collect matches into one Dataset, emitted exactly once.
     * Bson filter = Filters.eq("department", "Sales");
     * Mono<Dataset> salesData = userMapper.query(filter);   // cold; emits one Dataset on subscribe
     * salesData.subscribe(data -> generateReport(data));
     *
     * // Edge: no match -> still emits exactly one (EMPTY) Dataset, never completes empty.
     * userMapper.query(Filters.eq("department", "no-such-dept"))
     *     .subscribe(data -> System.out.println("rows: " + data.size()));   // emits Dataset with size 0
     *
     * // Edge: cold publisher — building it issues no query until subscribed.
     * Mono<Dataset> notRunYet = userMapper.query(filter);   // nothing executed yet
     *
     * // Negative: a null filter is rejected with IllegalArgumentException, thrown synchronously
     * // at the call site (before any Mono is built).
     * userMapper.query((Bson) null);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param filter the query filter to match documents
     * @return a {@code Mono} that, on subscription, emits exactly one {@code Dataset} containing
     *         all matching documents (possibly empty), then completes
     * @throws IllegalArgumentException if filter is null (thrown synchronously at the call site)
     * @see Dataset
     * @see MongoCollectionExecutor#query(Bson, Class)
     */
    public Mono<Dataset> query(final Bson filter) {
        return collectionExecutor.query(filter, rowType);
    }

    /**
     * Queries documents with pagination and returns them as a Dataset.
     *
     * <p>Retrieves a paginated subset of documents matching the filter and returns them
     * as a Dataset. This is useful for processing large result sets in chunks.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: first page of up to 100 as one Dataset.
     * Bson filter = Filters.gte("score", 90);
     * Mono<Dataset> topScorers = userMapper.query(filter, 0, 100);   // cold; one Dataset (<=100 rows)
     * topScorers.subscribe(data -> displayLeaderboard(data));
     *
     * // Edge: offset past the end -> still emits one Dataset, but it is EMPTY.
     * userMapper.query(filter, 1_000_000, 100)
     *     .subscribe(data -> System.out.println("rows: " + data.size()));   // emits Dataset with size 0
     *
     * // Edge: cold publisher — nothing runs until subscription.
     * Mono<Dataset> notRunYet = userMapper.query(filter, 0, 100);   // no query issued yet
     *
     * // Negative: a negative offset is rejected with IllegalArgumentException.
     * userMapper.query(filter, -1, 100);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param filter the query filter to match documents against
     * @param offset the number of documents to skip (must be >= 0)
     * @param count the maximum number of documents to return (must be >= 0; {@code 0} yields an empty result)
     * @return a Mono that emits a Dataset containing the paginated results
     * @throws IllegalArgumentException if filter is null, offset is negative, or count is negative
     */
    public Mono<Dataset> query(final Bson filter, final int offset, final int count) {
        return collectionExecutor.query(filter, offset, count, rowType);
    }

    /**
     * Queries documents with field projection and returns them as a Dataset.
     *
     * <p>Retrieves documents matching the filter with only specified fields included
     * and returns them as a Dataset. This optimizes memory usage and network traffic.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: Dataset whose columns are exactly the selected fields.
     * List<String> fields = Arrays.asList("name", "email", "department");
     * Bson filter = Filters.eq("active", true);
     * Mono<Dataset> activeUsers = userMapper.query(fields, filter);   // cold; one Dataset with 3 columns
     * activeUsers.subscribe(data -> System.out.println("columns: " + data.columnNames()));
     *
     * // Edge: no match -> still emits one (EMPTY) Dataset.
     * userMapper.query(fields, Filters.eq("active", false))
     *     .subscribe(data -> System.out.println("rows: " + data.size()));   // emits Dataset with size 0
     *
     * // Edge: cold publisher — nothing runs until subscription.
     * Mono<Dataset> notRunYet = userMapper.query(fields, filter);   // no query issued yet
     *
     * // Negative: a null filter is rejected with IllegalArgumentException, thrown synchronously
     * // at the call site (before any Mono is built).
     * userMapper.query(fields, (Bson) null);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param selectPropNames the collection of property names to include in the results (null or empty selects all fields)
     * @param filter the query filter to match documents
     * @return a Mono that emits a Dataset with projected fields
     * @throws IllegalArgumentException if filter is null
     */
    public Mono<Dataset> query(final Collection<String> selectPropNames, final Bson filter) {
        return collectionExecutor.query(selectPropNames, filter, rowType);
    }

    /**
     * Queries documents with field projection and pagination, returning a Dataset.
     *
     * <p>Combines field projection with pagination to retrieve a subset of documents
     * with only specific fields, returned as a Dataset for structured data processing.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: projected first page of up to 50 rows.
     * List<String> fields = Arrays.asList("id", "name", "salary");
     * Bson filter = Filters.gt("salary", 50000);
     * Mono<Dataset> highEarners = userMapper.query(fields, filter, 0, 50);   // cold; one Dataset (<=50 rows)
     * highEarners.subscribe(data -> System.out.println("rows: " + data.size()));
     *
     * // Edge: offset past the end -> still emits one (EMPTY) Dataset.
     * userMapper.query(fields, filter, 1_000_000, 50)
     *     .subscribe(data -> System.out.println("rows: " + data.size()));   // emits Dataset with size 0
     *
     * // Edge: cold publisher — nothing runs until subscription.
     * Mono<Dataset> notRunYet = userMapper.query(fields, filter, 0, 50);   // no query issued yet
     *
     * // Negative: a negative offset is rejected with IllegalArgumentException.
     * userMapper.query(fields, filter, -1, 50);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param selectPropNames the collection of property names to include in the results
     * @param filter the query filter to match documents against
     * @param offset the number of documents to skip (must be >= 0)
     * @param count the maximum number of documents to return (must be >= 0; {@code 0} yields an empty result)
     * @return a Mono that emits a Dataset with projected and paginated results
     * @throws IllegalArgumentException if filter is null, offset is negative, or count is negative
     */
    public Mono<Dataset> query(final Collection<String> selectPropNames, final Bson filter, final int offset, final int count) {
        return collectionExecutor.query(selectPropNames, filter, offset, count, rowType);
    }

    /**
     * Queries documents with field projection and sorting, returning a Dataset.
     *
     * <p>Retrieves sorted documents matching the filter with specified field projection,
     * returned as a Dataset. The sorting order determines the sequence of rows in the Dataset.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: rows ordered newest-first by joinDate.
     * List<String> fields = Arrays.asList("name", "joinDate", "level");
     * Bson filter = Filters.eq("status", "member");
     * Bson sort = Sorts.descending("joinDate");
     * Mono<Dataset> members = userMapper.query(fields, filter, sort);   // cold; one Dataset, newest-first
     * members.subscribe(data -> System.out.println("rows: " + data.size()));
     *
     * // Edge: no match -> still emits one (EMPTY) Dataset.
     * userMapper.query(fields, Filters.eq("status", "no-such-status"), sort)
     *     .subscribe(data -> System.out.println("rows: " + data.size()));   // emits Dataset with size 0
     *
     * // Edge: null sort is tolerated -> rows in natural order.
     * Mono<Dataset> unsorted = userMapper.query(fields, filter, (Bson) null);   // no sort applied; not an error
     *
     * // Negative: a null filter is rejected with IllegalArgumentException, thrown synchronously
     * // at the call site (before any Mono is built).
     * userMapper.query(fields, (Bson) null, sort);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param selectPropNames the collection of property names to include in the results
     * @param filter the query filter to match documents against
     * @param sort the sort specification for ordering results
     * @return a Mono that emits a sorted Dataset with projected fields
     * @throws IllegalArgumentException if filter is null
     */
    public Mono<Dataset> query(final Collection<String> selectPropNames, final Bson filter, final Bson sort) {
        return collectionExecutor.query(selectPropNames, filter, sort, rowType);
    }

    /**
     * Queries documents with complete control over projection, filtering, sorting, and pagination.
     *
     * <p>Provides full control over query execution with field projection, filtering, sorting,
     * and pagination, returning results as a Dataset for structured data manipulation.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: top-10 leaderboard Dataset.
     * List<String> fields = Arrays.asList("id", "name", "score", "rank");
     * Bson filter = Filters.gte("score", 80);
     * Bson sort = Sorts.descending("score");
     * Mono<Dataset> topPlayers = userMapper.query(fields, filter, sort, 0, 10);   // cold; one Dataset (<=10 rows)
     * topPlayers.subscribe(data -> System.out.println("rows: " + data.size()));
     *
     * // Edge: offset past the end -> still emits one (EMPTY) Dataset.
     * userMapper.query(fields, filter, sort, 1_000_000, 10)
     *     .subscribe(data -> System.out.println("rows: " + data.size()));   // emits Dataset with size 0
     *
     * // Edge: cold publisher — nothing runs until subscription.
     * Mono<Dataset> notRunYet = userMapper.query(fields, filter, sort, 0, 10);   // no query issued yet
     *
     * // Negative: a negative offset is rejected with IllegalArgumentException.
     * userMapper.query(fields, filter, sort, -1, 10);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param selectPropNames the collection of property names to include in the results
     * @param filter the query filter to match documents against
     * @param sort the sort specification for ordering results
     * @param offset the number of documents to skip (must be >= 0)
     * @param count the maximum number of documents to return (must be >= 0; {@code 0} yields an empty result)
     * @return a Mono that emits a fully controlled Dataset result
     * @throws IllegalArgumentException if filter is null, offset is negative, or count is negative
     */
    public Mono<Dataset> query(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final int offset, final int count) {
        return collectionExecutor.query(selectPropNames, filter, sort, offset, count, rowType);
    }

    /**
     * Queries documents using BSON projection with sorting, returning a Dataset.
     *
     * <p>Uses MongoDB projection document for field selection with sorting support,
     * returning results as a Dataset. Provides fine-grained control over field inclusion/exclusion.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: include name/stats, drop _id, sort by name.
     * Bson projection = Projections.fields(
     *     Projections.include("name", "stats"),
     *     Projections.excludeId()
     * );
     * Bson filter = Filters.eq("type", "player");
     * Bson sort = Sorts.ascending("name");
     * Mono<Dataset> playerStats = userMapper.query(projection, filter, sort);   // cold; one projected, sorted Dataset
     * playerStats.subscribe(data -> System.out.println("rows: " + data.size()));
     *
     * // Edge: no match -> still emits one (EMPTY) Dataset.
     * userMapper.query(projection, Filters.eq("type", "no-such-type"), sort)
     *     .subscribe(data -> System.out.println("rows: " + data.size()));   // emits Dataset with size 0
     *
     * // Edge: null sort is tolerated (it is optional here) -> natural order.
     * Mono<Dataset> unsorted = userMapper.query(projection, filter, (Bson) null);   // no sort applied; not an error
     *
     * // Negative: a null filter is rejected with IllegalArgumentException, thrown synchronously
     * // at the call site (before any Mono is built).
     * userMapper.query(projection, (Bson) null, sort);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param projection the BSON projection document for field selection
     * @param filter the query filter to match documents against
     * @param sort the sort specification for ordering results (can be null)
     * @return a Mono that emits a Dataset with projection and sorting applied
     * @throws IllegalArgumentException if filter is null
     */
    public Mono<Dataset> query(final Bson projection, final Bson filter, final Bson sort) {
        return collectionExecutor.query(projection, filter, sort, rowType);
    }

    /**
     * Queries documents with BSON projection and full pagination control, returning a Dataset.
     *
     * <p>Combines BSON projection with complete query control including filtering, sorting,
     * and pagination. Returns results as a Dataset for maximum flexibility in data processing.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: most-recent 1000 metrics as one projected, sorted Dataset.
     * Bson projection = Projections.include("id", "metrics", "timestamp");
     * Bson filter = Filters.gte("timestamp", startDate);
     * Bson sort = Sorts.descending("timestamp");
     * Mono<Dataset> recentMetrics = userMapper.query(projection, filter, sort, 0, 1000);   // cold; one Dataset (<=1000 rows)
     * recentMetrics.subscribe(data -> System.out.println("rows: " + data.size()));
     *
     * // Edge: offset past the end -> still emits one (EMPTY) Dataset.
     * userMapper.query(projection, filter, sort, 1_000_000, 1000)
     *     .subscribe(data -> System.out.println("rows: " + data.size()));   // emits Dataset with size 0
     *
     * // Edge: null sort is tolerated -> natural order, still paginated.
     * Mono<Dataset> unsorted = userMapper.query(projection, filter, (Bson) null, 0, 1000);   // no sort applied
     *
     * // Negative: a negative offset is rejected with IllegalArgumentException.
     * userMapper.query(projection, filter, sort, -1, 1000);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param projection the BSON projection document for field selection
     * @param filter the query filter to match documents against
     * @param sort the sort specification for ordering results (can be null)
     * @param offset the number of documents to skip (must be >= 0)
     * @param count the maximum number of documents to return (must be >= 0; {@code 0} yields an empty result)
     * @return a Mono that emits a fully controlled Dataset with projection
     * @throws IllegalArgumentException if filter is null
     */
    public Mono<Dataset> query(final Bson projection, final Bson filter, final Bson sort, final int offset, final int count) {
        return collectionExecutor.query(projection, filter, sort, offset, count, rowType);
    }

    /**
     * Inserts a single entity into the collection reactively.
     *
     * <p>This method provides a reactive way to insert a single entity of the mapped type into the
     * collection. The entity is automatically converted to a MongoDB document using the configured
     * codec registry.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: insert one entity; subscribing performs the write and emits the result.
     * User newUser = new User("John Doe", "john@example.com");
     * userMapper.insertOne(newUser)
     *     .subscribe(result -> System.out.println("Inserted: " + result.getInsertedId()));   // emits one InsertOneResult
     *
     * // Edge: cold publisher — building the Mono does NOT insert until subscribed.
     * Mono<InsertOneResult> pending = userMapper.insertOne(newUser);   // nothing written yet
     * pending.subscribe();                                             // subscription actually performs the insert
     *
     * // Edge: subscribing the SAME publisher twice issues the underlying insert twice.
     * Mono<InsertOneResult> ins = userMapper.insertOne(newUser);
     * ins.subscribe();   // first insert
     * ins.subscribe();   // second insert (likely a duplicate-key MongoWriteException via onError if _id repeats)
     *
     * // Negative: inserting an entity whose _id already exists -> MongoWriteException on subscription.
     * userMapper.insertOne(existingUser)
     *     .subscribe(r -> {}, err -> System.err.println("Duplicate key: " + err));   // onError(MongoWriteException)
     * }</pre>
     *
     * @param obj the entity to insert
     * @return a cold {@code Mono} that, on subscription, emits exactly one
     *         {@link InsertOneResult} when the insert completes, then completes
     * @throws IllegalArgumentException if obj is null
     * @throws com.mongodb.MongoWriteException if the insert operation fails (signalled via
     *         {@code Mono})
     * @throws com.mongodb.MongoException if the database operation fails (signalled via
     *         {@code Mono})
     * @see #insertOne(Object, InsertOneOptions)
     * @see #insertMany(Collection)
     */
    public Mono<InsertOneResult> insertOne(final T obj) {
        return collectionExecutor.insertOne(obj);
    }

    /**
     * Inserts a single entity into the collection with custom options reactively.
     *
     * <p>This method provides a reactive way to insert a single entity with additional insertion options
     * such as bypass document validation. The entity is automatically converted to a MongoDB document.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: insert bypassing schema validation.
     * User newUser = new User("John Doe", "john@example.com");
     * InsertOneOptions options = new InsertOneOptions().bypassDocumentValidation(true);
     * userMapper.insertOne(newUser, options)
     *     .subscribe(result -> System.out.println("Inserted: " + result.getInsertedId()));   // emits one InsertOneResult
     *
     * // Edge: null options behaves like the no-options overload (driver defaults).
     * userMapper.insertOne(newUser, (InsertOneOptions) null).subscribe();   // default insert
     *
     * // Edge: cold publisher — building it inserts nothing until subscribed.
     * Mono<InsertOneResult> pending = userMapper.insertOne(newUser, options);   // nothing written yet
     *
     * // Negative: duplicate _id -> MongoWriteException on subscription.
     * userMapper.insertOne(existingUser, options)
     *     .subscribe(r -> {}, err -> System.err.println("Duplicate key: " + err));   // onError(MongoWriteException)
     * }</pre>
     *
     * @param obj the entity to insert
     * @param options the insert options to apply (null uses default settings)
     * @return a cold {@code Mono} that, on subscription, emits exactly one
     *         {@link InsertOneResult} when the insert completes, then completes
     * @throws IllegalArgumentException if obj is null
     * @throws com.mongodb.MongoWriteException if the insert operation fails (signalled via
     *         {@code Mono})
     * @throws com.mongodb.MongoException if the database operation fails (signalled via
     *         {@code Mono})
     * @see #insertOne(Object)
     * @see InsertOneOptions
     */
    public Mono<InsertOneResult> insertOne(final T obj, final InsertOneOptions options) {
        return collectionExecutor.insertOne(obj, options);
    }

    /**
     * Inserts multiple entities into the collection reactively.
     *
     * <p>This method provides a reactive way to insert a collection of entities of the mapped type.
     * All entities are automatically converted to MongoDB documents using the configured codec
     * registry.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: insert several entities; result reports one inserted id per entity.
     * List<User> users = Arrays.asList(
     *     new User("John", "john@example.com"),
     *     new User("Jane", "jane@example.com")
     * );
     * userMapper.insertMany(users)
     *     .subscribe(result -> System.out.println("Inserted: " + result.getInsertedIds().size()));   // emits 2 ids
     *
     * // Edge: cold publisher — building it inserts nothing until subscribed.
     * Mono<InsertManyResult> pending = userMapper.insertMany(users);   // nothing written yet
     *
     * // Negative: a null or empty collection -> IllegalArgumentException.
     * userMapper.insertMany(Collections.emptyList());   // throws IllegalArgumentException
     *
     * // Negative: if any document fails (e.g. duplicate _id) the batch fails via onError.
     * userMapper.insertMany(usersWithDuplicate)
     *     .subscribe(r -> {}, err -> System.err.println("Bulk insert failed: " + err));   // onError(MongoBulkWriteException)
     * }</pre>
     *
     * @param objList the collection of entities to insert
     * @return a cold {@code Mono} that, on subscription, emits exactly one
     *         {@link InsertManyResult} when the operation completes, then completes
     * @throws IllegalArgumentException if objList is null or empty
     * @throws com.mongodb.MongoBulkWriteException if any document fails to insert (signalled via
     *         {@code Mono})
     */
    public Mono<InsertManyResult> insertMany(final Collection<? extends T> objList) {
        return collectionExecutor.insertMany(objList);
    }

    /**
     * Inserts multiple entities into the collection with custom options reactively.
     *
     * <p>This method provides a reactive way to insert a collection of entities with additional
     * insertion options such as ordered insertion and bypass document validation. All entities
     * are automatically converted to MongoDB documents.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<User> users = Arrays.asList(
     *     new User("John", "john@example.com"),
     *     new User("Jane", "jane@example.com")
     * );
     * InsertManyOptions options = new InsertManyOptions().ordered(false);
     * userMapper.insertMany(users, options)
     *     .subscribe(result -> System.out.println("Inserted: " + result.getInsertedIds().size()));
     * }</pre>
     *
     * @param objList the collection of entities to insert
     * @param options the insert options to apply (can be null)
     * @return a Mono that emits the insert result when the operation completes
     * @throws IllegalArgumentException if objList is null or empty
     * @see InsertManyOptions
     */
    public Mono<InsertManyResult> insertMany(final Collection<? extends T> objList, final InsertManyOptions options) {
        return collectionExecutor.insertMany(objList, options);
    }

    /**
     * Updates one document identified by string ID with the provided entity reactively.
     *
     * <p>Updates the first document matching the specified string object ID with the values
     * from the provided entity in a reactive manner. The entity is converted to update operations automatically.
     * Returns a Mono that emits the update result.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: matched id -> getModifiedCount() == 1 (when a value actually changed).
     * User updatedUser = new User();
     * updatedUser.setEmail("newemail@example.com");
     * Mono<UpdateResult> result = userMapper.updateOne("507f1f77bcf86cd799439011", updatedUser);   // cold
     * result.subscribe(r -> System.out.println("Modified: " + r.getModifiedCount()));              // emits 1 on a real change
     *
     * // Edge: no document with that id -> result has matchedCount == 0 and modifiedCount == 0.
     * userMapper.updateOne("000000000000000000000000", updatedUser)
     *     .subscribe(r -> System.out.println("matched: " + r.getMatchedCount()));   // emits 0
     *
     * // Edge: cold publisher — building it writes nothing until subscribed.
     * Mono<UpdateResult> pending = userMapper.updateOne("507f1f77bcf86cd799439011", updatedUser);   // nothing written yet
     *
     * // Negative: a malformed id -> IllegalArgumentException.
     * userMapper.updateOne("not-hex", updatedUser);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param objectId the string representation of the MongoDB ObjectId
     * @param update the entity containing the update values
     * @return a Mono that emits the update result
     * @throws IllegalArgumentException if objectId or update is null, or if objectId is not a valid ObjectId format
     */
    public Mono<UpdateResult> updateOne(final String objectId, final T update) {
        return collectionExecutor.updateOne(objectId, update);
    }

    /**
     * Updates one document identified by ObjectId with the provided entity reactively.
     *
     * <p>Updates the first document matching the specified ObjectId with the values
     * from the provided entity in a reactive manner. This method provides type-safe ObjectId handling
     * and returns a Mono.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
     * User updates = new User();
     * updates.setStatus("active");
     * Mono<UpdateResult> result = userMapper.updateOne(id, updates);
     * }</pre>
     *
     * @param objectId the MongoDB ObjectId to match
     * @param update the entity containing the update values
     * @return a Mono that emits the update result
     * @throws IllegalArgumentException if objectId or update is null
     */
    public Mono<UpdateResult> updateOne(final ObjectId objectId, final T update) {
        return collectionExecutor.updateOne(objectId, update);
    }

    /**
     * Updates one document matching the filter with the provided entity reactively.
     *
     * <p>Updates the first document matching the specified filter with values from
     * the provided entity in a reactive manner. This is the most flexible single-document update method.
     * Returns a Mono that emits upon completion.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("email", "old@example.com");
     * User updates = new User();
     * updates.setEmail("new@example.com");
     * Mono<UpdateResult> result = userMapper.updateOne(filter, updates);
     * }</pre>
     *
     * @param filter the query filter to match documents
     * @param update the entity containing the update values
     * @return a Mono that emits the update result
     * @throws IllegalArgumentException if filter or update is null (thrown synchronously at the call site)
     */
    public Mono<UpdateResult> updateOne(final Bson filter, final T update) {
        return collectionExecutor.updateOne(filter, update);
    }

    /**
     * Updates one document with custom update options.
     *
     * <p>Updates the first document matching the filter with values from the entity,
     * applying custom update options such as upsert behavior.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("userId", userId);
     * User updates = new User();
     * updates.setLastLogin(new Date());
     * UpdateOptions options = new UpdateOptions().upsert(true);
     * Mono<UpdateResult> result = userMapper.updateOne(filter, updates, options);
     * }</pre>
     *
     * @param filter the query filter to match documents against
     * @param update the entity containing the update values
     * @param options the update options to apply (can be null)
     * @return a Mono that emits the update result
     * @throws IllegalArgumentException if filter or update is null
     * @see UpdateOptions
     */
    public Mono<UpdateResult> updateOne(final Bson filter, final T update, final UpdateOptions options) {
        return collectionExecutor.updateOne(filter, update, options);
    }

    /**
     * Updates one document using multiple entities for complex updates.
     *
     * <p>Updates the first matching document using values from multiple entities.
     * This is useful for partial updates from multiple sources or complex update logic.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("id", userId);
     * List<User> updates = Arrays.asList(profileUpdate, settingsUpdate);
     * Mono<UpdateResult> result = userMapper.updateOne(filter, updates);
     * }</pre>
     *
     * @param filter the query filter to match documents against
     * @param objList the collection of entities containing update values
     * @return a Mono that emits the update result
     * @throws IllegalArgumentException if filter or objList is null or empty
     */
    public Mono<UpdateResult> updateOne(final Bson filter, final Collection<? extends T> objList) {
        return collectionExecutor.updateOne(filter, objList);
    }

    /**
     * Updates one document using multiple entities with custom options.
     *
     * <p>Updates the first matching document using values from multiple entities,
     * with support for custom update options like upsert.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("email", email);
     * List<User> updates = Arrays.asList(profileData, preferences);
     * UpdateOptions options = new UpdateOptions().upsert(true);
     * Mono<UpdateResult> result = userMapper.updateOne(filter, updates, options);
     * }</pre>
     *
     * @param filter the query filter to match documents against
     * @param objList the collection of entities containing update values
     * @param options the update options to apply (can be null)
     * @return a Mono that emits the update result
     * @throws IllegalArgumentException if filter or objList is null or empty
     */
    public Mono<UpdateResult> updateOne(final Bson filter, final Collection<? extends T> objList, final UpdateOptions options) {
        return collectionExecutor.updateOne(filter, objList, options);
    }

    /**
     * Updates all documents matching the filter with the provided entity reactively.
     *
     * <p>Updates all documents matching the specified filter with values from
     * the provided entity in a reactive manner. This is useful for bulk updates of multiple documents.
     * Returns a Mono that emits the update result.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: update every matching document; modifiedCount reflects how many changed.
     * Bson filter = Filters.eq("department", "Engineering");
     * User updates = new User();
     * updates.setSalaryMultiplier(1.1);
     * Mono<UpdateResult> result = userMapper.updateMany(filter, updates);   // cold
     * result.subscribe(r -> System.out.println("Updated " + r.getModifiedCount() + " documents"));
     *
     * // Edge: a filter matching nothing -> matchedCount == 0 and modifiedCount == 0.
     * userMapper.updateMany(Filters.eq("department", "no-such-dept"), updates)
     *     .subscribe(r -> System.out.println("matched: " + r.getMatchedCount()));   // emits 0
     *
     * // Edge: cold publisher — building it writes nothing until subscribed.
     * Mono<UpdateResult> pending = userMapper.updateMany(filter, updates);   // nothing written yet
     *
     * // Negative: a null filter throws IllegalArgumentException synchronously (at the call site,
     * // before any Mono is returned), because filter is validated eagerly.
     * userMapper.updateMany((Bson) null, updates);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param filter the query filter to match documents against
     * @param update the entity containing the update values
     * @return a Mono that emits the update result with modified count
     * @throws IllegalArgumentException if filter or update is null
     */
    public Mono<UpdateResult> updateMany(final Bson filter, final T update) {
        return collectionExecutor.updateMany(filter, update);
    }

    /**
     * Updates all documents matching the filter with custom options.
     *
     * <p>Updates all matching documents with values from the entity, applying
     * custom update options such as upsert behavior for bulk operations.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.lt("lastActive", thirtyDaysAgo);
     * User updates = new User();
     * updates.setStatus("inactive");
     * UpdateOptions options = new UpdateOptions().upsert(false);
     * Mono<UpdateResult> result = userMapper.updateMany(filter, updates, options);
     * }</pre>
     *
     * @param filter the query filter to match documents against
     * @param update the entity containing the update values
     * @param options the update options to apply (can be null)
     * @return a Mono that emits the update result with modified count
     * @throws IllegalArgumentException if filter or update is null
     */
    public Mono<UpdateResult> updateMany(final Bson filter, final T update, final UpdateOptions options) {
        return collectionExecutor.updateMany(filter, update, options);
    }

    /**
     * Updates all documents matching the filter using multiple entities.
     *
     * <p>Updates all matching documents using values from multiple entities.
     * This enables complex bulk updates with data from multiple sources.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("needsUpdate", true);
     * List<User> updates = Arrays.asList(defaultValues, overrideValues);
     * Mono<UpdateResult> result = userMapper.updateMany(filter, updates);
     * }</pre>
     *
     * @param filter the query filter to match documents against
     * @param objList the collection of entities containing update values
     * @return a Mono that emits the update result with modified count
     * @throws IllegalArgumentException if filter or objList is null or empty
     */
    public Mono<UpdateResult> updateMany(final Bson filter, final Collection<? extends T> objList) {
        return collectionExecutor.updateMany(filter, objList);
    }

    /**
     * Updates all documents matching the filter using multiple entities with options.
     *
     * <p>Updates all matching documents using values from multiple entities,
     * with support for custom update options in bulk operations.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.in("status", "pending", "draft");
     * List<User> updates = Arrays.asList(statusUpdate, timestampUpdate);
     * UpdateOptions options = new UpdateOptions().upsert(false);
     * Mono<UpdateResult> result = userMapper.updateMany(filter, updates, options);
     * }</pre>
     *
     * @param filter the query filter to match documents against
     * @param objList the collection of entities containing update values
     * @param options the update options to apply (can be null)
     * @return a Mono that emits the update result with modified count
     * @throws IllegalArgumentException if filter or objList is null or empty
     */
    public Mono<UpdateResult> updateMany(final Bson filter, final Collection<? extends T> objList, final UpdateOptions options) {
        return collectionExecutor.updateMany(filter, objList, options);
    }

    /**
     * Replaces one document identified by string ID with a new entity.
     *
     * <p>Completely replaces the document with the specified string object ID
     * with the provided entity. Unlike update operations, this replaces the entire document.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: matched id -> the whole document (except _id) is overwritten; modifiedCount == 1 on change.
     * User newUser = new User("John", "john@example.com");
     * Mono<UpdateResult> result = userMapper.replaceOne("507f1f77bcf86cd799439011", newUser);   // cold
     * result.subscribe(r -> System.out.println("Replaced: " + r.getModifiedCount()));           // emits 1 on a real change
     *
     * // Edge: no document with that id -> matchedCount == 0, nothing replaced.
     * userMapper.replaceOne("000000000000000000000000", newUser)
     *     .subscribe(r -> System.out.println("matched: " + r.getMatchedCount()));   // emits 0
     *
     * // Edge: cold publisher — building it writes nothing until subscribed.
     * Mono<UpdateResult> pending = userMapper.replaceOne("507f1f77bcf86cd799439011", newUser);   // nothing written yet
     *
     * // Negative: a malformed id -> IllegalArgumentException.
     * userMapper.replaceOne("not-hex", newUser);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param objectId the string representation of the MongoDB ObjectId
     * @param replacement the entity to replace the existing document with
     * @return a Mono that emits the update result
     * @throws IllegalArgumentException if objectId or replacement is null, or if objectId is not a valid ObjectId format
     */
    public Mono<UpdateResult> replaceOne(final String objectId, final T replacement) {
        return collectionExecutor.replaceOne(objectId, replacement);
    }

    /**
     * Replaces one document identified by ObjectId with a new entity.
     *
     * <p>Completely replaces the document with the specified ObjectId
     * with the provided entity. This provides type-safe document replacement.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
     * User newUser = new User("Jane", "jane@example.com");
     * Mono<UpdateResult> result = userMapper.replaceOne(id, newUser);
     * }</pre>
     *
     * @param objectId the MongoDB ObjectId to match
     * @param replacement the entity to replace the existing document with
     * @return a Mono that emits the update result
     * @throws IllegalArgumentException if objectId or replacement is null
     */
    public Mono<UpdateResult> replaceOne(final ObjectId objectId, final T replacement) {
        return collectionExecutor.replaceOne(objectId, replacement);
    }

    /**
     * Replaces one document matching the filter with a new entity.
     *
     * <p>Completely replaces the first document matching the filter with the provided entity.
     * This is the most flexible replacement method.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("username", "oldusername");
     * User newUser = new User("newusername", "new@example.com");
     * Mono<UpdateResult> result = userMapper.replaceOne(filter, newUser);
     * }</pre>
     *
     * @param filter the query filter to match documents against
     * @param replacement the entity to replace the existing document with
     * @return a Mono that emits the update result
     * @throws IllegalArgumentException if filter or replacement is null
     */
    public Mono<UpdateResult> replaceOne(final Bson filter, final T replacement) {
        return collectionExecutor.replaceOne(filter, replacement);
    }

    /**
     * Replaces one document with custom replace options.
     *
     * <p>Completely replaces a document matching the filter with the provided entity,
     * applying custom options such as upsert behavior.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("externalId", externalId);
     * User newUser = createUserFromExternalData();
     * ReplaceOptions options = new ReplaceOptions().upsert(true);
     * Mono<UpdateResult> result = userMapper.replaceOne(filter, newUser, options);
     * }</pre>
     *
     * @param filter the query filter to match documents against
     * @param replacement the entity to replace the existing document with
     * @param options the replace options to apply (can be null)
     * @return a Mono that emits the update result
     * @throws IllegalArgumentException if filter or replacement is null
     * @see ReplaceOptions
     */
    public Mono<UpdateResult> replaceOne(final Bson filter, final T replacement, final ReplaceOptions options) {
        return collectionExecutor.replaceOne(filter, replacement, options);
    }

    /**
     * Deletes one document identified by string ID.
     *
     * <p>Deletes the document with the specified string object ID from the collection.
     * This is a convenient method for deleting documents by their primary key.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: existing id -> getDeletedCount() == 1.
     * Mono<DeleteResult> result = userMapper.deleteOne("507f1f77bcf86cd799439011");   // cold
     * result.subscribe(r -> System.out.println("Deleted: " + r.getDeletedCount()));   // emits 1 when present
     *
     * // Edge: an unused id -> getDeletedCount() == 0 (no error).
     * userMapper.deleteOne("000000000000000000000000")
     *     .subscribe(r -> System.out.println("deleted: " + r.getDeletedCount()));   // emits 0
     *
     * // Edge: cold publisher — building it deletes nothing until subscribed.
     * Mono<DeleteResult> pending = userMapper.deleteOne("507f1f77bcf86cd799439011");   // nothing deleted yet
     *
     * // Negative: a malformed id -> IllegalArgumentException.
     * userMapper.deleteOne("not-hex");   // throws IllegalArgumentException
     * }</pre>
     *
     * @param objectId the string representation of the MongoDB ObjectId
     * @return a Mono that emits the delete result
     * @throws IllegalArgumentException if objectId is null, or if objectId is not a valid ObjectId format
     */
    public Mono<DeleteResult> deleteOne(final String objectId) {
        return collectionExecutor.deleteOne(objectId);
    }

    /**
     * Deletes one document identified by ObjectId.
     *
     * <p>Deletes the document with the specified ObjectId from the collection.
     * This provides type-safe deletion by primary key.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
     * Mono<DeleteResult> result = userMapper.deleteOne(id);
     * result.subscribe(r -> System.out.println("Deleted: " + r.getDeletedCount()));
     * }</pre>
     *
     * @param objectId the MongoDB ObjectId to match
     * @return a Mono that emits the delete result
     * @throws IllegalArgumentException if objectId is null
     */
    public Mono<DeleteResult> deleteOne(final ObjectId objectId) {
        return collectionExecutor.deleteOne(objectId);
    }

    /**
     * Deletes one document matching the filter reactively.
     *
     * <p>Deletes the first document matching the specified filter from the collection in a reactive manner.
     * This is the most flexible single-document deletion method. Returns a Mono that emits upon completion.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: deletes at most one matching document.
     * Bson filter = Filters.eq("email", "user@example.com");
     * Mono<DeleteResult> result = userMapper.deleteOne(filter);                       // cold
     * result.subscribe(r -> System.out.println("Deleted: " + r.getDeletedCount()));   // emits 0 or 1
     *
     * // Edge: a filter matching nothing -> getDeletedCount() == 0 (no error).
     * userMapper.deleteOne(Filters.eq("email", "missing@example.com"))
     *     .subscribe(r -> System.out.println("deleted: " + r.getDeletedCount()));   // emits 0
     *
     * // Edge: cold publisher — building it deletes nothing until subscribed.
     * Mono<DeleteResult> pending = userMapper.deleteOne(filter);   // nothing deleted yet
     *
     * // Negative: a null filter throws IllegalArgumentException synchronously (at the call site,
     * // before any Mono is returned), because filter is validated eagerly.
     * userMapper.deleteOne((Bson) null);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param filter the query filter to match documents against
     * @return a Mono that emits the delete result
     * @throws IllegalArgumentException if filter is null (thrown synchronously at the call site)
     */
    public Mono<DeleteResult> deleteOne(final Bson filter) {
        return collectionExecutor.deleteOne(filter);
    }

    /**
     * Deletes one document with custom delete options.
     *
     * <p>Deletes the first document matching the filter with custom options
     * such as collation settings for locale-specific matching.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("username", "JohnDoe");
     * DeleteOptions options = new DeleteOptions().collation(
     *     Collation.builder().locale("en").strength(2).build()
     * );
     * Mono<DeleteResult> result = userMapper.deleteOne(filter, options);
     * }</pre>
     *
     * @param filter the query filter to match documents against
     * @param options the delete options to apply (can be null)
     * @return a Mono that emits the delete result
     * @throws IllegalArgumentException if filter is null
     * @see DeleteOptions
     */
    public Mono<DeleteResult> deleteOne(final Bson filter, final DeleteOptions options) {
        return collectionExecutor.deleteOne(filter, options);
    }

    /**
     * Deletes all documents matching the filter reactively.
     *
     * <p>Deletes all documents that match the specified filter from the collection in a reactive manner.
     * This is useful for bulk deletion operations. Returns a Mono that emits the result upon completion.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: bulk-delete every matching document.
     * Bson filter = Filters.lt("lastLogin", oneYearAgo);
     * Mono<DeleteResult> result = userMapper.deleteMany(filter);   // cold
     * result.subscribe(r -> System.out.println("Deleted " + r.getDeletedCount() + " inactive users"));
     *
     * // Edge: a filter matching nothing -> getDeletedCount() == 0 (no error).
     * userMapper.deleteMany(Filters.eq("status", "no-such-status"))
     *     .subscribe(r -> System.out.println("deleted: " + r.getDeletedCount()));   // emits 0
     *
     * // Edge: an empty filter deletes EVERY document in the collection — use with care.
     * userMapper.deleteMany(Filters.empty()).subscribe(r -> System.out.println("wiped: " + r.getDeletedCount()));
     *
     * // Negative: a null filter throws IllegalArgumentException synchronously (at the call site,
     * // before any Mono is returned), because filter is validated eagerly.
     * userMapper.deleteMany((Bson) null);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param filter the query filter to match documents against
     * @return a Mono that emits the delete result with deleted count
     * @throws IllegalArgumentException if filter is null
     */
    public Mono<DeleteResult> deleteMany(final Bson filter) {
        return collectionExecutor.deleteMany(filter);
    }

    /**
     * Deletes all documents matching the filter with custom options.
     *
     * <p>Performs a bulk deletion with additional control over the operation behavior
     * through DeleteOptions. This allows configuration of features like collation for
     * locale-specific filtering or hints for query optimization.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DeleteOptions options = new DeleteOptions().collation(Collation.builder().locale("en").build());
     * userMapper.deleteMany(Filters.eq("status", "inactive"), options)
     *     .subscribe(r -> System.out.println("Deleted: " + r.getDeletedCount()));
     * }</pre>
     *
     * @param filter the query filter to match documents for deletion
     * @param options additional options to configure the delete operation
     * @return a cold {@code Mono} that, on subscription, emits exactly one {@link DeleteResult}
     *         with deletion statistics, then completes
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoWriteException if the delete operation fails (signalled via
     *         {@code Mono})
     * @throws com.mongodb.MongoException if the database operation fails (signalled via
     *         {@code Mono})
     */
    public Mono<DeleteResult> deleteMany(final Bson filter, final DeleteOptions options) {
        return collectionExecutor.deleteMany(filter, options);
    }

    /**
     * Performs a bulk insert of multiple entities into the collection in a reactive manner.
     *
     * <p>Each entity is wrapped in an {@link com.mongodb.client.model.InsertOneModel} and
     * submitted as a single bulk write. Delegates to
     * {@link MongoCollectionExecutor#bulkInsert(Collection)}, which emits the underlying
     * {@link BulkWriteResult}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: emits the BulkWriteResult (one inserted document per entity on success).
     * List<User> newUsers = Arrays.asList(user1, user2, user3);
     * userMapper.bulkInsert(newUsers)
     *     .subscribe(result -> System.out.println("Inserted " + result.getInsertedCount() + " users"));   // inserted 3
     *
     * // Edge: cold publisher — building it inserts nothing until subscribed.
     * Mono<BulkWriteResult> pending = userMapper.bulkInsert(newUsers);   // nothing written yet
     *
     * // Negative: a null or empty collection -> IllegalArgumentException.
     * userMapper.bulkInsert(Collections.emptyList());   // throws IllegalArgumentException
     *
     * // Negative: a per-document failure (e.g. duplicate _id) -> onError(MongoBulkWriteException).
     * userMapper.bulkInsert(usersWithDuplicate)
     *     .subscribe(r -> {}, err -> System.err.println("Bulk insert failed: " + err));   // onError(MongoBulkWriteException)
     * }</pre>
     *
     * @param entities the collection of entities to insert (must not be null or empty)
     * @return a {@code Mono} that, on subscription, emits exactly one {@link BulkWriteResult}
     *         (use {@link BulkWriteResult#getInsertedCount()} for the inserted count), then completes
     * @throws IllegalArgumentException if entities is null or empty
     * @throws com.mongodb.MongoBulkWriteException if the bulk write reports any per-document
     *         failures (signalled via {@code Mono})
     * @see MongoCollectionExecutor#bulkInsert(Collection)
     */
    public Mono<BulkWriteResult> bulkInsert(final Collection<? extends T> entities) {
        return collectionExecutor.bulkInsert(entities);
    }

    /**
     * Performs a bulk insert with custom write options.
     *
     * <p>Inserts multiple entities with additional control over the write behavior,
     * such as ordering guarantees and bypass document validation. Ordered inserts
     * stop on first error, while unordered continue despite errors.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * BulkWriteOptions options = new BulkWriteOptions().ordered(false);
     * userMapper.bulkInsert(userList, options)
     *     .subscribe(result -> System.out.println("Bulk inserted: " + result.getInsertedCount()));
     * }</pre>
     *
     * @param entities the collection of entities to insert
     * @param options configuration options for the bulk write operation (may be null to use defaults)
     * @return a Mono emitting the {@link BulkWriteResult} (use {@link BulkWriteResult#getInsertedCount()}
     *         for the inserted count)
     * @throws IllegalArgumentException if entities is null or empty
     */
    public Mono<BulkWriteResult> bulkInsert(final Collection<? extends T> entities, final BulkWriteOptions options) {
        return collectionExecutor.bulkInsert(entities, options);
    }

    /**
     * Executes multiple write operations in a single bulk request.
     *
     * <p>Performs a batch of mixed write operations (inserts, updates, deletes) atomically.
     * This method provides maximum flexibility for complex bulk operations and ensures
     * optimal performance when multiple different operations need to be executed.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: mixed insert/update/delete in one round-trip; result.getModifiedCount() is one of
     * // several per-type counts.
     * List<WriteModel<Document>> operations = Arrays.asList(
     *     new InsertOneModel<>(doc1),
     *     new UpdateOneModel<>(filter, update),
     *     new DeleteOneModel<>(deleteFilter));
     * userMapper.bulkWrite(operations).subscribe(result ->
     *     System.out.println("Modified: " + result.getModifiedCount()));   // emits one BulkWriteResult
     *
     * // Edge: cold publisher — building it writes nothing until subscribed.
     * Mono<BulkWriteResult> pending = userMapper.bulkWrite(operations);   // nothing written yet
     *
     * // Negative: a null or empty request list -> IllegalArgumentException.
     * userMapper.bulkWrite(Collections.emptyList());   // throws IllegalArgumentException
     *
     * // Negative: a failing operation (e.g. duplicate _id on insert) -> onError(MongoBulkWriteException).
     * userMapper.bulkWrite(operationsWithConflict)
     *     .subscribe(r -> {}, err -> System.err.println("Bulk write failed: " + err));   // onError(MongoBulkWriteException)
     * }</pre>
     *
     * @param requests list of write operations to execute (must not be null or empty)
     * @return a Mono emitting BulkWriteResult with detailed operation statistics
     * @throws IllegalArgumentException if requests is null or empty
     */
    public Mono<BulkWriteResult> bulkWrite(final List<? extends WriteModel<? extends Document>> requests) {
        return collectionExecutor.bulkWrite(requests);
    }

    /**
     * Executes bulk write operations with custom options.
     *
     * <p>Performs mixed write operations with fine-grained control over execution behavior,
     * including ordering, validation bypass, and write concern. Useful for complex
     * transactional-like operations requiring specific consistency guarantees.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * BulkWriteOptions options = new BulkWriteOptions()
     *     .ordered(true)
     *     .bypassDocumentValidation(false);
     * userMapper.bulkWrite(operations, options).subscribe(result ->
     *     System.out.println("Bulk write completed: " + result));
     * }</pre>
     *
     * @param requests list of write operations to execute
     * @param options configuration for the bulk write behavior (may be null to use defaults)
     * @return a Mono emitting BulkWriteResult with operation statistics
     * @throws IllegalArgumentException if requests is null or empty
     */
    public Mono<BulkWriteResult> bulkWrite(final List<? extends WriteModel<? extends Document>> requests, final BulkWriteOptions options) {
        return collectionExecutor.bulkWrite(requests, options);
    }

    /**
     * Atomically finds and updates a single document, returning it as the mapper's entity type.
     *
     * <p>Locates the first document matching the filter and applies the update atomically. By
     * default the driver returns the document <i>before</i> the update; use the
     * {@link FindOneAndUpdateOptions}-overload to request the post-update document. This
     * operation prevents race conditions in concurrent environments.</p>
     *
     * <p><b>Empty vs. present semantics:</b> on subscription, the returned {@code Mono} emits the
     * matched document decoded as {@code T} and then completes, or completes <i>empty</i> when no
     * document matches the filter (no update is performed in that case).</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: match -> emits the PRE-update document (default returnDocument is BEFORE).
     * User updatedUser = new User().setStatus("active");
     * userMapper.findOneAndUpdate(Filters.eq("_id", userId), updatedUser)
     *     .subscribe(user -> System.out.println("Old status was: " + user.getStatus()));   // emits doc before update
     *
     * // Edge: no match -> Mono completes EMPTY and NO update is performed.
     * userMapper.findOneAndUpdate(Filters.eq("_id", "missing"), updatedUser)
     *     .hasElement().subscribe(present -> System.out.println("updated? " + present));   // emits false
     *
     * // Edge: cold publisher — building it performs no update until subscribed.
     * Mono<User> pending = userMapper.findOneAndUpdate(Filters.eq("_id", userId), updatedUser);   // nothing written yet
     *
     * // Negative: a null filter throws IllegalArgumentException synchronously (at the call site,
     * // before any Mono is returned), because filter is validated eagerly.
     * userMapper.findOneAndUpdate((Bson) null, updatedUser);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param filter the query filter to identify the document to update
     * @param update the entity containing update values
     * @return a {@code Mono} that emits the matched document (before update) decoded as {@code T},
     *         or completes empty when no document matches
     * @throws IllegalArgumentException if filter or update is null
     * @see MongoCollectionExecutor#findOneAndUpdate(Bson, Object, Class)
     */
    public Mono<T> findOneAndUpdate(final Bson filter, final T update) {
        return collectionExecutor.findOneAndUpdate(filter, update, rowType);
    }

    /**
     * Atomically finds and updates a document with custom options.
     *
     * <p>Performs an atomic find-and-update operation with additional control over behavior,
     * such as returning the document before or after modification, upsert capability,
     * and projection of returned fields.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndUpdateOptions options = new FindOneAndUpdateOptions()
     *     .returnDocument(ReturnDocument.AFTER)
     *     .upsert(true);
     * userMapper.findOneAndUpdate(filter, update, options)
     *     .subscribe(user -> System.out.println("User after update: " + user));
     * }</pre>
     *
     * @param filter the query filter to identify the document
     * @param update the entity containing update values
     * @param options configuration for the find-and-update operation
     * @return a {@code Mono} that emits the matched document decoded as {@code T} — the pre- or
     *         post-write version per {@code options} — or completes empty when no document matches
     * @throws IllegalArgumentException if filter or update is null
     */
    public Mono<T> findOneAndUpdate(final Bson filter, final T update, final FindOneAndUpdateOptions options) {
        return collectionExecutor.findOneAndUpdate(filter, update, options, rowType);
    }

    /**
     * Atomically finds and updates using multiple update objects.
     *
     * <p>Locates a document matching the filter and applies updates from a collection
     * of objects. This is useful when updates come from multiple sources or need to
     * be aggregated from various entities.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<User> updates = Arrays.asList(statusUpdate, profileUpdate);
     * userMapper.findOneAndUpdate(Filters.eq("_id", userId), updates)
     *     .subscribe(user -> System.out.println("Updated from multiple sources"));
     * }</pre>
     *
     * @param filter the query filter to identify the document
     * @param objList collection of objects containing update values
     * @return a Mono emitting the found document
     * @throws IllegalArgumentException if filter is null, or objList is null or empty
     */
    public Mono<T> findOneAndUpdate(final Bson filter, final Collection<? extends T> objList) {
        return collectionExecutor.findOneAndUpdate(filter, objList, rowType);
    }

    /**
     * Atomically finds and updates using multiple objects with options.
     *
     * <p>Performs atomic update using values from multiple source objects with custom
     * options for controlling the operation behavior, including upsert and return
     * document preferences.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndUpdateOptions options = new FindOneAndUpdateOptions().upsert(true);
     * userMapper.findOneAndUpdate(filter, updatesList, options)
     *     .subscribe(user -> System.out.println("Merged updates applied"));
     * }</pre>
     *
     * @param filter the query filter to identify the document
     * @param objList collection of objects containing update values
     * @param options configuration for the operation
     * @return a {@code Mono} that emits the matched document decoded as {@code T} — the pre- or
     *         post-write version per {@code options} — or completes empty when no document matches
     * @throws IllegalArgumentException if filter is null, or objList is null or empty
     */
    public Mono<T> findOneAndUpdate(final Bson filter, final Collection<? extends T> objList, final FindOneAndUpdateOptions options) {
        return collectionExecutor.findOneAndUpdate(filter, objList, options, rowType);
    }

    /**
     * Atomically finds and replaces a single document, returning it as the mapper's entity type.
     *
     * <p>Locates the first document matching the filter and replaces it entirely with the
     * provided replacement document. Unlike update operations, this completely overwrites the
     * existing document while preserving the {@code _id} field. By default the driver returns
     * the document <i>before</i> replacement; use the {@link FindOneAndReplaceOptions}-overload
     * to request the post-replacement document.</p>
     *
     * <p><b>Empty vs. present semantics:</b> on subscription, the returned {@code Mono} emits the
     * matched document (before replacement) decoded as {@code T} and then completes, or completes
     * <i>empty</i> when no document matches the filter and the operation is not configured to
     * upsert.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: match -> emits the PRE-replacement document (default returnDocument is BEFORE).
     * User newUser = new User("John", "john@example.com", "active");
     * userMapper.findOneAndReplace(Filters.eq("_id", userId), newUser)
     *     .subscribe(oldUser -> System.out.println("Replaced: " + oldUser.getName()));   // emits doc before replace
     *
     * // Edge: no match -> Mono completes EMPTY and nothing is replaced (without upsert).
     * userMapper.findOneAndReplace(Filters.eq("_id", "missing"), newUser)
     *     .hasElement().subscribe(present -> System.out.println("replaced? " + present));   // emits false
     *
     * // Edge: cold publisher — building it performs no replace until subscribed.
     * Mono<User> pending = userMapper.findOneAndReplace(Filters.eq("_id", userId), newUser);   // nothing written yet
     *
     * // Negative: a null filter throws IllegalArgumentException synchronously (at the call site,
     * // before any Mono is returned), because filter is validated eagerly.
     * userMapper.findOneAndReplace((Bson) null, newUser);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param filter the query filter to identify the document to replace
     * @param replacement the complete replacement document
     * @return a {@code Mono} that emits the matched document (before replacement) decoded as
     *         {@code T}, or completes empty when no document matches the filter
     * @throws IllegalArgumentException if filter or replacement is null
     * @see MongoCollectionExecutor#findOneAndReplace(Bson, Object, Class)
     */
    public Mono<T> findOneAndReplace(final Bson filter, final T replacement) {
        return collectionExecutor.findOneAndReplace(filter, replacement, rowType);
    }

    /**
     * Atomically finds and replaces a document with custom options.
     *
     * <p>Performs an atomic find-and-replace with additional control over the operation,
     * including upsert capability, return document preference, and projection of fields
     * in the returned document.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndReplaceOptions options = new FindOneAndReplaceOptions()
     *     .returnDocument(ReturnDocument.AFTER)
     *     .upsert(true);
     * userMapper.findOneAndReplace(filter, replacement, options)
     *     .subscribe(user -> System.out.println("New document: " + user));
     * }</pre>
     *
     * @param filter the query filter to identify the document
     * @param replacement the complete replacement document
     * @param options configuration for the replace operation
     * @return a {@code Mono} that emits the matched document decoded as {@code T} — the pre- or
     *         post-write version per {@code options} — or completes empty when no document matches
     * @throws IllegalArgumentException if filter or replacement is null
     */
    public Mono<T> findOneAndReplace(final Bson filter, final T replacement, final FindOneAndReplaceOptions options) {
        return collectionExecutor.findOneAndReplace(filter, replacement, options, rowType);
    }

    /**
     * Atomically finds and deletes a single document, returning it as the mapper's entity type.
     *
     * <p>Locates the first document matching the filter and removes it from the collection
     * atomically. This ensures the document is retrieved before deletion in a single atomic
     * operation.</p>
     *
     * <p><b>Empty vs. present semantics:</b> on subscription, the returned {@code Mono} emits the
     * just-deleted document decoded as {@code T} and then completes, or completes <i>empty</i>
     * when no document matches the filter (nothing is deleted in that case).</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: match -> emits the just-deleted document, then completes.
     * userMapper.findOneAndDelete(Filters.eq("status", "deleted"))
     *     .subscribe(deletedUser -> System.out.println("Removed user: " + deletedUser.getName()));   // emits deleted doc
     *
     * // Edge: no match -> Mono completes EMPTY and nothing is deleted.
     * userMapper.findOneAndDelete(Filters.eq("status", "no-such-status"))
     *     .hasElement().subscribe(present -> System.out.println("deleted? " + present));   // emits false
     *
     * // Edge: cold publisher — building it deletes nothing until subscribed.
     * Mono<User> pending = userMapper.findOneAndDelete(Filters.eq("status", "deleted"));   // nothing deleted yet
     *
     * // Negative: a null filter is rejected with IllegalArgumentException, thrown synchronously
     * // at the call site (before any Mono is built).
     * userMapper.findOneAndDelete((Bson) null);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param filter the query filter to identify the document to delete
     * @return a {@code Mono} that emits the deleted document decoded as {@code T}, or completes
     *         empty when no document matches the filter
     * @throws IllegalArgumentException if filter is null
     * @see MongoCollectionExecutor#findOneAndDelete(Bson, Class)
     */
    public Mono<T> findOneAndDelete(final Bson filter) {
        return collectionExecutor.findOneAndDelete(filter, rowType);
    }

    /**
     * Atomically finds and deletes a document with custom options.
     *
     * <p>Performs an atomic find-and-delete operation with additional control over
     * the operation behavior, such as sort order for selecting which document to
     * delete when multiple matches exist, and projection of returned fields.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndDeleteOptions options = new FindOneAndDeleteOptions()
     *     .sort(Sorts.ascending("createdAt"));
     * userMapper.findOneAndDelete(filter, options)
     *     .subscribe(user -> System.out.println("Deleted oldest: " + user));
     * }</pre>
     *
     * @param filter the query filter to identify the document
     * @param options configuration for the delete operation
     * @return a Mono emitting the deleted document
     * @throws IllegalArgumentException if filter is null
     */
    public Mono<T> findOneAndDelete(final Bson filter, final FindOneAndDeleteOptions options) {
        return collectionExecutor.findOneAndDelete(filter, options, rowType);
    }

    /**
     * Returns distinct values of the specified field across the whole collection, each surfaced on
     * an entity of the mapper's row type {@code T}.
     *
     * <p>Each distinct value is surfaced under {@code fieldName} on an entity of the mapped type, and
     * is only readable if the entity declares a matching property. Internally this runs a
     * {@code $group}/{@code $project} aggregation pipeline (the same approach as {@link #groupBy(String)}),
     * so scalar field values decode cleanly into {@code T} — unlike the driver's native
     * {@code distinct(field, T.class)}, which throws {@code BsonInvalidOperationException} when
     * {@code T} is a POJO and the field is scalar. To obtain raw scalar values instead, use
     * {@link MongoCollectionExecutor#distinct(String, Class)} with an explicit value class.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Distinct "country" values across the whole collection, each surfaced on a T:
     * userMapper.distinct("country")
     *     .collectList()
     *     .subscribe(countries -> System.out.println("Countries: " + countries));
     *
     * // For raw scalar values, use the executor directly with an explicit value class:
     * userMapper.collectionExecutor().distinct("country", String.class)
     *     .collectList()
     *     .subscribe(countries -> System.out.println("Countries: " + countries));
     *
     * // Edge: an empty collection (no documents) -> Flux completes with ZERO emissions.
     * userMapper.distinct("country").count().subscribe(n -> System.out.println("distinct: " + n));   // emits 0 if empty
     *
     * // Edge: cold publisher — building it issues no query until subscribed.
     * Flux<T> notRunYet = userMapper.distinct("country");   // nothing executed yet
     *
     * // Negative: a null fieldName is rejected with IllegalArgumentException.
     * userMapper.distinct((String) null);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param fieldName the name of the field to get distinct values for
     * @return a cold {@code Flux} that, on subscription, emits each distinct value of the field
     *         surfaced on an entity of type {@code T}, then completes; completes empty when no
     *         distinct values are found (no documents, or the field is absent from all of them)
     * @throws IllegalArgumentException if fieldName is null or empty (thrown synchronously at the call site)
     * @see #distinct(String, Bson)
     * @see #groupBy(String)
     * @see MongoCollectionExecutor#distinct(String, Class)
     */
    public Flux<T> distinct(final String fieldName) {
        N.checkArgNotEmpty(fieldName, "fieldName");

        return collectionExecutor.aggregate(distinctPipeline(fieldName, null), rowType);
    }

    // Routes distinct through a $group/$project pipeline (like groupBy) so each distinct scalar value
    // comes back as a {fieldName: value} document decodable into the mapped entity type, as documented.
    // The driver's native distinct(fieldName, entityClass) decodes each raw VALUE with the entity codec
    // and throws BsonInvalidOperationException for any scalar field. Mirrors the blocking
    // MongoCollectionMapper.distinct(...) so both layers behave identically.
    private static List<Bson> distinctPipeline(final String fieldName, final Bson filter) {
        final List<Bson> pipeline = new ArrayList<>(3);

        if (filter != null) {
            pipeline.add(Aggregates.match(filter));
        }

        pipeline.add(new Document("$group", new Document("_id", "$" + fieldName)));
        pipeline.add(new Document("$project", new Document("_id", 0).append(fieldName, "$_id")));

        return pipeline;
    }

    /**
     * Returns distinct values of the specified field among documents matching the filter, each
     * surfaced on an entity of the mapper's row type {@code T}.
     *
     * <p>Like {@link #distinct(String)} but only considers documents matching {@code filter}. Each
     * distinct value is surfaced under {@code fieldName} on an entity of the mapped type via a
     * {@code $group}/{@code $project} pipeline, so scalar values decode cleanly into {@code T}. To
     * obtain raw scalar values instead, use {@link MongoCollectionExecutor#distinct(String, Bson, Class)}
     * with an explicit value class.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("status", "active");
     * // Distinct "department" values among active users, each surfaced on a T:
     * userMapper.distinct("department", filter)
     *     .collectList()
     *     .subscribe(depts -> System.out.println("Active departments: " + depts));
     *
     * // For raw scalar values, use the executor directly with an explicit value class:
     * userMapper.collectionExecutor().distinct("department", filter, String.class)
     *     .collectList()
     *     .subscribe(depts -> System.out.println("Active departments: " + depts));
     * }</pre>
     *
     * @param fieldName the name of the field to get distinct values for
     * @param filter the query filter to apply before extracting distinct values (must not be null)
     * @return a cold {@code Flux} that, on subscription, emits each distinct value of the field
     *         (among matching documents) surfaced on an entity of type {@code T}, then completes;
     *         completes empty when no distinct values are found
     * @throws IllegalArgumentException if fieldName is null or empty, or if filter is null (thrown synchronously at the call site)
     * @see #distinct(String)
     * @see MongoCollectionExecutor#distinct(String, Bson, Class)
     */
    public Flux<T> distinct(final String fieldName, final Bson filter) {
        N.checkArgNotEmpty(fieldName, "fieldName");
        N.checkArgNotNull(filter, "filter");

        return collectionExecutor.aggregate(distinctPipeline(fieldName, filter), rowType);
    }

    /**
     * Executes an aggregation pipeline on the collection, decoding each output document as the
     * mapper's row type {@code T}.
     *
     * <p>Processes documents through the supplied pipeline stages to perform filtering, grouping,
     * sorting, projection, and other transformations.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: match then group; each output document is decoded as T.
     * List<Bson> pipeline = Arrays.asList(
     *     Aggregates.match(Filters.gte("age", 18)),
     *     Aggregates.group("$country", Accumulators.sum("count", 1)));
     * userMapper.aggregate(pipeline)
     *     .subscribe(result -> System.out.println("Aggregation result: " + result));   // one emission per group
     *
     * // Edge: an empty pipeline returns every document (no transformation).
     * Flux<T> all = userMapper.aggregate(Collections.emptyList());   // emits each document as T
     *
     * // Edge: a pipeline that matches nothing -> Flux completes with ZERO emissions.
     * userMapper.aggregate(Arrays.asList(Aggregates.match(Filters.gte("age", 1000))))
     *     .count().subscribe(n -> System.out.println("groups: " + n));   // emits 0
     *
     * // Negative: the driver validates the pipeline eagerly, so a null pipeline throws
     * // IllegalArgumentException synchronously at the call site (before any Flux is returned).
     * userMapper.aggregate((List<Bson>) null);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param pipeline list of aggregation stages to execute in order; the pipeline itself may be
     *        empty (which returns every document)
     * @return a cold {@code Flux} that, on subscription, emits each output document decoded as
     *         {@code T} (one per emission, honouring downstream demand), then completes; completes
     *         empty if the pipeline produces no documents
     * @throws IllegalArgumentException if pipeline is null
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Flux})
     */
    public Flux<T> aggregate(final List<? extends Bson> pipeline) {
        return collectionExecutor.aggregate(pipeline, rowType);
    }

    /**
     * Groups documents by a single field (Beta feature).
     *
     * <p>Issues an aggregation pipeline with a single {@code $group} stage keyed on
     * {@code fieldName} and decodes each resulting group document to the mapper's row type
     * {@code T}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: one emission per distinct department value.
     * userMapper.groupBy("department")
     *     .collectList()
     *     .subscribe(groups -> System.out.println("Grouped by department: " + groups));   // one group per department
     *
     * // Edge: an empty collection -> Flux completes with ZERO emissions.
     * userMapper.groupBy("department").count().subscribe(n -> System.out.println("groups: " + n));   // emits 0 if empty
     *
     * // Edge: cold publisher — building it issues no aggregation until subscribed.
     * Flux<T> notRunYet = userMapper.groupBy("department");   // nothing executed yet
     *
     * // Negative: a database/pipeline failure is delivered through onError, never thrown to the subscriber.
     * userMapper.groupBy("department")
     *     .onErrorResume(err -> { System.err.println("group failed: " + err); return Flux.empty(); })
     *     .subscribe(g -> handleGroup(g));   // errors routed to onErrorResume
     * }</pre>
     *
     * @param fieldName the field name to group documents by
     * @return a cold {@code Flux} that, on subscription, emits each group result decoded as
     *         {@code T} (one per emission, honouring downstream demand), then completes
     * @throws IllegalArgumentException if fieldName is null or empty
     */
    @Beta
    public Flux<T> groupBy(final String fieldName) {
        return collectionExecutor.groupBy(fieldName, rowType);
    }

    /**
     * Groups documents by multiple fields (Beta feature).
     *
     * <p>Issues an aggregation pipeline whose {@code $group} key is the composition of the supplied
     * field names; each unique combination forms a distinct output document, decoded to the
     * mapper's row type {@code T}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: one emission per unique (country, department) combination.
     * List<String> fields = Arrays.asList("country", "department");
     * userMapper.groupBy(fields)
     *     .collectList()
     *     .subscribe(groups -> System.out.println("Multi-field groups: " + groups));   // one group per combo
     *
     * // Edge: an empty collection -> Flux completes with ZERO emissions.
     * userMapper.groupBy(fields).count().subscribe(n -> System.out.println("groups: " + n));   // emits 0 if empty
     *
     * // Edge: cold publisher — building it issues no aggregation until subscribed.
     * Flux<T> notRunYet = userMapper.groupBy(fields);   // nothing executed yet
     *
     * // Negative: a null or empty fieldNames collection -> IllegalArgumentException.
     * userMapper.groupBy(Collections.emptyList());   // throws IllegalArgumentException
     * }</pre>
     *
     * @param fieldNames collection of field names to compose the group key
     * @return a cold {@code Flux} that, on subscription, emits each group result decoded as
     *         {@code T} (one per emission, honouring downstream demand), then completes
     * @throws IllegalArgumentException if fieldNames is null or empty
     */
    @Beta
    public Flux<T> groupBy(final Collection<String> fieldNames) {
        return collectionExecutor.groupBy(fieldNames, rowType);
    }

    /**
     * Groups documents by a field and counts frequency (Beta feature).
     *
     * <p>Equivalent to {@link #groupBy(String)} with a {@code $sum: 1} accumulator: each emitted
     * document represents one group and carries the count of documents that fell into it. Useful
     * for frequency distributions and summary statistics.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: one emission per status, each carrying its document count.
     * userMapper.groupByAndCount("status")
     *     .collectList()
     *     .subscribe(counts -> System.out.println("Status counts: " + counts));   // one entry per status
     *
     * // Edge: an empty collection -> Flux completes with ZERO emissions.
     * userMapper.groupByAndCount("status").count().subscribe(n -> System.out.println("groups: " + n));   // emits 0
     *
     * // Edge: cold publisher — building it issues no aggregation until subscribed.
     * Flux<T> notRunYet = userMapper.groupByAndCount("status");   // nothing executed yet
     *
     * // Edge: a database/pipeline failure is delivered through onError, never thrown synchronously.
     * userMapper.groupByAndCount("status")
     *     .onErrorResume(err -> Flux.empty())
     *     .subscribe(g -> handleCount(g));   // errors routed to onErrorResume
     * }</pre>
     *
     * @param fieldName the field name to group and count by
     * @return a cold {@code Flux} that, on subscription, emits each group-with-count result decoded
     *         as {@code T} (one per emission, honouring downstream demand), then completes
     * @throws IllegalArgumentException if fieldName is null or empty
     */
    @Beta
    public Flux<T> groupByAndCount(final String fieldName) {
        return collectionExecutor.groupByAndCount(fieldName, rowType);
    }

    /**
     * Groups documents by multiple fields with counts (Beta feature).
     *
     * <p>Equivalent to {@link #groupBy(Collection)} with a {@code $sum: 1} accumulator: each
     * emitted document represents one unique combination of {@code fieldNames} and carries the
     * count of documents that fell into it.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: one emission per unique (country, status) combination, each with its count.
     * List<String> fields = Arrays.asList("country", "status");
     * userMapper.groupByAndCount(fields)
     *     .collectList()
     *     .subscribe(counts -> System.out.println("Distribution: " + counts));   // one entry per combo
     *
     * // Edge: an empty collection -> Flux completes with ZERO emissions.
     * userMapper.groupByAndCount(fields).count().subscribe(n -> System.out.println("groups: " + n));   // emits 0
     *
     * // Edge: cold publisher — building it issues no aggregation until subscribed.
     * Flux<T> notRunYet = userMapper.groupByAndCount(fields);   // nothing executed yet
     *
     * // Negative: a null or empty fieldNames collection -> IllegalArgumentException.
     * userMapper.groupByAndCount(Collections.emptyList());   // throws IllegalArgumentException
     * }</pre>
     *
     * @param fieldNames collection of field names to compose the group key
     * @return a cold {@code Flux} that, on subscription, emits each group-with-count result decoded
     *         as {@code T} (one per emission, honouring downstream demand), then completes
     * @throws IllegalArgumentException if fieldNames is null or empty
     */
    @Beta
    public Flux<T> groupByAndCount(final Collection<String> fieldNames) {
        return collectionExecutor.groupByAndCount(fieldNames, rowType);
    }

    /**
     * Executes a map-reduce operation on the collection, decoding each emitted result document as
     * the mapper's row type {@code T}.
     *
     * <p>Performs a map-reduce operation using the supplied JavaScript map and reduce functions
     * and emits each output document, decoded to the mapper's entity type, on the returned
     * {@link Flux}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: classic count-by-category map-reduce; each output document decoded as T.
     * String mapFunction = "function() { emit(this.category, 1); }";
     * String reduceFunction = "function(key, values) { return Array.sum(values); }";
     * userMapper.mapReduce(mapFunction, reduceFunction)
     *     .subscribe(result -> System.out.println("Map-reduce result: " + result));   // one emission per key
     *
     * // Edge: an empty collection -> Flux completes with ZERO emissions.
     * userMapper.mapReduce(mapFunction, reduceFunction)
     *     .count().subscribe(n -> System.out.println("keys: " + n));   // emits 0 if empty
     *
     * // Edge: cold publisher — building it issues no map-reduce until subscribed.
     * Flux<T> notRunYet = userMapper.mapReduce(mapFunction, reduceFunction);   // nothing executed yet
     *
     * // Negative: a null map function is rejected with IllegalArgumentException.
     * userMapper.mapReduce((String) null, reduceFunction);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param mapFunction the JavaScript map function; must not be null
     * @param reduceFunction the JavaScript reduce function; must not be null
     * @return a {@code Flux} that, on subscription, emits each map-reduce output document decoded
     *         as {@code T}, then completes; completes empty when the operation produces no output
     * @throws IllegalArgumentException if mapFunction or reduceFunction is null
     * @throws com.mongodb.MongoException if the database operation fails (signalled via {@code Flux})
     * @deprecated Map-reduce is deprecated in MongoDB 5.0+. Use {@link #aggregate(List)} with an
     *             aggregation pipeline instead.
     */
    @Deprecated
    public Flux<T> mapReduce(final String mapFunction, final String reduceFunction) {
        return collectionExecutor.mapReduce(mapFunction, reduceFunction, rowType);
    }
}
