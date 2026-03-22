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

import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.landawn.abacus.annotation.Beta;
import com.landawn.abacus.util.Dataset;
import com.mongodb.bulk.BulkWriteResult;
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
 * Reactive type-safe MongoDB collection mapper providing object-document mapping with reactive streams support.
 *
 * <p>This class combines the functionality of reactive {@code MongoCollectionExecutor} with automatic type conversion
 * to provide a strongly-typed, reactive interface for MongoDB operations. It eliminates manual document-to-object
 * conversion while providing full reactive streams capabilities including backpressure, error handling,
 * and seamless integration with reactive frameworks.</p>
 *
 * <h2>Key Features</h2>
 * <h3>Core Capabilities:</h3>
 * <ul>
 *   <li><strong>Reactive Type Safety:</strong> All operations return Publishers with properly typed objects</li>
 *   <li><strong>Automatic Conversion:</strong> Seamless conversion between Java entities and BSON with reactive streams</li>
 *   <li><strong>ID Mapping:</strong> Automatic mapping between entity ID fields and MongoDB's "_id" field</li>
 *   <li><strong>Backpressure Support:</strong> Built-in backpressure handling for streaming operations</li>
 *   <li><strong>Error Propagation:</strong> Reactive error handling through Publisher error signals</li>
 *   <li><strong>Framework Integration:</strong> Direct compatibility with Project Reactor, RxJava, and reactive frameworks</li>
 * </ul>
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
 * MongoCollectionMapper<User> userMapper = reactiveMongoDB.collMapper("users", User.class);
 * 
 * // Reactive type-safe operations with Project Reactor:
 * userMapper.insertOne(new User("John", "john@example.com"));
 * 
 * Flux<User> activeUsers = userMapper.list(Filters.eq("status", "active"))
 *     .filter(user -> user.getCreatedAt().after(cutoffDate))
 *     .take(100)  // Backpressure control
 *     .doOnNext(user -> System.out.println("Processing user: " + user.getName()));
 * 
 * // Reactive aggregation with type conversion:
 * Mono<Long> userCount = Mono.from(userMapper.count("{department: 'Engineering'}"))
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

    private final MongoCollectionExecutor collExecutor;

    private final Class<T> rowType;

    /**
     * Package-private constructor for creating a reactive MongoDB collection mapper.
     *
     * @param collExecutor the reactive collection executor to use for operations
     * @param resultClass the Class representing the entity type for mapping operations
     */
    MongoCollectionMapper(final MongoCollectionExecutor collExecutor, final Class<T> resultClass) {
        this.collExecutor = collExecutor;
        rowType = resultClass;
    }

    /**
     * Returns the underlying reactive MongoCollectionExecutor for advanced operations.
     *
     * <p>This method provides access to the lower-level reactive executor, allowing for advanced
     * operations not directly exposed by this mapper while maintaining reactive capabilities.</p>
     *
     * @return the reactive MongoCollectionExecutor instance
     * @see MongoCollectionExecutor
     */
    public MongoCollectionExecutor collExecutor() {
        return collExecutor;
    }

    /**
     * Checks if a document exists with the specified ObjectId string reactively.
     *
     * <p>This method provides a reactive way to verify document existence without retrieving the full document,
     * making it efficient for existence checks in a type-safe manner.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> userMapper = reactiveMongoDB.collMapper("users", User.class);
     *
     * Mono<Boolean> existsMono = userMapper.exists("507f1f77bcf86cd799439011");
     * existsMono.subscribe(
     *     exists -> System.out.println(exists ? "User found" : "User not found")
     * );
     * }</pre>
     *
     * @param objectId the string representation of the ObjectId to check for existence
     * @return a Mono that emits {@code true} if a document with the specified ObjectId exists, {@code false} otherwise
     * @throws IllegalArgumentException if objectId is null or empty
     * @throws org.bson.BsonInvalidOperationException if objectId string is not a valid ObjectId format
     * @see ObjectId
     */
    public Mono<Boolean> exists(final String objectId) {
        return collExecutor.exists(objectId);
    }

    /**
     * Checks if a document exists with the specified ObjectId reactively.
     *
     * <p>This method provides a reactive way to verify document existence using a typed ObjectId,
     * making it efficient for existence checks in a type-safe context.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId userId = new ObjectId("507f1f77bcf86cd799439011");
     * Mono<Boolean> existsMono = userMapper.exists(userId);
     * 
     * existsMono.subscribe(
     *     exists -> System.out.println("Document exists: " + exists)
     * );
     * }</pre>
     *
     * @param objectId the ObjectId to check for existence
     * @return a Mono that emits {@code true} if a document with the specified ObjectId exists, {@code false} otherwise
     * @throws IllegalArgumentException if objectId is null
     * @see ObjectId
     */
    public Mono<Boolean> exists(final ObjectId objectId) {
        return collExecutor.exists(objectId);
    }

    /**
     * Checks if any documents exist matching the specified filter reactively.
     *
     * <p>This method provides a reactive way to verify if documents matching the given filter exist
     * without retrieving the actual documents, optimized for the mapped entity type.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson activeUserFilter = Filters.eq("status", "active");
     * Mono<Boolean> existsMono = userMapper.exists(activeUserFilter);
     * 
     * existsMono.subscribe(
     *     exists -> System.out.println("Active users exist: " + exists)
     * );
     * }</pre>
     *
     * @param filter the query filter to match documents against
     * @return a Mono that emits {@code true} if any documents match the filter, {@code false} otherwise
     * @throws IllegalArgumentException if filter is null
     * @see Bson
     * @see com.mongodb.client.model.Filters
     */
    public Mono<Boolean> exists(final Bson filter) {
        return collExecutor.exists(filter);
    }

    /**
     * Counts all documents in the mapped collection reactively.
     *
     * <p>This method provides a reactive way to count all documents in the collection
     * mapped to the specified entity type. The operation is performed asynchronously.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Mono<Long> countMono = userMapper.count();
     * 
     * countMono.subscribe(
     *     count -> System.out.println("Total users: " + count)
     * );
     * }</pre>
     *
     * @return a Mono that emits the total count of documents in the collection
     */
    public Mono<Long> count() {
        return collExecutor.count();
    }

    /**
     * Counts documents matching the specified filter reactively.
     *
     * <p>This method provides a reactive way to count documents that match the given filter criteria
     * in the context of the mapped entity type. The operation is performed asynchronously.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson activeFilter = Filters.eq("status", "active");
     * Mono<Long> countMono = userMapper.count(activeFilter);
     *
     * countMono.subscribe(
     *     count -> System.out.println("Active users: " + count)
     * );
     * }</pre>
     *
     * @param filter the query filter to match documents
     * @return a Mono that emits the count of documents matching the filter
     * @throws IllegalArgumentException if filter is null
     * @see Bson
     * @see com.mongodb.client.model.Filters
     */
    public Mono<Long> count(final Bson filter) {
        return collExecutor.count(filter);
    }

    /**
     * Counts documents matching the specified filter with custom options reactively.
     *
     * <p>This method provides a reactive way to count documents with additional counting options
     * such as limit, skip, maxTime, and collation for the mapped entity type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CountOptions options = new CountOptions().limit(1000).maxTime(30, TimeUnit.SECONDS);
     * Mono<Long> countMono = userMapper.count(Filters.gte("age", 18), options);
     *
     * countMono.subscribe(
     *     count -> System.out.println("Adult users (max 1000): " + count)
     * );
     * }</pre>
     *
     * @param filter the query filter to match documents
     * @param options the count options to apply (can be null)
     * @return a Mono that emits the count of documents matching the filter with applied options
     * @throws IllegalArgumentException if filter is null
     * @see Bson
     * @see CountOptions
     * @see com.mongodb.client.model.Filters
     */
    public Mono<Long> count(final Bson filter, final CountOptions options) {
        return collExecutor.count(filter, options);
    }

    /**
     * Retrieves a single entity by its ObjectId string reactively.
     *
     * <p>This method provides a reactive way to retrieve a document using its string ObjectId representation
     * and automatically convert it to the mapped entity type using the configured codec registry.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Mono<User> userMono = userMapper.get("507f1f77bcf86cd799439011");
     *
     * userMono.subscribe(
     *     user -> System.out.println("Found user: " + user.getName()),
     *     error -> System.err.println("Error: " + error),
     *     () -> System.out.println("User not found")
     * );
     * }</pre>
     *
     * @param objectId the string representation of the ObjectId to search for
     * @return a Mono that emits the found entity, or empty if no document matches the ObjectId
     * @throws IllegalArgumentException if objectId is null or empty
     * @throws org.bson.BsonInvalidOperationException if objectId string is not a valid ObjectId format
     * @see ObjectId
     */
    public Mono<T> get(final String objectId) {
        return collExecutor.get(objectId, rowType);
    }

    /**
     * Retrieves a single entity by its ObjectId reactively.
     *
     * <p>This method provides a reactive way to retrieve a document using a typed ObjectId
     * and automatically convert it to the mapped entity type using the configured codec registry.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId userId = new ObjectId("507f1f77bcf86cd799439011");
     * Mono<User> userMono = userMapper.get(userId);
     * 
     * userMono.subscribe(
     *     user -> processUser(user),
     *     error -> handleError(error)
     * );
     * }</pre>
     *
     * @param objectId the ObjectId to search for
     * @return a Mono that emits the found entity, or empty if no document matches the ObjectId
     * @throws IllegalArgumentException if objectId is null
     * @see ObjectId
     */
    public Mono<T> get(final ObjectId objectId) {
        return collExecutor.get(objectId, rowType);
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
     * Collection<String> fields = Arrays.asList("name", "email", "status");
     * Mono<User> userMono = userMapper.get("507f1f77bcf86cd799439011", fields);
     *
     * userMono.subscribe(
     *     user -> System.out.println("User basic info: " + user.getName())
     * );
     * }</pre>
     *
     * @param objectId the string representation of the ObjectId to search for
     * @param selectPropNames the collection of field names to include in the projection
     * @return a Mono that emits the projected entity, or empty if no document matches the ObjectId
     * @throws IllegalArgumentException if objectId is null/empty or selectPropNames is null
     * @throws org.bson.BsonInvalidOperationException if objectId string is not a valid ObjectId format
     * @see ObjectId
     * @see com.mongodb.client.model.Projections
     */
    public Mono<T> get(final String objectId, final Collection<String> selectPropNames) {
        return collExecutor.get(objectId, selectPropNames, rowType);
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
     * ObjectId userId = new ObjectId("507f1f77bcf86cd799439011");
     * Collection<String> fields = Arrays.asList("name", "email");
     * Mono<User> userMono = userMapper.get(userId, fields);
     * 
     * userMono.subscribe(
     *     user -> processUserBasicInfo(user)
     * );
     * }</pre>
     *
     * @param objectId the ObjectId to search for
     * @param selectPropNames the collection of field names to include in the projection
     * @return a Mono that emits the projected entity, or empty if no document matches the ObjectId
     * @throws IllegalArgumentException if objectId or selectPropNames is null
     * @see ObjectId
     * @see com.mongodb.client.model.Projections
     */
    public Mono<T> get(final ObjectId objectId, final Collection<String> selectPropNames) {
        return collExecutor.get(objectId, selectPropNames, rowType);
    }

    /**
     * Finds the first entity matching the specified filter reactively.
     *
     * <p>This method provides a reactive way to retrieve the first document that matches the given
     * filter criteria and automatically convert it to the mapped entity type. The operation completes
     * when the first matching document is found or when it's determined that no matching documents exist.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("status", "active");
     * Mono<User> userMono = userMapper.findFirst(filter);
     *
     * userMono.subscribe(
     *     user -> System.out.println("First active user: " + user.getName()),
     *     error -> System.err.println("Error: " + error)
     * );
     * }</pre>
     *
     * @param filter the query filter to match documents
     * @return a Mono that emits the first matching entity, or empty if no documents match the filter
     * @throws IllegalArgumentException if filter is null
     * @see Bson
     * @see com.mongodb.client.model.Filters
     */
    public Mono<T> findFirst(final Bson filter) {
        return collExecutor.findFirst(filter, rowType);
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
     * Collection<String> fields = Arrays.asList("name", "email", "createdAt");
     * Bson filter = Filters.eq("department", "Engineering");
     * Mono<User> userMono = userMapper.findFirst(fields, filter);
     *
     * userMono.subscribe(
     *     user -> System.out.println("Engineer: " + user.getName())
     * );
     * }</pre>
     *
     * @param selectPropNames the collection of field names to include in the projection
     * @param filter the query filter to match documents
     * @return a Mono that emits the first matching projected entity, or empty if no documents match
     * @throws IllegalArgumentException if selectPropNames or filter is null
     * @see Bson
     * @see com.mongodb.client.model.Filters
     * @see com.mongodb.client.model.Projections
     */
    public Mono<T> findFirst(final Collection<String> selectPropNames, final Bson filter) {
        return collExecutor.findFirst(selectPropNames, filter, rowType);
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
     * Collection<String> fields = Arrays.asList("name", "score", "updatedAt");
     * Bson filter = Filters.gte("score", 90);
     * Bson sort = Sorts.descending("score");
     * Mono<User> userMono = userMapper.findFirst(fields, filter, sort);
     *
     * userMono.subscribe(
     *     user -> System.out.println("Top scorer: " + user.getName())
     * );
     * }</pre>
     *
     * @param selectPropNames the collection of field names to include in the projection
     * @param filter the query filter to match documents
     * @param sort the sort specification for ordering results
     * @return a Mono that emits the first matching projected entity with applied sorting, or empty if no documents match
     * @throws IllegalArgumentException if selectPropNames, filter, or sort is null
     * @see Bson
     * @see com.mongodb.client.model.Filters
     * @see com.mongodb.client.model.Projections
     * @see com.mongodb.client.model.Sorts
     */
    public Mono<T> findFirst(final Collection<String> selectPropNames, final Bson filter, final Bson sort) {
        return collExecutor.findFirst(selectPropNames, filter, sort, rowType);
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
     * Mono<User> userMono = userMapper.findFirst(projection, filter, sort);
     *
     * userMono.subscribe(
     *     user -> System.out.println("Active user: " + user.getName())
     * );
     * }</pre>
     *
     * @param projection the BSON projection specification for field selection
     * @param filter the query filter to match documents
     * @param sort the sort specification for ordering results
     * @return a Mono that emits the first matching projected entity with applied sorting, or empty if no documents match
     * @throws IllegalArgumentException if projection, filter, or sort is null
     * @see Bson
     * @see com.mongodb.client.model.Filters
     * @see com.mongodb.client.model.Projections
     * @see com.mongodb.client.model.Sorts
     */
    public Mono<T> findFirst(final Bson projection, final Bson filter, final Bson sort) {
        return collExecutor.findFirst(projection, filter, sort, rowType);
    }

    /**
     * Lists all entities matching the specified filter reactively.
     *
     * <p>This method provides a reactive way to retrieve all documents that match the given
     * filter criteria and automatically convert them to the mapped entity type. The results
     * are streamed asynchronously with backpressure support.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("status", "active");
     * Flux<User> userFlux = userMapper.list(filter);
     *
     * userFlux
     *     .take(100)  // Limit to 100 users for backpressure control
     *     .subscribe(
     *         user -> System.out.println("Active user: " + user.getName()),
     *         error -> System.err.println("Error: " + error),
     *         () -> System.out.println("All active users processed")
     *     );
     * }</pre>
     *
     * @param filter the query filter to match documents
     * @return a Flux that emits all matching entities
     * @throws IllegalArgumentException if filter is null
     * @see Bson
     * @see com.mongodb.client.model.Filters
     */
    public Flux<T> list(final Bson filter) {
        return collExecutor.list(filter, rowType);
    }

    /**
     * Lists entities matching the specified filter with pagination support.
     *
     * <p>Retrieves a subset of documents that match the filter criteria, with support for offset-based
     * pagination. The results are automatically converted to the mapped entity type and streamed reactively.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.gt("age", 18);
     * Flux<User> adults = userMapper.list(filter, 20, 10);   // Skip 20, take 10
     * adults.subscribe(user -> processUser(user));
     * }</pre>
     *
     * @param filter the query filter to match documents against
     * @param offset the number of documents to skip (must be >= 0)
     * @param count the maximum number of documents to return (must be > 0)
     * @return a Flux that emits the paginated matching entities
     * @throws IllegalArgumentException if filter is null, offset is negative, or count is non-positive
     */
    public Flux<T> list(final Bson filter, final int offset, final int count) {
        return collExecutor.list(filter, offset, count, rowType);
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
     * List<String> fields = Arrays.asList("name", "email");
     * Bson filter = Filters.eq("department", "IT");
     * Flux<User> itUsers = userMapper.list(fields, filter);
     * }</pre>
     *
     * @param selectPropNames the collection of property names to include in the results
     * @param filter the query filter to match documents
     * @return a Flux that emits matching entities with only the specified fields populated
     * @throws IllegalArgumentException if selectPropNames is empty or filter is null
     */
    public Flux<T> list(final Collection<String> selectPropNames, final Bson filter) {
        return collExecutor.list(selectPropNames, filter, rowType);
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
     * List<String> fields = Arrays.asList("id", "name", "status");
     * Bson filter = Filters.eq("active", true);
     * Flux<User> users = userMapper.list(fields, filter, 0, 50);
     * }</pre>
     *
     * @param selectPropNames the collection of property names to include in the results
     * @param filter the query filter to match documents against
     * @param offset the number of documents to skip (must be >= 0)
     * @param count the maximum number of documents to return (must be > 0)
     * @return a Flux that emits the paginated matching entities with specified fields
     * @throws IllegalArgumentException if parameters are invalid
     */
    public Flux<T> list(final Collection<String> selectPropNames, final Bson filter, final int offset, final int count) {
        return collExecutor.list(selectPropNames, filter, offset, count, rowType);
    }

    /**
     * Lists entities with specified fields and sorting.
     *
     * <p>Retrieves documents matching the filter criteria with specified field projection and
     * sorting order. The results are streamed in the order defined by the sort specification.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<String> fields = Arrays.asList("name", "createdAt");
     * Bson filter = Filters.eq("status", "pending");
     * Bson sort = Sorts.descending("createdAt");
     * Flux<User> recentPending = userMapper.list(fields, filter, sort);
     * }</pre>
     *
     * @param selectPropNames the collection of property names to include in the results
     * @param filter the query filter to match documents against
     * @param sort the sort specification for ordering results
     * @return a Flux that emits sorted matching entities with specified fields
     * @throws IllegalArgumentException if parameters are invalid
     * @see com.mongodb.client.model.Sorts
     */
    public Flux<T> list(final Collection<String> selectPropNames, final Bson filter, final Bson sort) {
        return collExecutor.list(selectPropNames, filter, sort, rowType);
    }

    /**
     * Lists entities with full control over projection, filtering, sorting, and pagination.
     *
     * <p>Provides complete control over the query execution including field selection, filtering,
     * sorting, and pagination. This is the most flexible list method for complex query requirements.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<String> fields = Arrays.asList("id", "name", "score");
     * Bson filter = Filters.gte("score", 80);
     * Bson sort = Sorts.descending("score");
     * Flux<User> topScorers = userMapper.list(fields, filter, sort, 0, 10);
     * }</pre>
     *
     * @param selectPropNames the collection of property names to include in the results
     * @param filter the query filter to match documents against
     * @param sort the sort specification for ordering results
     * @param offset the number of documents to skip (must be >= 0)
     * @param count the maximum number of documents to return (must be > 0)
     * @return a Flux that emits the fully controlled query results
     * @throws IllegalArgumentException if parameters are invalid
     */
    public Flux<T> list(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final int offset, final int count) {
        return collExecutor.list(selectPropNames, filter, sort, offset, count, rowType);
    }

    /**
     * Lists entities using a BSON projection document for field selection.
     *
     * <p>Uses a MongoDB projection document to control which fields are included or excluded
     * in the results. This provides fine-grained control over field inclusion/exclusion patterns.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson projection = Projections.fields(
     *     Projections.include("name", "email"),
     *     Projections.excludeId()
     * );
     * Flux<User> users = userMapper.list(projection, Filters.empty(), null);
     * }</pre>
     *
     * @param projection the BSON projection document for field selection
     * @param filter the query filter to match documents against
     * @param sort the sort specification for ordering results (can be null)
     * @return a Flux that emits matching entities with projection applied
     * @throws IllegalArgumentException if projection or filter is null
     * @see com.mongodb.client.model.Projections
     */
    public Flux<T> list(final Bson projection, final Bson filter, final Bson sort) {
        return collExecutor.list(projection, filter, sort, rowType);
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
     * Bson projection = Projections.include("id", "name", "tags");
     * Bson filter = Filters.in("tags", "premium", "verified");
     * Bson sort = Sorts.ascending("name");
     * Flux<User> premiumUsers = userMapper.list(projection, filter, sort, 0, 100);
     * }</pre>
     *
     * @param projection the BSON projection document for field selection
     * @param filter the query filter to match documents against
     * @param sort the sort specification for ordering results (can be null)
     * @param offset the number of documents to skip (must be >= 0)
     * @param count the maximum number of documents to return (must be > 0)
     * @return a Flux that emits the fully controlled query results with projection
     * @throws IllegalArgumentException if required parameters are invalid
     */
    public Flux<T> list(final Bson projection, final Bson filter, final Bson sort, final int offset, final int count) {
        return collExecutor.list(projection, filter, sort, offset, count, rowType);
    }

    /**
     * Queries for a single boolean value from a document property.
     *
     * <p>Retrieves the boolean value of a specific property from the first document matching
     * the filter. This is useful for checking flags or boolean states in documents.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("username", "admin");
     * Mono<Boolean> isActive = userMapper.queryForBoolean("isActive", filter);
     * isActive.subscribe(active -> System.out.println("Admin active: " + active));
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against
     * @return a Mono that emits the boolean value, or empty if not found
     * @throws IllegalArgumentException if propName or filter is null
     */
    @Beta
    public Mono<Boolean> queryForBoolean(final String propName, final Bson filter) {
        return collExecutor.queryForBoolean(propName, filter);
    }

    /**
     * Queries for a single character value from a document property.
     *
     * <p>Retrieves the character value of a specific property from the first document matching
     * the filter. This is useful for retrieving single character codes or flags.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("id", userId);
     * Mono<Character> grade = userMapper.queryForChar("grade", filter);
     * grade.subscribe(g -> System.out.println("User grade: " + g));
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against
     * @return a Mono that emits the character value, or empty if not found
     * @throws IllegalArgumentException if propName or filter is null
     */
    @Beta
    public Mono<Character> queryForChar(final String propName, final Bson filter) {
        return collExecutor.queryForChar(propName, filter);
    }

    /**
     * Queries for a single byte value from a document property.
     *
     * <p>Retrieves the byte value of a specific property from the first document matching
     * the filter. This is useful for retrieving small numeric values or byte flags.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("productId", productId);
     * Mono<Byte> status = userMapper.queryForByte("statusCode", filter);
     * status.subscribe(code -> processStatusCode(code));
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against
     * @return a Mono that emits the byte value, or empty if not found
     * @throws IllegalArgumentException if propName or filter is null
     */
    @Beta
    public Mono<Byte> queryForByte(final String propName, final Bson filter) {
        return collExecutor.queryForByte(propName, filter);
    }

    /**
     * Queries for a single short value from a document property.
     *
     * <p>Retrieves the short integer value of a specific property from the first document
     * matching the filter. This is useful for retrieving small numeric values.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("orderId", orderId);
     * Mono<Short> quantity = userMapper.queryForShort("quantity", filter);
     * quantity.subscribe(qty -> System.out.println("Order quantity: " + qty));
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against
     * @return a Mono that emits the short value, or empty if not found
     * @throws IllegalArgumentException if propName or filter is null
     */
    @Beta
    public Mono<Short> queryForShort(final String propName, final Bson filter) {
        return collExecutor.queryForShort(propName, filter);
    }

    /**
     * Queries for a single integer value from a document property.
     *
     * <p>Retrieves the integer value of a specific property from the first document matching
     * the filter. This is one of the most commonly used scalar query methods.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("email", "user@example.com");
     * Mono<Integer> age = userMapper.queryForInt("age", filter);
     * age.subscribe(a -> System.out.println("User age: " + a));
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against
     * @return a Mono that emits the integer value, or empty if not found
     * @throws IllegalArgumentException if propName or filter is null
     */
    @Beta
    public Mono<Integer> queryForInt(final String propName, final Bson filter) {
        return collExecutor.queryForInt(propName, filter);
    }

    /**
     * Queries for a single long value from a document property.
     *
     * <p>Retrieves the long integer value of a specific property from the first document
     * matching the filter. This is useful for retrieving timestamps, IDs, or large numeric values.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("sessionId", sessionId);
     * Mono<Long> timestamp = userMapper.queryForLong("lastAccessTime", filter);
     * timestamp.subscribe(time -> updateLastAccess(time));
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against
     * @return a Mono that emits the long value, or empty if not found
     * @throws IllegalArgumentException if propName or filter is null
     */
    @Beta
    public Mono<Long> queryForLong(final String propName, final Bson filter) {
        return collExecutor.queryForLong(propName, filter);
    }

    /**
     * Queries for a single float value from a document property.
     *
     * <p>Retrieves the float value of a specific property from the first document matching
     * the filter. This is useful for retrieving decimal values with single precision.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("productId", productId);
     * Mono<Float> price = userMapper.queryForFloat("price", filter);
     * price.subscribe(p -> System.out.println("Product price: $" + p));
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against
     * @return a Mono that emits the float value, or empty if not found
     * @throws IllegalArgumentException if propName or filter is null
     */
    @Beta
    public Mono<Float> queryForFloat(final String propName, final Bson filter) {
        return collExecutor.queryForFloat(propName, filter);
    }

    /**
     * Queries for a single double value from a document property.
     *
     * <p>Retrieves the double precision value of a specific property from the first document
     * matching the filter. This is ideal for financial calculations or scientific data.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("accountId", accountId);
     * Mono<Double> balance = userMapper.queryForDouble("balance", filter);
     * balance.subscribe(b -> System.out.println("Account balance: " + b));
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against
     * @return a Mono that emits the double value, or empty if not found
     * @throws IllegalArgumentException if propName or filter is null
     */
    @Beta
    public Mono<Double> queryForDouble(final String propName, final Bson filter) {
        return collExecutor.queryForDouble(propName, filter);
    }

    /**
     * Queries for a single string value from a document property.
     *
     * <p>Retrieves the string value of a specific property from the first document matching
     * the filter. This is one of the most frequently used query methods for text data.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("userId", userId);
     * Mono<String> username = userMapper.queryForString("username", filter);
     * username.subscribe(name -> System.out.println("Username: " + name));
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against
     * @return a Mono that emits the string value, or empty if not found
     * @throws IllegalArgumentException if propName or filter is null
     */
    @Beta
    public Mono<String> queryForString(final String propName, final Bson filter) {
        return collExecutor.queryForString(propName, filter);
    }

    /**
     * Queries for a single Date value from a document property.
     *
     * <p>Retrieves the Date value of a specific property from the first document matching
     * the filter. This is useful for retrieving timestamps and date fields.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("orderId", orderId);
     * Mono<Date> orderDate = userMapper.queryForDate("createdAt", filter);
     * orderDate.subscribe(date -> System.out.println("Order date: " + date));
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against
     * @return a Mono that emits the Date value, or empty if not found
     * @throws IllegalArgumentException if propName or filter is null
     */
    @Beta
    public Mono<Date> queryForDate(final String propName, final Bson filter) {
        return collExecutor.queryForDate(propName, filter);
    }

    /**
     * Queries for a typed Date subclass value from a document property.
     *
     * <p>Retrieves a date value and converts it to a specific Date subclass. This is useful
     * when working with custom date types like java.sql.Timestamp or java.sql.Date.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("transactionId", txId);
     * Mono<Timestamp> timestamp = userMapper.queryForDate("processedAt", filter, Timestamp.class);
     * timestamp.subscribe(ts -> logTransaction(ts));
     * }</pre>
     *
     * @param <P> the specific Date subclass type
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against
     * @param valueType the class of the Date subclass to convert to
     * @return a Mono that emits the typed date value, or empty if not found
     * @throws IllegalArgumentException if any parameter is null
     * @throws ClassCastException if the value cannot be converted to the specified type
     */
    public <P extends Date> Mono<P> queryForDate(final String propName, final Bson filter, final Class<P> valueType) {
        return collExecutor.queryForDate(propName, filter, valueType);
    }

    /**
     * Queries for a single value of any type from a document property.
     *
     * <p>Retrieves a value of the specified type from a specific property in the first document
     * matching the filter. This is the most flexible single-value query method, supporting
     * automatic type conversion for custom types.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("id", entityId);
     * Mono<BigDecimal> amount = userMapper.queryForSingleResult("amount", filter, BigDecimal.class);
     * amount.subscribe(amt -> processPayment(amt));
     * }</pre>
     *
     * @param <V> the type of value to retrieve
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against
     * @param valueType the class of the value type to convert to
     * @return a Mono that emits the typed value, or empty if not found
     * @throws IllegalArgumentException if any parameter is null
     * @throws ClassCastException if the value cannot be converted to the specified type
     */
    public <V> Mono<V> queryForSingleResult(final String propName, final Bson filter, final Class<V> valueType) {
        return collExecutor.queryForSingleResult(propName, filter, valueType);
    }

    /**
     * Queries all matching documents and returns them as a Dataset.
     *
     * <p>Retrieves all documents matching the filter and returns them as a Dataset,
     * which provides tabular data manipulation capabilities. Useful for data analysis
     * and reporting scenarios.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("department", "Sales");
     * Mono<Dataset> salesData = userMapper.query(filter);
     * salesData.subscribe(data -> generateReport(data));
     * }</pre>
     *
     * @param filter the query filter to match documents
     * @return a Mono that emits a Dataset containing all matching documents
     * @throws IllegalArgumentException if filter is null
     * @see Dataset
     */
    public Mono<Dataset> query(final Bson filter) {
        return collExecutor.query(filter, rowType);
    }

    /**
     * Queries documents with pagination and returns them as a Dataset.
     *
     * <p>Retrieves a paginated subset of documents matching the filter and returns them
     * as a Dataset. This is useful for processing large result sets in chunks.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.gte("score", 90);
     * Mono<Dataset> topScorers = userMapper.query(filter, 0, 100);
     * topScorers.subscribe(data -> displayLeaderboard(data));
     * }</pre>
     *
     * @param filter the query filter to match documents against
     * @param offset the number of documents to skip (must be >= 0)
     * @param count the maximum number of documents to return (must be > 0)
     * @return a Mono that emits a Dataset containing the paginated results
     * @throws IllegalArgumentException if parameters are invalid
     */
    public Mono<Dataset> query(final Bson filter, final int offset, final int count) {
        return collExecutor.query(filter, offset, count, rowType);
    }

    /**
     * Queries documents with field projection and returns them as a Dataset.
     *
     * <p>Retrieves documents matching the filter with only specified fields included
     * and returns them as a Dataset. This optimizes memory usage and network traffic.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<String> fields = Arrays.asList("name", "email", "department");
     * Bson filter = Filters.eq("active", true);
     * Mono<Dataset> activeUsers = userMapper.query(fields, filter);
     * }</pre>
     *
     * @param selectPropNames the collection of property names to include in the results
     * @param filter the query filter to match documents
     * @return a Mono that emits a Dataset with projected fields
     * @throws IllegalArgumentException if selectPropNames is empty or filter is null
     */
    public Mono<Dataset> query(final Collection<String> selectPropNames, final Bson filter) {
        return collExecutor.query(selectPropNames, filter, rowType);
    }

    /**
     * Queries documents with field projection and pagination, returning a Dataset.
     *
     * <p>Combines field projection with pagination to retrieve a subset of documents
     * with only specific fields, returned as a Dataset for structured data processing.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<String> fields = Arrays.asList("id", "name", "salary");
     * Bson filter = Filters.gt("salary", 50000);
     * Mono<Dataset> highEarners = userMapper.query(fields, filter, 0, 50);
     * }</pre>
     *
     * @param selectPropNames the collection of property names to include in the results
     * @param filter the query filter to match documents against
     * @param offset the number of documents to skip (must be >= 0)
     * @param count the maximum number of documents to return (must be > 0)
     * @return a Mono that emits a Dataset with projected and paginated results
     * @throws IllegalArgumentException if parameters are invalid
     */
    public Mono<Dataset> query(final Collection<String> selectPropNames, final Bson filter, final int offset, final int count) {
        return collExecutor.query(selectPropNames, filter, offset, count, rowType);
    }

    /**
     * Queries documents with field projection and sorting, returning a Dataset.
     *
     * <p>Retrieves sorted documents matching the filter with specified field projection,
     * returned as a Dataset. The sorting order determines the sequence of rows in the Dataset.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<String> fields = Arrays.asList("name", "joinDate", "level");
     * Bson filter = Filters.eq("status", "member");
     * Bson sort = Sorts.descending("joinDate");
     * Mono<Dataset> members = userMapper.query(fields, filter, sort);
     * }</pre>
     *
     * @param selectPropNames the collection of property names to include in the results
     * @param filter the query filter to match documents against
     * @param sort the sort specification for ordering results
     * @return a Mono that emits a sorted Dataset with projected fields
     * @throws IllegalArgumentException if required parameters are invalid
     */
    public Mono<Dataset> query(final Collection<String> selectPropNames, final Bson filter, final Bson sort) {
        return collExecutor.query(selectPropNames, filter, sort, rowType);
    }

    /**
     * Queries documents with complete control over projection, filtering, sorting, and pagination.
     *
     * <p>Provides full control over query execution with field projection, filtering, sorting,
     * and pagination, returning results as a Dataset for structured data manipulation.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<String> fields = Arrays.asList("id", "name", "score", "rank");
     * Bson filter = Filters.gte("score", 80);
     * Bson sort = Sorts.descending("score");
     * Mono<Dataset> topPlayers = userMapper.query(fields, filter, sort, 0, 10);
     * }</pre>
     *
     * @param selectPropNames the collection of property names to include in the results
     * @param filter the query filter to match documents against
     * @param sort the sort specification for ordering results
     * @param offset the number of documents to skip (must be >= 0)
     * @param count the maximum number of documents to return (must be > 0)
     * @return a Mono that emits a fully controlled Dataset result
     * @throws IllegalArgumentException if parameters are invalid
     */
    public Mono<Dataset> query(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final int offset, final int count) {
        return collExecutor.query(selectPropNames, filter, sort, offset, count, rowType);
    }

    /**
     * Queries documents using BSON projection with sorting, returning a Dataset.
     *
     * <p>Uses MongoDB projection document for field selection with sorting support,
     * returning results as a Dataset. Provides fine-grained control over field inclusion/exclusion.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson projection = Projections.fields(
     *     Projections.include("name", "stats"),
     *     Projections.excludeId()
     * );
     * Bson filter = Filters.eq("type", "player");
     * Bson sort = Sorts.ascending("name");
     * Mono<Dataset> playerStats = userMapper.query(projection, filter, sort);
     * }</pre>
     *
     * @param projection the BSON projection document for field selection
     * @param filter the query filter to match documents against
     * @param sort the sort specification for ordering results (can be null)
     * @return a Mono that emits a Dataset with projection and sorting applied
     * @throws IllegalArgumentException if projection or filter is null
     */
    public Mono<Dataset> query(final Bson projection, final Bson filter, final Bson sort) {
        return collExecutor.query(projection, filter, sort, rowType);
    }

    /**
     * Queries documents with BSON projection and full pagination control, returning a Dataset.
     *
     * <p>Combines BSON projection with complete query control including filtering, sorting,
     * and pagination. Returns results as a Dataset for maximum flexibility in data processing.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson projection = Projections.include("id", "metrics", "timestamp");
     * Bson filter = Filters.gte("timestamp", startDate);
     * Bson sort = Sorts.descending("timestamp");
     * Mono<Dataset> recentMetrics = userMapper.query(projection, filter, sort, 0, 1000);
     * }</pre>
     *
     * @param projection the BSON projection document for field selection
     * @param filter the query filter to match documents against
     * @param sort the sort specification for ordering results (can be null)
     * @param offset the number of documents to skip (must be >= 0)
     * @param count the maximum number of documents to return (must be > 0)
     * @return a Mono that emits a fully controlled Dataset with projection
     * @throws IllegalArgumentException if required parameters are invalid
     */
    public Mono<Dataset> query(final Bson projection, final Bson filter, final Bson sort, final int offset, final int count) {
        return collExecutor.query(projection, filter, sort, offset, count, rowType);
    }

    /**
     * Inserts a single entity into the collection reactively.
     *
     * <p>This method provides a reactive way to insert a single entity of the mapped type into the collection.
     * The entity is automatically converted to a MongoDB document using the configured codec registry.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User newUser = new User("John Doe", "john@example.com");
     * userMapper.insertOne(newUser)
     *     .subscribe(result -> System.out.println("Inserted: " + result.getInsertedId()));
     * }</pre>
     *
     * @param obj the entity to insert
     * @return a Mono that emits the insert result when the operation completes
     * @throws IllegalArgumentException if obj is null
     * @throws com.mongodb.MongoWriteException if the insert operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #insertOne(Object, InsertOneOptions)
     * @see #insertMany(Collection)
     */
    public Mono<InsertOneResult> insertOne(final T obj) {
        return collExecutor.insertOne(obj);
    }

    /**
     * Inserts a single entity into the collection with custom options reactively.
     *
     * <p>This method provides a reactive way to insert a single entity with additional insertion options
     * such as bypass document validation. The entity is automatically converted to a MongoDB document.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User newUser = new User("John Doe", "john@example.com");
     * InsertOneOptions options = new InsertOneOptions().bypassDocumentValidation(true);
     * userMapper.insertOne(newUser, options)
     *     .subscribe(result -> System.out.println("Inserted: " + result.getInsertedId()));
     * }</pre>
     *
     * @param obj the entity to insert
     * @param options the insert options to apply (null uses default settings)
     * @return a Mono that emits the insert result when the operation completes
     * @throws IllegalArgumentException if obj is null
     * @throws com.mongodb.MongoWriteException if the insert operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #insertOne(Object)
     * @see InsertOneOptions
     */
    public Mono<InsertOneResult> insertOne(final T obj, final InsertOneOptions options) {
        return collExecutor.insertOne(obj, options);
    }

    /**
     * Inserts multiple entities into the collection reactively.
     *
     * <p>This method provides a reactive way to insert a collection of entities of the mapped type.
     * All entities are automatically converted to MongoDB documents using the configured codec registry.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<User> users = Arrays.asList(
     *     new User("John", "john@example.com"),
     *     new User("Jane", "jane@example.com")
     * );
     * userMapper.insertMany(users)
     *     .subscribe(result -> System.out.println("Inserted: " + result.getInsertedIds().size()));
     * }</pre>
     *
     * @param objList the collection of entities to insert
     * @return a Mono that emits the insert result when the operation completes
     * @throws IllegalArgumentException if objList is null or empty
     */
    public Mono<InsertManyResult> insertMany(final Collection<? extends T> objList) {
        return collExecutor.insertMany(objList);
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
        return collExecutor.insertMany(objList, options);
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
     * User updatedUser = new User();
     * updatedUser.setEmail("newemail@example.com");
     * Mono<UpdateResult> result = userMapper.updateOne("507f1f77bcf86cd799439011", updatedUser);
     * result.subscribe(r -> System.out.println("Modified: " + r.getModifiedCount()));
     * }</pre>
     *
     * @param objectId the string representation of the MongoDB ObjectId
     * @param update the entity containing the update values
     * @return a Mono that emits the update result
     * @throws IllegalArgumentException if objectId or update is null
     * @throws IllegalArgumentException if objectId is not a valid ObjectId format
     */
    public Mono<UpdateResult> updateOne(final String objectId, final T update) {
        return collExecutor.updateOne(objectId, update);
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
        return collExecutor.updateOne(objectId, update);
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
     * @throws IllegalArgumentException if filter or update is null
     */
    public Mono<UpdateResult> updateOne(final Bson filter, final T update) {
        return collExecutor.updateOne(filter, update);
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
        return collExecutor.updateOne(filter, update, options);
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
        return collExecutor.updateOne(filter, objList);
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
        return collExecutor.updateOne(filter, objList, options);
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
     * Bson filter = Filters.eq("department", "Engineering");
     * User updates = new User();
     * updates.setSalaryMultiplier(1.1);
     * Mono<UpdateResult> result = userMapper.updateMany(filter, updates);
     * result.subscribe(r -> System.out.println("Updated " + r.getModifiedCount() + " documents"));
     * }</pre>
     *
     * @param filter the query filter to match documents against
     * @param update the entity containing the update values
     * @return a Mono that emits the update result with modified count
     * @throws IllegalArgumentException if filter or update is null
     */
    public Mono<UpdateResult> updateMany(final Bson filter, final T update) {
        return collExecutor.updateMany(filter, update);
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
        return collExecutor.updateMany(filter, update, options);
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
        return collExecutor.updateMany(filter, objList);
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
        return collExecutor.updateMany(filter, objList, options);
    }

    /**
     * Replaces one document identified by string ID with a new entity.
     *
     * <p>Completely replaces the document with the specified string object ID
     * with the provided entity. Unlike update operations, this replaces the entire document.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User newUser = new User("John", "john@example.com");
     * Mono<UpdateResult> result = userMapper.replaceOne("507f1f77bcf86cd799439011", newUser);
     * result.subscribe(r -> System.out.println("Replaced: " + r.getModifiedCount()));
     * }</pre>
     *
     * @param objectId the string representation of the MongoDB ObjectId
     * @param replacement the entity to replace the existing document with
     * @return a Mono that emits the update result
     * @throws IllegalArgumentException if objectId or replacement is null
     * @throws IllegalArgumentException if objectId is not a valid ObjectId format
     */
    public Mono<UpdateResult> replaceOne(final String objectId, final T replacement) {
        return collExecutor.replaceOne(objectId, replacement);
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
        return collExecutor.replaceOne(objectId, replacement);
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
        return collExecutor.replaceOne(filter, replacement);
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
        return collExecutor.replaceOne(filter, replacement, options);
    }

    /**
     * Deletes one document identified by string ID.
     *
     * <p>Deletes the document with the specified string object ID from the collection.
     * This is a convenient method for deleting documents by their primary key.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Mono<DeleteResult> result = userMapper.deleteOne("507f1f77bcf86cd799439011");
     * result.subscribe(r -> System.out.println("Deleted: " + r.getDeletedCount()));
     * }</pre>
     *
     * @param objectId the string representation of the MongoDB ObjectId
     * @return a Mono that emits the delete result
     * @throws IllegalArgumentException if objectId is null
     * @throws IllegalArgumentException if objectId is not a valid ObjectId format
     */
    public Mono<DeleteResult> deleteOne(final String objectId) {
        return collExecutor.deleteOne(objectId);
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
        return collExecutor.deleteOne(objectId);
    }

    /**
     * Deletes one document matching the filter reactively.
     *
     * <p>Deletes the first document matching the specified filter from the collection in a reactive manner.
     * This is the most flexible single-document deletion method. Returns a Mono that emits upon completion.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("email", "user@example.com");
     * Mono<DeleteResult> result = userMapper.deleteOne(filter);
     * result.subscribe(r -> System.out.println("Deleted: " + r.getDeletedCount()));
     * }</pre>
     *
     * @param filter the query filter to match documents against
     * @return a Mono that emits the delete result
     * @throws IllegalArgumentException if filter is null
     */
    public Mono<DeleteResult> deleteOne(final Bson filter) {
        return collExecutor.deleteOne(filter);
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
        return collExecutor.deleteOne(filter, options);
    }

    /**
     * Deletes all documents matching the filter reactively.
     *
     * <p>Deletes all documents that match the specified filter from the collection in a reactive manner.
     * This is useful for bulk deletion operations. Returns a Mono that emits the result upon completion.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.lt("lastLogin", oneYearAgo);
     * Mono<DeleteResult> result = userMapper.deleteMany(filter);
     * result.subscribe(r -> System.out.println("Deleted " + r.getDeletedCount() + " inactive users"));
     * }</pre>
     *
     * @param filter the query filter to match documents against
     * @return a Mono that emits the delete result with deleted count
     * @throws IllegalArgumentException if filter is null
     */
    public Mono<DeleteResult> deleteMany(final Bson filter) {
        return collExecutor.deleteMany(filter);
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
     * @return a Mono emitting DeleteResult with deletion statistics
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoWriteException if the delete operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public Mono<DeleteResult> deleteMany(final Bson filter, final DeleteOptions options) {
        return collExecutor.deleteMany(filter, options);
    }

    /**
     * Performs a bulk insert of multiple entities into the collection.
     *
     * <p>Efficiently inserts a collection of entities in a single batch operation.
     * This method is optimized for inserting large volumes of data and provides
     * better performance than individual inserts for multiple documents.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<User> newUsers = Arrays.asList(user1, user2, user3);
     * userMapper.bulkInsert(newUsers)
     *     .subscribe(count -> System.out.println("Inserted " + count + " users"));
     * }</pre>
     *
     * @param entities the collection of entities to insert (must not be null or empty)
     * @return a Mono emitting the count of successfully inserted documents
     * @throws IllegalArgumentException if entities is null or empty
     */
    public Mono<Integer> bulkInsert(final Collection<? extends T> entities) {
        return collExecutor.bulkInsert(entities);
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
     *     .subscribe(count -> System.out.println("Bulk inserted: " + count));
     * }</pre>
     *
     * @param entities the collection of entities to insert
     * @param options configuration options for the bulk write operation
     * @return a Mono emitting the count of successfully inserted documents
     * @throws IllegalArgumentException if entities is null/empty or options is null
     */
    public Mono<Integer> bulkInsert(final Collection<? extends T> entities, final BulkWriteOptions options) {
        return collExecutor.bulkInsert(entities, options);
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
     * List<WriteModel<Document>> operations = Arrays.asList(
     *     new InsertOneModel<>(doc1),
     *     new UpdateOneModel<>(filter, update),
     *     new DeleteOneModel<>(deleteFilter));
     * userMapper.bulkWrite(operations).subscribe(result -> 
     *     System.out.println("Modified: " + result.getModifiedCount()));
     * }</pre>
     *
     * @param requests list of write operations to execute (must not be null or empty)
     * @return a Mono emitting BulkWriteResult with detailed operation statistics
     * @throws IllegalArgumentException if requests is null or empty
     */
    public Mono<BulkWriteResult> bulkWrite(final List<? extends WriteModel<? extends Document>> requests) {
        return collExecutor.bulkWrite(requests);
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
     * @param options configuration for the bulk write behavior
     * @return a Mono emitting BulkWriteResult with operation statistics
     * @throws IllegalArgumentException if requests is null/empty or options is null
     */
    public Mono<BulkWriteResult> bulkWrite(final List<? extends WriteModel<? extends Document>> requests, final BulkWriteOptions options) {
        return collExecutor.bulkWrite(requests, options);
    }

    /**
     * Atomically finds and updates a single document.
     *
     * <p>Locates the first document matching the filter and applies the update atomically,
     * returning the document either before or after the modification based on default
     * settings. This operation prevents race conditions in concurrent environments.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User updatedUser = new User().setStatus("active");
     * userMapper.findOneAndUpdate(Filters.eq("_id", userId), updatedUser)
     *     .subscribe(user -> System.out.println("Updated user: " + user.getName()));
     * }</pre>
     *
     * @param filter the query filter to identify the document to update
     * @param update the entity containing update values
     * @return a Mono emitting the found document (before or after update based on defaults)
     * @throws IllegalArgumentException if filter or update is null
     */
    public Mono<T> findOneAndUpdate(final Bson filter, final T update) {
        return collExecutor.findOneAndUpdate(filter, update, rowType);
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
     * @return a Mono emitting the document based on returnDocument option
     * @throws IllegalArgumentException if any parameter is null
     */
    public Mono<T> findOneAndUpdate(final Bson filter, final T update, final FindOneAndUpdateOptions options) {
        return collExecutor.findOneAndUpdate(filter, update, options, rowType);
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
     * @throws IllegalArgumentException if filter or objList is null/empty
     */
    public Mono<T> findOneAndUpdate(final Bson filter, final Collection<? extends T> objList) {
        return collExecutor.findOneAndUpdate(filter, objList, rowType);
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
     * @return a Mono emitting the document based on options
     * @throws IllegalArgumentException if any parameter is null or objList is empty
     */
    public Mono<T> findOneAndUpdate(final Bson filter, final Collection<? extends T> objList, final FindOneAndUpdateOptions options) {
        return collExecutor.findOneAndUpdate(filter, objList, options, rowType);
    }

    /**
     * Atomically finds and replaces a single document.
     *
     * <p>Locates the first document matching the filter and replaces it entirely with
     * the provided replacement document. Unlike update operations, this completely
     * overwrites the existing document while preserving the _id field.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User newUser = new User("John", "john@example.com", "active");
     * userMapper.findOneAndReplace(Filters.eq("_id", userId), newUser)
     *     .subscribe(oldUser -> System.out.println("Replaced: " + oldUser.getName()));
     * }</pre>
     *
     * @param filter the query filter to identify the document to replace
     * @param replacement the complete replacement document
     * @return a Mono emitting the replaced document (before replacement by default)
     * @throws IllegalArgumentException if filter or replacement is null
     */
    public Mono<T> findOneAndReplace(final Bson filter, final T replacement) {
        return collExecutor.findOneAndReplace(filter, replacement, rowType);
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
     * @return a Mono emitting the document based on returnDocument option
     * @throws IllegalArgumentException if any parameter is null
     */
    public Mono<T> findOneAndReplace(final Bson filter, final T replacement, final FindOneAndReplaceOptions options) {
        return collExecutor.findOneAndReplace(filter, replacement, options, rowType);
    }

    /**
     * Atomically finds and deletes a single document.
     *
     * <p>Locates the first document matching the filter and removes it from the collection
     * atomically, returning the deleted document. This ensures the document is retrieved
     * before deletion in a single atomic operation.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * userMapper.findOneAndDelete(Filters.eq("status", "deleted"))
     *     .subscribe(deletedUser -> 
     *         System.out.println("Removed user: " + deletedUser.getName()));
     * }</pre>
     *
     * @param filter the query filter to identify the document to delete
     * @return a Mono emitting the deleted document
     * @throws IllegalArgumentException if filter is null
     */
    public Mono<T> findOneAndDelete(final Bson filter) {
        return collExecutor.findOneAndDelete(filter, rowType);
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
     * @throws IllegalArgumentException if filter or options is null
     */
    public Mono<T> findOneAndDelete(final Bson filter, final FindOneAndDeleteOptions options) {
        return collExecutor.findOneAndDelete(filter, options, rowType);
    }

    /**
     * Returns distinct values for a specified field across the collection.
     *
     * <p>Retrieves all unique values of the specified field from all documents in
     * the collection. This is useful for getting lists of unique categories, tags,
     * or any other field where duplicate values should be eliminated.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * userMapper.distinct("country")
     *     .collectList()
     *     .subscribe(countries -> System.out.println("Countries: " + countries));
     * }</pre>
     *
     * @param fieldName the name of the field to get distinct values for
     * @return a Flux emitting distinct values of the specified field
     * @throws IllegalArgumentException if fieldName is null or empty
     */
    public Flux<T> distinct(final String fieldName) {
        return collExecutor.distinct(fieldName, rowType);
    }

    /**
     * Returns distinct values for a field with filtering.
     *
     * <p>Retrieves unique values of the specified field from documents matching
     * the provided filter. This allows for conditional distinct operations, such as
     * getting unique values only from active records or within a date range.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("status", "active");
     * userMapper.distinct("department", filter)
     *     .collectList()
     *     .subscribe(depts -> System.out.println("Active departments: " + depts));
     * }</pre>
     *
     * @param fieldName the name of the field to get distinct values for
     * @param filter the query filter to apply before extracting distinct values
     * @return a Flux emitting distinct values from filtered documents
     * @throws IllegalArgumentException if fieldName is null/empty or filter is null
     */
    public Flux<T> distinct(final String fieldName, final Bson filter) {
        return collExecutor.distinct(fieldName, filter, rowType);
    }

    /**
     * Executes an aggregation pipeline on the collection.
     *
     * <p>Processes documents through a series of pipeline stages to perform complex
     * data transformations, computations, and analysis. Aggregation pipelines can
     * filter, group, sort, project, and perform various other operations on data.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Bson> pipeline = Arrays.asList(
     *     Aggregates.match(Filters.gte("age", 18)),
     *     Aggregates.group("$country", Accumulators.sum("count", 1)));
     * userMapper.aggregate(pipeline)
     *     .subscribe(result -> System.out.println("Aggregation result: " + result));
     * }</pre>
     *
     * @param pipeline list of aggregation stages to execute in order
     * @return a Flux emitting documents resulting from the aggregation
     * @throws IllegalArgumentException if pipeline is null or empty
     */
    public Flux<T> aggregate(final List<? extends Bson> pipeline) {
        return collExecutor.aggregate(pipeline, rowType);
    }

    /**
     * Groups documents by a single field (Beta feature).
     *
     * <p>Performs a simple grouping operation on documents based on the values of
     * a single field. This is a convenience method for basic grouping without
     * requiring a full aggregation pipeline.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * userMapper.groupBy("department")
     *     .collectList()
     *     .subscribe(groups -> System.out.println("Grouped by department: " + groups));
     * }</pre>
     *
     * @param fieldName the field name to group documents by
     * @return a Flux emitting grouped results
     * @throws IllegalArgumentException if fieldName is null or empty
     */
    @Beta
    public Flux<T> groupBy(final String fieldName) {
        return collExecutor.groupBy(fieldName, rowType);
    }

    /**
     * Groups documents by multiple fields (Beta feature).
     *
     * <p>Performs grouping based on the combination of values from multiple fields.
     * This creates groups where each unique combination of field values forms a
     * separate group.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<String> fields = Arrays.asList("country", "department");
     * userMapper.groupBy(fields)
     *     .collectList()
     *     .subscribe(groups -> System.out.println("Multi-field groups: " + groups));
     * }</pre>
     *
     * @param fieldNames collection of field names to group by
     * @return a Flux emitting grouped results
     * @throws IllegalArgumentException if fieldNames is null or empty
     */
    @Beta
    public Flux<T> groupBy(final Collection<String> fieldNames) {
        return collExecutor.groupBy(fieldNames, rowType);
    }

    /**
     * Groups documents by a field and counts frequency (Beta feature).
     *
     * <p>Performs grouping on a single field and includes a count of documents
     * in each group. This is useful for generating frequency distributions or
     * summary statistics.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * userMapper.groupByAndCount("status")
     *     .collectList()
     *     .subscribe(counts -> System.out.println("Status counts: " + counts));
     * }</pre>
     *
     * @param fieldName the field name to group and count by
     * @return a Flux emitting groups with document counts
     * @throws IllegalArgumentException if fieldName is null or empty
     */
    @Beta
    public Flux<T> groupByAndCount(final String fieldName) {
        return collExecutor.groupByAndCount(fieldName, rowType);
    }

    /**
     * Groups documents by multiple fields with counts (Beta feature).
     *
     * <p>Performs multi-field grouping and counts the number of documents in each
     * group. This provides frequency analysis across multiple dimensions of data.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<String> fields = Arrays.asList("country", "status");
     * userMapper.groupByAndCount(fields)
     *     .collectList()
     *     .subscribe(counts -> System.out.println("Distribution: " + counts));
     * }</pre>
     *
     * @param fieldNames collection of field names to group and count by
     * @return a Flux emitting groups with document counts
     * @throws IllegalArgumentException if fieldNames is null or empty
     */
    @Beta
    public Flux<T> groupByAndCount(final Collection<String> fieldNames) {
        return collExecutor.groupByAndCount(fieldNames, rowType);
    }

    /**
     * Executes a map-reduce operation on the collection (Deprecated).
     *
     * <p>Performs map-reduce operations using JavaScript functions for complex
     * data processing. This method is deprecated in favor of the aggregation
     * framework which provides better performance and more features.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String mapFunction = "function() { emit(this.category, 1); }";
     * String reduceFunction = "function(key, values) { return Array.sum(values); }";
     * userMapper.mapReduce(mapFunction, reduceFunction)
     *     .subscribe(result -> System.out.println("Map-reduce result: " + result));
     * }</pre>
     *
     * @param mapFunction JavaScript function to map documents
     * @param reduceFunction JavaScript function to reduce mapped values
     * @return a Flux emitting map-reduce results
     * @throws IllegalArgumentException if mapFunction or reduceFunction is null
     * @deprecated Use {@link #aggregate(List)} with aggregation pipeline instead.
     */
    @Deprecated
    public Flux<T> mapReduce(final String mapFunction, final String reduceFunction) {
        return collExecutor.mapReduce(mapFunction, reduceFunction, rowType);
    }
}
