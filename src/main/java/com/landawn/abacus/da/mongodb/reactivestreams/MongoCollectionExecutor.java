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
import com.landawn.abacus.da.mongodb.MongoDBBase;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.function.Function;
import com.mongodb.BasicDBObject;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.EstimatedDocumentCountOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Reactive MongoDB collection executor providing Publisher-based database operations with full reactive streams support.
 *
 * <p>This class provides a comprehensive reactive interface for MongoDB collection operations, returning
 * Publishers for all database interactions. It integrates seamlessly with reactive frameworks like Project Reactor,
 * RxJava, and supports the full reactive streams specification including backpressure, error handling,
 * and completion semantics.</p>
 *
 * <h2>Key Features</h2>
 * <h3>Core Capabilities:</h3>
 * <ul>
 *   <li><strong>Publisher-Based Operations:</strong> All methods return Publishers for reactive composition</li>
 *   <li><strong>Backpressure Support:</strong> Automatic backpressure handling for streaming operations</li>
 *   <li><strong>Error Propagation:</strong> Reactive error handling through Publisher error signals</li>
 *   <li><strong>Resource Management:</strong> Automatic cursor and connection lifecycle management</li>
 *   <li><strong>Integration Ready:</strong> Direct compatibility with Reactor, RxJava, and other reactive libraries</li>
 *   <li><strong>Stream Processing:</strong> Natural integration with reactive stream processing pipelines</li>
 * </ul>
 *
 * <h3>Reactive Patterns:</h3>
 * <ul>
 *   <li><strong>Cold Publishers:</strong> Operations start when subscribed to</li>
 *   <li><strong>Hot Streams:</strong> Change streams for real-time data monitoring</li>
 *   <li><strong>Single-Value Operations:</strong> Mono-like semantics for single results</li>
 *   <li><strong>Multi-Value Streams:</strong> Flux-like semantics for result sets</li>
 * </ul>
 *
 * <h3>Thread Safety:</h3>
 * <p>This class is thread-safe. All operations can be called concurrently from multiple threads.
 * The underlying reactive MongoDB driver handles thread safety and resource management.</p>
 *
 * <h3>Performance Considerations:</h3>
 * <ul>
 *   <li>Publishers are lazy - computation starts only on subscription</li>
 *   <li>Use appropriate schedulers to avoid blocking reactive threads</li>
 *   <li>Leverage backpressure to prevent memory overflow with large datasets</li>
 *   <li>Consider connection pooling settings for high-concurrency scenarios</li>
 *   <li>Use projection to reduce network bandwidth for large documents</li>
 * </ul>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * MongoCollectionExecutor executor = reactiveMongoDB.collExecutor("users");
 * 
 * // Single value operations (Mono-like):
 * Publisher<Long> countPublisher = executor.count();
 * Publisher<Document> findFirstPublisher = executor.findFirst(Filters.eq("status", "active"));
 *
 * // Multi-value operations (Flux-like):
 * Flux<Document> findPublisher = executor.list(Filters.gte("age", 18));
 * 
 * // With Project Reactor integration:
 * Mono<Long> totalCount = Mono.from(countPublisher);
 * Flux<Document> documents = Flux.from(findPublisher)
 *     .filter(doc -> doc.getInteger("score") > 80)
 *     .take(100)
 *     .doOnNext(doc -> System.out.println(doc.toJson()));
 * 
 * // Reactive aggregation pipeline:
 * List<Bson> pipeline = Arrays.asList(
 *     Aggregates.match(Filters.eq("status", "active")),
 *     Aggregates.group("$department", Accumulators.sum("count", 1))
 * );
 * 
 * Flux<Document> aggregated = Flux.from(executor.aggregate(pipeline))
 *     .doOnComplete(() -> System.out.println("Aggregation complete"))
 *     .doOnError(error -> System.err.println("Error: " + error));
 * 
 * // Subscribe to execute:
 * aggregated.subscribe(
 *     result -> processResult(result),
 *     error -> handleError(error),
 *     () -> System.out.println("Processing complete")
 * );
 * }</pre>
 *
 * @see com.mongodb.reactivestreams.client.MongoCollection
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
public final class MongoCollectionExecutor {

    static final String _$ = "$";

    static final String _$SET = "$set";

    static final String _$GROUP = "$group";

    static final String _$SUM = "$sum";

    static final String _COUNT = "count";

    private final MongoCollection<Document> coll;

    /**
     * Package-private constructor for creating a reactive MongoDB collection executor.
     *
     * @param coll the reactive MongoDB collection to wrap
     */
    MongoCollectionExecutor(final MongoCollection<Document> coll) {
        this.coll = coll;
    }

    /**
     * Returns the underlying reactive MongoDB collection for advanced operations.
     *
     * <p>This method provides direct access to the MongoDB reactive streams driver's {@code MongoCollection}
     * object, allowing for advanced reactive operations not directly exposed by this executor.</p>
     *
     * @return the reactive MongoDB collection instance
     * @see com.mongodb.reactivestreams.client.MongoCollection
     */
    public MongoCollection<Document> coll() {
        return coll;
    }

    /**
     * Checks if a document exists with the specified ObjectId string in a reactive manner.
     *
     * <p>This method provides a reactive way to verify document existence without retrieving the full document,
     * making it efficient for existence checks. The operation completes when the existence check finishes.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionExecutor executor = reactiveMongoDB.collExecutor("users");
     * 
     * Mono<Boolean> existsMono = executor.exists("507f1f77bcf86cd799439011");
     * existsMono.subscribe(
     *     exists -> System.out.println(exists ? "User found" : "User not found"),
     *     error -> System.err.println("Error checking existence: " + error)
     * );
     * }</pre>
     *
     * @param objectId the ObjectId as a string to check for existence
     * @return a Mono that emits {@code true} if a document with the specified ObjectId exists, {@code false} otherwise
     * @throws IllegalArgumentException if objectId is null or empty
     * @throws org.bson.BsonInvalidOperationException if objectId string is not a valid ObjectId format
     * @see ObjectId
     */
    public Mono<Boolean> exists(final String objectId) {
        return exists(createObjectId(objectId));
    }

    /**
     * Checks if a document exists with the specified ObjectId in a reactive manner.
     *
     * <p>This method provides a reactive way to verify document existence using a typed ObjectId,
     * making it efficient for existence checks. The operation completes when the existence check finishes.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId userId = new ObjectId("507f1f77bcf86cd799439011");
     * Mono<Boolean> existsMono = executor.exists(userId);
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
        return exists(MongoDBBase.objectId2Filter(objectId));
    }

    /**
     * Checks if any documents exist matching the specified filter in a reactive manner.
     *
     * <p>This method provides a reactive way to verify if documents matching the given filter exist
     * without retrieving the actual documents. It's optimized to stop after finding the first match,
     * making it efficient for existence checks on filtered queries.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson activeUserFilter = Filters.eq("status", "active");
     * Mono<Boolean> existsMono = executor.exists(activeUserFilter);
     * 
     * existsMono.subscribe(
     *     exists -> System.out.println("Active users exist: " + exists),
     *     error -> System.err.println("Error: " + error)
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
        return count(filter, new CountOptions().limit(1)).map(c -> c > 0);
    }

    /**
     * Counts all documents in the collection reactively.
     *
     * <p>This method provides a reactive way to count all documents in the collection.
     * The operation is performed asynchronously and the count is emitted when available.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Mono<Long> countMono = executor.count();
     * 
     * countMono.subscribe(
     *     count -> System.out.println("Total documents: " + count),
     *     error -> System.err.println("Error counting documents: " + error)
     * );
     * }</pre>
     *
     * @return a Mono that emits the total count of documents in the collection
     * @see com.mongodb.reactivestreams.client.MongoCollection#countDocuments()
     */
    public Mono<Long> count() {
        return Mono.from(coll.countDocuments());
    }

    /**
     * Counts documents matching the specified filter reactively.
     *
     * <p>This method provides a reactive way to count documents that match the given filter criteria.
     * The operation is performed asynchronously and the count is emitted when available.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson activeFilter = Filters.eq("status", "active");
     * Mono<Long> countMono = executor.count(activeFilter);
     * 
     * countMono.subscribe(
     *     count -> System.out.println("Active documents: " + count)
     * );
     * }</pre>
     *
     * @param filter the query filter to match documents against
     * @return a Mono that emits the count of documents matching the filter
     * @throws IllegalArgumentException if filter is null
     * @see Bson
     * @see com.mongodb.client.model.Filters
     */
    public Mono<Long> count(final Bson filter) {
        return Mono.from(coll.countDocuments(filter));
    }

    /**
     * Counts documents matching the specified filter with custom options reactively.
     *
     * <p>This method provides a reactive way to count documents with additional counting options
     * such as limit, skip, maxTime, and collation. The operation is performed asynchronously.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CountOptions options = new CountOptions().limit(1000).maxTime(30, TimeUnit.SECONDS);
     * Mono<Long> countMono = executor.count(Filters.gte("age", 18), options);
     * 
     * countMono.subscribe(
     *     count -> System.out.println("Adult count (max 1000): " + count)
     * );
     * }</pre>
     *
     * @param filter the query filter to match documents against
     * @param options the count options to apply (can be null)
     * @return a Mono that emits the count of documents matching the filter with applied options
     * @throws IllegalArgumentException if filter is null
     * @see Bson
     * @see CountOptions
     * @see com.mongodb.client.model.Filters
     */
    public Mono<Long> count(final Bson filter, final CountOptions options) {
        if (options == null) {
            return Mono.from(coll.countDocuments(filter));
        } else {
            return Mono.from(coll.countDocuments(filter, options));
        }
    }

    /**
     * Returns an estimated count of documents in the collection reactively.
     *
     * <p>This method provides a reactive way to get an estimated document count, which is faster than
     * an exact count but may be less accurate. It uses collection metadata rather than scanning documents.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Mono<Long> estimatedCountMono = executor.estimatedDocumentCount();
     * 
     * estimatedCountMono.subscribe(
     *     count -> System.out.println("Estimated documents: " + count)
     * );
     * }</pre>
     *
     * @return a Mono that emits the estimated count of documents in the collection
     * @see com.mongodb.reactivestreams.client.MongoCollection#estimatedDocumentCount()
     */
    public Mono<Long> estimatedDocumentCount() {
        return Mono.from(coll.estimatedDocumentCount());
    }

    /**
     * Returns an estimated count of documents in the collection with custom options reactively.
     *
     * <p>This method provides a reactive way to get an estimated document count with additional options
     * such as maxTime. The estimate is faster than exact counting but may be less accurate.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * EstimatedDocumentCountOptions options = new EstimatedDocumentCountOptions()
     *     .maxTime(10, TimeUnit.SECONDS);
     * 
     * Mono<Long> estimatedCountMono = executor.estimatedDocumentCount(options);
     * 
     * estimatedCountMono.subscribe(
     *     count -> System.out.println("Estimated count: " + count)
     * );
     * }</pre>
     *
     * @param options the estimation options to apply
     * @return a Mono that emits the estimated count with applied options
     * @throws IllegalArgumentException if options is null
     * @see EstimatedDocumentCountOptions
     */
    public Mono<Long> estimatedDocumentCount(final EstimatedDocumentCountOptions options) {
        return Mono.from(coll.estimatedDocumentCount(options));
    }

    /**
     * Retrieves a single document by its ObjectId string reactively.
     *
     * <p>This method provides a reactive way to retrieve a document using its string ObjectId representation.
     * The operation completes when the document is found or determined to be missing.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Mono<Document> docMono = executor.get("507f1f77bcf86cd799439011");
     * 
     * docMono.subscribe(
     *     doc -> System.out.println("Found: " + doc.toJson()),
     *     error -> System.err.println("Error: " + error),
     *     () -> System.out.println("Document not found")
     * );
     * }</pre>
     *
     * @param objectId the ObjectId as a string to search for
     * @return a Mono that emits the found document, or empty if no document matches the ObjectId
     * @throws IllegalArgumentException if objectId is null or empty
     * @throws org.bson.BsonInvalidOperationException if objectId string is not a valid ObjectId format
     * @see Document
     * @see ObjectId
     */
    public Mono<Document> get(final String objectId) {
        return get(createObjectId(objectId));
    }

    /**
     * Retrieves a single document by its ObjectId reactively.
     *
     * <p>This method provides a reactive way to retrieve a document using a typed ObjectId.
     * Returns the document as a MongoDB Document object.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId userId = new ObjectId("507f1f77bcf86cd799439011");
     * Mono<Document> docMono = executor.get(userId);
     * 
     * docMono.subscribe(
     *     doc -> processDocument(doc),
     *     error -> handleError(error)
     * );
     * }</pre>
     *
     * @param objectId the ObjectId to search for
     * @return a Mono that emits the found document, or empty if no document matches the ObjectId
     * @throws IllegalArgumentException if objectId is null
     * @see Document
     * @see ObjectId
     */
    public Mono<Document> get(final ObjectId objectId) {
        return get(objectId, Document.class);
    }

    /**
     * Retrieves a single document by its ObjectId string and converts it to the specified type reactively.
     *
     * <p>This method provides a reactive way to retrieve a document and automatically convert it to
     * a Java object of the specified type using the configured codec registry.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Mono<User> userMono = executor.get("507f1f77bcf86cd799439011", User.class);
     * 
     * userMono.subscribe(
     *     user -> System.out.println("User: " + user.getName()),
     *     error -> System.err.println("Error: " + error)
     * );
     * }</pre>
     *
     * @param <T> the type to convert the document to
     * @param objectId the ObjectId as a string to search for
     * @param rowType the Class representing the target type for conversion
     * @return a Mono that emits the converted object, or empty if no document matches the ObjectId
     * @throws IllegalArgumentException if objectId is null/empty or rowType is null
     * @throws org.bson.BsonInvalidOperationException if objectId string is not a valid ObjectId format
     * @see ObjectId
     */
    public <T> Mono<T> get(final String objectId, final Class<T> rowType) {
        return get(createObjectId(objectId), rowType);
    }

    /**
     * Retrieves a single document by its ObjectId and converts it to the specified type reactively.
     *
     * <p>This method provides a reactive way to retrieve a document and automatically convert it to
     * a Java object of the specified type using the configured codec registry.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId userId = new ObjectId("507f1f77bcf86cd799439011");
     * Mono<User> userMono = executor.get(userId, User.class);
     * 
     * userMono.subscribe(
     *     user -> processUser(user),
     *     error -> handleError(error)
     * );
     * }</pre>
     *
     * @param <T> the type to convert the document to
     * @param objectId the ObjectId to search for
     * @param rowType the Class representing the target type for conversion
     * @return a Mono that emits the converted object, or empty if no document matches the ObjectId
     * @throws IllegalArgumentException if objectId or rowType is null
     * @see ObjectId
     */
    public <T> Mono<T> get(final ObjectId objectId, final Class<T> rowType) {
        return get(objectId, null, rowType);
    }

    /**
     * Retrieves a single document by its ObjectId string with field projection and type conversion reactively.
     *
     * <p>This method provides a reactive way to retrieve specific fields from a document identified by
     * its ObjectId string, then convert the result to the specified Java type. Field projection helps
     * reduce network traffic and memory usage by only retrieving necessary fields.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("name", "email", "status");
     * Mono<User> userMono = executor.get("507f1f77bcf86cd799439011", fields, User.class);
     * 
     * userMono.subscribe(
     *     user -> System.out.println("User basic info: " + user.getName())
     * );
     * }</pre>
     *
     * @param <T> the type to convert the projected document to
     * @param objectId the ObjectId as a string to search for
     * @param selectPropNames the collection of field names to include in the projection
     * @param rowType the Class representing the target type for conversion
     * @return a Mono that emits the converted projected object, or empty if no document matches the ObjectId
     * @throws IllegalArgumentException if objectId is null/empty, selectPropNames is null, or rowType is null
     * @throws org.bson.BsonInvalidOperationException if objectId string is not a valid ObjectId format
     * @see ObjectId
     * @see com.mongodb.client.model.Projections
     */
    public <T> Mono<T> get(final String objectId, final Collection<String> selectPropNames, final Class<T> rowType) {
        return get(createObjectId(objectId), selectPropNames, rowType);
    }

    /**
     * Retrieves a single document by its ObjectId with field projection and type conversion reactively.
     *
     * <p>This method provides a reactive way to retrieve specific fields from a document identified by
     * its ObjectId, then convert the result to the specified Java type. Field projection reduces
     * network traffic and memory usage by only retrieving necessary fields.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId userId = new ObjectId("507f1f77bcf86cd799439011");
     * Collection<String> fields = Arrays.asList("name", "email");
     * Mono<User> userMono = executor.get(userId, fields, User.class);
     * 
     * userMono.subscribe(
     *     user -> processUserBasicInfo(user)
     * );
     * }</pre>
     *
     * @param <T> the type to convert the projected document to
     * @param objectId the ObjectId to search for
     * @param selectPropNames the collection of field names to include in the projection
     * @param rowType the Class representing the target type for conversion
     * @return a Mono that emits the converted projected object, or empty if no document matches the ObjectId
     * @throws IllegalArgumentException if objectId, selectPropNames, or rowType is null
     * @see ObjectId
     * @see com.mongodb.client.model.Projections
     */
    public <T> Mono<T> get(final ObjectId objectId, final Collection<String> selectPropNames, final Class<T> rowType) {
        return findFirst(selectPropNames, MongoDBBase.objectId2Filter(objectId), null, rowType);
    }

    /**
     * Finds the first document matching the specified filter reactively.
     *
     * <p>This method provides a reactive way to retrieve the first document that matches the given
     * filter criteria. The operation completes when the first matching document is found or
     * when it's determined that no matching documents exist.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("status", "active");
     * Mono<Document> docMono = executor.findFirst(filter);
     * 
     * docMono.subscribe(
     *     doc -> System.out.println("First active user: " + doc.toJson()),
     *     error -> System.err.println("Error: " + error)
     * );
     * }</pre>
     *
     * @param filter the query filter to match documents against
     * @return a Mono that emits the first matching document, or empty if no documents match the filter
     * @throws IllegalArgumentException if filter is null
     * @see Document
     * @see Bson
     * @see com.mongodb.client.model.Filters
     */
    public Mono<Document> findFirst(final Bson filter) {
        return findFirst(filter, Document.class);
    }

    /**
     * Finds the first document matching the specified filter and converts it to the specified type reactively.
     *
     * <p>This method provides a reactive way to retrieve and convert the first document that matches
     * the given filter criteria. The document is automatically converted to the specified Java type
     * using the configured codec registry.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.and(
     *     Filters.eq("status", "active"),
     *     Filters.gte("age", 18)
     * );
     * Mono<User> userMono = executor.findFirst(filter, User.class);
     * 
     * userMono.subscribe(
     *     user -> System.out.println("First adult active user: " + user.getName())
     * );
     * }</pre>
     *
     * @param <T> the type to convert the document to
     * @param filter the query filter to match documents against
     * @param rowType the Class representing the target type for conversion
     * @return a Mono that emits the first matching converted object, or empty if no documents match the filter
     * @throws IllegalArgumentException if filter or rowType is null
     * @see Bson
     * @see com.mongodb.client.model.Filters
     */
    public <T> Mono<T> findFirst(final Bson filter, final Class<T> rowType) {
        return findFirst(null, filter, rowType);
    }

    /**
     * Finds the first document matching the specified filter with field projection and type conversion reactively.
     *
     * <p>This method provides a reactive way to retrieve specific fields from the first document
     * that matches the given filter criteria, then convert the result to the specified Java type.
     * Field projection reduces network traffic and memory usage.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("name", "email", "createdAt");
     * Bson filter = Filters.eq("department", "Engineering");
     * Mono<User> userMono = executor.findFirst(fields, filter, User.class);
     * 
     * userMono.subscribe(
     *     user -> System.out.println("Engineer: " + user.getName())
     * );
     * }</pre>
     *
     * @param <T> the type to convert the projected document to
     * @param selectPropNames the collection of field names to include in the projection
     * @param filter the query filter to match documents against
     * @param rowType the Class representing the target type for conversion
     * @return a Mono that emits the first matching projected converted object, or empty if no documents match
     * @throws IllegalArgumentException if selectPropNames, filter, or rowType is null
     * @see Bson
     * @see com.mongodb.client.model.Filters
     * @see com.mongodb.client.model.Projections
     */
    public <T> Mono<T> findFirst(final Collection<String> selectPropNames, final Bson filter, final Class<T> rowType) {
        return findFirst(selectPropNames, filter, null, rowType);
    }

    /**
     * Finds the first document matching the filter with projection, sorting, and type conversion.
     *
     * <p>Retrieves the first document that matches the filter criteria, with specified field projection
     * and sorting, then converts the result to the target type. The sort parameter determines
     * which document is considered "first" when multiple documents match.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("name", "score");
     * Bson sort = Sorts.descending("score");
     * Mono<User> topUser = executor.findFirst(fields, filter, sort, User.class);
     * }</pre>
     *
     * @param <T> the type to convert the document to
     * @param selectPropNames the collection of field names to include in the projection
     * @param filter the query filter to match documents against
     * @param sort the sort criteria to determine document order
     * @param rowType the Class representing the target type for conversion
     * @return a Mono that emits the first matching document converted to type T, or empty if no match
     * @see com.mongodb.client.model.Sorts
     */
    public <T> Mono<T> findFirst(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final Class<T> rowType) {
        return query(selectPropNames, filter, sort, 0, 1).next().map(toEntity(rowType));
    }

    /**
     * Finds the first document using explicit Bson projection, filter, sort, and type conversion.
     *
     * <p>Provides full control over the projection by accepting a Bson projection object directly,
     * useful for complex projections like exclusions or computed fields. Results are converted
     * to the specified type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson projection = Projections.fields(Projections.include("name"), Projections.excludeId());
     * Bson sort = Sorts.ascending("createdAt");
     * Mono<User> firstUser = executor.findFirst(projection, filter, sort, User.class);
     * }</pre>
     *
     * @param <T> the type to convert the document to
     * @param projection the Bson projection specification for fields to include/exclude
     * @param filter the query filter to match documents against
     * @param sort the sort criteria to determine document order
     * @param rowType the Class representing the target type for conversion
     * @return a Mono that emits the first matching document converted to type T, or empty if no match
     */
    public <T> Mono<T> findFirst(final Bson projection, final Bson filter, final Bson sort, final Class<T> rowType) {
        return executeQuery(projection, filter, sort, 0, 1).next().map(toEntity(rowType));
    }

    /**
     * Lists all documents matching the specified filter.
     *
     * <p>Returns a reactive stream of all documents that match the given filter criteria.
     * Documents are returned as MongoDB Document objects without type conversion.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("status", "active");
     * Flux<Document> activeDocuments = executor.list(filter);
     * }</pre>
     *
     * @param filter the query filter to match documents against
     * @return a Flux that emits all matching documents
     */
    public Flux<Document> list(final Bson filter) {
        return list(filter, Document.class);
    }

    /**
     * Lists all documents matching the filter, converted to the specified type.
     *
     * <p>Returns a reactive stream of all documents that match the filter criteria,
     * with each document automatically converted to the specified Java type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.gte("age", 18);
     * Flux<User> adultUsers = executor.list(filter, User.class);
     * }</pre>
     *
     * @param <T> the type to convert documents to
     * @param filter the query filter to match documents against
     * @param rowType the Class representing the target type for conversion
     * @return a Flux that emits all matching documents converted to type T
     */
    public <T> Flux<T> list(final Bson filter, final Class<T> rowType) {
        return list(null, filter, rowType);
    }

    /**
     * Lists documents with pagination support and type conversion.
     *
     * <p>Returns a paginated reactive stream of documents that match the filter,
     * starting from the specified offset and limited to the count. Useful for
     * implementing pagination in reactive applications.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * int pageSize = 20;
     * int pageNumber = 3;
     * Flux<User> page = executor.list(filter, pageNumber * pageSize, pageSize, User.class);
     * }</pre>
     *
     * @param <T> the type to convert documents to
     * @param filter the query filter to match documents against
     * @param offset the number of documents to skip before returning results
     * @param count the maximum number of documents to return
     * @param rowType the Class representing the target type for conversion
     * @return a Flux that emits matching documents within the specified range, converted to type T
     * @throws IllegalArgumentException if offset or count is negative
     */
    public <T> Flux<T> list(final Bson filter, final int offset, final int count, final Class<T> rowType) {
        return list(null, filter, offset, count, rowType);
    }

    /**
     * Lists documents with field projection and type conversion.
     *
     * <p>Returns a reactive stream of documents matching the filter with only
     * the specified fields included, then converts each to the target type.
     * Projection reduces network traffic and memory usage.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("name", "email");
     * Flux<User> users = executor.list(fields, filter, User.class);
     * }</pre>
     *
     * @param <T> the type to convert documents to
     * @param selectPropNames the collection of field names to include in the projection
     * @param filter the query filter to match documents against
     * @param rowType the Class representing the target type for conversion
     * @return a Flux that emits all matching documents with projected fields, converted to type T
     */
    public <T> Flux<T> list(final Collection<String> selectPropNames, final Bson filter, final Class<T> rowType) {
        return list(selectPropNames, filter, 0, Integer.MAX_VALUE, rowType);
    }

    /**
     * Lists documents with field projection, pagination, and type conversion.
     *
     * <p>Combines field projection with pagination to return a limited set of documents
     * with only specified fields, starting from an offset. Each document is converted
     * to the target type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("id", "name", "score");
     * Flux<User> topScorers = executor.list(fields, filter, 0, 10, User.class);
     * }</pre>
     *
     * @param <T> the type to convert documents to
     * @param selectPropNames the collection of field names to include in the projection
     * @param filter the query filter to match documents against
     * @param offset the number of documents to skip before returning results
     * @param count the maximum number of documents to return
     * @param rowType the Class representing the target type for conversion
     * @return a Flux that emits matching documents within range with projected fields, converted to type T
     * @throws IllegalArgumentException if offset or count is negative
     */
    public <T> Flux<T> list(final Collection<String> selectPropNames, final Bson filter, final int offset, final int count, final Class<T> rowType) {
        return list(selectPropNames, filter, null, offset, count, rowType);
    }

    /**
     * Lists documents with field projection, sorting, and type conversion.
     *
     * <p>Returns a sorted reactive stream of documents matching the filter with
     * only specified fields included. Each document is converted to the target type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("name", "score");
     * Bson sort = Sorts.descending("score");
     * Flux<User> leaderboard = executor.list(fields, filter, sort, User.class);
     * }</pre>
     *
     * @param <T> the type to convert documents to
     * @param selectPropNames the collection of field names to include in the projection
     * @param filter the query filter to match documents against
     * @param sort the sort criteria to determine document order
     * @param rowType the Class representing the target type for conversion
     * @return a Flux that emits sorted matching documents with projected fields, converted to type T
     */
    public <T> Flux<T> list(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final Class<T> rowType) {
        return list(selectPropNames, filter, sort, 0, Integer.MAX_VALUE, rowType);
    }

    /**
     * Lists documents with all query options: projection, filter, sort, pagination, and type conversion.
     *
     * <p>Provides complete control over the query with field projection, filtering, sorting,
     * and pagination. This is the most comprehensive list method, suitable for complex
     * query requirements in reactive applications.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("name", "department", "salary");
     * Bson sort = Sorts.orderBy(Sorts.ascending("department"), Sorts.descending("salary"));
     * Flux<Employee> topEarners = executor.list(fields, filter, sort, 0, 5, Employee.class);
     * }</pre>
     *
     * @param <T> the type to convert documents to
     * @param selectPropNames the collection of field names to include in the projection
     * @param filter the query filter to match documents against
     * @param sort the sort criteria to determine document order
     * @param offset the number of documents to skip before returning results
     * @param count the maximum number of documents to return
     * @param rowType an entity class with getter/setter methods, Map.class, or basic single value type
     * @return a Flux that emits matching documents with all specified constraints, converted to type T
     * @throws IllegalArgumentException if offset or count is negative
     */
    public <T> Flux<T> list(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final int offset, final int count,
            final Class<T> rowType) {
        return query(selectPropNames, filter, sort, offset, count).map(toEntity(rowType));
    }

    /**
     * Lists documents with Bson projection, filter, sort, and type conversion.
     *
     * <p>Uses a Bson projection object for complex field specifications,
     * combined with filtering and sorting. All results are converted to the target type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson projection = Projections.fields(Projections.include("name", "age"), Projections.excludeId());
     * Flux<User> users = executor.list(projection, filter, sort, User.class);
     * }</pre>
     *
     * @param <T> the type to convert documents to
     * @param projection the Bson projection specification for fields to include/exclude
     * @param filter the query filter to match documents against
     * @param sort the sort criteria to determine document order
     * @param rowType the Class representing the target type for conversion
     * @return a Flux that emits all matching sorted documents with projection, converted to type T
     */
    public <T> Flux<T> list(final Bson projection, final Bson filter, final Bson sort, final Class<T> rowType) {
        return list(projection, filter, sort, 0, Integer.MAX_VALUE, rowType);
    }

    /**
     * Lists documents with Bson projection, filter, sort, pagination, and type conversion.
     *
     * <p>Provides maximum flexibility with Bson projection, filtering, sorting, and pagination.
     * This method is ideal for complex queries requiring fine-grained control over all aspects
     * of the query execution.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson projection = Projections.include("name", "status", "priority");
     * Bson sort = Sorts.descending("priority");
     * Flux<Task> urgentTasks = executor.list(projection, filter, sort, 0, 10, Task.class);
     * }</pre>
     *
     * @param <T> the type to convert documents to
     * @param projection the Bson projection specification for fields to include/exclude
     * @param filter the query filter to match documents against
     * @param sort the sort criteria to determine document order
     * @param offset the number of documents to skip before returning results
     * @param count the maximum number of documents to return
     * @param rowType an entity class with getter/setter methods, Map.class, or basic single value type
     * @return a Flux that emits matching documents with all specified constraints, converted to type T
     * @throws IllegalArgumentException if offset or count is negative
     */
    public <T> Flux<T> list(final Bson projection, final Bson filter, final Bson sort, final int offset, final int count, final Class<T> rowType) {
        return executeQuery(projection, filter, sort, offset, count).map(toEntity(rowType));
    }

    /**
     * Queries for a single boolean value from the first matching document.
     *
     * <p>Retrieves the boolean value of the specified property from the first document
     * that matches the filter. Returns empty if no document matches or the property is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Mono<Boolean> isActive = executor.queryForBoolean("isActive", userFilter);
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against
     * @return a Mono that emits the boolean value, or empty if not found
     */
    @Beta
    public Mono<Boolean> queryForBoolean(final String propName, final Bson filter) {
        return queryForSingleResult(propName, filter, Boolean.class);
    }

    /**
     * Queries for a single character value from the first matching document.
     *
     * <p>Retrieves the character value of the specified property from the first document
     * that matches the filter. Returns empty if no document matches or the property is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Mono<Character> grade = executor.queryForChar("grade", studentFilter);
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against
     * @return a Mono that emits the character value, or empty if not found
     */
    @Beta
    public Mono<Character> queryForChar(final String propName, final Bson filter) {
        return queryForSingleResult(propName, filter, Character.class);
    }

    /**
     * Queries for a single byte value from the first matching document.
     *
     * <p>Retrieves the byte value of the specified property from the first document
     * that matches the filter. Returns empty if no document matches or the property is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Mono<Byte> statusCode = executor.queryForByte("statusCode", recordFilter);
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against
     * @return a Mono that emits the byte value, or empty if not found
     */
    @Beta
    public Mono<Byte> queryForByte(final String propName, final Bson filter) {
        return queryForSingleResult(propName, filter, Byte.class);
    }

    /**
     * Queries for a single short value from the first matching document.
     *
     * <p>Retrieves the short value of the specified property from the first document
     * that matches the filter. Returns empty if no document matches or the property is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Mono<Short> quantity = executor.queryForShort("quantity", productFilter);
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against
     * @return a Mono that emits the short value, or empty if not found
     */
    @Beta
    public Mono<Short> queryForShort(final String propName, final Bson filter) {
        return queryForSingleResult(propName, filter, Short.class);
    }

    /**
     * Queries for a single integer value from the first matching document.
     *
     * <p>Retrieves the integer value of the specified property from the first document
     * that matches the filter. Returns empty if no document matches or the property is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Mono<Integer> age = executor.queryForInt("age", userFilter);
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against
     * @return a Mono that emits the integer value, or empty if not found
     */
    @Beta
    public Mono<Integer> queryForInt(final String propName, final Bson filter) {
        return queryForSingleResult(propName, filter, Integer.class);
    }

    /**
     * Queries for a single long value from the first matching document.
     *
     * <p>Retrieves the long value of the specified property from the first document
     * that matches the filter. Returns empty if no document matches or the property is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Mono<Long> timestamp = executor.queryForLong("timestamp", eventFilter);
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against
     * @return a Mono that emits the long value, or empty if not found
     */
    @Beta
    public Mono<Long> queryForLong(final String propName, final Bson filter) {
        return queryForSingleResult(propName, filter, Long.class);
    }

    /**
     * Queries for a single float value from the first matching document.
     *
     * <p>Retrieves the float value of the specified property from the first document
     * that matches the filter. Returns empty if no document matches or the property is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Mono<Float> rating = executor.queryForFloat("rating", productFilter);
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against
     * @return a Mono that emits the float value, or empty if not found
     */
    @Beta
    public Mono<Float> queryForFloat(final String propName, final Bson filter) {
        return queryForSingleResult(propName, filter, Float.class);
    }

    /**
     * Queries for a single double value from the first matching document.
     *
     * <p>Retrieves the double value of the specified property from the first document
     * that matches the filter. Returns empty if no document matches or the property is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Mono<Double> price = executor.queryForDouble("price", itemFilter);
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against
     * @return a Mono that emits the double value, or empty if not found
     */
    @Beta
    public Mono<Double> queryForDouble(final String propName, final Bson filter) {
        return queryForSingleResult(propName, filter, Double.class);
    }

    /**
     * Queries for a single string value from the first matching document.
     *
     * <p>Retrieves the string value of the specified property from the first document
     * that matches the filter. Returns empty if no document matches or the property is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Mono<String> username = executor.queryForString("username", userFilter);
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against
     * @return a Mono that emits the string value, or empty if not found
     */
    @Beta
    public Mono<String> queryForString(final String propName, final Bson filter) {
        return queryForSingleResult(propName, filter, String.class);
    }

    /**
     * Queries for a single Date value from the first matching document.
     *
     * <p>Retrieves the Date value of the specified property from the first document
     * that matches the filter. Returns empty if no document matches or the property is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Mono<Date> createdAt = executor.queryForDate("createdAt", recordFilter);
     * }</pre>
     *
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against
     * @return a Mono that emits the Date value, or empty if not found
     */
    @Beta
    public Mono<Date> queryForDate(final String propName, final Bson filter) {
        return queryForSingleResult(propName, filter, Date.class);
    }

    /**
     * Queries for a single Date subtype value from the first matching document.
     *
     * <p>Retrieves and converts the date property to a specific Date subclass type,
     * such as java.sql.Date or java.sql.Timestamp. Returns empty if no match or property is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Mono<Timestamp> lastModified = executor.queryForDate("lastModified", filter, Timestamp.class);
     * }</pre>
     *
     * @param <T> the specific Date subtype to return
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against
     * @param rowType the specific Date subclass to convert to
     * @return a Mono that emits the typed Date value, or empty if not found
     */
    public <T extends Date> Mono<T> queryForDate(final String propName, final Bson filter, final Class<T> rowType) {
        return queryForSingleResult(propName, filter, rowType);
    }

    /**
     * Queries for a single value of any type from the first matching document.
     *
     * <p>Generic method to retrieve any property value from the first matching document,
     * with automatic type conversion to the specified value type. Returns empty if no
     * document matches or the property is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Mono<BigDecimal> balance = executor.queryForSingleResult("balance", filter, BigDecimal.class);
     * }</pre>
     *
     * @param <V> the type of value to retrieve
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against
     * @param valueType the Class representing the target type for conversion
     * @return a Mono that emits the converted value, or empty if not found
     */
    public <V> Mono<V> queryForSingleResult(final String propName, final Bson filter, final Class<V> valueType) {
        return query(N.asList(propName), filter, null, 0, 1).next().flatMap(doc -> convert(doc, propName, valueType));
    }

    private static <V> Mono<V> convert(final Document doc, final String propName, final Class<V> targetType) {
        return N.isEmpty(doc) ? Mono.empty() : Mono.just(N.convert(doc.get(propName), targetType));
    }

    /**
     * Executes a query and returns results as a Dataset.
     *
     * <p>Performs a query with the specified filter and returns all matching documents
     * wrapped in a Dataset structure, which provides tabular data manipulation capabilities.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Mono<Dataset> dataset = executor.query(Filters.eq("status", "active"));
     * }</pre>
     *
     * @param filter the query filter to match documents against
     * @return a Mono that emits a Dataset containing all matching documents
     */
    public Mono<Dataset> query(final Bson filter) {
        return query(filter, Document.class);
    }

    /**
     * Executes a query and returns results as a typed Dataset.
     *
     * <p>Performs a query with the specified filter and returns all matching documents
     * in a Dataset, with rows converted to the specified type for easier manipulation.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Mono<Dataset> userDataset = executor.query(filter, User.class);
     * }</pre>
     *
     * @param <T> the type to convert documents to
     * @param filter the query filter to match documents against
     * @param rowType the Class representing the row type for the Dataset
     * @return a Mono that emits a Dataset with typed rows
     */
    public <T> Mono<Dataset> query(final Bson filter, final Class<T> rowType) {
        return query(null, filter, rowType);
    }

    /**
     * Executes a paginated query and returns results as a typed Dataset.
     *
     * <p>Performs a paginated query with the specified filter, offset, and count,
     * returning results in a Dataset structure with typed rows.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Mono<Dataset> pageData = executor.query(filter, 20, 10, User.class);
     * }</pre>
     *
     * @param <T> the type to convert documents to
     * @param filter the query filter to match documents against
     * @param offset the number of documents to skip before returning results
     * @param count the maximum number of documents to return
     * @param rowType the Class representing the row type for the Dataset
     * @return a Mono that emits a Dataset with typed rows within the specified range
     * @throws IllegalArgumentException if offset or count is negative
     */
    public <T> Mono<Dataset> query(final Bson filter, final int offset, final int count, final Class<T> rowType) {
        return query(null, filter, offset, count, rowType);
    }

    /**
     * Executes a query with field projection and returns results as a typed Dataset.
     *
     * <p>Performs a query with field projection, returning only specified fields
     * in a Dataset structure with typed rows for efficient data processing.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("name", "department");
     * Mono<Dataset> employeeData = executor.query(fields, filter, Employee.class);
     * }</pre>
     *
     * @param <T> the type to convert documents to
     * @param selectPropNames the collection of field names to include in the projection
     * @param filter the query filter to match documents against
     * @param rowType the Class representing the row type for the Dataset
     * @return a Mono that emits a Dataset with projected fields and typed rows
     */
    public <T> Mono<Dataset> query(final Collection<String> selectPropNames, final Bson filter, final Class<T> rowType) {
        return query(selectPropNames, filter, 0, Integer.MAX_VALUE, rowType);
    }

    /**
     * Executes a paginated query with field projection and returns results as a typed Dataset.
     *
     * <p>Combines field projection with pagination to return a limited set of documents
     * with only specified fields in a Dataset structure.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("id", "name", "email");
     * Mono<Dataset> userData = executor.query(fields, filter, 0, 50, User.class);
     * }</pre>
     *
     * @param <T> the type to convert documents to
     * @param selectPropNames the collection of field names to include in the projection
     * @param filter the query filter to match documents against
     * @param offset the number of documents to skip before returning results
     * @param count the maximum number of documents to return
     * @param rowType the Class representing the row type for the Dataset
     * @return a Mono that emits a Dataset with projected fields and typed rows within range
     * @throws IllegalArgumentException if offset or count is negative
     */
    public <T> Mono<Dataset> query(final Collection<String> selectPropNames, final Bson filter, final int offset, final int count, final Class<T> rowType) {
        return query(selectPropNames, filter, null, offset, count, rowType);
    }

    /**
     * Executes a sorted query with field projection and returns results as a typed Dataset.
     *
     * <p>Performs a sorted query with field projection, returning ordered results
     * with only specified fields in a Dataset structure.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("name", "score");
     * Bson sort = Sorts.descending("score");
     * Mono<Dataset> leaderboard = executor.query(fields, filter, sort, Player.class);
     * }</pre>
     *
     * @param <T> the type to convert documents to
     * @param selectPropNames the collection of field names to include in the projection
     * @param filter the query filter to match documents against
     * @param sort the sort criteria to determine document order
     * @param rowType the Class representing the row type for the Dataset
     * @return a Mono that emits a Dataset with projected fields and sorted typed rows
     */
    public <T> Mono<Dataset> query(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final Class<T> rowType) {
        return query(selectPropNames, filter, sort, 0, Integer.MAX_VALUE, rowType);
    }

    /**
     * Executes a fully customized query and returns results as a typed Dataset.
     *
     * <p>Performs a query with complete control over projection, filtering, sorting,
     * and pagination, returning results in a Dataset structure for advanced data processing.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("id", "name", "salary");
     * Bson sort = Sorts.descending("salary");
     * Mono<Dataset> topEarners = executor.query(fields, filter, sort, 0, 10, Employee.class);
     * }</pre>
     *
     * @param <T> the type to convert documents to
     * @param selectPropNames the collection of field names to include in the projection
     * @param filter the query filter to match documents against
     * @param sort the sort criteria to determine document order
     * @param offset the number of documents to skip before returning results
     * @param count the maximum number of documents to return
     * @param rowType the Class representing the row type for the Dataset
     * @return a Mono that emits a Dataset with all specified constraints applied
     * @throws IllegalArgumentException if offset or count is negative
     */
    public <T> Mono<Dataset> query(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final int offset, final int count,
            final Class<T> rowType) {
        if (N.isEmpty(selectPropNames)) {
            return query(selectPropNames, filter, sort, offset, count).collectList().map(rowList -> MongoDB.extractData(rowList, rowType));
        } else {
            return query(selectPropNames, filter, sort, offset, count).collectList().map(rowList -> MongoDB.extractData(selectPropNames, rowList, rowType));
        }
    }

    /**
     * Executes a query with Bson projection and returns results as a typed Dataset.
     *
     * <p>Uses a Bson projection object for complex field specifications combined
     * with filtering and sorting, returning results in a Dataset structure.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson projection = Projections.include("name", "status");
     * Mono<Dataset> statusReport = executor.query(projection, filter, sort, Task.class);
     * }</pre>
     *
     * @param <T> the type to convert documents to
     * @param projection the Bson projection specification for fields to include/exclude
     * @param filter the query filter to match documents against
     * @param sort the sort criteria to determine document order
     * @param rowType the Class representing the row type for the Dataset
     * @return a Mono that emits a Dataset with projected and sorted typed rows
     */
    public <T> Mono<Dataset> query(final Bson projection, final Bson filter, final Bson sort, final Class<T> rowType) {
        return query(projection, filter, sort, 0, Integer.MAX_VALUE, rowType);
    }

    /**
     * Executes a fully customized query with Bson projection and returns results as a typed Dataset.
     *
     * <p>Provides maximum query flexibility with Bson projection, filtering, sorting,
     * and pagination, returning results in a Dataset for complex data analysis.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson projection = Projections.fields(Projections.include("name"), Projections.computed("total", "$sum"));
     * Mono<Dataset> analysis = executor.query(projection, filter, sort, 0, 100, Report.class);
     * }</pre>
     *
     * @param <T> the type to convert documents to
     * @param projection the Bson projection specification for fields to include/exclude
     * @param filter the query filter to match documents against
     * @param sort the sort criteria to determine document order
     * @param offset the number of documents to skip before returning results
     * @param count the maximum number of documents to return
     * @param rowType the Class representing the row type for the Dataset
     * @return a Mono that emits a Dataset with all specified constraints applied
     * @throws IllegalArgumentException if offset or count is negative
     */
    public <T> Mono<Dataset> query(final Bson projection, final Bson filter, final Bson sort, final int offset, final int count, final Class<T> rowType) {
        return executeQuery(projection, filter, sort, offset, count).collectList().map(rowList -> MongoDB.extractData(rowList, rowType));
    }

    private static <T> Function<Document, T> toEntity(final Class<T> rowType) {
        return doc -> MongoDB.readRow(doc, rowType);
    }

    private Flux<Document> query(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final int offset, final int count) {
        if (N.isEmpty(selectPropNames)) {
            return executeQuery(null, filter, sort, offset, count);
        } else if (selectPropNames instanceof List) {
            return executeQuery(Projections.include((List<String>) selectPropNames), filter, sort, offset, count);
        } else {
            return executeQuery(Projections.include(new ArrayList<>(selectPropNames)), filter, sort, offset, count);
        }
    }

    private Flux<Document> executeQuery(final Bson projection, final Bson filter, final Bson sort, final int offset, final int count) {
        if (offset < 0 || count < 0) {
            throw new IllegalArgumentException("offset (" + offset + ") and count (" + count + ") cannot be negative");
        }

        FindPublisher<Document> findIterable = filter == null ? coll.find() : coll.find(filter);

        if (projection != null) {
            findIterable = findIterable.projection(projection);
        }

        if (sort != null) {
            findIterable = findIterable.sort(sort);
        }

        if (offset > 0) {
            findIterable = findIterable.skip(offset);
        }

        if (count < Integer.MAX_VALUE) {
            findIterable = findIterable.limit(count);
        }

        return Flux.from(findIterable);
    }

    /**
     * Creates a change stream to watch for changes in the collection.
     *
     * <p>Returns a reactive publisher that emits change events as they occur in the collection.
     * This is useful for real-time monitoring and reactive processing of database changes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ChangeStreamPublisher<Document> changeStream = executor.watch();
     * Flux.from(changeStream).subscribe(change -> processChange(change));
     * }</pre>
     *
     * @return a ChangeStreamPublisher that emits change events as Documents
     */
    public ChangeStreamPublisher<Document> watch() {
        return coll.watch();
    }

    /**
     * Creates a typed change stream to watch for changes in the collection.
     *
     * <p>Returns a reactive publisher that emits change events converted to the specified type,
     * enabling type-safe processing of collection changes in real-time.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ChangeStreamPublisher<UserChangeEvent> userChanges = executor.watch(UserChangeEvent.class);
     * Flux.from(userChanges).subscribe(event -> handleUserChange(event));
     * }</pre>
     *
     * @param <T> the type to convert change events to
     * @param rowType the Class representing the target type for change events
     * @return a ChangeStreamPublisher that emits typed change events
     */
    public <T> ChangeStreamPublisher<T> watch(final Class<T> rowType) {
        return coll.watch(rowType);
    }

    /**
     * Creates a change stream with an aggregation pipeline to watch filtered changes.
     *
     * <p>Returns a reactive publisher that emits change events filtered through an
     * aggregation pipeline, allowing for selective monitoring of specific changes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Bson> pipeline = Arrays.asList(
     *     Aggregates.match(Filters.in("operationType", "insert", "update"))
     * );
     * ChangeStreamPublisher<Document> filteredChanges = executor.watch(pipeline);
     * }</pre>
     *
     * @param pipeline the aggregation pipeline to filter change events
     * @return a ChangeStreamPublisher that emits filtered change events as Documents
     */
    public ChangeStreamPublisher<Document> watch(final List<? extends Bson> pipeline) {
        return coll.watch(pipeline);
    }

    /**
     * Creates a typed change stream with an aggregation pipeline to watch filtered changes.
     *
     * <p>Combines pipeline filtering with type conversion for change events,
     * providing both selectivity and type safety in change stream processing.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Bson> pipeline = Arrays.asList(
     *     Aggregates.match(Filters.eq("fullDocument.status", "critical"))
     * );
     * ChangeStreamPublisher<Alert> alerts = executor.watch(pipeline, Alert.class);
     * }</pre>
     *
     * @param <T> the type to convert change events to
     * @param pipeline the aggregation pipeline to filter change events
     * @param rowType the Class representing the target type for change events
     * @return a ChangeStreamPublisher that emits filtered and typed change events
     */
    public <T> ChangeStreamPublisher<T> watch(final List<? extends Bson> pipeline, final Class<T> rowType) {
        return coll.watch(pipeline, rowType);
    }

    /**
     * Inserts a single document into the collection reactively.
     *
     * <p>Inserts the provided object as a document in the collection. The object can be
     * a Document, Map, or an entity class with getter/setter methods. Returns the result
     * of the insert operation.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User newUser = new User("John", "john@example.com");
     * Mono<InsertOneResult> result = executor.insertOne(newUser);
     * }</pre>
     *
     * @param obj the object to insert (Document/Map/entity class)
     * @return a Mono that emits the InsertOneResult containing operation details
     */
    public Mono<InsertOneResult> insertOne(final Object obj) {
        return insertOne(obj, null);
    }

    /**
     * Inserts a single document with options into the collection reactively.
     *
     * <p>Inserts the provided object with custom insert options, such as bypass document
     * validation. The object is converted to a Document before insertion.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * InsertOneOptions options = new InsertOneOptions().bypassDocumentValidation(true);
     * Mono<InsertOneResult> result = executor.insertOne(entity, options);
     * }</pre>
     *
     * @param obj the object to insert (Document/Map/entity class)
     * @param options the options to apply to the insert operation
     * @return a Mono that emits the InsertOneResult containing operation details
     */
    public Mono<InsertOneResult> insertOne(final Object obj, final InsertOneOptions options) {
        if (options == null) {
            return Mono.from(coll.insertOne(toDocument(obj)));
        } else {
            return Mono.from(coll.insertOne(toDocument(obj), options));
        }
    }

    /**
     * Inserts multiple documents into the collection reactively.
     *
     * <p>Batch inserts a collection of objects as documents. Each object can be a
     * Document, Map, or entity class. This is more efficient than multiple single inserts.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<User> users = Arrays.asList(new User("Alice"), new User("Bob"));
     * Mono<InsertManyResult> result = executor.insertMany(users);
     * }</pre>
     *
     * @param objList the collection of objects to insert (Document/Map/entity classes)
     * @return a Mono that emits the InsertManyResult containing operation details
     */
    public Mono<InsertManyResult> insertMany(final Collection<?> objList) {
        return insertMany(objList, null);
    }

    /**
     * Inserts multiple documents with options into the collection reactively.
     *
     * <p>Batch inserts a collection of objects with custom insert options, such as
     * ordered/unordered insertion. Unordered insertion can continue even if some inserts fail.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * InsertManyOptions options = new InsertManyOptions().ordered(false);
     * Mono<InsertManyResult> result = executor.insertMany(documents, options);
     * }</pre>
     *
     * @param objList the collection of objects to insert (Document/Map/entity classes)
     * @param options the options to apply to the insert operation
     * @return a Mono that emits the InsertManyResult containing operation details
     */
    public Mono<InsertManyResult> insertMany(final Collection<?> objList, final InsertManyOptions options) {
        N.checkArgNotEmpty(objList, "objList");

        final List<Document> docs = toDocument(objList);

        if (options == null) {
            return Mono.from(coll.insertMany(docs));
        } else {
            return Mono.from(coll.insertMany(docs, options));
        }
    }

    private static Document toDocument(final Object obj) {
        return obj instanceof Document ? (Document) obj : MongoDBBase.toDocument(obj);
    }

    private List<Document> toDocument(final Collection<?> objList) {
        List<Document> docs = null;
        boolean allDocuments = true;

        for (final Object obj : objList) {
            if (!(obj instanceof Document)) {
                allDocuments = false;
                break;
            }
        }

        if (allDocuments) {
            if (objList instanceof List) {
                docs = (List<Document>) objList;
            } else {
                docs = new ArrayList<>((Collection<Document>) objList);
            }
        } else {
            docs = new ArrayList<>(objList.size());

            for (final Object entity : objList) {
                docs.add(MongoDBBase.toDocument(entity));
            }
        }

        return docs;
    }

    /**
     * Updates a single document by string ObjectId reactively.
     *
     * <p>Updates the document with the specified ObjectId using the provided update object.
     * If the update object implements DirtyMarker, only dirty properties are updated.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String objectId = "507f1f77bcf86cd799439011";
     * User updates = new User(); updates.setStatus("active");
     * Mono<UpdateResult> result = executor.updateOne(objectId, updates);
     * }</pre>
     *
     * @param objectId the string representation of the document's ObjectId
     * @param update the update specification (Bson/Document/Map/entity class)
     * @return a Mono that emits the UpdateResult containing operation details
     */
    public Mono<UpdateResult> updateOne(final String objectId, final Object update) {
        return updateOne(createObjectId(objectId), update);
    }

    /**
     * Updates a single document by ObjectId reactively.
     *
     * <p>Updates the document with the specified ObjectId using the provided update object.
     * The update is automatically wrapped in a $set operator unless it already contains operators.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
     * Document updates = new Document("status", "completed");
     * Mono<UpdateResult> result = executor.updateOne(id, updates);
     * }</pre>
     *
     * @param objectId the ObjectId of the document to update
     * @param update the update specification (Bson/Document/Map/entity class)
     * @return a Mono that emits the UpdateResult containing operation details
     */
    public Mono<UpdateResult> updateOne(final ObjectId objectId, final Object update) {
        return updateOne(MongoDBBase.objectId2Filter(objectId), update);
    }

    /**
     * Updates the first document matching the filter reactively.
     *
     * <p>Updates the first document that matches the filter criteria with the provided
     * update specification. The update is automatically wrapped in appropriate update operators.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("email", "user@example.com");
     * Document update = new Document("lastLogin", new Date());
     * Mono<UpdateResult> result = executor.updateOne(filter, update);
     * }</pre>
     *
     * @param filter the query filter to identify the document to update
     * @param update the update specification (Bson/Document/Map/entity class)
     * @return a Mono that emits the UpdateResult containing operation details
     */
    public Mono<UpdateResult> updateOne(final Bson filter, final Object update) {
        return updateOne(filter, update, null);
    }

    /**
     * Updates the first document matching the filter with options reactively.
     *
     * <p>Updates the first matching document with custom update options, such as
     * upsert (insert if not exists) or array filters for nested array updates.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * UpdateOptions options = new UpdateOptions().upsert(true);
     * Bson filter = Filters.eq("userId", 123);
     * Mono<UpdateResult> result = executor.updateOne(filter, updates, options);
     * }</pre>
     *
     * @param filter the query filter to identify the document to update
     * @param update the update specification (Bson/Document/Map/entity class)
     * @param options the options to apply to the update operation
     * @return a Mono that emits the UpdateResult containing operation details
     */
    public Mono<UpdateResult> updateOne(final Bson filter, final Object update, final UpdateOptions options) {
        if (options == null) {
            return Mono.from(coll.updateOne(filter, toBson(update)));
        } else {
            return Mono.from(coll.updateOne(filter, toBson(update), options));
        }
    }

    /**
     * Updates the first document using an aggregation pipeline reactively.
     *
     * <p>Updates the first matching document using an aggregation pipeline, which allows
     * for more complex update operations including field references and expressions.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Bson> pipeline = Arrays.asList(
     *     Updates.set("total", new Document("$sum", Arrays.asList("$price", "$tax")))
     * );
     * Mono<UpdateResult> result = executor.updateOne(filter, pipeline);
     * }</pre>
     *
     * @param filter the query filter to identify the document to update
     * @param objList the aggregation pipeline stages for the update
     * @return a Mono that emits the UpdateResult containing operation details
     */
    public Mono<UpdateResult> updateOne(final Bson filter, final Collection<?> objList) {
        return updateOne(filter, objList, null);
    }

    /**
     * Updates the first document using an aggregation pipeline with options reactively.
     *
     * <p>Performs a pipeline update on the first matching document with custom options,
     * enabling complex field transformations with upsert and other update behaviors.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * UpdateOptions options = new UpdateOptions().upsert(true);
     * List<Bson> pipeline = Arrays.asList(Updates.addFields(new Field("modified", "$$NOW")));
     * Mono<UpdateResult> result = executor.updateOne(filter, pipeline, options);
     * }</pre>
     *
     * @param filter the query filter to identify the document to update
     * @param objList the aggregation pipeline stages for the update
     * @param updateOptions the options to apply to the update operation
     * @return a Mono that emits the UpdateResult containing operation details
     */
    public Mono<UpdateResult> updateOne(final Bson filter, final Collection<?> objList, final UpdateOptions updateOptions) {
        final List<Bson> updateToUse = toBson(objList);

        if (updateOptions == null) {
            return Mono.from(coll.updateOne(filter, updateToUse));
        } else {
            return Mono.from(coll.updateOne(filter, updateToUse, updateOptions));
        }
    }

    private static ObjectId createObjectId(final String objectId) {
        N.checkArgNotEmpty(objectId, "objectId");

        return new ObjectId(objectId);
    }

    private static Bson toBson(final Object update) {
        final Bson bson = update instanceof Bson ? (Bson) update : MongoDBBase.toDocument(update, true);

        if (bson instanceof final Document doc) {
            if (!doc.isEmpty() && doc.keySet().iterator().next().startsWith(_$)) {
                return doc;
            }
        } else if ((bson instanceof final BasicDBObject dbObject) && (!dbObject.isEmpty() && dbObject.keySet().iterator().next().startsWith(_$))) { //NOSONAR
            return dbObject;
        }

        return new Document(_$SET, bson);
    }

    private List<Bson> toBson(final Collection<?> objList) {
        N.checkArgNotEmpty(objList, "objList");

        List<Bson> docs = null;
        boolean allBson = true;

        for (final Object obj : objList) {
            if (!(obj instanceof Bson)) {
                allBson = false;
                break;
            }
        }

        if (allBson) {
            if (objList instanceof List) {
                docs = (List<Bson>) objList;
            } else {
                docs = new ArrayList<>((Collection<Bson>) objList);
            }
        } else {
            docs = new ArrayList<>(objList.size());

            for (final Object entity : objList) {
                docs.add(toBson(entity));
            }
        }

        return docs;
    }

    /**
     * Updates all documents matching the filter reactively.
     *
     * <p>Updates all documents that match the filter criteria with the provided update
     * specification. If the update implements DirtyMarker, only dirty properties are updated.
     * This method is useful for bulk updates where you need to modify multiple documents
     * with the same update operations.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("status", "pending");
     * Document update = new Document("$set", new Document("status", "processed"));
     * Mono<UpdateResult> result = executor.updateMany(filter, update);
     * }</pre>
     *
     * @param filter the query filter to identify documents to update; must not be null
     * @param update the update specification which can be a Bson document, Document, Map, or entity class;
     *               must not be null
     * @return a Mono that emits the UpdateResult containing details about the operation including
     *         matched count, modified count, and upserted id if applicable
     * @throws IllegalArgumentException if filter or update is null
     * @see #updateMany(Bson, Object, UpdateOptions)
     */
    public Mono<UpdateResult> updateMany(final Bson filter, final Object update) {
        return updateMany(filter, update, null);
    }

    /**
     * Updates all documents matching the filter with custom options reactively.
     *
     * <p>Updates all matching documents with custom update options. This method provides full control
     * over the update operation including upsert behavior, validation bypass, array filters, and
     * collation settings. It's particularly useful for bulk updates with specific requirements
     * such as creating documents if they don't exist (upsert) or bypassing schema validation.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * UpdateOptions options = new UpdateOptions()
     *     .upsert(true)
     *     .bypassDocumentValidation(true);
     * Bson filter = Filters.lt("version", 2);
     * Document update = new Document("$set", new Document("version", 2));
     * Mono<UpdateResult> result = executor.updateMany(filter, update, options);
     * }</pre>
     *
     * @param filter the query filter to identify documents to update; must not be null
     * @param update the update specification which can be a Bson document, Document, Map, or entity class;
     *               must not be null
     * @param options the options to apply to the update operation such as upsert, validation bypass,
     *                array filters, or collation; may be null to use default options
     * @return a Mono that emits the UpdateResult containing details about the operation including
     *         matched count, modified count, and upserted id if applicable
     * @throws IllegalArgumentException if filter or update is null
     */
    public Mono<UpdateResult> updateMany(final Bson filter, final Object update, final UpdateOptions options) {
        if (options == null) {
            return Mono.from(coll.updateMany(filter, toBson(update)));
        } else {
            return Mono.from(coll.updateMany(filter, toBson(update), options));
        }
    }

    /**
     * Updates all documents matching the filter using a collection of update operations.
     *
     * <p>Applies multiple update operations from a collection to all matching documents.
     * This method is useful when you need to apply a pipeline of update operations or
     * multiple field updates in a specific order.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Bson> updates = Arrays.asList(
     *     Updates.set("status", "processed"),
     *     Updates.currentDate("lastModified")
     * );
     * Mono<UpdateResult> result = executor.updateMany(filter, updates);
     * }</pre>
     *
     * @param filter the query filter to identify documents to update; must not be null
     * @param objList collection of update operations to apply; must not be null or empty
     * @return a Mono that emits the UpdateResult containing operation details
     * @throws IllegalArgumentException if filter or objList is null or empty
     * @see #updateMany(Bson, Collection, UpdateOptions)
     */
    public Mono<UpdateResult> updateMany(final Bson filter, final Collection<?> objList) {
        return updateMany(filter, objList, null);
    }

    /**
     * Updates all documents matching the filter using a collection of update operations with options.
     *
     * <p>Applies multiple update operations from a collection to all matching documents with
     * custom update options. This provides maximum flexibility for complex bulk update scenarios
     * where multiple operations need to be applied in sequence with specific behaviors.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * UpdateOptions options = new UpdateOptions().upsert(true);
     * List<Bson> updates = Arrays.asList(
     *     Updates.inc("count", 1),
     *     Updates.addToSet("tags", "processed")
     * );
     * Mono<UpdateResult> result = executor.updateMany(filter, updates, options);
     * }</pre>
     *
     * @param filter the query filter to identify documents to update; must not be null
     * @param objList collection of update operations to apply; must not be null or empty
     * @param updateOptions the options to apply to the update operation; may be null for defaults
     * @return a Mono that emits the UpdateResult containing operation details
     * @throws IllegalArgumentException if filter or objList is null or empty
     */
    public Mono<UpdateResult> updateMany(final Bson filter, final Collection<?> objList, final UpdateOptions updateOptions) {
        N.checkArgNotEmpty(objList, "objList");

        final List<Bson> updateToUse = toBson(objList);

        if (updateOptions == null) {
            return Mono.from(coll.updateMany(filter, updateToUse));
        } else {
            return Mono.from(coll.updateMany(filter, updateToUse, updateOptions));
        }
    }

    /**
     * Replaces a single document identified by its ObjectId string reactively.
     *
     * <p>Completely replaces the document with the specified ObjectId with a new document in a reactive manner.
     * Unlike update operations, replace operations replace the entire document except for
     * the _id field. This is useful when you need to completely overwrite a document's content. Returns a Mono.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String id = "507f1f77bcf86cd799439011";
     * Document newDoc = new Document("name", "John").append("age", 30);
     * Mono<UpdateResult> result = executor.replaceOne(id, newDoc);
     * }</pre>
     *
     * @param objectId string representation of the ObjectId; must be a valid 24-character hex string
     * @param replacement the replacement document which can be Document, Map, or entity class with
     *                    getter/setter methods; must not be null
     * @return a Mono that emits the UpdateResult containing operation details
     * @throws IllegalArgumentException if objectId is invalid or replacement is null
     * @see #replaceOne(ObjectId, Object)
     */
    public Mono<UpdateResult> replaceOne(final String objectId, final Object replacement) {
        return replaceOne(createObjectId(objectId), replacement);
    }

    /**
     * Replaces a single document identified by its ObjectId reactively.
     *
     * <p>Completely replaces the document with the specified ObjectId with a new document in a reactive manner.
     * This method provides type-safe ObjectId handling for document replacement operations and returns a Mono.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
     * MyEntity entity = new MyEntity("John", 30);
     * Mono<UpdateResult> result = executor.replaceOne(id, entity);
     * }</pre>
     *
     * @param objectId the ObjectId of the document to replace; must not be null
     * @param replacement the replacement document which can be Document, Map, or entity class with
     *                    getter/setter methods; must not be null
     * @return a Mono that emits the UpdateResult containing operation details
     * @throws IllegalArgumentException if objectId or replacement is null
     * @see #replaceOne(Bson, Object)
     */
    public Mono<UpdateResult> replaceOne(final ObjectId objectId, final Object replacement) {
        return replaceOne(MongoDBBase.objectId2Filter(objectId), replacement);
    }

    /**
     * Replaces a single document matching the filter reactively.
     *
     * <p>Replaces the first document that matches the filter with the replacement document in a reactive manner.
     * If multiple documents match the filter, only the first one encountered is replaced.
     * The _id field is preserved from the original document. Returns a Mono.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("username", "olduser");
     * Document replacement = new Document("username", "newuser").append("updated", true);
     * Mono<UpdateResult> result = executor.replaceOne(filter, replacement);
     * }</pre>
     *
     * @param filter the query filter to identify the document to replace; must not be null
     * @param replacement the replacement document which can be Document, Map, or entity class with
     *                    getter/setter methods; must not be null
     * @return a Mono that emits the UpdateResult containing operation details
     * @throws IllegalArgumentException if filter or replacement is null
     * @see #replaceOne(Bson, Object, ReplaceOptions)
     */
    public Mono<UpdateResult> replaceOne(final Bson filter, final Object replacement) {
        return replaceOne(filter, replacement, null);
    }

    /**
     * Replaces a single document matching the filter with custom options reactively.
     *
     * <p>Replaces the first matching document with custom replace options in a reactive manner. This method provides
     * full control over the replace operation including upsert behavior, validation bypass,
     * and collation settings. It's particularly useful when you need to create a document
     * if it doesn't exist (upsert). Returns a Mono.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ReplaceOptions options = new ReplaceOptions().upsert(true);
     * Bson filter = Filters.eq("_id", documentId);
     * Document replacement = new Document("name", "John").append("version", 2);
     * Mono<UpdateResult> result = executor.replaceOne(filter, replacement, options);
     * }</pre>
     *
     * @param filter the query filter to identify the document to replace; must not be null
     * @param replacement the replacement document which can be Document, Map, or entity class with
     *                    getter/setter methods; must not be null
     * @param options the options to apply to the replace operation such as upsert or validation bypass;
     *                may be null to use default options
     * @return a Mono that emits the UpdateResult containing operation details including matched count,
     *         modified count, and upserted id if applicable
     * @throws IllegalArgumentException if filter or replacement is null
     */
    public Mono<UpdateResult> replaceOne(final Bson filter, final Object replacement, final ReplaceOptions options) {
        if (options == null) {
            return Mono.from(coll.replaceOne(filter, toDocument(replacement)));
        } else {
            return Mono.from(coll.replaceOne(filter, toDocument(replacement), options));
        }
    }

    /**
     * Deletes a single document by its ObjectId string reactively.
     *
     * <p>Deletes the document with the specified ObjectId in a reactive manner. This is a convenience method
     * for the common case of deleting a document by its primary key, returning a Mono.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String documentId = "507f1f77bcf86cd799439011";
     * Mono<DeleteResult> result = executor.deleteOne(documentId);
     * }</pre>
     *
     * @param objectId string representation of the ObjectId; must be a valid 24-character hex string
     * @return a Mono that emits the DeleteResult containing the count of deleted documents
     * @throws IllegalArgumentException if objectId is invalid
     * @see #deleteOne(ObjectId)
     */
    public Mono<DeleteResult> deleteOne(final String objectId) {
        return deleteOne(createObjectId(objectId));
    }

    /**
     * Deletes a single document by its ObjectId reactively.
     *
     * <p>Deletes the document with the specified ObjectId in a reactive manner. This method provides type-safe
     * ObjectId handling for deletion operations and returns a Mono that emits upon completion.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
     * Mono<DeleteResult> result = executor.deleteOne(id);
     * }</pre>
     *
     * @param objectId the ObjectId of the document to delete; must not be null
     * @return a Mono that emits the DeleteResult containing the count of deleted documents
     * @throws IllegalArgumentException if objectId is null
     * @see #deleteOne(Bson)
     */
    public Mono<DeleteResult> deleteOne(final ObjectId objectId) {
        return deleteOne(MongoDBBase.objectId2Filter(objectId));
    }

    /**
     * Deletes a single document matching the filter reactively.
     *
     * <p>Deletes the first document that matches the filter in a reactive manner. If multiple documents match,
     * only the first one encountered is deleted. This method is useful for precise
     * deletion operations based on custom criteria and returns a Mono.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.and(
     *     Filters.eq("status", "expired"),
     *     Filters.lt("created", oldDate)
     * );
     * Mono<DeleteResult> result = executor.deleteOne(filter);
     * }</pre>
     *
     * @param filter the query filter to identify the document to delete; must not be null
     * @return a Mono that emits the DeleteResult containing the count of deleted documents
     * @throws IllegalArgumentException if filter is null
     * @see #deleteOne(Bson, DeleteOptions)
     */
    public Mono<DeleteResult> deleteOne(final Bson filter) {
        return Mono.from(coll.deleteOne(filter));
    }

    /**
     * Deletes a single document matching the filter with custom options reactively.
     *
     * <p>Deletes the first matching document with custom delete options in a reactive manner. This method provides
     * control over the delete operation including collation settings and hint specifications
     * for index usage, returning a Mono that emits upon completion.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DeleteOptions options = new DeleteOptions()
     *     .collation(Collation.builder().locale("en").build());
     * Bson filter = Filters.eq("username", "JohnDoe");
     * Mono<DeleteResult> result = executor.deleteOne(filter, options);
     * }</pre>
     *
     * @param filter the query filter to identify the document to delete; must not be null
     * @param options the options to apply to the delete operation such as collation or hint;
     *                must not be null
     * @return a Mono that emits the DeleteResult containing the count of deleted documents
     * @throws IllegalArgumentException if filter or options is null
     */
    public Mono<DeleteResult> deleteOne(final Bson filter, final DeleteOptions options) {
        return Mono.from(coll.deleteOne(filter, options));
    }

    /**
     * Deletes all documents matching the filter reactively.
     *
     * <p>Deletes all documents that match the filter criteria in a reactive manner. This method is useful for
     * bulk deletion operations where multiple documents need to be removed based on
     * common criteria. Use with caution as it can delete multiple documents. Returns a Mono that emits upon completion.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("status", "archived");
     * Mono<DeleteResult> result = executor.deleteMany(filter);
     * }</pre>
     *
     * @param filter the query filter to identify documents to delete; must not be null
     * @return a Mono that emits the DeleteResult containing the count of deleted documents
     * @throws IllegalArgumentException if filter is null
     * @see #deleteMany(Bson, DeleteOptions)
     */
    public Mono<DeleteResult> deleteMany(final Bson filter) {
        return Mono.from(coll.deleteMany(filter));
    }

    /**
     * Deletes all documents matching the filter with custom options.
     *
     * <p>Deletes all matching documents with custom delete options. This method provides
     * full control over bulk deletion operations including collation settings and
     * index hints for optimized performance.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DeleteOptions options = new DeleteOptions()
     *     .hint(Indexes.ascending("status", "created"));
     * Bson filter = Filters.and(
     *     Filters.eq("status", "expired"),
     *     Filters.lt("created", cutoffDate)
     * );
     * Mono<DeleteResult> result = executor.deleteMany(filter, options);
     * }</pre>
     *
     * @param filter the query filter to identify documents to delete; must not be null
     * @param options the options to apply to the delete operation such as collation or hint;
     *                must not be null
     * @return a Mono that emits the DeleteResult containing the count of deleted documents
     * @throws IllegalArgumentException if filter or options is null
     */
    public Mono<DeleteResult> deleteMany(final Bson filter, final DeleteOptions options) {
        return Mono.from(coll.deleteMany(filter, options));
    }

    /**
     * Performs bulk insert of multiple documents.
     *
     * <p>Inserts multiple documents in a single bulk operation. This method is significantly
     * more efficient than inserting documents one by one, especially for large datasets.
     * The operation is atomic at the document level but not for the entire batch.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Document> documents = Arrays.asList(
     *     new Document("name", "Alice").append("age", 25),
     *     new Document("name", "Bob").append("age", 30)
     * );
     * Mono<Integer> insertedCount = executor.bulkInsert(documents);
     * }</pre>
     *
     * @param entities collection of documents or entities to insert; must not be null or empty
     * @return a Mono that emits the count of successfully inserted documents
     * @throws IllegalArgumentException if entities is null or empty
     * @see #bulkInsert(Collection, BulkWriteOptions)
     */
    public Mono<Integer> bulkInsert(final Collection<?> entities) {
        return bulkInsert(entities, null);
    }

    /**
     * Performs bulk insert of multiple documents with custom options.
     *
     * <p>Inserts multiple documents with custom bulk write options. This method provides
     * control over the bulk operation including ordered/unordered execution, validation
     * bypass, and write concern settings. Unordered operations can continue after errors
     * and may perform better.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * BulkWriteOptions options = new BulkWriteOptions()
     *     .ordered(false)
     *     .bypassDocumentValidation(true);
     * List<MyEntity> entities = loadEntities();
     * Mono<Integer> insertedCount = executor.bulkInsert(entities, options);
     * }</pre>
     *
     * @param entities collection of documents or entities to insert; must not be null or empty
     * @param options the options to apply to the bulk write operation such as ordered execution
     *                or validation bypass; may be null to use default options
     * @return a Mono that emits the count of successfully inserted documents
     * @throws IllegalArgumentException if entities is null or empty
     */
    public Mono<Integer> bulkInsert(final Collection<?> entities, final BulkWriteOptions options) {
        N.checkArgNotEmpty(entities, "entities");

        final List<InsertOneModel<Document>> list = new ArrayList<>(entities.size());

        for (final Object entity : entities) {
            if (entity instanceof final Document doc) {
                list.add(new InsertOneModel<>(doc));
            } else {
                list.add(new InsertOneModel<>(MongoDBBase.toDocument(entity)));
            }
        }

        return bulkWrite(list, options).map(BulkWriteResult::getInsertedCount);
    }

    /**
     * Executes a bulk write operation with multiple write models.
     *
     * <p>Performs a bulk write operation consisting of multiple insert, update, replace,
     * or delete operations. This method provides maximum flexibility for complex bulk
     * operations where different types of operations need to be executed together.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<WriteModel<Document>> operations = Arrays.asList(
     *     new InsertOneModel<>(new Document("type", "insert")),
     *     new UpdateOneModel<>(filter, update),
     *     new DeleteOneModel<>(deleteFilter)
     * );
     * Mono<BulkWriteResult> result = executor.bulkWrite(operations);
     * }</pre>
     *
     * @param requests list of write models representing the operations to perform; must not be null or empty
     * @return a Mono that emits the BulkWriteResult containing detailed operation results
     * @throws IllegalArgumentException if requests is null or empty
     * @see #bulkWrite(List, BulkWriteOptions)
     */
    public Mono<BulkWriteResult> bulkWrite(final List<? extends WriteModel<? extends Document>> requests) {
        return bulkWrite(requests, null);
    }

    /**
     * Executes a bulk write operation with multiple write models and custom options.
     *
     * <p>Performs a bulk write operation with custom options. This method provides complete
     * control over mixed bulk operations including ordering, validation, and error handling.
     * Unordered operations may provide better performance as they can be executed in parallel.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * BulkWriteOptions options = new BulkWriteOptions().ordered(false);
     * List<WriteModel<Document>> operations = buildOperations();
     * Mono<BulkWriteResult> result = executor.bulkWrite(operations, options);
     * }</pre>
     *
     * @param requests list of write models representing the operations to perform; must not be null or empty
     * @param options the options to apply to the bulk write operation such as ordered execution;
     *                may be null to use default options
     * @return a Mono that emits the BulkWriteResult containing detailed operation results including
     *         counts for inserted, updated, and deleted documents
     * @throws IllegalArgumentException if requests is null or empty
     */
    public Mono<BulkWriteResult> bulkWrite(final List<? extends WriteModel<? extends Document>> requests, final BulkWriteOptions options) {
        N.checkArgNotEmpty(requests, "requests");

        if (options == null) {
            return Mono.from(coll.bulkWrite(requests));
        } else {
            return Mono.from(coll.bulkWrite(requests, options));
        }
    }

    /**
     * Atomically finds and updates a single document.
     *
     * <p>Finds a document matching the filter and updates it atomically. Returns the document
     * either before or after the update depending on options. This operation is atomic,
     * preventing race conditions in concurrent environments.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("_id", documentId);
     * Document update = new Document("$inc", new Document("counter", 1));
     * Mono<Document> result = executor.findOneAndUpdate(filter, update);
     * }</pre>
     *
     * @param filter the query filter to find the document; must not be null
     * @param update the update operations to apply; must not be null
     * @return a Mono that emits the found document (before or after update based on default options)
     * @throws IllegalArgumentException if filter or update is null
     * @see #findOneAndUpdate(Bson, Object, FindOneAndUpdateOptions)
     */
    public Mono<Document> findOneAndUpdate(final Bson filter, final Object update) {
        return findOneAndUpdate(filter, update, (FindOneAndUpdateOptions) null);
    }

    /**
     * Atomically finds and updates a single document, returning it as a specific type.
     *
     * <p>Finds a document matching the filter, updates it atomically, and returns the result
     * mapped to the specified type. This method combines atomic update with type-safe
     * deserialization.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("username", "john");
     * Document update = new Document("$set", new Document("lastLogin", new Date()));
     * Mono<User> user = executor.findOneAndUpdate(filter, update, User.class);
     * }</pre>
     *
     * @param <T> the type of the returned document
     * @param filter the query filter to find the document; must not be null
     * @param update the update operations to apply; must not be null
     * @param rowType the class to deserialize the result into; must not be null
     * @return a Mono that emits the found document mapped to the specified type
     * @throws IllegalArgumentException if any parameter is null
     * @see #findOneAndUpdate(Bson, Object, FindOneAndUpdateOptions, Class)
     */
    public <T> Mono<T> findOneAndUpdate(final Bson filter, final Object update, final Class<T> rowType) {
        return findOneAndUpdate(filter, update, null, rowType);
    }

    /**
     * Atomically finds and updates a single document with custom options.
     *
     * <p>Finds and updates a document with full control over the operation. Options include
     * returning the document before or after update, upsert behavior, projection, sort order,
     * and more. This is the most flexible find-and-update method.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndUpdateOptions options = new FindOneAndUpdateOptions()
     *     .returnDocument(ReturnDocument.AFTER)
     *     .upsert(true);
     * Bson filter = Filters.eq("_id", id);
     * Document update = new Document("$setOnInsert", defaults);
     * Mono<Document> result = executor.findOneAndUpdate(filter, update, options);
     * }</pre>
     *
     * @param filter the query filter to find the document; must not be null
     * @param update the update operations to apply; must not be null
     * @param options the options such as upsert, return document, projection, or sort;
     *                may be null to use default options
     * @return a Mono that emits the found document (before or after update based on options)
     * @throws IllegalArgumentException if filter or update is null
     */
    public Mono<Document> findOneAndUpdate(final Bson filter, final Object update, final FindOneAndUpdateOptions options) {
        if (options == null) {
            return Mono.from(coll.findOneAndUpdate(filter, toBson(update)));
        } else {
            return Mono.from(coll.findOneAndUpdate(filter, toBson(update), options));
        }
    }

    /**
     * Atomically finds and updates a single document with options, returning it as a specific type.
     *
     * <p>Combines atomic find-and-update with custom options and type-safe deserialization.
     * This method provides maximum flexibility for atomic update operations with typed results.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndUpdateOptions options = new FindOneAndUpdateOptions()
     *     .returnDocument(ReturnDocument.AFTER);
     * Bson filter = Filters.eq("_id", userId);
     * Document update = new Document("$inc", new Document("loginCount", 1));
     * Mono<User> user = executor.findOneAndUpdate(filter, update, options, User.class);
     * }</pre>
     *
     * @param <T> the type of the returned document
     * @param filter the query filter to find the document; must not be null
     * @param update the update operations to apply; must not be null
     * @param options the options such as upsert, return document, projection, or sort;
     *                may be null to use default options
     * @param rowType the class to deserialize the result into; must not be null
     * @return a Mono that emits the found document mapped to the specified type
     * @throws IllegalArgumentException if filter, update, or rowType is null
     */
    public <T> Mono<T> findOneAndUpdate(final Bson filter, final Object update, final FindOneAndUpdateOptions options, final Class<T> rowType) {
        return findOneAndUpdate(filter, update, options).map(toEntity(rowType));
    }

    /**
     * Atomically finds and updates a document using multiple update operations.
     *
     * <p>Applies a collection of update operations atomically to a single document.
     * This is useful when multiple update operations need to be applied in a specific
     * order as an atomic operation.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Bson> updates = Arrays.asList(
     *     Updates.set("status", "processing"),
     *     Updates.currentDate("lastModified")
     * );
     * Mono<Document> result = executor.findOneAndUpdate(filter, updates);
     * }</pre>
     *
     * @param filter the query filter to find the document; must not be null
     * @param objList collection of update operations to apply; must not be null or empty
     * @return a Mono that emits the found document
     * @throws IllegalArgumentException if filter or objList is null or empty
     * @see #findOneAndUpdate(Bson, Collection, FindOneAndUpdateOptions)
     */
    public Mono<Document> findOneAndUpdate(final Bson filter, final Collection<?> objList) {
        return findOneAndUpdate(filter, objList, (FindOneAndUpdateOptions) null);
    }

    /**
     * Atomically finds and updates a document using multiple operations, returning it as a specific type.
     *
     * <p>Applies multiple update operations atomically and returns the result as the specified type.
     * Combines pipeline updates with type-safe deserialization.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Bson> updates = buildUpdatePipeline();
     * Mono<User> user = executor.findOneAndUpdate(filter, updates, User.class);
     * }</pre>
     *
     * @param <T> the type of the returned document
     * @param filter the query filter to find the document; must not be null
     * @param objList collection of update operations to apply; must not be null or empty
     * @param rowType the class to deserialize the result into; must not be null
     * @return a Mono that emits the found document mapped to the specified type
     * @throws IllegalArgumentException if any parameter is null or objList is empty
     * @see #findOneAndUpdate(Bson, Collection, FindOneAndUpdateOptions, Class)
     */
    public <T> Mono<T> findOneAndUpdate(final Bson filter, final Collection<?> objList, final Class<T> rowType) {
        return findOneAndUpdate(filter, objList, null, rowType);
    }

    /**
     * Atomically finds and updates a document using multiple operations with custom options.
     *
     * <p>Applies a pipeline of update operations atomically with full control over the operation.
     * This method is useful for complex atomic updates that require multiple operations
     * to be applied in sequence.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndUpdateOptions options = new FindOneAndUpdateOptions()
     *     .returnDocument(ReturnDocument.AFTER);
     * List<Bson> pipeline = Arrays.asList(
     *     new Document("$set", new Document("status", "processed")),
     *     new Document("$inc", new Document("processCount", 1))
     * );
     * Mono<Document> result = executor.findOneAndUpdate(filter, pipeline, options);
     * }</pre>
     *
     * @param filter the query filter to find the document; must not be null
     * @param objList collection of update operations to apply as a pipeline; must not be null or empty
     * @param options the options such as upsert, return document, projection, or sort;
     *                may be null to use default options
     * @return a Mono that emits the found document (before or after update based on options)
     * @throws IllegalArgumentException if filter or objList is null or empty
     */
    public Mono<Document> findOneAndUpdate(final Bson filter, final Collection<?> objList, final FindOneAndUpdateOptions options) {
        N.checkArgNotNull(filter, "filter");

        final List<Bson> updateToUse = toBson(objList);

        if (options == null) {
            return Mono.from(coll.findOneAndUpdate(filter, updateToUse));
        } else {
            return Mono.from(coll.findOneAndUpdate(filter, updateToUse, options));
        }
    }

    /**
     * Atomically finds and updates using multiple operations with options, returning as a specific type.
     *
     * <p>The most flexible find-and-update method combining pipeline updates, custom options,
     * and type-safe deserialization. Ideal for complex atomic operations with typed results.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndUpdateOptions options = new FindOneAndUpdateOptions()
     *     .returnDocument(ReturnDocument.AFTER)
     *     .projection(Projections.exclude("internalField"));
     * List<Bson> pipeline = buildComplexPipeline();
     * Mono<User> user = executor.findOneAndUpdate(filter, pipeline, options, User.class);
     * }</pre>
     *
     * @param <T> the type of the returned document
     * @param filter the query filter to find the document; must not be null
     * @param objList collection of update operations to apply as a pipeline; must not be null or empty
     * @param options the options such as upsert, return document, projection, or sort;
     *                may be null to use default options
     * @param rowType the class to deserialize the result into; must not be null
     * @return a Mono that emits the found document mapped to the specified type
     * @throws IllegalArgumentException if filter, objList, or rowType is null, or objList is empty
     */
    public <T> Mono<T> findOneAndUpdate(final Bson filter, final Collection<?> objList, final FindOneAndUpdateOptions options, final Class<T> rowType) {
        return findOneAndUpdate(filter, objList, options).map(toEntity(rowType));
    }

    /**
     * Atomically finds and replaces a single document.
     *
     * <p>Finds a document matching the filter and replaces it entirely with the replacement
     * document. The operation is atomic, preventing race conditions. The _id field is
     * preserved from the original document.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("_id", documentId);
     * Document replacement = new Document("name", "New Name").append("version", 2);
     * Mono<Document> result = executor.findOneAndReplace(filter, replacement);
     * }</pre>
     *
     * @param filter the query filter to find the document; must not be null
     * @param replacement the replacement document; must not be null
     * @return a Mono that emits the found document (before replacement by default)
     * @throws IllegalArgumentException if filter or replacement is null
     * @see #findOneAndReplace(Bson, Object, FindOneAndReplaceOptions)
     */
    public Mono<Document> findOneAndReplace(final Bson filter, final Object replacement) {
        return findOneAndReplace(filter, replacement, (FindOneAndReplaceOptions) null);
    }

    /**
     * Atomically finds and replaces a document, returning it as a specific type.
     *
     * <p>Finds and replaces a document atomically, returning the result as the specified type.
     * Combines atomic replacement with type-safe deserialization.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("username", "olduser");
     * User newUser = new User("newuser", "email@example.com");
     * Mono<User> oldUser = executor.findOneAndReplace(filter, newUser, User.class);
     * }</pre>
     *
     * @param <T> the type of the returned document
     * @param filter the query filter to find the document; must not be null
     * @param replacement the replacement document; must not be null
     * @param rowType the class to deserialize the result into; must not be null
     * @return a Mono that emits the found document mapped to the specified type
     * @throws IllegalArgumentException if any parameter is null
     * @see #findOneAndReplace(Bson, Object, FindOneAndReplaceOptions, Class)
     */
    public <T> Mono<T> findOneAndReplace(final Bson filter, final Object replacement, final Class<T> rowType) {
        return findOneAndReplace(filter, replacement, null, rowType);
    }

    /**
     * Atomically finds and replaces a document with custom options.
     *
     * <p>Finds and replaces a document with full control over the operation. Options include
     * returning the document before or after replacement, upsert behavior, projection,
     * sort order, and validation bypass.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndReplaceOptions options = new FindOneAndReplaceOptions()
     *     .returnDocument(ReturnDocument.AFTER)
     *     .upsert(true);
     * Bson filter = Filters.eq("_id", id);
     * Document replacement = buildReplacementDocument();
     * Mono<Document> result = executor.findOneAndReplace(filter, replacement, options);
     * }</pre>
     *
     * @param filter the query filter to find the document; must not be null
     * @param replacement the replacement document; must not be null
     * @param options the options such as upsert, return document, projection, or sort;
     *                may be null to use default options
     * @return a Mono that emits the found document (before or after replacement based on options)
     * @throws IllegalArgumentException if filter or replacement is null
     */
    public Mono<Document> findOneAndReplace(final Bson filter, final Object replacement, final FindOneAndReplaceOptions options) {
        if (options == null) {
            return Mono.from(coll.findOneAndReplace(filter, toDocument(replacement)));
        } else {
            return Mono.from(coll.findOneAndReplace(filter, toDocument(replacement), options));
        }
    }

    /**
     * Atomically finds and replaces with options, returning as a specific type.
     *
     * <p>The most flexible find-and-replace method combining custom options with type-safe
     * deserialization. Provides complete control over atomic replacement operations with
     * typed results.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndReplaceOptions options = new FindOneAndReplaceOptions()
     *     .returnDocument(ReturnDocument.AFTER)
     *     .projection(Projections.include("name", "email"));
     * User newUser = createUser();
     * Mono<User> result = executor.findOneAndReplace(filter, newUser, options, User.class);
     * }</pre>
     *
     * @param <T> the type of the returned document
     * @param filter the query filter to find the document; must not be null
     * @param replacement the replacement document; must not be null
     * @param options the options such as upsert, return document, projection, or sort;
     *                may be null to use default options
     * @param rowType the class to deserialize the result into; must not be null
     * @return a Mono that emits the found document mapped to the specified type
     * @throws IllegalArgumentException if filter, replacement, or rowType is null
     */
    public <T> Mono<T> findOneAndReplace(final Bson filter, final Object replacement, final FindOneAndReplaceOptions options, final Class<T> rowType) {
        return findOneAndReplace(filter, replacement, options).map(toEntity(rowType));
    }

    /**
     * Atomically finds and deletes a single document.
     *
     * <p>Finds a document matching the filter and deletes it atomically, returning the
     * deleted document. This operation is atomic, ensuring the document is retrieved
     * and deleted in a single operation without race conditions.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("status", "expired");
     * Mono<Document> deletedDoc = executor.findOneAndDelete(filter);
     * }</pre>
     *
     * @param filter the query filter to find the document to delete; must not be null
     * @return a Mono that emits the deleted document
     * @throws IllegalArgumentException if filter is null
     * @see #findOneAndDelete(Bson, FindOneAndDeleteOptions)
     */
    public Mono<Document> findOneAndDelete(final Bson filter) {
        return findOneAndDelete(filter, (FindOneAndDeleteOptions) null);
    }

    /**
     * Atomically finds and deletes a document, returning it as a specific type.
     *
     * <p>Finds and deletes a document atomically, returning the deleted document as the
     * specified type. Combines atomic deletion with type-safe deserialization.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("_id", userId);
     * Mono<User> deletedUser = executor.findOneAndDelete(filter, User.class);
     * }</pre>
     *
     * @param <T> the type of the returned document
     * @param filter the query filter to find the document to delete; must not be null
     * @param rowType the class to deserialize the deleted document into; must not be null
     * @return a Mono that emits the deleted document mapped to the specified type
     * @throws IllegalArgumentException if filter or rowType is null
     * @see #findOneAndDelete(Bson, FindOneAndDeleteOptions, Class)
     */
    public <T> Mono<T> findOneAndDelete(final Bson filter, final Class<T> rowType) {
        return findOneAndDelete(filter, null, rowType);
    }

    /**
     * Atomically finds and deletes a document with custom options.
     *
     * <p>Finds and deletes a document with control over the operation including projection,
     * sort order, and collation. This method is useful when you need to delete specific
     * documents based on sort criteria or with field projections.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndDeleteOptions options = new FindOneAndDeleteOptions()
     *     .sort(Sorts.ascending("priority"))
     *     .projection(Projections.include("_id", "name"));
     * Bson filter = Filters.eq("status", "pending");
     * Mono<Document> result = executor.findOneAndDelete(filter, options);
     * }</pre>
     *
     * @param filter the query filter to find the document to delete; must not be null
     * @param options the options such as projection, sort, or collation;
     *                may be null to use default options
     * @return a Mono that emits the deleted document
     * @throws IllegalArgumentException if filter is null
     */
    public Mono<Document> findOneAndDelete(final Bson filter, final FindOneAndDeleteOptions options) {
        if (options == null) {
            return Mono.from(coll.findOneAndDelete(filter));
        } else {
            return Mono.from(coll.findOneAndDelete(filter, options));
        }
    }

    /**
     * Atomically finds and deletes with options, returning as a specific type.
     *
     * <p>The most flexible find-and-delete method combining custom options with type-safe
     * deserialization. Provides complete control over atomic deletion operations with
     * typed results.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndDeleteOptions options = new FindOneAndDeleteOptions()
     *     .sort(Sorts.descending("created"))
     *     .projection(Projections.exclude("internalData"));
     * Mono<Order> oldestOrder = executor.findOneAndDelete(filter, options, Order.class);
     * }</pre>
     *
     * @param <T> the type of the returned document
     * @param filter the query filter to find the document to delete; must not be null
     * @param options the options such as projection, sort, or collation;
     *                may be null to use default options
     * @param rowType the class to deserialize the deleted document into; must not be null
     * @return a Mono that emits the deleted document mapped to the specified type
     * @throws IllegalArgumentException if filter or rowType is null
     */
    public <T> Mono<T> findOneAndDelete(final Bson filter, final FindOneAndDeleteOptions options, final Class<T> rowType) {
        return findOneAndDelete(filter, options).map(toEntity(rowType));
    }

    /**
     * Finds distinct values for a specified field.
     *
     * <p>Returns distinct values for the specified field across all documents in the collection.
     * This method is useful for getting unique values such as categories, tags, or statuses.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Flux<String> categories = executor.distinct("category", String.class);
     * }</pre>
     *
     * @param <T> the type of the distinct values
     * @param fieldName the field name to get distinct values for; must not be null
     * @param rowType the class type of the distinct values; must not be null
     * @return a Flux that emits the distinct values for the field
     * @throws IllegalArgumentException if fieldName or rowType is null
     * @see #distinct(String, Bson, Class)
     */
    public <T> Flux<T> distinct(final String fieldName, final Class<T> rowType) {
        return Flux.from(coll.distinct(fieldName, rowType));
    }

    /**
     * Finds distinct values for a field with a filter.
     *
     * <p>Returns distinct values for the specified field among documents matching the filter.
     * This method is useful for getting unique values within a subset of documents.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("status", "active");
     * Flux<String> activeUsernames = executor.distinct("username", filter, String.class);
     * }</pre>
     *
     * @param <T> the type of the distinct values
     * @param fieldName the field name to get distinct values for; must not be null
     * @param filter the query filter to apply before finding distinct values; must not be null
     * @param rowType the class type of the distinct values; must not be null
     * @return a Flux that emits the distinct values for the field within filtered documents
     * @throws IllegalArgumentException if any parameter is null
     */
    public <T> Flux<T> distinct(final String fieldName, final Bson filter, final Class<T> rowType) {
        return Flux.from(coll.distinct(fieldName, filter, rowType));
    }

    /**
     * Executes an aggregation pipeline.
     *
     * <p>Processes documents through an aggregation pipeline consisting of multiple stages.
     * Aggregation pipelines enable complex data processing including filtering, grouping,
     * sorting, reshaping, and computing derived values.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Bson> pipeline = Arrays.asList(
     *     Aggregates.match(Filters.eq("status", "completed")),
     *     Aggregates.group("$category", Accumulators.sum("total", "$amount"))
     * );
     * Flux<Document> results = executor.aggregate(pipeline);
     * }</pre>
     *
     * @param pipeline the aggregation pipeline stages; must not be null or empty
     * @return a Flux that emits the aggregation results as Documents
     * @throws IllegalArgumentException if pipeline is null or empty
     * @see #aggregate(List, Class)
     */
    public Flux<Document> aggregate(final List<? extends Bson> pipeline) {
        return aggregate(pipeline, Document.class);
    }

    /**
     * Executes an aggregation pipeline returning results as a specific type.
     *
     * <p>Processes documents through an aggregation pipeline and maps results to the specified
     * type. This method combines powerful aggregation capabilities with type-safe results.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Bson> pipeline = Arrays.asList(
     *     Aggregates.match(Filters.gte("date", startDate)),
     *     Aggregates.group("$userId", 
     *         Accumulators.sum("totalSpent", "$amount"),
     *         Accumulators.avg("avgSpent", "$amount"))
     * );
     * Flux<UserStats> stats = executor.aggregate(pipeline, UserStats.class);
     * }</pre>
     *
     * @param <T> the type of the aggregation results
     * @param pipeline the aggregation pipeline stages; must not be null or empty
     * @param rowType the class to deserialize results into; must not be null
     * @return a Flux that emits the aggregation results mapped to the specified type
     * @throws IllegalArgumentException if pipeline is null/empty or rowType is null
     */
    public <T> Flux<T> aggregate(final List<? extends Bson> pipeline, final Class<T> rowType) {
        return Flux.from(coll.aggregate(pipeline, Document.class)).map(toEntity(rowType));
    }

    /**
     * Groups documents by a single field.
     *
     * <p>Groups all documents in the collection by the specified field. This is a convenience
     * method for simple grouping operations. For more complex grouping, use the aggregate method.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Flux<Document> groupedByCategory = executor.groupBy("category");
     * }</pre>
     *
     * @param fieldName the field name to group by; must not be null
     * @return a Flux that emits documents grouped by the specified field
     * @throws IllegalArgumentException if fieldName is null
     * @see #groupBy(String, Class)
     */
    @Beta
    public Flux<Document> groupBy(final String fieldName) {
        return groupBy(fieldName, Document.class);
    }

    /**
     * Groups documents by a single field returning results as a specific type.
     *
     * <p>Groups documents by the specified field and maps results to the specified type.
     * This convenience method simplifies common grouping operations with typed results.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Flux<CategoryGroup> groups = executor.groupBy("category", CategoryGroup.class);
     * }</pre>
     *
     * @param <T> the type of the grouped results
     * @param fieldName the field name to group by; must not be null
     * @param rowType the class to deserialize results into; must not be null
     * @return a Flux that emits grouped documents mapped to the specified type
     * @throws IllegalArgumentException if fieldName or rowType is null
     * @see #groupBy(Collection, Class)
     */
    @Beta
    public <T> Flux<T> groupBy(final String fieldName, final Class<T> rowType) {
        return aggregate(N.asList(new Document(_$GROUP, new Document(MongoDBBase._ID, _$ + fieldName))), rowType);
    }

    /**
     * Groups documents by multiple fields.
     *
     * <p>Groups documents by multiple fields creating composite grouping keys. This is useful
     * for multi-dimensional analysis and reporting.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Flux<Document> grouped = executor.groupBy(Arrays.asList("category", "status"));
     * }</pre>
     *
     * @param fieldNames collection of field names to group by; must not be null or empty
     * @return a Flux that emits documents grouped by the specified fields
     * @throws IllegalArgumentException if fieldNames is null or empty
     * @see #groupBy(Collection, Class)
     */
    @Beta
    public Flux<Document> groupBy(final Collection<String> fieldNames) {
        return groupBy(fieldNames, Document.class);
    }

    /**
     * Groups documents by multiple fields returning results as a specific type.
     *
     * <p>Groups documents by multiple fields and maps results to the specified type.
     * Enables multi-dimensional grouping with type-safe results.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<String> fields = Arrays.asList("department", "role");
     * Flux<EmployeeGroup> groups = executor.groupBy(fields, EmployeeGroup.class);
     * }</pre>
     *
     * @param <T> the type of the grouped results
     * @param fieldNames collection of field names to group by; must not be null or empty
     * @param rowType the class to deserialize results into; must not be null
     * @return a Flux that emits grouped documents mapped to the specified type
     * @throws IllegalArgumentException if fieldNames is null/empty or rowType is null
     */
    @Beta
    public <T> Flux<T> groupBy(final Collection<String> fieldNames, final Class<T> rowType) {
        N.checkArgNotEmpty(fieldNames, "fieldNames");
        N.checkArgNotNull(rowType, "rowType");

        final Document groupFields = new Document();

        for (final String fieldName : fieldNames) {
            groupFields.put(fieldName, _$ + fieldName);
        }

        return aggregate(N.asList(new Document(_$GROUP, new Document(MongoDBBase._ID, groupFields))), rowType);
    }

    /**
     * Groups documents by a field and counts frequency.
     *
     * <p>Groups documents by the specified field and includes a count of documents in each group.
     * This is a common pattern for generating frequency distributions and summaries.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Flux<Document> categoryCounts = executor.groupByAndCount("category");
     * }</pre>
     *
     * @param fieldName the field name to group by; must not be null
     * @return a Flux that emits documents with _id (group key) and count fields
     * @throws IllegalArgumentException if fieldName is null
     * @see #groupByAndCount(String, Class)
     */
    @Beta
    public Flux<Document> groupByAndCount(final String fieldName) {
        return groupByAndCount(fieldName, Document.class);
    }

    /**
     * Groups documents by a field and counts frequency, returning as a specific type.
     *
     * <p>Groups documents by field, counts frequency, and maps results to the specified type.
     * Useful for generating typed frequency distributions and statistical summaries.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Flux<CategoryCount> counts = executor.groupByAndCount("category", CategoryCount.class);
     * }</pre>
     *
     * @param <T> the type of the grouped and counted results
     * @param fieldName the field name to group by; must not be null
     * @param rowType the class to deserialize results into; must not be null
     * @return a Flux that emits grouped and counted documents mapped to the specified type
     * @throws IllegalArgumentException if fieldName or rowType is null
     */
    @Beta
    public <T> Flux<T> groupByAndCount(final String fieldName, final Class<T> rowType) {
        return aggregate(N.asList(new Document(_$GROUP, new Document(MongoDBBase._ID, _$ + fieldName).append(_COUNT, new Document(_$SUM, 1)))), rowType);
    }

    /**
     * Groups documents by multiple fields and counts frequency.
     *
     * <p>Groups documents by multiple fields and includes a count of documents in each group.
     * Useful for multi-dimensional frequency analysis and cross-tabulation.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Flux<Document> counts = executor.groupByAndCount(Arrays.asList("status", "priority"));
     * }</pre>
     *
     * @param fieldNames collection of field names to group by; must not be null or empty
     * @return a Flux that emits documents with composite _id and count fields
     * @throws IllegalArgumentException if fieldNames is null or empty
     * @see #groupByAndCount(Collection, Class)
     */
    @Beta
    public Flux<Document> groupByAndCount(final Collection<String> fieldNames) {
        return groupByAndCount(fieldNames, Document.class);
    }

    /**
     * Groups by multiple fields with counts, returning as a specific type.
     *
     * <p>Groups documents by multiple fields, counts frequency, and maps results to the
     * specified type. Enables complex multi-dimensional analysis with type-safe results.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<String> dimensions = Arrays.asList("region", "product");
     * Flux<SalesCount> counts = executor.groupByAndCount(dimensions, SalesCount.class);
     * }</pre>
     *
     * @param <T> the type of the grouped and counted results
     * @param fieldNames collection of field names to group by; must not be null or empty
     * @param rowType the class to deserialize results into; must not be null
     * @return a Flux that emits grouped and counted documents mapped to the specified type
     * @throws IllegalArgumentException if fieldNames is null/empty or rowType is null
     */
    @Beta
    public <T> Flux<T> groupByAndCount(final Collection<String> fieldNames, final Class<T> rowType) {
        N.checkArgNotEmpty(fieldNames, "fieldNames");
        N.checkArgNotNull(rowType, "rowType");

        final Document groupFields = new Document();

        for (final String fieldName : fieldNames) {
            groupFields.put(fieldName, _$ + fieldName);
        }

        return aggregate(N.asList(new Document(_$GROUP, new Document(MongoDBBase._ID, groupFields).append(_COUNT, new Document(_$SUM, 1)))), rowType);
    }

    /**
     * Executes a map-reduce operation.
     *
     * <p>Performs a map-reduce operation using JavaScript functions. Map-reduce provides
     * a way to process large datasets with custom JavaScript logic. Note that aggregation
     * pipelines are generally preferred over map-reduce for better performance.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String mapFunction = "function() { emit(this.category, this.amount); }";
     * String reduceFunction = "function(key, values) { return Array.sum(values); }";
     * Flux<Document> results = executor.mapReduce(mapFunction, reduceFunction);
     * }</pre>
     *
     * @param mapFunction the JavaScript map function; must not be null
     * @param reduceFunction the JavaScript reduce function; must not be null
     * @return a Flux that emits the map-reduce results
     * @throws IllegalArgumentException if mapFunction or reduceFunction is null
     * @deprecated Map-reduce is deprecated in MongoDB 5.0+. Use aggregation pipelines instead.
     * @see #aggregate(List)
     */
    @Deprecated
    public Flux<Document> mapReduce(final String mapFunction, final String reduceFunction) {
        return mapReduce(mapFunction, reduceFunction, Document.class);
    }

    /**
     * Executes a map-reduce operation returning results as a specific type.
     *
     * <p>Performs a map-reduce operation and maps the results to the specified type.
     * This method combines powerful map-reduce capabilities with type-safe results.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String mapFunction = "function() { emit(this.category, this.amount); }";
     * String reduceFunction = "function(key, values) { return Array.sum(values); }";
     * Flux<CategoryTotal> totals = executor.mapReduce(mapFunction, reduceFunction, CategoryTotal.class);
     * }</pre>
     *
     * @param <T> the type of the map-reduce results
     * @param mapFunction the JavaScript map function; must not be null
     * @param reduceFunction the JavaScript reduce function; must not be null
     * @param rowType the class to deserialize results into; must not be null
     * @return a Flux that emits the map-reduce results mapped to the specified type
     * @throws IllegalArgumentException if any parameter is null
     * @deprecated Map-reduce is deprecated in MongoDB 5.0+. Use aggregation pipelines instead.
     */
    @Deprecated
    public <T> Flux<T> mapReduce(final String mapFunction, final String reduceFunction, final Class<T> rowType) {
        return Flux.from(coll.mapReduce(mapFunction, reduceFunction, Document.class)).map(toEntity(rowType));
    }

    //
    //    private String getCollectionName(final Class<?> cls) {
    //        final String collectionName = classCollectionMapper.get(cls);
    //
    //        if (N.isEmpty(collectionName)) {
    //            throw new IllegalArgumentException("No collection is mapped to class: " + cls);
    //        }
    //        return collectionName;
    //    }

    //
    //    private String getObjectId(Object obj) {
    //        String objectId = null;
    //
    //        try {
    //            objectId = N.convert(String.class, Beans.getPropValue(obj, "id"));
    //        } catch (Exception e) {
    //            // ignore
    //
    //            try {
    //                objectId = N.convert(String.class, Beans.getPropValue(obj, "objectId"));
    //            } catch (Exception e2) {
    //                // ignore
    //            }
    //        }
    //
    //        if (N.isEmpty(objectId)) {
    //            throw new IllegalArgumentException("Property value of 'id' or 'objectId' can't be null or empty for update or delete");
    //        }
    //
    //        return objectId;
    //    }

}
