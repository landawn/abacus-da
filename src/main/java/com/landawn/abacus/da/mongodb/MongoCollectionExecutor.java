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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.landawn.abacus.annotation.Beta;
import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.Fn;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.u.Nullable;
import com.landawn.abacus.util.u.Optional;
import com.landawn.abacus.util.u.OptionalBoolean;
import com.landawn.abacus.util.u.OptionalByte;
import com.landawn.abacus.util.u.OptionalChar;
import com.landawn.abacus.util.u.OptionalDouble;
import com.landawn.abacus.util.u.OptionalFloat;
import com.landawn.abacus.util.u.OptionalInt;
import com.landawn.abacus.util.u.OptionalLong;
import com.landawn.abacus.util.u.OptionalShort;
import com.landawn.abacus.util.function.Function;
import com.landawn.abacus.util.function.ToBooleanFunction;
import com.landawn.abacus.util.function.ToByteFunction;
import com.landawn.abacus.util.function.ToCharFunction;
import com.landawn.abacus.util.function.ToDoubleFunction;
import com.landawn.abacus.util.function.ToFloatFunction;
import com.landawn.abacus.util.function.ToIntFunction;
import com.landawn.abacus.util.function.ToLongFunction;
import com.landawn.abacus.util.function.ToShortFunction;
import com.landawn.abacus.util.stream.Stream;
import com.mongodb.BasicDBObject;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
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
import com.mongodb.client.result.UpdateResult;

/**
 * Comprehensive MongoDB collection executor providing high-level CRUD operations, aggregation pipelines, and advanced query capabilities.
 *
 * <p>This class serves as the primary interface for MongoDB collection operations, offering a rich set of methods
 * for document manipulation, querying, indexing, and bulk operations. It wraps the MongoDB Java driver's
 * {@code MongoCollection} to provide enhanced functionality, type safety, and convenient utility methods.</p>
 *
 * <h2>Key Features</h2>
 * <h3>Core Capabilities:</h3>
 * <ul>
 *   <li><strong>CRUD Operations:</strong> Complete create, read, update, delete functionality with flexible options</li>
 *   <li><strong>Query Builder Support:</strong> Integration with MongoDB's filter, projection, and sort builders</li>
 *   <li><strong>Aggregation Framework:</strong> Full support for MongoDB's aggregation pipeline operations</li>
 *   <li><strong>Bulk Operations:</strong> Efficient batch processing with ordered and unordered bulk writes</li>
 *   <li><strong>Change Streams:</strong> Real-time monitoring of collection changes with reactive support</li>
 *   <li><strong>GridFS Support:</strong> Large file storage and retrieval capabilities</li>
 *   <li><strong>Indexing:</strong> Index creation, management, and optimization utilities</li>
 *   <li><strong>Async Support:</strong> Non-blocking operations through integrated async executor</li>
 * </ul>
 *
 * <h3>Thread Safety:</h3>
 * <p>This class is thread-safe. All operations can be called concurrently from multiple threads without
 * external synchronization. The underlying MongoDB driver handles connection pooling and thread safety.</p>
 *
 * <h3>Performance Considerations:</h3>
 * <ul>
 *   <li>Use bulk operations for multiple document writes to minimize network round-trips</li>
 *   <li>Create appropriate indexes for frequently queried fields to optimize query performance</li>
 *   <li>Use projection to limit returned fields and reduce network bandwidth</li>
 *   <li>Consider read preferences for replica set deployments to balance load</li>
 *   <li>Use aggregation pipelines for complex data processing to leverage server-side computation</li>
 * </ul>
 *
 * <h3>Error Handling:</h3>
 * <p>All methods may throw {@code MongoException} and its subclasses for MongoDB-specific errors.
 * Common exceptions include:</p>
 * <ul>
 *   <li>{@code MongoWriteException} - For write operation failures</li>
 *   <li>{@code MongoQueryException} - For query syntax or execution errors</li>
 *   <li>{@code MongoTimeoutException} - For operation timeouts</li>
 *   <li>{@code MongoSecurityException} - For authentication/authorization failures</li>
 * </ul>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * MongoCollectionExecutor executor = mongoDB.collExecutor("users");
 * 
 * // Basic CRUD operations:
 * Document user = new Document("name", "John").append("age", 30);
 * executor.insertOne(user);
 * 
 * List<Document> results = executor.stream("{age: {$gte: 18}}").toList();
 * executor.updateMany("{active: false}", "{$set: {status: 'inactive'}}");
 * executor.deleteOne("{_id: ObjectId('...')}");
 * 
 * // Aggregation pipeline:
 * List<Document> aggregated = executor.aggregate(Arrays.asList(
 *     Aggregates.match(Filters.eq("status", "active")),
 *     Aggregates.group("$department", Accumulators.sum("count", 1)),
 *     Aggregates.sort(Sorts.descending("count"))
 * )).toList();
 * 
 * // Bulk operations:
 * List<WriteModel<Document>> operations = Arrays.asList(
 *     new InsertOneModel<>(doc1),
 *     new UpdateOneModel<>(filter, update),
 *     new DeleteOneModel<>(deleteFilter)
 * );
 * BulkWriteResult result = executor.bulkWrite(operations);
 * }</pre>
 *
 * @see MongoCollection
 * @see Document
 * @see Bson
 * @see com.mongodb.client.model.Filters
 * @see com.mongodb.client.model.Projections
 * @see com.mongodb.client.model.Sorts
 * @see com.mongodb.client.model.Updates
 * @see com.mongodb.client.model.Aggregates
 * @see com.mongodb.client.model.Indexes
 * @see <a href="https://www.mongodb.com/docs/drivers/java/sync/current/fundamentals/builders/">Simplify your Code with Builders</a>
 * @see <a href="https://www.mongodb.com/docs/drivers/java/sync/current/">MongoDB Java Driver</a>
 */
public final class MongoCollectionExecutor {

    static final String _$ = "$";

    static final String _$SET = "$set";

    static final String _$GROUP = "$group";

    static final String _$SUM = "$sum";

    static final String _COUNT = "count";

    private final MongoCollection<Document> coll;

    private final AsyncMongoCollectionExecutor asyncCollExecutor;

    MongoCollectionExecutor(final MongoCollection<Document> coll, final AsyncExecutor asyncExecutor) {
        this.coll = coll;
        asyncCollExecutor = new AsyncMongoCollectionExecutor(this, asyncExecutor);
    }

    /**
     * Returns the underlying MongoDB collection instance for advanced operations.
     *
     * <p>This method provides direct access to the native MongoDB Java driver's {@code MongoCollection}
     * for operations not exposed by this executor or when you need fine-grained control over
     * MongoDB driver features like read preferences, write concerns, or custom codecs.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollection<Document> collection = executor.coll();
     * collection.withWriteConcern(WriteConcern.MAJORITY).insertOne(document);
     * }</pre>
     *
     * @return the underlying MongoCollection instance
     * @see MongoCollection
     */
    public MongoCollection<Document> coll() {
        return coll;
    }

    /**
     * Returns an asynchronous version of this executor for non-blocking operations.
     *
     * <p>The async executor provides the same functionality as this synchronous executor but
     * returns {@code CompletableFuture} instances for all operations, enabling non-blocking
     * execution and reactive programming patterns.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncMongoCollectionExecutor asyncExecutor = executor.async();
     * CompletableFuture<List<Document>> future = asyncExecutor.find("{status: 'active'}");
     * }</pre>
     *
     * @return an AsyncMongoCollectionExecutor for asynchronous operations
     * @see java.util.concurrent.CompletableFuture
     */
    public AsyncMongoCollectionExecutor async() {
        return asyncCollExecutor;
    }

    /**
     * Checks if a document exists in the collection by its ObjectId string representation (blocking operation).
     *
     * <p>This method performs an efficient existence check using MongoDB's count operation
     * with a limit of 1. The string must be a valid 24-character hexadecimal ObjectId.</p>
     *
     * <p><b>Note:</b> This method performs a blocking operation. For non-blocking operations, use {@link #async()}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * if (executor.exists("507f1f77bcf86cd799439011")) {
     *     executor.updateOne("507f1f77bcf86cd799439011", "{$set: {lastSeen: new Date()}}");
     * }
     * }</pre>
     *
     * @param objectId the string representation of the ObjectId to check for existence
     * @return {@code true} if a document with the specified ObjectId exists, {@code false} otherwise
     * @throws IllegalArgumentException if objectId is null, empty, or invalid format
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #exists(ObjectId)
     * @see #exists(Bson)
     * @see #async()
     */
    public boolean exists(final String objectId) {
        return exists(createObjectId(objectId));
    }

    /**
     * Checks if a document exists in the collection by its ObjectId.
     *
     * <p>This method provides efficient document existence checking using the native ObjectId type.
     * It performs a count operation with a limit of 1 to minimize resource usage.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId oid = new ObjectId();
     * boolean exists = executor.exists(oid);
     * }</pre>
     *
     * @param objectId the ObjectId to check for existence
     * @return {@code true} if a document with the specified ObjectId exists, {@code false} otherwise
     * @throws IllegalArgumentException if objectId is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see ObjectId
     * @see #exists(String)
     */
    public boolean exists(final ObjectId objectId) {
        return exists(MongoDBBase.objectId2Filter(objectId));
    }

    /**
     * Checks if any documents exist in the collection matching the specified filter.
     *
     * <p>This method performs an efficient existence check using a count operation with a limit of 1.
     * It's more efficient than using {@code find().limit(1)} when you only need to know if matching
     * documents exist without retrieving them.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * boolean hasActiveUsers = executor.exists(Filters.eq("status", "active"));
     * boolean hasRecentUsers = executor.exists("{createdAt: {$gte: ISODate('2023-01-01')}}");
     * }</pre>
     *
     * @param filter the query filter to match documents against (null for all documents)
     * @return {@code true} if any documents match the filter, {@code false} otherwise
     * @throws com.mongodb.MongoException if the database operation fails
     * @see com.mongodb.client.model.Filters
     */
    public boolean exists(final Bson filter) {
        return count(filter, new CountOptions().limit(1)) > 0;
    }

    /**
     * Returns the total number of documents in the collection (blocking operation).
     *
     * <p>This method counts all documents in the collection without applying any filters.
     * For large collections, consider using {@link #estimatedDocumentCount()} for better
     * performance when exact counts are not required.</p>
     *
     * <p><b>Note:</b> This method performs a blocking operation. For non-blocking operations, use {@link #async()}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long totalUsers = executor.count();
     * System.out.println("Total users: " + totalUsers);
     * }</pre>
     *
     * @return the total number of documents in the collection
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #count(Bson)
     * @see #estimatedDocumentCount()
     * @see #async()
     */
    public long count() {
        return coll.countDocuments();
    }

    /**
     * Returns the number of documents in the collection matching the specified filter.
     *
     * <p>This method performs a precise count of documents matching the given filter criteria.
     * The count operation respects any read preference settings configured on the collection.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long activeUsers = executor.count(Filters.eq("status", "active"));
     * long adultUsers = executor.count("{age: {$gte: 18}}");
     * }</pre>
     *
     * @param filter the query filter to count matching documents (null for all documents)
     * @return the number of documents matching the filter
     * @throws com.mongodb.MongoException if the database operation fails
     * @see com.mongodb.client.model.Filters
     * @see #count(Bson, CountOptions)
     */
    public long count(final Bson filter) {
        return coll.countDocuments(filter);
    }

    /**
     * Returns the number of documents matching the filter with additional count options.
     *
     * <p>This method provides fine-grained control over the count operation through {@code CountOptions},
     * allowing specification of limits, skips, hints, and other count-specific parameters.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CountOptions options = new CountOptions().limit(1000);
     * long limitedCount = executor.count(Filters.exists("email"), options);
     * }</pre>
     *
     * @param filter the query filter to count matching documents (null for all documents)
     * @param options additional options for the count operation (null uses defaults)
     * @return the number of documents matching the filter within the specified constraints
     * @throws com.mongodb.MongoException if the database operation fails
     * @see CountOptions
     * @see com.mongodb.client.model.Filters
     */
    public long count(final Bson filter, final CountOptions options) {
        if (options == null) {
            return coll.countDocuments(filter);
        } else {
            return coll.countDocuments(filter, options);
        }
    }

    /**
     * Returns an estimate of the number of documents in the collection based on collection metadata.
     *
     * <p>This method provides a fast approximation of document count using collection statistics
     * rather than scanning all documents. It's significantly faster than {@code count()} for large
     * collections but may be less accurate, especially after recent write operations.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long approxUserCount = executor.estimatedDocumentCount();
     * System.out.println("Approximately " + approxUserCount + " users");
     * }</pre>
     *
     * @return an estimated count of documents in the collection
     * @throws com.mongodb.MongoException if the database operation fails
     * @see MongoCollection#estimatedDocumentCount()
     * @see #count()
     */
    public long estimatedDocumentCount() {
        return coll.estimatedDocumentCount();
    }

    /**
     * Returns an estimated document count with additional options for controlling the estimation process.
     *
     * <p>This method allows customization of the estimation operation through {@code EstimatedDocumentCountOptions},
     * including timeouts and read preferences for the metadata query.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * EstimatedDocumentCountOptions options = new EstimatedDocumentCountOptions()
     *     .maxTime(5, TimeUnit.SECONDS);
     * long estimate = executor.estimatedDocumentCount(options);
     * }</pre>
     *
     * @param options configuration options for the estimation operation
     * @return an estimated count of documents in the collection
     * @throws IllegalArgumentException if options is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see EstimatedDocumentCountOptions
     * @see MongoCollection#estimatedDocumentCount(EstimatedDocumentCountOptions)
     */
    public long estimatedDocumentCount(final EstimatedDocumentCountOptions options) {
        return coll.estimatedDocumentCount(options);
    }

    /**
     * Retrieves a document by its ObjectId string representation.
     *
     * <p>This method performs a find operation using the provided ObjectId string to locate
     * a single document. The ObjectId string must be a valid 24-character hexadecimal representation.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Optional<Document> user = executor.get("507f1f77bcf86cd799439011");
     * user.ifPresent(doc -> System.out.println("Found user: " + doc.getString("name")));
     * }</pre>
     *
     * @param objectId the string representation of the ObjectId to search for
     * @return an Optional containing the document if found, or empty if not found
     * @throws IllegalArgumentException if objectId is null, empty, or invalid format
     * @throws com.mongodb.MongoException if the database operation fails
     * @see Optional
     * @see #get(ObjectId)
     */
    public Optional<Document> get(final String objectId) {
        return get(createObjectId(objectId));
    }

    /**
     * Retrieves a document by its ObjectId.
     *
     * <p>This method performs a find operation using the native ObjectId type to locate
     * a single document in the collection efficiently.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId id = new ObjectId();
     * Optional<Document> result = executor.get(id);
     * }</pre>
     *
     * @param objectId the ObjectId to search for
     * @return an Optional containing the document if found, or empty if not found
     * @throws IllegalArgumentException if objectId is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see ObjectId
     * @see Optional
     */
    public Optional<Document> get(final ObjectId objectId) {
        return get(objectId, Document.class);
    }

    /**
     * Retrieves and converts a document by its ObjectId string with automatic type conversion.
     *
     * <p>This method combines document retrieval with automatic conversion to the specified target type.
     * It supports entity classes, Map types, and primitive types based on document structure.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Optional<User> user = executor.get("507f1f77bcf86cd799439011", User.class);
     * user.ifPresent(u -> System.out.println("User name: " + u.getName()));
     * }</pre>
     *
     * @param <T> the target type for the retrieved document
     * @param objectId the string representation of the ObjectId to search for
     * @param rowType the Class representing the target type for conversion
     * @return an Optional containing the converted object if found, or empty if not found
     * @throws IllegalArgumentException if objectId or rowType is null, or if objectId format is invalid
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #get(ObjectId, Class)
     */
    public <T> Optional<T> get(final String objectId, final Class<T> rowType) {
        return get(createObjectId(objectId), rowType);
    }

    /**
     * Retrieves and converts a document by its ObjectId with automatic type conversion.
     *
     * <p>This method performs efficient document retrieval using the native ObjectId and converts
     * the result to the specified target type. It supports entity classes, collections, and primitive types.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId id = new ObjectId();
     * Optional<User> user = executor.get(id, User.class);
     * }</pre>
     *
     * @param <T> the target type for the retrieved document
     * @param objectId the ObjectId to search for
     * @param rowType the Class representing the target type for conversion
     * @return an Optional containing the converted object if found, or empty if not found
     * @throws IllegalArgumentException if objectId or rowType is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #get(ObjectId, Collection, Class)
     */
    public <T> Optional<T> get(final ObjectId objectId, final Class<T> rowType) {
        return get(objectId, null, rowType);
    }

    /**
     * Retrieves a document with field projection by ObjectId string and converts to the specified type.
     *
     * <p>This method combines document retrieval with field projection and type conversion. Only the specified
     * fields are retrieved from the database, reducing network bandwidth and improving performance for
     * documents with many fields.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("name", "email", "createdAt");
     * Optional<User> partialUser = executor.get("507f1f77bcf86cd799439011", fields, User.class);
     * }</pre>
     *
     * @param <T> the target type for the retrieved document
     * @param objectId the string representation of the ObjectId to search for
     * @param selectPropNames collection of field names to include in the projection (null for all fields)
     * @param rowType the Class representing the target type for conversion
     * @return an Optional containing the converted object with projected fields, or empty if not found
     * @throws IllegalArgumentException if objectId or rowType is null, or objectId format is invalid
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #get(ObjectId, Collection, Class)
     */
    public <T> Optional<T> get(final String objectId, final Collection<String> selectPropNames, final Class<T> rowType) {
        return get(createObjectId(objectId), selectPropNames, rowType);
    }

    /**
     * Retrieves a document with field projection by ObjectId and converts to the specified type.
     *
     * <p>This method provides efficient document retrieval with projection using the native ObjectId.
     * Field projection reduces the amount of data transferred and can significantly improve performance
     * when working with documents containing many fields or large embedded objects.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Set.of("name", "email");
     * Optional<User> user = executor.get(id, fields, User.class);
     * }</pre>
     *
     * @param <T> the target type for the retrieved document
     * @param objectId the ObjectId to search for
     * @param selectPropNames collection of field names to include (null includes all fields)
     * @param rowType the Class representing the target type for conversion
     * @return an Optional containing the converted object with only the specified fields, or empty if not found
     * @throws IllegalArgumentException if objectId or rowType is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see com.mongodb.client.model.Projections
     */
    public <T> Optional<T> get(final ObjectId objectId, final Collection<String> selectPropNames, final Class<T> rowType) {
        return findFirst(selectPropNames, MongoDBBase.objectId2Filter(objectId), null, rowType);
    }

    /**
     * Retrieves a document by its ObjectId string representation, returning null if not found.
     *
     * <p>This method is the historical nullable counterpart to {@link #get(String)}.
     * It returns {@code null} instead of an empty Optional when the document is not found.
     * The {@code gett} name is preserved for API compatibility; prefer {@link #get(String)}
     * when Optional-based handling is clearer.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Document user = executor.gett("507f1f77bcf86cd799439011");
     * if (user != null) {
     *     String name = user.getString("name");
     * }
     * }</pre>
     *
     * @param objectId the string representation of the ObjectId (24 hex characters)
     * @return the matching document, or {@code null} if not found
     * @throws IllegalArgumentException if objectId is null, empty, or invalid format
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #get(String)
     * @see #gett(ObjectId)
     */
    public Document gett(final String objectId) {
        return gett(createObjectId(objectId));
    }

    /**
     * Retrieves a document by its ObjectId, returning null if not found.
     *
     * <p>This method is a convenience variant of {@link #get(ObjectId)} that returns {@code null} instead
     * of an empty Optional when the document is not found. Use this method when null checks are preferred
     * over Optional handling.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId id = new ObjectId();
     * Document doc = executor.gett(id);
     * if (doc != null) {
     *     processDocument(doc);
     * }
     * }</pre>
     *
     * @param objectId the ObjectId to search for
     * @return the matching document, or {@code null} if not found
     * @throws IllegalArgumentException if objectId is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #get(ObjectId)
     * @see #gett(String)
     */
    public Document gett(final ObjectId objectId) {
        return gett(objectId, Document.class);
    }

    /**
     * Retrieves and converts a document by its ObjectId string, returning null if not found.
     *
     * <p>This method combines document retrieval with automatic type conversion, returning {@code null}
     * instead of an empty Optional when no matching document exists. It supports entity classes,
     * Map types, and primitive types based on the document structure.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User user = executor.gett("507f1f77bcf86cd799439011", User.class);
     * if (user != null) {
     *     sendWelcomeEmail(user.getEmail());
     * }
     * }</pre>
     *
     * @param <T> the target type for the retrieved document
     * @param objectId the string representation of the ObjectId (24 hex characters)
     * @param rowType the Class representing the target type for conversion
     * @return the matching document converted to the specified type, or {@code null} if not found
     * @throws IllegalArgumentException if objectId or rowType is null, or if objectId format is invalid
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #get(String, Class)
     * @see #gett(ObjectId, Class)
     */
    public <T> T gett(final String objectId, final Class<T> rowType) {
        return gett(createObjectId(objectId), rowType);
    }

    /**
     * Retrieves and converts a document by its ObjectId, returning null if not found.
     *
     * <p>This method performs efficient document retrieval using the native ObjectId and converts
     * the result to the specified target type. Returns {@code null} instead of an empty Optional
     * when no matching document exists.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId id = new ObjectId();
     * User user = executor.gett(id, User.class);
     * if (user != null) {
     *     System.out.println("Found: " + user.getName());
     * }
     * }</pre>
     *
     * @param <T> the target type for the retrieved document
     * @param objectId the ObjectId to search for
     * @param rowType the Class representing the target type for conversion
     * @return the matching document converted to the specified type, or {@code null} if not found
     * @throws IllegalArgumentException if objectId or rowType is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #get(ObjectId, Class)
     * @see #gett(String, Class)
     */
    public <T> T gett(final ObjectId objectId, final Class<T> rowType) {
        return gett(objectId, null, rowType);
    }

    /**
     * Retrieves a document with field projection by ObjectId string, returning null if not found.
     *
     * <p>This method combines document retrieval with field projection and type conversion, returning
     * {@code null} instead of an empty Optional when no matching document exists. Only the specified
     * fields are retrieved from the database, reducing network bandwidth and improving performance.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("name", "email", "department");
     * User partialUser = executor.gett("507f1f77bcf86cd799439011", fields, User.class);
     * if (partialUser != null) {
     *     System.out.println("User department: " + partialUser.getDepartment());
     * }
     * }</pre>
     *
     * @param <T> the target type for the retrieved document
     * @param objectId the string representation of the ObjectId (24 hex characters)
     * @param selectPropNames collection of field names to include in the projection (null for all fields)
     * @param rowType the Class representing the target type for conversion
     * @return the matching document with projected fields converted to the specified type, or {@code null} if not found
     * @throws IllegalArgumentException if objectId or rowType is null, or if objectId format is invalid
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #get(String, Collection, Class)
     * @see #gett(ObjectId, Collection, Class)
     */
    public <T> T gett(final String objectId, final Collection<String> selectPropNames, final Class<T> rowType) {
        return gett(createObjectId(objectId), selectPropNames, rowType);
    }

    /**
     * Retrieves a document with field projection by ObjectId, returning null if not found.
     *
     * <p>This method provides efficient document retrieval with projection using the native ObjectId,
     * returning {@code null} instead of an empty Optional when no matching document exists. Field
     * projection reduces the amount of data transferred and can significantly improve performance
     * when working with documents containing many fields or large embedded objects.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId id = new ObjectId();
     * Collection<String> fields = Set.of("name", "email");
     * User user = executor.gett(id, fields, User.class);
     * if (user != null) {
     *     sendEmail(user.getEmail());
     * }
     * }</pre>
     *
     * @param <T> the target type for the retrieved document
     * @param objectId the ObjectId to search for
     * @param selectPropNames collection of field names to include in the projection (null for all fields)
     * @param rowType the Class representing the target type for conversion
     * @return the matching document with projected fields converted to the specified type, or {@code null} if not found
     * @throws IllegalArgumentException if objectId or rowType is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #get(ObjectId, Collection, Class)
     * @see com.mongodb.client.model.Projections
     */
    public <T> T gett(final ObjectId objectId, final Collection<String> selectPropNames, final Class<T> rowType) {
        return findFirst(selectPropNames, MongoDBBase.objectId2Filter(objectId), null, rowType).orElse(null);
    }

    /**
     * Finds the first document matching the specified filter criteria.
     *
     * <p>This method performs a find operation that returns at most one document matching the given
     * filter. It's equivalent to a find operation with a limit of 1. The result is wrapped in an
     * Optional to handle the case where no matching documents exist.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Optional<Document> user = executor.findFirst(Filters.eq("status", "active"));
     * user.ifPresent(doc -> processUser(doc));
     * }</pre>
     *
     * @param filter the query filter to match documents against (null for all documents)
     * @return an Optional containing the first matching document, or empty if none found
     * @throws com.mongodb.MongoException if the database operation fails
     * @see Optional
     * @see Document
     * @see com.mongodb.client.model.Filters
     */
    public Optional<Document> findFirst(final Bson filter) {
        return findFirst(filter, Document.class);
    }

    /**
     * Finds the first document matching the filter with automatic type conversion.
     *
     * <p>This method performs a find operation that returns at most one document matching the given
     * filter, automatically converting the result to the specified type. It supports entity classes,
     * Map types, and primitive types based on document structure.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Optional<User> user = executor.findFirst(Filters.eq("status", "active"), User.class);
     * user.ifPresent(u -> System.out.println("Found: " + u.getName()));
     * }</pre>
     *
     * @param <T> the target type for the retrieved document
     * @param filter the query filter to match documents against
     * @param rowType the Class representing the target type for conversion
     * @return an Optional containing the first matching converted object, or empty if none found
     * @throws IllegalArgumentException if filter or rowType is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #findFirst(Collection, Bson, Class)
     */
    public <T> Optional<T> findFirst(final Bson filter, final Class<T> rowType) {
        return findFirst(null, filter, rowType);
    }

    /**
     * Finds the first document matching the filter with field projection and type conversion.
     *
     * <p>This method combines document finding with field projection and automatic type conversion.
     * Only the specified fields are retrieved from the database, reducing network bandwidth and
     * improving performance. The result is converted to the specified target type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("name", "email", "status");
     * Optional<User> partialUser = executor.findFirst(fields, 
     *     Filters.eq("department", "Engineering"), User.class);
     * }</pre>
     *
     * @param <T> the target type for the retrieved document
     * @param selectPropNames collection of field names to include in the projection (null for all fields)
     * @param filter the query filter to match documents against
     * @param rowType the Class representing the target type for conversion
     * @return an Optional containing the first matching converted object with projected fields, or empty if none found
     * @throws IllegalArgumentException if filter or rowType is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see com.mongodb.client.model.Projections
     */
    public <T> Optional<T> findFirst(final Collection<String> selectPropNames, final Bson filter, final Class<T> rowType) {
        return findFirst(selectPropNames, filter, null, rowType);
    }

    /**
     * Finds the first document matching the filter with field projection, sorting, and type conversion.
     *
     * <p>This method provides comprehensive document finding with field projection, sorting, and automatic
     * type conversion. The result is sorted according to the specified criteria, then the first document
     * is retrieved with only the specified fields and converted to the target type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("name", "score", "createdAt");
     * Optional<Student> topStudent = executor.findFirst(fields,
     *     Filters.eq("active", true),
     *     Sorts.descending("score"),
     *     Student.class);
     * }</pre>
     *
     * @param <T> the target type for the retrieved document
     * @param selectPropNames collection of field names to include in the projection (null for all fields)
     * @param filter the query filter to match documents against
     * @param sort the sort criteria for ordering results (null for no sorting)
     * @param rowType the Class representing the target type for conversion
     * @return an Optional containing the first matching sorted converted object with projected fields, or empty if none found
     * @throws IllegalArgumentException if filter or rowType is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see com.mongodb.client.model.Projections
     * @see com.mongodb.client.model.Sorts
     */
    public <T> Optional<T> findFirst(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final Class<T> rowType) {
        final FindIterable<Document> findIterable = query(selectPropNames, filter, sort, 0, 1);

        final T result = toEntity(findIterable, rowType);

        return result == null ? (Optional<T>) Optional.empty() : Optional.of(result);
    }

    /**
     * Finds the first document matching the filter with BSON projection, sorting, and type conversion.
     *
     * <p>This method provides comprehensive document finding with BSON-based projection, sorting, and 
     * automatic type conversion. BSON projection allows for more advanced field selection including
     * computed fields and array operations.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson projection = Projections.fields(
     *     Projections.include("name", "score"),
     *     Projections.excludeId()
     * );
     * Optional<Student> topStudent = executor.findFirst(projection,
     *     Filters.eq("active", true),
     *     Sorts.descending("score"),
     *     Student.class);
     * }</pre>
     *
     * @param <T> the target type for the retrieved document
     * @param projection BSON projection specification for field selection (null for all fields)
     * @param filter the query filter to match documents against
     * @param sort the sort criteria for ordering results (null for no sorting)
     * @param rowType the Class representing the target type for conversion
     * @return an Optional containing the first matching sorted converted object with projected fields, or empty if none found
     * @throws IllegalArgumentException if filter or rowType is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see com.mongodb.client.model.Projections
     * @see com.mongodb.client.model.Sorts
     */
    public <T> Optional<T> findFirst(final Bson projection, final Bson filter, final Bson sort, final Class<T> rowType) {
        final FindIterable<Document> findIterable = executeQuery(projection, filter, sort, 0, 1);

        final T result = toEntity(findIterable, rowType);

        return result == null ? (Optional<T>) Optional.empty() : Optional.of(result);
    }

    /**
     * Retrieves all documents matching the specified filter as a list.
     *
     * <p>This method performs a find operation and returns all matching documents as a List of Document objects.
     * Use this method when you need to retrieve all matching documents at once. For large result sets,
     * consider using streaming operations or pagination to manage memory usage.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Document> activeUsers = executor.list(Filters.eq("status", "active"));
     * activeUsers.forEach(user -> processUser(user));
     * }</pre>
     *
     * @param filter the query filter to match documents against (null for all documents)
     * @return a List containing all matching documents (empty list if none found)
     * @throws com.mongodb.MongoException if the database operation fails
     * @see Document
     * @see #stream(Bson)
     * @see com.mongodb.client.model.Filters
     */
    public List<Document> list(final Bson filter) {
        return list(filter, Document.class);
    }

    /**
     * Retrieves all documents matching the filter with automatic type conversion.
     *
     * <p>This method performs a find operation and returns all matching documents as a List of the specified type.
     * Each document is automatically converted to the target type. This provides type safety and eliminates
     * manual conversion code.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<User> activeUsers = executor.list(Filters.eq("status", "active"), User.class);
     * activeUsers.forEach(user -> processUser(user));
     * }</pre>
     *
     * @param <T> the target type for the retrieved documents
     * @param filter the query filter to match documents against
     * @param rowType the Class representing the target type for conversion
     * @return a List containing all matching converted objects (empty list if none found)
     * @throws IllegalArgumentException if filter or rowType is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #list(Bson, int, int, Class)
     */
    public <T> List<T> list(final Bson filter, final Class<T> rowType) {
        return list(null, filter, rowType);
    }

    /**
     * Retrieves documents matching the filter with pagination and type conversion.
     *
     * <p>This method performs a find operation with pagination support, returning a specific range of
     * matching documents converted to the specified type. This is useful for implementing paginated
     * results and managing memory usage with large datasets.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Get second page of 20 active users:
     * List<User> page2Users = executor.list(
     *     Filters.eq("status", "active"), 20, 20, User.class);
     * }</pre>
     *
     * @param <T> the target type for the retrieved documents
     * @param filter the query filter to match documents against
     * @param offset the number of documents to skip (0-based)
     * @param count the maximum number of documents to return
     * @param rowType the Class representing the target type for conversion
     * @return a List containing the specified range of matching converted objects
     * @throws IllegalArgumentException if filter or rowType is null, or if offset/count is negative
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #list(Collection, Bson, int, int, Class)
     */
    public <T> List<T> list(final Bson filter, final int offset, final int count, final Class<T> rowType) {
        return list(null, filter, offset, count, rowType);
    }

    /**
     * Retrieves all documents matching the filter with field projection and type conversion.
     *
     * <p>This method performs a find operation with field projection, returning all matching documents
     * converted to the specified type. Only the selected properties are retrieved from the database,
     * reducing network bandwidth and memory usage while providing type safety.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("name", "email");
     * List<User> users = executor.list(fields, Filters.eq("status", "active"), User.class);
     * }</pre>
     *
     * @param <T> the target type for each document in the result list
     * @param selectPropNames collection of field names to include in projection (null for all fields)
     * @param filter BSON filter criteria to match documents (null for no filtering)
     * @param rowType the target type for conversion of each document
     * @return a list of all matching documents converted to the specified type
     * @throws IllegalArgumentException if rowType is null or unsupported
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #list(Collection, Bson, int, int, Class)
     * @see #list(Collection, Bson, Bson, Class)
     */
    public <T> List<T> list(final Collection<String> selectPropNames, final Bson filter, final Class<T> rowType) {
        return list(selectPropNames, filter, 0, Integer.MAX_VALUE, rowType);
    }

    /**
     * Retrieves a paginated list of documents with field projection, filtering, and type conversion.
     *
     * <p>This method combines field projection, filtering, and pagination to efficiently retrieve
     * a specific subset of matching documents. This is essential for implementing paginated UIs
     * while minimizing data transfer through projection.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("name", "email", "createdAt");
     * List<User> page2Users = executor.list(fields, Filters.eq("status", "active"), 20, 20, User.class);
     * }</pre>
     *
     * @param <T> the target type for each document in the result list
     * @param selectPropNames collection of field names to include in projection (null for all fields)
     * @param filter BSON filter criteria to match documents (null for no filtering)
     * @param offset number of documents to skip from the beginning (0 for first page)
     * @param count maximum number of documents to return
     * @param rowType the target type for conversion of each document
     * @return a paginated list of matching documents converted to the specified type
     * @throws IllegalArgumentException if rowType is null, offset is negative, or count is non-positive
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #list(Collection, Bson, Class)
     * @see #list(Collection, Bson, Bson, int, int, Class)
     */
    public <T> List<T> list(final Collection<String> selectPropNames, final Bson filter, final int offset, final int count, final Class<T> rowType) {
        return list(selectPropNames, filter, null, offset, count, rowType);
    }

    /**
     * Retrieves all documents matching the filter with field projection, sorting, and type conversion.
     *
     * <p>This method combines filtering, field projection, and sorting to retrieve all matching
     * documents in a predictable order with only the selected fields. The sorting ensures consistent
     * result ordering, which is important for UI display and pagination consistency.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("title", "author", "publishedAt");
     * Bson sort = Sorts.descending("publishedAt");
     * List<Article> articles = executor.list(fields, Filters.eq("status", "published"), sort, Article.class);
     * }</pre>
     *
     * @param <T> the target type for each document in the result list
     * @param selectPropNames collection of field names to include in projection (null for all fields)
     * @param filter BSON filter criteria to match documents (null for no filtering)
     * @param sort BSON sort specification for result ordering (null for natural order)
     * @param rowType the target type for conversion of each document
     * @return a sorted list of all matching documents converted to the specified type
     * @throws IllegalArgumentException if rowType is null or unsupported
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #list(Collection, Bson, Class)
     * @see #list(Collection, Bson, Bson, int, int, Class)
     * @see com.mongodb.client.model.Sorts
     */
    public <T> List<T> list(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final Class<T> rowType) {
        return list(selectPropNames, filter, sort, 0, Integer.MAX_VALUE, rowType);
    }

    /**
     * Retrieves a list of documents with field projection, filtering, sorting, and pagination support.
     *
     * <p>This method provides comprehensive query capabilities including field selection (projection),
     * filtering criteria, result ordering, and result pagination. It's ideal for retrieving specific
     * data subsets efficiently while minimizing network bandwidth and memory usage through projection.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("name", "email");
     * Bson sort = Sorts.ascending("name");
     * List<User> users = executor.list(fields, Filters.eq("status", "active"), sort, 0, 10, User.class);
     * }</pre>
     *
     * @param <T> the target type for each document in the result list
     * @param selectPropNames collection of field names to include in projection (null for all fields)
     * @param filter BSON filter criteria to match documents (null for no filtering)
     * @param sort BSON sort specification for result ordering (null for natural order)
     * @param offset number of documents to skip from the beginning of results (0 for no offset)
     * @param count maximum number of documents to return (Integer.MAX_VALUE for all matching)
     * @param rowType an entity class with getter/setter method, <code>Map.class</code> or basic single value type (Primitive/String/Date...)
     * @return a list of documents converted to the specified type, may be empty but never null
     * @throws IllegalArgumentException if rowType is null or unsupported
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #list(Bson, Bson, Bson, int, int, Class)
     * @see com.mongodb.client.model.Filters
     * @see com.mongodb.client.model.Sorts
     */
    public <T> List<T> list(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final int offset, final int count,
            final Class<T> rowType) {
        final FindIterable<Document> findIterable = query(selectPropNames, filter, sort, offset, count);

        return MongoDBBase.toList(findIterable, rowType);
    }

    /**
     * Retrieves all matching documents with BSON projection, filtering, and sorting.
     *
     * <p>This method retrieves all documents matching the specified criteria without pagination.
     * It uses BSON projection for more advanced field selection capabilities compared to simple
     * property name collections. This is suitable when you need all matching results or when
     * the result set is expected to be manageable in size.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson projection = Projections.include("name", "email");
     * Bson sort = Sorts.ascending("name");
     * List<User> allActiveUsers = executor.list(projection, Filters.eq("status", "active"), sort, User.class);
     * }</pre>
     *
     * @param <T> the target type for each document in the result list
     * @param projection BSON projection specification for field selection (null for all fields)
     * @param filter BSON filter criteria to match documents (null for no filtering)
     * @param sort BSON sort specification for result ordering (null for natural order)
     * @param rowType an entity class with getter/setter method, <code>Map.class</code> or basic single value type (Primitive/String/Date...)
     * @return a list of all matching documents converted to the specified type, may be empty but never null
     * @throws IllegalArgumentException if rowType is null or unsupported
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #list(Bson, Bson, Bson, int, int, Class)
     * @see com.mongodb.client.model.Projections
     */
    public <T> List<T> list(final Bson projection, final Bson filter, final Bson sort, final Class<T> rowType) {
        return list(projection, filter, sort, 0, Integer.MAX_VALUE, rowType);
    }

    /**
     * Retrieves a list of documents with BSON projection, filtering, sorting, and pagination support.
     *
     * <p>This method provides the most comprehensive query capabilities using BSON objects for
     * projection, filtering, and sorting. It offers advanced projection features like computed fields,
     * array slicing, and conditional inclusions that are not available with simple field name collections.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson projection = Projections.fields(
     *     Projections.include("name", "email"),
     *     Projections.slice("tags", 5)
     * );
     * Bson filter = Filters.and(
     *     Filters.eq("status", "active"),
     *     Filters.gte("lastLogin", thirtyDaysAgo)
     * );
     * List<User> recentUsers = executor.list(projection, filter, Sorts.descending("lastLogin"), 0, 50, User.class);
     * }</pre>
     *
     * @param <T> the target type for each document in the result list
     * @param projection BSON projection specification for advanced field selection (null for all fields)
     * @param filter BSON filter criteria to match documents (null for no filtering)
     * @param sort BSON sort specification for result ordering (null for natural order)
     * @param offset number of documents to skip from the beginning of results (0 for no offset)
     * @param count maximum number of documents to return (Integer.MAX_VALUE for all matching)
     * @param rowType an entity class with getter/setter method, <code>Map.class</code> or basic single value type (Primitive/String/Date...)
     * @return a list of documents converted to the specified type, may be empty but never null
     * @throws IllegalArgumentException if rowType is null or unsupported
     * @throws com.mongodb.MongoException if the database operation fails
     * @see com.mongodb.client.model.Projections
     * @see com.mongodb.client.model.Filters
     * @see com.mongodb.client.model.Sorts
     */
    public <T> List<T> list(final Bson projection, final Bson filter, final Bson sort, final int offset, final int count, final Class<T> rowType) {
        final FindIterable<Document> findIterable = executeQuery(projection, filter, sort, offset, count);

        return MongoDBBase.toList(findIterable, rowType);
    }

    /**
     * Queries for a single boolean value from a specific field matching the given filter.
     *
     * <p>This convenience method retrieves a boolean field value from the first document
     * matching the filter criteria. It's useful for checking flags, status fields, or
     * other boolean properties in documents. Returns OptionalBoolean.empty() if no
     * matching document is found or the field value is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OptionalBoolean isActive = executor.queryForBoolean("active", Filters.eq("userId", "user123"));
     * if (isActive.isPresent() && isActive.get()) {
     *     // User is active
     * }
     * }</pre>
     *
     * @param propName the name of the field to retrieve the boolean value from
     * @param filter BSON filter criteria to match documents (null matches all)
     * @return an OptionalBoolean containing the boolean value if found, empty otherwise
     * @throws IllegalArgumentException if propName is null or empty
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #queryForSingleResult(String, Bson, Class)
     */
    @Beta
    public OptionalBoolean queryForBoolean(final String propName, final Bson filter) {
        return queryForSingleResult(propName, filter, Boolean.class).mapToBoolean(ToBooleanFunction.UNBOX);
    }

    /**
     * Queries for a single character value from a specific field matching the given filter.
     *
     * <p>This convenience method retrieves a character field value from the first document
     * matching the filter criteria. It's useful for status codes, grade letters, or other
     * single-character fields. Returns OptionalChar.empty() if no matching document is
     * found or the field value is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OptionalChar grade = executor.queryForChar("grade", Filters.eq("studentId", "S12345"));
     * if (grade.isPresent()) {
     *     char letterGrade = grade.get();   // 'A', 'B', 'C', etc.
     * }
     * }</pre>
     *
     * @param propName the name of the field to retrieve the character value from
     * @param filter BSON filter criteria to match documents (null matches all)
     * @return an OptionalChar containing the character value if found, empty otherwise
     * @throws IllegalArgumentException if propName is null or empty
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #queryForSingleResult(String, Bson, Class)
     */
    @Beta
    public OptionalChar queryForChar(final String propName, final Bson filter) {
        return queryForSingleResult(propName, filter, Character.class).mapToChar(ToCharFunction.UNBOX);
    }

    /**
     * Queries for a single byte value from a specific field matching the given filter.
     *
     * <p>This convenience method retrieves a byte field value from the first document
     * matching the filter criteria. It's useful for small numeric values, flags, or
     * compact data storage. Returns OptionalByte.empty() if no matching document is
     * found or the field value is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OptionalByte priority = executor.queryForByte("priority", Filters.eq("taskId", "TASK001"));
     * if (priority.isPresent()) {
     *     byte level = priority.get();
     * }
     * }</pre>
     *
     * @param propName the name of the field to retrieve the byte value from
     * @param filter BSON filter criteria to match documents (null matches all)
     * @return an OptionalByte containing the byte value if found, empty otherwise
     * @throws IllegalArgumentException if propName is null or empty
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #queryForSingleResult(String, Bson, Class)
     */
    @Beta
    public OptionalByte queryForByte(final String propName, final Bson filter) {
        return queryForSingleResult(propName, filter, Byte.class).mapToByte(ToByteFunction.UNBOX);
    }

    /**
     * Queries for a single short value from a specific field matching the given filter.
     *
     * <p>This convenience method retrieves a short field value from the first document
     * matching the filter criteria. It's useful for small numeric values, counters, or
     * compact integer storage. Returns OptionalShort.empty() if no matching document is
     * found or the field value is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OptionalShort stock = executor.queryForShort("stockCount", Filters.eq("productId", "P12345"));
     * if (stock.isPresent() && stock.get() > 0) {
     *     // Product is in stock
     * }
     * }</pre>
     *
     * @param propName the name of the field to retrieve the short value from
     * @param filter BSON filter criteria to match documents (null matches all)
     * @return an OptionalShort containing the short value if found, empty otherwise
     * @throws IllegalArgumentException if propName is null or empty
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #queryForSingleResult(String, Bson, Class)
     */
    @Beta
    public OptionalShort queryForShort(final String propName, final Bson filter) {
        return queryForSingleResult(propName, filter, Short.class).mapToShort(ToShortFunction.UNBOX);
    }

    /**
     * Queries for a single integer value from a specific field matching the given filter.
     *
     * <p>This convenience method retrieves an integer field value from the first document
     * matching the filter criteria. It's useful for counts, IDs, ages, quantities, or any
     * numeric integer data. Returns OptionalInt.empty() if no matching document is found
     * or the field value is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OptionalInt age = executor.queryForInt("age", Filters.eq("userId", "user123"));
     * if (age.isPresent()) {
     *     int userAge = age.get();
     * }
     * }</pre>
     *
     * @param propName the name of the field to retrieve the integer value from
     * @param filter BSON filter criteria to match documents (null matches all)
     * @return an OptionalInt containing the integer value if found, empty otherwise
     * @throws IllegalArgumentException if propName is null or empty
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #queryForSingleResult(String, Bson, Class)
     */
    @Beta
    public OptionalInt queryForInt(final String propName, final Bson filter) {
        return queryForSingleResult(propName, filter, Integer.class).mapToInt(ToIntFunction.UNBOX);
    }

    /**
     * Queries for a single long value from a specific field matching the given filter.
     *
     * <p>This convenience method retrieves a long field value from the first document
     * matching the filter criteria. It's useful for timestamps, large IDs, file sizes,
     * or any numeric data requiring 64-bit precision. Returns OptionalLong.empty() if
     * no matching document is found or the field value is null.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Get a file's size in bytes:
     * OptionalLong fileSize = executor.queryForLong("sizeBytes", Filters.eq("fileId", "FILE001"));
     * if (fileSize.isPresent()) {
     *     long size = fileSize.get();
     * }
     * 
     * // Get last modified timestamp:
     * OptionalLong lastModified = executor.queryForLong("lastModified", Filters.eq("documentId", "DOC123"));
     * }</pre>
     *
     * @param propName the name of the field to retrieve the long value from
     * @param filter BSON filter criteria to match documents (null matches all)
     * @return an OptionalLong containing the long value if found, empty otherwise
     * @throws IllegalArgumentException if propName is null or empty
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #queryForSingleResult(String, Bson, Class)
     */
    @Beta
    public OptionalLong queryForLong(final String propName, final Bson filter) {
        return queryForSingleResult(propName, filter, Long.class).mapToLong(ToLongFunction.UNBOX);
    }

    /**
     * Queries for a single float value from a specific field matching the given filter.
     *
     * <p>This convenience method retrieves a float field value from the first document
     * matching the filter criteria. It's useful for prices, ratings, percentages, or any
     * floating-point numeric data with single precision. Returns OptionalFloat.empty()
     * if no matching document is found or the field value is null.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Get a product's rating:
     * OptionalFloat rating = executor.queryForFloat("rating", Filters.eq("productId", "P12345"));
     * if (rating.isPresent()) {
     *     float productRating = rating.get();   // e.g., 4.5
     * }
     * 
     * // Get discount percentage:
     * OptionalFloat discount = executor.queryForFloat("discountPercent", Filters.eq("promoCode", "SAVE20"));
     * }</pre>
     *
     * @param propName the name of the field to retrieve the float value from
     * @param filter BSON filter criteria to match documents (null matches all)
     * @return an OptionalFloat containing the float value if found, empty otherwise
     * @throws IllegalArgumentException if propName is null or empty
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #queryForSingleResult(String, Bson, Class)
     */
    @Beta
    public OptionalFloat queryForFloat(final String propName, final Bson filter) {
        return queryForSingleResult(propName, filter, Float.class).mapToFloat(ToFloatFunction.UNBOX);
    }

    /**
     * Queries for a single double value from a specific field matching the given filter.
     *
     * <p>This convenience method retrieves a double field value from the first document
     * matching the filter criteria. It's useful for precise calculations, coordinates,
     * financial amounts, or any floating-point numeric data requiring double precision.
     * Returns OptionalDouble.empty() if no matching document is found or the field value is null.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Get account balance with high precision:
     * OptionalDouble balance = executor.queryForDouble("balance", Filters.eq("accountId", "ACC123"));
     * if (balance.isPresent()) {
     *     double accountBalance = balance.get();   // e.g., 1234.56
     * }
     * 
     * // Get GPS coordinates:
     * OptionalDouble latitude = executor.queryForDouble("latitude", Filters.eq("locationId", "LOC001"));
     * OptionalDouble longitude = executor.queryForDouble("longitude", Filters.eq("locationId", "LOC001"));
     * }</pre>
     *
     * @param propName the name of the field to retrieve the double value from
     * @param filter BSON filter criteria to match documents (null matches all)
     * @return an OptionalDouble containing the double value if found, empty otherwise
     * @throws IllegalArgumentException if propName is null or empty
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #queryForSingleResult(String, Bson, Class)
     */
    @Beta
    public OptionalDouble queryForDouble(final String propName, final Bson filter) {
        return queryForSingleResult(propName, filter, Double.class).mapToDouble(ToDoubleFunction.UNBOX);
    }

    /**
     * Queries for a single string value from a specific field matching the given filter.
     *
     * <p>This convenience method retrieves a string field value from the first document
     * matching the filter criteria. It's useful for names, descriptions, IDs, or any
     * textual data. Returns Nullable.empty() if no matching document is found or the
     * field value is null.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Get a user's email:
     * Nullable<String> email = executor.queryForString("email", Filters.eq("userId", "user123"));
     * if (email.isPresent()) {
     *     String userEmail = email.get();
     *     sendEmail(userEmail);
     * }
     * 
     * // Get product name:
     * Nullable<String> productName = executor.queryForString("name", Filters.eq("productId", "P001"));
     * }</pre>
     *
     * @param propName the name of the field to retrieve the string value from
     * @param filter BSON filter criteria to match documents (null matches all)
     * @return a Nullable containing the string value if found, empty otherwise
     * @throws IllegalArgumentException if propName is null or empty
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #queryForSingleResult(String, Bson, Class)
     */
    @Beta
    public Nullable<String> queryForString(final String propName, final Bson filter) {
        return queryForSingleResult(propName, filter, String.class);
    }

    /**
     * Queries for a single Date value from a specific field matching the given filter.
     *
     * <p>This convenience method retrieves a Date field value from the first document
     * matching the filter criteria. It's useful for timestamps, creation dates, deadlines,
     * or any date-time data. Returns Nullable.empty() if no matching document is found
     * or the field value is null.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Get user's last login date:
     * Nullable<Date> lastLogin = executor.queryForDate("lastLoginDate", Filters.eq("userId", "user123"));
     * if (lastLogin.isPresent()) {
     *     Date loginDate = lastLogin.get();
     *     // Check if login was recent
     * }
     * 
     * // Get document creation date:
     * Nullable<Date> created = executor.queryForDate("createdAt", Filters.eq("docId", "DOC001"));
     * }</pre>
     *
     * @param propName the name of the field to retrieve the Date value from
     * @param filter BSON filter criteria to match documents (null matches all)
     * @return a Nullable containing the Date value if found, empty otherwise
     * @throws IllegalArgumentException if propName is null or empty
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #queryForDate(String, Bson, Class)
     * @see #queryForSingleResult(String, Bson, Class)
     */
    @Beta
    public Nullable<Date> queryForDate(final String propName, final Bson filter) {
        return queryForSingleResult(propName, filter, Date.class);
    }

    /**
     * Queries for a single Date value with specific type from a field matching the given filter.
     *
     * <p>This generic method retrieves a Date field value of a specific Date subtype from
     * the first document matching the filter criteria. It's useful when you need specific
     * Date implementations like java.sql.Date, java.sql.Timestamp, or custom Date subclasses.
     * Returns Nullable.empty() if no matching document is found or the field value is null.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Get SQL Timestamp:
     * Nullable<Timestamp> timestamp = executor.queryForDate("updatedAt", 
     *                                                       Filters.eq("recordId", "R001"), 
     *                                                       Timestamp.class);
     * 
     * // Get SQL Date (date only, no time):
     * Nullable<java.sql.Date> birthDate = executor.queryForDate("birthDate", 
     *                                                            Filters.eq("userId", "user123"), 
     *                                                            java.sql.Date.class);
     * }</pre>
     *
     * @param <T> the specific Date type to retrieve
     * @param propName the name of the field to retrieve the Date value from
     * @param filter BSON filter criteria to match documents (null matches all)
     * @param rowType the specific Date class to convert the value to
     * @return a Nullable containing the typed Date value if found, empty otherwise
     * @throws IllegalArgumentException if propName or rowType is null or empty
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #queryForDate(String, Bson)
     * @see #queryForSingleResult(String, Bson, Class)
     */
    public <T extends Date> Nullable<T> queryForDate(final String propName, final Bson filter, final Class<T> rowType) {
        return queryForSingleResult(propName, filter, rowType);
    }

    /**
     * Queries for a single field value from the first document matching the filter.
     *
     * <p>This is the foundational method used by all other queryForX convenience methods.
     * It retrieves a specific field value from the first document matching the filter criteria
     * and converts it to the specified type. Returns Nullable.empty() if no matching document
     * is found or if the field value is null.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Get user email as String:
     * Nullable<String> email = executor.queryForSingleResult("email", 
     *                                                        Filters.eq("userId", "user123"), 
     *                                                        String.class);
     * 
     * // Get product price as BigDecimal:
     * Nullable<BigDecimal> price = executor.queryForSingleResult("price", 
     *                                                            Filters.eq("productId", "P001"), 
     *                                                            BigDecimal.class);
     * 
     * // Handle null values:
     * if (email.isPresent()) {
     *     sendNotification(email.get());
     * } else {
     *     System.out.println("User not found or email is null");
     * }
     * }</pre>
     *
     * @param <V> the target type for the field value
     * @param propName the name of the field to retrieve the value from
     * @param filter BSON filter criteria to match documents (null matches all)
     * @param valueType the target type for conversion of the field value
     * @return a Nullable containing the converted field value if found, empty otherwise
     * @throws IllegalArgumentException if propName or valueType is null or empty
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #queryForSingleNonNull(String, Bson, Class)
     * @see com.landawn.abacus.util.u.Nullable
     */
    public <V> Nullable<V> queryForSingleResult(final String propName, final Bson filter, final Class<V> valueType) {
        final FindIterable<Document> findIterable = query(N.asList(propName), filter, null, 0, 1);

        final Document doc = findIterable.first();

        return N.isEmpty(doc) ? (Nullable<V>) Nullable.empty() : Nullable.of(N.convert(doc.get(propName), valueType));
    }

    /**
     * Queries for a single non-null field value from the first document matching the filter.
     *
     * <p>This method is similar to {@link #queryForSingleResult(String, Bson, Class)} but returns
     * an Optional instead of Nullable, providing better null-safety guarantees. The Optional will
     * be empty if no matching document is found, but if a document is found, the value is guaranteed
     * to be non-null (after type conversion).</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Get active user count (guaranteed non-null if present):
     * Optional<Integer> activeCount = executor.queryForSingleNonNull("activeUsers", 
     *                                                                Filters.eq("type", "summary"), 
     *                                                                Integer.class);
     * 
     * // Use with Optional patterns:
     * String message = activeCount.map(count -> "Active users: " + count)
     *                             .orElse("No summary data available");
     * 
     * // Chain operations:
     * activeCount.filter(count -> count > 100)
     *           .ifPresent(count -> sendHighActivityAlert());
     * }</pre>
     *
     * @param <V> the target type for the field value
     * @param propName the name of the field to retrieve the value from
     * @param filter BSON filter criteria to match documents (null matches all)
     * @param valueType the target type for conversion of the field value
     * @return an Optional containing the converted non-null field value if found, empty otherwise
     * @throws IllegalArgumentException if propName or valueType is null or empty
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #queryForSingleResult(String, Bson, Class)
     * @see java.util.Optional
     */
    public <V> Optional<V> queryForSingleNonNull(final String propName, final Bson filter, final Class<V> valueType) {
        final FindIterable<Document> findIterable = query(N.asList(propName), filter, null, 0, 1);

        final Document doc = findIterable.first();

        if (N.isEmpty(doc)) {
            return (Optional<V>) Optional.empty();
        }

        final Object value = doc.get(propName);

        return value == null ? (Optional<V>) Optional.empty() : Optional.ofNullable(N.convert(value, valueType));
    }

    /**
     * Executes a query and returns the results as a Dataset with Document rows.
     *
     * <p>This method performs a find operation and converts the results into a tabular Dataset
     * structure with each MongoDB document represented as a row. This provides a convenient
     * way to work with query results in a tabular format, useful for reporting, data analysis,
     * or integration with other data processing frameworks.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Get all active users as a Dataset:
     * Dataset activeUsers = executor.query(Filters.eq("status", "active"));
     * 
     * // Export to CSV:
     * activeUsers.toCsv("active_users.csv");
     * 
     * // Print tabular format:
     * activeUsers.println();
     * 
     * // Access column data:
     * List<String> userNames = activeUsers.getColumn("name");
     * }</pre>
     *
     * @param filter BSON filter criteria to match documents (null for all documents)
     * @return a Dataset containing the query results with Document-based rows
     * @throws com.mongodb.MongoException if the database operation fails
     * @see Dataset
     * @see #query(Bson, Class)
     * @see #query(Collection, Bson, Class)
     */
    public Dataset query(final Bson filter) {
        return query(filter, Document.class);
    }

    /**
     * Executes a query and returns the results as a Dataset with typed rows.
     *
     * <p>This method performs a find operation and converts the results into a Dataset with
     * each document converted to the specified row type. This provides type safety and enables
     * working with strongly-typed data in a tabular format, combining the benefits of object
     * mapping with tabular data operations.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Get active users as a Dataset of User objects:
     * Dataset userDataset = executor.query(Filters.eq("status", "active"), User.class);
     * 
     * // Access typed data:
     * List<User> users = userDataset.toList(User.class);
     * User firstUser = userDataset.getRow(0, User.class);
     * 
     * // Get data as Maps for flexible processing:
     * Dataset mapDataset = executor.query(Filters.eq("category", "electronics"), Map.class);
     * 
     * // Export with type safety:
     * userDataset.toCsv("typed_users.csv");
     * }</pre>
     *
     * @param <T> the target type for each row in the Dataset
     * @param filter BSON filter criteria to match documents (null for all documents)
     * @param rowType the target type for conversion of each document
     * @return a Dataset containing the query results with typed rows
     * @throws IllegalArgumentException if rowType is null or unsupported
     * @throws com.mongodb.MongoException if the database operation fails
     * @see Dataset
     * @see #query(Bson)
     * @see #query(Collection, Bson, Class)
     */
    public <T> Dataset query(final Bson filter, final Class<T> rowType) {
        return query(null, filter, rowType);
    }

    /**
     * Executes a paginated query and returns the results as a Dataset with typed rows.
     *
     * <p>This method performs a find operation with pagination support, converting the results
     * into a Dataset with each document converted to the specified row type. This is useful
     * for implementing paginated data views while maintaining type safety and tabular data operations.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Get page 2 of products (20 per page) as a Dataset:
     * Dataset productPage = executor.query(Filters.eq("category", "electronics"), 20, 20, Map.class);
     * 
     * // Export paginated results:
     * productPage.toCsv("products_page2.csv");
     * 
     * // Process paginated User objects:
     * Dataset userPage = executor.query(null, 0, 50, User.class);
     * List<User> firstUsers = userPage.toList(User.class);
     * }</pre>
     *
     * @param <T> the target type for each row in the Dataset
     * @param filter BSON filter criteria to match documents (null for all documents)
     * @param offset number of documents to skip from the beginning (0 for first page)
     * @param count maximum number of documents to return
     * @param rowType the target type for conversion of each document
     * @return a Dataset containing the paginated query results with typed rows
     * @throws IllegalArgumentException if rowType is null, offset is negative, or count is non-positive
     * @throws com.mongodb.MongoException if the database operation fails
     * @see Dataset
     * @see #query(Bson, Class)
     */
    public <T> Dataset query(final Bson filter, final int offset, final int count, final Class<T> rowType) {
        return query(null, filter, offset, count, rowType);
    }

    /**
     * Executes a query with field projection and returns the results as a Dataset with typed rows.
     *
     * <p>This method performs a find operation with field projection, converting the results
     * into a Dataset with only the selected fields populated in each typed row. This combination
     * provides both bandwidth optimization through projection and type safety through row conversion.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Get active users with selected fields as a Dataset:
     * Collection<String> fields = Arrays.asList("name", "email", "department");
     * Dataset userData = executor.query(fields, Filters.eq("status", "active"), User.class);
     * 
     * // Export projected data:
     * userData.toCsv("user_contact_info.csv");
     * 
     * // Access specific columns:
     * List<String> departments = userData.getColumn("department");
     * }</pre>
     *
     * @param <T> the target type for each row in the Dataset
     * @param selectPropNames collection of field names to include in projection (null for all fields)
     * @param filter BSON filter criteria to match documents (null for all documents)
     * @param rowType the target type for conversion of each document
     * @return a Dataset containing the query results with projected fields and typed rows
     * @throws IllegalArgumentException if rowType is null or unsupported
     * @throws com.mongodb.MongoException if the database operation fails
     * @see Dataset
     * @see #query(Collection, Bson, int, int, Class)
     */
    public <T> Dataset query(final Collection<String> selectPropNames, final Bson filter, final Class<T> rowType) {
        return query(selectPropNames, filter, 0, Integer.MAX_VALUE, rowType);
    }

    /**
     * Executes a paginated query with field projection and returns the results as a Dataset with typed rows.
     *
     * <p>This method combines field projection, filtering, and pagination to efficiently retrieve
     * a specific subset of documents with only selected fields. The results are converted to the
     * specified row type and organized in a Dataset for tabular operations.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Get page 1 of orders with selected fields:
     * Collection<String> orderFields = Arrays.asList("orderId", "customerId", "total", "status");
     * Dataset orderPage = executor.query(orderFields, 
     *                                    Filters.eq("status", "pending"), 
     *                                    0, 25, 
     *                                    Order.class);
     * 
     * // Export paginated projected data:
     * orderPage.toCsv("pending_orders_page1.csv");
     * 
     * // Calculate totals from projected data:
     * double totalAmount = orderPage.getColumn("total", Double.class).stream()
     *                                .mapToDouble(Double::doubleValue)
     *                                .sum();
     * }</pre>
     *
     * @param <T> the target type for each row in the Dataset
     * @param selectPropNames collection of field names to include in projection (null for all fields)
     * @param filter BSON filter criteria to match documents (null for all documents)
     * @param offset number of documents to skip from the beginning (0 for first page)
     * @param count maximum number of documents to return
     * @param rowType the target type for conversion of each document
     * @return a Dataset containing the paginated query results with projected fields and typed rows
     * @throws IllegalArgumentException if rowType is null, offset is negative, or count is non-positive
     * @throws com.mongodb.MongoException if the database operation fails
     * @see Dataset
     * @see #query(Collection, Bson, Class)
     */
    public <T> Dataset query(final Collection<String> selectPropNames, final Bson filter, final int offset, final int count, final Class<T> rowType) {
        return query(selectPropNames, filter, null, offset, count, rowType);
    }

    /**
     * Executes a sorted query with field projection and returns the results as a Dataset with typed rows.
     *
     * <p>This method performs a find operation with field projection and sorting, converting all
     * matching results into a Dataset with predictable ordering. The combination of projection,
     * sorting, and type conversion provides optimized data retrieval with consistent ordering.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Get all products sorted by price with selected fields:
     * Collection<String> fields = Arrays.asList("name", "price", "category", "rating");
     * Bson filter = Filters.gte("rating", 4.0);
     * Bson sort = Sorts.ascending("price");
     * Dataset productData = executor.query(fields, filter, sort, Product.class);
     * 
     * // Export sorted projected data:
     * productData.toCsv("high_rated_products_by_price.csv");
     * 
     * // Analyze sorted data:
     * List<Double> prices = productData.getColumn("price", Double.class);
     * // Prices are guaranteed to be in ascending order
     * }</pre>
     *
     * @param <T> the target type for each row in the Dataset
     * @param selectPropNames collection of field names to include in projection (null for all fields)
     * @param filter BSON filter criteria to match documents (null for all documents)
     * @param sort BSON sort specification for result ordering (null for natural order)
     * @param rowType the target type for conversion of each document
     * @return a Dataset containing the sorted query results with projected fields and typed rows
     * @throws IllegalArgumentException if rowType is null or unsupported
     * @throws com.mongodb.MongoException if the database operation fails
     * @see Dataset
     * @see #query(Collection, Bson, Bson, int, int, Class)
     * @see com.mongodb.client.model.Sorts
     */
    public <T> Dataset query(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final Class<T> rowType) {
        return query(selectPropNames, filter, sort, 0, Integer.MAX_VALUE, rowType);
    }

    /**
     * Executes a sorted and paginated query with field projection and returns the results as a Dataset with typed rows.
     *
     * <p>This method combines field projection, filtering, sorting, and pagination to efficiently retrieve
     * a specific subset of documents with only selected fields, ordered by the specified criteria. The results
     * are converted to the specified row type and organized in a Dataset for tabular operations.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Get page 1 of active users sorted by last login date:
     * Collection<String> fields = Arrays.asList("userId", "name", "lastLogin");
     * Bson filter = Filters.eq("status", "active");
     * Bson sort = Sorts.descending("lastLogin");
     * Dataset userPage = executor.query(fields, filter, sort, 0, 50, User.class);
     * 
     * // Export paginated sorted projected data:
     * userPage.toCsv("active_users_page1_sorted.csv");
     * 
     * // Process sorted data:
     * List<Date> lastLogins = userPage.getColumn("lastLogin", Date.class);
     * }</pre>
     *
     * @param <T> the target type for each row in the Dataset
     * @param selectPropNames collection of field names to include in projection (null for all fields)
     * @param filter BSON filter criteria to match documents (null for all documents)
     * @param sort BSON sort specification for result ordering (null for natural order)
     * @param offset number of documents to skip from the beginning (0 for first page)
     * @param count maximum number of documents to return
     * @param rowType the target type for conversion of each document
     * @return a Dataset containing the sorted and paginated query results with projected fields and typed rows
     * @throws IllegalArgumentException if rowType is null, offset is negative, or count is non-positive
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public <T> Dataset query(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final int offset, final int count,
            final Class<T> rowType) {
        final FindIterable<Document> findIterable = query(selectPropNames, filter, sort, offset, count);

        if (N.isEmpty(selectPropNames)) {
            return MongoDBBase.extractData(findIterable, rowType);
        } else {
            return MongoDBBase.extractData(selectPropNames, findIterable, rowType);
        }
    }

    /**
     * Executes a query with field projection, filtering, sorting, and pagination, returning the results as a Dataset with typed rows.
     *
     * <p>This method combines field projection, filtering, sorting, and pagination to efficiently retrieve
     * a specific subset of documents with only selected fields. The results are converted to the specified row type
     * and organized in a Dataset for tabular operations.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Get page 1 of active users with selected fields:
     * Collection<String> fields = Arrays.asList("userId", "name", "email");
     * Bson filter = Filters.eq("status", "active");
     * Bson sort = Sorts.ascending("name");
     * Dataset userPage = executor.query(fields, filter, sort, 0, 50, User.class);
     * 
     * // Export paginated sorted projected data:
     * userPage.toCsv("active_users_page1.csv");
     * 
     * // Process paginated data:
     * List<String> emails = userPage.getColumn("email", String.class);
     * }</pre>
     *
     * @param <T> the target type for each row in the Dataset
     * @param projection BSON projection specification for field selection (null for all fields)
     * @param filter BSON filter criteria to match documents (null for all documents)
     * @param sort BSON sort specification for result ordering (null for natural order)
     * @param rowType the target type for conversion of each document
     * @return a Dataset containing the paginated query results with projected fields and typed rows
     */
    public <T> Dataset query(final Bson projection, final Bson filter, final Bson sort, final Class<T> rowType) {
        return query(projection, filter, sort, 0, Integer.MAX_VALUE, rowType);
    }

    /**
     * Executes a query with field projection, filtering, sorting, and pagination, returning the results as a Dataset with typed rows.
     *
     * <p>This method combines field projection, filtering, sorting, and pagination to efficiently retrieve
     * a specific subset of documents with only selected fields. The results are converted to the specified row type
     * and organized in a Dataset for tabular operations.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Get page 1 of active users with selected fields:
     * Bson projection = Projections.fields(Projections.include("userId", "name", "email"));
     * Bson filter = Filters.eq("status", "active");
     * Bson sort = Sorts.ascending("name");
     * Dataset userPage = executor.query(projection, filter, sort, 0, 50, User.class);
     * 
     * // Export paginated sorted projected data:
     * userPage.toCsv("active_users_page1.csv");
     * 
     * // Process paginated data:
     * List<String> emails = userPage.getColumn("email", String.class);
     * }</pre>
     *
     * @param <T> the target type for each row in the Dataset
     * @param projection BSON projection specification for field selection (null for all fields)
     * @param filter BSON filter criteria to match documents (null for all documents)
     * @param sort BSON sort specification for result ordering (null for natural order)
     * @param offset number of documents to skip from the beginning (0 for first page)
     * @param count maximum number of documents to return
     * @param rowType the target type for conversion of each document
     * @return a Dataset containing the paginated query results with projected fields and typed rows
     */
    public <T> Dataset query(final Bson projection, final Bson filter, final Bson sort, final int offset, final int count, final Class<T> rowType) {
        final FindIterable<Document> findIterable = executeQuery(projection, filter, sort, offset, count);

        return MongoDBBase.extractData(findIterable, rowType);
    }

    /**
     * Creates a stream of all documents in the collection.
     *
     * <p>This method returns a lazy stream of all documents in the collection without any filtering.
     * The stream provides functional programming capabilities for processing large result sets
     * efficiently with automatic cursor management and resource cleanup.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Process all documents in the collection:
     * executor.stream()
     *         .filter(doc -> doc.getInteger("age", 0) > 18)
     *         .limit(100)
     *         .forEach(doc -> processAdultDocument(doc));
     * 
     * // Count documents with functional operations:
     * long activeCount = executor.stream()
     *                           .filter(doc -> "active".equals(doc.getString("status")))
     *                           .count();
     * 
     * // Transform and collect:
     * List<String> names = executor.stream()
     *                              .map(doc -> doc.getString("name"))
     *                              .filter(Objects::nonNull)
     *                              .collect(Collectors.toList());
     * }</pre>
     *
     * @return a Stream of Document objects representing all documents in the collection
     * @throws com.mongodb.MongoException if the database operation fails
     * @see Stream
     * @see #stream(Class)
     * @see #stream(Bson)
     */
    public Stream<Document> stream() {
        return stream(Document.class);
    }

    /**
     * Creates a typed stream of all documents in the collection with automatic conversion.
     *
     * <p>This method returns a lazy stream of all documents in the collection, with each document
     * automatically converted to the specified type. This provides type safety and eliminates
     * the need for manual conversion in stream operations while maintaining efficient lazy evaluation.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Process all users with type safety:
     * executor.stream(User.class)
     *         .filter(User::isActive)
     *         .map(User::getEmail)
     *         .filter(Objects::nonNull)
     *         .forEach(email -> sendNotification(email));
     * 
     * // Collect typed results:
     * List<User> activeAdults = executor.stream(User.class)
     *                                  .filter(user -> user.getAge() > 18)
     *                                  .filter(User::isActive)
     *                                  .collect(Collectors.toList());
     * 
     * // Use with Maps for flexible processing:
     * executor.stream(Map.class)
     *         .filter(map -> "premium".equals(map.get("tier")))
     *         .forEach(this::processPremiumAccount);
     * }</pre>
     *
     * @param <T> the target type for each document in the stream
     * @param rowType the target type for conversion of each document
     * @return a Stream of typed objects representing all documents in the collection
     * @throws IllegalArgumentException if rowType is null or unsupported
     * @throws com.mongodb.MongoException if the database operation fails
     * @see Stream
     * @see #stream()
     * @see #stream(Bson, Class)
     */
    public <T> Stream<T> stream(final Class<T> rowType) {
        final MongoCursor<Document> cursor = coll.find().iterator();

        if (rowType.isAssignableFrom(Document.class)) {
            return (Stream<T>) Stream.of(cursor).onClose(Fn.close(cursor));
        } else {
            return Stream.of(cursor).map(toEntity(rowType)).onClose(Fn.close(cursor));
        }
    }

    /**
     * Creates a filtered stream of documents from the collection.
     *
     * <p>This method returns a lazy stream of documents matching the specified filter criteria.
     * The stream provides efficient processing of filtered result sets with automatic cursor
     * management and resource cleanup. This is ideal for functional processing of query results.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Process active users:
     * Bson activeFilter = Filters.eq("status", "active");
     * executor.stream(activeFilter)
     *         .filter(doc -> doc.getDate("lastLogin").after(thirtyDaysAgo))
     *         .forEach(doc -> updateActivityScore(doc));
     * 
     * // Find and process premium accounts:
     * Bson premiumFilter = Filters.eq("accountType", "premium");
     * Optional<Document> firstExpired = executor.stream(premiumFilter)
     *                                          .filter(doc -> doc.getDate("expiresAt").before(new Date()))
     *                                          .findFirst();
     * 
     * // Aggregate data with streams:
     * double avgAge = executor.stream(Filters.exists("age"))
     *                        .mapToInt(doc -> doc.getInteger("age", 0))
     *                        .average()
     *                        .orElse(0.0);
     * }</pre>
     *
     * @param filter BSON filter criteria to match documents (null for all documents)
     * @return a Stream of Document objects matching the filter criteria
     * @throws com.mongodb.MongoException if the database operation fails
     * @see Stream
     * @see #stream(Bson, Class)
     * @see com.mongodb.client.model.Filters
     */
    public Stream<Document> stream(final Bson filter) {
        return stream(filter, Document.class);
    }

    /**
     * Creates a filtered, typed stream of documents with automatic conversion.
     *
     * <p>This method combines filtering and type conversion to return a lazy stream of documents
     * matching the specified criteria, with each document automatically converted to the target type.
     * This provides both query filtering and type safety in a single operation.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Process active users with type safety:
     * Bson activeFilter = Filters.eq("status", "active");
     * executor.stream(activeFilter, User.class)
     *         .filter(user -> user.getLastLoginDate().after(recentDate))
     *         .map(User::getEmail)
     *         .forEach(this::sendWelcomeBackEmail);
     * 
     * // Find high-value orders:
     * Bson highValueFilter = Filters.gte("total", 1000.0);
     * List<Order> highValueOrders = executor.stream(highValueFilter, Order.class)
     *                                      .filter(order -> order.getStatus() == OrderStatus.PENDING)
     *                                      .collect(Collectors.toList());
     * 
     * // Process as Maps for flexible handling:
     * executor.stream(Filters.eq("category", "electronics"), Map.class)
     *         .filter(map -> (Double) map.get("rating") > 4.0)
     *         .limit(10)
     *         .forEach(this::processHighRatedProduct);
     * }</pre>
     *
     * @param <T> the target type for each document in the stream
     * @param filter BSON filter criteria to match documents (null for all documents)
     * @param rowType the target type for conversion of each document
     * @return a Stream of typed objects matching the filter criteria
     * @throws IllegalArgumentException if rowType is null or unsupported
     * @throws com.mongodb.MongoException if the database operation fails
     * @see Stream
     * @see #stream(Bson)
     * @see #stream(Bson, int, int, Class)
     */
    public <T> Stream<T> stream(final Bson filter, final Class<T> rowType) {
        return stream(null, filter, rowType);
    }

    /**
     * Creates a paginated stream of filtered documents with type conversion.
     * 
     * <p>This method provides pagination support for large result sets by specifying
     * an offset and count. Useful for implementing pagination in web applications
     * or processing large datasets in chunks.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Get page 3 with 20 items per page
     * executor.stream(filter, 40, 20, Product.class)
     *         .forEach(this::displayProduct);
     * }</pre>
     * 
     * @param <T> the target type for each document
     * @param filter BSON filter criteria to match documents
     * @param offset number of documents to skip (must be >= 0)
     * @param count maximum number of documents to return (must be >= 0)
     * @param rowType the target type for conversion
     * @return a Stream of typed objects with pagination applied
     * @throws IllegalArgumentException if offset or count is negative, or rowType is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public <T> Stream<T> stream(final Bson filter, final int offset, final int count, final Class<T> rowType) {
        return stream(null, filter, offset, count, rowType);
    }

    /**
     * Creates a stream with projection and filtering.
     * 
     * <p>This method allows specifying which fields to include in the results,
     * reducing network traffic and improving performance when only specific
     * fields are needed.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Get only name and email fields
     * List<String> fields = Arrays.asList("name", "email");
     * executor.stream(fields, filter, User.class)
     *         .forEach(this::processUser);
     * }</pre>
     * 
     * @param <T> the target type for each document
     * @param selectPropNames collection of property names to include in results (null for all fields)
     * @param filter BSON filter criteria to match documents
     * @param rowType the target type for conversion
     * @return a Stream of typed objects with specified fields
     * @throws IllegalArgumentException if rowType is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public <T> Stream<T> stream(final Collection<String> selectPropNames, final Bson filter, final Class<T> rowType) {
        return stream(selectPropNames, filter, 0, Integer.MAX_VALUE, rowType);
    }

    /**
     * Creates a paginated stream with projection and filtering.
     * 
     * <p>Combines projection, filtering, and pagination for maximum control over
     * query results. This is the most flexible streaming method for basic queries.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Get specific fields with pagination
     * List<String> fields = Arrays.asList("name", "status");
     * executor.stream(fields, filter, 0, 100, User.class)
     *         .collect(Collectors.toList());
     * }</pre>
     * 
     * @param <T> the target type for each document
     * @param selectPropNames collection of property names to include
     * @param filter BSON filter criteria
     * @param offset number of documents to skip
     * @param count maximum number of documents to return
     * @param rowType the target type for conversion
     * @return a Stream of typed objects
     * @throws IllegalArgumentException if parameters are invalid
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public <T> Stream<T> stream(final Collection<String> selectPropNames, final Bson filter, final int offset, final int count, final Class<T> rowType) {
        return stream(selectPropNames, filter, null, offset, count, rowType);
    }

    /**
     * Creates a sorted stream with projection and filtering.
     * 
     * <p>Adds sorting capability to the stream, allowing results to be ordered
     * by one or more fields before processing.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Get users sorted by creation date
     * Bson sort = Sorts.descending("createdAt");
     * executor.stream(fields, filter, sort, User.class)
     *         .limit(10)
     *         .forEach(this::displayRecentUser);
     * }</pre>
     * 
     * @param <T> the target type for each document
     * @param selectPropNames collection of property names to include
     * @param filter BSON filter criteria
     * @param sort BSON sort criteria (null for no sorting)
     * @param rowType the target type for conversion
     * @return a sorted Stream of typed objects
     * @throws IllegalArgumentException if rowType is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public <T> Stream<T> stream(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final Class<T> rowType) {
        return stream(selectPropNames, filter, sort, 0, Integer.MAX_VALUE, rowType);
    }

    /**
     * Creates a fully-featured stream with projection, filtering, sorting, and pagination.
     * 
     * <p>This is the most comprehensive streaming method using collection-based projection,
     * providing all query features in a single operation.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Complex query with all features
     * List<String> fields = Arrays.asList("name", "email", "score");
     * Bson filter = Filters.gte("score", 80);
     * Bson sort = Sorts.descending("score");
     * executor.stream(fields, filter, sort, 0, 50, Student.class)
     *         .forEach(this::awardHonor);
     * }</pre>
     * 
     * @param <T> the target type for each document
     * @param selectPropNames collection of property names to include
     * @param filter BSON filter criteria
     * @param sort BSON sort criteria
     * @param offset number of documents to skip
     * @param count maximum number of documents to return
     * @param rowType the target type for conversion
     * @return a fully-featured Stream of typed objects
     * @throws IllegalArgumentException if parameters are invalid
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public <T> Stream<T> stream(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final int offset, final int count,
            final Class<T> rowType) {
        final MongoCursor<Document> cursor = query(selectPropNames, filter, sort, offset, count).iterator();

        if (rowType.isAssignableFrom(Document.class)) {
            return (Stream<T>) Stream.of(cursor).onClose(Fn.close(cursor));
        } else {
            return Stream.of(cursor).map(toEntity(rowType)).onClose(Fn.close(cursor));
        }
    }

    /**
     * Creates a stream with BSON projection, filtering, and sorting.
     * 
     * <p>Uses BSON projection for more complex field selection scenarios,
     * such as including/excluding nested fields or using projection operators.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Complex projection with nested fields
     * Bson projection = Projections.fields(
     *     Projections.include("name", "address.city"),
     *     Projections.excludeId()
     * );
     * executor.stream(projection, filter, sort, User.class)
     *         .forEach(this::processUser);
     * }</pre>
     * 
     * @param <T> the target type for each document
     * @param projection BSON projection document (null for all fields)
     * @param filter BSON filter criteria
     * @param sort BSON sort criteria
     * @param rowType the target type for conversion
     * @return a Stream with complex projection
     * @throws IllegalArgumentException if rowType is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public <T> Stream<T> stream(final Bson projection, final Bson filter, final Bson sort, final Class<T> rowType) {
        return stream(projection, filter, sort, 0, Integer.MAX_VALUE, rowType);
    }

    /**
     * Creates a fully-featured stream with BSON projection.
     * 
     * <p>The most flexible streaming method, supporting BSON projection for
     * complex field selection along with all other query features.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Advanced query with all features
     * Bson projection = Projections.fields(
     *     Projections.include("scores"),
     *     Projections.elemMatch("scores", Filters.gte("value", 90))
     * );
     * executor.stream(projection, filter, sort, 0, 10, Student.class)
     *         .forEach(this::processTopScorer);
     * }</pre>
     * 
     * @param <T> the target type for each document
     * @param projection BSON projection document
     * @param filter BSON filter criteria
     * @param sort BSON sort criteria
     * @param offset number of documents to skip
     * @param count maximum number of documents to return
     * @param rowType the target type for conversion
     * @return a fully-featured Stream with BSON projection
     * @throws IllegalArgumentException if parameters are invalid
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public <T> Stream<T> stream(final Bson projection, final Bson filter, final Bson sort, final int offset, final int count, final Class<T> rowType) {
        final MongoCursor<Document> cursor = executeQuery(projection, filter, sort, offset, count).iterator();

        if (rowType.isAssignableFrom(Document.class)) {
            return (Stream<T>) Stream.of(cursor).onClose(Fn.close(cursor));
        } else {
            return Stream.of(cursor).map(toEntity(rowType)).onClose(Fn.close(cursor));
        }
    }

    /**
     * Converts a FindIterable result to an entity of the specified type.
     * 
     * <p>Internal helper method that retrieves the first document from a FindIterable
     * and converts it to the target type. Returns null if no document is found.</p>
     * 
     * @param <T> the target type
     * @param findIterable the MongoDB find result
     * @param rowType the target class for conversion
     * @return the converted entity or null if no document found
     */
    private static <T> T toEntity(final FindIterable<Document> findIterable, final Class<T> rowType) {
        if (findIterable == null) {
            return null;
        }

        final Document doc = findIterable.first();

        return N.isEmpty(doc) ? null : MongoDBBase.readRow(doc, rowType);
    }

    /**
     * Converts a Document to an entity of the specified type.
     * 
     * <p>Internal helper method for converting a single Document to the target type.
     * Returns null if the document is null or empty.</p>
     * 
     * @param <T> the target type
     * @param doc the document to convert
     * @param rowType the target class for conversion
     * @return the converted entity or null if document is empty
     */
    private static <T> T toEntity(final Document doc, final Class<T> rowType) {
        return N.isEmpty(doc) ? null : MongoDBBase.readRow(doc, rowType);
    }

    /**
     * Creates a function that converts Documents to entities.
     * 
     * <p>Internal helper that returns a reusable function for converting
     * Documents to the specified entity type within stream operations.</p>
     * 
     * @param <T> the target type
     * @param rowType the target class for conversion
     * @return a Function that converts Documents to entities
     */
    private static <T> Function<Document, T> toEntity(final Class<T> rowType) {
        return doc -> N.isEmpty(doc) ? null : MongoDBBase.readRow(doc, rowType);
    }

    /**
     * Executes a query with field projection.
     * 
     * <p>Internal method that constructs and executes a MongoDB query with
     * the specified field projection, filter, sort, and pagination.</p>
     * 
     * @param selectPropNames property names to include in projection
     * @param filter query filter
     * @param sort sort criteria
     * @param offset number of documents to skip
     * @param count maximum number of documents
     * @return FindIterable for the query results
     */
    private FindIterable<Document> query(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final int offset, final int count) {
        if (N.isEmpty(selectPropNames)) {
            return executeQuery(null, filter, sort, offset, count);
        } else if (selectPropNames instanceof List) {
            return executeQuery(Projections.include((List<String>) selectPropNames), filter, sort, offset, count);
        } else {
            return executeQuery(Projections.include(new ArrayList<>(selectPropNames)), filter, sort, offset, count);
        }
    }

    /**
     * Executes a MongoDB query with all options.
     * 
     * <p>Core internal method that executes the actual MongoDB query with
     * projection, filtering, sorting, and pagination options.</p>
     * 
     * @param projection BSON projection
     * @param filter query filter
     * @param sort sort criteria
     * @param offset number of documents to skip
     * @param count maximum number of documents
     * @return FindIterable for the query results
     * @throws IllegalArgumentException if offset or count is negative
     */
    private FindIterable<Document> executeQuery(final Bson projection, final Bson filter, final Bson sort, final int offset, final int count) {
        if (offset < 0 || count < 0) {
            throw new IllegalArgumentException("offset (" + offset + ") and count (" + count + ") cannot be negative");
        }

        FindIterable<Document> findIterable = filter == null ? coll.find() : coll.find(filter);

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

        return findIterable;
    }

    /**
     * Creates a change stream to monitor all changes in the collection.
     * 
     * <p>Change streams allow applications to access real-time data changes
     * without the complexity and risk of tailing the oplog. Returns a change
     * stream that monitors all changes in the collection.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * executor.watch()
     *     .forEach(change -> System.out.println("Change: " + change));
     * }</pre>
     * 
     * @return a ChangeStreamIterable for monitoring collection changes
     * @throws com.mongodb.MongoException if the database operation fails
     * @see ChangeStreamIterable
     */
    public ChangeStreamIterable<Document> watch() {
        return coll.watch();
    }

    /**
     * Creates a typed change stream to monitor collection changes.
     * 
     * <p>Similar to watch() but returns change documents converted to the
     * specified type for type-safe change processing.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * executor.watch(UserChange.class)
     *     .forEach(change -> processUserChange(change));
     * }</pre>
     * 
     * @param <T> the target type for change documents
     * @param rowType the class to convert change documents to
     * @return a typed ChangeStreamIterable
     * @throws IllegalArgumentException if rowType is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public <T> ChangeStreamIterable<T> watch(final Class<T> rowType) {
        return coll.watch(rowType);
    }

    /**
     * Creates a change stream with an aggregation pipeline.
     * 
     * <p>Allows filtering and transforming change events using an aggregation
     * pipeline. Useful for monitoring specific types of changes.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Bson> pipeline = Arrays.asList(
     *     Aggregates.match(Filters.in("operationType", "insert", "update"))
     * );
     * executor.watch(pipeline)
     *     .forEach(change -> processChange(change));
     * }</pre>
     * 
     * @param pipeline aggregation pipeline to apply to change events
     * @return a filtered ChangeStreamIterable
     * @throws IllegalArgumentException if pipeline is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public ChangeStreamIterable<Document> watch(final List<? extends Bson> pipeline) {
        return coll.watch(pipeline);
    }

    /**
     * Creates a typed change stream with an aggregation pipeline.
     * 
     * <p>Combines pipeline filtering with type conversion for maximum
     * flexibility in change stream processing.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Bson> pipeline = Arrays.asList(
     *     Aggregates.match(Filters.eq("fullDocument.status", "critical"))
     * );
     * executor.watch(pipeline, Alert.class)
     *     .forEach(alert -> sendNotification(alert));
     * }</pre>
     * 
     * @param <T> the target type for change documents
     * @param pipeline aggregation pipeline to apply
     * @param rowType the class to convert change documents to
     * @return a filtered and typed ChangeStreamIterable
     * @throws IllegalArgumentException if parameters are null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public <T> ChangeStreamIterable<T> watch(final List<? extends Bson> pipeline, final Class<T> rowType) {
        return coll.watch(pipeline, rowType);
    }

    /**
     * Inserts a single document into the collection (blocking operation).
     *
     * <p>This method inserts a single document into the collection. The object can be a Document,
     * Map, or any entity class with getter/setter methods. Entity objects are automatically
     * converted to BSON documents using the configured codec registry.</p>
     *
     * <p><b>Note:</b> This method performs a blocking operation. For non-blocking operations, use {@link #async()}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Insert a Document:
     * Document userDoc = new Document("name", "John")
     *                      .append("email", "john@example.com")
     *                      .append("age", 30);
     * executor.insertOne(userDoc);
     *
     * // Insert an entity:
     * User user = new User("Jane", "jane@example.com", 25);
     * executor.insertOne(user);
     * }</pre>
     *
     * @param obj the object to insert - can be Document, {@code Map<String, Object>}, or entity class with getter/setter methods
     * @throws IllegalArgumentException if obj is null
     * @throws com.mongodb.MongoWriteException if the insert operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #insertOne(Object, InsertOneOptions)
     * @see #insertMany(Collection)
     * @see #async()
     */
    public void insertOne(final Object obj) {
        insertOne(obj, null);
    }

    /**
     * Inserts a single document into the collection with additional options.
     *
     * <p>This method provides fine-grained control over the insert operation through InsertOneOptions,
     * allowing specification of write concerns, bypass document validation, and other insert-specific settings.
     * The object is automatically converted to a BSON document.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User user = new User("Alice", "alice@example.com", 28);
     * 
     * // Insert with custom options:
     * InsertOneOptions options = new InsertOneOptions().bypassDocumentValidation(true);
     * executor.insertOne(user, options);
     * }</pre>
     *
     * @param obj the object to insert - can be Document, {@code Map<String, Object>}, or entity class with getter/setter methods
     * @param options additional options for the insert operation (null uses defaults)
     * @throws IllegalArgumentException if obj is null
     * @throws com.mongodb.MongoWriteException if the insert operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see InsertOneOptions
     * @see #insertOne(Object)
     */
    public void insertOne(final Object obj, final InsertOneOptions options) {
        if (options == null) {
            coll.insertOne(toDocument(obj));
        } else {
            coll.insertOne(toDocument(obj), options);
        }
    }

    /**
     * Inserts multiple documents into the collection in a single operation (blocking operation).
     *
     * <p>This method inserts multiple documents into the collection efficiently in a single batch operation.
     * Each object in the collection can be a Document, Map, or entity class. All objects are automatically
     * converted to BSON documents. Batch operations are more efficient than individual inserts for multiple documents.</p>
     *
     * <p><b>Note:</b> This method performs a blocking operation. For non-blocking operations, use {@link #async()}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Insert multiple entities:
     * List<User> users = Arrays.asList(
     *     new User("John", "john@example.com", 30),
     *     new User("Jane", "jane@example.com", 25),
     *     new User("Bob", "bob@example.com", 35)
     * );
     * executor.insertMany(users);
     * }</pre>
     *
     * @param objList collection of objects to insert - each can be Document, {@code Map<String, Object>}, or entity class with getter/setter methods
     * @throws IllegalArgumentException if objList is null or empty
     * @throws com.mongodb.MongoBulkWriteException if one or more insert operations fail
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #insertMany(Collection, InsertManyOptions)
     * @see #insertOne(Object)
     * @see #async()
     */
    public void insertMany(final Collection<?> objList) {
        insertMany(objList, null);
    }

    /**
     * Inserts multiple documents into the collection with additional options.
     *
     * <p>This method provides fine-grained control over the bulk insert operation through InsertManyOptions,
     * allowing specification of ordered/unordered inserts, write concerns, and document validation settings.
     * All objects are automatically converted to BSON documents before insertion.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<User> users = Arrays.asList(
     *     new User("John", "john@example.com", 30),
     *     new User("Jane", "jane@example.com", 25)
     * );
     * 
     * // Insert with unordered execution (faster for large batches):
     * InsertManyOptions options = new InsertManyOptions().ordered(false);
     * executor.insertMany(users, options);
     * }</pre>
     *
     * @param objList collection of objects to insert - each can be Document, {@code Map<String, Object>}, or entity class with getter/setter methods
     * @param options additional options for the insert operation (null uses defaults)
     * @throws IllegalArgumentException if objList is null or empty
     * @throws com.mongodb.MongoBulkWriteException if one or more insert operations fail
     * @throws com.mongodb.MongoException if the database operation fails
     * @see InsertManyOptions
     * @see #insertMany(Collection)
     */
    public void insertMany(final Collection<?> objList, final InsertManyOptions options) {
        N.checkArgNotEmpty(objList, "objList");

        final List<Document> docs = toDocument(objList);

        if (options == null) {
            coll.insertMany(docs);
        } else {
            coll.insertMany(docs, options);
        }
    }

    /**
     * Converts an object to a MongoDB Document.
     * 
     * <p>Internal helper method that converts various object types to Documents.
     * If the object is already a Document, it's returned as-is. Otherwise,
     * it's converted using the MongoDB utility.</p>
     * 
     * @param obj the object to convert
     * @return a Document representation of the object
     */
    private static Document toDocument(final Object obj) {
        return obj instanceof Document ? (Document) obj : MongoDBBase.toDocument(obj);
    }

    /**
     * Converts a collection of objects to Documents.
     * 
     * <p>Internal helper that efficiently converts a collection of objects
     * to a list of Documents, optimizing for the case where objects are
     * already Documents.</p>
     * 
     * @param objList the collection of objects to convert
     * @return a List of Documents
     */
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
     * Updates a single document identified by ObjectId string with the specified update operations (blocking operation).
     *
     * <p>This method updates a single document matching the provided ObjectId string with the specified
     * update operations. The update can be a Bson update document, a Document with update operators,
     * a Map, or an entity class. If the update object implements the DirtyMarker interface, only the
     * dirty properties will be updated. If the update doesn't contain MongoDB update operators, it's
     * automatically wrapped in a $set operation.</p>
     *
     * <p><b>Note:</b> This method performs a blocking operation. For non-blocking operations, use {@link #async()}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String userId = "507f1f77bcf86cd799439011";
     *
     * // Update with MongoDB update operators:
     * UpdateResult result1 = executor.updateOne(userId,
     *     Updates.combine(Updates.set("status", "active"), Updates.inc("loginCount", 1)));
     *
     * // Update with entity (automatically wrapped in $set):
     * User updatedUser = new User();
     * updatedUser.setStatus("inactive");
     * UpdateResult result2 = executor.updateOne(userId, updatedUser);
     * }</pre>
     *
     * @param objectId the string representation of the ObjectId identifying the document to update
     * @param update the update operations - can be Bson, Document with update operators, {@code Map<String, Object>}, or entity class with getter/setter methods
     * @return UpdateResult containing information about the update operation
     * @throws IllegalArgumentException if objectId or update is null, or objectId format is invalid
     * @throws com.mongodb.MongoWriteException if the update operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see UpdateResult
     * @see #updateOne(ObjectId, Object)
     * @see com.mongodb.client.model.Updates
     * @see #async()
     */
    public UpdateResult updateOne(final String objectId, final Object update) {
        return updateOne(createObjectId(objectId), update);
    }

    /**
     * Updates a single document identified by ObjectId.
     *
     * <p>Updates the document with the specified ObjectId. If the update object
     * implements DirtyMarker interface, only dirty properties are updated for efficiency.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
     * executor.updateOne(id, Updates.set("lastModified", new Date()));
     * }</pre>
     *
     * @param objectId the ObjectId of the document to update
     * @param update can be Bson/Document/{@code Map<String, Object>}/entity class with getter/setter methods
     * @return UpdateResult containing update operation details
     * @throws IllegalArgumentException if objectId or update is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public UpdateResult updateOne(final ObjectId objectId, final Object update) {
        return updateOne(MongoDBBase.objectId2Filter(objectId), update);
    }

    /**
     * Updates a single document matching the filter.
     *
     * <p>Updates the first document that matches the filter criteria. If the update
     * object implements DirtyMarker interface, only dirty properties are updated.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("email", "john@example.com");
     * executor.updateOne(filter, Updates.set("verified", true));
     * }</pre>
     *
     * @param filter BSON filter to identify the document
     * @param update can be Bson/Document/{@code Map<String, Object>}/entity class with getter/setter methods
     * @return UpdateResult containing update operation details
     * @throws IllegalArgumentException if filter or update is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public UpdateResult updateOne(final Bson filter, final Object update) {
        return updateOne(filter, update, null);
    }

    /**
     * Updates a single document with additional options.
     *
     * <p>Provides full control over the update operation including upsert behavior,
     * write concern, and array filters.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * UpdateOptions options = new UpdateOptions().upsert(true);
     * executor.updateOne(filter, update, options);
     * }</pre>
     *
     * @param filter BSON filter to identify the document
     * @param update can be Bson/Document/{@code Map<String, Object>}/entity class with getter/setter methods
     * @param options additional update options (null uses defaults)
     * @return UpdateResult containing update operation details
     * @throws IllegalArgumentException if filter or update is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public UpdateResult updateOne(final Bson filter, final Object update, final UpdateOptions options) {
        if (options == null) {
            return coll.updateOne(filter, toBson(update));
        } else {
            return coll.updateOne(filter, toBson(update), options);
        }
    }

    /**
     * Updates a single document using multiple update operations.
     * 
     * <p>Applies a collection of update operations to the first matching document.
     * Useful for complex updates requiring multiple operations.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Bson> updates = Arrays.asList(
     *     Updates.set("status", "processed"),
     *     Updates.inc("processCount", 1)
     * );
     * executor.updateOne(filter, updates);
     * }</pre>
     * 
     * @param filter BSON filter to identify the document
     * @param objList collection of update operations
     * @return UpdateResult containing update operation details
     * @throws IllegalArgumentException if parameters are null or empty
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public UpdateResult updateOne(final Bson filter, final Collection<?> objList) {
        return updateOne(filter, objList, null);
    }

    /**
     * Updates a single document using multiple update operations with options.
     * 
     * <p>Applies a collection of update operations to the first matching document
     * with additional control through UpdateOptions.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * UpdateOptions options = new UpdateOptions().upsert(true);
     * executor.updateOne(filter, updatesList, options);
     * }</pre>
     * 
     * @param filter BSON filter to identify the document
     * @param objList collection of update operations
     * @param updateOptions additional update options
     * @return UpdateResult containing update operation details
     * @throws IllegalArgumentException if filter or objList is null or empty
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public UpdateResult updateOne(final Bson filter, final Collection<?> objList, final UpdateOptions updateOptions) {
        final List<Bson> updateToUse = toBson(objList);

        if (updateOptions == null) {
            return coll.updateOne(filter, updateToUse);
        } else {
            return coll.updateOne(filter, updateToUse, updateOptions);
        }
    }

    /**
     * Creates an ObjectId from a string representation.
     * 
     * <p>Internal helper method that validates and creates an ObjectId
     * from its string representation.</p>
     * 
     * @param objectId the string representation of the ObjectId
     * @return the created ObjectId
     * @throws IllegalArgumentException if objectId is null or empty
     */
    private static ObjectId createObjectId(final String objectId) {
        N.checkArgNotEmpty(objectId, "objectId");

        return new ObjectId(objectId);
    }

    /**
     * Converts an update object to BSON format.
     * 
     * <p>Internal helper that converts various update formats to BSON.
     * If the update doesn't contain MongoDB operators (starting with $),
     * it's automatically wrapped in a $set operation.</p>
     * 
     * @param update the update object to convert
     * @return BSON representation of the update
     */
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

    /**
     * Converts a collection of update objects to BSON format.
     * 
     * <p>Internal helper that efficiently converts a collection of update
     * objects to BSON format, optimizing for the case where objects are
     * already BSON.</p>
     * 
     * @param objList the collection of update objects
     * @return a List of BSON updates
     */
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
     * Updates multiple documents matching the filter with the specified update operations (blocking operation).
     *
     * <p>This method performs a bulk update operation on all documents that match the given filter.
     * The update parameter can be a BSON update document, a Map, or an entity object. If the update
     * object implements DirtyMarker interface, only modified properties will be updated for efficiency.</p>
     *
     * <p><b>Note:</b> This method performs a blocking operation. For non-blocking operations, use {@link #async()}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Update all inactive users:
     * Bson filter = Filters.eq("status", "inactive");
     * Bson update = Updates.set("status", "archived");
     * UpdateResult result = executor.updateMany(filter, update);
     * System.out.println("Updated " + result.getModifiedCount() + " users");
     * }</pre>
     *
     * @param filter the query filter to identify documents to update
     * @param update the update operations to apply; can be Bson/Document/{@code Map<String, Object>}/entity class with getter/setter methods
     * @return UpdateResult containing information about the update operation
     * @throws IllegalArgumentException if filter or update is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #updateMany(Bson, Object, UpdateOptions)
     * @see #updateOne(Bson, Object)
     * @see UpdateResult
     * @see #async()
     */
    public UpdateResult updateMany(final Bson filter, final Object update) {
        return updateMany(filter, update, null);
    }

    /**
     * Updates multiple documents matching the filter with the specified update operations and options.
     *
     * <p>This method performs a bulk update operation on all documents that match the given filter
     * with additional control through UpdateOptions. Options allow specification of upsert behavior,
     * write concern, bypass document validation, and array filters for advanced update scenarios.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Update with upsert and array filters:
     * Bson filter = Filters.eq("category", "electronics");
     * Bson update = Updates.inc("inventory.$[item].quantity", -1);
     * UpdateOptions options = new UpdateOptions()
     *     .upsert(false)
     *     .arrayFilters(Arrays.asList(Filters.gte("item.quantity", 1)));
     * 
     * UpdateResult result = executor.updateMany(filter, update, options);
     * }</pre>
     *
     * @param filter the query filter to identify documents to update
     * @param update the update operations to apply; can be Bson/Document/{@code Map<String, Object>}/entity class with getter/setter methods
     * @param options additional options for the update operation (null uses defaults)
     * @return UpdateResult containing information about the update operation
     * @throws IllegalArgumentException if filter or update is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #updateMany(Bson, Object)
     * @see UpdateOptions
     * @see UpdateResult
     */
    public UpdateResult updateMany(final Bson filter, final Object update, final UpdateOptions options) {
        if (options == null) {
            return coll.updateMany(filter, toBson(update));
        } else {
            return coll.updateMany(filter, toBson(update), options);
        }
    }

    /**
     * Updates multiple documents matching the filter using a collection of update operations.
     *
     * <p>This method performs a bulk update operation using multiple update documents or objects.
     * Each item in the collection represents an update operation that will be applied to matching
     * documents. This is useful for performing multiple different updates in a single operation.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Apply multiple updates to matching documents:
     * Bson filter = Filters.eq("status", "pending");
     * List<Bson> updates = Arrays.asList(
     *     Updates.set("status", "processed"),
     *     Updates.set("processedAt", new Date()),
     *     Updates.inc("processCount", 1)
     * );
     * UpdateResult result = executor.updateMany(filter, updates);
     * }</pre>
     *
     * @param filter the query filter to identify documents to update
     * @param objList collection of update operations; each can be Bson/Document/Map or entity objects
     * @return UpdateResult containing information about the update operation
     * @throws IllegalArgumentException if filter or objList is null or empty
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #updateMany(Bson, Collection, UpdateOptions)
     * @see #updateMany(Bson, Object)
     */
    public UpdateResult updateMany(final Bson filter, final Collection<?> objList) {
        return updateMany(filter, objList, null);
    }

    /**
     * Updates multiple documents matching the filter using a collection of update operations with options.
     *
     * <p>This method performs a bulk update operation using multiple update documents with additional
     * control through UpdateOptions. This provides maximum flexibility for complex update scenarios
     * requiring multiple operations with specific write concerns, upsert behavior, or array filters.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Complex update with multiple operations and options:
     * Bson filter = Filters.eq("status", "shipping");
     * List<Bson> updates = Arrays.asList(
     *     Updates.set("status", "delivered"),
     *     Updates.set("deliveredAt", LocalDateTime.now()),
     *     Updates.push("statusHistory", new Document("status", "delivered"))
     * );
     * UpdateOptions options = new UpdateOptions().bypassDocumentValidation(true);
     * UpdateResult result = executor.updateMany(filter, updates, options);
     * }</pre>
     *
     * @param filter the query filter to identify documents to update
     * @param objList collection of update operations; each can be Bson/Document/Map or entity objects
     * @param updateOptions additional options for the update operation (null uses defaults)
     * @return UpdateResult containing information about the update operation
     * @throws IllegalArgumentException if filter or objList is null or empty
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #updateMany(Bson, Collection)
     * @see UpdateOptions
     */
    public UpdateResult updateMany(final Bson filter, final Collection<?> objList, final UpdateOptions updateOptions) {
        N.checkArgNotEmpty(objList, "objList");

        final List<Bson> updateToUse = toBson(objList);

        if (updateOptions == null) {
            return coll.updateMany(filter, updateToUse);
        } else {
            return coll.updateMany(filter, updateToUse, updateOptions);
        }
    }

    /**
     * Replaces a document identified by ObjectId string.
     * 
     * <p>Completely replaces the document with the specified ObjectId string.
     * Unlike update operations, this replaces the entire document except for the _id field.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String id = "507f1f77bcf86cd799439011";
     * User newUser = new User("John", "john@example.com");
     * executor.replaceOne(id, newUser);
     * }</pre>
     *
     * @param objectId string representation of the ObjectId
     * @param replacement can be Document/{@code Map<String, Object>}/entity class with getter/setter methods
     * @return UpdateResult containing replace operation details
     * @throws IllegalArgumentException if objectId is invalid or replacement is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public UpdateResult replaceOne(final String objectId, final Object replacement) {
        return replaceOne(createObjectId(objectId), replacement);
    }

    /**
     * Replaces a document identified by ObjectId.
     * 
     * <p>Completely replaces the document with the specified ObjectId.
     * The entire document is replaced except for the _id field.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
     * Document newDoc = new Document("name", "Jane").append("status", "active");
     * executor.replaceOne(id, newDoc);
     * }</pre>
     *
     * @param objectId the ObjectId of the document to replace
     * @param replacement can be Document/{@code Map<String, Object>}/entity class with getter/setter methods
     * @return UpdateResult containing replace operation details
     * @throws IllegalArgumentException if objectId or replacement is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public UpdateResult replaceOne(final ObjectId objectId, final Object replacement) {
        return replaceOne(MongoDBBase.objectId2Filter(objectId), replacement);
    }

    /**
     * Replaces a single document matching the filter.
     * 
     * <p>Replaces the first document that matches the filter criteria with
     * the replacement document. The _id field is preserved.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("email", "old@example.com");
     * User newUser = new User("John", "new@example.com");
     * executor.replaceOne(filter, newUser);
     * }</pre>
     *
     * @param filter BSON filter to identify the document
     * @param replacement can be Document/{@code Map<String, Object>}/entity class with getter/setter methods
     * @return UpdateResult containing replace operation details
     * @throws IllegalArgumentException if filter or replacement is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public UpdateResult replaceOne(final Bson filter, final Object replacement) {
        return replaceOne(filter, replacement, null);
    }

    /**
     * Replaces a single document with additional options.
     * 
     * <p>Provides full control over the replace operation including upsert
     * behavior and write concern through ReplaceOptions.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ReplaceOptions options = new ReplaceOptions().upsert(true);
     * executor.replaceOne(filter, replacement, options);
     * }</pre>
     *
     * @param filter BSON filter to identify the document
     * @param replacement can be Document/{@code Map<String, Object>}/entity class with getter/setter methods
     * @param options additional replace options (null uses defaults)
     * @return UpdateResult containing replace operation details
     * @throws IllegalArgumentException if filter or replacement is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public UpdateResult replaceOne(final Bson filter, final Object replacement, final ReplaceOptions options) {
        if (options == null) {
            return coll.replaceOne(filter, toDocument(replacement));
        } else {
            return coll.replaceOne(filter, toDocument(replacement), options);
        }
    }

    /**
     * Deletes the document with the specified ObjectId string from the collection (blocking operation).
     *
     * <p><b>Note:</b> This method performs a blocking operation. For non-blocking operations, use {@link #async()}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String id = "507f1f77bcf86cd799439011";
     * DeleteResult result = executor.deleteOne(id);
     * System.out.println("Deleted: " + result.getDeletedCount());
     * }</pre>
     *
     * @param objectId string representation of the ObjectId
     * @return DeleteResult containing deletion details
     * @throws IllegalArgumentException if objectId is invalid
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #async()
     */
    public DeleteResult deleteOne(final String objectId) {
        return deleteOne(createObjectId(objectId));
    }

    /**
     * Deletes the document with the specified ObjectId from the collection.
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
     * DeleteResult result = executor.deleteOne(id);
     * }</pre>
     * 
     * @param objectId the ObjectId of the document to delete
     * @return DeleteResult containing deletion details
     * @throws IllegalArgumentException if objectId is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public DeleteResult deleteOne(final ObjectId objectId) {
        return deleteOne(MongoDBBase.objectId2Filter(objectId));
    }

    /**
     * Deletes the first document that matches the filter criteria.
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("status", "expired");
     * DeleteResult result = executor.deleteOne(filter);
     * }</pre>
     * 
     * @param filter BSON filter to identify the document
     * @return DeleteResult containing deletion details
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public DeleteResult deleteOne(final Bson filter) {
        return coll.deleteOne(filter);
    }

    /**
     * Deletes a single document with additional options, including collation and hint specifications
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DeleteOptions options = new DeleteOptions().collation(collation);
     * executor.deleteOne(filter, options);
     * }</pre>
     * 
     * @param filter BSON filter to identify the document
     * @param options additional delete options (null uses defaults)
     * @return DeleteResult containing deletion details
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public DeleteResult deleteOne(final Bson filter, final DeleteOptions options) {
        return coll.deleteOne(filter, options);
    }

    /**
     * Deletes all documents that match the filter criteria (blocking operation).
     *
     * <p><b>Note:</b> This method performs a blocking operation. For non-blocking operations, use {@link #async()}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.lt("expiryDate", new Date());
     * DeleteResult result = executor.deleteMany(filter);
     * System.out.println("Deleted " + result.getDeletedCount() + " documents");
     * }</pre>
     * 
     * @param filter BSON filter to identify documents to delete
     * @return DeleteResult containing deletion details
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public DeleteResult deleteMany(final Bson filter) {
        return coll.deleteMany(filter);
    }

    /**
     * Deletes all matching documents with additional control through DeleteOptions.
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DeleteOptions options = new DeleteOptions().collation(collation);
     * DeleteResult result = executor.deleteMany(filter, options);
     * }</pre>
     * 
     * @param filter BSON filter to identify documents to delete
     * @param options additional delete options (null uses defaults)
     * @return DeleteResult containing deletion details
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public DeleteResult deleteMany(final Bson filter, final DeleteOptions options) {
        return coll.deleteMany(filter, options);
    }

    /**
     * Performs bulk insert of multiple entities.
     * 
     * <p>Efficiently inserts a collection of entities using MongoDB's bulk write API.
     * More efficient than individual inserts for large datasets.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<User> users = generateUsers(1000);
     * int inserted = executor.bulkInsert(users);
     * System.out.println("Inserted " + inserted + " users");
     * }</pre>
     * 
     * @param entities collection of entities to insert
     * @return number of documents inserted
     * @throws IllegalArgumentException if entities is null or empty
     * @throws com.mongodb.MongoBulkWriteException if bulk write fails
     */
    public int bulkInsert(final Collection<?> entities) {
        return bulkInsert(entities, null);
    }

    /**
     * Performs bulk insert with additional options.
     * 
     * <p>Provides control over bulk insert behavior including ordered/unordered
     * execution and write concern through BulkWriteOptions.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * BulkWriteOptions options = new BulkWriteOptions().ordered(false);
     * int inserted = executor.bulkInsert(entities, options);
     * }</pre>
     * 
     * @param entities collection of entities to insert
     * @param options additional bulk write options (null uses defaults)
     * @return number of documents inserted
     * @throws IllegalArgumentException if entities is null or empty
     * @throws com.mongodb.MongoBulkWriteException if bulk write fails
     */
    public int bulkInsert(final Collection<?> entities, final BulkWriteOptions options) {
        N.checkArgNotEmpty(entities, "entities");

        final List<InsertOneModel<Document>> list = new ArrayList<>(entities.size());

        for (final Object entity : entities) {
            if (entity instanceof Document) {
                list.add(new InsertOneModel<>((Document) entity));
            } else {
                list.add(new InsertOneModel<>(MongoDBBase.toDocument(entity)));
            }
        }

        return bulkWrite(list, options).getInsertedCount();
    }

    /**
     * Executes bulk write operations.
     * 
     * <p>Performs multiple write operations (insert, update, delete) in a single
     * batch for maximum efficiency.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<WriteModel<Document>> operations = Arrays.asList(
     *     new InsertOneModel<>(doc1),
     *     new UpdateOneModel<>(filter, update),
     *     new DeleteOneModel<>(deleteFilter)
     * );
     * BulkWriteResult result = executor.bulkWrite(operations);
     * }</pre>
     * 
     * @param requests list of write operations to execute
     * @return BulkWriteResult containing operation details
     * @throws IllegalArgumentException if requests is null or empty
     * @throws com.mongodb.MongoBulkWriteException if bulk write fails
     */
    public BulkWriteResult bulkWrite(final List<? extends WriteModel<? extends Document>> requests) {
        return bulkWrite(requests, null);
    }

    /**
     * Executes bulk write operations with additional options.
     * 
     * <p>Provides full control over bulk write execution including ordered/unordered
     * processing and write concern.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * BulkWriteOptions options = new BulkWriteOptions()
     *     .ordered(false)
     *     .bypassDocumentValidation(true);
     * BulkWriteResult result = executor.bulkWrite(operations, options);
     * }</pre>
     * 
     * @param requests list of write operations to execute
     * @param options additional bulk write options (null uses defaults)
     * @return BulkWriteResult containing operation details
     * @throws IllegalArgumentException if requests is null or empty
     * @throws com.mongodb.MongoBulkWriteException if bulk write fails
     */
    public BulkWriteResult bulkWrite(final List<? extends WriteModel<? extends Document>> requests, final BulkWriteOptions options) {
        N.checkArgNotEmpty(requests, "requests");

        if (options == null) {
            return coll.bulkWrite(requests);
        } else {
            return coll.bulkWrite(requests, options);
        }
    }

    /**
     * Finds and updates a single document atomically.
     * 
     * <p>Atomically finds a document and updates it, returning the original document.
     * Useful for implementing locks or counters.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("_id", id);
     * Bson update = Updates.inc("counter", 1);
     * Document original = executor.findOneAndUpdate(filter, update);
     * }</pre>
     * 
     * @param filter BSON filter to identify the document
     * @param update update operations to apply
     * @return the original document before update, or null if not found
     * @throws IllegalArgumentException if filter or update is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public Document findOneAndUpdate(final Bson filter, final Object update) {
        return findOneAndUpdate(filter, update, (FindOneAndUpdateOptions) null);
    }

    /**
     * Finds and updates a single document atomically with type conversion.
     * 
     * <p>Atomically finds and updates a document, returning the result as the specified type.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User original = executor.findOneAndUpdate(filter, update, User.class);
     * }</pre>
     * 
     * @param <T> the target type for the result
     * @param filter BSON filter to identify the document
     * @param update update operations to apply
     * @param rowType class to convert the result to
     * @return the original document as the specified type, or null if not found
     * @throws IllegalArgumentException if parameters are null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public <T> T findOneAndUpdate(final Bson filter, final Object update, final Class<T> rowType) {
        return findOneAndUpdate(filter, update, null, rowType);
    }

    /**
     * Finds and updates a single document atomically with options.
     * 
     * <p>Provides full control over the find-and-update operation including
     * projection, sort, upsert, and whether to return the original or updated document.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndUpdateOptions options = new FindOneAndUpdateOptions()
     *     .returnDocument(ReturnDocument.AFTER)
     *     .upsert(true);
     * Document updated = executor.findOneAndUpdate(filter, update, options);
     * }</pre>
     * 
     * @param filter BSON filter to identify the document
     * @param update update operations to apply
     * @param options additional options (null uses defaults)
     * @return the document (original or updated based on options), or null if not found
     * @throws IllegalArgumentException if filter or update is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public Document findOneAndUpdate(final Bson filter, final Object update, final FindOneAndUpdateOptions options) {
        if (options == null) {
            return coll.findOneAndUpdate(filter, toBson(update));
        } else {
            return coll.findOneAndUpdate(filter, toBson(update), options);
        }
    }

    /**
     * Finds and updates a single document atomically with options and type conversion.
     * 
     * <p>Combines atomic find-and-update with type conversion and full option control.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndUpdateOptions options = new FindOneAndUpdateOptions()
     *     .returnDocument(ReturnDocument.AFTER);
     * User updated = executor.findOneAndUpdate(filter, update, options, User.class);
     * }</pre>
     * 
     * @param <T> the target type for the result
     * @param filter BSON filter to identify the document
     * @param update update operations to apply
     * @param options additional options (null uses defaults)
     * @param rowType class to convert the result to
     * @return the document as the specified type, or null if not found
     * @throws IllegalArgumentException if filter, update, or rowType is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public <T> T findOneAndUpdate(final Bson filter, final Object update, final FindOneAndUpdateOptions options, final Class<T> rowType) {
        if (options == null) {
            return toEntity(coll.findOneAndUpdate(filter, toBson(update)), rowType);
        } else {
            return toEntity(coll.findOneAndUpdate(filter, toBson(update), options), rowType);
        }
    }

    /**
     * Finds and updates a document using multiple update operations.
     * 
     * <p>Atomically applies multiple update operations to a single document.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Bson> updates = Arrays.asList(
     *     Updates.set("status", "processed"),
     *     Updates.currentDate("lastModified")
     * );
     * Document result = executor.findOneAndUpdate(filter, updates);
     * }</pre>
     * 
     * @param filter BSON filter to identify the document
     * @param objList collection of update operations
     * @return the original document before update, or null if not found
     * @throws IllegalArgumentException if parameters are null or empty
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public Document findOneAndUpdate(final Bson filter, final Collection<?> objList) {
        return findOneAndUpdate(filter, objList, (FindOneAndUpdateOptions) null);
    }

    /**
     * Finds and updates a document using multiple operations with type conversion.
     * 
     * <p>Atomically applies multiple updates and returns the result as the specified type.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Order original = executor.findOneAndUpdate(filter, updatesList, Order.class);
     * }</pre>
     * 
     * @param <T> the target type for the result
     * @param filter BSON filter to identify the document
     * @param objList collection of update operations
     * @param rowType class to convert the result to
     * @return the original document as the specified type, or null if not found
     * @throws IllegalArgumentException if parameters are null or empty
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public <T> T findOneAndUpdate(final Bson filter, final Collection<?> objList, final Class<T> rowType) {
        return findOneAndUpdate(filter, objList, null, rowType);
    }

    /**
     * Finds and updates a document using multiple operations with options.
     * 
     * <p>Provides full control over atomic update with multiple operations.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndUpdateOptions options = new FindOneAndUpdateOptions()
     *     .returnDocument(ReturnDocument.AFTER);
     * Document updated = executor.findOneAndUpdate(filter, updatesList, options);
     * }</pre>
     * 
     * @param filter BSON filter to identify the document
     * @param objList collection of update operations
     * @param options additional options (null uses defaults)
     * @return the document (original or updated based on options), or null if not found
     * @throws IllegalArgumentException if filter or objList is null or empty
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public Document findOneAndUpdate(final Bson filter, final Collection<?> objList, final FindOneAndUpdateOptions options) {
        N.checkArgNotNull(filter, "filter");

        final List<Bson> updateToUse = toBson(objList);

        if (options == null) {
            return coll.findOneAndUpdate(filter, updateToUse);
        } else {
            return coll.findOneAndUpdate(filter, updateToUse, options);
        }
    }

    /**
     * Finds and updates using multiple operations with options and type conversion.
     * 
     * <p>The most flexible find-and-update method with multiple operations.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndUpdateOptions options = new FindOneAndUpdateOptions()
     *     .projection(Projections.include("name", "status"));
     * User result = executor.findOneAndUpdate(filter, updatesList, options, User.class);
     * }</pre>
     * 
     * @param <T> the target type for the result
     * @param filter BSON filter to identify the document
     * @param objList collection of update operations
     * @param options additional options (null uses defaults)
     * @param rowType class to convert the result to
     * @return the document as the specified type, or null if not found
     * @throws IllegalArgumentException if parameters are null or empty
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public <T> T findOneAndUpdate(final Bson filter, final Collection<?> objList, final FindOneAndUpdateOptions options, final Class<T> rowType) {
        final List<Bson> updateToUse = toBson(objList);

        if (options == null) {
            return toEntity(coll.findOneAndUpdate(filter, updateToUse), rowType);
        } else {
            return toEntity(coll.findOneAndUpdate(filter, updateToUse, options), rowType);
        }
    }

    /**
     * Finds and replaces a single document atomically.
     * 
     * <p>Atomically finds a document and replaces it entirely, returning the original.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User newUser = new User("John", "john@example.com");
     * Document original = executor.findOneAndReplace(filter, newUser);
     * }</pre>
     * 
     * @param filter BSON filter to identify the document
     * @param replacement the replacement document
     * @return the original document before replacement, or null if not found
     * @throws IllegalArgumentException if filter or replacement is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public Document findOneAndReplace(final Bson filter, final Object replacement) {
        return findOneAndReplace(filter, replacement, (FindOneAndReplaceOptions) null);
    }

    /**
     * Finds and replaces a document atomically with type conversion.
     * 
     * <p>Atomically replaces a document and returns the result as the specified type.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User original = executor.findOneAndReplace(filter, newUser, User.class);
     * }</pre>
     * 
     * @param <T> the target type for the result
     * @param filter BSON filter to identify the document
     * @param replacement the replacement document
     * @param rowType class to convert the result to
     * @return the original document as the specified type, or null if not found
     * @throws IllegalArgumentException if parameters are null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public <T> T findOneAndReplace(final Bson filter, final Object replacement, final Class<T> rowType) {
        return findOneAndReplace(filter, replacement, null, rowType);
    }

    /**
     * Finds and replaces a document atomically with options.
     * 
     * <p>Provides full control over the find-and-replace operation including
     * projection, sort, upsert, and whether to return the original or new document.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndReplaceOptions options = new FindOneAndReplaceOptions()
     *     .returnDocument(ReturnDocument.AFTER)
     *     .upsert(true);
     * Document newDoc = executor.findOneAndReplace(filter, replacement, options);
     * }</pre>
     * 
     * @param filter BSON filter to identify the document
     * @param replacement the replacement document
     * @param options additional options (null uses defaults)
     * @return the document (original or new based on options), or null if not found
     * @throws IllegalArgumentException if filter or replacement is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public Document findOneAndReplace(final Bson filter, final Object replacement, final FindOneAndReplaceOptions options) {
        if (options == null) {
            return coll.findOneAndReplace(filter, toDocument(replacement));
        } else {
            return coll.findOneAndReplace(filter, toDocument(replacement), options);
        }
    }

    /**
     * Finds and replaces with options and type conversion.
     * 
     * <p>The most flexible find-and-replace method with full option control and type conversion.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndReplaceOptions options = new FindOneAndReplaceOptions()
     *     .projection(Projections.exclude("_id"));
     * User result = executor.findOneAndReplace(filter, newUser, options, User.class);
     * }</pre>
     * 
     * @param <T> the target type for the result
     * @param filter BSON filter to identify the document
     * @param replacement the replacement document
     * @param options additional options (null uses defaults)
     * @param rowType class to convert the result to
     * @return the document as the specified type, or null if not found
     * @throws IllegalArgumentException if filter, replacement, or rowType is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public <T> T findOneAndReplace(final Bson filter, final Object replacement, final FindOneAndReplaceOptions options, final Class<T> rowType) {
        if (options == null) {
            return toEntity(coll.findOneAndReplace(filter, toDocument(replacement)), rowType);
        } else {
            return toEntity(coll.findOneAndReplace(filter, toDocument(replacement), options), rowType);
        }
    }

    /**
     * Finds and deletes a single document atomically.
     * 
     * <p>Atomically finds and deletes a document, returning the deleted document.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.eq("status", "expired");
     * Document deleted = executor.findOneAndDelete(filter);
     * }</pre>
     * 
     * @param filter BSON filter to identify the document
     * @return the deleted document, or null if not found
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public Document findOneAndDelete(final Bson filter) {
        return findOneAndDelete(filter, (FindOneAndDeleteOptions) null);
    }

    /**
     * Finds and deletes a document atomically with type conversion.
     * 
     * <p>Atomically deletes a document and returns it as the specified type.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User deleted = executor.findOneAndDelete(filter, User.class);
     * }</pre>
     * 
     * @param <T> the target type for the result
     * @param filter BSON filter to identify the document
     * @param rowType class to convert the result to
     * @return the deleted document as the specified type, or null if not found
     * @throws IllegalArgumentException if filter or rowType is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public <T> T findOneAndDelete(final Bson filter, final Class<T> rowType) {
        return findOneAndDelete(filter, null, rowType);
    }

    /**
     * Finds and deletes a document atomically with options.
     * 
     * <p>Provides control over the find-and-delete operation including
     * projection, sort, and collation.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndDeleteOptions options = new FindOneAndDeleteOptions()
     *     .sort(Sorts.ascending("priority"));
     * Document deleted = executor.findOneAndDelete(filter, options);
     * }</pre>
     * 
     * @param filter BSON filter to identify the document
     * @param options additional options (null uses defaults)
     * @return the deleted document, or null if not found
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public Document findOneAndDelete(final Bson filter, final FindOneAndDeleteOptions options) {
        if (options == null) {
            return coll.findOneAndDelete(filter);
        } else {
            return coll.findOneAndDelete(filter, options);
        }
    }

    /**
     * Finds and deletes with options and type conversion.
     * 
     * <p>The most flexible find-and-delete method with full option control and type conversion.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndDeleteOptions options = new FindOneAndDeleteOptions()
     *     .projection(Projections.include("name", "email"));
     * User deleted = executor.findOneAndDelete(filter, options, User.class);
     * }</pre>
     * 
     * @param <T> the target type for the result
     * @param filter BSON filter to identify the document
     * @param options additional options (null uses defaults)
     * @param rowType class to convert the result to
     * @return the deleted document as the specified type, or null if not found
     * @throws IllegalArgumentException if filter or rowType is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public <T> T findOneAndDelete(final Bson filter, final FindOneAndDeleteOptions options, final Class<T> rowType) {
        if (options == null) {
            return toEntity(coll.findOneAndDelete(filter), rowType);
        } else {
            return toEntity(coll.findOneAndDelete(filter, options), rowType);
        }
    }

    /**
     * Returns a stream of distinct values for the specified field.
     * 
     * <p>Retrieves all unique values for a field across the collection.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * executor.distinct("category", String.class)
     *     .forEach(category -> System.out.println("Category: " + category));
     * }</pre>
     * 
     * @param <T> the type of the distinct values
     * @param fieldName the field to get distinct values for
     * @param rowType the class of the field values
     * @return a Stream of distinct values
     * @throws IllegalArgumentException if fieldName or rowType is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public <T> Stream<T> distinct(final String fieldName, final Class<T> rowType) {
        final MongoCursor<T> cursor = coll.distinct(fieldName, rowType).iterator();

        return Stream.of(cursor).onClose(Fn.close(cursor));
    }

    /**
     * Returns a stream of distinct values with filtering.
     * 
     * <p>Retrieves unique values for a field from documents matching the filter.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.gte("price", 100);
     * executor.distinct("brand", filter, String.class)
     *     .forEach(brand -> System.out.println("Premium brand: " + brand));
     * }</pre>
     * 
     * @param <T> the type of the distinct values
     * @param fieldName the field to get distinct values for
     * @param filter BSON filter to apply before getting distinct values
     * @param rowType the class of the field values
     * @return a Stream of distinct values from filtered documents
     * @throws IllegalArgumentException if parameters are null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public <T> Stream<T> distinct(final String fieldName, final Bson filter, final Class<T> rowType) {
        final MongoCursor<T> cursor = coll.distinct(fieldName, filter, rowType).iterator();

        return Stream.of(cursor).onClose(Fn.close(cursor));
    }

    /**
     * Executes an aggregation pipeline on the collection.
     * 
     * <p>Processes documents through a series of stages to transform and analyze data.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Bson> pipeline = Arrays.asList(
     *     Aggregates.match(Filters.eq("status", "active")),
     *     Aggregates.group("$category", Accumulators.sum("total", 1))
     * );
     * executor.aggregate(pipeline)
     *     .forEach(doc -> System.out.println(doc));
     * }</pre>
     * 
     * @param pipeline the aggregation pipeline stages
     * @return a Stream of aggregation results as Documents
     * @throws IllegalArgumentException if pipeline is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public Stream<Document> aggregate(final List<? extends Bson> pipeline) {
        return aggregate(pipeline, Document.class);
    }

    /**
     * Executes an aggregation pipeline with type conversion.
     * 
     * <p>Processes documents through an aggregation pipeline and converts results to the specified type.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Bson> pipeline = Arrays.asList(
     *     Aggregates.match(Filters.gte("score", 80)),
     *     Aggregates.project(Projections.include("name", "score"))
     * );
     * executor.aggregate(pipeline, StudentScore.class)
     *     .forEach(score -> processScore(score));
     * }</pre>
     * 
     * @param <T> the target type for results
     * @param pipeline the aggregation pipeline stages
     * @param rowType the class to convert results to
     * @return a Stream of typed aggregation results
     * @throws IllegalArgumentException if pipeline or rowType is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    public <T> Stream<T> aggregate(final List<? extends Bson> pipeline, final Class<T> rowType) {
        final MongoCursor<Document> cursor = coll.aggregate(pipeline, Document.class).iterator();

        return Stream.of(cursor).map(toEntity(rowType)).onClose(Fn.close(cursor));
    }

    /**
     * Groups documents by a single field.
     * 
     * <p>A convenience method for simple grouping operations. Creates an aggregation
     * pipeline that groups documents by the specified field.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * executor.groupBy("category")
     *     .forEach(group -> System.out.println(group));
     * }</pre>
     * 
     * @param fieldName the field to group by
     * @return a Stream of grouped documents
     * @throws IllegalArgumentException if fieldName is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    @Beta
    public Stream<Document> groupBy(final String fieldName) {
        return groupBy(fieldName, Document.class);
    }

    /**
     * Groups documents by a single field with type conversion.
     * 
     * <p>Groups documents and converts results to the specified type.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * executor.groupBy("department", DepartmentGroup.class)
     *     .forEach(group -> processDepartment(group));
     * }</pre>
     * 
     * @param <T> the target type for results
     * @param fieldName the field to group by
     * @param rowType the class to convert results to
     * @return a Stream of typed grouped documents
     * @throws IllegalArgumentException if fieldName or rowType is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    @Beta
    public <T> Stream<T> groupBy(final String fieldName, final Class<T> rowType) {
        return aggregate(N.asList(new Document(_$GROUP, new Document(MongoDBBase._ID, _$ + fieldName))), rowType);
    }

    /**
     * Groups documents by multiple fields.
     * 
     * <p>Creates an aggregation pipeline that groups documents by multiple fields.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<String> fields = Arrays.asList("category", "status");
     * executor.groupBy(fields)
     *     .forEach(group -> System.out.println(group));
     * }</pre>
     * 
     * @param fieldNames collection of fields to group by
     * @return a Stream of grouped documents
     * @throws IllegalArgumentException if fieldNames is null or empty
     * @throws com.mongodb.MongoException if the database operation fails
     */
    @Beta
    public Stream<Document> groupBy(final Collection<String> fieldNames) {
        return groupBy(fieldNames, Document.class);
    }

    /**
     * Groups documents by multiple fields with type conversion.
     * 
     * <p>Groups by multiple fields and converts results to the specified type.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<String> fields = Arrays.asList("year", "month");
     * executor.groupBy(fields, MonthlyGroup.class)
     *     .forEach(group -> processMonthly(group));
     * }</pre>
     * 
     * @param <T> the target type for results
     * @param fieldNames collection of fields to group by
     * @param rowType the class to convert results to
     * @return a Stream of typed grouped documents
     * @throws IllegalArgumentException if fieldNames is empty or rowType is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    @Beta
    public <T> Stream<T> groupBy(final Collection<String> fieldNames, final Class<T> rowType) {
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
     * <p>A convenience method that groups documents and includes a count of documents in each group.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * executor.groupByAndCount("status")
     *     .forEach(group -> System.out.println(
     *         "Status: " + group.get("_id") + ", Count: " + group.get("count")));
     * }</pre>
     * 
     * @param fieldName the field to group by
     * @return a Stream of documents with group id and count
     * @throws IllegalArgumentException if fieldName is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    @Beta
    public Stream<Document> groupByAndCount(final String fieldName) {
        return groupByAndCount(fieldName, Document.class);
    }

    /**
     * Groups and counts with type conversion.
     * 
     * <p>Groups documents, counts frequency, and converts results to the specified type.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * executor.groupByAndCount("category", CategoryCount.class)
     *     .forEach(count -> processCount(count));
     * }</pre>
     * 
     * @param <T> the target type for results
     * @param fieldName the field to group by
     * @param rowType the class to convert results to
     * @return a Stream of typed documents with counts
     * @throws IllegalArgumentException if fieldName or rowType is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    @Beta
    public <T> Stream<T> groupByAndCount(final String fieldName, final Class<T> rowType) {
        return aggregate(N.asList(new Document(_$GROUP, new Document(MongoDBBase._ID, _$ + fieldName).append(_COUNT, new Document(_$SUM, 1)))), rowType);
    }

    /**
     * Groups documents by multiple fields and counts frequency.
     * 
     * <p>Groups by multiple fields and includes a count of documents in each group.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<String> fields = Arrays.asList("category", "status");
     * executor.groupByAndCount(fields)
     *     .forEach(group -> processGroupCount(group));
     * }</pre>
     * 
     * @param fieldNames collection of fields to group by
     * @return a Stream of documents with group ids and counts
     * @throws IllegalArgumentException if fieldNames is null or empty
     * @throws com.mongodb.MongoException if the database operation fails
     */
    @Beta
    public Stream<Document> groupByAndCount(final Collection<String> fieldNames) {
        return groupByAndCount(fieldNames, Document.class);
    }

    /**
     * Groups by multiple fields, counts, and converts to type.
     * 
     * <p>The most flexible grouping and counting method with type conversion.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<String> fields = Arrays.asList("year", "quarter");
     * executor.groupByAndCount(fields, QuarterlyCount.class)
     *     .forEach(count -> processQuarterly(count));
     * }</pre>
     * 
     * @param <T> the target type for results
     * @param fieldNames collection of fields to group by
     * @param rowType the class to convert results to
     * @return a Stream of typed documents with counts
     * @throws IllegalArgumentException if fieldNames is empty or rowType is null
     * @throws com.mongodb.MongoException if the database operation fails
     */
    @Beta
    public <T> Stream<T> groupByAndCount(final Collection<String> fieldNames, final Class<T> rowType) {
        N.checkArgNotEmpty(fieldNames, "fieldNames");
        N.checkArgNotNull(rowType, "rowType");

        final Document groupFields = new Document();

        for (final String fieldName : fieldNames) {
            groupFields.put(fieldName, _$ + fieldName);
        }

        return aggregate(N.asList(new Document(_$GROUP, new Document(MongoDBBase._ID, groupFields).append(_COUNT, new Document(_$SUM, 1)))), rowType);
    }

    /**
     * Executes a map-reduce operation on the collection.
     * 
     * <p>Performs server-side JavaScript processing using map and reduce functions.
     * Note: Map-reduce is deprecated in MongoDB 5.0+. Use aggregation pipeline instead.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String mapFunction = "function() { emit(this.category, 1); }";
     * String reduceFunction = "function(key, values) { return Array.sum(values); }";
     * executor.mapReduce(mapFunction, reduceFunction)
     *     .forEach(result -> System.out.println(result));
     * }</pre>
     * 
     * @param mapFunction JavaScript map function as string
     * @param reduceFunction JavaScript reduce function as string
     * @return a Stream of map-reduce results
     * @throws IllegalArgumentException if functions are null
     * @throws com.mongodb.MongoException if the database operation fails
     * @deprecated Map-reduce is deprecated in MongoDB 5.0+. Use aggregate() instead.
     */
    @Deprecated
    public Stream<Document> mapReduce(final String mapFunction, final String reduceFunction) {
        return mapReduce(mapFunction, reduceFunction, Document.class);
    }

    /**
     * Executes map-reduce with type conversion.
     * 
     * <p>Performs map-reduce and converts results to the specified type.
     * Note: Map-reduce is deprecated in MongoDB 5.0+. Use aggregation pipeline instead.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * executor.mapReduce(mapFunction, reduceFunction, CategoryTotal.class)
     *     .forEach(total -> processTotal(total));
     * }</pre>
     * 
     * @param <T> the target type for results
     * @param mapFunction JavaScript map function as string
     * @param reduceFunction JavaScript reduce function as string
     * @param rowType the class to convert results to
     * @return a Stream of typed map-reduce results
     * @throws IllegalArgumentException if parameters are null
     * @throws com.mongodb.MongoException if the database operation fails
     * @deprecated Map-reduce is deprecated in MongoDB 5.0+. Use aggregate() instead.
     */
    @Deprecated
    public <T> Stream<T> mapReduce(final String mapFunction, final String reduceFunction, final Class<T> rowType) {
        final MongoCursor<Document> cursor = coll.mapReduce(mapFunction, reduceFunction, Document.class).iterator();

        return Stream.of(cursor).map(toEntity(rowType)).onClose(Fn.close(cursor));
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
