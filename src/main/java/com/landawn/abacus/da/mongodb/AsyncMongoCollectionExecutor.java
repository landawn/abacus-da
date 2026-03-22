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

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.landawn.abacus.annotation.Beta;
import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.Dataset;
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
import com.landawn.abacus.util.stream.Stream;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.ChangeStreamIterable;
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
import com.mongodb.client.result.UpdateResult;

/**
 * Asynchronous MongoDB collection executor providing non-blocking database operations with CompletableFuture support.
 *
 * <p>This class wraps the synchronous {@code MongoCollectionExecutor} to provide asynchronous execution
 * of all MongoDB operations. Each method returns a {@code ContinuableFuture} that can be used for
 * non-blocking programming patterns, reactive streams integration, and parallel processing.</p>
 *
 * <h2>Key Features</h2>
 * <h3>Core Capabilities:</h3>
 * <ul>
 *   <li><strong>Non-blocking Operations:</strong> All database operations return immediately with futures</li>
 *   <li><strong>Thread Pool Management:</strong> Uses configurable AsyncExecutor for thread pool control</li>
 *   <li><strong>Future Composition:</strong> ContinuableFuture supports chaining and transformation</li>
 *   <li><strong>Error Handling:</strong> Exceptions are propagated through the future completion</li>
 *   <li><strong>Reactive Integration:</strong> Compatible with reactive streams and CompletableFuture APIs</li>
 * </ul>
 *
 * <h3>Thread Safety:</h3>
 * <p>This class is thread-safe. All methods can be called concurrently from multiple threads.
 * The underlying executor manages thread pool and task scheduling safely.</p>
 *
 * <h3>Performance Considerations:</h3>
 * <ul>
 *   <li>Configure AsyncExecutor thread pool based on expected concurrent operations</li>
 *   <li>Use future composition to avoid blocking on individual operations</li>
 *   <li>Consider batch operations for multiple database calls</li>
 *   <li>Monitor thread pool utilization to prevent resource exhaustion</li>
 * </ul>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * AsyncMongoCollectionExecutor async = executor.async();
 * 
 * // Non-blocking existence check:
 * ContinuableFuture<Boolean> existsFuture = async.exists("507f1f77bcf86cd799439011");
 * existsFuture.thenRunAsync(exists -> {
 *     if (exists) {
 *         System.out.println("Document found");
 *     }
 * });
 * 
 * // Chained operations:
 * async.findFirst(Filters.eq("status", "pending"))
 *      .thenCompose(doc -> doc.isPresent() ?
 *          async.updateOne(doc.get(), Updates.set("status", "processing")) :
 *          ContinuableFuture.completedFuture(null))
 *      .thenRunAsync(result -> System.out.println("Update completed"));
 *
 * // Parallel operations:
 * ContinuableFuture<Void> allOps = ContinuableFuture.allOf(
 *     async.count(Filters.eq("active", true)),
 *     async.findFirst(Filters.eq("priority", "high")),
 *     async.estimatedDocumentCount()
 * );
 * }</pre>
 *
 * @see MongoCollectionExecutor
 * @see ContinuableFuture
 * @see AsyncExecutor
 * @see com.mongodb.client.model.Filters
 * @see com.mongodb.client.model.Projections
 * @see com.mongodb.client.model.Sorts
 * @see com.mongodb.client.model.Updates
 * @see com.mongodb.client.model.Aggregates
 * @see com.mongodb.client.model.Indexes
 * @see <a href="https://www.mongodb.com/docs/drivers/java/sync/current/fundamentals/builders/">Simplify your Code with Builders</a>
 * @see <a href="https://www.mongodb.com/docs/drivers/java/sync/current/">MongoDB Java Driver</a>
 */
public final class AsyncMongoCollectionExecutor {

    private final MongoCollectionExecutor collExecutor;

    private final AsyncExecutor asyncExecutor;

    AsyncMongoCollectionExecutor(final MongoCollectionExecutor collExecutor, final AsyncExecutor asyncExecutor) {
        this.collExecutor = collExecutor;
        this.asyncExecutor = asyncExecutor;
    }

    /**
     * Returns the underlying synchronous MongoCollectionExecutor for blocking operations.
     *
     * <p>This method provides access to the synchronous executor when blocking operations are
     * needed or when integrating with synchronous code paths. The returned executor shares
     * the same MongoDB collection and configuration as this async executor.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncMongoCollectionExecutor async = executor.async();
     * MongoCollectionExecutor sync = async.sync();
     * Optional<Document> doc = sync.findFirst(Filters.eq("urgent", true));
     * }</pre>
     *
     * @return the synchronous MongoCollectionExecutor instance
     * @see MongoCollectionExecutor
     */
    public MongoCollectionExecutor sync() {
        return collExecutor;
    }

    /**
     * Asynchronously checks if a document exists by its ObjectId string representation.
     *
     * <p>This method returns immediately with a ContinuableFuture that will complete with
     * the existence check result. The ObjectId string must be valid 24-character hexadecimal.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.exists("507f1f77bcf86cd799439011")
     *      .thenRunAsync(exists -> System.out.println("User exists: " + exists));
     * }</pre>
     *
     * @param objectId the string representation of the ObjectId to check
     * @return a ContinuableFuture that completes with {@code true} if the document exists, {@code false} otherwise
     * @throws IllegalArgumentException if objectId is null, empty, or invalid format (propagated through future)
     * @see ContinuableFuture
     * @see #exists(ObjectId)
     */
    public ContinuableFuture<Boolean> exists(final String objectId) {
        return asyncExecutor.execute(() -> collExecutor.exists(objectId));
    }

    /**
     * Asynchronously checks if a document exists by its ObjectId.
     *
     * <p>This method performs non-blocking existence verification using the native ObjectId type.
     * The operation is executed on the configured thread pool and the result is delivered
     * through the returned ContinuableFuture.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId id = new ObjectId();
     * async.exists(id).thenRunAsync(exists -> processExistence(exists));
     * }</pre>
     *
     * @param objectId the ObjectId to check for existence
     * @return a ContinuableFuture that completes with {@code true} if the document exists, {@code false} otherwise
     * @throws IllegalArgumentException if objectId is null (propagated through future)
     * @see ObjectId
     * @see ContinuableFuture
     */
    public ContinuableFuture<Boolean> exists(final ObjectId objectId) {
        return asyncExecutor.execute(() -> collExecutor.exists(objectId));
    }

    /**
     * Asynchronously checks if any documents exist matching the specified filter.
     *
     * <p>This method performs a non-blocking existence check using the provided filter criteria.
     * It's more efficient than retrieving documents when you only need to verify existence.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.exists(Filters.eq("status", "active"))
     *      .thenRunAsync(hasActive -> System.out.println("Has active users: " + hasActive));
     * }</pre>
     *
     * @param filter the query filter to match documents against
     * @return a ContinuableFuture that completes with {@code true} if matching documents exist, {@code false} otherwise
     * @throws IllegalArgumentException if filter is null (propagated through future)
     * @see com.mongodb.client.model.Filters
     * @see ContinuableFuture
     */
    public ContinuableFuture<Boolean> exists(final Bson filter) {
        return asyncExecutor.execute(() -> collExecutor.exists(filter));
    }

    /**
     * Asynchronously returns the total number of documents in the collection.
     *
     * <p>This method performs a non-blocking count of all documents in the collection.
     * The operation is executed on the thread pool and the count result is delivered
     * through the returned ContinuableFuture.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.count().thenRunAsync(total -> System.out.println("Total documents: " + total));
     * }</pre>
     *
     * @return a ContinuableFuture that completes with the total document count
     * @see ContinuableFuture
     * @see #count(Bson)
     */
    public ContinuableFuture<Long> count() {
        return asyncExecutor.execute((Callable<Long>) collExecutor::count);
    }

    /**
     * Asynchronously returns the number of documents matching the specified filter.
     *
     * <p>This method performs a non-blocking count operation on documents that match
     * the given filter criteria. The count respects any read preference settings
     * configured on the collection.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.count(Filters.eq("status", "active"))
     *      .thenRunAsync(activeCount -> System.out.println("Active users: " + activeCount));
     * }</pre>
     *
     * @param filter the query filter to count matching documents
     * @return a ContinuableFuture that completes with the count of matching documents
     * @throws IllegalArgumentException if filter is null (propagated through future)
     * @see com.mongodb.client.model.Filters
     * @see ContinuableFuture
     */
    public ContinuableFuture<Long> count(final Bson filter) {
        return asyncExecutor.execute(() -> collExecutor.count(filter));
    }

    /**
     * Asynchronously returns the count of documents matching the filter with additional options.
     *
     * <p>This method provides non-blocking count operations with fine-grained control through
     * CountOptions, including limits, skips, and hints for query optimization.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CountOptions options = new CountOptions().limit(1000).maxTime(5, TimeUnit.SECONDS);
     * async.count(Filters.exists("email"), options)
     *      .thenRunAsync(count -> System.out.println("Users with email: " + count));
     * }</pre>
     *
     * @param filter the query filter to count matching documents
     * @param options additional options for the count operation (null uses defaults)
     * @return a ContinuableFuture that completes with the count within the specified constraints
     * @throws IllegalArgumentException if filter is null (propagated through future)
     * @see CountOptions
     * @see ContinuableFuture
     */
    public ContinuableFuture<Long> count(final Bson filter, final CountOptions options) {
        return asyncExecutor.execute(() -> collExecutor.count(filter, options));
    }

    /**
     * Asynchronously retrieves a document by its ObjectId string representation.
     *
     * <p>This method performs a non-blocking find operation using the provided ObjectId string
     * to locate a single document. The ObjectId string must be a valid 24-character hexadecimal
     * representation. The operation is executed on the configured thread pool.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.get("507f1f77bcf86cd799439011")
     *      .thenRunAsync(docOpt -> docOpt.ifPresent(doc -> processDocument(doc)));
     * }</pre>
     *
     * @param objectId the string representation of the ObjectId to search for
     * @return a ContinuableFuture that completes with an Optional containing the document if found, or empty if not found
     * @throws IllegalArgumentException if objectId is null, empty, or invalid format (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see Optional
     * @see Document
     * @see #get(ObjectId)
     */
    public ContinuableFuture<Optional<Document>> get(final String objectId) {
        return asyncExecutor.execute(() -> collExecutor.get(objectId));
    }

    /**
     * Asynchronously retrieves a document by its ObjectId.
     *
     * <p>This method performs a non-blocking find operation using the native ObjectId type
     * to locate a single document in the collection efficiently. The operation is executed
     * on the configured thread pool.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId id = new ObjectId();
     * async.get(id).thenRunAsync(docOpt -> docOpt.ifPresent(doc -> processDocument(doc)));
     * }</pre>
     *
     * @param objectId the ObjectId to search for
     * @return a ContinuableFuture that completes with an Optional containing the document if found, or empty if not found
     * @throws IllegalArgumentException if objectId is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see ObjectId
     * @see Optional
     * @see Document
     */
    public ContinuableFuture<Optional<Document>> get(final ObjectId objectId) {
        return asyncExecutor.execute(() -> collExecutor.get(objectId));
    }

    /**
     * Asynchronously retrieves and converts a document by its ObjectId string with automatic type conversion.
     *
     * <p>This method combines non-blocking document retrieval with automatic conversion to the specified
     * target type. It supports entity classes, Map types, and primitive types based on document structure.
     * The operation is executed on the configured thread pool.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.get("507f1f77bcf86cd799439011", User.class)
     *      .thenRunAsync(userOpt -> userOpt.ifPresent(user -> System.out.println(user.getName())));
     * }</pre>
     *
     * @param <T> the target type for the retrieved document
     * @param objectId the string representation of the ObjectId to search for
     * @param rowType the Class representing the target type for conversion
     * @return a ContinuableFuture that completes with an Optional containing the converted object if found, or empty if not found
     * @throws IllegalArgumentException if objectId or rowType is null, or if objectId format is invalid (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #get(ObjectId, Class)
     */
    public <T> ContinuableFuture<Optional<T>> get(final String objectId, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.get(objectId, rowType));
    }

    /**
     * Asynchronously retrieves and converts a document by its ObjectId with automatic type conversion.
     *
     * <p>This method performs non-blocking document retrieval using the native ObjectId and converts
     * the result to the specified target type. It supports entity classes, collections, and primitive types.
     * The operation is executed on the configured thread pool.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId id = new ObjectId();
     * async.get(id, User.class)
     *      .thenRunAsync(userOpt -> userOpt.ifPresent(user -> processUser(user)));
     * }</pre>
     *
     * @param <T> the target type for the retrieved document
     * @param objectId the ObjectId to search for
     * @param rowType the Class representing the target type for conversion
     * @return a ContinuableFuture that completes with an Optional containing the converted object if found, or empty if not found
     * @throws IllegalArgumentException if objectId or rowType is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #get(ObjectId, Collection, Class)
     */
    public <T> ContinuableFuture<Optional<T>> get(final ObjectId objectId, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.get(objectId, rowType));
    }

    /**
     * Asynchronously retrieves a document with field projection by ObjectId string and converts to the specified type.
     *
     * <p>This method combines non-blocking document retrieval with field projection and type conversion. Only the
     * specified fields are retrieved from the database, reducing network bandwidth and improving performance for
     * documents with many fields. The operation is executed on the configured thread pool.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("name", "email");
     * async.get("507f1f77bcf86cd799439011", fields, User.class)
     *      .thenRunAsync(user -> user.ifPresent(u -> System.out.println(u.getName())));
     * }</pre>
     *
     * @param <T> the target type for the retrieved document
     * @param objectId the string representation of the ObjectId to search for
     * @param selectPropNames collection of field names to include in the projection (null for all fields)
     * @param rowType the Class representing the target type for conversion
     * @return a ContinuableFuture that completes with an Optional containing the converted object with projected fields, or empty if not found
     * @throws IllegalArgumentException if objectId or rowType is null, or objectId format is invalid (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #get(ObjectId, Collection, Class)
     */
    public <T> ContinuableFuture<Optional<T>> get(final String objectId, final Collection<String> selectPropNames, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.get(objectId, selectPropNames, rowType));
    }

    /**
     * Asynchronously retrieves a document with field projection by ObjectId and converts to the specified type.
     *
     * <p>This method provides non-blocking document retrieval with projection using the native ObjectId.
     * Field projection reduces the amount of data transferred and can significantly improve performance
     * when working with documents containing many fields or large embedded objects. The operation is
     * executed on the configured thread pool.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId id = new ObjectId();
     * Collection<String> fields = Set.of("name", "email");
     * async.get(id, fields, User.class)
     *      .thenRunAsync(user -> user.ifPresent(u -> processPartialUser(u)));
     * }</pre>
     *
     * @param <T> the target type for the retrieved document
     * @param objectId the ObjectId to search for
     * @param selectPropNames collection of field names to include (null includes all fields)
     * @param rowType the Class representing the target type for conversion
     * @return a ContinuableFuture that completes with an Optional containing the converted object with only the specified fields, or empty if not found
     * @throws IllegalArgumentException if objectId or rowType is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see com.mongodb.client.model.Projections
     */
    public <T> ContinuableFuture<Optional<T>> get(final ObjectId objectId, final Collection<String> selectPropNames, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.get(objectId, selectPropNames, rowType));
    }

    /**
     * Asynchronously retrieves a document by its ObjectId string, returning null if not found.
     *
     * <p>This method performs a non-blocking document retrieval that returns a ContinuableFuture completing
     * with the Document or {@code null} if no document matches the ObjectId. It is the historical
     * nullable counterpart to {@link #get(String)}. The {@code gett} name is preserved for API
     * compatibility; prefer {@link #get(String)} when Optional-based handling is clearer.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.gett("507f1f77bcf86cd799439011")
     *      .thenRunAsync(doc -> {
     *          if (doc != null) {
     *              String name = doc.getString("name");
     *          }
     *      });
     * }</pre>
     *
     * @param objectId the string representation of the ObjectId (24 hex characters)
     * @return a ContinuableFuture that completes with the matching document, or {@code null} if not found
     * @throws IllegalArgumentException if objectId is null, empty, or invalid format (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #get(String)
     * @see #gett(ObjectId)
     */
    public ContinuableFuture<Document> gett(final String objectId) {
        return asyncExecutor.execute(() -> collExecutor.gett(objectId));
    }

    /**
     * Asynchronously retrieves a document by its ObjectId, returning null if not found.
     *
     * <p>This method performs a non-blocking document retrieval using the native ObjectId type,
     * returning a ContinuableFuture that completes with the Document or {@code null} if no document
     * matches. Use this method when null handling is preferred over Optional patterns.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId id = new ObjectId();
     * async.gett(id)
     *      .thenRunAsync(doc -> {
     *          if (doc != null) {
     *              processDocument(doc);
     *          }
     *      });
     * }</pre>
     *
     * @param objectId the ObjectId to search for
     * @return a ContinuableFuture that completes with the matching document, or {@code null} if not found
     * @throws IllegalArgumentException if objectId is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #get(ObjectId)
     * @see #gett(String)
     */
    public ContinuableFuture<Document> gett(final ObjectId objectId) {
        return asyncExecutor.execute(() -> collExecutor.gett(objectId));
    }

    /**
     * Asynchronously retrieves and converts a document by its ObjectId string, returning null if not found.
     *
     * <p>This method performs a non-blocking document retrieval with automatic type conversion, returning
     * a ContinuableFuture that completes with the converted entity or {@code null} if no document matches.
     * It supports entity classes, Map types, and primitive types based on the document structure.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.gett("507f1f77bcf86cd799439011", User.class)
     *      .thenRunAsync(user -> {
     *          if (user != null) {
     *              sendWelcomeEmail(user.getEmail());
     *          }
     *      });
     * }</pre>
     *
     * @param <T> the target type for the retrieved document
     * @param objectId the string representation of the ObjectId (24 hex characters)
     * @param rowType the Class representing the target type for conversion
     * @return a ContinuableFuture that completes with the converted entity, or {@code null} if not found
     * @throws IllegalArgumentException if objectId or rowType is null, or if objectId format is invalid (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #get(String, Class)
     * @see #gett(ObjectId, Class)
     */
    public <T> ContinuableFuture<T> gett(final String objectId, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.gett(objectId, rowType));
    }

    /**
     * Asynchronously retrieves and converts a document by its ObjectId, returning null if not found.
     *
     * <p>This method performs a non-blocking document retrieval using the native ObjectId with automatic
     * type conversion. The ContinuableFuture completes with the converted entity or {@code null} if no
     * document matches. Use this method when null handling is preferred over Optional patterns.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId id = new ObjectId();
     * async.gett(id, User.class)
     *      .thenRunAsync(user -> {
     *          if (user != null) {
     *              System.out.println("Found: " + user.getName());
     *          }
     *      });
     * }</pre>
     *
     * @param <T> the target type for the retrieved document
     * @param objectId the ObjectId to search for
     * @param rowType the Class representing the target type for conversion
     * @return a ContinuableFuture that completes with the converted entity, or {@code null} if not found
     * @throws IllegalArgumentException if objectId or rowType is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #get(ObjectId, Class)
     * @see #gett(String, Class)
     */
    public <T> ContinuableFuture<T> gett(final ObjectId objectId, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.gett(objectId, rowType));
    }

    /**
     * Asynchronously retrieves a document with field projection by ObjectId string, returning null if not found.
     *
     * <p>This method performs a non-blocking document retrieval with field projection and type conversion.
     * Only the specified fields are retrieved from the database, reducing network bandwidth and improving
     * performance. The ContinuableFuture completes with the converted entity or {@code null} if not found.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("name", "email", "department");
     * async.gett("507f1f77bcf86cd799439011", fields, User.class)
     *      .thenRunAsync(user -> {
     *          if (user != null) {
     *              System.out.println("User department: " + user.getDepartment());
     *          }
     *      });
     * }</pre>
     *
     * @param <T> the target type for the retrieved document
     * @param objectId the string representation of the ObjectId (24 hex characters)
     * @param selectPropNames collection of field names to include in the projection (null for all fields)
     * @param rowType the Class representing the target type for conversion
     * @return a ContinuableFuture that completes with the converted entity with projected fields, or {@code null} if not found
     * @throws IllegalArgumentException if objectId or rowType is null, or if objectId format is invalid (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #get(String, Collection, Class)
     * @see #gett(ObjectId, Collection, Class)
     */
    public <T> ContinuableFuture<T> gett(final String objectId, final Collection<String> selectPropNames, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.gett(objectId, selectPropNames, rowType));
    }

    /**
     * Asynchronously retrieves a document with field projection by ObjectId, returning null if not found.
     *
     * <p>This method performs a non-blocking document retrieval with projection using the native ObjectId.
     * Field projection reduces the amount of data transferred and can significantly improve performance
     * when working with documents containing many fields or large embedded objects. The ContinuableFuture
     * completes with the converted entity or {@code null} if no document matches.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId id = new ObjectId();
     * Collection<String> fields = Set.of("name", "email");
     * async.gett(id, fields, User.class)
     *      .thenRunAsync(user -> {
     *          if (user != null) {
     *              sendEmail(user.getEmail());
     *          }
     *      });
     * }</pre>
     *
     * @param <T> the target type for the retrieved document
     * @param objectId the ObjectId to search for
     * @param selectPropNames collection of field names to include in the projection (null for all fields)
     * @param rowType the Class representing the target type for conversion
     * @return a ContinuableFuture that completes with the converted entity with projected fields, or {@code null} if not found
     * @throws IllegalArgumentException if objectId or rowType is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #get(ObjectId, Collection, Class)
     * @see com.mongodb.client.model.Projections
     */
    public <T> ContinuableFuture<T> gett(final ObjectId objectId, final Collection<String> selectPropNames, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.gett(objectId, selectPropNames, rowType));
    }

    /**
     * Asynchronously finds the first document matching the specified filter criteria.
     *
     * <p>This method performs a non-blocking find operation that returns at most one document matching
     * the given filter. It's equivalent to a find operation with a limit of 1. The result is wrapped
     * in an Optional to handle the case where no matching documents exist. The operation is executed
     * on the configured thread pool.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.findFirst(Filters.eq("status", "active"))
     *      .thenRunAsync(userOpt -> userOpt.ifPresent(user -> System.out.println(user.getString("name"))));
     * }</pre>
     *
     * @param filter the query filter to match documents against
     * @return a ContinuableFuture that completes with an Optional containing the first matching document, or empty if none found
     * @throws IllegalArgumentException if filter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see Optional
     * @see Document
     * @see com.mongodb.client.model.Filters
     */
    public ContinuableFuture<Optional<Document>> findFirst(final Bson filter) {
        return asyncExecutor.execute(() -> collExecutor.findFirst(filter));
    }

    /**
     * Asynchronously finds the first document matching the filter and converts it to the specified type.
     *
     * <p>This method performs a non-blocking find operation for the first document matching the filter
     * criteria, with automatic conversion to the target type. The operation returns a ContinuableFuture
     * that completes with an Optional containing the converted entity, or empty if no match is found.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.findFirst(Filters.eq("status", "active"), User.class)
     *      .thenRunAsync(user -> user.ifPresent(u -> processUser(u)));
     * }</pre>
     *
     * @param <T> the target type for the document conversion
     * @param filter BSON filter criteria to match documents (null for all documents)
     * @param rowType the target type for conversion of the document
     * @return a ContinuableFuture that completes with an Optional containing the converted entity, or empty if none found
     * @throws IllegalArgumentException if rowType is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #findFirst(Bson)
     * @see #findFirst(Collection, Bson, Class)
     */
    public <T> ContinuableFuture<Optional<T>> findFirst(final Bson filter, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.findFirst(filter, rowType));
    }

    /**
     * Asynchronously finds the first document with field projection and converts it to the specified type.
     *
     * <p>This method performs a non-blocking find operation with field projection for the first document
     * matching the filter criteria. Only the specified fields are retrieved from the database, reducing
     * network bandwidth and improving performance while providing automatic type conversion.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("name", "email");
     * async.findFirst(fields, Filters.eq("status", "active"), User.class)
     *      .thenRunAsync(user -> user.ifPresent(u -> System.out.println(u.getName())));
     * }</pre>
     *
     * @param <T> the target type for the document conversion
     * @param selectPropNames collection of field names to include in projection (null for all fields)
     * @param filter BSON filter criteria to match documents (null for all documents)
     * @param rowType the target type for conversion of the document
     * @return a ContinuableFuture that completes with an Optional containing the converted entity with projected fields, or empty if none found
     * @throws IllegalArgumentException if rowType is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #findFirst(Bson, Class)
     * @see #findFirst(Collection, Bson, Bson, Class)
     */
    public <T> ContinuableFuture<Optional<T>> findFirst(final Collection<String> selectPropNames, final Bson filter, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.findFirst(selectPropNames, filter, rowType));
    }

    /**
     * Asynchronously finds the first document with field projection and sorting, then converts it to the specified type.
     *
     * <p>This method performs a non-blocking find operation with field projection and sorting for the first document
     * matching the filter criteria. The sort parameter determines which document is considered "first" when multiple
     * documents match the filter. Only the specified fields are retrieved, improving performance.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("name", "score");
     * async.findFirst(fields, Filters.eq("status", "active"), Sorts.descending("score"), User.class)
     *      .thenRunAsync(topUser -> topUser.ifPresent(u -> System.out.println("Top user: " + u.getName())));
     * }</pre>
     *
     * @param <T> the target type for the document conversion
     * @param selectPropNames collection of field names to include in projection (null for all fields)
     * @param filter BSON filter criteria to match documents (null for all documents)
     * @param sort BSON sort criteria to determine document order (null for natural order)
     * @param rowType the target type for conversion of the document
     * @return a ContinuableFuture that completes with an Optional containing the converted entity with projected fields, or empty if none found
     * @throws IllegalArgumentException if rowType is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see com.mongodb.client.model.Sorts
     * @see #findFirst(Collection, Bson, Class)
     */
    public <T> ContinuableFuture<Optional<T>> findFirst(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.findFirst(selectPropNames, filter, sort, rowType));
    }

    /**
     * Asynchronously finds the first document with projection and sorting, then converts it to the specified type.
     *
     * <p>This method provides maximum control over the find operation with custom projection (using BSON),
     * filtering, and sorting. The projection parameter allows for complex field inclusion/exclusion patterns
     * beyond simple field name lists.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson projection = Projections.fields(Projections.include("name", "email"), Projections.excludeId());
     * async.findFirst(projection, Filters.eq("active", true), Sorts.ascending("createdAt"), User.class)
     *      .thenRunAsync(user -> user.ifPresent(u -> processUser(u)));
     * }</pre>
     *
     * @param <T> the target type for the document conversion
     * @param projection BSON projection document for field inclusion/exclusion (null for all fields)
     * @param filter BSON filter criteria to match documents (null for all documents)
     * @param sort BSON sort criteria to determine document order (null for natural order)
     * @param rowType the target type for conversion of the document
     * @return a ContinuableFuture that completes with an Optional containing the converted entity with projected fields, or empty if none found
     * @throws IllegalArgumentException if rowType is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see com.mongodb.client.model.Projections
     * @see com.mongodb.client.model.Sorts
     */
    public <T> ContinuableFuture<Optional<T>> findFirst(final Bson projection, final Bson filter, final Bson sort, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.findFirst(projection, filter, sort, rowType));
    }

    /**
     * Asynchronously retrieves all documents matching the specified filter as a list.
     *
     * <p>This method performs a non-blocking find operation and returns all matching documents as a
     * List of Document objects. The operation is executed on the configured thread pool and the
     * complete result set is delivered through the returned ContinuableFuture. Use this method when
     * you need to retrieve all matching documents at once.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.list(Filters.eq("status", "active"))
     *      .thenRunAsync(activeUsers -> activeUsers.forEach(user -> processUser(user)));
     * }</pre>
     *
     * @param filter the query filter to match documents against
     * @return a ContinuableFuture that completes with a List containing all matching documents (empty list if none found)
     * @throws IllegalArgumentException if filter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see Document
     * @see #stream(Bson)
     * @see com.mongodb.client.model.Filters
     */
    public ContinuableFuture<List<Document>> list(final Bson filter) {
        return asyncExecutor.execute(() -> collExecutor.list(filter));
    }

    /**
     * Asynchronously retrieves all documents matching the filter and converts them to the specified type.
     *
     * <p>This method performs a non-blocking find operation that returns all documents matching the
     * filter criteria, with each document automatically converted to the target type. The operation
     * is executed on the thread pool and results are delivered through the returned ContinuableFuture.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.list(Filters.eq("status", "active"), User.class)
     *      .thenRunAsync(users -> users.forEach(user -> processUser(user)));
     * }</pre>
     *
     * @param <T> the target type for document conversion
     * @param filter BSON filter criteria to match documents (null for all documents)
     * @param rowType the target type for conversion of each document
     * @return a ContinuableFuture that completes with a List of converted entities
     * @throws IllegalArgumentException if rowType is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #list(Bson, int, int, Class)
     * @see #list(Collection, Bson, Class)
     */
    public <T> ContinuableFuture<List<T>> list(final Bson filter, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.list(filter, rowType));
    }

    /**
     * Asynchronously retrieves a paginated list of documents matching the filter with offset and limit.
     *
     * <p>This method performs a non-blocking paginated find operation, retrieving a subset of matching
     * documents starting from the specified offset and limited to the specified count. Each document
     * is converted to the target type. This is useful for implementing pagination in applications.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.list(Filters.eq("active", true), 20, 10, User.class)
     *      .thenRunAsync(users -> displayPage(users));   // Shows users 21-30
     * }</pre>
     *
     * @param <T> the target type for document conversion
     * @param filter BSON filter criteria to match documents (null for all documents)
     * @param offset the number of documents to skip before starting to return results
     * @param count the maximum number of documents to return
     * @param rowType the target type for conversion of each document
     * @return a ContinuableFuture that completes with a List of converted entities within the specified range
     * @throws IllegalArgumentException if rowType is null or offset/count are negative (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #list(Bson, Class)
     */
    public <T> ContinuableFuture<List<T>> list(final Bson filter, final int offset, final int count, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.list(filter, offset, count, rowType));
    }

    /**
     * Asynchronously retrieves documents with field projection matching the filter and converts them to the specified type.
     *
     * <p>This method performs a non-blocking find operation with field projection, retrieving only the
     * specified fields for all matching documents. Each document is converted to the target type.
     * This reduces network traffic and improves performance for documents with many fields.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("name", "email");
     * async.list(fields, Filters.eq("department", "Engineering"), User.class)
     *      .thenRunAsync(engineers -> engineers.forEach(u -> System.out.println(u.getName())));
     * }</pre>
     *
     * @param <T> the target type for document conversion
     * @param selectPropNames collection of field names to include in projection (null for all fields)
     * @param filter BSON filter criteria to match documents (null for all documents)
     * @param rowType the target type for conversion of each document
     * @return a ContinuableFuture that completes with a List of converted entities with only the projected fields
     * @throws IllegalArgumentException if rowType is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #list(Bson, Class)
     */
    public <T> ContinuableFuture<List<T>> list(final Collection<String> selectPropNames, final Bson filter, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.list(selectPropNames, filter, rowType));
    }

    /**
     * Asynchronously retrieves a paginated list with field projection matching the filter.
     *
     * <p>This method combines pagination and field projection in a non-blocking operation.
     * Only the specified fields are retrieved for documents within the offset/count range,
     * and each document is converted to the target type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("name", "email", "score");
     * async.list(fields, Filters.gte("score", 80), 10, 5, User.class)
     *      .thenRunAsync(topScorers -> displayTopScorers(topScorers));   // Shows users 11-15
     * }</pre>
     *
     * @param <T> the target type for document conversion
     * @param selectPropNames collection of field names to include in projection (null for all fields)
     * @param filter BSON filter criteria to match documents (null for all documents)
     * @param offset the number of documents to skip before starting to return results
     * @param count the maximum number of documents to return
     * @param rowType the target type for conversion of each document
     * @return a ContinuableFuture that completes with a List of converted entities with projected fields within the range
     * @throws IllegalArgumentException if rowType is null or offset/count are negative (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #list(Collection, Bson, Class)
     */
    public <T> ContinuableFuture<List<T>> list(final Collection<String> selectPropNames, final Bson filter, final int offset, final int count,
            final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.list(selectPropNames, filter, offset, count, rowType));
    }

    /**
     * Asynchronously retrieves sorted documents with field projection matching the filter.
     *
     * <p>This method performs a non-blocking find operation with field projection and sorting.
     * Documents are sorted according to the specified criteria before being returned, with only
     * the specified fields included in each document.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("name", "salary");
     * async.list(fields, Filters.eq("department", "Sales"), Sorts.descending("salary"), Employee.class)
     *      .thenRunAsync(employees -> employees.forEach(e -> System.out.println(e.getName() + ": " + e.getSalary())));
     * }</pre>
     *
     * @param <T> the target type for document conversion
     * @param selectPropNames collection of field names to include in projection (null for all fields)
     * @param filter BSON filter criteria to match documents (null for all documents)
     * @param sort BSON sort criteria to determine document order (null for natural order)
     * @param rowType the target type for conversion of each document
     * @return a ContinuableFuture that completes with a sorted List of converted entities with projected fields
     * @throws IllegalArgumentException if rowType is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see com.mongodb.client.model.Sorts
     */
    public <T> ContinuableFuture<List<T>> list(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.list(selectPropNames, filter, sort, rowType));
    }

    /**
     * Asynchronously retrieves a sorted, paginated list with field projection matching the filter.
     *
     * <p>This method combines all find operation features: filtering, projection, sorting, and pagination
     * in a single non-blocking operation. This is the most comprehensive list retrieval method.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("name", "score", "rank");
     * async.list(fields, Filters.gte("score", 90), Sorts.descending("score"), 0, 10, Player.class)
     *      .thenRunAsync(topPlayers -> displayLeaderboard(topPlayers));   // Top 10 high scorers
     * }</pre>
     *
     * @param <T> the target type for document conversion
     * @param selectPropNames collection of field names to include in projection (null for all fields)
     * @param filter BSON filter criteria to match documents (null for all documents)
     * @param sort BSON sort criteria to determine document order (null for natural order)
     * @param offset the number of documents to skip before starting to return results
     * @param count the maximum number of documents to return
     * @param rowType the target type for conversion of each document
     * @return a ContinuableFuture that completes with a sorted List of converted entities with projected fields within the range
     * @throws IllegalArgumentException if rowType is null or offset/count are negative (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see com.mongodb.client.model.Sorts
     */
    public <T> ContinuableFuture<List<T>> list(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final int offset, final int count,
            final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.list(selectPropNames, filter, sort, offset, count, rowType));
    }

    /**
     * Asynchronously retrieves sorted documents with BSON projection matching the filter.
     *
     * <p>This method uses a BSON projection document for complex field inclusion/exclusion patterns,
     * combined with filtering and sorting. The projection parameter provides more control than
     * simple field name lists.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson projection = Projections.fields(Projections.include("name", "dept"), Projections.excludeId());
     * async.list(projection, Filters.eq("active", true), Sorts.ascending("name"), Employee.class)
     *      .thenRunAsync(employees -> processEmployees(employees));
     * }</pre>
     *
     * @param <T> the target type for document conversion
     * @param projection BSON projection document for field inclusion/exclusion (null for all fields)
     * @param filter BSON filter criteria to match documents (null for all documents)
     * @param sort BSON sort criteria to determine document order (null for natural order)
     * @param rowType the target type for conversion of each document
     * @return a ContinuableFuture that completes with a sorted List of converted entities with projected fields
     * @throws IllegalArgumentException if rowType is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see com.mongodb.client.model.Projections
     * @see com.mongodb.client.model.Sorts
     */
    public <T> ContinuableFuture<List<T>> list(final Bson projection, final Bson filter, final Bson sort, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.list(projection, filter, sort, rowType));
    }

    /**
     * Asynchronously retrieves a sorted, paginated list with BSON projection matching the filter.
     *
     * <p>This method provides maximum control over the find operation with BSON projection,
     * filtering, sorting, and pagination. This is the most flexible list retrieval method.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson projection = Projections.fields(Projections.include("name", "score"), Projections.excludeId());
     * async.list(projection, Filters.gte("level", 10), Sorts.descending("score"), 0, 20, Player.class)
     *      .thenRunAsync(topPlayers -> updateLeaderboard(topPlayers));
     * }</pre>
     *
     * @param <T> the target type for document conversion
     * @param projection BSON projection document for field inclusion/exclusion (null for all fields)
     * @param filter BSON filter criteria to match documents (null for all documents)
     * @param sort BSON sort criteria to determine document order (null for natural order)
     * @param offset the number of documents to skip before starting to return results
     * @param count the maximum number of documents to return
     * @param rowType the target type for conversion of each document
     * @return a ContinuableFuture that completes with a sorted List of converted entities with projected fields within the range
     * @throws IllegalArgumentException if rowType is null or offset/count are negative (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see com.mongodb.client.model.Projections
     * @see com.mongodb.client.model.Sorts
     */
    public <T> ContinuableFuture<List<T>> list(final Bson projection, final Bson filter, final Bson sort, final int offset, final int count,
            final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.list(projection, filter, sort, offset, count, rowType));
    }

    /**
     * Asynchronously queries for a single boolean value from the first matching document.
     *
     * <p>This method retrieves the value of a specific boolean field from the first document
     * matching the filter. The result is wrapped in OptionalBoolean to handle cases where
     * the field doesn't exist or is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.queryForBoolean("isActive", Filters.eq("userId", "123"))
     *      .thenRunAsync(isActive -> isActive.ifPresent(active -> System.out.println("Active: " + active)));
     * }</pre>
     *
     * @param propName the name of the boolean property to retrieve
     * @param filter the query filter to match documents
     * @return a ContinuableFuture that completes with OptionalBoolean containing the value if present
     * @throws IllegalArgumentException if propName or filter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see OptionalBoolean
     */
    public ContinuableFuture<OptionalBoolean> queryForBoolean(final String propName, final Bson filter) {
        return asyncExecutor.execute(() -> collExecutor.queryForBoolean(propName, filter));
    }

    /**
     * Asynchronously queries for a single character value from the first matching document.
     *
     * <p>This method retrieves the value of a specific character field from the first document
     * matching the filter. The result is wrapped in OptionalChar to handle cases where
     * the field doesn't exist or is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.queryForChar("grade", Filters.eq("studentId", "456"))
     *      .thenRunAsync(grade -> grade.ifPresent(g -> System.out.println("Grade: " + g)));
     * }</pre>
     *
     * @param propName the name of the character property to retrieve
     * @param filter the query filter to match documents
     * @return a ContinuableFuture that completes with OptionalChar containing the value if present
     * @throws IllegalArgumentException if propName or filter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see OptionalChar
     */
    public ContinuableFuture<OptionalChar> queryForChar(final String propName, final Bson filter) {
        return asyncExecutor.execute(() -> collExecutor.queryForChar(propName, filter));
    }

    /**
     * Asynchronously queries for a single byte value from the first matching document.
     *
     * <p>This method retrieves the value of a specific byte field from the first document
     * matching the filter. The result is wrapped in OptionalByte to handle cases where
     * the field doesn't exist or is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.queryForByte("priority", Filters.eq("taskId", "789"))
     *      .thenRunAsync(priority -> priority.ifPresent(p -> System.out.println("Priority: " + p)));
     * }</pre>
     *
     * @param propName the name of the byte property to retrieve
     * @param filter the query filter to match documents
     * @return a ContinuableFuture that completes with OptionalByte containing the value if present
     * @throws IllegalArgumentException if propName or filter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see OptionalByte
     */
    public ContinuableFuture<OptionalByte> queryForByte(final String propName, final Bson filter) {
        return asyncExecutor.execute(() -> collExecutor.queryForByte(propName, filter));
    }

    /**
     * Asynchronously queries for a single short value from the first matching document.
     *
     * <p>This method retrieves the value of a specific short field from the first document
     * matching the filter. The result is wrapped in OptionalShort to handle cases where
     * the field doesn't exist or is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.queryForShort("year", Filters.eq("eventId", "2024"))
     *      .thenRunAsync(year -> year.ifPresent(y -> System.out.println("Year: " + y)));
     * }</pre>
     *
     * @param propName the name of the short property to retrieve
     * @param filter the query filter to match documents
     * @return a ContinuableFuture that completes with OptionalShort containing the value if present
     * @throws IllegalArgumentException if propName or filter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see OptionalShort
     */
    public ContinuableFuture<OptionalShort> queryForShort(final String propName, final Bson filter) {
        return asyncExecutor.execute(() -> collExecutor.queryForShort(propName, filter));
    }

    /**
     * Asynchronously queries for a single integer value from the first matching document.
     *
     * <p>This method retrieves the value of a specific integer field from the first document
     * matching the filter. The result is wrapped in OptionalInt to handle cases where
     * the field doesn't exist or is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.queryForInt("age", Filters.eq("userId", "user123"))
     *      .thenRunAsync(age -> age.ifPresent(a -> System.out.println("Age: " + a)));
     * }</pre>
     *
     * @param propName the name of the integer property to retrieve
     * @param filter the query filter to match documents
     * @return a ContinuableFuture that completes with OptionalInt containing the value if present
     * @throws IllegalArgumentException if propName or filter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see OptionalInt
     */
    public ContinuableFuture<OptionalInt> queryForInt(final String propName, final Bson filter) {
        return asyncExecutor.execute(() -> collExecutor.queryForInt(propName, filter));
    }

    /**
     * Asynchronously queries for a single long value from the first matching document.
     *
     * <p>This method retrieves the value of a specific long field from the first document
     * matching the filter. The result is wrapped in OptionalLong to handle cases where
     * the field doesn't exist or is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.queryForLong("timestamp", Filters.eq("eventId", "evt123"))
     *      .thenRunAsync(ts -> ts.ifPresent(t -> System.out.println("Timestamp: " + t)));
     * }</pre>
     *
     * @param propName the name of the long property to retrieve
     * @param filter the query filter to match documents
     * @return a ContinuableFuture that completes with OptionalLong containing the value if present
     * @throws IllegalArgumentException if propName or filter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see OptionalLong
     */
    public ContinuableFuture<OptionalLong> queryForLong(final String propName, final Bson filter) {
        return asyncExecutor.execute(() -> collExecutor.queryForLong(propName, filter));
    }

    /**
     * Asynchronously queries for a single float value from the first matching document.
     *
     * <p>Retrieves a float field value from the first document matching the provided filter.
     * Returns OptionalFloat to safely handle cases where the field is missing or null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.queryForFloat("price", Filters.eq("productId", "prod456"))
     *      .thenRunAsync(price -> price.ifPresent(p -> System.out.println("Price: $" + p)));
     * }</pre>
     *
     * @param propName the name of the float property to retrieve
     * @param filter the query filter to match documents
     * @return a ContinuableFuture that completes with OptionalFloat containing the value if present
     * @throws IllegalArgumentException if propName or filter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see OptionalFloat
     * @see #queryForDouble(String, Bson)
     */
    public ContinuableFuture<OptionalFloat> queryForFloat(final String propName, final Bson filter) {
        return asyncExecutor.execute(() -> collExecutor.queryForFloat(propName, filter));
    }

    /**
     * Asynchronously queries for a single double value from the first matching document.
     *
     * <p>Retrieves a double field value from the first document matching the filter criteria.
     * Useful for retrieving numeric values like prices, scores, or measurements.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.queryForDouble("averageRating", Filters.eq("movieId", "movie789"))
     *      .thenRunAsync(rating -> rating.ifPresent(r -> System.out.println("Rating: " + r)));
     * }</pre>
     *
     * @param propName the name of the double property to retrieve
     * @param filter the query filter to match documents
     * @return a ContinuableFuture that completes with OptionalDouble containing the value if present
     * @throws IllegalArgumentException if propName or filter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see OptionalDouble
     * @see #queryForFloat(String, Bson)
     */
    public ContinuableFuture<OptionalDouble> queryForDouble(final String propName, final Bson filter) {
        return asyncExecutor.execute(() -> collExecutor.queryForDouble(propName, filter));
    }

    /**
     * Asynchronously queries for a single string value from the first matching document.
     *
     * <p>Retrieves a string field value from the first document matching the filter.
     * Returns Nullable to handle cases where the field is missing, null, or not a string type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.queryForString("username", Filters.eq("userId", "user123"))
     *      .thenRunAsync(name -> System.out.println("Username: " + name.orElse("Unknown")));
     * }</pre>
     *
     * @param propName the name of the string property to retrieve
     * @param filter the query filter to match documents
     * @return a ContinuableFuture that completes with Nullable containing the string value if present
     * @throws IllegalArgumentException if propName or filter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see Nullable
     */
    public ContinuableFuture<Nullable<String>> queryForString(final String propName, final Bson filter) {
        return asyncExecutor.execute(() -> collExecutor.queryForString(propName, filter));
    }

    /**
     * Asynchronously queries for a single Date value from the first matching document.
     *
     * <p>Retrieves a Date field value from the first document matching the filter.
     * Commonly used for timestamp fields like createdAt, updatedAt, or lastLogin.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.queryForDate("lastLogin", Filters.eq("username", "john.doe"))
     *      .thenRunAsync(date -> date.ifPresent(d -> System.out.println("Last login: " + d)));
     * }</pre>
     *
     * @param propName the name of the Date property to retrieve
     * @param filter the query filter to match documents
     * @return a ContinuableFuture that completes with Nullable containing the Date value if present
     * @throws IllegalArgumentException if propName or filter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see Nullable
     * @see Date
     * @see #queryForDate(String, Bson, Class)
     */
    public ContinuableFuture<Nullable<Date>> queryForDate(final String propName, final Bson filter) {
        return asyncExecutor.execute(() -> collExecutor.queryForDate(propName, filter));
    }

    /**
     * Asynchronously queries for a Date value with a specific Date subclass type.
     *
     * <p>Retrieves a Date field and casts it to the specified Date subclass type.
     * Useful when working with custom Date implementations or specific date types.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.queryForDate("timestamp", Filters.eq("eventId", "evt123"), java.sql.Timestamp.class)
     *      .thenRunAsync(ts -> ts.ifPresent(t -> System.out.println("SQL Timestamp: " + t)));
     * }</pre>
     *
     * @param <T> the specific Date subclass type
     * @param propName the name of the Date property to retrieve
     * @param filter the query filter to match documents
     * @param valueType the Class object representing the Date subclass type
     * @return a ContinuableFuture that completes with Nullable containing the typed Date value if present
     * @throws IllegalArgumentException if any parameter is null (propagated through future)
     * @throws ClassCastException if the value cannot be cast to the specified type (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #queryForDate(String, Bson)
     */
    public <T extends Date> ContinuableFuture<Nullable<T>> queryForDate(final String propName, final Bson filter, final Class<T> valueType) {
        return asyncExecutor.execute(() -> collExecutor.queryForDate(propName, filter, valueType));
    }

    /**
     * Asynchronously queries for a single field value of a specified type from the first matching document.
     *
     * <p>Generic method to retrieve any field value with type safety. The value is automatically
     * converted to the specified type using the configured codec registry.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.queryForSingleResult("metadata", Filters.eq("docId", "doc123"), MetadataClass.class)
     *      .thenRunAsync(meta -> meta.ifPresent(m -> processMetadata(m)));
     * }</pre>
     *
     * @param <V> the type of the value to retrieve
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents
     * @param valueType the Class object representing the value type
     * @return a ContinuableFuture that completes with Nullable containing the typed value if present
     * @throws IllegalArgumentException if any parameter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see Nullable
     * @see #queryForSingleNonNull(String, Bson, Class)
     */
    public <V> ContinuableFuture<Nullable<V>> queryForSingleResult(final String propName, final Bson filter, final Class<V> valueType) {
        return asyncExecutor.execute(() -> collExecutor.queryForSingleResult(propName, filter, valueType));
    }

    /**
     * Asynchronously queries for a single non-null field value from the first matching document.
     *
     * <p>Similar to queryForSingleResult but returns Optional instead of Nullable,
     * ensuring the value is never null when present. Useful when null values should be treated as absent.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.queryForSingleNonNull("config", Filters.eq("appId", "app123"), ConfigClass.class)
     *      .thenRunAsync(config -> config.ifPresent(c -> applyConfiguration(c)));
     * }</pre>
     *
     * @param <V> the type of the value to retrieve
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents
     * @param valueType the Class object representing the value type
     * @return a ContinuableFuture that completes with Optional containing the non-null value if present
     * @throws IllegalArgumentException if any parameter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see Optional
     * @see #queryForSingleResult(String, Bson, Class)
     */
    public <V> ContinuableFuture<Optional<V>> queryForSingleNonNull(final String propName, final Bson filter, final Class<V> valueType) {
        return asyncExecutor.execute(() -> collExecutor.queryForSingleNonNull(propName, filter, valueType));
    }

    /**
     * Asynchronously queries for documents and returns results as a Dataset.
     *
     * <p>Retrieves all documents matching the filter and returns them as a Dataset,
     * which provides tabular data manipulation capabilities similar to a database result set.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.query(Filters.eq("status", "active"))
     *      .thenRunAsync(dataset -> dataset.forEach(row -> System.out.println(row)));
     * }</pre>
     *
     * @param filter the query filter to match documents
     * @return a ContinuableFuture that completes with a Dataset containing the query results
     * @throws IllegalArgumentException if filter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see Dataset
     * @see #query(Bson, Class)
     */
    public ContinuableFuture<Dataset> query(final Bson filter) {
        return asyncExecutor.execute(() -> collExecutor.query(filter));
    }

    /**
     * Asynchronously queries for documents with results mapped to a specific row type.
     *
     * <p>Retrieves documents matching the filter and maps each document to the specified
     * row type for type-safe data access within the Dataset.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.query(Filters.eq("category", "electronics"), Product.class)
     *      .thenRunAsync(dataset -> dataset.forEach(product -> processProduct(product)));
     * }</pre>
     *
     * @param <T> the type to map each document row to
     * @param filter the query filter to match documents
     * @param rowType the Class object representing the row type
     * @return a ContinuableFuture that completes with a typed Dataset containing the query results
     * @throws IllegalArgumentException if any parameter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see Dataset
     * @see #query(Bson)
     */
    public <T> ContinuableFuture<Dataset> query(final Bson filter, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.query(filter, rowType));
    }

    /**
     * Asynchronously queries for documents with pagination support.
     *
     * <p>Retrieves a subset of documents matching the filter, starting at the specified offset
     * and limited to the specified count. Useful for implementing pagination in applications.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.query(Filters.eq("type", "article"), 20, 10, Article.class)
     *      .thenRunAsync(dataset -> displayPage(dataset));   // Shows articles 21-30
     * }</pre>
     *
     * @param <T> the type to map each document row to
     * @param filter the query filter to match documents
     * @param offset the number of documents to skip
     * @param count the maximum number of documents to return
     * @param rowType the Class object representing the row type
     * @return a ContinuableFuture that completes with a typed Dataset containing the paginated results
     * @throws IllegalArgumentException if any parameter is null or offset/count is negative (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see Dataset
     * @see #query(Bson, Class)
     */
    public <T> ContinuableFuture<Dataset> query(final Bson filter, final int offset, final int count, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.query(filter, offset, count, rowType));
    }

    /**
     * Asynchronously queries for specific fields from matching documents.
     *
     * <p>Retrieves only the specified fields from documents matching the filter,
     * reducing network overhead and improving query performance.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.query(Arrays.asList("name", "email"), Filters.eq("role", "admin"), User.class)
     *      .thenRunAsync(dataset -> exportToCSV(dataset));
     * }</pre>
     *
     * @param <T> the type to map each document row to
     * @param selectPropNames the collection of property names to include in the projection
     * @param filter the query filter to match documents
     * @param rowType the Class object representing the row type
     * @return a ContinuableFuture that completes with a typed Dataset containing projected results
     * @throws IllegalArgumentException if any parameter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see Dataset
     * @see com.mongodb.client.model.Projections
     */
    public <T> ContinuableFuture<Dataset> query(final Collection<String> selectPropNames, final Bson filter, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.query(selectPropNames, filter, rowType));
    }

    /**
     * Asynchronously queries for specific fields with pagination support.
     *
     * <p>Combines field projection with pagination, retrieving only specified fields
     * from a subset of matching documents.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.query(Arrays.asList("title", "author"), Filters.eq("genre", "fiction"), 0, 20, Book.class)
     *      .thenRunAsync(dataset -> displayBookList(dataset));
     * }</pre>
     *
     * @param <T> the type to map each document row to
     * @param selectPropNames the collection of property names to include in the projection
     * @param filter the query filter to match documents
     * @param offset the number of documents to skip
     * @param count the maximum number of documents to return
     * @param rowType the Class object representing the row type
     * @return a ContinuableFuture that completes with a typed Dataset containing paginated projected results
     * @throws IllegalArgumentException if any parameter is null or offset/count is negative (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see Dataset
     */
    public <T> ContinuableFuture<Dataset> query(final Collection<String> selectPropNames, final Bson filter, final int offset, final int count,
            final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.query(selectPropNames, filter, offset, count, rowType));
    }

    /**
     * Asynchronously queries for specific fields with sorting.
     *
     * <p>Retrieves specified fields from documents matching the filter, sorted according
     * to the provided sort specification.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.query(Arrays.asList("name", "score"), Filters.gte("score", 80), 
     *            Sorts.descending("score"), Student.class)
     *      .thenRunAsync(dataset -> displayTopStudents(dataset));
     * }</pre>
     *
     * @param <T> the type to map each document row to
     * @param selectPropNames the collection of property names to include in the projection
     * @param filter the query filter to match documents
     * @param sort the sort specification
     * @param rowType the Class object representing the row type
     * @return a ContinuableFuture that completes with a typed Dataset containing sorted projected results
     * @throws IllegalArgumentException if any parameter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see Dataset
     * @see com.mongodb.client.model.Sorts
     */
    public <T> ContinuableFuture<Dataset> query(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.query(selectPropNames, filter, sort, rowType));
    }

    /**
     * Asynchronously queries with projection, filtering, sorting, and pagination.
     *
     * <p>Comprehensive query method combining all query features: field projection,
     * filtering, sorting, and pagination for maximum control over query results.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.query(Arrays.asList("productName", "price"), Filters.eq("inStock", true),
     *            Sorts.ascending("price"), 0, 50, Product.class)
     *      .thenRunAsync(dataset -> displayProductCatalog(dataset));
     * }</pre>
     *
     * @param <T> the type to map each document row to
     * @param selectPropNames the collection of property names to include in the projection
     * @param filter the query filter to match documents
     * @param sort the sort specification
     * @param offset the number of documents to skip
     * @param count the maximum number of documents to return
     * @param rowType the Class object representing the row type
     * @return a ContinuableFuture that completes with a typed Dataset containing the complete query results
     * @throws IllegalArgumentException if any parameter is null or offset/count is negative (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see Dataset
     */
    public <T> ContinuableFuture<Dataset> query(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final int offset, final int count,
            final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.query(selectPropNames, filter, sort, offset, count, rowType));
    }

    /**
     * Asynchronously queries with BSON projection, filtering, and sorting.
     *
     * <p>Uses a BSON projection document for complex field projections including
     * computed fields, array slicing, and nested field selection.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson projection = Projections.fields(
     *     Projections.include("name", "tags"),
     *     Projections.slice("comments", 5)
     * );
     * async.query(projection, Filters.eq("published", true), Sorts.descending("date"), Post.class)
     *      .thenRunAsync(dataset -> renderBlogPosts(dataset));
     * }</pre>
     *
     * @param <T> the type to map each document row to
     * @param projection the BSON projection specification
     * @param filter the query filter to match documents
     * @param sort the sort specification
     * @param rowType the Class object representing the row type
     * @return a ContinuableFuture that completes with a typed Dataset containing projected and sorted results
     * @throws IllegalArgumentException if any parameter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see Dataset
     * @see com.mongodb.client.model.Projections
     */
    public <T> ContinuableFuture<Dataset> query(final Bson projection, final Bson filter, final Bson sort, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.query(projection, filter, sort, rowType));
    }

    /**
     * Asynchronously queries with BSON projection, filtering, sorting, and pagination.
     *
     * <p>Complete query method using BSON projection for advanced field selection
     * combined with filtering, sorting, and pagination.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson projection = Projections.exclude("_id", "internalData");
     * async.query(projection, Filters.eq("status", "active"), Sorts.ascending("priority"),
     *            10, 20, Task.class)
     *      .thenRunAsync(dataset -> updateTaskBoard(dataset));
     * }</pre>
     *
     * @param <T> the type to map each document row to
     * @param projection the BSON projection specification
     * @param filter the query filter to match documents
     * @param sort the sort specification
     * @param offset the number of documents to skip
     * @param count the maximum number of documents to return
     * @param rowType the Class object representing the row type
     * @return a ContinuableFuture that completes with a typed Dataset containing the complete query results
     * @throws IllegalArgumentException if any parameter is null or offset/count is negative (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see Dataset
     * @see com.mongodb.client.model.Projections
     */
    public <T> ContinuableFuture<Dataset> query(final Bson projection, final Bson filter, final Bson sort, final int offset, final int count,
            final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.query(projection, filter, sort, offset, count, rowType));
    }

    /**
     * Asynchronously creates a stream of documents matching the filter.
     *
     * <p>Returns a Stream for lazy evaluation and processing of query results.
     * The stream should be properly closed after use to release resources.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.stream(Filters.eq("type", "log"))
     *      .thenRunAsync(stream -> stream.filter(doc -> doc.getDate("timestamp").after(yesterday))
     *                               .forEach(doc -> processLog(doc)));
     * }</pre>
     *
     * @param filter the query filter to match documents
     * @return a ContinuableFuture that completes with a Stream of Document objects
     * @throws IllegalArgumentException if filter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see Stream
     * @see Document
     */
    public ContinuableFuture<Stream<Document>> stream(final Bson filter) {
        return asyncExecutor.execute(() -> collExecutor.stream(filter));
    }

    /**
     * Asynchronously creates a typed stream of documents matching the filter.
     *
     * <p>Returns a Stream of objects mapped to the specified type for type-safe
     * stream processing operations.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.stream(Filters.eq("department", "sales"), Employee.class)
     *      .thenRunAsync(stream -> stream.map(Employee::getSalary)
     *                               .filter(salary -> salary > 50000)
     *                               .forEach(System.out::println));
     * }</pre>
     *
     * @param <T> the type to map each document to
     * @param filter the query filter to match documents
     * @param rowType the Class object representing the row type
     * @return a ContinuableFuture that completes with a typed Stream
     * @throws IllegalArgumentException if any parameter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see Stream
     */
    public <T> ContinuableFuture<Stream<T>> stream(final Bson filter, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.stream(filter, rowType));
    }

    /**
     * Asynchronously creates a typed stream with pagination support.
     *
     * <p>Returns a Stream of objects with offset and limit applied for
     * controlled stream processing of large result sets.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.stream(Filters.exists("email"), 100, 50, Customer.class)
     *      .thenRunAsync(stream -> stream.forEach(customer -> sendNewsletter(customer)));
     * }</pre>
     *
     * @param <T> the type to map each document to
     * @param filter the query filter to match documents
     * @param offset the number of documents to skip
     * @param count the maximum number of documents to stream
     * @param rowType the Class object representing the row type
     * @return a ContinuableFuture that completes with a paginated typed Stream
     * @throws IllegalArgumentException if any parameter is null or offset/count is negative (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see Stream
     */
    public <T> ContinuableFuture<Stream<T>> stream(final Bson filter, final int offset, final int count, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.stream(filter, offset, count, rowType));
    }

    /**
     * Asynchronously creates a stream with field projection.
     *
     * <p>Returns a Stream containing only the specified fields from matching documents,
     * reducing memory usage and network overhead.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.stream(Arrays.asList("orderId", "total"), Filters.eq("status", "completed"), Order.class)
     *      .thenRunAsync(stream -> calculateDailyRevenue(stream));
     * }</pre>
     *
     * @param <T> the type to map each document to
     * @param selectPropNames the collection of property names to include in the projection
     * @param filter the query filter to match documents
     * @param rowType the Class object representing the row type
     * @return a ContinuableFuture that completes with a projected typed Stream
     * @throws IllegalArgumentException if any parameter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see Stream
     */
    public <T> ContinuableFuture<Stream<T>> stream(final Collection<String> selectPropNames, final Bson filter, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.stream(selectPropNames, filter, rowType));
    }

    /**
     * Asynchronously creates a stream with field projection and pagination.
     *
     * <p>Combines field projection with pagination for efficient streaming
     * of large result sets with reduced data transfer.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.stream(Arrays.asList("userId", "action"), Filters.eq("type", "audit"),
     *             0, 1000, AuditLog.class)
     *      .thenRunAsync(stream -> archiveAuditLogs(stream));
     * }</pre>
     *
     * @param <T> the type to map each document to
     * @param selectPropNames the collection of property names to include in the projection
     * @param filter the query filter to match documents
     * @param offset the number of documents to skip
     * @param count the maximum number of documents to stream
     * @param rowType the Class object representing the row type
     * @return a ContinuableFuture that completes with a paginated projected typed Stream
     * @throws IllegalArgumentException if any parameter is null or offset/count is negative (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see Stream
     */
    public <T> ContinuableFuture<Stream<T>> stream(final Collection<String> selectPropNames, final Bson filter, final int offset, final int count,
            final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.stream(selectPropNames, filter, offset, count, rowType));
    }

    /**
     * Asynchronously creates a stream with field projection and sorting.
     *
     * <p>Returns a sorted Stream containing only the specified fields from matching documents,
     * ideal for ordered data processing.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.stream(Arrays.asList("name", "highScore"), Filters.exists("highScore"),
     *             Sorts.descending("highScore"), Player.class)
     *      .thenRunAsync(stream -> displayLeaderboard(stream));
     * }</pre>
     *
     * @param <T> the type to map each document to
     * @param selectPropNames the collection of property names to include in the projection
     * @param filter the query filter to match documents
     * @param sort the sort specification
     * @param rowType the Class object representing the row type
     * @return a ContinuableFuture that completes with a sorted projected typed Stream
     * @throws IllegalArgumentException if any parameter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see Stream
     * @see com.mongodb.client.model.Sorts
     */
    public <T> ContinuableFuture<Stream<T>> stream(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.stream(selectPropNames, filter, sort, rowType));
    }

    /**
     * Asynchronously creates a stream with projection, sorting, and pagination.
     *
     * <p>Complete stream method combining field projection, sorting, and pagination
     * for comprehensive control over streamed data.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.stream(Arrays.asList("title", "views"), Filters.eq("published", true),
     *             Sorts.descending("views"), 0, 10, Article.class)
     *      .thenRunAsync(stream -> displayTopArticles(stream));
     * }</pre>
     *
     * @param <T> the type to map each document to
     * @param selectPropNames the collection of property names to include in the projection
     * @param filter the query filter to match documents
     * @param sort the sort specification
     * @param offset the number of documents to skip
     * @param count the maximum number of documents to stream
     * @param rowType the Class object representing the row type
     * @return a ContinuableFuture that completes with a fully configured typed Stream
     * @throws IllegalArgumentException if any parameter is null or offset/count is negative (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see Stream
     */
    public <T> ContinuableFuture<Stream<T>> stream(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final int offset,
            final int count, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.stream(selectPropNames, filter, sort, offset, count, rowType));
    }

    /**
     * Asynchronously creates a stream with BSON projection and sorting.
     *
     * <p>Uses BSON projection for advanced field selection including computed fields,
     * combined with sorting for ordered stream processing.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson projection = Projections.computed("fullName", 
     *                                      new Document("$concat", Arrays.asList("$firstName", " ", "$lastName")));
     * async.stream(projection, Filters.eq("active", true), Sorts.ascending("lastName"), User.class)
     *      .thenRunAsync(stream -> exportUserList(stream));
     * }</pre>
     *
     * @param <T> the type to map each document to
     * @param projection the BSON projection specification
     * @param filter the query filter to match documents
     * @param sort the sort specification
     * @param rowType the Class object representing the row type
     * @return a ContinuableFuture that completes with a projected and sorted typed Stream
     * @throws IllegalArgumentException if any parameter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see Stream
     * @see com.mongodb.client.model.Projections
     */
    public <T> ContinuableFuture<Stream<T>> stream(final Bson projection, final Bson filter, final Bson sort, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.stream(projection, filter, sort, rowType));
    }

    /**
     * Asynchronously creates a stream with BSON projection, sorting, and pagination.
     *
     * <p>Complete stream method using BSON projection for advanced field selection
     * combined with sorting and pagination for full control.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson projection = Projections.fields(
     *     Projections.include("title", "author"),
     *     Projections.elemMatch("reviews", Filters.gte("rating", 4))
     * );
     * async.stream(projection, Filters.exists("reviews"), Sorts.descending("publicationDate"),
     *             0, 25, Book.class)
     *      .thenRunAsync(stream -> displayBookRecommendations(stream));
     * }</pre>
     *
     * @param <T> the type to map each document to
     * @param projection the BSON projection specification
     * @param filter the query filter to match documents
     * @param sort the sort specification
     * @param offset the number of documents to skip
     * @param count the maximum number of documents to stream
     * @param rowType the Class object representing the row type
     * @return a ContinuableFuture that completes with a fully configured typed Stream
     * @throws IllegalArgumentException if any parameter is null or offset/count is negative (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see Stream
     * @see com.mongodb.client.model.Projections
     */
    public <T> ContinuableFuture<Stream<T>> stream(final Bson projection, final Bson filter, final Bson sort, final int offset, final int count,
            final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.stream(projection, filter, sort, offset, count, rowType));
    }

    /**
     * Asynchronously creates a change stream to watch for changes in the collection.
     *
     * <p>Returns a ChangeStreamIterable that can be used to monitor real-time changes
     * to the collection including inserts, updates, deletes, and replaces.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.watch()
     *      .thenRunAsync(changeStream -> changeStream.forEach(change -> 
     *          System.out.println("Change detected: " + change.getOperationType())));
     * }</pre>
     *
     * @return a ContinuableFuture that completes with a ChangeStreamIterable for Document changes
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see ChangeStreamIterable
     * @see #watch(Class)
     */
    public ContinuableFuture<ChangeStreamIterable<Document>> watch() {
        return asyncExecutor.execute((Callable<ChangeStreamIterable<Document>>) collExecutor::watch);
    }

    /**
     * Asynchronously creates a typed change stream to watch for changes.
     *
     * <p>Returns a ChangeStreamIterable that deserializes change events to the specified type
     * for type-safe change monitoring.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.watch(Product.class)
     *      .thenRunAsync(changeStream -> changeStream.forEach(change -> 
     *          updateInventoryCache(change.getFullDocument())));
     * }</pre>
     *
     * @param <T> the type to deserialize change events to
     * @param rowType the Class object representing the row type
     * @return a ContinuableFuture that completes with a typed ChangeStreamIterable
     * @throws IllegalArgumentException if rowType is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see ChangeStreamIterable
     * @see #watch()
     */
    public <T> ContinuableFuture<ChangeStreamIterable<T>> watch(final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.watch(rowType));
    }

    /**
     * Asynchronously creates a change stream with an aggregation pipeline.
     *
     * <p>Returns a ChangeStreamIterable that applies the specified aggregation pipeline
     * to filter and transform change events before they are returned.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Bson> pipeline = Arrays.asList(
     *     Aggregates.match(Filters.in("operationType", "insert", "update"))
     * );
     * async.watch(pipeline)
     *      .thenRunAsync(changeStream -> processFilteredChanges(changeStream));
     * }</pre>
     *
     * @param pipeline the aggregation pipeline to apply to change events
     * @return a ContinuableFuture that completes with a filtered ChangeStreamIterable for Document changes
     * @throws IllegalArgumentException if pipeline is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see ChangeStreamIterable
     * @see com.mongodb.client.model.Aggregates
     */
    public ContinuableFuture<ChangeStreamIterable<Document>> watch(final List<? extends Bson> pipeline) {
        return asyncExecutor.execute(() -> collExecutor.watch(pipeline));
    }

    /**
     * Asynchronously creates a typed change stream with an aggregation pipeline.
     *
     * <p>Combines pipeline filtering with type-safe deserialization for advanced
     * change stream processing with custom types.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Bson> pipeline = Arrays.asList(
     *     Aggregates.match(Filters.eq("fullDocument.priority", "high"))
     * );
     * async.watch(pipeline, Task.class)
     *      .thenRunAsync(changeStream -> notifyHighPriorityChanges(changeStream));
     * }</pre>
     *
     * @param <T> the type to deserialize change events to
     * @param pipeline the aggregation pipeline to apply to change events
     * @param rowType the Class object representing the row type
     * @return a ContinuableFuture that completes with a filtered typed ChangeStreamIterable
     * @throws IllegalArgumentException if any parameter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see ChangeStreamIterable
     * @see com.mongodb.client.model.Aggregates
     */
    public <T> ContinuableFuture<ChangeStreamIterable<T>> watch(final List<? extends Bson> pipeline, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.watch(pipeline, rowType));
    }

    /**
     * Asynchronously inserts a single document into the collection.
     *
     * <p>This method performs a non-blocking insert operation for a single document. The object can be
     * a Document, Map, or any entity class with getter/setter methods. Entity objects are automatically
     * converted to BSON documents using the configured codec registry. The operation is executed on
     * the configured thread pool.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Insert a Document asynchronously:
     * Document userDoc = new Document("name", "John")
     *                      .append("email", "john@example.com")
     *                      .append("age", 30);
     * 
     * async.insertOne(userDoc)
     *      .thenRunAsync(() -> {
     *          System.out.println("User inserted successfully");
     *          // The document's _id field is now populated
     *          ObjectId userId = userDoc.getObjectId("_id");
     *      })
     *      .exceptionally(throwable -> {
     *          System.err.println("Insert failed: " + throwable.getMessage());
     *          return null;
     *      });
     * 
     * // Insert an entity:
     * User user = new User("Jane", "jane@example.com", 25);
     * async.insertOne(user)
     *      .thenCompose(result -> {
     *          // Chain with another operation
     *          return sendWelcomeEmailAsync(user);
     *      });
     * }</pre>
     *
     * @param obj the object to insert - can be Document, {@code Map<String, Object>}, or entity class with getter/setter methods
     * @return a ContinuableFuture that completes when the insert operation finishes
     * @throws IllegalArgumentException if obj is null (propagated through future)
     * @throws com.mongodb.MongoWriteException if the insert operation fails (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #insertOne(Object, InsertOneOptions)
     * @see #insertMany(Collection)
     */
    public ContinuableFuture<Void> insertOne(final Object obj) {
        return asyncExecutor.execute(() -> {
            collExecutor.insertOne(obj);
            return null;
        });
    }

    /**
     * Asynchronously inserts a single document with custom options.
     *
     * <p>This method performs a non-blocking insertion of a single document into the collection
     * with custom insertion options. The document is converted to BSON format using the configured
     * codec registry, and the insertion is executed on the thread pool.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User newUser = new User("john", "john@example.com");
     * InsertOneOptions options = new InsertOneOptions().bypassDocumentValidation(true);
     * 
     * async.insertOne(newUser, options)
     *      .thenRunAsync(() -> System.out.println("User inserted with validation bypass"))
     *      .exceptionally(throwable -> {
     *          System.err.println("Insert failed: " + throwable.getMessage());
     *          return null;
     *      });
     * }</pre>
     *
     * @param obj the object to insert, which will be converted to a Document
     * @param options the options to apply to the insert operation
     * @return a ContinuableFuture that completes when the insertion finishes
     * @throws IllegalArgumentException if obj is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see InsertOneOptions
     * @see #insertOne(Object)
     */
    public ContinuableFuture<Void> insertOne(final Object obj, final InsertOneOptions options) {
        return asyncExecutor.execute(() -> {
            collExecutor.insertOne(obj, options);
            return null;
        });
    }

    /**
     * Asynchronously inserts multiple documents into the collection.
     *
     * <p>This method performs a non-blocking insertion of multiple documents in a single operation.
     * The documents are converted to BSON format and inserted efficiently using MongoDB's bulk
     * write capabilities. This is more efficient than individual insertOne operations.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<User> users = Arrays.asList(
     *     new User("alice", "alice@example.com"),
     *     new User("bob", "bob@example.com"),
     *     new User("charlie", "charlie@example.com")
     * );
     * 
     * async.insertMany(users)
     *      .thenRunAsync(() -> System.out.println("All users inserted successfully"))
     *      .exceptionally(throwable -> {
     *          System.err.println("Batch insert failed: " + throwable.getMessage());
     *          return null;
     *      });
     * }</pre>
     *
     * @param objList the collection of objects to insert, each will be converted to a Document
     * @return a ContinuableFuture that completes when all insertions finish
     * @throws IllegalArgumentException if objList is null or empty (propagated through future)
     * @throws com.mongodb.MongoException if any database operation fails (propagated through future)
     * @see #insertMany(Collection, InsertManyOptions)
     * @see #insertOne(Object)
     */
    public ContinuableFuture<Void> insertMany(final Collection<?> objList) {
        return asyncExecutor.execute(() -> {
            collExecutor.insertMany(objList);
            return null;
        });
    }

    /**
     * Asynchronously inserts multiple documents with custom options.
     *
     * <p>This method performs a non-blocking insertion of multiple documents using custom
     * insertion options. Options can control ordering behavior, document validation bypass,
     * and error handling strategies for bulk operations.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Product> products = loadProductsFromCsv();
     * InsertManyOptions options = new InsertManyOptions()
     *     .ordered(false)  // Continue on errors
     *     .bypassDocumentValidation(true);
     * 
     * async.insertMany(products, options)
     *      .thenRunAsync(() -> System.out.println("Bulk product import completed"))
     *      .exceptionally(throwable -> {
     *          System.err.println("Some products failed to insert: " + throwable.getMessage());
     *          return null;
     *      });
     * }</pre>
     *
     * @param objList the collection of objects to insert, each will be converted to a Document
     * @param options the options to apply to the insert operation
     * @return a ContinuableFuture that completes when all insertions finish
     * @throws IllegalArgumentException if objList is null or empty (propagated through future)
     * @throws com.mongodb.MongoException if any database operation fails (propagated through future)
     * @see InsertManyOptions
     * @see #insertMany(Collection)
     */
    public ContinuableFuture<Void> insertMany(final Collection<?> objList, final InsertManyOptions options) {
        return asyncExecutor.execute(() -> {
            collExecutor.insertMany(objList, options);
            return null;
        });
    }

    /**
     * Asynchronously updates a single document identified by ObjectId string.
     *
     * <p>This method performs a non-blocking update operation on the document with the specified
     * ObjectId. The update can be a replacement document or contain update operators like $set,
     * $push, $inc, etc. Only the first matching document is updated.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String userId = "507f1f77bcf86cd799439011";
     * Document update = new Document("$set", new Document("lastLogin", new Date()));
     * 
     * async.updateOne(userId, update)
     *      .thenRunAsync(result -> {
     *          System.out.println("Modified count: " + result.getModifiedCount());
     *          if (result.getModifiedCount() == 0) {
     *              System.out.println("User not found");
     *          }
     *      });
     * }</pre>
     *
     * @param objectId the string representation of the ObjectId to identify the document
     * @param update the update specification (can be update operators or replacement document)
     * @return a ContinuableFuture that completes with UpdateResult containing operation details
     * @throws IllegalArgumentException if objectId format is invalid or update is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see UpdateResult
     * @see #updateOne(ObjectId, Object)
     * @see com.mongodb.client.model.Updates
     */
    public ContinuableFuture<UpdateResult> updateOne(final String objectId, final Object update) {
        return asyncExecutor.execute(() -> collExecutor.updateOne(objectId, update));
    }

    /**
     * Asynchronously updates a single document identified by ObjectId.
     *
     * <p>This method performs a non-blocking update operation on the document with the specified
     * ObjectId. The update can be a replacement document or contain update operators. This variant
     * uses the native ObjectId type for better performance compared to string conversion.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId productId = new ObjectId();
     * Bson update = Updates.combine(
     *     Updates.set("lastUpdated", new Date()),
     *     Updates.inc("version", 1)
     * );
     * 
     * async.updateOne(productId, update)
     *      .thenRunAsync(result -> {
     *          if (result.getMatchedCount() > 0) {
     *              System.out.println("Product updated successfully");
     *          }
     *      });
     * }</pre>
     *
     * @param objectId the ObjectId to identify the document to update
     * @param update the update specification (can be update operators or replacement document)
     * @return a ContinuableFuture that completes with UpdateResult containing operation details
     * @throws IllegalArgumentException if objectId or update is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see UpdateResult
     * @see #updateOne(String, Object)
     * @see ObjectId
     */
    public ContinuableFuture<UpdateResult> updateOne(final ObjectId objectId, final Object update) {
        return asyncExecutor.execute(() -> collExecutor.updateOne(objectId, update));
    }

    /**
     * Asynchronously updates a single document matching the specified filter.
     *
     * <p>This method performs a non-blocking update operation on the first document that matches
     * the provided filter criteria. This is the most flexible update method, allowing complex
     * query conditions beyond simple ID-based lookups.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.and(
     *     Filters.eq("status", "pending"),
     *     Filters.lt("createdAt", Date.from(Instant.now().minus(1, ChronoUnit.HOURS)))
     * );
     * Bson update = Updates.set("status", "expired");
     * 
     * async.updateOne(filter, update)
     *      .thenRunAsync(result -> {
     *          System.out.println("Expired " + result.getModifiedCount() + " pending items");
     *      });
     * }</pre>
     *
     * @param filter the query filter to select the document to update
     * @param update the update specification (can be update operators or replacement document)
     * @return a ContinuableFuture that completes with UpdateResult containing operation details
     * @throws IllegalArgumentException if filter or update is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see UpdateResult
     * @see #updateOne(Bson, Object, UpdateOptions)
     * @see com.mongodb.client.model.Filters
     * @see com.mongodb.client.model.Updates
     */
    public ContinuableFuture<UpdateResult> updateOne(final Bson filter, final Object update) {
        return asyncExecutor.execute(() -> collExecutor.updateOne(filter, update));
    }

    /**
     * Asynchronously updates a single document with custom update options.
     *
     * <p>Performs a non-blocking update with additional options such as upsert,
     * bypass validation, or custom collation settings.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * UpdateOptions options = new UpdateOptions().upsert(true);
     * async.updateOne(Filters.eq("userId", "user123"), Updates.inc("loginCount", 1), options)
     *      .thenRunAsync(result -> System.out.println("Upserted: " + result.getUpsertedId()));
     * }</pre>
     *
     * @param filter the query filter to select the document to update
     * @param update the update specification
     * @param options the options to apply to the update operation
     * @return a ContinuableFuture that completes with UpdateResult containing operation details
     * @throws IllegalArgumentException if any parameter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see UpdateOptions
     * @see UpdateResult
     */
    public ContinuableFuture<UpdateResult> updateOne(final Bson filter, final Object update, final UpdateOptions options) {
        return asyncExecutor.execute(() -> collExecutor.updateOne(filter, update, options));
    }

    /**
     * Asynchronously updates a single document using a collection of update operations.
     *
     * <p>Applies multiple update operations from a collection to the first matching document.
     * Useful for building dynamic updates from multiple sources.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Bson> updates = Arrays.asList(
     *     Updates.set("status", "processed"),
     *     Updates.currentDate("processedAt")
     * );
     * async.updateOne(Filters.eq("_id", docId), updates)
     *      .thenRunAsync(result -> System.out.println("Document processed"));
     * }</pre>
     *
     * @param filter the query filter to select the document to update
     * @param objList the collection of update operations to apply
     * @return a ContinuableFuture that completes with UpdateResult containing operation details
     * @throws IllegalArgumentException if filter or objList is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see UpdateResult
     */
    public ContinuableFuture<UpdateResult> updateOne(final Bson filter, final Collection<?> objList) {
        return asyncExecutor.execute(() -> collExecutor.updateOne(filter, objList));
    }

    /**
     * Asynchronously updates a single document using a collection of update operations with options.
     *
     * <p>Applies multiple update operations from a collection to the first matching document,
     * allowing for additional options like upsert or collation.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Bson> updates = Arrays.asList(
     *     Updates.set("status", "processed"),
     *     Updates.currentDate("processedAt")
     * );
     * UpdateOptions options = new UpdateOptions().upsert(true);
     * 
     * async.updateOne(Filters.eq("_id", docId), updates, options)
     *      .thenRunAsync(result -> System.out.println("Document processed with upsert"));
     * }</pre>
     *
     * @param filter the query filter to select the document to update
     * @param objList the collection of update operations to apply
     * @param options the options to apply to the update operation
     * @return a ContinuableFuture that completes with UpdateResult containing operation details
     * @throws IllegalArgumentException if filter or objList is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see UpdateResult
     */
    public ContinuableFuture<UpdateResult> updateOne(final Bson filter, final Collection<?> objList, final UpdateOptions options) {
        return asyncExecutor.execute(() -> collExecutor.updateOne(filter, objList, options));
    }

    /**
     * Asynchronously updates multiple documents matching the specified filter.
     *
     * <p>This method performs a non-blocking update operation on all documents that match the
     * provided filter criteria. Unlike updateOne, this method updates all matching documents
     * in a single operation, making it efficient for bulk updates.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Update all inactive users:
     * Bson filter = Filters.eq("status", "inactive");
     * Bson update = Updates.combine(
     *     Updates.set("status", "archived"),
     *     Updates.set("archivedAt", new Date())
     * );
     * 
     * async.updateMany(filter, update)
     *      .thenRunAsync(result -> {
     *          System.out.println("Archived " + result.getModifiedCount() + " users");
     *      })
     *      .exceptionally(throwable -> {
     *          System.err.println("Archive operation failed: " + throwable.getMessage());
     *          return null;
     *      });
     * }</pre>
     *
     * @param filter the query filter to select documents to update
     * @param update the update specification (can be update operators or replacement document)
     * @return a ContinuableFuture that completes with UpdateResult containing operation details
     * @throws IllegalArgumentException if filter or update is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see UpdateResult
     * @see #updateMany(Bson, Object, UpdateOptions)
     * @see #updateOne(Bson, Object)
     */
    public ContinuableFuture<UpdateResult> updateMany(final Bson filter, final Object update) {
        return asyncExecutor.execute(() -> collExecutor.updateMany(filter, update));
    }

    /**
     * Asynchronously updates multiple documents with custom options.
     *
     * <p>This method performs a non-blocking update operation on all documents that match the
     * provided filter criteria, allowing for additional options such as upsert, collation,
     * and write concern.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * UpdateOptions options = new UpdateOptions().upsert(true);
     * async.updateMany(Filters.eq("status", "pending"), Updates.set("status", "processed"), options)
     *      .thenRunAsync(result -> System.out.println("Processed " + result.getModifiedCount() + " items"));
     * }</pre>
     *
     * @param filter the query filter to select documents to update
     * @param update the update specification (can be update operators or replacement document)
     * @param options the options to apply to the update operation
     * @return a ContinuableFuture that completes with UpdateResult containing operation details
     * @throws IllegalArgumentException if any parameter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     */
    public ContinuableFuture<UpdateResult> updateMany(final Bson filter, final Object update, final UpdateOptions options) {
        return asyncExecutor.execute(() -> collExecutor.updateMany(filter, update, options));
    }

    /**
     * Asynchronously updates multiple documents using a collection of update operations.
     *
     * <p>Applies multiple update operations from a collection to all matching documents.
     * This is useful for bulk updates where the same set of operations needs to be applied
     * to multiple documents.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Bson> updates = Arrays.asList(
     *     Updates.set("status", "processed"),
     *     Updates.currentDate("processedAt")
     * );
     * async.updateMany(Filters.eq("status", "pending"), updates)
     *      .thenRunAsync(result -> System.out.println("Processed " + result.getModifiedCount() + " pending items"));
     * }</pre>
     *
     * @param filter the query filter to select documents to update
     * @param objList the collection of update operations to apply
     * @return a ContinuableFuture that completes with UpdateResult containing operation details
     * @throws IllegalArgumentException if filter or objList is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     */
    public ContinuableFuture<UpdateResult> updateMany(final Bson filter, final Collection<?> objList) {
        return asyncExecutor.execute(() -> collExecutor.updateMany(filter, objList));
    }

    /**
     * Asynchronously updates multiple documents using a collection of update operations with options.
     *
     * <p>Applies multiple update operations from a collection to all matching documents, allowing
     * for additional options like upsert or collation settings.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Bson> updates = Arrays.asList(
     *     Updates.set("status", "processed"),
     *     Updates.currentDate("processedAt")
     * );
     * UpdateOptions options = new UpdateOptions().upsert(true);
     * 
     * async.updateMany(Filters.eq("status", "pending"), updates, options)
     *      .thenRunAsync(result -> System.out.println("Processed " + result.getModifiedCount() + " pending items with upsert"));
     * }</pre>
     *
     * @param filter the query filter to select documents to update
     * @param objList the collection of update operations to apply
     * @param options the options to apply to the update operation
     * @return a ContinuableFuture that completes with UpdateResult containing operation details
     * @throws IllegalArgumentException if filter or objList is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     */
    public ContinuableFuture<UpdateResult> updateMany(final Bson filter, final Collection<?> objList, final UpdateOptions options) {
        return asyncExecutor.execute(() -> collExecutor.updateMany(filter, objList, options));
    }

    /**
     * Asynchronously replaces a single document by its ObjectId string representation.
     *
     * <p>This method performs a non-blocking replace operation on the document with the specified
     * ObjectId. The entire document (except the _id field) is replaced with the new document.
     * If no document exists with the given ObjectId, no operation is performed.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User updatedUser = new User("John Doe", "john@example.com");
     * async.replaceOne("507f1f77bcf86cd799439011", updatedUser)
     *      .thenRunAsync(result -> System.out.println("Replaced: " + result.getModifiedCount()));
     * }</pre>
     *
     * @param objectId the string representation of the ObjectId to identify the document
     * @param replacement the replacement document (will preserve the original _id)
     * @return a ContinuableFuture that completes with UpdateResult containing operation details
     * @throws IllegalArgumentException if objectId is null, empty, or invalid format (propagated through future)
     * @throws IllegalArgumentException if replacement is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see UpdateResult
     * @see #replaceOne(ObjectId, Object)
     * @see #replaceOne(Bson, Object)
     */
    public ContinuableFuture<UpdateResult> replaceOne(final String objectId, final Object replacement) {
        return asyncExecutor.execute(() -> collExecutor.replaceOne(objectId, replacement));
    }

    /**
     * Asynchronously replaces a single document by its ObjectId.
     *
     * <p>This method performs a non-blocking replace operation on the document with the specified
     * ObjectId. The entire document (except the _id field) is replaced with the new document.
     * If no document exists with the given ObjectId, no operation is performed.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
     * Document newDoc = new Document("name", "Jane").append("age", 30);
     * async.replaceOne(id, newDoc).thenRunAsync(result -> System.out.println("Updated: " + result));
     * }</pre>
     *
     * @param objectId the ObjectId to identify the document for replacement
     * @param replacement the replacement document (will preserve the original _id)
     * @return a ContinuableFuture that completes with UpdateResult containing operation details
     * @throws IllegalArgumentException if objectId is null (propagated through future)
     * @throws IllegalArgumentException if replacement is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see UpdateResult
     * @see #replaceOne(String, Object)
     * @see #replaceOne(Bson, Object)
     */
    public ContinuableFuture<UpdateResult> replaceOne(final ObjectId objectId, final Object replacement) {
        return asyncExecutor.execute(() -> collExecutor.replaceOne(objectId, replacement));
    }

    /**
     * Asynchronously replaces a single document matching the specified filter.
     *
     * <p>This method performs a non-blocking replace operation on the first document that matches
     * the provided filter criteria. Unlike update operations that modify specific fields, replace
     * operations substitute the entire document (except for the _id field) with the new document.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Replace user document with updated profile:
     * User updatedProfile = new User();
     * updatedProfile.setName("John Doe Updated");
     * updatedProfile.setEmail("john.updated@example.com");
     * updatedProfile.setRole("senior_developer");
     * 
     * Bson filter = Filters.eq("userId", "user123");
     * 
     * async.replaceOne(filter, updatedProfile)
     *      .thenRunAsync(result -> {
     *          if (result.getMatchedCount() > 0) {
     *              System.out.println("User profile replaced successfully");
     *          } else {
     *              System.out.println("No user found with the specified ID");
     *          }
     *      });
     * }</pre>
     *
     * @param filter the query filter to select the document to replace
     * @param replacement the replacement document (will preserve the original _id)
     * @return a ContinuableFuture that completes with UpdateResult containing operation details
     * @throws IllegalArgumentException if filter or replacement is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see UpdateResult
     * @see #replaceOne(Bson, Object, ReplaceOptions)
     * @see #updateOne(Bson, Object)
     */
    public ContinuableFuture<UpdateResult> replaceOne(final Bson filter, final Object replacement) {
        return asyncExecutor.execute(() -> collExecutor.replaceOne(filter, replacement));
    }

    /**
     * Asynchronously replaces a single document matching the specified filter with custom options.
     *
     * <p>This method performs a non-blocking replace operation with additional control through
     * ReplaceOptions. You can specify whether to upsert (insert if no match found) and set
     * other replacement behaviors. The operation replaces the entire document except the _id field.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ReplaceOptions options = new ReplaceOptions().upsert(true);
     * Document replacement = new Document("status", "active").append("updated", new Date());
     * async.replaceOne(Filters.eq("userId", "123"), replacement, options)
     *      .thenRunAsync(result -> System.out.println("Upserted: " + result.getUpsertedId()));
     * }</pre>
     *
     * @param filter the query filter to select the document to replace
     * @param replacement the replacement document (will preserve the original _id)
     * @param options the options to apply to the replace operation
     * @return a ContinuableFuture that completes with UpdateResult containing operation details
     * @throws IllegalArgumentException if filter, replacement, or options is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see UpdateResult
     * @see ReplaceOptions
     * @see #replaceOne(Bson, Object)
     */
    public ContinuableFuture<UpdateResult> replaceOne(final Bson filter, final Object replacement, final ReplaceOptions options) {
        return asyncExecutor.execute(() -> collExecutor.replaceOne(filter, replacement, options));
    }

    /**
     * Asynchronously deletes a single document by its ObjectId string representation.
     *
     * <p>This method performs a non-blocking delete operation on the document with the specified
     * ObjectId. The ObjectId string must be a valid 24-character hexadecimal representation.
     * Only one document is deleted, even if multiple documents somehow have the same _id.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String userId = "507f1f77bcf86cd799439011";
     * 
     * async.deleteOne(userId)
     *      .thenRunAsync(result -> {
     *          if (result.getDeletedCount() > 0) {
     *              System.out.println("User deleted successfully");
     *          } else {
     *              System.out.println("No user found with ID: " + userId);
     *          }
     *      })
     *      .exceptionally(throwable -> {
     *          System.err.println("Delete operation failed: " + throwable.getMessage());
     *          return null;
     *      });
     * }</pre>
     *
     * @param objectId the string representation of the ObjectId to identify the document for deletion
     * @return a ContinuableFuture that completes with DeleteResult containing operation details
     * @throws IllegalArgumentException if objectId is null, empty, or invalid format (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see DeleteResult
     * @see #deleteOne(ObjectId)
     * @see #deleteOne(Bson)
     */
    public ContinuableFuture<DeleteResult> deleteOne(final String objectId) {
        return asyncExecutor.execute(() -> collExecutor.deleteOne(objectId));
    }

    /**
     * Asynchronously deletes a single document by its ObjectId.
     *
     * <p>This method performs a non-blocking delete operation on the document with the specified
     * ObjectId. Only one document is deleted. If no document exists with the given ObjectId,
     * the operation completes successfully with a deleted count of zero.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
     * async.deleteOne(id)
     *      .thenRunAsync(result -> System.out.println("Deleted: " + result.getDeletedCount()));
     * }</pre>
     *
     * @param objectId the ObjectId to identify the document for deletion
     * @return a ContinuableFuture that completes with DeleteResult containing operation details
     * @throws IllegalArgumentException if objectId is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see DeleteResult
     * @see #deleteOne(String)
     * @see #deleteOne(Bson)
     */
    public ContinuableFuture<DeleteResult> deleteOne(final ObjectId objectId) {
        return asyncExecutor.execute(() -> collExecutor.deleteOne(objectId));
    }

    /**
     * Asynchronously deletes a single document matching the specified filter.
     *
     * <p>This method performs a non-blocking delete operation on the first document that matches
     * the provided filter criteria. Only one document is deleted, even if multiple documents
     * match the filter. If no documents match, the operation completes with a deleted count of zero.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson filter = Filters.and(Filters.eq("status", "inactive"), Filters.lt("lastLogin", oldDate));
     * async.deleteOne(filter)
     *      .thenRunAsync(result -> System.out.println("Deleted inactive user: " + result.wasAcknowledged()));
     * }</pre>
     *
     * @param filter the query filter to select the document for deletion
     * @return a ContinuableFuture that completes with DeleteResult containing operation details
     * @throws IllegalArgumentException if filter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see DeleteResult
     * @see #deleteOne(Bson, DeleteOptions)
     * @see #deleteMany(Bson)
     */
    public ContinuableFuture<DeleteResult> deleteOne(final Bson filter) {
        return asyncExecutor.execute(() -> collExecutor.deleteOne(filter));
    }

    /**
     * Asynchronously deletes a single document matching the specified filter with custom options.
     *
     * <p>This method performs a non-blocking delete operation with additional control through
     * DeleteOptions. You can specify collation rules and other delete behaviors. Only one
     * document is deleted, even if multiple documents match the filter.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DeleteOptions options = new DeleteOptions().collation(Collation.builder().locale("en").build());
     * async.deleteOne(Filters.eq("username", "JohnDoe"), options)
     *      .thenRunAsync(result -> System.out.println("Case-insensitive delete: " + result.getDeletedCount()));
     * }</pre>
     *
     * @param filter the query filter to select the document for deletion
     * @param options the options to apply to the delete operation
     * @return a ContinuableFuture that completes with DeleteResult containing operation details
     * @throws IllegalArgumentException if filter or options is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see DeleteResult
     * @see DeleteOptions
     * @see #deleteOne(Bson)
     */
    public ContinuableFuture<DeleteResult> deleteOne(final Bson filter, final DeleteOptions options) {
        return asyncExecutor.execute(() -> collExecutor.deleteOne(filter, options));
    }

    /**
     * Asynchronously deletes multiple documents matching the specified filter.
     *
     * <p>This method performs a non-blocking delete operation on all documents that match the
     * provided filter criteria. This is efficient for removing multiple documents in a single
     * operation. Be cautious with this operation as it can delete many documents at once.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Delete all expired sessions:
     * Date thirtyDaysAgo = Date.from(Instant.now().minus(30, ChronoUnit.DAYS));
     * Bson filter = Filters.lt("lastAccessed", thirtyDaysAgo);
     * 
     * async.deleteMany(filter)
     *      .thenRunAsync(result -> {
     *          System.out.println("Deleted " + result.getDeletedCount() + " expired sessions");
     *      })
     *      .exceptionally(throwable -> {
     *          System.err.println("Cleanup operation failed: " + throwable.getMessage());
     *          return null;
     *      });
     * 
     * // Delete all documents with specific status:
     * async.deleteMany(Filters.eq("status", "temporary"))
     *      .thenRunAsync(result -> {
     *          if (result.getDeletedCount() == 0) {
     *              System.out.println("No temporary documents found");
     *          }
     *      });
     * }</pre>
     *
     * @param filter the query filter to select documents for deletion
     * @return a ContinuableFuture that completes with DeleteResult containing operation details
     * @throws IllegalArgumentException if filter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see DeleteResult
     * @see #deleteMany(Bson, DeleteOptions)
     * @see #deleteOne(Bson)
     */
    public ContinuableFuture<DeleteResult> deleteMany(final Bson filter) {
        return asyncExecutor.execute(() -> collExecutor.deleteMany(filter));
    }

    /**
     * Asynchronously deletes multiple documents matching the specified filter with custom options.
     *
     * <p>This method performs a non-blocking delete operation on all matching documents with
     * additional control through DeleteOptions. You can specify collation rules and other
     * delete behaviors. All documents matching the filter are deleted in a single operation.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DeleteOptions options = new DeleteOptions().hint(Indexes.ascending("status"));
     * async.deleteMany(Filters.eq("status", "archived"), options)
     *      .thenRunAsync(result -> System.out.println("Archived records deleted: " + result.getDeletedCount()));
     * }</pre>
     *
     * @param filter the query filter to select documents for deletion
     * @param options the options to apply to the delete operation
     * @return a ContinuableFuture that completes with DeleteResult containing operation details
     * @throws IllegalArgumentException if filter or options is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see DeleteResult
     * @see DeleteOptions
     * @see #deleteMany(Bson)
     */
    public ContinuableFuture<DeleteResult> deleteMany(final Bson filter, final DeleteOptions options) {
        return asyncExecutor.execute(() -> collExecutor.deleteMany(filter, options));
    }

    /**
     * Asynchronously performs a bulk insert of multiple documents.
     *
     * <p>This method performs a non-blocking bulk insert operation, efficiently inserting
     * multiple documents in a single database round trip. The operation is ordered by default,
     * meaning it stops on the first error encountered.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<User> users = Arrays.asList(new User("Alice"), new User("Bob"), new User("Charlie"));
     * async.bulkInsert(users)
     *      .thenRunAsync(count -> System.out.println("Inserted " + count + " users"));
     * }</pre>
     *
     * @param entities the collection of documents to insert
     * @return a ContinuableFuture that completes with the number of documents inserted
     * @throws IllegalArgumentException if entities is null or empty (propagated through future)
     * @throws com.mongodb.MongoBulkWriteException if the bulk operation fails (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #bulkInsert(Collection, BulkWriteOptions)
     * @see #bulkWrite(List)
     */
    public ContinuableFuture<Integer> bulkInsert(final Collection<?> entities) {
        return asyncExecutor.execute(() -> collExecutor.bulkInsert(entities));
    }

    /**
     * Asynchronously performs a bulk insert of multiple documents with custom options.
     *
     * <p>This method performs a non-blocking bulk insert operation with additional control
     * through BulkWriteOptions. You can specify whether the operations should be ordered
     * (stop on first error) or unordered (continue despite errors), and control validation.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * BulkWriteOptions options = new BulkWriteOptions().ordered(false).bypassDocumentValidation(true);
     * async.bulkInsert(documents, options)
     *      .thenRunAsync(count -> System.out.println("Inserted " + count + " documents (unordered)"));
     * }</pre>
     *
     * @param entities the collection of documents to insert
     * @param options the options to apply to the bulk insert operation
     * @return a ContinuableFuture that completes with the number of documents inserted
     * @throws IllegalArgumentException if entities is null or empty, or options is null (propagated through future)
     * @throws com.mongodb.MongoBulkWriteException if the bulk operation fails (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see BulkWriteOptions
     * @see #bulkInsert(Collection)
     */
    public ContinuableFuture<Integer> bulkInsert(final Collection<?> entities, final BulkWriteOptions options) {
        return asyncExecutor.execute(() -> collExecutor.bulkInsert(entities, options));
    }

    /**
     * Asynchronously performs a bulk write operation with multiple write models.
     *
     * <p>This method performs a non-blocking bulk write operation that can combine different
     * types of operations (insert, update, replace, delete) in a single database round trip.
     * Each WriteModel represents a single write operation to be executed.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<WriteModel<Document>> operations = Arrays.asList(
     *     new InsertOneModel<>(new Document("type", "A")),
     *     new UpdateOneModel<>(Filters.eq("type", "B"), Updates.set("status", "updated")),
     *     new DeleteOneModel<>(Filters.eq("type", "C"))
     * );
     * async.bulkWrite(operations).thenRunAsync(result -> 
     *     System.out.println("Modified: " + result.getModifiedCount()));
     * }</pre>
     *
     * @param requests the list of write operations to perform
     * @return a ContinuableFuture that completes with BulkWriteResult containing operation details
     * @throws IllegalArgumentException if requests is null or empty (propagated through future)
     * @throws com.mongodb.MongoBulkWriteException if the bulk operation fails (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see BulkWriteResult
     * @see WriteModel
     * @see #bulkWrite(List, BulkWriteOptions)
     */
    public ContinuableFuture<BulkWriteResult> bulkWrite(final List<? extends WriteModel<? extends Document>> requests) {
        return asyncExecutor.execute(() -> collExecutor.bulkWrite(requests));
    }

    /**
     * Asynchronously performs a bulk write operation with multiple write models and custom options.
     *
     * <p>This method performs a non-blocking bulk write operation with additional control
     * through BulkWriteOptions. You can specify whether operations should be ordered,
     * bypass document validation, and control other bulk write behaviors.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * BulkWriteOptions options = new BulkWriteOptions().ordered(false);
     * async.bulkWrite(operations, options).thenRunAsync(result -> {
     *     System.out.println("Inserted: " + result.getInsertedCount());
     *     System.out.println("Deleted: " + result.getDeletedCount());
     * });
     * }</pre>
     *
     * @param requests the list of write operations to perform
     * @param options the options to apply to the bulk write operation
     * @return a ContinuableFuture that completes with BulkWriteResult containing operation details
     * @throws IllegalArgumentException if requests is null or empty, or options is null (propagated through future)
     * @throws com.mongodb.MongoBulkWriteException if the bulk operation fails (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see BulkWriteResult
     * @see BulkWriteOptions
     * @see #bulkWrite(List)
     */
    public ContinuableFuture<BulkWriteResult> bulkWrite(final List<? extends WriteModel<? extends Document>> requests, final BulkWriteOptions options) {
        return asyncExecutor.execute(() -> collExecutor.bulkWrite(requests, options));
    }

    /**
     * Asynchronously finds and updates a single document atomically.
     *
     * <p>This method performs a non-blocking atomic find-and-update operation. It finds the
     * first document matching the filter, applies the update, and returns the document.
     * By default, returns the document before the update was applied.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson update = Updates.combine(Updates.set("status", "processing"), Updates.inc("attempts", 1));
     * async.findOneAndUpdate(Filters.eq("status", "pending"), update)
     *      .thenRunAsync(doc -> System.out.println("Processing: " + doc));
     * }</pre>
     *
     * @param filter the query filter to find the document
     * @param update the update operations to apply
     * @return a ContinuableFuture that completes with the found document (before update by default)
     * @throws IllegalArgumentException if filter or update is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #findOneAndUpdate(Bson, Object, FindOneAndUpdateOptions)
     * @see #findOneAndUpdate(Bson, Object, Class)
     */
    public ContinuableFuture<Document> findOneAndUpdate(final Bson filter, final Object update) {
        return asyncExecutor.execute(() -> collExecutor.findOneAndUpdate(filter, update));
    }

    /**
     * Asynchronously finds and updates a single document atomically with type conversion.
     *
     * <p>This method performs a non-blocking atomic find-and-update operation with automatic
     * deserialization to the specified type. The document is converted to the target class
     * after retrieval.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.findOneAndUpdate(Filters.eq("userId", "123"), Updates.set("lastSeen", new Date()), User.class)
     *      .thenRunAsync(user -> System.out.println("Updated user: " + user.getName()));
     * }</pre>
     *
     * @param <T> the type of the result document
     * @param filter the query filter to find the document
     * @param update the update operations to apply
     * @param rowType the class to deserialize the result document into
     * @return a ContinuableFuture that completes with the found document as the specified type
     * @throws IllegalArgumentException if filter, update, or rowType is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #findOneAndUpdate(Bson, Object)
     * @see #findOneAndUpdate(Bson, Object, FindOneAndUpdateOptions, Class)
     */
    public <T> ContinuableFuture<T> findOneAndUpdate(final Bson filter, final Object update, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.findOneAndUpdate(filter, update, rowType));
    }

    /**
     * Asynchronously finds and updates a single document atomically with custom options.
     *
     * <p>This method performs a non-blocking atomic find-and-update operation with additional
     * control through FindOneAndUpdateOptions. You can specify whether to return the document
     * before or after the update, set projections, sorts, and enable upsert behavior.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndUpdateOptions options = new FindOneAndUpdateOptions()
     *     .returnDocument(ReturnDocument.AFTER)
     *     .upsert(true);
     * async.findOneAndUpdate(Filters.eq("_id", id), Updates.inc("counter", 1), options)
     *      .thenRunAsync(doc -> System.out.println("Counter value: " + doc.getInteger("counter")));
     * }</pre>
     *
     * @param filter the query filter to find the document
     * @param update the update operations to apply
     * @param options the options to apply to the operation
     * @return a ContinuableFuture that completes with the found document
     * @throws IllegalArgumentException if filter, update, or options is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see FindOneAndUpdateOptions
     * @see #findOneAndUpdate(Bson, Object)
     */
    public ContinuableFuture<Document> findOneAndUpdate(final Bson filter, final Object update, final FindOneAndUpdateOptions options) {
        return asyncExecutor.execute(() -> collExecutor.findOneAndUpdate(filter, update, options));
    }

    /**
     * Asynchronously finds and updates a single document atomically with custom options and type conversion.
     *
     * <p>This method combines atomic find-and-update with custom options and automatic
     * deserialization to the specified type. Provides full control over the operation
     * behavior and result type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndUpdateOptions options = new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER);
     * async.findOneAndUpdate(filter, update, options, User.class)
     *      .thenRunAsync(user -> System.out.println("Updated: " + user));
     * }</pre>
     *
     * @param <T> the type of the result document
     * @param filter the query filter to find the document
     * @param update the update operations to apply
     * @param options the options to apply to the operation
     * @param rowType the class to deserialize the result document into
     * @return a ContinuableFuture that completes with the found document as the specified type
     * @throws IllegalArgumentException if any parameter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see FindOneAndUpdateOptions
     * @see #findOneAndUpdate(Bson, Object, Class)
     */
    public <T> ContinuableFuture<T> findOneAndUpdate(final Bson filter, final Object update, final FindOneAndUpdateOptions options, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.findOneAndUpdate(filter, update, options, rowType));
    }

    /**
     * Asynchronously finds and updates a single document using a collection of update operations.
     *
     * <p>This method performs a non-blocking atomic find-and-update operation where multiple
     * update operations are combined from a collection. Useful when building dynamic updates
     * from multiple sources or conditions.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Bson> updates = Arrays.asList(Updates.set("status", "active"), Updates.currentDate("modified"));
     * async.findOneAndUpdate(Filters.eq("_id", id), updates)
     *      .thenRunAsync(doc -> System.out.println("Updated document: " + doc));
     * }</pre>
     *
     * @param filter the query filter to find the document
     * @param objList the collection of update operations to apply
     * @return a ContinuableFuture that completes with the found document (before update by default)
     * @throws IllegalArgumentException if filter or objList is null or empty (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #findOneAndUpdate(Bson, Object)
     * @see #findOneAndUpdate(Bson, Collection, FindOneAndUpdateOptions)
     */
    public ContinuableFuture<Document> findOneAndUpdate(final Bson filter, final Collection<?> objList) {
        return asyncExecutor.execute(() -> collExecutor.findOneAndUpdate(filter, objList));
    }

    /**
     * Asynchronously finds and updates a single document using a collection of update operations with type conversion.
     *
     * <p>This method performs a non-blocking atomic find-and-update operation with multiple
     * update operations and automatic deserialization to the specified type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Bson> updates = Arrays.asList(Updates.set("verified", true), Updates.inc("version", 1));
     * async.findOneAndUpdate(filter, updates, User.class)
     *      .thenRunAsync(user -> System.out.println("Verified user: " + user.getName()));
     * }</pre>
     *
     * @param <T> the type of the result document
     * @param filter the query filter to find the document
     * @param objList the collection of update operations to apply
     * @param rowType the class to deserialize the result document into
     * @return a ContinuableFuture that completes with the found document as the specified type
     * @throws IllegalArgumentException if any parameter is null or objList is empty (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #findOneAndUpdate(Bson, Collection)
     */
    public <T> ContinuableFuture<T> findOneAndUpdate(final Bson filter, final Collection<?> objList, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.findOneAndUpdate(filter, objList, rowType));
    }

    /**
     * Asynchronously finds and updates a single document using a collection of update operations with custom options.
     *
     * <p>This method provides full control over an atomic find-and-update operation with
     * multiple update operations combined from a collection and custom options.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndUpdateOptions options = new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER);
     * List<Bson> updates = buildDynamicUpdates();
     * async.findOneAndUpdate(filter, updates, options)
     *      .thenRunAsync(doc -> System.out.println("Result: " + doc));
     * }</pre>
     *
     * @param filter the query filter to find the document
     * @param objList the collection of update operations to apply
     * @param options the options to apply to the operation
     * @return a ContinuableFuture that completes with the found document
     * @throws IllegalArgumentException if any parameter is null or objList is empty (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see FindOneAndUpdateOptions
     * @see #findOneAndUpdate(Bson, Collection)
     */
    public ContinuableFuture<Document> findOneAndUpdate(final Bson filter, final Collection<?> objList, final FindOneAndUpdateOptions options) {
        return asyncExecutor.execute(() -> collExecutor.findOneAndUpdate(filter, objList, options));
    }

    /**
     * Asynchronously finds and updates a single document with collection of operations, options, and type conversion.
     *
     * <p>This method provides the most comprehensive control over an atomic find-and-update
     * operation, combining multiple update operations, custom options, and automatic
     * deserialization to the specified type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndUpdateOptions options = new FindOneAndUpdateOptions().upsert(true);
     * async.findOneAndUpdate(filter, updatesList, options, User.class)
     *      .thenRunAsync(user -> processUpdatedUser(user));
     * }</pre>
     *
     * @param <T> the type of the result document
     * @param filter the query filter to find the document
     * @param objList the collection of update operations to apply
     * @param options the options to apply to the operation
     * @param rowType the class to deserialize the result document into
     * @return a ContinuableFuture that completes with the found document as the specified type
     * @throws IllegalArgumentException if any parameter is null or objList is empty (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see FindOneAndUpdateOptions
     * @see #findOneAndUpdate(Bson, Collection, Class)
     */
    public <T> ContinuableFuture<T> findOneAndUpdate(final Bson filter, final Collection<?> objList, final FindOneAndUpdateOptions options,
            final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.findOneAndUpdate(filter, objList, options, rowType));
    }

    /**
     * Asynchronously finds and replaces a single document atomically.
     *
     * <p>This method performs a non-blocking atomic find-and-replace operation. It finds the
     * first document matching the filter and replaces it entirely (except the _id) with the
     * replacement document. Returns the document before replacement by default.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User newUser = new User("Jane Doe", "jane@example.com");
     * async.findOneAndReplace(Filters.eq("userId", "123"), newUser)
     *      .thenRunAsync(oldDoc -> System.out.println("Replaced: " + oldDoc));
     * }</pre>
     *
     * @param filter the query filter to find the document
     * @param replacement the replacement document
     * @return a ContinuableFuture that completes with the found document (before replacement by default)
     * @throws IllegalArgumentException if filter or replacement is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #findOneAndReplace(Bson, Object, FindOneAndReplaceOptions)
     */
    public ContinuableFuture<Document> findOneAndReplace(final Bson filter, final Object replacement) {
        return asyncExecutor.execute(() -> collExecutor.findOneAndReplace(filter, replacement));
    }

    /**
     * Asynchronously finds and replaces a single document atomically with type conversion.
     *
     * <p>This method performs a non-blocking atomic find-and-replace operation with automatic
     * deserialization of the returned document to the specified type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User replacement = new User("Updated Name", "new@email.com");
     * async.findOneAndReplace(Filters.eq("_id", id), replacement, User.class)
     *      .thenRunAsync(oldUser -> System.out.println("Previous: " + oldUser.getName()));
     * }</pre>
     *
     * @param <T> the type of the result document
     * @param filter the query filter to find the document
     * @param replacement the replacement document
     * @param rowType the class to deserialize the result document into
     * @return a ContinuableFuture that completes with the found document as the specified type
     * @throws IllegalArgumentException if any parameter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #findOneAndReplace(Bson, Object)
     */
    public <T> ContinuableFuture<T> findOneAndReplace(final Bson filter, final Object replacement, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.findOneAndReplace(filter, replacement, rowType));
    }

    /**
     * Asynchronously finds and replaces a single document atomically with custom options.
     *
     * <p>This method performs a non-blocking atomic find-and-replace operation with additional
     * control through FindOneAndReplaceOptions. You can specify whether to return the document
     * before or after replacement, enable upsert, and set other behaviors.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndReplaceOptions options = new FindOneAndReplaceOptions()
     *     .returnDocument(ReturnDocument.AFTER)
     *     .upsert(true);
     * async.findOneAndReplace(filter, replacement, options)
     *      .thenRunAsync(doc -> System.out.println("New document: " + doc));
     * }</pre>
     *
     * @param filter the query filter to find the document
     * @param replacement the replacement document
     * @param options the options to apply to the operation
     * @return a ContinuableFuture that completes with the found document
     * @throws IllegalArgumentException if any parameter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see FindOneAndReplaceOptions
     * @see #findOneAndReplace(Bson, Object)
     */
    public ContinuableFuture<Document> findOneAndReplace(final Bson filter, final Object replacement, final FindOneAndReplaceOptions options) {
        return asyncExecutor.execute(() -> collExecutor.findOneAndReplace(filter, replacement, options));
    }

    /**
     * Asynchronously finds and replaces a single document with custom options and type conversion.
     *
     * <p>This method provides full control over an atomic find-and-replace operation with
     * custom options and automatic deserialization to the specified type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndReplaceOptions options = new FindOneAndReplaceOptions().projection(Projections.include("name", "email"));
     * async.findOneAndReplace(filter, newUser, options, User.class)
     *      .thenRunAsync(user -> logUserChange(user));
     * }</pre>
     *
     * @param <T> the type of the result document
     * @param filter the query filter to find the document
     * @param replacement the replacement document
     * @param options the options to apply to the operation
     * @param rowType the class to deserialize the result document into
     * @return a ContinuableFuture that completes with the found document as the specified type
     * @throws IllegalArgumentException if any parameter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see FindOneAndReplaceOptions
     * @see #findOneAndReplace(Bson, Object, Class)
     */
    public <T> ContinuableFuture<T> findOneAndReplace(final Bson filter, final Object replacement, final FindOneAndReplaceOptions options,
            final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.findOneAndReplace(filter, replacement, options, rowType));
    }

    /**
     * Asynchronously finds and deletes a single document atomically.
     *
     * <p>This method performs a non-blocking atomic find-and-delete operation. It finds the
     * first document matching the filter, deletes it, and returns the deleted document.
     * This is useful when you need to retrieve a document while ensuring it's removed.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.findOneAndDelete(Filters.eq("status", "processed"))
     *      .thenRunAsync(doc -> System.out.println("Deleted document: " + doc));
     * }</pre>
     *
     * @param filter the query filter to find the document
     * @return a ContinuableFuture that completes with the deleted document
     * @throws IllegalArgumentException if filter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #findOneAndDelete(Bson, FindOneAndDeleteOptions)
     * @see #deleteOne(Bson)
     */
    public ContinuableFuture<Document> findOneAndDelete(final Bson filter) {
        return asyncExecutor.execute(() -> collExecutor.findOneAndDelete(filter));
    }

    /**
     * Asynchronously finds and deletes a single document atomically with type conversion.
     *
     * <p>This method performs a non-blocking atomic find-and-delete operation with automatic
     * deserialization of the deleted document to the specified type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.findOneAndDelete(Filters.eq("expired", true), Session.class)
     *      .thenRunAsync(session -> logExpiredSession(session));
     * }</pre>
     *
     * @param <T> the type of the result document
     * @param filter the query filter to find the document
     * @param rowType the class to deserialize the result document into
     * @return a ContinuableFuture that completes with the deleted document as the specified type
     * @throws IllegalArgumentException if filter or rowType is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #findOneAndDelete(Bson)
     */
    public <T> ContinuableFuture<T> findOneAndDelete(final Bson filter, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.findOneAndDelete(filter, rowType));
    }

    /**
     * Asynchronously finds and deletes a single document atomically with custom options.
     *
     * <p>This method performs a non-blocking atomic find-and-delete operation with additional
     * control through FindOneAndDeleteOptions. You can specify sort order, projections,
     * and other behaviors for the operation.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndDeleteOptions options = new FindOneAndDeleteOptions()
     *     .sort(Sorts.ascending("priority"))
     *     .projection(Projections.include("taskId", "status"));
     * async.findOneAndDelete(Filters.eq("status", "pending"), options)
     *      .thenRunAsync(doc -> processNextTask(doc));
     * }</pre>
     *
     * @param filter the query filter to find the document
     * @param options the options to apply to the operation
     * @return a ContinuableFuture that completes with the deleted document
     * @throws IllegalArgumentException if filter or options is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see FindOneAndDeleteOptions
     * @see #findOneAndDelete(Bson)
     */
    public ContinuableFuture<Document> findOneAndDelete(final Bson filter, final FindOneAndDeleteOptions options) {
        return asyncExecutor.execute(() -> collExecutor.findOneAndDelete(filter, options));
    }

    /**
     * Asynchronously finds and deletes a single document with custom options and type conversion.
     *
     * <p>This method provides full control over an atomic find-and-delete operation with
     * custom options and automatic deserialization to the specified type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndDeleteOptions options = new FindOneAndDeleteOptions().maxTime(5, TimeUnit.SECONDS);
     * async.findOneAndDelete(filter, options, Task.class)
     *      .thenRunAsync(task -> completeTask(task));
     * }</pre>
     *
     * @param <T> the type of the result document
     * @param filter the query filter to find the document
     * @param options the options to apply to the operation
     * @param rowType the class to deserialize the result document into
     * @return a ContinuableFuture that completes with the deleted document as the specified type
     * @throws IllegalArgumentException if any parameter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see FindOneAndDeleteOptions
     * @see #findOneAndDelete(Bson, Class)
     */
    public <T> ContinuableFuture<T> findOneAndDelete(final Bson filter, final FindOneAndDeleteOptions options, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.findOneAndDelete(filter, options, rowType));
    }

    /**
     * Asynchronously retrieves distinct values for a specified field.
     *
     * <p>This method performs a non-blocking operation to get all distinct values of a
     * specific field across all documents in the collection. Returns the values as a
     * stream of the specified type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.distinct("category", String.class)
     *      .thenRunAsync(stream -> stream.forEach(System.out::println));
     * }</pre>
     *
     * @param <T> the type of the distinct values
     * @param fieldName the field name to get distinct values for
     * @param rowType the class to deserialize the distinct values into
     * @return a ContinuableFuture that completes with a Stream of distinct values
     * @throws IllegalArgumentException if fieldName or rowType is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #distinct(String, Bson, Class)
     */
    public <T> ContinuableFuture<Stream<T>> distinct(final String fieldName, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.distinct(fieldName, rowType));
    }

    /**
     * Asynchronously retrieves distinct values for a specified field with a filter.
     *
     * <p>This method performs a non-blocking operation to get distinct values of a specific
     * field, but only from documents matching the provided filter. Returns the values as
     * a stream of the specified type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.distinct("department", Filters.eq("active", true), String.class)
     *      .thenRunAsync(stream -> stream.sorted().forEach(System.out::println));
     * }</pre>
     *
     * @param <T> the type of the distinct values
     * @param fieldName the field name to get distinct values for
     * @param filter the query filter to apply before getting distinct values
     * @param rowType the class to deserialize the distinct values into
     * @return a ContinuableFuture that completes with a Stream of distinct values
     * @throws IllegalArgumentException if any parameter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #distinct(String, Class)
     */
    public <T> ContinuableFuture<Stream<T>> distinct(final String fieldName, final Bson filter, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.distinct(fieldName, filter, rowType));
    }

    /**
     * Asynchronously executes an aggregation pipeline and returns results as Documents.
     *
     * <p>This method performs a non-blocking aggregation operation using the provided pipeline.
     * Aggregation pipelines allow for complex data transformations and analysis operations
     * including filtering, grouping, sorting, and reshaping documents.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Bson> pipeline = Arrays.asList(
     *     Aggregates.match(Filters.eq("status", "active")),
     *     Aggregates.group("$category", Accumulators.sum("total", 1))
     * );
     * async.aggregate(pipeline).thenRunAsync(stream -> 
     *     stream.forEach(doc -> System.out.println(doc)));
     * }</pre>
     *
     * @param pipeline the aggregation pipeline to execute
     * @return a ContinuableFuture that completes with a Stream of result Documents
     * @throws IllegalArgumentException if pipeline is null or empty (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #aggregate(List, Class)
     * @see com.mongodb.client.model.Aggregates
     */
    public ContinuableFuture<Stream<Document>> aggregate(final List<? extends Bson> pipeline) {
        return asyncExecutor.execute(() -> collExecutor.aggregate(pipeline));
    }

    /**
     * Asynchronously executes an aggregation pipeline with type conversion.
     *
     * <p>This method performs a non-blocking aggregation operation with automatic
     * deserialization of results to the specified type. Useful when the aggregation
     * output matches a known document structure.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Bson> pipeline = Arrays.asList(
     *     Aggregates.match(Filters.gte("score", 80)),
     *     Aggregates.sort(Sorts.descending("score"))
     * );
     * async.aggregate(pipeline, StudentResult.class).thenRunAsync(stream ->
     *     stream.limit(10).forEach(result -> System.out.println(result)));
     * }</pre>
     *
     * @param <T> the type of the result documents
     * @param pipeline the aggregation pipeline to execute
     * @param rowType the class to deserialize the result documents into
     * @return a ContinuableFuture that completes with a Stream of result objects
     * @throws IllegalArgumentException if pipeline is null or empty, or rowType is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #aggregate(List)
     * @see com.mongodb.client.model.Aggregates
     */
    public <T> ContinuableFuture<Stream<T>> aggregate(final List<? extends Bson> pipeline, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.aggregate(pipeline, rowType));
    }

    /**
     * Asynchronously groups documents by a single field.
     *
     * <p>This method performs a non-blocking group operation on documents by the specified field.
     * It's a convenience method that creates an aggregation pipeline with a $group stage.
     * This method is marked as @Beta and may change in future versions.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.groupBy("department")
     *      .thenRunAsync(stream -> stream.forEach(group -> System.out.println(group)));
     * }</pre>
     *
     * @param fieldName the field name to group by
     * @return a ContinuableFuture that completes with a Stream of grouped Documents
     * @throws IllegalArgumentException if fieldName is null or empty (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #groupBy(Collection)
     * @see #aggregate(List)
     */
    @Beta
    public ContinuableFuture<Stream<Document>> groupBy(final String fieldName) {
        return asyncExecutor.execute(() -> collExecutor.groupBy(fieldName));
    }

    /**
     * Asynchronously groups documents by multiple fields.
     *
     * <p>This method performs a non-blocking group operation on documents by multiple fields.
     * It's a convenience method that creates an aggregation pipeline with a $group stage
     * for composite grouping. This method is marked as @Beta and may change in future versions.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.groupBy(Arrays.asList("department", "location"))
     *      .thenRunAsync(stream -> stream.forEach(group -> System.out.println(group)));
     * }</pre>
     *
     * @param fieldNames the collection of field names to group by
     * @return a ContinuableFuture that completes with a Stream of grouped Documents
     * @throws IllegalArgumentException if fieldNames is null or empty (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #groupBy(String)
     * @see #aggregate(List)
     */
    @Beta
    public ContinuableFuture<Stream<Document>> groupBy(final Collection<String> fieldNames) {
        return asyncExecutor.execute(() -> collExecutor.groupBy(fieldNames));
    }

    /**
     * Asynchronously groups documents by a single field and counts frequency in each group.
     *
     * <p>This method performs a non-blocking group operation that both groups documents by the
     * specified field and counts the number of documents in each group. It's a convenience method
     * that creates an aggregation pipeline with $group and $sum stages. The result includes the
     * grouping field value and a count of documents for each distinct value.</p>
     *
     * <p>This method is marked as @Beta and may change in future versions.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Count users by department:
     * async.groupByAndCount("department")
     *      .thenRunAsync(stream -> {
     *          stream.forEach(group -> {
     *              System.out.println("Department: " + group.get("_id") + 
     *                               ", Count: " + group.get("count"));
     *          });
     *      });
     *
     * // Count products by category:
     * async.groupByAndCount("category")
     *      .thenRunAsync(stream -> stream.forEach(System.out::println));
     * }</pre>
     *
     * @param fieldName the field name to group by and count
     * @return a ContinuableFuture that completes with a Stream of Documents containing group keys and counts
     * @throws IllegalArgumentException if fieldName is null or empty (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #groupByAndCount(Collection)
     * @see #groupBy(String)
     * @see #aggregate(List)
     */
    @Beta
    public ContinuableFuture<Stream<Document>> groupByAndCount(final String fieldName) {
        return asyncExecutor.execute(() -> collExecutor.groupByAndCount(fieldName));
    }

    /**
     * Asynchronously groups documents by multiple fields and counts frequency in each group.
     *
     * <p>This method performs a non-blocking group operation that groups documents by multiple
     * fields and counts the number of documents in each group. It's a convenience method that
     * creates an aggregation pipeline with $group and $sum stages for composite grouping. The
     * result includes the grouping field values as a composite key and a count of documents.</p>
     *
     * <p>This method is marked as @Beta and may change in future versions.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Count employees by department and location:
     * async.groupByAndCount(Arrays.asList("department", "location"))
     *      .thenRunAsync(stream -> {
     *          stream.forEach(group -> {
     *              Document groupKey = (Document) group.get("_id");
     *              System.out.println("Department: " + groupKey.get("department") +
     *                               ", Location: " + groupKey.get("location") +
     *                               ", Count: " + group.get("count"));
     *          });
     *      });
     *
     * // Count products by category and brand:
     * List<String> fields = Arrays.asList("category", "brand", "status");
     * async.groupByAndCount(fields)
     *      .thenRunAsync(stream -> stream.forEach(System.out::println));
     * }</pre>
     *
     * @param fieldNames the collection of field names to group by and count
     * @return a ContinuableFuture that completes with a Stream of Documents containing composite group keys and counts
     * @throws IllegalArgumentException if fieldNames is null or empty (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @see #groupByAndCount(String)
     * @see #groupBy(Collection)
     * @see #aggregate(List)
     */
    @Beta
    public ContinuableFuture<Stream<Document>> groupByAndCount(final Collection<String> fieldNames) {
        return asyncExecutor.execute(() -> collExecutor.groupByAndCount(fieldNames));
    }

    /**
     * Asynchronously executes a map-reduce operation.
     *
     * <p>This method performs a non-blocking map-reduce operation using JavaScript functions.
     * Note: This method is deprecated as MongoDB recommends using the aggregation framework
     * instead for better performance and functionality.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String mapFunction = "function() { emit(this.category, this.price); }";
     * String reduceFunction = "function(key, values) { return Array.sum(values); }";
     * async.mapReduce(mapFunction, reduceFunction)
     *      .thenRunAsync(stream -> stream.forEach(System.out::println));
     * }</pre>
     *
     * @param mapFunction the JavaScript map function
     * @param reduceFunction the JavaScript reduce function
     * @return a ContinuableFuture that completes with a Stream of result Documents
     * @throws IllegalArgumentException if mapFunction or reduceFunction is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @deprecated Use {@link #aggregate(List)} with aggregation pipeline instead.
     * @see #aggregate(List)
     */
    @Deprecated
    public ContinuableFuture<Stream<Document>> mapReduce(final String mapFunction, final String reduceFunction) {
        return asyncExecutor.execute(() -> collExecutor.mapReduce(mapFunction, reduceFunction));
    }

    /**
     * Asynchronously executes a map-reduce operation with type conversion.
     *
     * <p>This method performs a non-blocking map-reduce operation with automatic
     * deserialization of results to the specified type. Note: This method is deprecated
     * as MongoDB recommends using the aggregation framework instead.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.mapReduce(mapFunction, reduceFunction, CategorySum.class)
     *      .thenRunAsync(stream -> stream.forEach(sum -> System.out.println(sum)));
     * }</pre>
     *
     * @param <T> the type of the result documents
     * @param mapFunction the JavaScript map function
     * @param reduceFunction the JavaScript reduce function
     * @param rowType the class to deserialize the result documents into
     * @return a ContinuableFuture that completes with a Stream of result objects
     * @throws IllegalArgumentException if any parameter is null (propagated through future)
     * @throws com.mongodb.MongoException if the database operation fails (propagated through future)
     * @deprecated Use {@link #aggregate(List, Class)} with aggregation pipeline instead.
     * @see #aggregate(List, Class)
     */
    @Deprecated
    public <T> ContinuableFuture<Stream<T>> mapReduce(final String mapFunction, final String reduceFunction, final Class<T> rowType) {
        return asyncExecutor.execute(() -> collExecutor.mapReduce(mapFunction, reduceFunction, rowType));
    }
}
