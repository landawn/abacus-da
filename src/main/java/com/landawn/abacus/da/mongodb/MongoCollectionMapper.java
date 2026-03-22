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

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.landawn.abacus.annotation.Beta;
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
 * Type-safe MongoDB collection mapper providing object-document mapping (ODM) for strongly-typed entity operations.
 *
 * <p>This class combines the functionality of {@code MongoCollectionExecutor} with automatic type conversion
 * to provide a type-safe, object-oriented interface for MongoDB operations. It eliminates the need for manual
 * document-to-object conversion while maintaining full access to MongoDB's query and update capabilities.</p>
 *
 * <h2>Key Features</h2>
 * <h3>Core Capabilities:</h3>
 * <ul>
 *   <li><strong>Type Safety:</strong> All operations return properly typed objects instead of raw Documents</li>
 *   <li><strong>Automatic Conversion:</strong> Seamless conversion between Java entities and BSON documents</li>
 *   <li><strong>ID Mapping:</strong> Automatic mapping between entity ID fields and MongoDB's "_id" field</li>
 *   <li><strong>Full Query Support:</strong> Complete access to MongoDB query operators and aggregation</li>
 *   <li><strong>Bulk Operations:</strong> Type-safe bulk insert, update, and delete operations</li>
 *   <li><strong>Streaming:</strong> Stream-based processing with automatic object conversion</li>
 * </ul>
 *
 * <h3>Entity Requirements:</h3>
 * <p>Entity classes used with this mapper should follow these conventions:</p>
 * <ul>
 *   <li>Have a default (no-argument) constructor</li>
 *   <li>Use proper getter/setter methods for properties</li>
 *   <li>Mark ID fields with @Id annotation or use "id" property name</li>
 *   <li>Use MongoDB-compatible data types (or provide custom converters)</li>
 * </ul>
 *
 * <h3>Thread Safety:</h3>
 * <p>This class is thread-safe. All operations can be called concurrently from multiple threads
 * without external synchronization.</p>
 *
 * <h3>Performance Considerations:</h3>
 * <ul>
 *   <li>Object conversion adds overhead compared to raw Document operations</li>
 *   <li>Use projection to limit fields when full objects are not needed</li>
 *   <li>Batch operations are more efficient than individual entity operations</li>
 *   <li>Consider caching for frequently accessed reference data</li>
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
 * // Create mapper:
 * MongoCollectionMapper<User> userMapper = mongoDB.collMapper("users", User.class);
 *
 * // Type-safe operations:
 * User newUser = new User("John Doe", "john@example.com", new Date());
 * userMapper.insertOne(newUser);
 *
 * Optional<User> user = userMapper.findFirst(Filters.eq("email", "john@example.com"));
 * List<User> activeUsers = userMapper.stream(Filters.eq("active", true)).toList();
 *
 * userMapper.updateMany(Filters.lt("lastLogin", new Date(1672531200000L)),
 *                        Updates.set("status", "inactive"));
 * }</pre>
 *
 * @param <T> the entity type for object-document mapping operations
 * @see MongoCollectionExecutor
 * @see com.landawn.abacus.annotation.Id
 * @see com.mongodb.client.model.Filters
 * @see com.mongodb.client.model.Projections
 * @see com.mongodb.client.model.Sorts
 * @see com.mongodb.client.model.Updates
 * @see com.mongodb.client.model.Aggregates
 * @see com.mongodb.client.model.Indexes
 * @see <a href="https://www.mongodb.com/docs/drivers/java/sync/current/fundamentals/builders/">Simplify your Code with Builders</a>
 * @see <a href="https://www.mongodb.com/docs/drivers/java/sync/current/">MongoDB Java Driver</a>
 */
public final class MongoCollectionMapper<T> {

    private final MongoCollectionExecutor collExecutor;

    private final Class<T> rowType;

    MongoCollectionMapper(final MongoCollectionExecutor collExecutor, final Class<T> resultClass) {
        this.collExecutor = collExecutor;
        rowType = resultClass;
    }

    /**
     * Returns the underlying MongoCollectionExecutor for advanced operations.
     *
     * <p>This method provides access to the raw executor when you need operations not
     * exposed by the mapper or when working with untyped Documents is more appropriate.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> userMapper = mongoDB.collMapper(User.class);
     * MongoCollectionExecutor executor = userMapper.collExecutor();
     * // Use raw executor for complex operations:
     * Document complexResult = executor.aggregate(complexPipeline).first();
     * }</pre>
     *
     * @return the underlying MongoCollectionExecutor instance
     * @see MongoCollectionExecutor
     */
    public MongoCollectionExecutor collExecutor() {
        return collExecutor;
    }

    /**
     * Checks if an entity exists by its ObjectId string representation.
     *
     * <p>This method performs an efficient existence check for the mapped entity type
     * using the provided ObjectId string. The ObjectId must be a valid 24-character
     * hexadecimal representation.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> userMapper = mongoDB.collMapper(User.class);
     * String userId = "507f1f77bcf86cd799439011";
     * if (userMapper.exists(userId)) {
     *     System.out.println("User exists");
     * }
     * }</pre>
     *
     * @param objectId the string representation of the ObjectId to check
     * @return {@code true} if an entity with the specified ObjectId exists, {@code false} otherwise
     * @throws IllegalArgumentException if objectId is null, empty, or invalid format
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #exists(ObjectId)
     */
    public boolean exists(final String objectId) {
        return collExecutor.exists(objectId);
    }

    /**
     * Checks if an entity exists by its ObjectId.
     *
     * <p>This method performs efficient existence verification using the native ObjectId type
     * for the mapped entity class.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId userId = new ObjectId();
     * MongoCollectionMapper<User> mapper = mongoDB.collMapper(User.class);
     * boolean userExists = mapper.exists(userId);
     * }</pre>
     *
     * @param objectId the ObjectId to check for existence
     * @return {@code true} if an entity with the specified ObjectId exists, {@code false} otherwise
     * @throws IllegalArgumentException if objectId is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see ObjectId
     */
    public boolean exists(final ObjectId objectId) {
        return collExecutor.exists(objectId);
    }

    /**
     * Checks if any entities exist matching the specified filter criteria.
     *
     * <p>This method performs an existence check for the mapped entity type using the provided
     * filter. It's more efficient than retrieving entities when you only need to verify existence.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collMapper(User.class);
     * // Check if any active users exist:
     * boolean hasActiveUsers = mapper.exists(Filters.eq("status", "active"));
     * // Check using JSON filter:
     * boolean hasRecentUsers = mapper.exists("{createdAt: {$gte: ISODate('2023-01-01')}}");
     * }</pre>
     *
     * @param filter the query filter to match entities against
     * @return {@code true} if any entities match the filter, {@code false} otherwise
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see com.mongodb.client.model.Filters
     */
    public boolean exists(final Bson filter) {
        return collExecutor.exists(filter);
    }

    /**
     * Returns the total number of entities in the collection (blocking operation).
     *
     * <p>This method counts all entities of the mapped type in the collection.
     * For large collections, consider using {@link #collExecutor()}{@code .estimatedDocumentCount()} for better performance
     * when exact counts are not required.</p>
     *
     * <p><b>Note:</b> This method performs a blocking operation. For non-blocking operations, use
     * {@link #collExecutor()}.{@code async()}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> userMapper = mongoDB.collMapper(User.class);
     * long totalUsers = userMapper.count();
     * System.out.println("Total users: " + totalUsers);
     * }</pre>
     *
     * @return the total number of entities in the collection
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #count(Bson)
     * @see #collExecutor()
     */
    public long count() {
        return collExecutor.count();
    }

    /**
     * Returns the number of entities matching the specified filter.
     *
     * <p>This method counts entities of the mapped type that match the given filter criteria.
     * The count operation respects any read preference settings configured on the collection.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collMapper(User.class);
     * // Count active users:
     * long activeUsers = mapper.count(Filters.eq("status", "active"));
     * // Count using JSON filter:
     * long adultUsers = mapper.count("{age: {$gte: 18}}");
     * }</pre>
     *
     * @param filter the query filter to count matching entities
     * @return the number of entities matching the filter
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see com.mongodb.client.model.Filters
     */
    public long count(final Bson filter) {
        return collExecutor.count(filter);
    }

    /**
     * Returns the count of entities matching the filter with additional count options.
     *
     * <p>This method provides fine-grained control over the count operation for the mapped
     * entity type through CountOptions, allowing specification of limits, skips, and hints.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collMapper(User.class);
     * // Count with limit to avoid expensive operations:
     * CountOptions options = new CountOptions().limit(1000);
     * long limitedCount = mapper.count(Filters.exists("email"), options);
     * }</pre>
     *
     * @param filter the query filter to count matching entities
     * @param options additional options for the count operation (null uses defaults)
     * @return the number of entities matching the filter within the specified constraints
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see CountOptions
     */
    public long count(final Bson filter, final CountOptions options) {
        return collExecutor.count(filter, options);
    }

    /**
     * Retrieves an entity by its ObjectId string representation with automatic type conversion.
     *
     * <p>This method performs a find operation using the provided ObjectId string and automatically
     * converts the result to the mapped entity type. The ObjectId string must be a valid
     * 24-character hexadecimal representation.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collMapper(User.class);
     * String userId = "507f1f77bcf86cd799439011";
     * Optional<User> user = mapper.get(userId);
     * user.ifPresent(u -> System.out.println("Found: " + u.getName()));
     * }</pre>
     *
     * @param objectId the string representation of the ObjectId to retrieve
     * @return an Optional containing the entity if found, or empty if not found
     * @throws IllegalArgumentException if objectId is null, empty, or invalid format
     * @throws com.mongodb.MongoException if the database operation fails
     * @see Optional
     * @see #get(ObjectId)
     */
    public Optional<T> get(final String objectId) {
        return collExecutor.get(objectId, rowType);
    }

    /**
     * Retrieves an entity by its ObjectId with automatic type conversion.
     *
     * <p>This method performs a find operation using the native ObjectId type and automatically
     * converts the result to the mapped entity type. This provides type safety and eliminates
     * manual conversion code while using the efficient native ObjectId format.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collMapper(User.class);
     * ObjectId userId = new ObjectId();
     * Optional<User> user = mapper.get(userId);
     * user.ifPresent(u -> System.out.println("Found: " + u.getName()));
     * }</pre>
     *
     * @param objectId the ObjectId to search for
     * @return an Optional containing the entity if found, or empty if not found
     * @throws IllegalArgumentException if objectId is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see ObjectId
     * @see Optional
     */
    public Optional<T> get(final ObjectId objectId) {
        return collExecutor.get(objectId, rowType);
    }

    /**
     * Retrieves an entity with field projection by ObjectId string.
     *
     * <p>This method combines entity retrieval with field projection and automatic type conversion.
     * Only the specified fields are retrieved from the database, reducing network bandwidth and
     * improving performance for entities with many fields. The result is automatically converted
     * to the mapped entity type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collMapper(User.class);
     * String userId = "507f1f77bcf86cd799439011";
     * Collection<String> fields = Arrays.asList("name", "email", "status");
     * Optional<User> partialUser = mapper.get(userId, fields);
     * }</pre>
     *
     * @param objectId the string representation of the ObjectId to search for
     * @param selectPropNames collection of field names to include (null includes all fields)
     * @return an Optional containing the entity with only the specified fields populated, or empty if not found
     * @throws IllegalArgumentException if objectId is null, empty, or invalid format
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #get(ObjectId, Collection)
     */
    public Optional<T> get(final String objectId, final Collection<String> selectPropNames) {
        return collExecutor.get(objectId, selectPropNames, rowType);
    }

    /**
     * Retrieves an entity with field projection by ObjectId.
     *
     * <p>This method provides efficient entity retrieval with projection using the native ObjectId.
     * Field projection reduces the amount of data transferred and can significantly improve performance
     * when working with entities containing many fields or large embedded objects. The result is
     * automatically converted to the mapped entity type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collMapper(User.class);
     * ObjectId id = new ObjectId();
     * Collection<String> fields = Set.of("name", "email");
     * Optional<User> user = mapper.get(id, fields);
     * }</pre>
     *
     * @param objectId the ObjectId to search for
     * @param selectPropNames collection of field names to include (null includes all fields)
     * @return an Optional containing the entity with only the specified fields populated, or empty if not found
     * @throws IllegalArgumentException if objectId is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see com.mongodb.client.model.Projections
     */
    public Optional<T> get(final ObjectId objectId, final Collection<String> selectPropNames) {
        return collExecutor.get(objectId, selectPropNames, rowType);
    }

    /**
     * Retrieves an entity by its ObjectId string representation, returning null if not found.
     *
     * <p>This method is the historical nullable counterpart to {@link #get(String)}.
     * It returns {@code null} instead of an empty Optional when the entity is not found.
     * The {@code gett} name is preserved for API compatibility; prefer {@link #get(String)}
     * when Optional-based handling is clearer.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collMapper(User.class);
     * User user = mapper.gett("507f1f77bcf86cd799439011");
     * if (user != null) {
     *     System.out.println("Found user: " + user.getName());
     * }
     * }</pre>
     *
     * @param objectId the string representation of the ObjectId (24 hex characters)
     * @return the matching entity, or {@code null} if not found
     * @throws IllegalArgumentException if objectId is null, empty, or invalid format
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #get(String)
     * @see #gett(ObjectId)
     */
    public T gett(final String objectId) {
        return collExecutor.gett(objectId, rowType);
    }

    /**
     * Retrieves an entity by its ObjectId, returning null if not found.
     *
     * <p>This method is a convenience variant of {@link #get(ObjectId)} that returns {@code null} instead
     * of an empty Optional when the entity is not found. Use this method when null handling is preferred
     * over Optional patterns.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collMapper(User.class);
     * ObjectId id = new ObjectId();
     * User user = mapper.gett(id);
     * if (user != null) {
     *     processUser(user);
     * }
     * }</pre>
     *
     * @param objectId the ObjectId to search for
     * @return the matching entity, or {@code null} if not found
     * @throws IllegalArgumentException if objectId is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #get(ObjectId)
     * @see #gett(String)
     */
    public T gett(final ObjectId objectId) {
        return collExecutor.gett(objectId, rowType);
    }

    /**
     * Retrieves an entity with field projection by ObjectId string, returning null if not found.
     *
     * <p>This method combines entity retrieval with field projection, returning {@code null} instead
     * of an empty Optional when no matching entity exists. Only the specified fields are retrieved
     * from the database, reducing network bandwidth and improving performance.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collMapper(User.class);
     * Collection<String> fields = Arrays.asList("name", "email", "department");
     * User partialUser = mapper.gett("507f1f77bcf86cd799439011", fields);
     * if (partialUser != null) {
     *     System.out.println("User department: " + partialUser.getDepartment());
     * }
     * }</pre>
     *
     * @param objectId the string representation of the ObjectId (24 hex characters)
     * @param selectPropNames collection of field names to include in the projection (null for all fields)
     * @return the matching entity with projected fields, or {@code null} if not found
     * @throws IllegalArgumentException if objectId is null, empty, or invalid format
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #get(String, Collection)
     * @see #gett(ObjectId, Collection)
     */
    public T gett(final String objectId, final Collection<String> selectPropNames) {
        return collExecutor.gett(objectId, selectPropNames, rowType);
    }

    /**
     * Retrieves an entity with field projection by ObjectId, returning null if not found.
     *
     * <p>This method provides efficient entity retrieval with projection using the native ObjectId,
     * returning {@code null} instead of an empty Optional when no matching entity exists. Field
     * projection reduces the amount of data transferred and can significantly improve performance
     * when working with entities containing many fields or large embedded objects.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collMapper(User.class);
     * ObjectId id = new ObjectId();
     * Collection<String> fields = Set.of("name", "email");
     * User user = mapper.gett(id, fields);
     * if (user != null) {
     *     sendEmail(user.getEmail());
     * }
     * }</pre>
     *
     * @param objectId the ObjectId to search for
     * @param selectPropNames collection of field names to include in the projection (null for all fields)
     * @return the matching entity with projected fields, or {@code null} if not found
     * @throws IllegalArgumentException if objectId is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #get(ObjectId, Collection)
     * @see com.mongodb.client.model.Projections
     */
    public T gett(final ObjectId objectId, final Collection<String> selectPropNames) {
        return collExecutor.gett(objectId, selectPropNames, rowType);
    }

    /**
     * Finds the first entity matching the specified filter criteria.
     *
     * <p>This method performs a find operation that returns at most one entity matching the given
     * filter, automatically converted to the mapped entity type. It's equivalent to a find operation
     * with a limit of 1. The result is wrapped in an Optional to handle the case where no matching
     * entities exist.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collMapper(User.class);
     * // Find the first active user:
     * Optional<User> user = mapper.findFirst(Filters.eq("status", "active"));
     * // Using JSON filter with Document.parse():
     * Optional<User> recentUser = mapper.findFirst(Document.parse("{createdAt: {$gte: ISODate('2023-01-01')}}"));
     * }</pre>
     *
     * @param filter the query filter to match entities against
     * @return an Optional containing the first matching entity, or empty if none found
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see Optional
     * @see com.mongodb.client.model.Filters
     */
    public Optional<T> findFirst(final Bson filter) {
        return collExecutor.findFirst(filter, rowType);
    }

    /**
     * Finds the first entity matching the filter with field projection.
     *
     * <p>This method performs a find operation with field projection to retrieve only the specified
     * fields from the first matching document. This is more efficient when you only need specific
     * fields from the entity, reducing network bandwidth and memory usage. The result is automatically
     * converted to the entity type with only the projected fields populated.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collMapper(User.class);
     * Collection<String> fields = Arrays.asList("name", "email");
     * // Get first active user with only name and email:
     * Optional<User> user = mapper.findFirst(fields, Filters.eq("status", "active"));
     * }</pre>
     *
     * @param selectPropNames collection of field names to include in the projection (null for all fields)
     * @param filter the query filter to match entities against
     * @return an Optional containing the first matching entity with projected fields, or empty if none found
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #findFirst(Bson)
     * @see #findFirst(Collection, Bson, Bson)
     */
    public Optional<T> findFirst(final Collection<String> selectPropNames, final Bson filter) {
        return collExecutor.findFirst(selectPropNames, filter, rowType);
    }

    /**
     * Finds the first entity matching the filter with field projection and sorting.
     *
     * <p>This method combines filtering, field projection, and sorting to retrieve the first entity
     * that matches the criteria. The sorting determines which entity is considered "first" when
     * multiple documents match the filter. Only the specified fields are retrieved and populated
     * in the returned entity.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Order> mapper = mongoDB.collMapper(Order.class);
     * Collection<String> fields = Arrays.asList("orderId", "total", "status");
     * Bson filter = Filters.eq("customerId", "CUST123");
     * Bson sort = Sorts.descending("createdAt");   // Most recent first
     * Optional<Order> recentOrder = mapper.findFirst(fields, filter, sort);
     * }</pre>
     *
     * @param selectPropNames collection of field names to include in the projection (null for all fields)
     * @param filter the query filter to match entities against
     * @param sort the sort specification to determine result ordering
     * @return an Optional containing the first matching entity with projected fields, or empty if none found
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #findFirst(Collection, Bson)
     * @see #findFirst(Bson, Bson, Bson)
     * @see com.mongodb.client.model.Sorts
     */
    public Optional<T> findFirst(final Collection<String> selectPropNames, final Bson filter, final Bson sort) {
        return collExecutor.findFirst(selectPropNames, filter, sort, rowType);
    }

    /**
     * Finds the first entity matching the filter with BSON projection and sorting.
     *
     * <p>This method provides the most advanced find operation using BSON objects for projection
     * and sorting. BSON projection offers more capabilities than simple field lists, including
     * computed fields, array slicing, and conditional inclusions. This is the most flexible
     * findFirst variant for complex projection requirements.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Article> mapper = mongoDB.collMapper(Article.class);
     * Bson projection = Projections.fields(
     *     Projections.include("title", "author"),
     *     Projections.slice("tags", 5)
     * );
     * Bson filter = Filters.eq("status", "published");
     * Bson sort = Sorts.descending("views");
     * Optional<Article> popularArticle = mapper.findFirst(projection, filter, sort);
     * }</pre>
     *
     * @param projection the BSON projection specification for field selection (null for all fields)
     * @param filter the query filter to match entities against
     * @param sort the BSON sort specification for result ordering
     * @return an Optional containing the first matching entity with projected fields, or empty if none found
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #findFirst(Collection, Bson, Bson)
     * @see com.mongodb.client.model.Projections
     * @see com.mongodb.client.model.Sorts
     */
    public Optional<T> findFirst(final Bson projection, final Bson filter, final Bson sort) {
        return collExecutor.findFirst(projection, filter, sort, rowType);
    }

    /**
     * Retrieves all entities matching the specified filter as a list.
     *
     * <p>This method performs a find operation and returns all matching entities as a List of the mapped type.
     * Each document is automatically converted to the entity type. Use this method when you need to retrieve
     * all matching entities at once. For large result sets, consider using streaming operations or pagination
     * to manage memory usage.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collMapper(User.class);
     * // Find all active users:
     * List<User> activeUsers = mapper.list(Filters.eq("status", "active"));
     * activeUsers.forEach(user -> processUser(user));
     * }</pre>
     *
     * @param filter the query filter to match entities against
     * @return a List containing all matching entities (empty list if none found)
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #stream(Bson)
     * @see com.mongodb.client.model.Filters
     */
    public List<T> list(final Bson filter) {
        return collExecutor.list(filter, rowType);
    }

    /**
     * Retrieves a paginated list of entities matching the specified filter.
     *
     * <p>This method performs a find operation with pagination support, returning a subset
     * of matching entities based on the offset and count parameters. This is essential for
     * implementing efficient pagination in applications with large datasets.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Product> mapper = mongoDB.collMapper(Product.class);
     * Bson filter = Filters.eq("category", "electronics");
     * // Get page 2 (next 20 products):
     * List<Product> page2 = mapper.list(filter, 20, 20);
     * }</pre>
     *
     * @param filter the query filter to match entities against
     * @param offset the number of documents to skip from the beginning (0 for first page)
     * @param count the maximum number of entities to return
     * @return a List containing the requested page of matching entities
     * @throws IllegalArgumentException if filter is null, offset is negative, or count is non-positive
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #list(Bson)
     * @see #list(Collection, Bson, int, int)
     */
    public List<T> list(final Bson filter, final int offset, final int count) {
        return collExecutor.list(filter, offset, count, rowType);
    }

    /**
     * Retrieves all entities matching the filter with field projection.
     *
     * <p>This method performs a find operation with field projection, returning all matching entities
     * with only the specified fields populated. This reduces network bandwidth and memory usage
     * when you don't need all fields from the entity.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collMapper(User.class);
     * Collection<String> fields = Arrays.asList("name", "email", "status");
     * // Get all active users with only name, email, and status:
     * List<User> users = mapper.list(fields, Filters.eq("status", "active"));
     * }</pre>
     *
     * @param selectPropNames collection of field names to include in the projection (null for all fields)
     * @param filter the query filter to match entities against
     * @return a List containing all matching entities with projected fields
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #list(Bson)
     * @see #list(Collection, Bson, int, int)
     */
    public List<T> list(final Collection<String> selectPropNames, final Bson filter) {
        return collExecutor.list(selectPropNames, filter, rowType);
    }

    /**
     * Retrieves entities matching the filter with field projection and pagination.
     *
     * <p>This method combines filtering, field projection, and pagination to retrieve a specific
     * range of matching entities with only the selected fields populated. This is the most
     * memory-efficient way to implement paginated results with large datasets.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Product> mapper = mongoDB.collMapper(Product.class);
     * Collection<String> fields = Arrays.asList("name", "price", "category");
     * Bson filter = Filters.eq("inStock", true);
     * // Get second page of available products with minimal data:
     * List<Product> page2 = mapper.list(fields, filter, 20, 20);
     * }</pre>
     *
     * @param selectPropNames collection of field names to include in the projection (null for all fields)
     * @param filter the query filter to match entities against
     * @param offset the number of entities to skip (0-based)
     * @param count the maximum number of entities to return
     * @return a List containing the specified range of matching entities with projected fields
     * @throws IllegalArgumentException if filter is null, or if offset/count is negative
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #list(Collection, Bson)
     * @see #list(Bson, int, int)
     */
    public List<T> list(final Collection<String> selectPropNames, final Bson filter, final int offset, final int count) {
        return collExecutor.list(selectPropNames, filter, offset, count, rowType);
    }

    /**
     * Retrieves all entities matching the filter with field projection and sorting.
     *
     * <p>This method combines filtering, field projection, and sorting to retrieve all matching
     * entities in the specified order with only the selected fields. The sorting ensures predictable
     * result ordering, which is essential for consistent pagination and user interfaces.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Article> mapper = mongoDB.collMapper(Article.class);
     * Collection<String> fields = Arrays.asList("title", "author", "publishedAt", "viewCount");
     * Bson filter = Filters.eq("status", "published");
     * Bson sort = Sorts.descending("viewCount");   // Most viewed first
     * List<Article> articles = mapper.list(fields, filter, sort);
     * }</pre>
     *
     * @param selectPropNames collection of field names to include in the projection (null for all fields)
     * @param filter the query filter to match entities against
     * @param sort the sort specification for result ordering
     * @return a List containing all matching entities with projected fields in sorted order
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #list(Collection, Bson)
     * @see #list(Collection, Bson, Bson, int, int)
     * @see com.mongodb.client.model.Sorts
     */
    public List<T> list(final Collection<String> selectPropNames, final Bson filter, final Bson sort) {
        return collExecutor.list(selectPropNames, filter, sort, rowType);
    }

    /**
     * Retrieves entities matching the filter with field projection, sorting, and pagination.
     *
     * <p>This method provides the most comprehensive querying capability, combining filtering,
     * field projection, sorting, and pagination. It's ideal for implementing advanced search
     * interfaces with efficient data transfer and predictable ordering.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collMapper(User.class);
     * Collection<String> fields = Arrays.asList("username", "email", "lastLoginAt");
     * Bson filter = Filters.eq("status", "active");
     * Bson sort = Sorts.descending("lastLoginAt");
     * // Get second page of active users sorted by last login:
     * List<User> recentUsers = mapper.list(fields, filter, sort, 25, 25);
     * }</pre>
     *
     * @param selectPropNames collection of field names to include in the projection (null for all fields)
     * @param filter the query filter to match entities against
     * @param sort the sort specification for result ordering
     * @param offset the number of entities to skip (0-based)
     * @param count the maximum number of entities to return
     * @return a List containing the specified range of matching entities with projected fields in sorted order
     * @throws IllegalArgumentException if filter is null, or if offset/count is negative
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #list(Collection, Bson, Bson)
     * @see #list(Collection, Bson, int, int)
     * @see com.mongodb.client.model.Sorts
     */
    public List<T> list(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final int offset, final int count) {
        return collExecutor.list(selectPropNames, filter, sort, offset, count, rowType);
    }

    /**
     * Retrieves all entities matching the filter with BSON projection and sorting.
     *
     * <p>This method uses BSON-based projection instead of field name collections, providing
     * more advanced projection capabilities such as computed fields, subdocument selection,
     * and MongoDB aggregation-style projections. All matching entities are returned in sorted order.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Order> mapper = mongoDB.collMapper(Order.class);
     * Bson projection = Projections.fields(
     *     Projections.include("customerId", "items"),
     *     Projections.computed("totalAmount", new Document("$sum", "$items.price"))
     * );
     * Bson filter = Filters.gte("orderDate", LocalDate.now().minusDays(30));
     * Bson sort = Sorts.descending("orderDate");
     * List<Order> recentOrders = mapper.list(projection, filter, sort);
     * }</pre>
     *
     * @param projection the BSON projection specification for field selection and transformation
     * @param filter the query filter to match entities against
     * @param sort the sort specification for result ordering
     * @return a List containing all matching entities with BSON-projected fields in sorted order
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #list(Collection, Bson, Bson)
     * @see com.mongodb.client.model.Projections
     * @see com.mongodb.client.model.Sorts
     */
    public List<T> list(final Bson projection, final Bson filter, final Bson sort) {
        return collExecutor.list(projection, filter, sort, rowType);
    }

    /**
     * Retrieves entities matching the filter with BSON projection, sorting, and pagination.
     *
     * <p>This method provides maximum querying flexibility using BSON-based projection with
     * pagination support. It enables advanced field transformations, computed fields, and
     * subdocument selections while efficiently handling large result sets through pagination.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Product> mapper = mongoDB.collMapper(Product.class);
     * Bson projection = Projections.fields(
     *     Projections.include("name", "category"),
     *     Projections.elemMatch("reviews", Filters.gte("rating", 4))
     * );
     * Bson filter = Filters.eq("category", "electronics");
     * Bson sort = Sorts.descending("avgRating");
     * // Get top-rated electronics, page 1:
     * List<Product> topRated = mapper.list(projection, filter, sort, 0, 20);
     * }</pre>
     *
     * @param projection the BSON projection specification for field selection and transformation
     * @param filter the query filter to match entities against
     * @param sort the sort specification for result ordering
     * @param offset the number of entities to skip (0-based)
     * @param count the maximum number of entities to return
     * @return a List containing the specified range of matching entities with BSON-projected fields in sorted order
     * @throws IllegalArgumentException if filter is null, or if offset/count is negative
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #list(Bson, Bson, Bson)
     * @see #list(Collection, Bson, Bson, int, int)
     * @see com.mongodb.client.model.Projections
     */
    public List<T> list(final Bson projection, final Bson filter, final Bson sort, final int offset, final int count) {
        return collExecutor.list(projection, filter, sort, offset, count, rowType);
    }

    /**
     * Queries for a single boolean value from entities matching the filter.
     *
     * <p>This method retrieves the value of a specific boolean property from the first entity
     * matching the filter. The result is wrapped in an OptionalBoolean which handles the case
     * where no matching entity is found or the property value is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collMapper(User.class);
     * OptionalBoolean isActive = mapper.queryForBoolean("active", Filters.eq("userId", "123"));
     * if (isActive.isPresent()) {
     *     System.out.println("Active: " + isActive.getAsBoolean());
     * }
     * }</pre>
     *
     * @param propName the name of the boolean property to retrieve
     * @param filter the query filter to match entities against
     * @return an OptionalBoolean containing the property value if found, or empty if not found
     * @throws IllegalArgumentException if propName or filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see OptionalBoolean
     */
    @Beta
    public OptionalBoolean queryForBoolean(final String propName, final Bson filter) {
        return collExecutor.queryForBoolean(propName, filter);
    }

    /**
     * Queries for a single character value from entities matching the filter.
     *
     * <p>This method retrieves the value of a specific character property from the first entity
     * matching the filter. The result is wrapped in an OptionalChar which handles the case
     * where no matching entity is found or the property value is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Entity> mapper = mongoDB.collMapper(Entity.class);
     * OptionalChar grade = mapper.queryForChar("grade", Filters.eq("studentId", "456"));
     * grade.ifPresent(g -> System.out.println("Grade: " + g));
     * }</pre>
     *
     * @param propName the name of the character property to retrieve
     * @param filter the query filter to match entities against
     * @return an OptionalChar containing the property value if found, or empty if not found
     * @throws IllegalArgumentException if propName or filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see OptionalChar
     */
    @Beta
    public OptionalChar queryForChar(final String propName, final Bson filter) {
        return collExecutor.queryForChar(propName, filter);
    }

    /**
     * Queries for a single byte value from entities matching the filter.
     *
     * <p>This method retrieves the value of a specific byte property from the first entity
     * matching the filter. The result is wrapped in an OptionalByte which handles the case
     * where no matching entity is found or the property value is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Entity> mapper = mongoDB.collMapper(Entity.class);
     * OptionalByte flags = mapper.queryForByte("flags", Filters.eq("id", "789"));
     * flags.ifPresent(f -> System.out.println("Flags: " + f));
     * }</pre>
     *
     * @param propName the name of the byte property to retrieve
     * @param filter the query filter to match entities against
     * @return an OptionalByte containing the property value if found, or empty if not found
     * @throws IllegalArgumentException if propName or filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see OptionalByte
     */
    @Beta
    public OptionalByte queryForByte(final String propName, final Bson filter) {
        return collExecutor.queryForByte(propName, filter);
    }

    /**
     * Queries for a single short value from entities matching the filter.
     *
     * <p>This method retrieves the value of a specific short property from the first entity
     * matching the filter. The result is wrapped in an OptionalShort which handles the case
     * where no matching entity is found or the property value is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Product> mapper = mongoDB.collMapper(Product.class);
     * OptionalShort quantity = mapper.queryForShort("quantity", Filters.eq("sku", "ABC123"));
     * quantity.ifPresent(q -> System.out.println("Quantity: " + q));
     * }</pre>
     *
     * @param propName the name of the short property to retrieve
     * @param filter the query filter to match entities against
     * @return an OptionalShort containing the property value if found, or empty if not found
     * @throws IllegalArgumentException if propName or filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see OptionalShort
     */
    @Beta
    public OptionalShort queryForShort(final String propName, final Bson filter) {
        return collExecutor.queryForShort(propName, filter);
    }

    /**
     * Queries for a single integer value from entities matching the filter.
     *
     * <p>This method retrieves the value of a specific integer property from the first entity
     * matching the filter. The result is wrapped in an OptionalInt which handles the case
     * where no matching entity is found or the property value is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collMapper(User.class);
     * OptionalInt age = mapper.queryForInt("age", Filters.eq("userId", "user123"));
     * age.ifPresent(a -> System.out.println("Age: " + a));
     * }</pre>
     *
     * @param propName the name of the integer property to retrieve
     * @param filter the query filter to match entities against
     * @return an OptionalInt containing the property value if found, or empty if not found
     * @throws IllegalArgumentException if propName or filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see OptionalInt
     */
    @Beta
    public OptionalInt queryForInt(final String propName, final Bson filter) {
        return collExecutor.queryForInt(propName, filter);
    }

    /**
     * Queries for a single long value from entities matching the filter.
     *
     * <p>This method retrieves the value of a specific long property from the first entity
     * matching the filter. The result is wrapped in an OptionalLong which handles the case
     * where no matching entity is found or the property value is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Transaction> mapper = mongoDB.collMapper(Transaction.class);
     * OptionalLong timestamp = mapper.queryForLong("timestamp", Filters.eq("txnId", "TXN789"));
     * timestamp.ifPresent(t -> System.out.println("Timestamp: " + new Date(t)));
     * }</pre>
     *
     * @param propName the name of the long property to retrieve
     * @param filter the query filter to match entities against
     * @return an OptionalLong containing the property value if found, or empty if not found
     * @throws IllegalArgumentException if propName or filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see OptionalLong
     */
    @Beta
    public OptionalLong queryForLong(final String propName, final Bson filter) {
        return collExecutor.queryForLong(propName, filter);
    }

    /**
     * Queries for a single float value from entities matching the filter.
     *
     * <p>This method retrieves the value of a specific float property from the first entity
     * matching the filter. The result is wrapped in an OptionalFloat which handles the case
     * where no matching entity is found or the property value is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Product> mapper = mongoDB.collMapper(Product.class);
     * OptionalFloat rating = mapper.queryForFloat("rating", Filters.eq("productId", "PROD456"));
     * rating.ifPresent(r -> System.out.println("Rating: " + r));
     * }</pre>
     *
     * @param propName the name of the float property to retrieve
     * @param filter the query filter to match entities against
     * @return an OptionalFloat containing the property value if found, or empty if not found
     * @throws IllegalArgumentException if propName or filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see OptionalFloat
     */
    @Beta
    public OptionalFloat queryForFloat(final String propName, final Bson filter) {
        return collExecutor.queryForFloat(propName, filter);
    }

    /**
     * Queries for a single double value from entities matching the filter.
     *
     * <p>This method retrieves the value of a specific double property from the first entity
     * matching the filter. The result is wrapped in an OptionalDouble which handles the case
     * where no matching entity is found or the property value is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Order> mapper = mongoDB.collMapper(Order.class);
     * OptionalDouble total = mapper.queryForDouble("totalAmount", Filters.eq("orderId", "ORD123"));
     * total.ifPresent(t -> System.out.println("Total: $" + t));
     * }</pre>
     *
     * @param propName the name of the double property to retrieve
     * @param filter the query filter to match entities against
     * @return an OptionalDouble containing the property value if found, or empty if not found
     * @throws IllegalArgumentException if propName or filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see OptionalDouble
     */
    @Beta
    public OptionalDouble queryForDouble(final String propName, final Bson filter) {
        return collExecutor.queryForDouble(propName, filter);
    }

    /**
     * Queries for a single string value from entities matching the filter.
     *
     * <p>This method retrieves the value of a specific string property from the first entity
     * matching the filter. The result is wrapped in a Nullable which handles the case
     * where no matching entity is found or the property value is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collMapper(User.class);
     * Nullable<String> email = mapper.queryForString("email", Filters.eq("userId", "user456"));
     * email.ifPresent(e -> System.out.println("Email: " + e));
     * }</pre>
     *
     * @param propName the name of the string property to retrieve
     * @param filter the query filter to match entities against
     * @return a Nullable containing the property value if found, or empty if not found
     * @throws IllegalArgumentException if propName or filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see Nullable
     */
    @Beta
    public Nullable<String> queryForString(final String propName, final Bson filter) {
        return collExecutor.queryForString(propName, filter);
    }

    /**
     * Queries for a single date value from entities matching the filter.
     *
     * <p>This method retrieves the value of a specific date property from the first entity
     * matching the filter. The result is wrapped in a Nullable which handles the case
     * where no matching entity is found or the property value is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Event> mapper = mongoDB.collMapper(Event.class);
     * Nullable<Date> startDate = mapper.queryForDate("startDate", Filters.eq("eventId", "EVT789"));
     * startDate.ifPresent(d -> System.out.println("Start Date: " + d));
     * }</pre>
     *
     * @param propName the name of the date property to retrieve
     * @param filter the query filter to match entities against
     * @return a Nullable containing the property value if found, or empty if not found
     * @throws IllegalArgumentException if propName or filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see Nullable
     * @see Date
     */
    @Beta
    public Nullable<Date> queryForDate(final String propName, final Bson filter) {
        return collExecutor.queryForDate(propName, filter);
    }

    /**
     * Queries for a single date value of a specific type from entities matching the filter.
     *
     * <p>This method retrieves the value of a specific date property from the first entity
     * matching the filter and converts it to the specified Date subtype. The result is wrapped
     * in a Nullable which handles the case where no matching entity is found or the property value is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Task> mapper = mongoDB.collMapper(Task.class);
     * Nullable<Timestamp> created = mapper.queryForDate("createdAt", 
     *     Filters.eq("taskId", "TASK123"), Timestamp.class);
     * created.ifPresent(t -> System.out.println("Created: " + t));
     * }</pre>
     *
     * @param <P> the specific Date subtype to return
     * @param propName the name of the date property to retrieve
     * @param filter the query filter to match entities against
     * @param valueType the class of the Date subtype to convert to
     * @return a Nullable containing the property value if found, or empty if not found
     * @throws IllegalArgumentException if propName, filter, or valueType is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see Nullable
     * @see Date
     */
    public <P extends Date> Nullable<P> queryForDate(final String propName, final Bson filter, final Class<P> valueType) {
        return collExecutor.queryForDate(propName, filter, valueType);
    }

    /**
     * Queries for a single value of a specific type from entities matching the filter.
     *
     * <p>This method retrieves the value of a specific property from the first entity
     * matching the filter and converts it to the specified type. The result is wrapped
     * in a Nullable which handles the case where no matching entity is found or the property value is null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Document> mapper = mongoDB.collMapper(Document.class);
     * Nullable<BigDecimal> price = mapper.queryForSingleResult("price", 
     *     Filters.eq("productId", "PROD999"), BigDecimal.class);
     * price.ifPresent(p -> System.out.println("Price: $" + p));
     * }</pre>
     *
     * @param <V> the type to convert the property value to
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match entities against
     * @param valueType the class of the type to convert to
     * @return a Nullable containing the property value if found, or empty if not found
     * @throws IllegalArgumentException if propName, filter, or valueType is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see Nullable
     */
    public <V> Nullable<V> queryForSingleResult(final String propName, final Bson filter, final Class<V> valueType) {
        return collExecutor.queryForSingleResult(propName, filter, valueType);
    }

    /**
     * Executes a query and returns results as a Dataset.
     *
     * <p>This method performs a find operation and returns the results as a Dataset,
     * which provides tabular data representation with rows and columns. This is useful
     * for analytical operations and data transformation tasks.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Sales> mapper = mongoDB.collMapper(Sales.class);
     * Dataset ds = mapper.query(Filters.gte("amount", 1000));
     * ds.forEach(row -> System.out.println(row));
     * }</pre>
     *
     * @param filter the query filter to match entities against
     * @return a Dataset containing the query results
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see Dataset
     */
    public Dataset query(final Bson filter) {
        return collExecutor.query(filter, rowType);
    }

    /**
     * Executes a paginated query and returns results as a Dataset.
     *
     * <p>This method performs a find operation with pagination and returns the results as a Dataset.
     * This is useful for processing large result sets in manageable chunks while maintaining
     * the tabular data representation benefits of Dataset.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Sales> mapper = mongoDB.collMapper(Sales.class);
     * Dataset ds = mapper.query(Filters.gte("amount", 1000), 0, 100);
     * System.out.println("First 100 high-value sales: " + ds.size());
     * }</pre>
     *
     * @param filter the query filter to match entities against
     * @param offset the number of documents to skip (0-based)
     * @param count the maximum number of entities to return
     * @return a Dataset containing the paginated query results
     * @throws IllegalArgumentException if filter is null, or if offset/count is negative
     * @throws com.mongodb.MongoException if the database operation fails
     * @see Dataset
     */
    public Dataset query(final Bson filter, final int offset, final int count) {
        return collExecutor.query(filter, offset, count, rowType);
    }

    /**
     * Executes a query with field projection and returns results as a Dataset.
     *
     * <p>This method performs a find operation with field projection and returns the results
     * as a Dataset. Only the specified fields are included in the Dataset columns,
     * reducing memory usage and improving performance for large datasets.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Customer> mapper = mongoDB.collMapper(Customer.class);
     * Collection<String> fields = Arrays.asList("name", "email", "city");
     * Dataset ds = mapper.query(fields, Filters.eq("status", "active"));
     * }</pre>
     *
     * @param selectPropNames collection of field names to include in the projection (null for all fields)
     * @param filter the query filter to match entities against
     * @return a Dataset containing the query results with projected fields
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see Dataset
     */
    public Dataset query(final Collection<String> selectPropNames, final Bson filter) {
        return collExecutor.query(selectPropNames, filter, rowType);
    }

    /**
     * Executes a paginated query with field projection and returns results as a Dataset.
     * 
     * <p>This method performs a find operation with field projection and pagination, returning
     * a subset of matching documents as a Dataset. Pagination parameters control which portion
     * of the result set is returned, enabling efficient processing of large collections in chunks.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("orderId", "total", "status");
     * Dataset orders = mapper.query(fields, Filters.gte("total", 1000), 20, 10);
     * // Returns 10 orders starting from the 21st match
     * }</pre>
     * 
     * @param selectPropNames collection of field names to include in the projection (null for all fields)
     * @param filter the query filter to match entities against
     * @param offset the number of matching documents to skip (0-based)
     * @param count the maximum number of documents to return
     * @return a Dataset containing the paginated query results with projected fields
     * @throws IllegalArgumentException if filter is null or offset/count are negative
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #query(Collection, Bson)
     */
    public Dataset query(final Collection<String> selectPropNames, final Bson filter, final int offset, final int count) {
        return collExecutor.query(selectPropNames, filter, offset, count, rowType);
    }

    /**
     * Executes a sorted query with field projection and returns results as a Dataset.
     * 
     * <p>This method performs a find operation with field projection and sorting, returning
     * ordered results as a Dataset. The sort parameter determines the order of documents
     * in the result set, which is essential for consistent data processing and reporting.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("username", "score", "level");
     * Bson sort = Sorts.descending("score");
     * Dataset topPlayers = mapper.query(fields, Filters.exists("score"), sort);
     * }</pre>
     * 
     * @param selectPropNames collection of field names to include in the projection (null for all fields)
     * @param filter the query filter to match entities against
     * @param sort the sort specification for result ordering
     * @return a Dataset containing the sorted query results with projected fields
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see com.mongodb.client.model.Sorts
     */
    public Dataset query(final Collection<String> selectPropNames, final Bson filter, final Bson sort) {
        return collExecutor.query(selectPropNames, filter, sort, rowType);
    }

    /**
     * Executes a paginated and sorted query with field projection returning results as a Dataset.
     * 
     * <p>This method combines field projection, sorting, and pagination to retrieve a specific
     * window of ordered documents as a Dataset. This is the most comprehensive query method,
     * ideal for implementing pagination in user interfaces or processing large datasets in sorted chunks.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("productName", "price", "rating");
     * Bson sort = Sorts.orderBy(Sorts.descending("rating"), Sorts.ascending("price"));
     * Dataset products = mapper.query(fields, Filters.gte("rating", 4.0), sort, 0, 20);
     * }</pre>
     * 
     * @param selectPropNames collection of field names to include in the projection (null for all fields)
     * @param filter the query filter to match entities against
     * @param sort the sort specification for result ordering
     * @param offset the number of matching documents to skip (0-based)
     * @param count the maximum number of documents to return
     * @return a Dataset containing the paginated and sorted query results with projected fields
     * @throws IllegalArgumentException if filter is null or offset/count are negative
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #query(Collection, Bson, Bson)
     */
    public Dataset query(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final int offset, final int count) {
        return collExecutor.query(selectPropNames, filter, sort, offset, count, rowType);
    }

    /**
     * Executes a sorted query with BSON projection and returns results as a Dataset.
     * 
     * <p>This method uses BSON-based projection for advanced field selection and transformation,
     * including computed fields, subdocument selection, and field exclusion. BSON projection
     * provides more flexibility than simple field name lists for complex data transformations.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson projection = Projections.fields(
     *     Projections.include("name", "address"),
     *     Projections.excludeId()
     * );
     * Dataset results = mapper.query(projection, Filters.eq("city", "NYC"), Sorts.ascending("name"));
     * }</pre>
     * 
     * @param projection the BSON projection specification for field selection and transformation
     * @param filter the query filter to match entities against
     * @param sort the sort specification for result ordering
     * @return a Dataset containing the sorted query results with BSON-projected fields
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see com.mongodb.client.model.Projections
     */
    public Dataset query(final Bson projection, final Bson filter, final Bson sort) {
        return collExecutor.query(projection, filter, sort, rowType);
    }

    /**
     * Executes a paginated and sorted query with BSON projection returning results as a Dataset.
     * 
     * <p>This method provides maximum query flexibility using BSON-based projection with
     * pagination and sorting. It enables sophisticated data retrieval scenarios with computed
     * fields, complex transformations, and precise data windowing in a single operation.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Bson projection = Projections.fields(
     *     Projections.include("customer", "items"),
     *     Projections.computed("itemCount", new Document("$size", "$items"))
     * );
     * Dataset orders = mapper.query(projection, Filters.gte("date", startDate), 
     *                               Sorts.descending("date"), 0, 50);
     * }</pre>
     * 
     * @param projection the BSON projection specification for field selection and transformation
     * @param filter the query filter to match entities against
     * @param sort the sort specification for result ordering
     * @param offset the number of matching documents to skip (0-based)
     * @param count the maximum number of documents to return
     * @return a Dataset containing the paginated and sorted query results with BSON-projected fields
     * @throws IllegalArgumentException if filter is null or offset/count are negative
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #query(Bson, Bson, Bson)
     */
    public Dataset query(final Bson projection, final Bson filter, final Bson sort, final int offset, final int count) {
        return collExecutor.query(projection, filter, sort, offset, count, rowType);
    }

    /**
     * Creates a stream of entities matching the specified filter criteria.
     *
     * <p>This method performs a find operation and returns a Stream of entities of the mapped type.
     * Streaming is memory-efficient for processing large result sets as entities are loaded and
     * converted on-demand. The stream should be closed after use to free database resources.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collMapper(User.class);
     * 
     * // Stream active users for processing:
     * try (Stream<User> userStream = mapper.stream(Filters.eq("status", "active"))) {
     *     userStream
     *         .filter(user -> user.getAge() >= 18)
     *         .forEach(user -> processUser(user));
     * }
     * 
     * // Stream with JSON filter:
     * try (Stream<User> recentUsers = mapper.stream(
     *     "{createdAt: {$gte: ISODate('2023-01-01')}}")) {
     *     
     *     long count = recentUsers.count();
     *     System.out.println("Recent users: " + count);
     * }
     * }</pre>
     *
     * @param filter the query filter to match entities against
     * @return a Stream of entities matching the filter criteria
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see Stream
     * @see com.mongodb.client.model.Filters
     * @see #list(Bson)
     */
    public Stream<T> stream(final Bson filter) {
        return collExecutor.stream(filter, rowType);
    }

    /**
     * Creates a paginated stream of entities matching the specified filter criteria.
     *
     * <p>This method performs a find operation with pagination and returns a Stream of entities.
     * Streaming with pagination is useful for processing specific ranges of large result sets
     * memory-efficiently while maintaining precise control over the data window.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Order> mapper = mongoDB.collMapper(Order.class);
     * Bson filter = Filters.gte("orderDate", LocalDate.now().minusDays(7));
     * 
     * // Process second batch of recent orders:
     * try (Stream<Order> orderStream = mapper.stream(filter, 100, 100)) {
     *     double avgValue = orderStream
     *         .mapToDouble(Order::getTotalAmount)
     *         .average()
     *         .orElse(0.0);
     *     System.out.println("Average order value: $" + avgValue);
     * }
     * }</pre>
     *
     * @param filter the query filter to match entities against
     * @param offset the number of entities to skip (0-based)
     * @param count the maximum number of entities to include in the stream
     * @return a Stream of entities matching the filter within the specified range
     * @throws IllegalArgumentException if filter is null, or if offset/count is negative
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #stream(Bson)
     * @see Stream
     */
    public Stream<T> stream(final Bson filter, final int offset, final int count) {
        return collExecutor.stream(filter, offset, count, rowType);
    }

    /**
     * Creates a stream of entities matching the filter with field projection.
     *
     * <p>This method combines filtering and field projection to stream entities with only
     * the specified fields populated. This reduces memory usage and network bandwidth,
     * making it ideal for processing large datasets where only specific fields are needed.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collMapper(User.class);
     * Collection<String> fields = Arrays.asList("email", "preferences.newsletter");
     * Bson filter = Filters.eq("preferences.newsletter", true);
     * 
     * // Stream newsletter subscribers with minimal data:
     * try (Stream<User> subscriberStream = mapper.stream(fields, filter)) {
     *     subscriberStream
     *         .map(User::getEmail)
     *         .forEach(email -> sendNewsletter(email));
     * }
     * }</pre>
     *
     * @param selectPropNames collection of field names to include in the projection (null for all fields)
     * @param filter the query filter to match entities against
     * @return a Stream of entities with projected fields matching the filter
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #stream(Bson)
     * @see #list(Collection, Bson)
     */
    public Stream<T> stream(final Collection<String> selectPropNames, final Bson filter) {
        return collExecutor.stream(selectPropNames, filter, rowType);
    }

    /**
     * Creates a paginated stream of entities with field projection.
     *
     * <p>This method combines field projection and pagination to stream a specific range
     * of entities with only selected fields populated. This provides maximum efficiency
     * for processing bounded subsets of large collections with minimal data transfer.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Product> mapper = mongoDB.collMapper(Product.class);
     * Collection<String> fields = Arrays.asList("name", "price", "category");
     * Bson filter = Filters.lt("price", 100.0);
     * 
     * // Process affordable products in batches:
     * try (Stream<Product> productStream = mapper.stream(fields, filter, 0, 500)) {
     *     Map<String, Long> categoryCount = productStream
     *         .collect(Collectors.groupingBy(
     *             Product::getCategory,
     *             Collectors.counting()
     *         ));
     *     categoryCount.forEach((cat, count) -> 
     *         System.out.println(cat + ": " + count + " affordable items"));
     * }
     * }</pre>
     *
     * @param selectPropNames collection of field names to include in the projection (null for all fields)
     * @param filter the query filter to match entities against
     * @param offset the number of entities to skip (0-based)
     * @param count the maximum number of entities to include in the stream
     * @return a Stream of entities with projected fields within the specified range
     * @throws IllegalArgumentException if filter is null, or if offset/count is negative
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #stream(Collection, Bson)
     * @see #stream(Bson, int, int)
     */
    public Stream<T> stream(final Collection<String> selectPropNames, final Bson filter, final int offset, final int count) {
        return collExecutor.stream(selectPropNames, filter, offset, count, rowType);
    }

    /**
     * Creates a stream of entities with field projection and sorting.
     *
     * <p>This method combines field projection and sorting to stream entities with only
     * selected fields in a specific order. Sorting ensures predictable ordering for
     * consistent processing and is essential when stream processing depends on data sequence.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Article> mapper = mongoDB.collMapper(Article.class);
     * Collection<String> fields = Arrays.asList("title", "publishedAt", "viewCount");
     * Bson filter = Filters.eq("status", "published");
     * Bson sort = Sorts.descending("publishedAt");   // Latest first
     * 
     * // Stream articles in chronological order:
     * try (Stream<Article> articleStream = mapper.stream(fields, filter, sort)) {
     *     articleStream
     *         .limit(50) // Top 50 recent articles
     *         .forEach(article -> indexForSearch(article));
     * }
     * }</pre>
     *
     * @param selectPropNames collection of field names to include in the projection (null for all fields)
     * @param filter the query filter to match entities against
     * @param sort the sort specification for result ordering
     * @return a Stream of entities with projected fields in sorted order
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #stream(Collection, Bson)
     * @see #stream(Collection, Bson, Bson, int, int)
     * @see com.mongodb.client.model.Sorts
     */
    public Stream<T> stream(final Collection<String> selectPropNames, final Bson filter, final Bson sort) {
        return collExecutor.stream(selectPropNames, filter, sort, rowType);
    }

    /**
     * Creates a paginated stream of entities with field projection and sorting.
     *
     * <p>This method provides the most comprehensive streaming capability, combining field projection,
     * sorting, and pagination. It's ideal for processing specific windows of large, ordered datasets
     * with minimal memory footprint and optimal network utilization.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Transaction> mapper = mongoDB.collMapper(Transaction.class);
     * Collection<String> fields = Arrays.asList("amount", "timestamp", "accountId");
     * Bson filter = Filters.gte("amount", 1000.0);   // High-value transactions
     * Bson sort = Sorts.descending("timestamp");     // Most recent first
     * 
     * // Process recent high-value transactions in batches:
     * int batchSize = 100;
     * for (int page = 0; page < 10; page++) {
     *     try (Stream<Transaction> txnStream = mapper.stream(
     *             fields, filter, sort, page * batchSize, batchSize)) {
     *         
     *         txnStream.forEach(txn -> auditTransaction(txn));
     *     }
     * }
     * }</pre>
     *
     * @param selectPropNames collection of field names to include in the projection (null for all fields)
     * @param filter the query filter to match entities against
     * @param sort the sort specification for result ordering
     * @param offset the number of entities to skip (0-based)
     * @param count the maximum number of entities to include in the stream
     * @return a Stream of entities with projected fields in sorted order within the specified range
     * @throws IllegalArgumentException if filter is null, or if offset/count is negative
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #stream(Collection, Bson, Bson)
     * @see #stream(Collection, Bson, int, int)
     */
    public Stream<T> stream(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final int offset, final int count) {
        return collExecutor.stream(selectPropNames, filter, sort, offset, count, rowType);
    }

    /**
     * Creates a stream of entities with BSON projection and sorting.
     *
     * <p>This method uses BSON-based projection for advanced field selection and transformation
     * combined with sorting. BSON projection allows for computed fields, subdocument selection,
     * and complex transformations during the streaming process.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Order> mapper = mongoDB.collMapper(Order.class);
     * 
     * // Complex projection with computed total:
     * Bson projection = Projections.fields(
     *     Projections.include("customerId", "items"),
     *     Projections.computed("total", new Document("$sum", "$items.price"))
     * );
     * Bson filter = Filters.gte("orderDate", LocalDate.now().minusMonths(1));
     * Bson sort = Sorts.descending("total");   // Highest value first
     * 
     * // Stream high-value recent orders:
     * try (Stream<Order> orderStream = mapper.stream(projection, filter, sort)) {
     *     orderStream
     *         .filter(order -> order.getTotal() > 500.0)
     *         .forEach(order -> processHighValueOrder(order));
     * }
     * }</pre>
     *
     * @param projection the BSON projection specification for field selection and transformation
     * @param filter the query filter to match entities against
     * @param sort the sort specification for result ordering
     * @return a Stream of entities with BSON-projected fields in sorted order
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #stream(Collection, Bson, Bson)
     * @see com.mongodb.client.model.Projections
     */
    public Stream<T> stream(final Bson projection, final Bson filter, final Bson sort) {
        return collExecutor.stream(projection, filter, sort, rowType);
    }

    /**
     * Creates a paginated stream of entities with BSON projection and sorting.
     *
     * <p>This method provides maximum streaming flexibility using BSON-based projection with
     * pagination and sorting. It enables the most sophisticated data processing scenarios
     * with computed fields, complex transformations, and precise data windowing.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Analytics> mapper = mongoDB.collMapper(Analytics.class);
     * 
     * // Advanced analytics with computed metrics:
     * Bson projection = Projections.fields(
     *     Projections.include("userId", "events"),
     *     Projections.computed("conversionRate", 
     *         new Document("$divide", Arrays.asList(
     *             new Document("$size", "$conversions"),
     *             new Document("$size", "$events")
     *         ))),
     *     Projections.computed("totalValue", 
     *         new Document("$sum", "$conversions.value"))
     * );
     * Bson filter = Filters.gte("lastActivity", LocalDate.now().minusWeeks(2));
     * Bson sort = Sorts.descending("conversionRate");
     * 
     * // Process top performers in batches:
     * try (Stream<Analytics> analyticsStream = mapper.stream(
     *         projection, filter, sort, 0, 100)) {
     *     
     *     analyticsStream.forEach(analytics -> generateReport(analytics));
     * }
     * }</pre>
     *
     * @param projection the BSON projection specification for field selection and transformation
     * @param filter the query filter to match entities against
     * @param sort the sort specification for result ordering
     * @param offset the number of entities to skip (0-based)
     * @param count the maximum number of entities to include in the stream
     * @return a Stream of entities with BSON-projected fields in sorted order within the specified range
     * @throws IllegalArgumentException if filter is null, or if offset/count is negative
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #stream(Bson, Bson, Bson)
     * @see #stream(Collection, Bson, Bson, int, int)
     */
    public Stream<T> stream(final Bson projection, final Bson filter, final Bson sort, final int offset, final int count) {
        return collExecutor.stream(projection, filter, sort, offset, count, rowType);
    }

    /**
     * Inserts a single entity into the collection (blocking operation).
     *
     * <p>This method inserts a single entity of the mapped type into the collection. The entity
     * is automatically converted to a BSON document using the configured codec registry, handling
     * ID field mapping and type conversions transparently.</p>
     *
     * <p><b>Note:</b> This method performs a blocking operation. For non-blocking operations, use
     * the underlying executor's async methods via {@link #collExecutor()}.{@code async()}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collMapper(User.class);
     *
     * User newUser = new User("John Doe", "john@example.com", 30);
     * mapper.insertOne(newUser);
     *
     * // The entity's ID field will be automatically populated if it was null
     * System.out.println("Inserted user with ID: " + newUser.getId());
     * }</pre>
     *
     * @param obj the entity to insert
     * @throws IllegalArgumentException if obj is null
     * @throws com.mongodb.MongoWriteException if the insert operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #insertOne(Object, InsertOneOptions)
     * @see #insertMany(Collection)
     * @see #collExecutor()
     */
    public void insertOne(final T obj) {
        collExecutor.insertOne(obj);
    }

    /**
     * Inserts a single entity into the collection with additional options.
     *
     * <p>This method inserts a single entity with fine-grained control over the insert operation
     * through InsertOneOptions. Options allow specification of write concern, bypass document
     * validation, and other advanced insertion behaviors.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collMapper(User.class);
     * User newUser = new User("Jane Smith", "jane@example.com", 25);
     *
     * // Insert with custom write concern:
     * InsertOneOptions options = new InsertOneOptions()
     *     .bypassDocumentValidation(false);
     *
     * mapper.insertOne(newUser, options);
     * System.out.println("User inserted with options: " + newUser.getId());
     * }</pre>
     *
     * @param obj the entity to insert
     * @param options additional options for the insert operation (null uses defaults)
     * @throws IllegalArgumentException if obj is null
     * @throws com.mongodb.MongoWriteException if the insert operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #insertOne(Object)
     * @see InsertOneOptions
     */
    public void insertOne(final T obj, final InsertOneOptions options) {
        collExecutor.insertOne(obj, options);
    }

    /**
     * Inserts multiple entities into the collection in a single batch operation.
     *
     * <p>This method inserts multiple entities of the mapped type into the collection efficiently
     * in a single batch operation. Each entity is automatically converted to a BSON document.
     * Batch operations are more efficient than individual inserts for multiple entities and
     * provide better performance for bulk data loading.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collMapper(User.class);
     *
     * List<User> users = Arrays.asList(
     *     new User("John", "john@example.com", 30),
     *     new User("Jane", "jane@example.com", 25),
     *     new User("Bob", "bob@example.com", 35)
     * );
     *
     * mapper.insertMany(users);
     *
     * // All users' ID fields will be automatically populated
     * users.forEach(user -> System.out.println("Inserted: " + user.getId()));
     * }</pre>
     *
     * @param objList collection of entities to insert
     * @throws IllegalArgumentException if objList is null or empty
     * @throws com.mongodb.MongoBulkWriteException if one or more insert operations fail
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #insertMany(Collection, InsertManyOptions)
     * @see #insertOne(Object)
     */
    public void insertMany(final Collection<? extends T> objList) {
        collExecutor.insertMany(objList);
    }

    /**
     * Inserts multiple entities into the collection with additional options.
     * 
     * <p>This method inserts multiple entities in a batch operation with fine-grained control
     * through InsertManyOptions. Options allow configuration of ordered/unordered inserts,
     * bypass document validation, and custom write concerns for advanced bulk insertion scenarios.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Product> products = loadProductsFromFile();
     * InsertManyOptions options = new InsertManyOptions()
     *     .ordered(false)  // Continue on error
     *     .bypassDocumentValidation(true);
     * mapper.insertMany(products, options);
     * }</pre>
     * 
     * @param objList collection of entities to insert
     * @param options additional options for the insert operation (null uses defaults)
     * @throws IllegalArgumentException if objList is null or empty
     * @throws com.mongodb.MongoBulkWriteException if one or more insert operations fail
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #insertMany(Collection)
     * @see InsertManyOptions
     */
    public void insertMany(final Collection<? extends T> objList, final InsertManyOptions options) {
        collExecutor.insertMany(objList, options);
    }

    /**
     * Updates a single entity identified by ObjectId string with the specified update operations.
     *
     * <p>This method updates a single entity matching the provided ObjectId string with the specified
     * update entity. The update entity is automatically converted to appropriate update operations.
     * If the update entity implements the DirtyMarker interface, only the dirty properties will be updated.
     * Otherwise, all non-null properties are included in the update operation.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collMapper(User.class);
     * String userId = "507f1f77bcf86cd799439011";
     *
     * // Update with partial entity:
     * User updateData = new User();
     * updateData.setStatus("inactive");
     * updateData.setLastSeen(new Date());
     *
     * UpdateResult result = mapper.updateOne(userId, updateData);
     * System.out.println("Modified " + result.getModifiedCount() + " entity");
     * }</pre>
     *
     * @param objectId the string representation of the ObjectId identifying the entity to update
     * @param update the entity containing update data
     * @return UpdateResult containing information about the update operation
     * @throws IllegalArgumentException if objectId or update is null, or objectId format is invalid
     * @throws com.mongodb.MongoWriteException if the update operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see UpdateResult
     * @see #updateOne(ObjectId, Object)
     */
    public UpdateResult updateOne(final String objectId, final T update) {
        return collExecutor.updateOne(objectId, update);
    }

    /**
     * Updates a single entity identified by ObjectId with the specified update operations.
     * 
     * <p>This method updates a single entity matching the provided ObjectId. The update entity
     * contains the fields to modify, with automatic handling of dirty tracking if supported.
     * This is the most direct way to update a known entity by its database identifier.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
     * User updates = new User();
     * updates.setLastLogin(new Date());
     * UpdateResult result = mapper.updateOne(id, updates);
     * }</pre>
     * 
     * @param objectId the ObjectId identifying the entity to update
     * @param update the entity containing update data
     * @return UpdateResult containing information about the update operation
     * @throws IllegalArgumentException if objectId or update is null
     * @throws com.mongodb.MongoWriteException if the update operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #updateOne(String, Object)
     * @see UpdateResult
     */
    public UpdateResult updateOne(final ObjectId objectId, final T update) {
        return collExecutor.updateOne(objectId, update);
    }

    /**
     * Updates a single entity matching the filter with the specified update operations.
     * 
     * <p>This method updates the first entity matching the provided filter. The update entity
     * specifies which fields to modify. If multiple documents match the filter, only the first
     * one found will be updated. Use updateMany for updating multiple documents.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User updates = new User();
     * updates.setVerified(true);
     * UpdateResult result = mapper.updateOne(
     *     Filters.eq("email", "user@example.com"), updates);
     * }</pre>
     * 
     * @param filter the query filter to match the entity to update
     * @param update the entity containing update data
     * @return UpdateResult containing information about the update operation
     * @throws IllegalArgumentException if filter or update is null
     * @throws com.mongodb.MongoWriteException if the update operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #updateMany(Bson, Object)
     * @see UpdateResult
     */
    public UpdateResult updateOne(final Bson filter, final T update) {
        return collExecutor.updateOne(filter, update);
    }

    /**
     * Updates a single entity matching the filter with additional update options.
     * 
     * <p>This method provides fine-grained control over single entity updates through UpdateOptions.
     * Options enable upsert operations (insert if not found), array filters for nested updates,
     * collation for language-specific matching, and custom write concerns.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * UpdateOptions options = new UpdateOptions()
     *     .upsert(true)  // Create if not exists
     *     .bypassDocumentValidation(false);
     * User updates = new User();
     * updates.setActive(true);
     * UpdateResult result = mapper.updateOne(
     *     Filters.eq("username", "newuser"), updates, options);
     * }</pre>
     * 
     * @param filter the query filter to match the entity to update
     * @param update the entity containing update data
     * @param options additional options for the update operation (null uses defaults)
     * @return UpdateResult containing information about the update operation
     * @throws IllegalArgumentException if filter or update is null
     * @throws com.mongodb.MongoWriteException if the update operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #updateOne(Bson, Object)
     * @see UpdateOptions
     */
    public UpdateResult updateOne(final Bson filter, final T update, final UpdateOptions options) {
        return collExecutor.updateOne(filter, update, options);
    }

    /**
     * Updates a single entity using multiple update entities as a pipeline.
     * 
     * <p>This method applies multiple update entities in sequence as an aggregation pipeline
     * update. This enables complex, multi-stage updates that can reference existing field values,
     * perform calculations, and conditionally modify documents.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<User> updatePipeline = Arrays.asList(
     *     new User().setProcessed(true),
     *     new User().setUpdatedAt(new Date())
     * );
     * UpdateResult result = mapper.updateOne(
     *     Filters.eq("status", "pending"), updatePipeline);
     * }</pre>
     * 
     * @param filter the query filter to match the entity to update
     * @param objList collection of entities forming the update pipeline
     * @return UpdateResult containing information about the update operation
     * @throws IllegalArgumentException if filter or objList is null or empty
     * @throws com.mongodb.MongoWriteException if the update operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #updateOne(Bson, Object)
     */
    public UpdateResult updateOne(final Bson filter, final Collection<? extends T> objList) {
        return collExecutor.updateOne(filter, objList);
    }

    /**
     * Updates a single entity using multiple update entities with additional options.
     * 
     * <p>This method combines pipeline-based updates with UpdateOptions for maximum flexibility.
     * Pipeline updates allow complex transformations while options provide control over upsert
     * behavior, validation, and other advanced update features.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * UpdateOptions options = new UpdateOptions().upsert(true);
     * List<Product> pipeline = createComplexUpdatePipeline();
     * UpdateResult result = mapper.updateOne(
     *     Filters.eq("sku", "PROD-123"), pipeline, options);
     * }</pre>
     * 
     * @param filter the query filter to match the entity to update
     * @param objList collection of entities forming the update pipeline
     * @param options additional options for the update operation (null uses defaults)
     * @return UpdateResult containing information about the update operation
     * @throws IllegalArgumentException if filter or objList is null or empty
     * @throws com.mongodb.MongoWriteException if the update operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #updateOne(Bson, Collection)
     * @see UpdateOptions
     */
    public UpdateResult updateOne(final Bson filter, final Collection<? extends T> objList, final UpdateOptions options) {
        return collExecutor.updateOne(filter, objList, options);
    }

    /**
     * Updates all entities matching the filter with the specified update operations (blocking operation).
     *
     * <p>This method updates all entities that match the provided filter criteria. Unlike updateOne,
     * this operation modifies every matching document in the collection. Use with caution on
     * filters that may match large numbers of documents.</p>
     *
     * <p><b>Note:</b> This method performs a blocking operation. For non-blocking operations, use
     * {@link #collExecutor()}.{@code async()}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User updates = new User();
     * updates.setNewsletterOptIn(false);
     * UpdateResult result = mapper.updateMany(
     *     Filters.eq("country", "EU"), updates);
     * System.out.println("Updated " + result.getModifiedCount() + " users");
     * }</pre>
     *
     * @param filter the query filter to match entities to update
     * @param update the entity containing update data
     * @return UpdateResult containing information about the update operation
     * @throws IllegalArgumentException if filter or update is null
     * @throws com.mongodb.MongoWriteException if the update operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #updateOne(Bson, Object)
     * @see UpdateResult
     * @see #collExecutor()
     */
    public UpdateResult updateMany(final Bson filter, final T update) {
        return collExecutor.updateMany(filter, update);
    }

    /**
     * Updates all entities matching the filter with additional update options.
     * 
     * <p>This method provides fine-grained control over bulk updates through UpdateOptions.
     * Options enable upsert operations for all matches, array filters for nested updates,
     * and custom write concerns for consistency requirements.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * UpdateOptions options = new UpdateOptions()
     *     .bypassDocumentValidation(true);
     * Product updates = new Product();
     * updates.setDiscontinued(true);
     * UpdateResult result = mapper.updateMany(
     *     Filters.lt("stock", 10), updates, options);
     * }</pre>
     * 
     * @param filter the query filter to match entities to update
     * @param update the entity containing update data
     * @param options additional options for the update operation (null uses defaults)
     * @return UpdateResult containing information about the update operation
     * @throws IllegalArgumentException if filter or update is null
     * @throws com.mongodb.MongoWriteException if the update operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #updateMany(Bson, Object)
     * @see UpdateOptions
     */
    public UpdateResult updateMany(final Bson filter, final T update, final UpdateOptions options) {
        return collExecutor.updateMany(filter, update, options);
    }

    /**
     * Updates all entities matching the filter using multiple update entities as a pipeline.
     * 
     * <p>This method applies a pipeline of update operations to all matching documents.
     * Pipeline updates enable complex transformations that can reference existing field values,
     * perform calculations, and apply conditional logic across multiple documents.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Order> pipeline = Arrays.asList(
     *     new Order().setProcessed(true),
     *     new Order().setProcessedDate(new Date())
     * );
     * UpdateResult result = mapper.updateMany(
     *     Filters.eq("status", "pending"), pipeline);
     * }</pre>
     * 
     * @param filter the query filter to match entities to update
     * @param objList collection of entities forming the update pipeline
     * @return UpdateResult containing information about the update operation
     * @throws IllegalArgumentException if filter or objList is null or empty
     * @throws com.mongodb.MongoWriteException if the update operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #updateMany(Bson, Object)
     */
    public UpdateResult updateMany(final Bson filter, final Collection<? extends T> objList) {
        return collExecutor.updateMany(filter, objList);
    }

    /**
     * Updates all entities matching the filter using a pipeline with additional options.
     * 
     * <p>This method combines pipeline-based bulk updates with UpdateOptions for maximum control.
     * It enables complex multi-stage transformations across multiple documents with fine-grained
     * control over the update behavior.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * UpdateOptions options = new UpdateOptions()
     *     .collation(Collation.builder().locale("en").build());
     * List<Customer> pipeline = createBulkUpdatePipeline();
     * UpdateResult result = mapper.updateMany(
     *     Filters.regex("name", "^Corp"), pipeline, options);
     * }</pre>
     * 
     * @param filter the query filter to match entities to update
     * @param objList collection of entities forming the update pipeline
     * @param options additional options for the update operation (null uses defaults)
     * @return UpdateResult containing information about the update operation
     * @throws IllegalArgumentException if filter or objList is null or empty
     * @throws com.mongodb.MongoWriteException if the update operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #updateMany(Bson, Collection)
     * @see UpdateOptions
     */
    public UpdateResult updateMany(final Bson filter, final Collection<? extends T> objList, final UpdateOptions options) {
        return collExecutor.updateMany(filter, objList, options);
    }

    /**
     * Replaces a single entity identified by ObjectId string with a new entity.
     * 
     * <p>This method completely replaces an existing entity with a new one, removing all
     * existing fields and replacing them with the fields from the replacement entity.
     * The _id field is preserved. This differs from update operations which modify specific fields.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String userId = "507f1f77bcf86cd799439011";
     * User newUserData = new User("John Updated", "john.new@example.com", 31);
     * UpdateResult result = mapper.replaceOne(userId, newUserData);
     * }</pre>
     * 
     * @param objectId the string representation of the ObjectId identifying the entity to replace
     * @param replacement the new entity to replace the existing one
     * @return UpdateResult containing information about the replace operation
     * @throws IllegalArgumentException if objectId or replacement is null, or objectId format is invalid
     * @throws com.mongodb.MongoWriteException if the replace operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #replaceOne(ObjectId, Object)
     * @see UpdateResult
     */
    public UpdateResult replaceOne(final String objectId, final T replacement) {
        return collExecutor.replaceOne(objectId, replacement);
    }

    /**
     * Replaces a single entity identified by ObjectId with a new entity.
     * 
     * <p>This method completely replaces an existing entity identified by its ObjectId.
     * All existing fields except _id are removed and replaced with fields from the
     * replacement entity. Use update operations to modify specific fields instead.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
     * Product newProduct = createUpdatedProduct();
     * UpdateResult result = mapper.replaceOne(id, newProduct);
     * }</pre>
     * 
     * @param objectId the ObjectId identifying the entity to replace
     * @param replacement the new entity to replace the existing one
     * @return UpdateResult containing information about the replace operation
     * @throws IllegalArgumentException if objectId or replacement is null
     * @throws com.mongodb.MongoWriteException if the replace operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #replaceOne(String, Object)
     * @see UpdateResult
     */
    public UpdateResult replaceOne(final ObjectId objectId, final T replacement) {
        return collExecutor.replaceOne(objectId, replacement);
    }

    /**
     * Replaces a single entity matching the filter with a new entity.
     * 
     * <p>This method finds the first entity matching the filter and completely replaces it
     * with the replacement entity. All existing fields except _id are removed. If multiple
     * documents match, only the first one found is replaced.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Customer newCustomer = createCustomerFromForm();
     * UpdateResult result = mapper.replaceOne(
     *     Filters.eq("customerId", "CUST-123"), newCustomer);
     * }</pre>
     * 
     * @param filter the query filter to match the entity to replace
     * @param replacement the new entity to replace the existing one
     * @return UpdateResult containing information about the replace operation
     * @throws IllegalArgumentException if filter or replacement is null
     * @throws com.mongodb.MongoWriteException if the replace operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #replaceOne(Bson, Object, ReplaceOptions)
     * @see UpdateResult
     */
    public UpdateResult replaceOne(final Bson filter, final T replacement) {
        return collExecutor.replaceOne(filter, replacement);
    }

    /**
     * Replaces a single entity matching the filter with additional replace options.
     * 
     * <p>This method provides fine-grained control over replacement operations through ReplaceOptions.
     * Options enable upsert behavior (insert if not found), bypass validation, and specify
     * collation for language-specific matching.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ReplaceOptions options = new ReplaceOptions()
     *     .upsert(true)  // Insert if not found
     *     .bypassDocumentValidation(false);
     * Article article = new Article(title, content, author);
     * UpdateResult result = mapper.replaceOne(
     *     Filters.eq("slug", articleSlug), article, options);
     * }</pre>
     * 
     * @param filter the query filter to match the entity to replace
     * @param replacement the new entity to replace the existing one
     * @param options additional options for the replace operation (null uses defaults)
     * @return UpdateResult containing information about the replace operation
     * @throws IllegalArgumentException if filter or replacement is null
     * @throws com.mongodb.MongoWriteException if the replace operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #replaceOne(Bson, Object)
     * @see ReplaceOptions
     */
    public UpdateResult replaceOne(final Bson filter, final T replacement, final ReplaceOptions options) {
        return collExecutor.replaceOne(filter, replacement, options);
    }

    /**
     * Deletes a single entity identified by ObjectId string.
     * 
     * <p>This method deletes a single entity from the collection using its ObjectId string.
     * This is the most direct way to delete a known entity. The operation is atomic and
     * returns immediately after the deletion.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String userId = "507f1f77bcf86cd799439011";
     * DeleteResult result = mapper.deleteOne(userId);
     * System.out.println("Deleted " + result.getDeletedCount() + " entity");
     * }</pre>
     * 
     * @param objectId the string representation of the ObjectId identifying the entity to delete
     * @return DeleteResult containing information about the delete operation
     * @throws IllegalArgumentException if objectId is null or has invalid format
     * @throws com.mongodb.MongoWriteException if the delete operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #deleteOne(ObjectId)
     * @see DeleteResult
     */
    public DeleteResult deleteOne(final String objectId) {
        return collExecutor.deleteOne(objectId);
    }

    /**
     * Deletes a single entity identified by ObjectId.
     * 
     * <p>This method deletes a single entity from the collection using its ObjectId.
     * This provides type-safe deletion when working directly with ObjectId instances
     * rather than string representations.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
     * DeleteResult result = mapper.deleteOne(id);
     * if (result.getDeletedCount() > 0) {
     *     System.out.println("Entity deleted successfully");
     * }
     * }</pre>
     * 
     * @param objectId the ObjectId identifying the entity to delete
     * @return DeleteResult containing information about the delete operation
     * @throws IllegalArgumentException if objectId is null
     * @throws com.mongodb.MongoWriteException if the delete operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #deleteOne(String)
     * @see DeleteResult
     */
    public DeleteResult deleteOne(final ObjectId objectId) {
        return collExecutor.deleteOne(objectId);
    }

    /**
     * Deletes a single entity matching the specified filter.
     * 
     * <p>This method deletes the first entity that matches the provided filter criteria.
     * If multiple documents match the filter, only the first one found is deleted.
     * Use deleteMany to delete all matching documents.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DeleteResult result = mapper.deleteOne(
     *     Filters.and(
     *         Filters.eq("status", "inactive"),
     *         Filters.lt("lastLogin", thirtyDaysAgo)
     *     )
     * );
     * }</pre>
     * 
     * @param filter the query filter to match the entity to delete
     * @return DeleteResult containing information about the delete operation
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoWriteException if the delete operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #deleteMany(Bson)
     * @see DeleteResult
     */
    public DeleteResult deleteOne(final Bson filter) {
        return collExecutor.deleteOne(filter);
    }

    /**
     * Deletes a single entity matching the filter with additional delete options.
     * 
     * <p>This method provides fine-grained control over single entity deletion through DeleteOptions.
     * Options allow specification of collation for language-specific matching and hints for
     * index usage optimization.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DeleteOptions options = new DeleteOptions()
     *     .collation(Collation.builder().locale("en").build());
     * DeleteResult result = mapper.deleteOne(
     *     Filters.eq("email", "user@example.com"), options);
     * }</pre>
     * 
     * @param filter the query filter to match the entity to delete
     * @param options additional options for the delete operation (null uses defaults)
     * @return DeleteResult containing information about the delete operation
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoWriteException if the delete operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #deleteOne(Bson)
     * @see DeleteOptions
     */
    public DeleteResult deleteOne(final Bson filter, final DeleteOptions options) {
        return collExecutor.deleteOne(filter, options);
    }

    /**
     * Deletes all entities matching the specified filter (blocking operation).
     *
     * <p>This method deletes all entities that match the provided filter criteria.
     * Use with caution as this operation can delete large numbers of documents.
     * Consider using deleteOne for single deletions or adding specific filters to limit scope.</p>
     *
     * <p><b>Note:</b> This method performs a blocking operation. For non-blocking operations, use
     * {@link #collExecutor()}.{@code async()}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DeleteResult result = mapper.deleteMany(
     *     Filters.lt("expiryDate", new Date())
     * );
     * System.out.println("Deleted " + result.getDeletedCount() + " expired entities");
     * }</pre>
     *
     * @param filter the query filter to match entities to delete
     * @return DeleteResult containing information about the delete operation
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoWriteException if the delete operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #deleteOne(Bson)
     * @see DeleteResult
     * @see #collExecutor()
     */
    public DeleteResult deleteMany(final Bson filter) {
        return collExecutor.deleteMany(filter);
    }

    /**
     * Deletes all entities matching the filter with additional delete options.
     * 
     * <p>This method provides fine-grained control over bulk deletion through DeleteOptions.
     * Options enable collation for language-specific matching and index hints for
     * performance optimization when deleting large numbers of documents.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DeleteOptions options = new DeleteOptions()
     *     .hint(Indexes.ascending("status", "createdDate"));
     * DeleteResult result = mapper.deleteMany(
     *     Filters.and(
     *         Filters.eq("status", "archived"),
     *         Filters.lt("createdDate", oneYearAgo)
     *     ), options);
     * }</pre>
     * 
     * @param filter the query filter to match entities to delete
     * @param options additional options for the delete operation (null uses defaults)
     * @return DeleteResult containing information about the delete operation
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoWriteException if the delete operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #deleteMany(Bson)
     * @see DeleteOptions
     */
    public DeleteResult deleteMany(final Bson filter, final DeleteOptions options) {
        return collExecutor.deleteMany(filter, options);
    }

    /**
     * Performs a bulk insert of entities with optimized performance.
     * 
     * <p>This method inserts multiple entities using MongoDB's bulk write operations for
     * optimal performance. It's more efficient than insertMany for very large datasets
     * and provides better throughput for high-volume data loading scenarios.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Product> products = loadLargeProductCatalog();
     * int insertedCount = mapper.bulkInsert(products);
     * System.out.println("Bulk inserted " + insertedCount + " products");
     * }</pre>
     * 
     * @param entities collection of entities to insert in bulk
     * @return the number of entities successfully inserted
     * @throws IllegalArgumentException if entities is null or empty
     * @throws com.mongodb.MongoBulkWriteException if one or more operations fail
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #bulkInsert(Collection, BulkWriteOptions)
     * @see #insertMany(Collection)
     */
    public int bulkInsert(final Collection<? extends T> entities) {
        return collExecutor.bulkInsert(entities);
    }

    /**
     * Performs a bulk insert of entities with additional bulk write options.
     * 
     * <p>This method provides fine-grained control over bulk insert operations through
     * BulkWriteOptions. Options enable ordered/unordered execution, bypass validation,
     * and custom write concerns for maximum performance and flexibility.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * BulkWriteOptions options = new BulkWriteOptions()
     *     .ordered(false)  // Continue on error for maximum throughput
     *     .bypassDocumentValidation(true);
     * List<LogEntry> logs = collectLogEntries();
     * int count = mapper.bulkInsert(logs, options);
     * }</pre>
     * 
     * @param entities collection of entities to insert in bulk
     * @param options additional options for the bulk write operation (null uses defaults)
     * @return the number of entities successfully inserted
     * @throws IllegalArgumentException if entities is null or empty
     * @throws com.mongodb.MongoBulkWriteException if one or more operations fail
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #bulkInsert(Collection)
     * @see BulkWriteOptions
     */
    public int bulkInsert(final Collection<? extends T> entities, final BulkWriteOptions options) {
        return collExecutor.bulkInsert(entities, options);
    }

    /**
     * Executes a bulk write operation with mixed write models.
     * 
     * <p>This method performs multiple write operations (insert, update, replace, delete)
     * in a single bulk operation. It provides maximum flexibility for complex batch
     * operations that involve different types of modifications.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<WriteModel<Document>> operations = Arrays.asList(
     *     new InsertOneModel<>(doc1),
     *     new UpdateOneModel<>(filter1, update1),
     *     new DeleteOneModel<>(filter2)
     * );
     * BulkWriteResult result = mapper.bulkWrite(operations);
     * }</pre>
     * 
     * @param requests list of write models defining the operations to perform
     * @return BulkWriteResult containing detailed information about the bulk operation
     * @throws IllegalArgumentException if requests is null or empty
     * @throws com.mongodb.MongoBulkWriteException if one or more operations fail
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #bulkWrite(List, BulkWriteOptions)
     * @see BulkWriteResult
     */
    public BulkWriteResult bulkWrite(final List<? extends WriteModel<? extends Document>> requests) {
        return collExecutor.bulkWrite(requests);
    }

    /**
     * Executes a bulk write operation with mixed write models and additional options.
     * 
     * <p>This method combines the flexibility of mixed bulk operations with fine-grained
     * control through BulkWriteOptions. It enables complex batch processing with custom
     * ordering, validation bypass, and write concern configuration.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * BulkWriteOptions options = new BulkWriteOptions()
     *     .ordered(true)  // Execute in order, stop on error
     *     .bypassDocumentValidation(false);
     * List<WriteModel<Document>> operations = createComplexBatch();
     * BulkWriteResult result = mapper.bulkWrite(operations, options);
     * }</pre>
     * 
     * @param requests list of write models defining the operations to perform
     * @param options additional options for the bulk write operation (null uses defaults)
     * @return BulkWriteResult containing detailed information about the bulk operation
     * @throws IllegalArgumentException if requests is null or empty
     * @throws com.mongodb.MongoBulkWriteException if one or more operations fail
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #bulkWrite(List)
     * @see BulkWriteOptions
     */
    public BulkWriteResult bulkWrite(final List<? extends WriteModel<? extends Document>> requests, final BulkWriteOptions options) {
        return collExecutor.bulkWrite(requests, options);
    }

    /**
     * Finds and updates a single entity atomically, returning the entity.
     * 
     * <p>This method atomically finds and updates a single entity matching the filter,
     * returning either the original or updated entity based on options. The operation
     * is atomic, preventing race conditions in concurrent environments.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User updates = new User();
     * updates.setLoginCount(5);
     * User user = mapper.findOneAndUpdate(
     *     Filters.eq("email", "user@example.com"), updates);
     * }</pre>
     * 
     * @param filter the query filter to match the entity to update
     * @param update the entity containing update data
     * @return the entity before or after the update (based on default options), or null if not found
     * @throws IllegalArgumentException if filter or update is null
     * @throws com.mongodb.MongoWriteException if the operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #findOneAndUpdate(Bson, Object, FindOneAndUpdateOptions)
     */
    public T findOneAndUpdate(final Bson filter, final T update) {
        return collExecutor.findOneAndUpdate(filter, update, rowType);
    }

    /**
     * Finds and updates a single entity atomically with additional update options.
     *
     * <p>This method provides fine-grained control over atomic find-and-update operations through
     * FindOneAndUpdateOptions. Options enable returning the updated document, upsert behavior,
     * field projection, sorting, and array filters for nested updates.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndUpdateOptions options = new FindOneAndUpdateOptions()
     *     .returnDocument(ReturnDocument.AFTER)  // Return updated document
     *     .upsert(true)
     *     .projection(Projections.include("name", "status"));
     * User updates = new User();
     * updates.setLastSeen(new Date());
     * User updatedUser = mapper.findOneAndUpdate(
     *     Filters.eq("email", "user@example.com"), updates, options);
     * }</pre>
     *
     * @param filter the query filter to match the entity to update
     * @param update the entity containing update data
     * @param options additional options for the find and update operation (null uses defaults)
     * @return the entity before or after the update (based on options), or null if not found
     * @throws IllegalArgumentException if filter or update is null
     * @throws com.mongodb.MongoWriteException if the operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #findOneAndUpdate(Bson, Object)
     * @see FindOneAndUpdateOptions
     */
    public T findOneAndUpdate(final Bson filter, final T update, final FindOneAndUpdateOptions options) {
        return collExecutor.findOneAndUpdate(filter, update, options, rowType);
    }

    /**
     * Finds and updates a single entity atomically using a pipeline of update operations.
     *
     * <p>This method performs atomic find-and-update using an aggregation pipeline for complex
     * transformations. Pipeline updates enable field calculations, conditional logic, and
     * multi-stage transformations that reference existing document values.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<User> updatePipeline = Arrays.asList(
     *     new User().setVisitCount(5),  // Increment visits
     *     new User().setLastUpdated(new Date())
     * );
     * User user = mapper.findOneAndUpdate(
     *     Filters.eq("userId", "USER123"), updatePipeline);
     * }</pre>
     *
     * @param filter the query filter to match the entity to update
     * @param objList collection of entities forming the update pipeline
     * @return the entity before the update (default behavior), or null if not found
     * @throws IllegalArgumentException if filter or objList is null or empty
     * @throws com.mongodb.MongoWriteException if the operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #findOneAndUpdate(Bson, Object)
     * @see #findOneAndUpdate(Bson, Collection, FindOneAndUpdateOptions)
     */
    public T findOneAndUpdate(final Bson filter, final Collection<? extends T> objList) {
        return collExecutor.findOneAndUpdate(filter, objList, rowType);
    }

    /**
     * Finds and updates a single entity atomically using a pipeline with additional options.
     *
     * <p>This method combines pipeline-based updates with FindOneAndUpdateOptions for maximum
     * flexibility in atomic operations. It enables complex transformations with precise control
     * over return behavior, upsert operations, and field projections.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndUpdateOptions options = new FindOneAndUpdateOptions()
     *     .returnDocument(ReturnDocument.AFTER)
     *     .upsert(true);
     * List<Product> pipeline = createPriceUpdatePipeline();
     * Product updated = mapper.findOneAndUpdate(
     *     Filters.eq("sku", "PROD-456"), pipeline, options);
     * }</pre>
     *
     * @param filter the query filter to match the entity to update
     * @param objList collection of entities forming the update pipeline
     * @param options additional options for the find and update operation (null uses defaults)
     * @return the entity before or after the update (based on options), or null if not found
     * @throws IllegalArgumentException if filter or objList is null or empty
     * @throws com.mongodb.MongoWriteException if the operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #findOneAndUpdate(Bson, Collection)
     * @see FindOneAndUpdateOptions
     */
    public T findOneAndUpdate(final Bson filter, final Collection<? extends T> objList, final FindOneAndUpdateOptions options) {
        return collExecutor.findOneAndUpdate(filter, objList, options, rowType);
    }

    /**
     * Finds and replaces a single entity atomically.
     *
     * <p>This method atomically finds and completely replaces a single entity matching the filter.
     * Unlike update operations that modify specific fields, this replaces the entire document
     * except for the _id field. The operation is atomic, preventing race conditions.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User replacement = new User("John Updated", "john.new@example.com", 32);
     * User originalUser = mapper.findOneAndReplace(
     *     Filters.eq("username", "john"), replacement);
     * }</pre>
     *
     * @param filter the query filter to match the entity to replace
     * @param replacement the new entity to replace the existing one
     * @return the original entity before replacement, or null if not found
     * @throws IllegalArgumentException if filter or replacement is null
     * @throws com.mongodb.MongoWriteException if the operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #findOneAndReplace(Bson, Object, FindOneAndReplaceOptions)
     * @see #replaceOne(Bson, Object)
     */
    public T findOneAndReplace(final Bson filter, final T replacement) {
        return collExecutor.findOneAndReplace(filter, replacement, rowType);
    }

    /**
     * Finds and replaces a single entity atomically with additional replace options.
     *
     * <p>This method provides fine-grained control over atomic find-and-replace operations through
     * FindOneAndReplaceOptions. Options enable returning the updated document, upsert behavior,
     * field projection, sorting, and bypass validation for advanced replacement scenarios.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndReplaceOptions options = new FindOneAndReplaceOptions()
     *     .returnDocument(ReturnDocument.AFTER)  // Return new document
     *     .upsert(true)
     *     .projection(Projections.exclude("_id"));
     * Article newArticle = createUpdatedArticle();
     * Article result = mapper.findOneAndReplace(
     *     Filters.eq("slug", "article-slug"), newArticle, options);
     * }</pre>
     *
     * @param filter the query filter to match the entity to replace
     * @param replacement the new entity to replace the existing one
     * @param options additional options for the find and replace operation (null uses defaults)
     * @return the entity before or after replacement (based on options), or null if not found
     * @throws IllegalArgumentException if filter or replacement is null
     * @throws com.mongodb.MongoWriteException if the operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #findOneAndReplace(Bson, Object)
     * @see FindOneAndReplaceOptions
     */
    public T findOneAndReplace(final Bson filter, final T replacement, final FindOneAndReplaceOptions options) {
        return collExecutor.findOneAndReplace(filter, replacement, options, rowType);
    }

    /**
     * Finds and deletes a single entity atomically.
     *
     * <p>This method atomically finds and deletes a single entity matching the filter,
     * returning the deleted entity. The operation is atomic, ensuring the entity cannot
     * be modified by other operations between the find and delete.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User deletedUser = mapper.findOneAndDelete(
     *     Filters.and(
     *         Filters.eq("status", "inactive"),
     *         Filters.lt("lastLogin", thirtyDaysAgo)
     *     )
     * );
     * if (deletedUser != null) {
     *     auditUserDeletion(deletedUser);
     * }
     * }</pre>
     *
     * @param filter the query filter to match the entity to delete
     * @return the deleted entity, or null if no entity matched the filter
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoWriteException if the operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #findOneAndDelete(Bson, FindOneAndDeleteOptions)
     * @see #deleteOne(Bson)
     */
    public T findOneAndDelete(final Bson filter) {
        return collExecutor.findOneAndDelete(filter, rowType);
    }

    /**
     * Finds and deletes a single entity atomically with additional delete options.
     *
     * <p>This method provides fine-grained control over atomic find-and-delete operations through
     * FindOneAndDeleteOptions. Options enable field projection, sorting, collation, and hints
     * for optimized atomic deletion scenarios.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * FindOneAndDeleteOptions options = new FindOneAndDeleteOptions()
     *     .sort(Sorts.ascending("createdAt"))  // Delete oldest first
     *     .projection(Projections.include("email", "name"));
     * User deletedUser = mapper.findOneAndDelete(
     *     Filters.eq("status", "pending"), options);
     * }</pre>
     *
     * @param filter the query filter to match the entity to delete
     * @param options additional options for the find and delete operation (null uses defaults)
     * @return the deleted entity with projected fields, or null if no entity matched
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoWriteException if the operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #findOneAndDelete(Bson)
     * @see FindOneAndDeleteOptions
     */
    public T findOneAndDelete(final Bson filter, final FindOneAndDeleteOptions options) {
        return collExecutor.findOneAndDelete(filter, options, rowType);
    }

    /**
     * Returns a stream of distinct values for the specified field across all entities.
     *
     * <p>This method streams all unique values for the specified field name across the entire
     * collection. The values are automatically converted to the entity type, useful for getting
     * distinct field values for analysis or dropdown populations.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Get all unique categories:
     * try (Stream<Product> categoryStream = mapper.distinct("category")) {
     *     List<String> categories = categoryStream
     *         .map(Product::getCategory)
     *         .collect(Collectors.toList());
     * }
     * }</pre>
     *
     * @param fieldName the name of the field to get distinct values from
     * @return a Stream of entities containing only the distinct field values
     * @throws IllegalArgumentException if fieldName is null or empty
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #distinct(String, Bson)
     * @see Stream
     */
    public Stream<T> distinct(final String fieldName) {
        return collExecutor.distinct(fieldName, rowType);
    }

    /**
     * Returns a stream of distinct values for the specified field matching the filter.
     *
     * <p>This method streams unique values for the specified field from entities that match
     * the filter criteria. This is useful for getting distinct values from a subset of the
     * collection based on specific conditions.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Get distinct categories for active products:
     * try (Stream<Product> stream = mapper.distinct("category",
     *         Filters.eq("active", true))) {
     *     stream.map(Product::getCategory)
     *           .forEach(System.out::println);
     * }
     * }</pre>
     *
     * @param fieldName the name of the field to get distinct values from
     * @param filter the query filter to match entities before extracting distinct values
     * @return a Stream of entities containing only the distinct field values from matching entities
     * @throws IllegalArgumentException if fieldName is null or empty, or filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #distinct(String)
     * @see Stream
     */
    public Stream<T> distinct(final String fieldName, final Bson filter) {
        return collExecutor.distinct(fieldName, filter, rowType);
    }

    /**
     * Executes an aggregation pipeline and returns a stream of transformed entities.
     *
     * <p>This method executes a MongoDB aggregation pipeline, automatically converting the
     * results to the entity type. Aggregation pipelines enable complex data transformations,
     * grouping, filtering, and analysis operations that go beyond simple queries.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Bson> pipeline = Arrays.asList(
     *     Aggregates.match(Filters.gte("price", 100.0)),
     *     Aggregates.group("$category", Accumulators.avg("avgPrice", "$price")),
     *     Aggregates.sort(Sorts.descending("avgPrice"))
     * );
     * try (Stream<Product> results = mapper.aggregate(pipeline)) {
     *     results.forEach(stat -> System.out.println(
     *         stat.getCategory() + ": $" + stat.getAvgPrice()));
     * }
     * }</pre>
     *
     * @param pipeline list of aggregation pipeline stages to execute
     * @return a Stream of entities representing the aggregation results
     * @throws IllegalArgumentException if pipeline is null or empty
     * @throws com.mongodb.MongoException if the database operation fails
     * @see Stream
     * @see com.mongodb.client.model.Aggregates
     * @see com.mongodb.client.model.Accumulators
     */
    public Stream<T> aggregate(final List<? extends Bson> pipeline) {
        return collExecutor.aggregate(pipeline, rowType);
    }

    /**
     * Groups entities by the specified field and returns a stream of grouped results.
     *
     * <p><strong>Beta Feature:</strong> This method is experimental and may change in future versions.</p>
     *
     * <p>This method groups entities by a single field value, useful for basic grouping operations.
     * The grouped results are returned as a stream of entities representing each group.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * try (Stream<User> groupedStream = mapper.groupBy("department")) {
     *     groupedStream.forEach(group -> 
     *         System.out.println("Department: " + group.getDepartment()));
     * }
     * }</pre>
     *
     * @param fieldName the field name to group entities by
     * @return a Stream of entities representing grouped results
     * @throws IllegalArgumentException if fieldName is null or empty
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #groupBy(Collection)
     * @see #groupByAndCount(String)
     */
    @Beta
    public Stream<T> groupBy(final String fieldName) {
        return collExecutor.groupBy(fieldName, rowType);
    }

    /**
     * Groups entities by multiple fields and returns a stream of grouped results.
     *
     * <p><strong>Beta Feature:</strong> This method is experimental and may change in future versions.</p>
     *
     * <p>This method groups entities by multiple field values, enabling compound grouping
     * for more complex analysis. Each group represents a unique combination of the specified fields.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> groupFields = Arrays.asList("department", "level");
     * try (Stream<Employee> groups = mapper.groupBy(groupFields)) {
     *     groups.forEach(group -> System.out.println(
     *         group.getDepartment() + "/" + group.getLevel()));
     * }
     * }</pre>
     *
     * @param fieldNames collection of field names to group entities by
     * @return a Stream of entities representing grouped results
     * @throws IllegalArgumentException if fieldNames is null or empty
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #groupBy(String)
     * @see #groupByAndCount(Collection)
     */
    @Beta
    public Stream<T> groupBy(final Collection<String> fieldNames) {
        return collExecutor.groupBy(fieldNames, rowType);
    }

    /**
     * Groups entities by the specified field and includes count information.
     *
     * <p><strong>Beta Feature:</strong> This method is experimental and may change in future versions.</p>
     *
     * <p>This method groups entities by a field value and includes the count of entities in each group.
     * This is useful for generating summary statistics and understanding data distribution.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * try (Stream<Order> countedGroups = mapper.groupByAndCount("status")) {
     *     countedGroups.forEach(group -> System.out.println(
     *         group.getStatus() + ": " + group.getCount() + " orders"));
     * }
     * }</pre>
     *
     * @param fieldName the field name to group entities by
     * @return a Stream of entities with group information and counts
     * @throws IllegalArgumentException if fieldName is null or empty
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #groupBy(String)
     * @see #groupByAndCount(Collection)
     */
    @Beta
    public Stream<T> groupByAndCount(final String fieldName) {
        return collExecutor.groupByAndCount(fieldName, rowType);
    }

    /**
     * Groups entities by multiple fields and includes count information.
     *
     * <p><strong>Beta Feature:</strong> This method is experimental and may change in future versions.</p>
     *
     * <p>This method groups entities by multiple field values and includes the count of entities
     * in each group. This enables multi-dimensional analysis with count statistics for
     * complex data aggregation scenarios.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("region", "productType");
     * try (Stream<Sale> stats = mapper.groupByAndCount(fields)) {
     *     stats.forEach(stat -> System.out.println(
     *         stat.getRegion() + "/" + stat.getProductType() + ": " + 
     *         stat.getCount() + " sales"));
     * }
     * }</pre>
     *
     * @param fieldNames collection of field names to group entities by
     * @return a Stream of entities with group information and counts
     * @throws IllegalArgumentException if fieldNames is null or empty
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #groupBy(Collection)
     * @see #groupByAndCount(String)
     */
    @Beta
    public Stream<T> groupByAndCount(final Collection<String> fieldNames) {
        return collExecutor.groupByAndCount(fieldNames, rowType);
    }

    /**
     * Executes a MapReduce operation and returns a stream of results.
     *
     * <p><strong>Deprecated:</strong> MapReduce is deprecated in MongoDB in favor of aggregation pipelines.
     * Use {@link #aggregate(List)} with aggregation stages for better performance and functionality.</p>
     *
     * <p>This method executes a MapReduce operation using JavaScript functions for mapping and reducing.
     * MapReduce operations are less efficient than aggregation pipelines and should be avoided in new code.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String mapFunction = "function() { emit(this.category, this.price); }";
     * String reduceFunction = "function(key, values) { return Array.sum(values); }";
     * try (Stream<Product> results = mapper.mapReduce(mapFunction, reduceFunction)) {
     *     results.forEach(result -> System.out.println(result));
     * }
     * }</pre>
     *
     * @param mapFunction JavaScript map function as a string
     * @param reduceFunction JavaScript reduce function as a string
     * @return a Stream of entities representing the MapReduce results
     * @throws IllegalArgumentException if mapFunction or reduceFunction is null or empty
     * @throws com.mongodb.MongoException if the database operation fails
     * @deprecated Use {@link #aggregate(List)} with aggregation pipeline instead.
     * @see #aggregate(List)
     * @see com.mongodb.client.model.Aggregates
     */
    @Deprecated
    public Stream<T> mapReduce(final String mapFunction, final String reduceFunction) {
        return collExecutor.mapReduce(mapFunction, reduceFunction, rowType);
    }
}
