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
import com.landawn.abacus.util.Dataset;
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
import com.landawn.abacus.util.stream.Stream;
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
import com.mongodb.client.result.UpdateResult;

/**
 * Synchronous, strongly-typed façade over a MongoDB {@code MongoCollection<Document>}.
 *
 * <p>This class wraps a {@link MongoCollectionExecutor} (which works in raw {@link Document}s) and
 * adds automatic object-document mapping for a single entity type {@code T}: queries return
 * {@code T} (or {@code Optional<T>}, {@code Stream<T>}, ...), while writes accept {@code T} and are
 * converted to {@link Document}s before being sent to the driver.</p>
 *
 * <h2>{@code @Id} Mapping</h2>
 * <ul>
 *   <li>An entity property annotated with {@code @Id} (or named {@code id} when no annotation is
 *       present) is mapped to MongoDB's {@code _id} field on writes and back to the same property
 *       on reads.</li>
 *   <li>When the {@code _id} value is omitted, MongoDB assigns an {@link ObjectId} during insert; the
 *       generated value is <strong>not</strong> written back into the entity — the entity's id property
 *       remains {@code null} after the insert returns, and the generated {@code _id} exists only in the
 *       database.</li>
 *   <li>The {@code get/gett} and id-keyed {@code updateOne/replaceOne/deleteOne} overloads accept
 *       either an {@link ObjectId} or its 24-hex-character {@link String} form; the string overloads
 *       throw {@link IllegalArgumentException} if the string is not a valid hex ObjectId.</li>
 * </ul>
 *
 * <h2>Property &harr; Field Mapping</h2>
 * <ul>
 *   <li>Java property names map 1:1 to BSON field names by default; an annotation supported by the
 *       configured parser (e.g. {@code @Column}) can override the BSON field name.</li>
 *   <li>Conversion is delegated to the codec registry configured on the underlying
 *       {@code MongoCollection}, so any custom codecs registered there are honored.</li>
 *   <li>Embedded objects, collections and maps are converted recursively. BSON {@code null} and
 *       missing fields both surface as Java {@code null} in the mapped entity.</li>
 * </ul>
 *
 * <h2>Projection / Sort / Limit Semantics</h2>
 * <ul>
 *   <li>{@code selectPropNames} is a collection of <i>property</i> names (not BSON field names);
 *       it is translated to a projection on the corresponding mapped fields. Passing {@code null}
 *       selects every field. The {@code _id} field is always returned unless explicitly excluded
 *       via the {@link Bson}-projection overloads.</li>
 *   <li>{@link Bson}-projection overloads pass the projection through unchanged, so any
 *       {@code com.mongodb.client.model.Projections} expression (including computed/sliced fields)
 *       is supported. Computed fields may surface in the returned entity only when the entity has
 *       a matching property/setter.</li>
 *   <li>{@code sort} is any {@link Bson} sort expression
 *       (see {@code com.mongodb.client.model.Sorts}). When omitted, the driver returns documents in
 *       the natural order, which is not stable across queries.</li>
 *   <li>{@code offset}/{@code count} are forwarded as the driver's {@code skip} and {@code limit}
 *       hints. {@code offset == 0} disables skip; a negative {@code count} throws
 *       {@link IllegalArgumentException}, {@code count == 0} yields an empty result, and
 *       {@code Integer.MAX_VALUE} is effectively "no limit".</li>
 * </ul>
 *
 * <h2>Bulk-Write Atomicity</h2>
 * <ul>
 *   <li>{@link #bulkWrite(List)} and {@link #bulkWrite(List, BulkWriteOptions)} send all
 *       {@link WriteModel}s in a single bulk-write call to the server. Each individual write is
 *       atomic on its target document, but the bulk as a whole is <strong>not</strong> atomic
 *       across documents — it is not a transaction. Use a session-bound transaction at the
 *       executor level if cross-document atomicity is required.</li>
 *   <li>With {@code ordered = true} (the driver default), the server stops at the first failing
 *       operation; earlier successful operations remain applied. With {@code ordered = false},
 *       all operations are attempted and errors are reported in the bulk result.</li>
 *   <li>{@link #insertMany(Collection)} has the same per-document atomicity guarantees as
 *       {@code bulkWrite} with insert models.</li>
 * </ul>
 *
 * <h2>Thread Safety</h2>
 * <p>This class is immutable after construction and delegates to a thread-safe executor; instances
 * are safe for concurrent use from multiple threads.</p>
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * // Entity class:
 * public class User {
 *     @Id
 *     private String id;
 *     private String name;
 *     private String email;
 *     private Date createdAt;
 *     // getters and setters
 * }
 *
 * // Create mapper:
 * MongoCollectionMapper<User> userMapper = mongoDB.collectionMapper(User.class);
 *
 * // Type-safe operations:
 * User newUser = new User("John Doe", "john@example.com", new Date());
 * userMapper.insertOne(newUser);
 *
 * Optional<User> user = userMapper.findFirst(Filters.eq("email", "john@example.com"));
 * List<User> activeUsers = userMapper.list(Filters.eq("active", true));
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

    private final MongoCollectionExecutor collectionExecutor;

    private final Class<T> rowType;

    /**
     * Package-private constructor used by {@link MongoDB#collectionMapper(Class)} and friends.
     *
     * <p>Instances are not intended to be created directly by user code; obtain one from the parent
     * {@link MongoDB} instance instead.</p>
     *
     * @param collectionExecutor the underlying executor that performs the raw MongoDB operations
     * @param resultClass the entity {@link Class} used for object-document mapping
     */
    MongoCollectionMapper(final MongoCollectionExecutor collectionExecutor, final Class<T> resultClass) {
        this.collectionExecutor = collectionExecutor;
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
     * MongoCollectionMapper<User> userMapper = mongoDB.collectionMapper(User.class);
     * MongoCollectionExecutor executor = userMapper.mongoCollectionExecutor(); // never null; same instance on each call
     * // Use raw executor for operations not exposed by the mapper, e.g. an untyped aggregate:
     * Document complexResult = executor.aggregate(complexPipeline).first(); // raw Document, not the mapped entity
     * }</pre>
     *
     * @return the underlying MongoCollectionExecutor instance
     * @see MongoCollectionExecutor
     */
    public MongoCollectionExecutor mongoCollectionExecutor() {
        return collectionExecutor;
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
     * MongoCollectionMapper<User> userMapper = mongoDB.collectionMapper(User.class);
     * boolean present = userMapper.exists("507f1f77bcf86cd799439011"); // returns true if a doc with that _id exists
     * boolean absent  = userMapper.exists("000000000000000000000000"); // returns false when nothing matches
     *
     * // A malformed (non-24-hex) string is rejected before any DB call:
     * userMapper.exists("not-a-valid-id"); // throws IllegalArgumentException
     * }</pre>
     *
     * @param objectId the string representation of the ObjectId to check
     * @return {@code true} if an entity with the specified ObjectId exists, {@code false} otherwise
     * @throws IllegalArgumentException if objectId is null, empty, or invalid format
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #exists(ObjectId)
     */
    public boolean exists(final String objectId) {
        return collectionExecutor.exists(objectId);
    }

    /**
     * Checks if an entity exists by its ObjectId.
     *
     * <p>This method performs efficient existence verification using the native ObjectId type
     * for the mapped entity class.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collectionMapper(User.class);
     * ObjectId userId = new ObjectId("507f1f77bcf86cd799439011");
     * boolean userExists = mapper.exists(userId);  // returns true / false depending on the collection
     *
     * boolean missing = mapper.exists(new ObjectId()); // returns false: a brand-new id is almost certainly absent
     * }</pre>
     *
     * @param objectId the ObjectId to check for existence
     * @return {@code true} if an entity with the specified ObjectId exists, {@code false} otherwise
     * @throws IllegalArgumentException if objectId is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see ObjectId
     */
    public boolean exists(final ObjectId objectId) {
        return collectionExecutor.exists(objectId);
    }

    /**
     * Checks if any entities exist matching the specified filter criteria.
     *
     * <p>This method performs an existence check for the mapped entity type using the provided
     * filter. It's more efficient than retrieving entities when you only need to verify existence.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collectionMapper(User.class);
     * // Check if any active users exist:
     * boolean hasActiveUsers = mapper.exists(Filters.eq("status", "active")); // returns true if >=1 match
     * // Filters parsed from JSON must be wrapped in a Bson (Document.parse), not passed as a raw String:
     * boolean hasRecent = mapper.exists(Document.parse("{ createdAt: { $gte: ISODate('2023-01-01') } }"));
     * boolean none = mapper.exists(Filters.eq("status", "no-such-status")); // returns false: empty match set
     * }</pre>
     *
     * @param filter the query filter to match entities against
     * @return {@code true} if any entities match the filter, {@code false} otherwise
     * @throws IllegalArgumentException if {@code filter} is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see com.mongodb.client.model.Filters
     */
    public boolean exists(final Bson filter) {
        return collectionExecutor.exists(filter);
    }

    /**
     * Returns the total number of entities in the collection (blocking operation).
     *
     * <p>This method counts all entities of the mapped type in the collection.
     * For large collections, consider using {@link #mongoCollectionExecutor()}.{@code estimatedDocumentCount()} for better performance
     * when exact counts are not required.</p>
     *
     * <p><b>Note:</b> This method performs a blocking operation. For non-blocking operations, use
     * {@link #mongoCollectionExecutor()}.{@code async()}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> userMapper = mongoDB.collectionMapper(User.class);
     * long totalUsers = userMapper.count();   // returns the exact document count, e.g. 1500
     * // An empty collection counts as 0 (never negative, never null):
     * long emptyCount = mongoDB.collectionMapper(Audit.class).count(); // returns 0L when nothing has been inserted
     * }</pre>
     *
     * @return the total number of entities in the collection
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #count(Bson)
     * @see #mongoCollectionExecutor()
     */
    public long count() {
        return collectionExecutor.count();
    }

    /**
     * Returns the number of entities matching the specified filter.
     *
     * <p>This method counts entities of the mapped type that match the given filter criteria.
     * The count operation respects any read preference settings configured on the collection.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collectionMapper(User.class);
     * long activeUsers = mapper.count(Filters.eq("status", "active")); // returns the number of active users
     * // A JSON filter must be a Bson (Document.parse), not a raw String:
     * long adults = mapper.count(Document.parse("{ age: { $gte: 18 } }"));
     * long none = mapper.count(Filters.eq("status", "no-such-status")); // returns 0L when nothing matches
     * }</pre>
     *
     * @param filter the query filter to count matching entities
     * @return the number of entities matching the filter
     * @throws IllegalArgumentException if {@code filter} is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see com.mongodb.client.model.Filters
     */
    public long count(final Bson filter) {
        return collectionExecutor.count(filter);
    }

    /**
     * Returns the count of entities matching the filter with additional count options.
     *
     * <p>This method provides fine-grained control over the count operation for the mapped
     * entity type through CountOptions, allowing specification of limits, skips, and hints.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collectionMapper(User.class);
     * // Cap the count so a huge match set never costs more than scanning 1000 docs:
     * CountOptions options = new CountOptions().limit(1000);
     * long limitedCount = mapper.count(Filters.exists("email"), options); // returns at most 1000
     * // skip() shifts the window: skip the first 10 matches before counting the rest:
     * long afterSkip = mapper.count(Filters.exists("email"), new CountOptions().skip(10));
     * }</pre>
     *
     * @param filter the query filter to count matching entities
     * @param options additional options for the count operation (null uses defaults)
     * @return the number of entities matching the filter within the specified constraints
     * @throws IllegalArgumentException if {@code filter} is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see CountOptions
     */
    public long count(final Bson filter, final CountOptions options) {
        return collectionExecutor.count(filter, options);
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
     * MongoCollectionMapper<User> mapper = mongoDB.collectionMapper(User.class);
     * Optional<User> user = mapper.get("507f1f77bcf86cd799439011"); // returns Optional with the entity, or empty
     * String name = user.map(User::getName).orElse("<unknown>");    // safe access without an explicit isPresent() check
     *
     * Optional<User> missing = mapper.get("000000000000000000000000"); // returns Optional.empty() when no match
     * boolean absent = missing.isPresent() == false;                   // absent == true
     *
     * mapper.get("xyz"); // throws IllegalArgumentException: not a 24-hex ObjectId
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
        return collectionExecutor.get(objectId, rowType);
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
     * MongoCollectionMapper<User> mapper = mongoDB.collectionMapper(User.class);
     * ObjectId userId = new ObjectId("507f1f77bcf86cd799439011");
     * Optional<User> user = mapper.get(userId); // returns Optional with the entity, or empty
     * if (user.isPresent()) {
     *     User u = user.get();                  // safe: only called when present
     * }
     *
     * Optional<User> fresh = mapper.get(new ObjectId()); // returns Optional.empty(): unused id has no document
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
        return collectionExecutor.get(objectId, rowType);
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
     * MongoCollectionMapper<User> mapper = mongoDB.collectionMapper(User.class);
     * Collection<String> fields = Arrays.asList("name", "email", "status");
     * // Only name/email/status are populated; _id is always returned, other props stay null:
     * Optional<User> partialUser = mapper.get("507f1f77bcf86cd799439011", fields); // returns Optional, possibly empty
     *
     * // Passing null for selectPropNames is equivalent to fetching every field:
     * Optional<User> fullUser = mapper.get("507f1f77bcf86cd799439011", (Collection<String>) null);
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
        return collectionExecutor.get(objectId, selectPropNames, rowType);
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
     * MongoCollectionMapper<User> mapper = mongoDB.collectionMapper(User.class);
     * ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
     * Collection<String> fields = Set.of("name", "email");
     * Optional<User> user = mapper.get(id, fields);     // returns Optional with name/email populated, or empty
     * String email = user.map(User::getEmail).orElse(null);
     *
     * Optional<User> missing = mapper.get(new ObjectId(), fields); // returns Optional.empty() when no match
     * }</pre>
     *
     * @param objectId the ObjectId to search for
     * @param selectPropNames collection of field names to include (null includes all fields)
     * @return an Optional containing the entity with only the specified fields populated, or empty if not found
     * @throws IllegalArgumentException if objectId is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #get(String, Collection)
     * @see com.mongodb.client.model.Projections
     */
    public Optional<T> get(final ObjectId objectId, final Collection<String> selectPropNames) {
        return collectionExecutor.get(objectId, selectPropNames, rowType);
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
     * MongoCollectionMapper<User> mapper = mongoDB.collectionMapper(User.class);
     * User user = mapper.gett("507f1f77bcf86cd799439011"); // returns the entity, or null if absent
     * if (user != null) {
     *     process(user);                                   // null guard required, unlike the Optional-based get
     * }
     *
     * User missing = mapper.gett("000000000000000000000000"); // returns null: no document with that _id
     * mapper.gett("bad");                                      // throws IllegalArgumentException: not 24-hex
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
        return collectionExecutor.gett(objectId, rowType);
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
     * MongoCollectionMapper<User> mapper = mongoDB.collectionMapper(User.class);
     * ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
     * User user = mapper.gett(id); // returns the entity, or null if absent
     * if (user != null) {
     *     processUser(user);       // null guard required
     * }
     *
     * User missing = mapper.gett(new ObjectId()); // returns null: unused id has no document
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
        return collectionExecutor.gett(objectId, rowType);
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
     * MongoCollectionMapper<User> mapper = mongoDB.collectionMapper(User.class);
     * Collection<String> fields = Arrays.asList("name", "email", "department");
     * // Only the listed props (plus _id) are populated; others stay null:
     * User partialUser = mapper.gett("507f1f77bcf86cd799439011", fields); // returns the entity, or null if absent
     * if (partialUser != null) {
     *     String dept = partialUser.getDepartment(); // populated; e.g. getStatus() would be null (not selected)
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
        return collectionExecutor.gett(objectId, selectPropNames, rowType);
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
     * MongoCollectionMapper<User> mapper = mongoDB.collectionMapper(User.class);
     * ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
     * Collection<String> fields = Set.of("name", "email");
     * User user = mapper.gett(id, fields); // returns the entity (name/email populated), or null if absent
     * if (user != null) {
     *     sendEmail(user.getEmail());      // null guard required
     * }
     *
     * User missing = mapper.gett(new ObjectId(), fields); // returns null: unused id has no document
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
        return collectionExecutor.gett(objectId, selectPropNames, rowType);
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
     * MongoCollectionMapper<User> mapper = mongoDB.collectionMapper(User.class);
     * // Find the first active user (natural order; pass a sort overload for a stable "first"):
     * Optional<User> user = mapper.findFirst(Filters.eq("status", "active")); // returns Optional, possibly empty
     * user.ifPresent(u -> process(u));
     * // Using a JSON filter (must be a Bson via Document.parse, not a raw String):
     * Optional<User> recentUser = mapper.findFirst(Document.parse("{ createdAt: { $gte: ISODate('2023-01-01') } }"));
     * Optional<User> none = mapper.findFirst(Filters.eq("status", "no-such")); // returns Optional.empty()
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
        return collectionExecutor.findFirst(filter, rowType);
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
     * MongoCollectionMapper<User> mapper = mongoDB.collectionMapper(User.class);
     * Collection<String> fields = Arrays.asList("name", "email");
     * // First active user, with only name/email (plus _id) populated:
     * Optional<User> user = mapper.findFirst(fields, Filters.eq("status", "active")); // returns Optional, possibly empty
     * String email = user.map(User::getEmail).orElse(null);
     * // null fields means "all fields":
     * Optional<User> full = mapper.findFirst(null, Filters.eq("status", "active"));
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
        return collectionExecutor.findFirst(selectPropNames, filter, rowType);
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
     * MongoCollectionMapper<Order> mapper = mongoDB.collectionMapper(Order.class);
     * Collection<String> fields = Arrays.asList("orderId", "total", "status");
     * Bson filter = Filters.eq("customerId", "CUST123");
     * Bson sort = Sorts.descending("createdAt");   // Most recent first
     * // Sort makes "first" deterministic: the newest order for this customer:
     * Optional<Order> recentOrder = mapper.findFirst(fields, filter, sort);                    // returns Optional, possibly empty
     * Optional<Order> oldest = mapper.findFirst(fields, filter, Sorts.ascending("createdAt")); // earliest instead
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
        return collectionExecutor.findFirst(selectPropNames, filter, sort, rowType);
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
     * MongoCollectionMapper<Article> mapper = mongoDB.collectionMapper(Article.class);
     * Bson projection = Projections.fields(
     *     Projections.include("title", "author"),
     *     Projections.slice("tags", 5)
     * );
     * Bson filter = Filters.eq("status", "published");
     * Bson sort = Sorts.descending("views");
     * // Most-viewed published article, with tags trimmed to the first 5 entries:
     * Optional<Article> popularArticle = mapper.findFirst(projection, filter, sort);              // returns Optional, possibly empty
     * Optional<Article> none = mapper.findFirst(projection, Filters.eq("status", "draft"), sort); // empty if no drafts
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
        return collectionExecutor.findFirst(projection, filter, sort, rowType);
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
     * MongoCollectionMapper<User> mapper = mongoDB.collectionMapper(User.class);
     * List<User> activeUsers = mapper.list(Filters.eq("status", "active")); // returns all matches, e.g. size 42
     * activeUsers.forEach(user -> processUser(user));
     * // No match yields an empty (never null) list:
     * List<User> none = mapper.list(Filters.eq("status", "no-such")); // returns an empty List, none.isEmpty() == true
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
        return collectionExecutor.list(filter, rowType);
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
     * MongoCollectionMapper<Product> mapper = mongoDB.collectionMapper(Product.class);
     * Bson filter = Filters.eq("category", "electronics");
     * List<Product> page1 = mapper.list(filter, 0, 20);  // returns up to 20 matches (the first page)
     * List<Product> page2 = mapper.list(filter, 20, 20); // returns the next 20 (skip 20, limit 20)
     * // Past the end of the result set yields an empty list:
     * List<Product> empty = mapper.list(filter, 1_000_000, 20); // returns an empty List
     * }</pre>
     *
     * @param filter the query filter to match entities against
     * @param offset the number of documents to skip from the beginning (0 for first page)
     * @param count the maximum number of entities to return ({@code 0} yields an empty list;
     *        {@code Integer.MAX_VALUE} is effectively "no limit")
     * @return a List containing the requested page of matching entities
     * @throws IllegalArgumentException if filter is null, offset is negative, or count is negative
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #list(Bson)
     * @see #list(Collection, Bson, int, int)
     */
    public List<T> list(final Bson filter, final int offset, final int count) {
        return collectionExecutor.list(filter, offset, count, rowType);
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
     * MongoCollectionMapper<User> mapper = mongoDB.collectionMapper(User.class);
     * Collection<String> fields = Arrays.asList("name", "email", "status");
     * // All active users with only name/email/status (plus _id) populated:
     * List<User> users = mapper.list(fields, Filters.eq("status", "active")); // returns all matches (empty if none)
     * // null fields means "all fields":
     * List<User> full = mapper.list(null, Filters.eq("status", "active"));
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
        return collectionExecutor.list(selectPropNames, filter, rowType);
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
     * MongoCollectionMapper<Product> mapper = mongoDB.collectionMapper(Product.class);
     * Collection<String> fields = Arrays.asList("name", "price", "category");
     * Bson filter = Filters.eq("inStock", true);
     * // Second page of in-stock products, only name/price/category populated:
     * List<Product> page2 = mapper.list(fields, filter, 20, 20); // returns up to 20 matches (skip 20, limit 20)
     * List<Product> page1 = mapper.list(fields, filter, 0, 20);  // returns the first page
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
        return collectionExecutor.list(selectPropNames, filter, offset, count, rowType);
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
     * MongoCollectionMapper<Article> mapper = mongoDB.collectionMapper(Article.class);
     * Collection<String> fields = Arrays.asList("title", "author", "publishedAt", "viewCount");
     * Bson filter = Filters.eq("status", "published");
     * Bson sort = Sorts.descending("viewCount");   // Most viewed first
     * // Published articles, most-viewed first, only the listed props populated:
     * List<Article> articles = mapper.list(fields, filter, sort);                            // returns all matches in sorted order (empty if none)
     * List<Article> leastViewed = mapper.list(fields, filter, Sorts.ascending("viewCount")); // reverse order
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
        return collectionExecutor.list(selectPropNames, filter, sort, rowType);
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
     * MongoCollectionMapper<User> mapper = mongoDB.collectionMapper(User.class);
     * Collection<String> fields = Arrays.asList("username", "email", "lastLoginAt");
     * Bson filter = Filters.eq("status", "active");
     * Bson sort = Sorts.descending("lastLoginAt");
     * // Second page (rows 26-50) of active users, most-recent login first:
     * List<User> recentUsers = mapper.list(fields, filter, sort, 25, 25); // returns up to 25 matches (skip 25)
     * List<User> firstPage = mapper.list(fields, filter, sort, 0, 25);    // returns the first 25
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
        return collectionExecutor.list(selectPropNames, filter, sort, offset, count, rowType);
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
     * MongoCollectionMapper<Order> mapper = mongoDB.collectionMapper(Order.class);
     * Bson projection = Projections.fields(
     *     Projections.include("customerId", "items"),
     *     Projections.computed("totalAmount", new Document("$sum", "$items.price"))
     * );
     * Bson filter = Filters.gte("orderDate", LocalDate.now().minusDays(30));
     * Bson sort = Sorts.descending("orderDate");
     * // Last 30 days of orders, newest first, with a computed totalAmount per order:
     * List<Order> recentOrders = mapper.list(projection, filter, sort); // returns all matches in sorted order
     * // A computed field only surfaces on the entity if it has a matching property/setter.
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
        return collectionExecutor.list(projection, filter, sort, rowType);
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
     * MongoCollectionMapper<Product> mapper = mongoDB.collectionMapper(Product.class);
     * Bson projection = Projections.fields(
     *     Projections.include("name", "category"),
     *     Projections.elemMatch("reviews", Filters.gte("rating", 4))
     * );
     * Bson filter = Filters.eq("category", "electronics");
     * Bson sort = Sorts.descending("avgRating");
     * // Top-rated electronics, first page of 20:
     * List<Product> topRated = mapper.list(projection, filter, sort, 0, 20);  // returns up to 20 matches, sorted
     * List<Product> nextPage = mapper.list(projection, filter, sort, 20, 20); // the following 20
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
        return collectionExecutor.list(projection, filter, sort, offset, count, rowType);
    }

    /**
     * Returns the value of {@code propName} from the first document matching {@code filter}, as a boolean value.
     *
     * <p>Only the value of {@code propName} on the first matching document is read; any remaining documents or fields are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code OptionalBoolean.empty()} is returned <i>only</i> when
     * no document matches the filter. If a document is matched, the returned
     * {@code OptionalBoolean} is <i>present</i> and holds the unboxed {@code Boolean} value — when the
     * field is missing or stored as BSON {@code null}, the JDBC-style primitive default {@code false}
     * may surface inside a present Optional. Use {@link #queryForSingleValue(String, Bson, Class)} with
     * {@code Boolean.class} if you need to distinguish a missing/{@code null} field from a real
     * {@code false}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collectionMapper(User.class);
     * OptionalBoolean isActive = mapper.queryForBoolean("active", Filters.eq("userId", "123"));
     * boolean active = isActive.orElse(false); // present -> the field value; empty (no match) -> the false fallback
     * // No matching document yields empty (NOT a present false):
     * OptionalBoolean none = mapper.queryForBoolean("active", Filters.eq("userId", "missing"));
     * boolean isEmpty = none.isPresent() == false; // isEmpty == true
     * }</pre>
     *
     * @param propName the name of the boolean property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @return a <i>present</i> {@code OptionalBoolean} holding the field value (or {@code false} for a
     *         missing/{@code null} field) when at least one document is matched;
     *         {@code OptionalBoolean.empty()} when no document matches
     * @throws IllegalArgumentException if propName is null or empty, or filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see OptionalBoolean
     * @see #queryForSingleValue(String, Bson, Class)
     */
    @Beta
    public OptionalBoolean queryForBoolean(final String propName, final Bson filter) {
        return collectionExecutor.queryForBoolean(propName, filter);
    }

    /**
     * Returns the value of {@code propName} from the first document matching {@code filter}, as a char value.
     *
     * <p>Only the value of {@code propName} on the first matching document is read; any remaining documents or fields are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code OptionalChar.empty()} is returned <i>only</i> when
     * no document matches the filter. If a document is matched, the returned
     * {@code OptionalChar} is <i>present</i> and holds the unboxed {@code Character} value — when the
     * field is missing or stored as BSON {@code null}, the JDBC-style primitive default {@code (char) 0}
     * (the NUL character) may surface inside a present Optional. Use
     * {@link #queryForSingleValue(String, Bson, Class)} with {@code Character.class} if you need to
     * distinguish a missing/{@code null} field from a real {@code (char) 0}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Entity> mapper = mongoDB.collectionMapper(Entity.class);
     * OptionalChar grade = mapper.queryForChar("grade", Filters.eq("studentId", "456"));
     * char g = grade.orElse('?'); // present -> the field value; empty (no match) -> the '?' fallback
     * // No match yields empty:
     * OptionalChar none = mapper.queryForChar("grade", Filters.eq("studentId", "missing"));
     * boolean isEmpty = none.isPresent() == false; // isEmpty == true
     * }</pre>
     *
     * @param propName the name of the character property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @return a <i>present</i> {@code OptionalChar} holding the field value (or the default {@code char}
     *         for a missing/{@code null} field) when at least one document is matched;
     *         {@code OptionalChar.empty()} when no document matches
     * @throws IllegalArgumentException if propName is null or empty, or filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see OptionalChar
     * @see #queryForSingleValue(String, Bson, Class)
     */
    @Beta
    public OptionalChar queryForChar(final String propName, final Bson filter) {
        return collectionExecutor.queryForChar(propName, filter);
    }

    /**
     * Returns the value of {@code propName} from the first document matching {@code filter}, as a byte value.
     *
     * <p>Only the value of {@code propName} on the first matching document is read; any remaining documents or fields are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code OptionalByte.empty()} is returned <i>only</i> when
     * no document matches the filter. If a document is matched, the returned
     * {@code OptionalByte} is <i>present</i> and holds the unboxed {@code Byte} value — when the field
     * is missing or stored as BSON {@code null}, the JDBC-style primitive default {@code (byte) 0} may
     * surface inside a present Optional. Use {@link #queryForSingleValue(String, Bson, Class)} with
     * {@code Byte.class} if you need to distinguish a missing/{@code null} field from a real {@code 0}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Entity> mapper = mongoDB.collectionMapper(Entity.class);
     * OptionalByte flags = mapper.queryForByte("flags", Filters.eq("id", "789"));
     * byte f = flags.orElse((byte) 0); // present -> the field value; empty (no match) -> the 0 fallback
     * // No match yields empty:
     * OptionalByte none = mapper.queryForByte("flags", Filters.eq("id", "missing"));
     * boolean isEmpty = none.isPresent() == false; // isEmpty == true
     * }</pre>
     *
     * @param propName the name of the byte property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @return a <i>present</i> {@code OptionalByte} holding the field value (or {@code 0} for a
     *         missing/{@code null} field) when at least one document is matched;
     *         {@code OptionalByte.empty()} when no document matches
     * @throws IllegalArgumentException if propName is null or empty, or filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see OptionalByte
     * @see #queryForSingleValue(String, Bson, Class)
     */
    @Beta
    public OptionalByte queryForByte(final String propName, final Bson filter) {
        return collectionExecutor.queryForByte(propName, filter);
    }

    /**
     * Returns the value of {@code propName} from the first document matching {@code filter}, as a short value.
     *
     * <p>Only the value of {@code propName} on the first matching document is read; any remaining documents or fields are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code OptionalShort.empty()} is returned <i>only</i> when
     * no document matches the filter. If a document is matched, the returned
     * {@code OptionalShort} is <i>present</i> and holds the unboxed {@code Short} value — when the
     * field is missing or stored as BSON {@code null}, the JDBC-style primitive default {@code (short) 0}
     * may surface inside a present Optional. Use {@link #queryForSingleValue(String, Bson, Class)} with
     * {@code Short.class} if you need to distinguish a missing/{@code null} field from a real
     * {@code 0}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Product> mapper = mongoDB.collectionMapper(Product.class);
     * OptionalShort quantity = mapper.queryForShort("quantity", Filters.eq("sku", "ABC123"));
     * short q = quantity.orElse((short) 0); // present -> the field value; empty (no match) -> the 0 fallback
     * // No match yields empty:
     * OptionalShort none = mapper.queryForShort("quantity", Filters.eq("sku", "missing"));
     * boolean isEmpty = none.isPresent() == false; // isEmpty == true
     * }</pre>
     *
     * @param propName the name of the short property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @return a <i>present</i> {@code OptionalShort} holding the field value (or {@code 0} for a
     *         missing/{@code null} field) when at least one document is matched;
     *         {@code OptionalShort.empty()} when no document matches
     * @throws IllegalArgumentException if propName is null or empty, or filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see OptionalShort
     * @see #queryForSingleValue(String, Bson, Class)
     */
    @Beta
    public OptionalShort queryForShort(final String propName, final Bson filter) {
        return collectionExecutor.queryForShort(propName, filter);
    }

    /**
     * Returns the value of {@code propName} from the first document matching {@code filter}, as an int value.
     *
     * <p>Only the value of {@code propName} on the first matching document is read; any remaining documents or fields are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code OptionalInt.empty()} is returned <i>only</i> when
     * no document matches the filter. If a document is matched, the returned
     * {@code OptionalInt} is <i>present</i> and holds the unboxed {@code Integer} value — when the
     * field is missing or stored as BSON {@code null}, the JDBC-style primitive default {@code 0} may
     * surface inside a present Optional. Use {@link #queryForSingleValue(String, Bson, Class)} with
     * {@code Integer.class} if you need to distinguish a missing/{@code null} field from a real
     * {@code 0}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collectionMapper(User.class);
     * OptionalInt age = mapper.queryForInt("age", Filters.eq("userId", "user123"));
     * int years = age.orElse(0); // present -> the field value; empty (no match) -> the 0 fallback
     * // No match yields empty:
     * OptionalInt none = mapper.queryForInt("age", Filters.eq("userId", "missing"));
     * boolean isEmpty = none.isPresent() == false; // isEmpty == true
     * }</pre>
     *
     * @param propName the name of the integer property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @return a <i>present</i> {@code OptionalInt} holding the field value (or {@code 0} for a
     *         missing/{@code null} field) when at least one document is matched;
     *         {@code OptionalInt.empty()} when no document matches
     * @throws IllegalArgumentException if propName is null or empty, or filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see OptionalInt
     * @see #queryForSingleValue(String, Bson, Class)
     */
    @Beta
    public OptionalInt queryForInt(final String propName, final Bson filter) {
        return collectionExecutor.queryForInt(propName, filter);
    }

    /**
     * Returns the value of {@code propName} from the first document matching {@code filter}, as a long value.
     *
     * <p>Only the value of {@code propName} on the first matching document is read; any remaining documents or fields are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code OptionalLong.empty()} is returned <i>only</i> when
     * no document matches the filter. If a document is matched, the returned
     * {@code OptionalLong} is <i>present</i> and holds the unboxed {@code Long} value — when the field
     * is missing or stored as BSON {@code null}, the JDBC-style primitive default {@code 0L} may
     * surface inside a present Optional. Use {@link #queryForSingleValue(String, Bson, Class)} with
     * {@code Long.class} if you need to distinguish a missing/{@code null} field from a real
     * {@code 0L}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Transaction> mapper = mongoDB.collectionMapper(Transaction.class);
     * OptionalLong timestamp = mapper.queryForLong("timestamp", Filters.eq("txnId", "TXN789"));
     * long epoch = timestamp.orElse(0L); // present -> the field value; empty (no match) -> the 0L fallback
     * // No match yields empty:
     * OptionalLong none = mapper.queryForLong("timestamp", Filters.eq("txnId", "missing"));
     * boolean isEmpty = none.isPresent() == false; // isEmpty == true
     * }</pre>
     *
     * @param propName the name of the long property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @return a <i>present</i> {@code OptionalLong} holding the field value (or {@code 0L} for a
     *         missing/{@code null} field) when at least one document is matched;
     *         {@code OptionalLong.empty()} when no document matches
     * @throws IllegalArgumentException if propName is null or empty, or filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see OptionalLong
     * @see #queryForSingleValue(String, Bson, Class)
     */
    @Beta
    public OptionalLong queryForLong(final String propName, final Bson filter) {
        return collectionExecutor.queryForLong(propName, filter);
    }

    /**
     * Returns the value of {@code propName} from the first document matching {@code filter}, as a float value.
     *
     * <p>Only the value of {@code propName} on the first matching document is read; any remaining documents or fields are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code OptionalFloat.empty()} is returned <i>only</i> when
     * no document matches the filter. If a document is matched, the returned
     * {@code OptionalFloat} is <i>present</i> and holds the unboxed {@code Float} value — when the
     * field is missing or stored as BSON {@code null}, the JDBC-style primitive default {@code 0.0f}
     * may surface inside a present Optional. Use {@link #queryForSingleValue(String, Bson, Class)} with
     * {@code Float.class} if you need to distinguish a missing/{@code null} field from a real
     * {@code 0.0f}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Product> mapper = mongoDB.collectionMapper(Product.class);
     * OptionalFloat rating = mapper.queryForFloat("rating", Filters.eq("productId", "PROD456"));
     * float r = rating.orElse(0.0f); // present -> the field value; empty (no match) -> the 0.0f fallback
     * // No match yields empty:
     * OptionalFloat none = mapper.queryForFloat("rating", Filters.eq("productId", "missing"));
     * boolean isEmpty = none.isPresent() == false; // isEmpty == true
     * }</pre>
     *
     * @param propName the name of the float property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @return a <i>present</i> {@code OptionalFloat} holding the field value (or {@code 0.0f} for a
     *         missing/{@code null} field) when at least one document is matched;
     *         {@code OptionalFloat.empty()} when no document matches
     * @throws IllegalArgumentException if propName is null or empty, or filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see OptionalFloat
     * @see #queryForSingleValue(String, Bson, Class)
     */
    @Beta
    public OptionalFloat queryForFloat(final String propName, final Bson filter) {
        return collectionExecutor.queryForFloat(propName, filter);
    }

    /**
     * Returns the value of {@code propName} from the first document matching {@code filter}, as a double value.
     *
     * <p>Only the value of {@code propName} on the first matching document is read; any remaining documents or fields are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code OptionalDouble.empty()} is returned <i>only</i> when
     * no document matches the filter. If a document is matched, the returned
     * {@code OptionalDouble} is <i>present</i> and holds the unboxed {@code Double} value — when the
     * field is missing or stored as BSON {@code null}, the JDBC-style primitive default {@code 0.0d}
     * may surface inside a present Optional. Use {@link #queryForSingleValue(String, Bson, Class)} with
     * {@code Double.class} if you need to distinguish a missing/{@code null} field from a real
     * {@code 0.0d}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Order> mapper = mongoDB.collectionMapper(Order.class);
     * OptionalDouble total = mapper.queryForDouble("totalAmount", Filters.eq("orderId", "ORD123"));
     * double amount = total.orElse(0.0d); // present -> the field value; empty (no match) -> the 0.0d fallback
     * // No match yields empty:
     * OptionalDouble none = mapper.queryForDouble("totalAmount", Filters.eq("orderId", "missing"));
     * boolean isEmpty = none.isPresent() == false; // isEmpty == true
     * }</pre>
     *
     * @param propName the name of the double property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @return a <i>present</i> {@code OptionalDouble} holding the field value (or {@code 0.0d} for a
     *         missing/{@code null} field) when at least one document is matched;
     *         {@code OptionalDouble.empty()} when no document matches
     * @throws IllegalArgumentException if propName is null or empty, or filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see OptionalDouble
     * @see #queryForSingleValue(String, Bson, Class)
     */
    @Beta
    public OptionalDouble queryForDouble(final String propName, final Bson filter) {
        return collectionExecutor.queryForDouble(propName, filter);
    }

    /**
     * Returns the value of {@code propName} from the first document matching {@code filter}, as a String value.
     *
     * <p>Only the value of {@code propName} on the first matching document is read; any remaining documents or fields are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code Nullable.empty()} is returned <i>only</i> when no
     * document matches the filter. If a document is matched, but the field is absent on the matched
     * document or the stored value is BSON {@code null}, the returned {@code Nullable} is
     * <i>present-but-null</i> ({@code Nullable.of(null)}). {@link Nullable}
     * preserves this distinction: callers can use {@link Nullable#isPresent()} to check for "document
     * found" and {@link Nullable#isNotNull()} (or {@link Nullable#orElse(Object) orElse(...)}) to check
     * for a non-null value.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collectionMapper(User.class);
     * Nullable<String> email = mapper.queryForString("email", Filters.eq("userId", "user456"));
     * String addr = email.orElse("unknown@example.com"); // value if non-null; fallback if empty OR present-but-null
     * // No match yields empty; a matched doc with a null/absent "email" yields present-but-null:
     * Nullable<String> none = mapper.queryForString("email", Filters.eq("userId", "missing"));
     * boolean isEmpty = none.isPresent() == false; // isEmpty == true (no document matched)
     * }</pre>
     *
     * @param propName the name of the string property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @return a <i>present</i> {@code Nullable<String>} holding the field value (possibly {@code null}
     *         for a missing/{@code null} field) when at least one document is matched;
     *         {@code Nullable.empty()} when no document matches
     * @throws IllegalArgumentException if propName is null or empty, or filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see Nullable
     * @see #queryForSingleValue(String, Bson, Class)
     */
    @Beta
    public Nullable<String> queryForString(final String propName, final Bson filter) {
        return collectionExecutor.queryForString(propName, filter);
    }

    /**
     * Returns the value of {@code propName} from the first document matching {@code filter}, as a Date value.
     *
     * <p>Only the value of {@code propName} on the first matching document is read; any remaining documents or fields are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code Nullable.empty()} is returned <i>only</i> when no
     * document matches the filter. If a document is matched, but the field is absent on the matched
     * document or the stored value is BSON {@code null}, the returned {@code Nullable} is
     * <i>present-but-null</i> ({@code Nullable.of(null)}), preserving the distinction between
     * "no document matched" and "document matched but value is null".</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Event> mapper = mongoDB.collectionMapper(Event.class);
     * Nullable<Date> startDate = mapper.queryForDate("startDate", Filters.eq("eventId", "EVT789"));
     * Date d = startDate.orElse(null); // value if present-and-non-null; null if empty OR present-but-null
     * // No match yields empty:
     * Nullable<Date> none = mapper.queryForDate("startDate", Filters.eq("eventId", "missing"));
     * boolean isEmpty = none.isPresent() == false; // isEmpty == true
     * }</pre>
     *
     * @param propName the name of the date property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @return a <i>present</i> {@code Nullable<Date>} holding the field value (possibly {@code null}
     *         for a missing/{@code null} field) when at least one document is matched;
     *         {@code Nullable.empty()} when no document matches
     * @throws IllegalArgumentException if propName is null or empty, or filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see Nullable
     * @see Date
     * @see #queryForSingleValue(String, Bson, Class)
     */
    @Beta
    public Nullable<Date> queryForDate(final String propName, final Bson filter) {
        return collectionExecutor.queryForDate(propName, filter);
    }

    /**
     * Returns the value of {@code propName} from the first document matching {@code filter}, as the specified Date subtype.
     *
     * <p>Only the value of {@code propName} on the first matching document is read; any remaining
     * documents or fields are ignored. The retrieved value is converted to the supplied
     * {@code valueType} (e.g. {@link java.sql.Timestamp}, {@link java.sql.Date}).</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code Nullable.empty()} is returned <i>only</i> when no
     * document matches the filter. If a document is matched, but the field is absent on the matched
     * document or the stored value is BSON {@code null}, the returned {@code Nullable} is
     * <i>present-but-null</i> ({@code Nullable.of(null)}), preserving the distinction between
     * "no document matched" and "document matched but value is null".</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Task> mapper = mongoDB.collectionMapper(Task.class);
     * // The stored date is converted to the requested Date subtype (here java.sql.Timestamp):
     * Nullable<Timestamp> created = mapper.queryForDate("createdAt",
     *     Filters.eq("taskId", "TASK123"), Timestamp.class);
     * Timestamp t = created.orElse(null); // value if present-and-non-null; null if empty OR present-but-null
     * // No match yields empty:
     * Nullable<Timestamp> none = mapper.queryForDate("createdAt", Filters.eq("taskId", "missing"), Timestamp.class);
     * boolean isEmpty = none.isPresent() == false; // isEmpty == true
     * }</pre>
     *
     * @param <P> the specific Date subtype to return
     * @param propName the name of the date property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @param valueType the class of the Date subtype to convert to
     * @return a <i>present</i> {@code Nullable<P>} holding the field value (possibly {@code null} for
     *         a missing or BSON {@code null} field) when at least one document is matched;
     *         {@code Nullable.empty()} when no document matches
     * @throws IllegalArgumentException if {@code propName} is null or empty, {@code filter} is null, or {@code valueType} is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see Nullable
     * @see Date
     * @see #queryForSingleValue(String, Bson, Class)
     */
    public <P extends Date> Nullable<P> queryForDate(final String propName, final Bson filter, final Class<P> valueType) {
        return collectionExecutor.queryForDate(propName, filter, valueType);
    }

    /**
     * Returns the value of {@code propName} from the first document matching {@code filter}, as the specified type.
     *
     * <p>Only the value of {@code propName} on the first matching document is read; any remaining documents or fields are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code Nullable.empty()} is returned <i>only</i> when no
     * document matches the filter. If a document is matched, but the field is absent on the matched
     * document or the stored value is BSON {@code null}, the returned {@code Nullable} is
     * <i>present-but-null</i> ({@code Nullable.of(null)}). {@link Nullable}
     * preserves the distinction between "no document matched" and "document matched but value is null".
     * Unlike the primitive {@code queryForXxx} variants (which surface a missing/{@code null} field as
     * the JDBC primitive default value wrapped in a present Optional), this overload — driven by a
     * wrapper / object {@code Class<V>} — always conveys missing or BSON {@code null} fields precisely
     * as Java {@code null} inside the Nullable.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<Product> mapper = mongoDB.collectionMapper(Product.class);
     * Nullable<BigDecimal> price = mapper.queryForSingleValue("price",
     *     Filters.eq("productId", "PROD999"), BigDecimal.class);
     * BigDecimal value = price.orElse(BigDecimal.ZERO); // value if present-and-non-null; ZERO otherwise
     * // No match yields empty; a matched doc whose "price" is null/absent yields present-but-null:
     * Nullable<BigDecimal> none = mapper.queryForSingleValue("price",
     *     Filters.eq("productId", "missing"), BigDecimal.class);
     * boolean isEmpty = none.isPresent() == false; // isEmpty == true (no document matched)
     * }</pre>
     *
     * @param <V> the type to convert the property value to
     * @param propName the name of the property to retrieve
     * @param filter the query filter to match documents against (must not be null)
     * @param valueType the class of the type to convert to
     * @return a <i>present</i> {@code Nullable<V>} holding the field value (possibly {@code null} for
     *         a missing or BSON {@code null} field) when at least one document is matched;
     *         {@code Nullable.empty()} when no document matches
     * @throws IllegalArgumentException if {@code propName} is null or empty, {@code filter} is null, or {@code valueType} is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see Nullable
     */
    public <V> Nullable<V> queryForSingleValue(final String propName, final Bson filter, final Class<V> valueType) {
        return collectionExecutor.queryForSingleValue(propName, filter, valueType);
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
     * MongoCollectionMapper<Sales> mapper = mongoDB.collectionMapper(Sales.class);
     * Dataset ds = mapper.query(Filters.gte("amount", 1000)); // returns a Dataset (one row per matching doc)
     * int rows = ds.size();                                   // number of matching documents
     * // No match yields a Dataset with zero rows (never null):
     * Dataset none = mapper.query(Filters.gte("amount", Long.MAX_VALUE)); // none.size() == 0
     * }</pre>
     *
     * @param filter the query filter to match entities against
     * @return a Dataset containing the query results
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoException if the database operation fails
     * @see Dataset
     */
    public Dataset query(final Bson filter) {
        return collectionExecutor.query(filter, rowType);
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
     * MongoCollectionMapper<Sales> mapper = mongoDB.collectionMapper(Sales.class);
     * Dataset page1 = mapper.query(Filters.gte("amount", 1000), 0, 100);   // returns up to 100 rows (first page)
     * Dataset page2 = mapper.query(Filters.gte("amount", 1000), 100, 100); // the next 100 (skip 100, limit 100)
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
        return collectionExecutor.query(filter, offset, count, rowType);
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
     * MongoCollectionMapper<Customer> mapper = mongoDB.collectionMapper(Customer.class);
     * Collection<String> fields = Arrays.asList("name", "email", "city");
     * // The Dataset has one column per selected field (plus _id):
     * Dataset ds = mapper.query(fields, Filters.eq("status", "active")); // returns a Dataset, possibly 0 rows
     * List<String> columns = ds.columnNames();
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
        return collectionExecutor.query(selectPropNames, filter, rowType);
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
     * Dataset orders = mapper.query(fields, Filters.gte("total", 1000), 20, 10);   // returns up to 10 rows (skip 20)
     * Dataset firstPage = mapper.query(fields, Filters.gte("total", 1000), 0, 10); // the first 10 instead
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
        return collectionExecutor.query(selectPropNames, filter, offset, count, rowType);
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
     * // Highest score first:
     * Dataset topPlayers = mapper.query(fields, Filters.exists("score"), sort);                    // returns a sorted Dataset
     * Dataset lowToHigh = mapper.query(fields, Filters.exists("score"), Sorts.ascending("score")); // reversed
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
        return collectionExecutor.query(selectPropNames, filter, sort, rowType);
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
     * // Best-rated (cheapest as tie-breaker), first page of 20:
     * Dataset products = mapper.query(fields, Filters.gte("rating", 4.0), sort, 0, 20);  // returns up to 20 rows
     * Dataset nextPage = mapper.query(fields, Filters.gte("rating", 4.0), sort, 20, 20); // the next 20
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
        return collectionExecutor.query(selectPropNames, filter, sort, offset, count, rowType);
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
     *     Projections.excludeId()   // drop the _id column entirely
     * );
     * // Name + address columns, _id excluded, sorted by name:
     * Dataset results = mapper.query(projection, Filters.eq("city", "NYC"), Sorts.ascending("name")); // returns Dataset
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
        return collectionExecutor.query(projection, filter, sort, rowType);
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
     * // Computed itemCount column, newest first, first page of 50:
     * Dataset orders = mapper.query(projection, Filters.gte("date", startDate),
     *                               Sorts.descending("date"), 0, 50); // returns up to 50 rows
     * Dataset nextPage = mapper.query(projection, Filters.gte("date", startDate),
     *                                 Sorts.descending("date"), 50, 50); // the next 50
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
        return collectionExecutor.query(projection, filter, sort, offset, count, rowType);
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
     * MongoCollectionMapper<User> mapper = mongoDB.collectionMapper(User.class);
     * 
     * // Stream active users for processing (close it to free the underlying cursor):
     * try (Stream<User> userStream = mapper.stream(Filters.eq("status", "active"))) { // returns a lazy Stream
     *     userStream
     *         .filter(user -> user.getAge() >= 18)
     *         .forEach(user -> processUser(user));
     * }
     *
     * // A JSON filter must be a Bson (Document.parse), not a raw String:
     * try (Stream<User> recentUsers = mapper.stream(
     *         Document.parse("{ createdAt: { $gte: ISODate('2023-01-01') } }"))) {
     *     long count = recentUsers.count(); // terminal op; 0 when nothing matches
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
        return collectionExecutor.stream(filter, rowType);
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
     * MongoCollectionMapper<Order> mapper = mongoDB.collectionMapper(Order.class);
     * Bson filter = Filters.gte("orderDate", LocalDate.now().minusDays(7));
     * 
     * // Process the second batch of recent orders (rows 101-200):
     * try (Stream<Order> orderStream = mapper.stream(filter, 100, 100)) { // returns a lazy Stream, skip 100 limit 100
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
        return collectionExecutor.stream(filter, offset, count, rowType);
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
     * MongoCollectionMapper<User> mapper = mongoDB.collectionMapper(User.class);
     * Collection<String> fields = Arrays.asList("email", "preferences.newsletter");
     * Bson filter = Filters.eq("preferences.newsletter", true);
     * 
     * // Stream newsletter subscribers with only the projected fields populated:
     * try (Stream<User> subscriberStream = mapper.stream(fields, filter)) { // returns a lazy Stream
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
        return collectionExecutor.stream(selectPropNames, filter, rowType);
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
     * MongoCollectionMapper<Product> mapper = mongoDB.collectionMapper(Product.class);
     * Collection<String> fields = Arrays.asList("name", "price", "category");
     * Bson filter = Filters.lt("price", 100.0);
     * 
     * // Process affordable products, first 500 only:
     * try (Stream<Product> productStream = mapper.stream(fields, filter, 0, 500)) { // returns a lazy Stream, limit 500
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
        return collectionExecutor.stream(selectPropNames, filter, offset, count, rowType);
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
     * MongoCollectionMapper<Article> mapper = mongoDB.collectionMapper(Article.class);
     * Collection<String> fields = Arrays.asList("title", "publishedAt", "viewCount");
     * Bson filter = Filters.eq("status", "published");
     * Bson sort = Sorts.descending("publishedAt");   // Latest first
     * 
     * // Stream articles newest-first, take the top 50:
     * try (Stream<Article> articleStream = mapper.stream(fields, filter, sort)) { // returns a lazy, sorted Stream
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
        return collectionExecutor.stream(selectPropNames, filter, sort, rowType);
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
     * MongoCollectionMapper<Transaction> mapper = mongoDB.collectionMapper(Transaction.class);
     * Collection<String> fields = Arrays.asList("amount", "timestamp", "accountId");
     * Bson filter = Filters.gte("amount", 1000.0);   // High-value transactions
     * Bson sort = Sorts.descending("timestamp");     // Most recent first
     * 
     * // Process recent high-value transactions in batches:
     * int batchSize = 100;
     * for (int page = 0; page < 10; page++) {
     *     try (Stream<Transaction> txnStream = mapper.stream(
     *             fields, filter, sort, page * batchSize, batchSize)) { // returns a lazy, sorted Stream per page
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
        return collectionExecutor.stream(selectPropNames, filter, sort, offset, count, rowType);
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
     * MongoCollectionMapper<Order> mapper = mongoDB.collectionMapper(Order.class);
     * 
     * // Complex projection with computed total:
     * Bson projection = Projections.fields(
     *     Projections.include("customerId", "items"),
     *     Projections.computed("total", new Document("$sum", "$items.price"))
     * );
     * Bson filter = Filters.gte("orderDate", LocalDate.now().minusMonths(1));
     * Bson sort = Sorts.descending("total");   // Highest value first
     * 
     * // Stream high-value recent orders, with a computed total per order:
     * try (Stream<Order> orderStream = mapper.stream(projection, filter, sort)) { // returns a lazy, sorted Stream
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
        return collectionExecutor.stream(projection, filter, sort, rowType);
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
     * MongoCollectionMapper<Analytics> mapper = mongoDB.collectionMapper(Analytics.class);
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
     * // Process the top 100 performers by conversion rate:
     * try (Stream<Analytics> analyticsStream = mapper.stream(
     *         projection, filter, sort, 0, 100)) { // returns a lazy, sorted Stream, limit 100
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
        return collectionExecutor.stream(projection, filter, sort, offset, count, rowType);
    }

    /**
     * Inserts a single entity into the collection (blocking operation).
     *
     * <p>This method inserts a single entity of the mapped type into the collection. The entity
     * is automatically converted to a BSON document using the configured codec registry, handling
     * ID field mapping and type conversions transparently.</p>
     *
     * <p><b>Note:</b> This method performs a blocking operation. For non-blocking operations, use
     * the underlying executor's async methods via {@link #mongoCollectionExecutor()}.{@code async()}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collectionMapper(User.class);
     *
     * User newUser = new User("John Doe", "john@example.com", 30);
     * mapper.insertOne(newUser);            // void; the server-assigned _id is NOT written back into the entity
     * String assignedId = newUser.getId();  // post-state: still null — the generated _id exists only in the database
     *
     * mapper.insertOne((User) null);        // throws IllegalArgumentException
     * }</pre>
     *
     * @param obj the entity to insert
     * @throws IllegalArgumentException if obj is null
     * @throws com.mongodb.MongoWriteException if the insert operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #insertOne(Object, InsertOneOptions)
     * @see #insertMany(Collection)
     * @see #mongoCollectionExecutor()
     */
    public void insertOne(final T obj) {
        collectionExecutor.insertOne(obj);
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
     * MongoCollectionMapper<User> mapper = mongoDB.collectionMapper(User.class);
     * User newUser = new User("Jane Smith", "jane@example.com", 25);
     *
     * InsertOneOptions options = new InsertOneOptions()
     *     .bypassDocumentValidation(false);
     * mapper.insertOne(newUser, options);  // void; the server-assigned _id is NOT written back into the entity
     * String assignedId = newUser.getId(); // post-state: still null — the generated _id exists only in the database
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
        collectionExecutor.insertOne(obj, options);
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
     * MongoCollectionMapper<User> mapper = mongoDB.collectionMapper(User.class);
     *
     * List<User> users = Arrays.asList(
     *     new User("John", "john@example.com", 30),
     *     new User("Jane", "jane@example.com", 25),
     *     new User("Bob", "bob@example.com", 35)
     * );
     *
     * mapper.insertMany(users);                                             // void; server-assigned _ids are NOT written back into the entities
     * boolean anyHasId = users.stream().anyMatch(u -> u.getId() != null);   // post-state: anyHasId == false — generated _ids exist only in the database
     *
     * mapper.insertMany(Collections.emptyList()); // throws IllegalArgumentException: empty list
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
        collectionExecutor.insertMany(objList);
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
     *     .ordered(false)  // unordered: attempt every insert, report errors at the end
     *     .bypassDocumentValidation(true);
     * mapper.insertMany(products, options); // void; generated _ids exist only in the database, not in the entities
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
        collectionExecutor.insertMany(objList, options);
    }

    /**
     * Updates the document whose {@code _id} matches the supplied 24-hex-character ObjectId string,
     * applying the fields of {@code update} via the {@code $set} operator.
     *
     * <p>If {@code update} is already a driver-built {@link Bson} update expression
     * (for example {@code Updates.set(...)}/{@code Updates.combine(...)}) it is used verbatim.
     * Otherwise the entity is converted to a {@link Document} via {@link MongoDBBase#toDocument(Object)}
     * and wrapped in {@code {$set: ...}}; properties whose getter returns {@code null} are
     * <i>dropped</i> by the bean conversion, so entity nulls cannot be used to clear fields through
     * this path. Use a driver-built {@link Bson} update (e.g. {@code Updates.set(field, null)})
     * when you need to explicitly write {@code null}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MongoCollectionMapper<User> mapper = mongoDB.collectionMapper(User.class);
     * String userId = "507f1f77bcf86cd799439011";
     *
     * User updateData = new User();
     * updateData.setStatus("inactive");
     * updateData.setLastSeen(new Date());
     *
     * UpdateResult result = mapper.updateOne(userId, updateData); // returns UpdateResult; never null
     * long modified = result.getModifiedCount();                  // 1 if the doc existed and changed, else 0
     *
     * mapper.updateOne("not-a-hex-id", updateData);               // throws IllegalArgumentException
     * }</pre>
     *
     * @param objectId the 24-hex-character ObjectId string identifying the entity to update
     * @param update the entity (or driver {@link Bson} update expression) containing the update data
     * @return UpdateResult containing information about the update operation
     * @throws IllegalArgumentException if {@code objectId} is null, empty, or not a valid hex ObjectId, or {@code update} is null
     * @throws com.mongodb.MongoWriteException if the update operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see UpdateResult
     * @see #updateOne(ObjectId, Object)
     */
    public UpdateResult updateOne(final String objectId, final T update) {
        return collectionExecutor.updateOne(objectId, update);
    }

    /**
     * Updates a single entity identified by ObjectId with the specified update operations.
     * 
     * <p>This method updates a single entity matching the provided ObjectId. The update entity
     * contains the fields to modify; the entity is converted to a {@code $set} update (null-valued
     * properties are dropped); see {@link #updateOne(String, Object)}.
     * This is the most direct way to update a known entity by its database identifier.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
     * User updates = new User();
     * updates.setLastLogin(new Date());
     * UpdateResult result = mapper.updateOne(id, updates); // returns UpdateResult; never null
     * long matched = result.getMatchedCount();             // 1 if the doc existed, else 0 (no upsert)
     * }</pre>
     *
     * @param objectId the ObjectId identifying the entity to update
     * @param update the entity, or a driver-built {@link Bson} update expression, containing the update data
     * @return UpdateResult containing information about the update operation
     * @throws IllegalArgumentException if objectId or update is null
     * @throws com.mongodb.MongoWriteException if the update operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #updateOne(String, Object)
     * @see UpdateResult
     */
    public UpdateResult updateOne(final ObjectId objectId, final T update) {
        return collectionExecutor.updateOne(objectId, update);
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
     * // Only the FIRST matching document is updated, even if several match:
     * UpdateResult result = mapper.updateOne(
     *     Filters.eq("email", "user@example.com"), updates); // returns UpdateResult; getModifiedCount() in {0,1}
     * }</pre>
     *
     * @param filter the query filter to match the entity to update
     * @param update the entity, or a driver-built {@link Bson} update expression, containing the update data
     * @return UpdateResult containing information about the update operation
     * @throws IllegalArgumentException if filter or update is null
     * @throws com.mongodb.MongoWriteException if the update operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #updateMany(Bson, Object)
     * @see UpdateResult
     */
    public UpdateResult updateOne(final Bson filter, final T update) {
        return collectionExecutor.updateOne(filter, update);
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
     *     .upsert(true)  // insert a new doc if nothing matches
     *     .bypassDocumentValidation(false);
     * User updates = new User();
     * updates.setActive(true);
     * UpdateResult result = mapper.updateOne(
     *     Filters.eq("username", "newuser"), updates, options); // returns UpdateResult
     * // With upsert and no prior match, getUpsertedId() is non-null and getMatchedCount() == 0:
     * Object upsertedId = result.getUpsertedId();
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
        return collectionExecutor.updateOne(filter, update, options);
    }

    /**
     * Updates a single entity using multiple update entities as a pipeline.
     * 
     * <p>This method applies multiple update entities in sequence as an aggregation pipeline
     * update. Each supplied item becomes a pipeline stage; plain entities contribute literal field
     * assignments, while driver-built {@link Bson} stages may reference existing values, compute, or
     * conditionally modify documents.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<User> updatePipeline = Arrays.asList(
     *     new User().setProcessed(true),
     *     new User().setUpdatedAt(new Date())
     * );
     * UpdateResult result = mapper.updateOne(
     *     Filters.eq("status", "pending"), updatePipeline); // returns UpdateResult; getModifiedCount() in {0,1}
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
        return collectionExecutor.updateOne(filter, objList);
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
     *     Filters.eq("sku", "PROD-123"), pipeline, options); // returns UpdateResult; getUpsertedId() set on insert
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
        return collectionExecutor.updateOne(filter, objList, options);
    }

    /**
     * Updates all entities matching the filter with the specified update operations (blocking operation).
     *
     * <p>This method updates all entities that match the provided filter criteria. Unlike updateOne,
     * this operation modifies every matching document in the collection. Use with caution on
     * filters that may match large numbers of documents.</p>
     *
     * <p><b>Note:</b> This method performs a blocking operation. For non-blocking operations, use
     * {@link #mongoCollectionExecutor()}.{@code async()}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User updates = new User();
     * updates.setNewsletterOptIn(false);
     * // EVERY matching document is updated (contrast with updateOne):
     * UpdateResult result = mapper.updateMany(
     *     Filters.eq("country", "EU"), updates); // returns UpdateResult; getModifiedCount() may be > 1
     * long updated = result.getModifiedCount();  // e.g. 250; 0 when nothing matched
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
     * @see #mongoCollectionExecutor()
     */
    public UpdateResult updateMany(final Bson filter, final T update) {
        return collectionExecutor.updateMany(filter, update);
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
     *     Filters.lt("stock", 10), updates, options); // returns UpdateResult; getModifiedCount() may be > 1
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
        return collectionExecutor.updateMany(filter, update, options);
    }

    /**
     * Updates all entities matching the filter using multiple update entities as a pipeline.
     * 
     * <p>This method applies a pipeline of update operations to all matching documents.
     * Each supplied item becomes a pipeline stage; plain entities contribute literal field
     * assignments, while driver-built {@link Bson} stages may reference existing values, compute, or
     * conditionally modify documents.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Order> pipeline = Arrays.asList(
     *     new Order().setProcessed(true),
     *     new Order().setProcessedDate(new Date())
     * );
     * UpdateResult result = mapper.updateMany(
     *     Filters.eq("status", "pending"), pipeline); // returns UpdateResult; getModifiedCount() may be > 1
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
        return collectionExecutor.updateMany(filter, objList);
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
     *     Filters.regex("name", "^Corp"), pipeline, options); // returns UpdateResult; getModifiedCount() may be > 1
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
        return collectionExecutor.updateMany(filter, objList, options);
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
     * // Replaces the whole document (keeping _id); fields absent from newUserData are removed:
     * UpdateResult result = mapper.replaceOne(userId, newUserData); // returns UpdateResult; never null
     * long modified = result.getModifiedCount();                    // 1 if the doc existed, else 0
     *
     * mapper.replaceOne("bad-id", newUserData);                     // throws IllegalArgumentException
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
        return collectionExecutor.replaceOne(objectId, replacement);
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
     * UpdateResult result = mapper.replaceOne(id, newProduct); // returns UpdateResult; never null
     * long modified = result.getModifiedCount();               // 1 if the doc existed, else 0
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
        return collectionExecutor.replaceOne(objectId, replacement);
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
     * // Replaces only the FIRST match; no upsert without ReplaceOptions:
     * UpdateResult result = mapper.replaceOne(
     *     Filters.eq("customerId", "CUST-123"), newCustomer); // returns UpdateResult; getModifiedCount() in {0,1}
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
        return collectionExecutor.replaceOne(filter, replacement);
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
     *     .upsert(true)  // insert if no document matches
     *     .bypassDocumentValidation(false);
     * Article article = new Article(title, content, author);
     * UpdateResult result = mapper.replaceOne(
     *     Filters.eq("slug", articleSlug), article, options); // returns UpdateResult
     * // On an upsert insert, getUpsertedId() is non-null and getMatchedCount() == 0:
     * Object upsertedId = result.getUpsertedId();
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
        return collectionExecutor.replaceOne(filter, replacement, options);
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
     * DeleteResult result = mapper.deleteOne(userId); // returns DeleteResult; never null
     * long deleted = result.getDeletedCount();        // 1 if a doc was deleted, else 0
     *
     * mapper.deleteOne("not-a-hex-id");               // throws IllegalArgumentException
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
        return collectionExecutor.deleteOne(objectId);
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
     * DeleteResult result = mapper.deleteOne(id);     // returns DeleteResult; never null
     * boolean removed = result.getDeletedCount() > 0; // true only if a matching doc existed
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
        return collectionExecutor.deleteOne(objectId);
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
     * // Only the FIRST matching document is deleted, even if many match:
     * DeleteResult result = mapper.deleteOne(
     *     Filters.and(
     *         Filters.eq("status", "inactive"),
     *         Filters.lt("lastLogin", thirtyDaysAgo)
     *     )
     * ); // returns DeleteResult; getDeletedCount() in {0,1}
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
        return collectionExecutor.deleteOne(filter);
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
     *     Filters.eq("email", "user@example.com"), options); // returns DeleteResult; getDeletedCount() in {0,1}
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
        return collectionExecutor.deleteOne(filter, options);
    }

    /**
     * Deletes all entities matching the specified filter (blocking operation).
     *
     * <p>This method deletes all entities that match the provided filter criteria.
     * Use with caution as this operation can delete large numbers of documents.
     * Consider using deleteOne for single deletions or adding specific filters to limit scope.</p>
     *
     * <p><b>Note:</b> This method performs a blocking operation. For non-blocking operations, use
     * {@link #mongoCollectionExecutor()}.{@code async()}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // EVERY matching document is deleted (contrast with deleteOne):
     * DeleteResult result = mapper.deleteMany(
     *     Filters.lt("expiryDate", new Date())
     * );                                       // returns DeleteResult; getDeletedCount() may be > 1
     * long removed = result.getDeletedCount(); // e.g. 37; 0 when nothing matched
     * }</pre>
     *
     * @param filter the query filter to match entities to delete
     * @return DeleteResult containing information about the delete operation
     * @throws IllegalArgumentException if filter is null
     * @throws com.mongodb.MongoWriteException if the delete operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #deleteOne(Bson)
     * @see DeleteResult
     * @see #mongoCollectionExecutor()
     */
    public DeleteResult deleteMany(final Bson filter) {
        return collectionExecutor.deleteMany(filter);
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
     *     ), options); // returns DeleteResult; getDeletedCount() may be > 1
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
        return collectionExecutor.deleteMany(filter, options);
    }

    /**
     * Inserts the supplied entities as a single MongoDB bulk-write of {@code InsertOneModel} entries
     * and returns the inserted count reported by the server.
     *
     * <p>Unlike {@link #insertMany(Collection)} (which returns {@code void}), this overload returns
     * the inserted count and reuses the bulk-write pathway. Per-document atomicity applies; the bulk
     * as a whole is not atomic across documents.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Product> products = loadLargeProductCatalog(); // e.g. 5000 products
     * int insertedCount = mapper.bulkInsert(products);    // returns the inserted count, e.g. 5000
     *
     * mapper.bulkInsert(Collections.emptyList());         // throws IllegalArgumentException: empty collection
     * }</pre>
     *
     * @param entities collection of entities to insert in bulk
     * @return the number of entities reported as inserted by the server
     * @throws IllegalArgumentException if entities is null or empty
     * @throws com.mongodb.MongoBulkWriteException if one or more operations fail
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #bulkInsert(Collection, BulkWriteOptions)
     * @see #insertMany(Collection)
     */
    public int bulkInsert(final Collection<? extends T> entities) {
        return collectionExecutor.bulkInsert(entities);
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
     *     .ordered(false)  // unordered: attempt every insert even if some fail
     *     .bypassDocumentValidation(true);
     * List<LogEntry> logs = collectLogEntries();
     * int count = mapper.bulkInsert(logs, options); // returns the inserted count
     * }</pre>
     *
     * @param entities collection of entities to insert in bulk
     * @param options additional options for the bulk write operation (null uses defaults)
     * @return the number of entities reported as inserted by the server
     * @throws IllegalArgumentException if entities is null or empty
     * @throws com.mongodb.MongoBulkWriteException if one or more operations fail
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #bulkInsert(Collection)
     * @see BulkWriteOptions
     */
    public int bulkInsert(final Collection<? extends T> entities, final BulkWriteOptions options) {
        return collectionExecutor.bulkInsert(entities, options);
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
     * BulkWriteResult result = mapper.bulkWrite(operations); // returns BulkWriteResult; never null
     * int inserted = result.getInsertedCount();              // per-type counts available on the result
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
        return collectionExecutor.bulkWrite(requests);
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
     *     .ordered(true)  // ordered: stop at the first failing op (earlier ops stay applied)
     *     .bypassDocumentValidation(false);
     * List<WriteModel<Document>> operations = createComplexBatch();
     * BulkWriteResult result = mapper.bulkWrite(operations, options); // returns BulkWriteResult; never null
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
        return collectionExecutor.bulkWrite(requests, options);
    }

    /**
     * Atomically finds the first document matching {@code filter}, applies the supplied update, and
     * returns the matched document as an entity. The find-and-modify is atomic on a single document.
     *
     * <p>By default the entity is returned in its <strong>pre-update</strong> state
     * (driver default {@code ReturnDocument.BEFORE}). Use
     * {@link #findOneAndUpdate(Bson, Object, FindOneAndUpdateOptions)} with
     * {@code returnDocument(ReturnDocument.AFTER)} to receive the post-update entity.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User updates = new User();
     * updates.setLoginCount(5);
     * // Returns the PRE-update entity by default (ReturnDocument.BEFORE):
     * User user = mapper.findOneAndUpdate(
     *     Filters.eq("email", "user@example.com"), updates); // returns the matched entity, or null if none matched
     * }</pre>
     *
     * @param filter the query filter to match the entity to update
     * @param update the entity containing update data, or a driver-built {@link Bson} update
     * @return the matched entity (pre-update by default), or {@code null} if no document matched
     * @throws IllegalArgumentException if filter or update is null
     * @throws com.mongodb.MongoWriteException if the operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #findOneAndUpdate(Bson, Object, FindOneAndUpdateOptions)
     */
    public T findOneAndUpdate(final Bson filter, final T update) {
        return collectionExecutor.findOneAndUpdate(filter, update, rowType);
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
     *     .returnDocument(ReturnDocument.AFTER)  // return the POST-update document instead of the default BEFORE
     *     .upsert(true)
     *     .projection(Projections.include("name", "status"));
     * User updates = new User();
     * updates.setLastSeen(new Date());
     * User updatedUser = mapper.findOneAndUpdate(
     *     Filters.eq("email", "user@example.com"), updates, options); // returns the AFTER entity, or null if no match
     * }</pre>
     *
     * @param filter the query filter to match the entity to update
     * @param update the entity, or a driver-built {@link Bson} update expression, containing the update data
     * @param options additional options for the find and update operation (null uses defaults)
     * @return the entity before or after the update (based on options), or null if not found
     * @throws IllegalArgumentException if filter or update is null
     * @throws com.mongodb.MongoWriteException if the operation fails
     * @throws com.mongodb.MongoException if the database operation fails
     * @see #findOneAndUpdate(Bson, Object)
     * @see FindOneAndUpdateOptions
     */
    public T findOneAndUpdate(final Bson filter, final T update, final FindOneAndUpdateOptions options) {
        return collectionExecutor.findOneAndUpdate(filter, update, options, rowType);
    }

    /**
     * Finds and updates a single entity atomically using a pipeline of update operations.
     *
     * <p>This method performs atomic find-and-update using an aggregation pipeline for complex
     * transformations. Each supplied item becomes a pipeline stage; plain entities contribute literal
     * field assignments, while driver-built {@link Bson} stages may reference existing values, compute,
     * or conditionally modify documents.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<User> updatePipeline = Arrays.asList(
     *     new User().setVisitCount(5),  // Increment visits
     *     new User().setLastUpdated(new Date())
     * );
     * // Pre-update entity by default (ReturnDocument.BEFORE):
     * User user = mapper.findOneAndUpdate(
     *     Filters.eq("userId", "USER123"), updatePipeline); // returns the matched entity, or null if none matched
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
        return collectionExecutor.findOneAndUpdate(filter, objList, rowType);
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
     *     .returnDocument(ReturnDocument.AFTER) // return the POST-update document
     *     .upsert(true);
     * List<Product> pipeline = createPriceUpdatePipeline();
     * Product updated = mapper.findOneAndUpdate(
     *     Filters.eq("sku", "PROD-456"), pipeline, options); // returns the AFTER entity (upsert guarantees non-null)
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
        return collectionExecutor.findOneAndUpdate(filter, objList, options, rowType);
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
     * // Returns the original (pre-replacement) entity by default:
     * User originalUser = mapper.findOneAndReplace(
     *     Filters.eq("username", "john"), replacement); // returns the original entity, or null if no match
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
        return collectionExecutor.findOneAndReplace(filter, replacement, rowType);
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
     *     .returnDocument(ReturnDocument.AFTER)  // return the replacement document, not the original
     *     .upsert(true)
     *     .projection(Projections.exclude("_id"));
     * Article newArticle = createUpdatedArticle();
     * Article result = mapper.findOneAndReplace(
     *     Filters.eq("slug", "article-slug"), newArticle, options); // returns the AFTER entity (upsert -> non-null)
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
        return collectionExecutor.findOneAndReplace(filter, replacement, options, rowType);
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
     * ); // returns the just-deleted entity, or null if nothing matched
     * if (deletedUser != null) {
     *     auditUserDeletion(deletedUser); // null guard: only the matched doc was removed
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
        return collectionExecutor.findOneAndDelete(filter, rowType);
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
     *     .sort(Sorts.ascending("createdAt"))  // among matches, delete the oldest one
     *     .projection(Projections.include("email", "name"));
     * // Only email/name (plus _id) are populated on the returned entity:
     * User deletedUser = mapper.findOneAndDelete(
     *     Filters.eq("status", "pending"), options); // returns the deleted entity, or null if nothing matched
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
        return collectionExecutor.findOneAndDelete(filter, options, rowType);
    }

    /**
     * Returns a stream of entities, each carrying one distinct value of the specified field across all entities.
     *
     * <p>This method streams all unique values for the specified field name across the entire
     * collection. Each distinct value is surfaced under {@code fieldName} on an entity of the mapped
     * type, and is only readable if the entity declares a matching property. This is useful for getting
     * distinct field values for analysis or dropdown populations.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Stream the unique "category" values across the whole collection:
     * try (Stream<Product> categoryStream = mapper.distinct("category")) { // returns a lazy Stream of distinct values
     *     List<String> categories = categoryStream
     *         .map(Product::getCategory)
     *         .toList();
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
        N.checkArgNotEmpty(fieldName, "fieldName");

        return collectionExecutor.aggregate(distinctPipeline(fieldName, null), rowType);
    }

    // Routes distinct through a $group/$project pipeline (like groupBy) so each distinct scalar value
    // comes back as a {fieldName: value} document decodable into the mapped entity type, as documented.
    // The driver's native distinct(fieldName, entityClass) decodes each raw VALUE with the entity codec
    // and throws BsonInvalidOperationException for any scalar field.
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
     * Returns a stream of entities, each carrying one distinct value of the specified field from matching entities.
     *
     * <p>This method streams unique values for the specified field from entities that match
     * the filter criteria. Each distinct value is surfaced under {@code fieldName} on an entity of the
     * mapped type, and is only readable if the entity declares a matching property. This is useful for
     * getting distinct values from a subset of the collection based on specific conditions.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Distinct "category" values, but only among active products:
     * try (Stream<Product> stream = mapper.distinct("category",
     *         Filters.eq("active", true))) { // returns a lazy Stream of distinct values from matches only
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
        N.checkArgNotEmpty(fieldName, "fieldName");
        N.checkArgNotNull(filter, "filter");

        return collectionExecutor.aggregate(distinctPipeline(fieldName, filter), rowType);
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
     * try (Stream<Product> results = mapper.aggregate(pipeline)) { // returns a lazy Stream of pipeline output docs
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
        return collectionExecutor.aggregate(pipeline, rowType);
    }

    /**
     * Returns a stream of entities, one per group, grouped by the specified field.
     *
     * <p><strong>Beta Feature:</strong> This method is experimental and may change in future versions.</p>
     *
     * <p>This method groups entities by a single field value, useful for basic grouping operations.
     * The grouped results are returned as a stream of entities representing each group.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // One entity per distinct "department" value:
     * try (Stream<User> groupedStream = mapper.groupBy("department")) { // returns a lazy Stream, one row per group
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
        return collectionExecutor.groupBy(fieldName, rowType);
    }

    /**
     * Returns a stream of entities, one per group, grouped by multiple fields.
     *
     * <p><strong>Beta Feature:</strong> This method is experimental and may change in future versions.</p>
     *
     * <p>This method groups entities by multiple field values, enabling compound grouping
     * for more complex analysis. Each group represents a unique combination of the specified fields.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> groupFields = Arrays.asList("department", "level");
     * // One entity per unique (department, level) combination:
     * try (Stream<Employee> groups = mapper.groupBy(groupFields)) { // returns a lazy Stream, one row per combination
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
        return collectionExecutor.groupBy(fieldNames, rowType);
    }

    /**
     * Returns a stream of entities, one per group, grouped by the specified field, each carrying its count.
     *
     * <p><strong>Beta Feature:</strong> This method is experimental and may change in future versions.</p>
     *
     * <p>This method groups entities by a field value and includes the count of entities in each group.
     * The count surfaces under a {@code count} field on the entity and is only readable if the entity
     * declares a matching property. This is useful for generating summary statistics and understanding
     * data distribution.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // One entity per "status" group, each carrying its member count:
     * try (Stream<Order> countedGroups = mapper.groupByAndCount("status")) { // returns a lazy Stream, one row per group
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
        return collectionExecutor.groupByAndCount(fieldName, rowType);
    }

    /**
     * Returns a stream of entities, one per group, grouped by multiple fields, each carrying its count.
     *
     * <p><strong>Beta Feature:</strong> This method is experimental and may change in future versions.</p>
     *
     * <p>This method groups entities by multiple field values and includes the count of entities
     * in each group. The count surfaces under a {@code count} field on the entity and is only readable
     * if the entity declares a matching property. This enables multi-dimensional analysis with count
     * statistics for complex data aggregation scenarios.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("region", "productType");
     * // One entity per (region, productType) combination, each carrying its count:
     * try (Stream<Sale> stats = mapper.groupByAndCount(fields)) { // returns a lazy Stream, one row per combination
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
        return collectionExecutor.groupByAndCount(fieldNames, rowType);
    }

    /**
     * Executes a MapReduce operation and returns a stream of entities representing the results.
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
     * // Prefer the aggregate pipeline in new code; mapReduce is retained for legacy scripts:
     * try (Stream<Product> results = mapper.mapReduce(mapFunction, reduceFunction)) { // returns a lazy Stream
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
        return collectionExecutor.mapReduce(mapFunction, reduceFunction, rowType);
    }
}
