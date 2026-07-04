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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.BsonTypeClassMap;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.landawn.abacus.parser.JsonParser;
import com.landawn.abacus.parser.ParserFactory;
import com.landawn.abacus.query.QueryUtil;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.Beans;
import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.Fn;
import com.landawn.abacus.util.IOUtil;
import com.landawn.abacus.util.IntFunctions;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.ObjectPool;
import com.landawn.abacus.util.u.Optional;
import com.landawn.abacus.util.stream.Stream;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;

/**
 * Base class providing essential MongoDB utilities, BSON conversion methods, and codec registry management.
 *
 * <p>This abstract class serves as the foundation for MongoDB integrations, offering comprehensive
 * utilities for BSON document manipulation, object-document mapping, data type conversions, and
 * MongoDB-specific operations like ObjectId handling and aggregation support.</p>
 *
 * <h2>Inheritance Contract</h2>
 * <p>{@code MongoDBBase} centralizes type conversion, ID mapping and codec configuration so that
 * concrete subclasses can focus on driver-specific orchestration. The shipped subclasses are:</p>
 * <ul>
 *   <li>{@link com.landawn.abacus.da.mongodb.MongoDB} &mdash; the synchronous facade built on top of
 *       the blocking {@code com.mongodb.client} driver. It exposes builders such as
 *       {@link com.landawn.abacus.da.mongodb.MongoCollectionExecutor} and
 *       {@link com.landawn.abacus.da.mongodb.MongoCollectionMapper}.</li>
 *   <li>{@code com.landawn.abacus.da.mongodb.reactivestreams.MongoDB} &mdash; a non-blocking facade
 *       built on the Reactive Streams MongoDB driver, returning {@code Publisher}-style results
 *       instead of materialized lists.</li>
 * </ul>
 * <p>Both subclasses share the protected helpers and constants defined here (codec registry,
 * ObjectId resolution, document/bean conversion, etc.) but differ in how they consume
 * {@link com.mongodb.client.MongoIterable} versus reactive publishers. Subclasses are expected to
 * call the {@code toDocument}, {@code toEntity} and {@code readRow} helpers when bridging between
 * BSON and user entity types so that ID handling stays consistent.</p>
 *
 * <h2>Key Features</h2>
 * <h3>Core Capabilities:</h3>
 * <ul>
 *   <li><strong>BSON Conversion:</strong> Bidirectional conversion between Java objects and BSON documents</li>
 *   <li><strong>ObjectId Management:</strong> Automatic ObjectId generation and mapping for entity classes</li>
 *   <li><strong>Codec Registry:</strong> Custom codec registry supporting POJO serialization/deserialization</li>
 *   <li><strong>Type Safety:</strong> Strongly-typed document operations with automatic type conversion</li>
 *   <li><strong>Data Extraction:</strong> Stream processing and Dataset extraction from MongoDB cursors</li>
 * </ul>
 *
 * <h3>Thread Safety:</h3>
 * <p>This class is thread-safe. All static utility methods can be called concurrently without
 * external synchronization. The codec registry and internal caches use thread-safe implementations.</p>
 *
 * <h3>Performance Considerations:</h3>
 * <ul>
 *   <li>Method reflection results are cached for improved performance</li>
 *   <li>Codec instances are pooled and reused across operations</li>
 *   <li>Stream operations provide lazy evaluation for large result sets</li>
 * </ul>
 *
 * <h3>BSON Type Mapping:</h3>
 * <ul>
 *   <li>Java String &rarr; BSON String</li>
 *   <li>Java primitives &rarr; BSON numeric types</li>
 *   <li>Java Date &rarr; BSON DateTime</li>
 *   <li>Java collections &rarr; BSON arrays</li>
 *   <li>Java Maps/POJOs &rarr; BSON documents</li>
 * </ul>
 *
 * @see Document
 * @see ObjectId
 * @see com.mongodb.client.MongoCollection
 * @see Codec
 * @see com.landawn.abacus.da.mongodb.MongoDB
 */
public abstract class MongoDBBase {

    /**
     * MongoDB's default document identifier field name used in BSON documents.
     *
     * <p>This constant represents the standard "_id" field that MongoDB automatically
     * creates for every document if not explicitly provided. It's used throughout
     * the framework for ObjectId operations and document identification.</p>
     */
    public static final String _ID = "_id";

    /**
     * Standard property name for entity ID fields in Java objects.
     *
     * <p>This constant is used as a fallback when mapping Java entity ID properties
     * to MongoDB's "_id" field. When an entity doesn't have an explicit {@code @Id} annotation,
     * the framework looks for a property named "id" to map to MongoDB's "_id".</p>
     */
    public static final String ID = "id";

    /**
     * Default {@link AsyncExecutor} shared by subclasses for asynchronous MongoDB operations.
     *
     * <p>Sized for I/O-bound workloads: the core pool is {@code max(64, CPU_CORES * 8)} threads,
     * the maximum pool is {@code max(128, CPU_CORES * 16)} threads, and idle threads above the
     * core size are reclaimed after 180&nbsp;seconds.</p>
     */
    protected static final AsyncExecutor DEFAULT_ASYNC_EXECUTOR = new AsyncExecutor(//
            N.max(64, IOUtil.CPU_CORES * 8), // coreThreadPoolSize
            N.max(128, IOUtil.CPU_CORES * 16), // maxThreadPoolSize
            180L, TimeUnit.SECONDS);
    private static final JsonParser jsonParser = ParserFactory.createJsonParser();

    /**
     * Shared {@link CodecRegistry} combining the MongoDB default codecs with the bean-aware
     * {@link GeneralCodecRegistry}. Subclasses use this registry when configuring collections so
     * arbitrary POJOs can be transparently (de)serialized to/from BSON.
     */
    protected static final CodecRegistry codecRegistry = CodecRegistries.fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
            new GeneralCodecRegistry());
    private static final Map<Class<?>, Method> classIdSetMethodPool = new ConcurrentHashMap<>();

    /**
     * Protected no-arg constructor for subclasses; this class is not intended to be instantiated
     * directly. Use a concrete subclass such as {@link MongoDB} (sync) or the reactive streams
     * variant {@code com.landawn.abacus.da.mongodb.reactivestreams.MongoDB}.
     */
    protected MongoDBBase() {
    }

    /**
     * Registers a custom property name to be mapped to MongoDB's "_id" field for a specific class.
     *
     * <p>This method allows explicit mapping of a Java property to MongoDB's "_id" field when
     * the default conventions ("id" property or @Id annotation) are not suitable. The specified
     * property must have both getter and setter methods and accept either String or ObjectId types.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Map a String "userId" property to MongoDB's _id field:
     * MongoDB.registerIdProperty(User.class, "userId");      // userId now (de)serializes as _id
     *
     * // An ObjectId-typed property is also accepted:
     * MongoDB.registerIdProperty(Account.class, "objectId");
     *
     * // The property type must be String or ObjectId:
     * MongoDB.registerIdProperty(User.class, "age");         // throws IllegalArgumentException (int is neither String nor ObjectId)
     *
     * // The property must have a getter and a setter:
     * MongoDB.registerIdProperty(User.class, "unknownProp"); // throws IllegalArgumentException (no getter/setter)
     *
     * // Preferred alternative -- annotate the property instead:
     * // public class User { @Id private String userId; }
     * }</pre>
     *
     * @param documentClass the entity class to configure ID property mapping for
     * @param idPropertyName the name of the property to map to MongoDB's "_id" field
     * @throws IllegalArgumentException if the class lacks getter/setter methods for the property,
     *                                  or if the property type is not {@link String} or {@link ObjectId}
     * @see com.landawn.abacus.annotation.Id
     * @see ObjectId
     * @deprecated Use {@code @Id} annotation on the desired property instead of programmatic registration.
     *             This approach provides better compile-time safety and clearer code intent.
     */
    @Deprecated
    public static void registerIdProperty(final Class<?> documentClass, final String idPropertyName) {
        if (Beans.getPropGetter(documentClass, idPropertyName) == null || Beans.getPropSetter(documentClass, idPropertyName) == null) {
            throw new IllegalArgumentException("The specified class: " + ClassUtil.getCanonicalClassName(documentClass)
                    + " doesn't have getter or setter method for the specified id property: " + idPropertyName);
        }

        final Method setMethod = Beans.getPropSetter(documentClass, idPropertyName);
        final Class<?> parameterType = setMethod.getParameterTypes()[0];

        if (!(String.class.isAssignableFrom(parameterType) || ObjectId.class.isAssignableFrom(parameterType))) {
            throw new IllegalArgumentException(
                    "The parameter type of the specified id setter method must be 'String' or 'ObjectId': " + setMethod.toGenericString());
        }

        classIdSetMethodPool.put(documentClass, setMethod);
    }

    /**
     * Creates a BSON filter document for querying by ObjectId using a string representation.
     *
     * <p>This utility method converts a string ObjectId to a proper BSON filter that can be
     * used in MongoDB find operations. The string must be a valid 24-character hexadecimal
     * ObjectId representation.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Build an _id filter from a 24-char hex string:
     * Bson filter = MongoDB.objectIdToFilter("507f1f77bcf86cd799439011");
     * // filter is a Document holding the ObjectId as the _id value: {"_id": ObjectId("507f1f77bcf86cd799439011")}
     * // (the {"$oid": "..."} form is the Extended-JSON serialization of that ObjectId, not the in-memory value)
     * Document doc = collection.find(filter).first();
     *
     * MongoDB.objectIdToFilter((String) null);            // throws IllegalArgumentException
     * MongoDB.objectIdToFilter("");                       // throws IllegalArgumentException
     * MongoDB.objectIdToFilter("not-a-valid-objectid");   // throws IllegalArgumentException
     * }</pre>
     *
     * @param objectId the string representation of the ObjectId (24 hex characters)
     * @return a Bson filter document that matches the specified ObjectId
     * @throws IllegalArgumentException if objectId is null or empty, or is not a valid hexadecimal ObjectId representation
     * @see ObjectId
     * @see #objectIdToFilter(ObjectId)
     */
    public static Bson objectIdToFilter(final String objectId) {
        N.checkArgNotEmpty(objectId, "objectId");

        return objectIdToFilter(new ObjectId(objectId));
    }

    /**
     * Creates a BSON filter document for querying by ObjectId.
     *
     * <p>This utility method creates a properly structured BSON filter document containing
     * the MongoDB "_id" field with the specified ObjectId value. This filter can be used
     * directly in find, update, or delete operations.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ObjectId oid = new ObjectId("507f1f77bcf86cd799439011");
     * Bson filter = MongoDB.objectIdToFilter(oid);
     * // filter is a Document holding the ObjectId as the _id value: {"_id": ObjectId("507f1f77bcf86cd799439011")}
     * // (the {"$oid": "..."} form is the Extended-JSON serialization of that ObjectId, not the in-memory value)
     * collection.deleteOne(filter);
     *
     * MongoDB.objectIdToFilter((ObjectId) null);          // throws IllegalArgumentException
     * }</pre>
     *
     * @param objectId the ObjectId to create a filter for
     * @return a Bson filter document that matches the specified ObjectId
     * @throws IllegalArgumentException if objectId is null
     * @see Document
     * @see #objectIdToFilter(String)
     */
    public static Bson objectIdToFilter(final ObjectId objectId) {
        N.checkArgNotNull(objectId, "objectId");

        return new Document(_ID, objectId);
    }

    /**
     * Creates an instance of the specified target class from a JSON string.
     *
     * <p>This method parses the provided JSON string and creates an instance of the specified BSON type.
     * It supports MongoDB's native BSON types including Document, BasicBSONObject, and BasicDBObject.
     * Parsing is performed by the framework's JSON parser, so the input should be standard JSON.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Parse JSON into a Document (numbers/booleans become Integer/Double/Boolean):
     * Document doc = MongoDB.fromJson("{\"name\": \"John\", \"age\": 30, \"active\": true}", Document.class);
     * doc.getString("name");                              // returns "John"
     * doc.get("age");                                     // returns Integer 30
     * doc.get("active");                                  // returns Boolean true
     *
     * // A decimal becomes a Double:
     * MongoDB.fromJson("{\"score\": 4.5}", Document.class).get("score"); // returns Double 4.5
     *
     * // The same JSON parsed into the legacy BSON types:
     * BasicBSONObject bson = MongoDB.fromJson("{\"name\": \"John\"}", BasicBSONObject.class);
     * BasicDBObject dbObj  = MongoDB.fromJson("{\"name\": \"John\"}", BasicDBObject.class);
     *
     * // Nested objects become nested maps:
     * Document nested = MongoDB.fromJson("{\"name\": \"John\", \"address\": {\"city\": \"NYC\"}}", Document.class);
     * ((Map<String, Object>) nested.get("address")).get("city"); // returns "NYC"
     *
     * // An unsupported target type is rejected:
     * MongoDB.fromJson("{}", String.class);              // throws IllegalArgumentException
     * }</pre>
     *
     * @param <T> the target BSON type
     * @param json the JSON string to parse
     * @param rowType the target class - must be one of: {@link Bson}, {@link Document},
     *                {@link BasicBSONObject}, or {@link BasicDBObject}
     * @return an instance of the specified type populated with the JSON data
     * @throws IllegalArgumentException if {@code rowType} is not one of the supported BSON types
     * @see Document
     * @see org.bson.BasicBSONObject
     * @see com.mongodb.BasicDBObject
     */
    public static <T> T fromJson(final String json, final Class<T> rowType) {
        if (rowType.equals(Bson.class) || rowType.equals(Document.class)) {
            final Document doc = new Document();
            jsonParser.parse(json, doc);
            return (T) doc;
        } else if (rowType.equals(BasicBSONObject.class)) {
            final BasicBSONObject result = new BasicBSONObject();
            jsonParser.parse(json, result);
            return (T) result;
        } else if (rowType.equals(BasicDBObject.class)) {
            final BasicDBObject result = new BasicDBObject();
            jsonParser.parse(json, result);
            return (T) result;
        } else {
            throw new IllegalArgumentException("Unsupported type: " + ClassUtil.getCanonicalClassName(rowType));
        }
    }

    /**
     * Converts a BSON object to its JSON string representation.
     *
     * <p>This method converts a BSON object to a JSON string using MongoDB's codec registry.
     * If the BSON object is already a Map, it's converted directly. Otherwise, it's first
     * converted to a BsonDocument and then to JSON format. The output follows standard
     * JSON format.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // A Document is a Map, so it is serialized directly:
     * Document doc = new Document("name", "John").append("age", 30);
     * String json = MongoDB.toJson(doc);                 // returns {"name": "John", "age": 30}
     *
     * // Nested documents are serialized recursively:
     * Document withNested = new Document("user", new Document("name", "John"));
     * MongoDB.toJson(withNested);                         // returns {"user": {"name": "John"}}
     *
     * // An empty document:
     * MongoDB.toJson(new Document());                     // returns {}
     * }</pre>
     *
     * @param bson the BSON object to convert to JSON
     * @return the JSON string representation of the BSON object
     * @see Document
     * @see org.bson.conversions.Bson
     */
    public static String toJson(final Bson bson) {
        return bson instanceof Map ? N.toJson(bson) : N.toJson(bson.toBsonDocument(Document.class, codecRegistry));
    }

    /**
     * Converts a BSONObject to its JSON string representation.
     *
     * <p>This method converts a BSONObject to a JSON string. If the BSONObject is already a Map,
     * it's converted directly. Otherwise, it's first converted to a Map and then to JSON format.
     * The output follows standard JSON format.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * BasicBSONObject bsonObj = new BasicBSONObject();
     * bsonObj.put("name", "Alice");
     * bsonObj.put("age", 25);
     * bsonObj.put("active", true);
     * String json = MongoDB.toJson(bsonObj);             // returns {"name": "Alice", "age": 25, "active": true}
     *
     * // An empty BSONObject:
     * MongoDB.toJson(new BasicBSONObject());             // returns {}
     * }</pre>
     *
     * @param bsonObject the BSONObject to convert to JSON
     * @return the JSON string representation of the BSONObject
     * @see org.bson.BSONObject
     */
    public static String toJson(final BSONObject bsonObject) {
        return bsonObject instanceof Map ? N.toJson(bsonObject) : N.toJson(bsonObject.toMap());
    }

    /**
     * Converts a BasicDBObject to its JSON string representation.
     *
     * <p>This method converts a MongoDB BasicDBObject to a JSON string using the standard JSON
     * serialization format. BasicDBObject is part of the legacy MongoDB Java driver and is
     * equivalent to a Map&lt;String, Object&gt; with BSON type awareness. The method handles all
     * standard BSON types and produces human-readable JSON output.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * BasicDBObject dbObj = new BasicDBObject();
     * dbObj.put("name", "Charlie");
     * dbObj.put("age", 28);
     * String json = MongoDB.toJson(dbObj);               // returns {"name": "Charlie", "age": 28}
     *
     * // An ObjectId value is rendered as its 24-char hex string (not as ObjectId("...")):
     * dbObj.put("_id", new ObjectId("507f1f77bcf86cd799439011"));
     * MongoDB.toJson(dbObj);
     * // returns {"name": "Charlie", "age": 28, "_id": "507f1f77bcf86cd799439011"}
     * }</pre>
     *
     * @param bsonObject the BasicDBObject to convert to JSON
     * @return the JSON string representation of the BasicDBObject
     * @see com.mongodb.BasicDBObject
     * @see #toJson(BSONObject)
     */
    public static String toJson(final BasicDBObject bsonObject) {
        return N.toJson(bsonObject);
    }

    /**
     * Converts an object to a BSON document.
     *
     * <p>This method converts various object types to BSON format for MongoDB operations. It accepts
     * entity objects with getter/setter methods, Maps, or arrays of property name-value pairs.
     * The resulting BSON document can be used directly in MongoDB operations.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Convert a Map to BSON:
     * Bson mapBson = MongoDB.toBson(Map.of("name", "Alice", "age", 25));
     * // returns a Document: {"name": "Alice", "age": 25}
     *
     * // Convert an entity (its readable properties become fields):
     * User user = new User("John", "john@example.com", 30);
     * Bson userBson = MongoDB.toBson(user);
     *
     * // A null argument is rejected:
     * MongoDB.toBson((Object) null);                      // throws NullPointerException
     * }</pre>
     *
     * @param obj the object to convert - can be an entity with getter/setter methods, {@code Map<String, Object>}, or array of property name-value pairs
     * @return a BSON document representation of the object
     * @throws NullPointerException if {@code obj} is {@code null}
     * @throws IllegalArgumentException if {@code obj} is an array with an odd number of elements,
     *         is a bean class with no readable properties, or is otherwise not convertible to a BSON document
     * @see Document
     * @see org.bson.conversions.Bson
     * @see #toDocument(Object)
     */
    public static Bson toBson(final Object obj) {
        return toDocument(obj);
    }

    /**
     * Creates a new BSON document with the specified parameters.
     *
     * <p>This method creates a BSON document from variable arguments that represent property name-value pairs,
     * a single object (Map or entity), or a combination. The arguments should be provided as alternating
     * property names and values, or as a single object to be converted.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Build BSON from alternating name/value pairs:
     * Bson userBson = MongoDB.toBson("name", "John", "age", 30, "active", true);
     * // returns a Document: {"name": "John", "age": 30, "active": true}
     *
     * // No arguments yields an empty document:
     * Bson empty = MongoDB.toBson();                      // returns an empty Document {}
     *
     * // An odd number of name/value elements is rejected:
     * MongoDB.toBson("name", "John", "age");              // throws IllegalArgumentException
     * }</pre>
     *
     * @param a variable arguments representing property name-value pairs, or a single object to convert.
     *          When {@code null} or empty an empty BSON document is returned. When length is 1 the single
     *          element is converted via {@link #toBson(Object)}. Otherwise the array is treated as alternating
     *          name-value pairs and must contain an even number of elements.
     * @return a BSON document created from the specified parameters; never {@code null}
     * @throws IllegalArgumentException if multiple arguments are provided but they don't form valid name-value pairs
     * @see #toBson(Object)
     * @see Document
     */
    public static Bson toBson(final Object... a) {
        return toDocument(a);
    }

    /**
     * Converts an object to a MongoDB Document.
     *
     * <p>This method converts various object types to MongoDB's Document format. It accepts entity objects
     * with getter/setter methods, Maps, or arrays of property name-value pairs. The conversion handles
     * ObjectId mapping and ensures proper BSON type compatibility. This is the preferred method for
     * converting Java objects to MongoDB documents.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Convert a Map to a Document:
     * Document mapDoc = MongoDB.toDocument(Map.of("name", "Alice", "age", 25));
     * // returns {"name": "Alice", "age": 25}
     *
     * // Convert an entity, then insert it:
     * User user = new User("John", "john@example.com", 30);
     * Document userDoc = MongoDB.toDocument(user);
     * collection.insertOne(userDoc);
     *
     * // A null argument is rejected:
     * MongoDB.toDocument((Object) null);                  // throws NullPointerException
     * }</pre>
     *
     * @param obj the object to convert - can be an entity with getter/setter methods, {@code Map<String, Object>}, or array of property name-value pairs
     * @return a MongoDB Document representation of the object; never {@code null}
     * @throws NullPointerException if {@code obj} is {@code null}
     * @throws IllegalArgumentException if {@code obj} is an array with an odd number of elements,
     *         is a bean class with no readable properties, or is not a {@link Map}, bean, or {@code Object[]}
     * @see Document
     * @see #toBson(Object)
     */
    public static Document toDocument(final Object obj) {
        return toDocument(obj, false);
    }

    /**
     * Creates a MongoDB Document from variable arguments representing property name-value pairs or a single object.
     *
     * <p>This method provides flexible Document creation supporting multiple input patterns:
     * alternating property names and values, a single object to be converted, or no arguments
     * for an empty Document. This is a convenience method that delegates to {@link #toDocument(Object)}
     * with appropriate argument handling.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Build a Document from alternating name/value pairs:
     * Document userDoc = MongoDB.toDocument("name", "John", "age", 30, "active", true);
     * // returns {"name": "John", "age": 30, "active": true}
     *
     * // No arguments yields an empty Document:
     * Document empty = MongoDB.toDocument();              // returns an empty Document {}
     *
     * // An odd number of name/value elements is rejected:
     * MongoDB.toDocument("name", "John", "age");          // throws IllegalArgumentException
     * }</pre>
     *
     * @param a variable arguments representing:
     *          <ul>
     *          <li>Alternating property names (String) and values: "name", "John", "age", 30</li>
     *          <li>A single entity object or Map to be converted</li>
     *          <li>No arguments for an empty Document</li>
     *          </ul>
     * @return a MongoDB Document created from the specified arguments
     * @throws IllegalArgumentException if arguments are malformed (odd number of name-value pairs)
     * @see #toDocument(Object)
     * @see Document
     */
    public static Document toDocument(final Object... a) {
        if (N.isEmpty(a)) {
            return new Document();
        }

        return a.length == 1 ? toDocument(a[0]) : toDocument((Object) a);
    }

    /**
     * Internal conversion helper that converts {@code obj} into a MongoDB {@link Document} and
     * applies ObjectId normalization to the resulting document.
     *
     * <p>Supported input types: {@link Map}, bean-style entities (via {@code Beans.isBeanClass}),
     * and {@code Object[]} containing alternating property-name / value pairs. After population the
     * helper invokes {@code resetObjectId} so that the document's id field is materialized as a
     * proper {@link ObjectId} when possible.</p>
     *
     * @param obj the source value; must not be {@code null}
     * @param isForUpdate reserved for callers that build {@code $set}-style update documents; currently unused
     * @return a {@link Document} populated from {@code obj}
     * @throws NullPointerException if {@code obj} is {@code null}
     * @throws IllegalArgumentException if {@code obj} is an array with an odd number of elements,
     *         a bean class with no getter/setter pairs, or an unsupported type
     */
    protected static Document toDocument(final Object obj, @SuppressWarnings("unused") final boolean isForUpdate) { //NOSONAR
        final Document result = new Document();

        if (obj instanceof Map) {
            result.putAll((Map<String, Object>) obj);
        } else if (Beans.isBeanClass(obj.getClass())) {
            final Map<String, Method> getterMethodList = Beans.getPropGetters(obj.getClass());

            if (getterMethodList.isEmpty()) {
                throw new IllegalArgumentException("No property getter/setter method found in the specified entity: " + obj.getClass().getCanonicalName());
            }

            String propName = null;
            Object propValue = null;

            for (final Map.Entry<String, Method> entry : getterMethodList.entrySet()) {
                propName = entry.getKey();
                propValue = Beans.getPropValue(obj, entry.getValue());

                if (propValue == null) {
                    continue;
                }

                result.put(propName, propValue);
            }
        } else if (obj instanceof final Object[] a) {
            if ((a.length % 2) != 0) {
                throw new IllegalArgumentException("Parameters must be property name-value pairs, a Map, or an entity with getter/setter methods");
            }

            for (int i = 0; i < a.length; i++) {
                // Validate eagerly so a non-String name surfaces as the documented IllegalArgumentException
                // instead of a raw ClassCastException.
                if (!(a[i] instanceof String)) {
                    throw new IllegalArgumentException("Parameters must be property name-value pairs whose names are Strings, but found "
                            + (a[i] == null ? "null" : a[i].getClass().getName()) + " at index " + i);
                }

                result.put((String) a[i], a[++i]);
            }
        } else {
            throw new IllegalArgumentException("Parameters must be a Map or an entity with getter/setter methods");
        }

        resetObjectId(obj, result);

        return result;
    }

    /**
     * Converts an object to a BasicBSONObject for MongoDB operations.
     *
     * <p>This method converts various object types to the legacy BasicBSONObject format used by
     * older MongoDB Java driver versions. It accepts entity objects with getter/setter methods,
     * Maps, or arrays of property name-value pairs. The resulting BasicBSONObject can be used
     * with legacy MongoDB driver APIs.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Convert a Map to a BasicBSONObject:
     * BasicBSONObject mapBson = MongoDB.toBSONObject(Map.of("name", "Alice", "age", 25));
     * // returns a BasicBSONObject: {"name": "Alice", "age": 25}
     *
     * // Convert an entity:
     * User user = new User("John", "john@example.com", 30);
     * BasicBSONObject userBson = MongoDB.toBSONObject(user);
     *
     * // null is rejected; an unsupported type is rejected:
     * MongoDB.toBSONObject((Object) null);                // throws NullPointerException
     * MongoDB.toBSONObject(new Object());                 // throws IllegalArgumentException
     * }</pre>
     *
     * @param obj the object to convert - can be an entity with getter/setter methods, {@code Map<String, Object>}, or array of property name-value pairs
     * @return a BasicBSONObject representation of the object; never {@code null}
     * @throws NullPointerException if {@code obj} is {@code null}
     * @throws IllegalArgumentException if {@code obj} is an array with an odd number of elements,
     *         or is not a {@link Map}, bean, or {@code Object[]}
     * @see BasicBSONObject
     * @see #toBSONObject(Object...)
     * @see #toDocument(Object)
     */
    public static BasicBSONObject toBSONObject(final Object obj) {
        final BasicBSONObject result = new BasicBSONObject();

        if (obj instanceof Map) {
            result.putAll((Map<String, Object>) obj);
        } else if (Beans.isBeanClass(obj.getClass())) {
            Beans.deepBeanToMap(obj, result);
        } else if (obj instanceof final Object[] a) {
            if ((a.length % 2) != 0) {
                throw new IllegalArgumentException("Parameters must be property name-value pairs, a Map, or an entity with getter/setter methods");
            }

            for (int i = 0; i < a.length; i++) {
                // Validate eagerly so a non-String name surfaces as the documented IllegalArgumentException
                // instead of a raw ClassCastException.
                if (!(a[i] instanceof String)) {
                    throw new IllegalArgumentException("Parameters must be property name-value pairs whose names are Strings, but found "
                            + (a[i] == null ? "null" : a[i].getClass().getName()) + " at index " + i);
                }

                result.put((String) a[i], a[++i]);
            }
        } else {
            throw new IllegalArgumentException("Parameters must be a Map or an entity with getter/setter methods");
        }

        resetObjectId(obj, result);

        return result;
    }

    /**
     * Converts a variable number of arguments to a BasicBSONObject for MongoDB operations.
     *
     * <p>This method provides a convenient way to create BasicBSONObject instances from various input types.
     * It handles both single object conversion and property name-value pairs. When multiple arguments
     * are provided, they are interpreted as alternating property names and values.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // No arguments yields an empty BasicBSONObject:
     * BasicBSONObject empty = MongoDB.toBSONObject();     // empty.isEmpty() == true
     *
     * // Build from alternating name/value pairs:
     * BasicBSONObject pairs = MongoDB.toBSONObject("name", "Bob", "age", 30);
     * // returns {"name": "Bob", "age": 30}  (size 2)
     *
     * // An odd number of elements is rejected:
     * MongoDB.toBSONObject("name", "Bob", "age");         // throws IllegalArgumentException
     * }</pre>
     *
     * @param a variable arguments that can be:
     *          <ul>
     *          <li>Empty - returns empty BasicBSONObject</li>
     *          <li>Single object - delegates to {@link #toBSONObject(Object)}</li>
     *          <li>Multiple objects - interpreted as property name-value pairs</li>
     *          </ul>
     * @return a BasicBSONObject representation of the arguments
     * @throws IllegalArgumentException if multiple arguments are provided but they don't form valid pairs
     * @see #toBSONObject(Object)
     * @see BasicBSONObject
     */
    public static BasicBSONObject toBSONObject(final Object... a) {
        if (N.isEmpty(a)) {
            return new BasicBSONObject();
        }

        return a.length == 1 ? toBSONObject(a[0]) : toBSONObject((Object) a);
    }

    /**
     * Converts an object to a BasicDBObject for legacy MongoDB operations.
     *
     * <p>This method converts various object types to the legacy BasicDBObject format used by
     * older MongoDB Java driver versions. It accepts entity objects with getter/setter methods,
     * Maps, or arrays of property name-value pairs. The resulting BasicDBObject can be used
     * with legacy MongoDB driver APIs that require this specific document format.</p>
     *
     * <p>The method handles ObjectId reset operations to ensure proper MongoDB document structure,
     * maintaining compatibility with MongoDB's document identification system.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Convert a Map to a BasicDBObject:
     * BasicDBObject mapDb = MongoDB.toDBObject(Map.of("name", "Alice", "age", 25));
     * // returns a BasicDBObject: {"name": "Alice", "age": 25}
     *
     * // Convert an entity:
     * User user = new User("John", "john@example.com", 30);
     * BasicDBObject userDb = MongoDB.toDBObject(user);
     *
     * // null is rejected; an unsupported type is rejected:
     * MongoDB.toDBObject((Object) null);                  // throws NullPointerException
     * MongoDB.toDBObject(new Object());                   // throws IllegalArgumentException
     * }</pre>
     *
     * @param obj the object to convert - can be an entity with getter/setter methods, {@code Map<String, Object>}, or array of property name-value pairs
     * @return a BasicDBObject representation of the object; never {@code null}
     * @throws NullPointerException if {@code obj} is {@code null}
     * @throws IllegalArgumentException if {@code obj} is an array with an odd number of elements,
     *         or is not a {@link Map}, bean, or {@code Object[]}
     * @see BasicDBObject
     * @see #toDBObject(Object...)
     * @see #toBSONObject(Object)
     */
    public static BasicDBObject toDBObject(final Object obj) {
        final BasicDBObject result = new BasicDBObject();

        if (obj instanceof Map) {
            result.putAll((Map<String, Object>) obj);
        } else if (Beans.isBeanClass(obj.getClass())) {
            Beans.deepBeanToMap(obj, result);
        } else if (obj instanceof final Object[] a) {
            if ((a.length % 2) != 0) {
                throw new IllegalArgumentException("Parameters must be property name-value pairs, a Map, or an entity with getter/setter methods");
            }

            for (int i = 0; i < a.length; i++) {
                // Validate eagerly so a non-String name surfaces as the documented IllegalArgumentException
                // instead of a raw ClassCastException.
                if (!(a[i] instanceof String)) {
                    throw new IllegalArgumentException("Parameters must be property name-value pairs whose names are Strings, but found "
                            + (a[i] == null ? "null" : a[i].getClass().getName()) + " at index " + i);
                }

                result.put((String) a[i], a[++i]);
            }
        } else {
            throw new IllegalArgumentException("Parameters must be a Map or an entity with getter/setter methods");
        }

        resetObjectId(obj, result);

        return result;
    }

    /**
     * Converts a variable number of arguments to a BasicDBObject for legacy MongoDB operations.
     *
     * <p>This method provides a convenient way to create BasicDBObject instances from various input types.
     * It handles both single object conversion and property name-value pairs. When multiple arguments
     * are provided, they are interpreted as alternating property names and values.</p>
     *
     * <p>This variant is particularly useful for building query objects or document structures inline
     * without requiring intermediate object creation.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // No arguments yields an empty BasicDBObject:
     * BasicDBObject empty = MongoDB.toDBObject();         // empty.isEmpty() == true
     *
     * // Build from alternating name/value pairs:
     * BasicDBObject query = MongoDB.toDBObject("status", "active", "age", 30);
     * // returns {"status": "active", "age": 30}  (size 2)
     *
     * // An odd number of elements is rejected:
     * MongoDB.toDBObject("status", "active", "age");      // throws IllegalArgumentException
     * }</pre>
     *
     * @param a variable arguments that can be:
     *          <ul>
     *          <li>Empty - returns empty BasicDBObject</li>
     *          <li>Single object - delegates to {@link #toDBObject(Object)}</li>
     *          <li>Multiple objects - interpreted as property name-value pairs</li>
     *          </ul>
     * @return a BasicDBObject representation of the arguments
     * @throws IllegalArgumentException if multiple arguments are provided but they don't form valid pairs
     * @see #toDBObject(Object)
     * @see BasicDBObject
     */
    public static BasicDBObject toDBObject(final Object... a) {
        if (N.isEmpty(a)) {
            return new BasicDBObject();
        }

        return a.length == 1 ? toDBObject(a[0]) : toDBObject((Object) a);
    }

    /**
     * Converts a MongoDB Document to a Java Map with default configuration.
     *
     * <p>This method converts a MongoDB Document to a standard Java Map&lt;String, Object&gt;
     * using default conversion settings. It provides a simple way to work with MongoDB
     * documents using familiar Java collection interfaces.</p>
     *
     * <p>The conversion handles nested documents, arrays, and MongoDB-specific types
     * appropriately, creating a nested Map structure that mirrors the document hierarchy.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Document doc = new Document("name", "John")
     *         .append("age", 30)
     *         .append("address", new Document("city", "NYC"));
     *
     * Map<String, Object> map = MongoDB.toMap(doc);       // returns a HashMap
     * map.get("name");                                    // returns "John"
     * map.get("age");                                     // returns Integer 30
     *
     * // The copy is shallow: a nested Document stays a Document (which is itself a Map):
     * map.get("address") instanceof Document;             // returns true
     * }</pre>
     *
     * @param doc the MongoDB Document to convert; must not be null
     * @return a Map representation of the document
     * @throws NullPointerException if doc is null
     * @see #toMap(Document, IntFunction)
     * @see Document
     */
    public static Map<String, Object> toMap(final Document doc) {
        return toMap(doc, IntFunctions.ofMap());
    }

    /**
     * Converts a MongoDB Document to a Java Map using a custom map supplier.
     *
     * <p>This method converts a MongoDB Document to a Map using a provided IntFunction
     * to create the target Map instance. This allows for precise control over the Map
     * implementation used, enabling optimizations such as pre-sizing the map based on
     * the document size or using specialized Map implementations.</p>
     *
     * <p>The mapSupplier function receives the document size as input, allowing it to
     * create appropriately sized Map instances to minimize rehashing and improve performance.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Document doc = new Document("name", "John").append("age", 30);
     *
     * // Preserve insertion order with a LinkedHashMap:
     * Map<String, Object> ordered = MongoDB.toMap(doc, IntFunctions.ofMap(LinkedHashMap.class));
     * ordered instanceof LinkedHashMap;                   // returns true
     *
     * // Sort keys with a TreeMap (this supplier ignores the size hint):
     * Map<String, Object> sorted = MongoDB.toMap(doc, size -> new TreeMap<>());
     * sorted.keySet();                                    // returns [age, name] in sorted order
     * }</pre>
     *
     * @param doc the MongoDB Document to convert; must not be null
     * @param mapSupplier a function that creates Map instances based on expected size
     * @return a Map representation of the document using the supplied Map type
     * @throws NullPointerException if doc or mapSupplier is null
     * @see #toMap(Document)
     * @see IntFunction
     * @see Document
     */
    public static Map<String, Object> toMap(final Document doc, final IntFunction<? extends Map<String, Object>> mapSupplier) {
        final Map<String, Object> map = mapSupplier.apply(doc.size());

        map.putAll(doc);

        return map;
    }

    /**
     * Converts a MongoDB Document to an entity instance with automatic ID field mapping.
     *
     * <p>This method converts a MongoDB Document to an instance of the specified entity type,
     * handling automatic mapping between MongoDB's "_id" field and the entity's ID property.
     * The method supports both String and ObjectId ID types and will automatically convert
     * between them as needed. The conversion preserves the original document structure.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // _id is an ObjectId. If the entity's id property is String-typed, it receives the hex string:
     * Document doc = new Document("_id", new ObjectId("507f1f77bcf86cd799439011")).append("name", "John");
     * User user = MongoDB.toEntity(doc, User.class);      // assuming class User { String id; String name; }
     * user.getId();                                       // returns "507f1f77bcf86cd799439011"
     * user.getName();                                     // returns "John"
     *
     * // If the id property is ObjectId-typed, the ObjectId is assigned as-is (no string conversion).
     *
     * // A null document yields null:
     * MongoDB.toEntity(null, User.class);                 // returns null
     * }</pre>
     *
     * @param <T> the target entity type
     * @param doc the MongoDB Document to convert; if {@code null}, {@code null} is returned
     * @param rowType the Class representing the target entity type; must not be {@code null} when {@code doc} is non-null
     * @return an entity instance populated with data from the document, or {@code null} if {@code doc} is {@code null}
     * @throws NullPointerException if {@code doc} is non-null and {@code rowType} is {@code null}
     * @see Document
     * @see #_ID
     * @see com.landawn.abacus.annotation.Id
     */
    public static <T> T toEntity(final Document doc, final Class<T> rowType) {
        if (doc == null) {
            return null;
        }

        final Method idSetMethod = getObjectIdSetMethod(rowType);
        final Class<?> parameterType = idSetMethod == null ? null : idSetMethod.getParameterTypes()[0];
        final boolean hasObjectId = doc.containsKey(_ID);
        final Object objectId = doc.get(_ID);
        T entity = null;

        doc.remove(_ID);

        try {
            entity = Beans.mapToBean(doc, rowType);

            if (objectId != null && parameterType != null && entity != null) {
                if (parameterType.isAssignableFrom(objectId.getClass()) || !parameterType.isAssignableFrom(String.class)) {
                    Beans.setPropValue(entity, idSetMethod, objectId);
                } else {
                    Beans.setPropValue(entity, idSetMethod, objectId.toString());
                }
            }
        } finally {
            if (hasObjectId) {
                doc.put(_ID, objectId);
            }
        }

        return entity;
    }

    /**
     * Converts a MongoDB query result to a strongly-typed List.
     *
     * <p>This method converts the results from a MongoDB query (returned as MongoIterable) to a List
     * of the specified type. It supports entity classes with getter/setter methods, Map.class, and
     * basic single value types. Each document in the result is automatically converted to the target type,
     * providing type safety and eliminating manual conversion code.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Convert query results to entities:
     * FindIterable<Document> userDocs = collection.find(Filters.eq("status", "active"));
     * List<User> users = MongoDB.toList(userDocs, User.class);
     *
     * // Documents requested as Map are returned unchanged (a Document is a Map):
     * List<Map<String, Object>> userMaps = MongoDB.toList(userDocs, Map.class);
     *
     * // A single-field projection can be read straight into scalars:
     * // documents [{"name": "alice"}, {"name": "bob"}]  ->  ["alice", "bob"]
     * List<String> names = MongoDB.toList(collection.find().projection(Projections.include("name")), String.class);
     *
     * // No matching documents yields an empty list.
     * // A document with more than two fields cannot be read into a scalar:
     * //     MongoDB.toList(threeFieldDocs, Integer.class)  ->  throws IllegalArgumentException
     * }</pre>
     *
     * @param <T> the target type for list elements
     * @param findIterable the MongoDB query result to convert
     * @param rowType the target class - can be an entity class with getter/setter methods, Map.class, or basic single value type (Primitive/String/Date...)
     * @return a List containing all results converted to the specified type (empty list if no results)
     * @throws NullPointerException if findIterable is null
     * @throws IllegalArgumentException if {@code rowType} is null, or if a result document cannot be projected onto {@code rowType} (e.g. a document with more than two fields targeted at a primitive/String/Date scalar)
     * @see com.mongodb.client.MongoIterable
     * @see #toEntity(Document, Class)
     */
    @SuppressWarnings("rawtypes")
    public static <T> List<T> toList(final MongoIterable<?> findIterable, final Class<T> rowType) {
        final Type<T> targetType = N.typeOf(rowType);
        final List<Object> rowList = findIterable.into(new ArrayList<>());
        final Optional<Object> firstNonNull = N.firstNonNull(rowList);

        if (firstNonNull.isPresent()) {
            if (rowType.isAssignableFrom(firstNonNull.get().getClass())) {
                return (List<T>) rowList;
            } else {
                final List<Object> resultList = new ArrayList<>(rowList.size());

                if (targetType.isBean() || targetType.isMap()) {
                    if (firstNonNull.get() instanceof Document) {
                        for (final Object row : rowList) {
                            resultList.add(readRow((Document) row, rowType));
                        }
                    } else if (targetType.isMap()) {
                        Map<String, Object> rowMap = null;
                        for (final Object row : rowList) {
                            rowMap = N.newMap((Class<Map>) rowType);
                            Beans.beanToMap(row, rowMap);
                            resultList.add(rowMap);
                        }
                    } else {
                        for (final Object row : rowList) {
                            resultList.add(Beans.copyAs(row, rowType));
                        }
                    }
                } else if (firstNonNull.get() instanceof Map && ((Map<String, Object>) firstNonNull.get()).size() <= 2) {
                    // Derive the scalar property name from the first row that actually carries a non-_id key.
                    // A matched document that lacks the projected field comes back as {_id: ...} only; if such
                    // a document happened to be first, every row would silently be read from "_id" instead.
                    String propName = null;

                    for (final Object row : rowList) {
                        if (row instanceof Map) {
                            propName = N.findFirst(((Map<String, Object>) row).keySet(), Fn.notEqual(_ID)).orElse(null);

                            if (propName != null) {
                                break;
                            }
                        }
                    }

                    if (propName == null) {
                        propName = _ID;
                    }

                    Object sampleValue = null;

                    for (final Object row : rowList) {
                        if (row instanceof Map && (sampleValue = ((Map<String, Object>) row).get(propName)) != null) {
                            break;
                        }
                    }

                    if (sampleValue != null && rowType.isAssignableFrom(sampleValue.getClass())) {
                        for (final Object row : rowList) {
                            resultList.add(((Map<String, Object>) row).get(propName));
                        }
                    } else {
                        for (final Object row : rowList) {
                            resultList.add(N.convert(((Map<String, Object>) row).get(propName), rowType));
                        }
                    }
                } else {
                    throw new IllegalArgumentException("Cannot convert document: " + firstNonNull + " to class: " + ClassUtil.getCanonicalClassName(rowType));
                }

                return (List<T>) resultList;
            }
        } else {
            return new ArrayList<>();
        }
    }

    /**
     * Converts a single MongoDB {@link Document} row into an instance of {@code rowType}.
     *
     * <p>Routing rules based on {@code rowType}:</p>
     * <ul>
     *   <li>{@code null} or an object-array type &rarr; the row values are placed into a new
     *       {@code Object[]} (component type derived from {@code rowType} when supplied).</li>
     *   <li>A {@link Collection} type &rarr; a new collection of that type containing the row's values.</li>
     *   <li>A {@link Map} type &rarr; a new map of that type populated via {@link #toMap(Document, IntFunction)}.</li>
     *   <li>A bean class &rarr; delegated to {@link #toEntity(Document, Class)}.</li>
     *   <li>Any other type when the row has at most two fields &rarr; the non-{@code _id} field value is
     *       converted to {@code rowType} via {@code N.convert}.</li>
     * </ul>
     *
     * @param <T> the target type
     * @param row the document to convert; if {@code null}, the default value of {@code rowType} is returned
     *            (or {@code null} when {@code rowType} is {@code null})
     * @param rowType the target type, or {@code null} to produce an {@code Object[]}
     * @return the converted value
     * @throws IllegalArgumentException if the row cannot be projected onto {@code rowType}
     */
    @SuppressWarnings("rawtypes")
    protected static <T> T readRow(final Document row, final Class<T> rowType) {
        if (row == null) {
            return rowType == null ? null : N.defaultValueOf(rowType);
        }

        final Type<?> targetType = rowType == null ? null : N.typeOf(rowType);
        final int columnCount = row.size();

        if (targetType == null || targetType.isObjectArray()) {
            final Object[] a = rowType == null ? new Object[columnCount] : N.newArray(rowType.getComponentType(), columnCount);
            int idx = 0;

            for (final Object value : row.values()) {
                a[idx++] = value;
            }

            return (T) a;
        } else if (targetType.isCollection()) {
            final Collection<Object> c = N.newCollection((Class<Collection>) rowType);

            c.addAll(row.values());

            return (T) c;
        } else if (targetType.isMap()) {
            return (T) toMap(row, IntFunctions.ofMap((Class<Map>) rowType));
        } else if (targetType.isBean()) {
            return toEntity(row, rowType);
        } else if (row.size() <= 2) {
            final String propName = N.findFirst(row.keySet(), Fn.notEqual(_ID)).orElse(_ID);

            return N.convert(row.get(propName), rowType);
        } else {
            throw new IllegalArgumentException("Cannot read a document with " + columnCount + " fields into single-value type: " + rowType);
        }
    }

    /**
     * Extracts data from a MongoDB query result into a Dataset with Map-based rows.
     *
     * <p>This method converts MongoDB query results into a Dataset structure, where each document
     * becomes a row represented as a Map. This provides a tabular view of the data that's useful
     * for reporting, data analysis, or integration with other data processing frameworks.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Each document becomes a Map-based row:
     * FindIterable<Document> userDocs = collection.find(Filters.eq("department", "Engineering"));
     * Dataset userData = MongoDB.extractData(userDocs);
     * userData.size();                                    // number of matched documents
     * userData.getColumn("name");                         // the "name" column, one value per row
     *
     * userData.println();        // prints the data as a table
     * userData.toCsv("users.csv");
     * }</pre>
     *
     * @param findIterable the MongoDB query result to extract data from
     * @return a Dataset containing the query results with Map-based rows
     * @throws NullPointerException if findIterable is null
     * @see Dataset
     * @see #extractData(MongoIterable, Class)
     */
    public static Dataset extractData(final MongoIterable<?> findIterable) {
        return extractData(findIterable, Map.class);
    }

    /**
     * Extracts data from a MongoDB query result into a Dataset with typed rows.
     *
     * <p>This method converts MongoDB query results into a Dataset structure with rows of the
     * specified type. This provides more type safety than the Map-based version and allows for
     * direct conversion to entity objects or other specific types. Each document in the result
     * is converted to the target row type using the framework's type conversion system.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Extract into a Dataset whose rows map to User objects:
     * FindIterable<Document> userDocs = collection.find(Filters.eq("active", true));
     * Dataset userData = MongoDB.extractData(userDocs, User.class);
     * List<User> users = userData.toList(User.class);
     * User firstUser = userData.getRow(0, User.class);
     *
     * // rowType must be a bean class or assignable to Map -- a scalar type is rejected:
     * MongoDB.extractData(userDocs, String.class);        // throws IllegalArgumentException
     * }</pre>
     *
     * @param findIterable the MongoDB query result to extract data from
     * @param rowType the target type for each row in the Dataset; must be an entity class with getter/setter methods or assignable to Map
     * @return a Dataset containing the query results with typed rows
     * @throws IllegalArgumentException if rowType is unsupported (not a bean class and not assignable to Map)
     * @throws NullPointerException if findIterable is null
     * @see Dataset
     * @see #extractData(MongoIterable)
     * @see #extractData(Collection, MongoIterable, Class)
     */
    public static Dataset extractData(final MongoIterable<?> findIterable, final Class<?> rowType) {
        return extractData(null, findIterable, rowType);
    }

    /**
     * Extracts selected properties from a MongoDB query result into a Dataset with typed rows.
     *
     * <p>This method provides selective data extraction by specifying which properties to include
     * in the resulting Dataset. This is useful for performance optimization when you only need
     * specific fields from large documents, or for creating focused data views. The selected
     * properties are extracted from each document and converted to the specified row type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<String> fields = Arrays.asList("name", "department");
     * FindIterable<Document> userDocs = collection.find();
     *
     * // Keep only the selected columns:
     * Dataset limited = MongoDB.extractData(fields, userDocs, Map.class);
     * limited.columnNames();                              // returns [name, department]
     *
     * // null selectPropNames keeps all columns; rowType must be a bean class or a Map:
     * Dataset all = MongoDB.extractData(null, userDocs, User.class);
     * }</pre>
     *
     * @param selectPropNames collection of property names to include in the Dataset; null to include all
     * @param findIterable the MongoDB query result to extract data from
     * @param rowType the target type for each row in the Dataset; must be an entity class with getter/setter methods or assignable to Map
     * @return a Dataset containing the selected properties with typed rows
     * @throws IllegalArgumentException if rowType is unsupported (not a bean class and not assignable to Map)
     * @throws NullPointerException if findIterable is null
     * @see Dataset
     * @see #extractData(MongoIterable, Class)
     */
    public static Dataset extractData(final Collection<String> selectPropNames, final MongoIterable<?> findIterable, final Class<?> rowType) {
        checkResultClass(rowType);

        final List<Object> rowList = findIterable.into(new ArrayList<>());
        return extractData(selectPropNames, rowList, rowType);
    }

    /**
     * Extracts data from a list of objects into a Dataset with Map-based rows.
     *
     * <p>This method converts a list of objects (typically MongoDB documents) into a Dataset
     * structure where each object becomes a row represented as a Map. This is useful when you
     * already have the data loaded into memory and want to convert it to a tabular format
     * for analysis, reporting, or further processing.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Document> documents = Arrays.asList(
     *         new Document("name", "John").append("age", 30),
     *         new Document("name", "Alice").append("age", 25));
     *
     * Dataset dataset = MongoDB.extractData(documents);
     * dataset.size();                                     // returns 2
     * dataset.getColumn("name");                          // returns [John, Alice]
     *
     * // An empty list yields an empty Dataset:
     * MongoDB.extractData(Collections.emptyList()).size(); // returns 0
     * }</pre>
     *
     * @param rowList the list of objects to convert to Dataset rows; may be empty (returns an empty Dataset) but must not be null
     * @return a Dataset containing the objects as Map-based rows
     * @throws NullPointerException if rowList is null
     * @see Dataset
     * @see #extractData(List, Class)
     */
    public static Dataset extractData(final List<?> rowList) {
        return extractData(rowList, Map.class);
    }

    /**
     * Extracts data from a list of objects into a Dataset with typed rows.
     *
     * <p>This method converts a list of objects into a Dataset structure with rows of the
     * specified type. Each object in the list is converted to the target row type, providing
     * type safety and enabling direct access to typed data. This is particularly useful when
     * you have pre-loaded data and need to work with it in a strongly-typed manner.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Document> docs = Arrays.asList(
     *         new Document("value", "a"),
     *         new Document("value", "b"));
     *
     * // Convert each row to the given type (here a bean with a "value" property):
     * Dataset data = MongoDB.extractData(docs, Account.class);
     * data.size();                                        // returns 2
     * List<Account> rows = data.toList(Account.class);
     *
     * // An empty list yields an empty Dataset:
     * MongoDB.extractData(Collections.emptyList(), Account.class).size(); // returns 0
     * }</pre>
     *
     * @param rowList the list of objects to convert to Dataset rows; may be empty (returns an empty Dataset) but must not be null
     * @param rowType the target type for each row in the Dataset
     * @return a Dataset containing the objects as typed rows
     * @throws NullPointerException if rowList is null
     * @see Dataset
     * @see #extractData(List)
     * @see #extractData(Collection, List, Class)
     */
    public static Dataset extractData(final List<?> rowList, final Class<?> rowType) {
        return extractData(null, rowList, rowType);
    }

    /**
     * Extracts selected properties from a list of objects into a Dataset with typed rows.
     *
     * <p>This method provides the most flexible data extraction by allowing both property selection
     * and target type specification. It converts a list of objects (typically MongoDB documents or Maps)
     * into a Dataset with only the specified properties, where each row is converted to the target type.
     * This is particularly useful for creating focused data views with specific columns and data types.</p>
     *
     * <p>The method handles different input types intelligently:</p>
     * <ul>
     * <li><strong>Map objects:</strong> Extracts specified keys and preserves Map structure if target type is Map</li>
     * <li><strong>Document objects:</strong> Converts to target type using the framework's type conversion system</li>
     * <li><strong>Other objects:</strong> Extracts the selected properties directly (no per-row conversion to the target type)</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Document> docs = Arrays.asList(
     *         new Document("name", "John").append("age", 30).append("city", "NYC"),
     *         new Document("name", "Alice").append("age", 25).append("city", "LA"));
     *
     * // Keep only the "name" and "city" columns:
     * Dataset ds = MongoDB.extractData(Arrays.asList("name", "city"), docs, Map.class);
     * ds.size();                                          // returns 2
     * ds.columnNames();                                   // returns [name, city]
     *
     * // null selectPropNames keeps every column:
     * Dataset all = MongoDB.extractData(null, docs, Map.class);
     * all.size();                                         // returns 2
     * }</pre>
     *
     * @param selectPropNames collection of property names to include in the Dataset (null to include all)
     * @param rowList the list of objects to extract data from; may be empty (returns an empty Dataset) but must not be null
     * @param rowType the target type for each row in the resulting Dataset
     * @return a Dataset containing the extracted properties as typed rows
     * @throws NullPointerException if rowList is null
     * @see Dataset
     * @see #extractData(List, Class)
     * @see #extractData(Collection, MongoIterable, Class)
     */
    public static Dataset extractData(final Collection<String> selectPropNames, final List<?> rowList, final Class<?> rowType) {
        final Optional<Object> first = N.firstNonNull(rowList);

        if (first.isPresent()) {
            /*
            if (Map.class.isAssignableFrom(first.get().getClass())) {
                if (N.isEmpty(selectPropNames)) {
                    final Set<String> columnNames = N.newLinkedHashSet();
                    @SuppressWarnings("rawtypes")
                    final List<Map<String, Object>> tmp = (List) rowList;
            
                    for (Map<String, Object> row : tmp) {
                        columnNames.addAll(row.keySet());
                    }
            
                    return N.newDataset(columnNames, rowList);
                } else {
                    return N.newDataset(selectPropNames, rowList);
                }
            } else {
                return N.newDataset(rowList);
            }
            */

            if (Map.class.isAssignableFrom(rowType) && Map.class.isAssignableFrom(first.get().getClass())) {
                if (N.isEmpty(selectPropNames)) {
                    final Set<String> columnNames = N.newLinkedHashSet();
                    @SuppressWarnings("rawtypes")
                    final List<Map<String, Object>> tmp = (List) rowList;

                    for (final Map<String, Object> row : tmp) {
                        columnNames.addAll(row.keySet());
                    }

                    return N.newDataset(columnNames, rowList);
                } else {
                    return N.newDataset(selectPropNames, rowList);
                }
            } else if (Document.class.isAssignableFrom(first.get().getClass())) {
                final List<Object> newRowList = new ArrayList<>(rowList.size());

                for (final Object row : rowList) {
                    newRowList.add(readRow((Document) row, rowType));
                }

                if (N.isEmpty(selectPropNames)) {
                    return N.newDataset(newRowList);
                } else {
                    return N.newDataset(selectPropNames, newRowList);
                }
            } else {
                // Mirror the branches above: null/empty selectPropNames means "include all"
                // (N.newDataset(columnNames, rows) rejects an empty columnNames with IAE).
                if (N.isEmpty(selectPropNames)) {
                    return N.newDataset(rowList);
                } else {
                    return N.newDataset(selectPropNames, rowList);
                }
            }
        } else {
            return N.newEmptyDataset();
        }
    }

    /**
     * Creates a Stream from a MongoIterable of Documents.
     *
     * <p>This method converts a MongoDB query result into a Java Stream, enabling functional
     * programming operations on the result set. The stream provides lazy evaluation and can
     * handle large result sets efficiently. The underlying MongoDB cursor will be properly
     * closed when the stream is closed.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Create a stream over the matched documents:
     * FindIterable<Document> findResult = collection.find(Filters.eq("status", "active"));
     * List<String> names = MongoDB.stream(findResult)
     *         .map(doc -> doc.getString("name"))
     *         .filter(name -> name.startsWith("A"))
     *         .toList();
     *
     * // The stream is backed by the iterable's cursor, which is closed on a terminal operation.
     * // An empty iterable produces an empty stream:
     * //     MongoDB.stream(emptyIterable).count()  ->  0
     * }</pre>
     *
     * @param iter the MongoIterable to convert to a Stream
     * @return a Stream of Document objects
     * @throws NullPointerException if iter is null
     * @see Stream
     * @see #stream(MongoIterable, Class)
     * @see #stream(MongoCursor)
     */
    public static Stream<Document> stream(final MongoIterable<Document> iter) {
        return stream(iter.iterator());
    }

    /**
     * Creates a Stream from a MongoIterable of Documents with automatic type conversion.
     *
     * <p>This method converts a MongoDB query result into a typed Java Stream, automatically
     * converting each Document to the specified target type. This provides type safety and
     * eliminates the need for manual conversion within stream operations. The conversion
     * handles entity mapping, Maps, and primitive types appropriately.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Each Document is converted to the target type as it is consumed:
     * FindIterable<Document> userDocs = collection.find();
     * List<String> emails = MongoDB.stream(userDocs, User.class)
     *         .filter(User::isActive)
     *         .map(User::getEmail)
     *         .toList();
     *
     * // Convert to Maps for flexible processing:
     * MongoDB.stream(userDocs, Map.class)
     *         .forEach(map -> processUserMap(map));
     *
     * // An empty iterable produces an empty stream:
     * //     MongoDB.stream(emptyIterable, User.class).count()  ->  0
     * }</pre>
     *
     * @param <T> the target type for stream elements
     * @param iter the MongoIterable to convert to a Stream
     * @param rowType the Class representing the target type for each stream element
     * @return a Stream of objects of the specified type
     * @throws NullPointerException if iter is null
     * @throws IllegalArgumentException if rowType is unsupported by {@code readRow}
     * @see Stream
     * @see #stream(MongoIterable)
     * @see #stream(MongoCursor, Class)
     */
    public static <T> Stream<T> stream(final MongoIterable<Document> iter, final Class<T> rowType) {
        return stream(iter.iterator(), rowType);
    }

    /**
     * Creates a Stream from a MongoCursor of Documents.
     *
     * <p>This method converts a MongoDB cursor into a Java Stream, providing a functional
     * programming interface for processing query results. The stream automatically manages
     * the cursor lifecycle, ensuring it is properly closed when the stream terminates.
     * This is particularly useful for processing large result sets with memory efficiency.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // The stream closes the cursor automatically on a terminal operation:
     * MongoCursor<Document> cursor = collection.find().iterator();
     * MongoDB.stream(cursor)
     *         .filter(doc -> doc.getInteger("age") > 18)
     *         .limit(100)
     *         .forEach(doc -> processAdult(doc));
     *
     * // Or manage it explicitly with try-with-resources:
     * try (Stream<Document> stream = MongoDB.stream(collection.find().iterator())) {
     *     List<String> names = stream.map(doc -> doc.getString("name")).toList();
     * }
     *
     * // An empty cursor produces an empty stream:
     * //     MongoDB.stream(emptyCursor).count()  ->  0
     * }</pre>
     *
     * @param cursor the MongoCursor to convert to a Stream
     * @return a Stream of Document objects with automatic cursor management
     * @see Stream
     * @see MongoCursor
     * @see #stream(MongoCursor, Class)
     */
    public static Stream<Document> stream(final MongoCursor<Document> cursor) {
        return Stream.of(cursor).onClose(Fn.close(cursor));
    }

    /**
     * Creates a Stream from a MongoCursor of Documents with automatic type conversion.
     *
     * <p>This method converts a MongoDB cursor into a typed Java Stream, automatically
     * converting each Document to the specified target type as it's consumed. The stream
     * provides both lazy evaluation and automatic cursor lifecycle management, making it
     * ideal for processing large datasets with type safety and memory efficiency.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Each Document is converted to the target type lazily; the cursor closes on a terminal op:
     * MongoCursor<Document> cursor = collection.find().iterator();
     * Optional<User> firstAdmin = MongoDB.stream(cursor, User.class)
     *         .filter(User::isActive)
     *         .filter(user -> "admin".equals(user.getRole()))
     *         .first();
     * firstAdmin.isPresent();   // true if a matching user was found
     *
     * // Convert rows to Maps:
     * try (Stream<Map<String, Object>> mapStream = MongoDB.stream(collection.find().iterator(), Map.class)) {
     *     long premium = mapStream.filter(map -> "premium".equals(map.get("tier"))).count();
     * }
     *
     * // An empty cursor produces an empty stream:
     * //     MongoDB.stream(emptyCursor, User.class).count()  ->  0
     * }</pre>
     *
     * @param <T> the target type for stream elements
     * @param cursor the MongoCursor to convert to a Stream
     * @param rowType the Class representing the target type for each stream element
     * @return a Stream of objects of the specified type with automatic cursor management
     * @throws IllegalArgumentException if rowType is unsupported by {@code readRow}
     * @see Stream
     * @see MongoCursor
     * @see #stream(MongoCursor)
     * @see #stream(MongoIterable, Class)
     */
    public static <T> Stream<T> stream(final MongoCursor<Document> cursor, final Class<T> rowType) {
        return Stream.of(cursor).map(it -> readRow(it, rowType)).onClose(Fn.close(cursor));
    }

    @SuppressWarnings("deprecation")
    private static Method getObjectIdSetMethod(final Class<?> rowType) {
        Method idSetMethod = classIdSetMethodPool.get(rowType);

        if (idSetMethod == null) {
            final List<String> idFieldNames = QueryUtil.getIdPropNames(rowType);
            Method idPropSetMethod = null;
            Class<?> parameterType = null;

            for (final String fieldName : idFieldNames) {
                idPropSetMethod = Beans.getPropSetter(rowType, fieldName);
                parameterType = idPropSetMethod == null ? null : idPropSetMethod.getParameterTypes()[0];

                if (parameterType != null && (String.class.isAssignableFrom(parameterType) || ObjectId.class.isAssignableFrom(parameterType))) {
                    idSetMethod = idPropSetMethod;

                    break;
                }
            }

            if (idSetMethod == null) {
                idPropSetMethod = Beans.getPropSetter(rowType, ID);
                parameterType = idPropSetMethod == null ? null : idPropSetMethod.getParameterTypes()[0];

                //            if (parameterType != null && (ObjectId.class.isAssignableFrom(parameterType) || String.class.isAssignableFrom(parameterType))) {
                //                idSetMethod = idPropSetMethod;
                //            }

                if (parameterType != null && (String.class.isAssignableFrom(parameterType) || ObjectId.class.isAssignableFrom(parameterType))) {
                    idSetMethod = idPropSetMethod;
                }
            }

            if (idSetMethod == null) {
                idSetMethod = ClassUtil.SENTINEL_METHOD;
            }

            classIdSetMethodPool.put(rowType, idSetMethod);
        }

        return idSetMethod == ClassUtil.SENTINEL_METHOD ? null : idSetMethod;
    }

    private static void resetObjectId(final Object obj, final Map<String, Object> doc) {
        String idPropertyName = _ID;
        final Class<?> cls = obj.getClass();

        if (Beans.isBeanClass(cls)) {
            final Method idSetMethod = getObjectIdSetMethod(cls);

            if (idSetMethod != null) {
                idPropertyName = Beans.getPropNameByMethod(idSetMethod);
            }
        }

        if (idPropertyName != null && doc.containsKey(idPropertyName)) {
            Object id = doc.remove(idPropertyName);

            // Convert to ObjectId only when the value unambiguously is one: a 24-hex-char String or a
            // 12-byte array. Any other legal _id value (an arbitrary String, a Date, other byte arrays)
            // is stored as-is. Converting unconditionally made any non-hex String id fail the whole write
            // with an IllegalArgumentException, and silently replaced a Date id with a freshly generated
            // (non-deterministic) ObjectId.
            if (id instanceof final String str && ObjectId.isValid(str)) {
                id = new ObjectId(str);
            } else if (id instanceof final byte[] bytes && bytes.length == 12) {
                id = new ObjectId(bytes);
            }

            if (id != null) {
                doc.put(_ID, id);
            } else {
                doc.remove(_ID);
            }
        }
    }

    private static void checkResultClass(final Class<?> rowType) {
        if (!(Beans.isBeanClass(rowType) || Map.class.isAssignableFrom(rowType))) {
            throw new IllegalArgumentException("The target class must be an entity class with getter/setter methods or Map.class/Document.class. But it is: "
                    + ClassUtil.getCanonicalClassName(rowType));
        }
    }

    /**
     * Internal {@link CodecRegistry} that lazily creates and caches {@link GeneralCodec} instances
     * for arbitrary Java types. Combined with the MongoDB default codec registry, this allows
     * arbitrary bean classes (and other types) to be (de)serialized to/from BSON.
     */
    static class GeneralCodecRegistry implements CodecRegistry {

        /** Codec cache keyed by the encoded class; populated on first request. */
        private static final Map<Class<?>, Codec<?>> pool = new ObjectPool<>(128);

        /**
         * Returns a {@link Codec} for {@code clazz}, creating and caching a new {@link GeneralCodec}
         * on the first lookup. Subsequent calls return the same cached instance.
         *
         * @param <T> the encoded Java type
         * @param clazz the class to obtain a codec for
         * @return a codec that handles {@code clazz}; never {@code null}
         */
        @Override
        public <T> Codec<T> get(final Class<T> clazz) {
            Codec<?> codec = pool.get(clazz);

            if (codec == null) {
                codec = new GeneralCodec<>(clazz);

                pool.put(clazz, codec);
            }

            return (Codec<T>) codec;
        }

        /**
         * Overload accepting a {@link CodecRegistry} hint; ignored here since this implementation
         * always uses its own {@link GeneralCodec}. Equivalent to {@link #get(Class)}.
         *
         * @param <T> the encoded Java type
         * @param clazz the class to obtain a codec for
         * @param registry the parent registry (ignored)
         * @return a codec that handles {@code clazz}; never {@code null}
         */
        @Override
        public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
            //    final Codec<T> codec = registry.get(clazz);
            //
            //    if (codec != null) {
            //        return codec;
            //    }

            return get(clazz);
        }
    }

    /**
     * Generic {@link Codec} that encodes bean-style entities as BSON documents (via
     * {@link MongoDBBase#toDocument(Object)}) and other types as their {@link N#stringOf(Object)}
     * string form. Decoding mirrors this: entity classes are read as Documents and then mapped to
     * the bean, while other types are read as strings and parsed via {@link N#valueOf(String, Class)}.
     */
    static class GeneralCodec<T> implements Codec<T> {

        /** Shared {@link DocumentCodec} used to encode/decode the BSON document representation of beans. */
        private static final DocumentCodec documentCodec = new DocumentCodec(codecRegistry, new BsonTypeClassMap());

        /** The Java class this codec handles. */
        private final Class<T> cls;

        /** {@code true} when {@link #cls} is a bean class; encoding switches between document and string forms accordingly. */
        private final boolean isEntityClass;

        /**
         * Creates a codec for the specified Java type.
         *
         * <p>The codec serializes bean-style entity classes as BSON documents and serializes
         * non-entity values as strings using {@code N.stringOf(Object)}.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Codec<MyEntity> codec = new MongoDBBase.GeneralCodec<>(MyEntity.class);
         * }</pre>
         *
         * @param cls the target Java type to encode/decode
         */
        public GeneralCodec(final Class<T> cls) {
            this.cls = cls;
            isEntityClass = Beans.isBeanClass(cls);
        }

        /**
         * Encodes {@code value} into the supplied BSON writer. Beans are first converted with
         * {@link MongoDBBase#toDocument(Object)} and written through the shared {@link DocumentCodec};
         * all other types are written as their {@code N.stringOf(Object)} string representation.
         *
         * @param writer destination writer
         * @param value the value to encode
         * @param encoderContext encoder context forwarded to the underlying document codec for bean values
         */
        @Override
        public void encode(final BsonWriter writer, final T value, final EncoderContext encoderContext) {
            if (isEntityClass) {
                documentCodec.encode(writer, toDocument(value), encoderContext);
            } else {
                writer.writeString(N.stringOf(value));
            }
        }

        /**
         * Decodes the next BSON value into an instance of {@link #cls}. Beans are read as a
         * {@link Document} through the shared {@link DocumentCodec} and then mapped via
         * {@link MongoDBBase#readRow(Document, Class)}; all other types are read as a BSON string and
         * parsed using {@code N.valueOf(String, Class)}.
         *
         * @param reader BSON reader positioned at the value to decode
         * @param decoderContext decoder context forwarded to the underlying document codec for bean values
         * @return the decoded value
         */
        @Override
        public T decode(final BsonReader reader, final DecoderContext decoderContext) {
            if (isEntityClass) {
                return readRow(documentCodec.decode(reader, decoderContext), cls);
            } else {
                return N.valueOf(reader.readString(), cls);
            }
        }

        /**
         * Returns the Java class this codec encodes.
         *
         * @return the encoded class; never {@code null}
         */
        @Override
        public Class<T> getEncoderClass() {
            return cls;
        }
    }

}
