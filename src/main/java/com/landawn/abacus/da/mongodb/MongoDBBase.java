package com.landawn.abacus.da.mongodb;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
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
 *   <li>Java String → BSON String</li>
 *   <li>Java primitives → BSON numeric types</li>
 *   <li>Java Date → BSON DateTime</li>
 *   <li>Java collections → BSON arrays</li>
 *   <li>Java Maps/POJOs → BSON documents</li>
 * </ul>
 *
 * @see Document
 * @see ObjectId
 * @see com.mongodb.client.MongoCollection
 * @see Codec
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
     * to MongoDB's "_id" field. When an entity doesn't have explicit @Id annotation,
     * the framework looks for a property named "id" to map to MongoDB's "_id".</p>
     */
    public static final String ID = "id";
    protected static final AsyncExecutor DEFAULT_ASYNC_EXECUTOR = new AsyncExecutor(//
            N.max(64, IOUtil.CPU_CORES * 8), // coreThreadPoolSize
            N.max(128, IOUtil.CPU_CORES * 16), // maxThreadPoolSize
            180L, TimeUnit.SECONDS);
    private static final JsonParser jsonParser = ParserFactory.createJsonParser();
    protected static final CodecRegistry codecRegistry = CodecRegistries.fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
            new GeneralCodecRegistry());
    private static final Map<Class<?>, Method> classIdSetMethodPool = new ConcurrentHashMap<>();

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
     * // Preferred approach:
     * public class User {
     *     @Id
     *     private String userId;  // Will be mapped to _id
     * }
     *
     * // Instead of:
     * MDB.registerIdProperty(User.class, "userId");
     * }</pre>
     *
     * @param documentClass the entity class to configure ID property mapping for
     * @param idPropertyName the name of the property to map to MongoDB's "_id" field
     * @throws IllegalArgumentException if the class lacks getter/setter methods for the property,
     *                                  or if the property type is not String or ObjectId
     * @see com.landawn.abacus.annotation.Id
     * @see ObjectId
     * @deprecated Use @Id annotation on the desired property instead of programmatic registration.
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
     * String id = "507f1f77bcf86cd799439011";
     * Bson filter = MDB.objectId2Filter(id);
     * Document doc = collection.find(filter).first();
     * }</pre>
     *
     * @param objectId the string representation of the ObjectId (24 hex characters)
     * @return a Bson filter document that matches the specified ObjectId
     * @throws IllegalArgumentException if objectId is null or empty
     * @see ObjectId
     * @see #objectId2Filter(ObjectId)
     */
    public static Bson objectId2Filter(final String objectId) {
        N.checkArgNotEmpty(objectId, "objectId");

        return objectId2Filter(new ObjectId(objectId));
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
     * ObjectId oid = new ObjectId();
     * Bson filter = MDB.objectId2Filter(oid);
     * collection.deleteOne(filter);
     * }</pre>
     *
     * @param objectId the ObjectId to create a filter for
     * @return a Bson filter document that matches the specified ObjectId
     * @throws IllegalArgumentException if objectId is null
     * @see Document
     * @see #objectId2Filter(String)
     */
    public static Bson objectId2Filter(final ObjectId objectId) {
        N.checkArgNotNull(objectId, "objectId");

        return new Document(MongoDB._ID, objectId);
    }

    /**
     * Creates an instance of the specified target class from a JSON string.
     *
     * <p>This method parses the provided JSON string and creates an instance of the specified BSON type.
     * It supports MongoDB's native BSON types including Document, BasicBSONObject, and BasicDBObject.
     * The JSON string should follow MongoDB's extended JSON format for proper parsing.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Create Document from JSON:
     * String userJson = "{\"name\": \"John\", \"age\": 30, \"active\": true}";
     * Document userDoc = MDB.fromJson(userJson, Document.class);
     *
     * // Create BasicBSONObject from JSON:
     * BasicBSONObject bsonObj = MDB.fromJson(userJson, BasicBSONObject.class);
     *
     * // MongoDB extended JSON with ObjectId:
     * String extendedJson = "{\"_id\": {\"$oid\": \"507f1f77bcf86cd799439011\"}, \"name\": \"John\"}";
     * Document doc = MDB.fromJson(extendedJson, Document.class);
     * }</pre>
     *
     * @param <T> the target BSON type
     * @param json the JSON string to parse (must be valid MongoDB extended JSON)
     * @param rowType the target class - must be one of: Bson.class, Document.class, BasicBSONObject.class, or BasicDBObject.class
     * @return an instance of the specified type populated with the JSON data
     * @throws IllegalArgumentException if rowType is not supported
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
     * converted to a BsonDocument and then to JSON format. The output follows MongoDB's
     * extended JSON format.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Convert Document to JSON:
     * Document doc = new Document("name", "John").append("age", 30);
     * String json = MDB.toJson(doc);
     * // Result: {"name": "John", "age": 30}
     *
     * // Convert filter to JSON:
     * Bson filter = Filters.and(Filters.eq("status", "active"), Filters.gte("age", 18));
     * String filterJson = MDB.toJson(filter);
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
     *
     * String json = MDB.toJson(bsonObj);
     * // Result: {"name": "Alice", "age": 25, "active": true}
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
     * dbObj.put("_id", new ObjectId());
     *
     * String json = MDB.toJson(dbObj);
     * // Result: {"name": "Charlie", "age": 28, "_id": ObjectId("...")}
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
     * // Convert entity to BSON:
     * User user = new User("John", "john@example.com", 30);
     * Bson userBson = MDB.toBson(user);
     *
     * // Convert Map to BSON:
     * Map<String, Object> userMap = Map.of("name", "Alice", "age", 25);
     * Bson mapBson = MDB.toBson(userMap);
     *
     * // Convert property pairs to BSON:
     * Bson pairsBson = MDB.toBson("name", "Bob", "age", 35, "active", true);
     * }</pre>
     *
     * @param obj the object to convert - can be an entity with getter/setter methods, {@code Map<String, Object>}, or array of property name-value pairs
     * @return a BSON document representation of the object
     * @throws IllegalArgumentException if obj cannot be converted to BSON
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
     * // Create BSON from property pairs:
     * Bson userBson = MDB.toBson("name", "John", "age", 30, "active", true);
     *
     * // Create BSON from single entity:
     * User user = new User("Alice", "alice@example.com", 25);
     * Bson entityBson = MDB.toBson(user);
     *
     * // Create empty BSON:
     * Bson emptyBson = MDB.toBson();
     * }</pre>
     *
     * @param a variable arguments representing property name-value pairs, or a single object to convert
     * @return a BSON document created from the specified parameters
     * @throws IllegalArgumentException if arguments are invalid or cannot be converted to BSON
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
     * // Convert entity to Document:
     * User user = new User("John", "john@example.com", 30);
     * Document userDoc = MDB.toDocument(user);
     *
     * // Convert Map to Document:
     * Map<String, Object> userMap = Map.of("name", "Alice", "age", 25);
     * Document mapDoc = MDB.toDocument(userMap);
     *
     * // The resulting Document can be used in MongoDB operations:
     * collection.insertOne(userDoc);
     * }</pre>
     *
     * @param obj the object to convert - can be an entity with getter/setter methods, {@code Map<String, Object>}, or array of property name-value pairs
     * @return a MongoDB Document representation of the object
     * @throws IllegalArgumentException if obj is null or cannot be converted to a Document
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
     * // Create Document from property pairs:
     * Document userDoc = MDB.toDocument("name", "John", "age", 30, "active", true);
     *
     * // Create Document from single entity:
     * User user = new User("Alice", 25);
     * Document entityDoc = MDB.toDocument(user);
     *
     * // Create empty Document:
     * Document emptyDoc = MDB.toDocument();
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
     * // Convert entity to BasicBSONObject:
     * User user = new User("John", "john@example.com", 30);
     * BasicBSONObject userBson = MDB.toBSONObject(user);
     *
     * // Convert Map to BasicBSONObject:
     * Map<String, Object> userMap = Map.of("name", "Alice", "age", 25);
     * BasicBSONObject mapBson = MDB.toBSONObject(userMap);
     * }</pre>
     *
     * @param obj the object to convert - can be an entity with getter/setter methods, {@code Map<String, Object>}, or array of property name-value pairs
     * @return a BasicBSONObject representation of the object
     * @throws IllegalArgumentException if obj is null or cannot be converted to BasicBSONObject
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
     * // Empty BasicBSONObject:
     * BasicBSONObject empty = MDB.toBSONObject();
     *
     * // Single object conversion:
     * User user = new User("Alice", 25);
     * BasicBSONObject userBson = MDB.toBSONObject(user);
     *
     * // Property pairs:
     * BasicBSONObject pairsBson = MDB.toBSONObject("name", "Bob", "age", 30);
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
     * // Convert entity to BasicDBObject:
     * User user = new User("John", "john@example.com", 30);
     * BasicDBObject userDb = MDB.toDBObject(user);
     *
     * // Convert Map to BasicDBObject:
     * Map<String, Object> userMap = Map.of("name", "Alice", "age", 25);
     * BasicDBObject mapDb = MDB.toDBObject(userMap);
     *
     * // Convert array to BasicDBObject:
     * Object[] userData = {"name", "Bob", "age", 35, "active", true};
     * BasicDBObject arrayDb = MDB.toDBObject(userData);
     * }</pre>
     *
     * @param obj the object to convert - can be an entity with getter/setter methods, {@code Map<String, Object>}, or array of property name-value pairs
     * @return a BasicDBObject representation of the object
     * @throws IllegalArgumentException if obj is null, cannot be converted to BasicDBObject, or array has odd number of elements
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
     * // Empty BasicDBObject:
     * BasicDBObject empty = MDB.toDBObject();
     *
     * // Single object conversion:
     * User user = new User("Alice", 25);
     * BasicDBObject userDb = MDB.toDBObject(user);
     *
     * // Property pairs for query building:
     * BasicDBObject query = MDB.toDBObject("status", "active", "age", Map.of("$gte", 18));
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
     * // Convert Document to Map:
     * Document userDoc = new Document("name", "John")
     *                       .append("age", 30)
     *                       .append("address", new Document("city", "NYC"));
     * Map<String, Object> userMap = MDB.toMap(userDoc);
     * // Result: {"name": "John", "age": 30, "address": {"city": "NYC"}}
     * }</pre>
     *
     * @param doc the MongoDB Document to convert; can be null
     * @return a Map representation of the document, or appropriate default if doc is null
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
     * // Using LinkedHashMap to preserve insertion order:
     * Document doc = new Document("name", "John").append("age", 30);
     * Map<String, Object> orderedMap = MDB.toMap(doc, LinkedHashMap::new);
     *
     * // Using TreeMap for sorted keys:
     * Map<String, Object> sortedMap = MDB.toMap(doc, size -> new TreeMap<>());
     *
     * // Pre-sized HashMap for performance:
     * Map<String, Object> preSizedMap = MDB.toMap(doc, HashMap::new);
     * }</pre>
     *
     * @param doc the MongoDB Document to convert; must not be null
     * @param mapSupplier a function that creates Map instances based on expected size
     * @return a Map representation of the document using the supplied Map type
     * @throws IllegalArgumentException if doc or mapSupplier is null
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
     * // Convert Document to User entity:
     * Document userDoc = new Document("_id", new ObjectId())
     *     .append("name", "John")
     *     .append("email", "john@example.com");
     *
     * User user = MDB.toEntity(userDoc, User.class);
     * // The user.getId() will contain the ObjectId from _id field
     *
     * // Works with String IDs too:
     * Document docWithStringId = new Document("_id", "user123")
     *     .append("name", "Alice");
     * User alice = MDB.toEntity(docWithStringId, User.class);
     * }</pre>
     *
     * @param <T> the target entity type
     * @param doc the MongoDB Document to convert
     * @param rowType the Class representing the target entity type
     * @return an entity instance populated with data from the document
     * @throws IllegalArgumentException if doc or rowType is null
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
            doc.put(_ID, objectId);
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
     * // Convert query results to User entities:
     * FindIterable<Document> userDocs = collection.find(Filters.eq("status", "active"));
     * List<User> users = MDB.toList(userDocs, User.class);
     *
     * // Convert to Maps:
     * List<Map<String, Object>> userMaps = MDB.toList(userDocs, Map.class);
     *
     * // Convert aggregation results to specific type:
     * AggregateIterable<Document> aggregated = collection.aggregate(pipeline);
     * List<UserStats> stats = MDB.toList(aggregated, UserStats.class);
     * }</pre>
     *
     * @param <T> the target type for list elements
     * @param findIterable the MongoDB query result to convert
     * @param rowType the target class - can be an entity class with getter/setter methods, Map.class, or basic single value type (Primitive/String/Date...)
     * @return a List containing all results converted to the specified type (empty list if no results)
     * @throws IllegalArgumentException if findIterable or rowType is null
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
                            resultList.add(Beans.copyInto(row, rowType));
                        }
                    }
                } else if (firstNonNull.get() instanceof Map && ((Map<String, Object>) firstNonNull.get()).size() <= 2) {
                    final Map<String, Object> m = (Map<String, Object>) firstNonNull.get();
                    final String propName = N.findFirst(m.keySet(), Fn.notEqual(_ID)).orElse(_ID);

                    if (m.get(propName) != null && rowType.isAssignableFrom(m.get(propName).getClass())) {
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
            throw new IllegalArgumentException("Unsupported target type: " + rowType);
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
     * // Extract user data into Dataset:
     * FindIterable<Document> userDocs = collection.find(Filters.eq("department", "Engineering"));
     * Dataset userData = MDB.extractData(userDocs);
     *
     * // Access data in tabular format:
     * userData.println();   // Print all data in table format
     * List<String> names = userData.getColumn("name");
     *
     * // Export to CSV or other formats:
     * userData.toCsv("users.csv");
     * }</pre>
     *
     * @param findIterable the MongoDB query result to extract data from
     * @return a Dataset containing the query results with Map-based rows
     * @throws IllegalArgumentException if findIterable is null
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
     * // Extract user data into Dataset with User objects:
     * FindIterable<Document> userDocs = collection.find(Filters.eq("active", true));
     * Dataset userData = MDB.extractData(userDocs, User.class);
     *
     * // Access typed data:
     * List<User> users = userData.toList(User.class);
     * User firstUser = userData.getRow(0, User.class);
     *
     * // Extract as String representations:
     * Dataset stringData = MDB.extractData(userDocs, String.class);
     * }</pre>
     *
     * @param findIterable the MongoDB query result to extract data from
     * @param rowType the target type for each row in the Dataset
     * @return a Dataset containing the query results with typed rows
     * @throws IllegalArgumentException if findIterable or rowType is null, or if rowType is unsupported
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
     * // Extract only specific fields:
     * Collection<String> fields = Arrays.asList("name", "email", "department");
     * FindIterable<Document> userDocs = collection.find();
     * Dataset limitedData = MDB.extractData(fields, userDocs, Map.class);
     *
     * // Create focused User objects with only selected properties:
     * Dataset userSummary = MDB.extractData(fields, userDocs, User.class);
     *
     * // Extract all properties when selectPropNames is null:
     * Dataset allData = MDB.extractData(null, userDocs, User.class);
     * }</pre>
     *
     * @param selectPropNames collection of property names to include in the Dataset; null to include all
     * @param findIterable the MongoDB query result to extract data from
     * @param rowType the target type for each row in the Dataset
     * @return a Dataset containing the selected properties with typed rows
     * @throws IllegalArgumentException if findIterable or rowType is null, or if rowType is unsupported
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
     * // Convert list of documents to Dataset:
     * List<Document> documents = Arrays.asList(
     *     new Document("name", "John").append("age", 30),
     *     new Document("name", "Alice").append("age", 25)
     * );
     * Dataset dataset = MDB.extractData(documents);
     *
     * // Access tabular data:
     * dataset.println();   // Print in table format
     * List<String> names = dataset.getColumn("name");
     * }</pre>
     *
     * @param rowList the list of objects to convert to Dataset rows
     * @return a Dataset containing the objects as Map-based rows
     * @throws IllegalArgumentException if rowList is null
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
     * // Convert list of documents to typed Dataset:
     * List<Document> userDocs = loadUserDocuments();
     * Dataset userData = MDB.extractData(userDocs, User.class);
     *
     * // Access typed rows:
     * List<User> users = userData.toList(User.class);
     * User firstUser = userData.getRow(0, User.class);
     *
     * // Convert to String representations:
     * Dataset stringData = MDB.extractData(userDocs, String.class);
     * }</pre>
     *
     * @param rowList the list of objects to convert to Dataset rows
     * @param rowType the target type for each row in the Dataset
     * @return a Dataset containing the objects as typed rows
     * @throws IllegalArgumentException if rowList or rowType is null, or if rowType is unsupported
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
     * <li><strong>Other objects:</strong> Uses property extraction and type conversion as appropriate</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Extract specific fields from Documents as Maps:
     * List<Document> docs = Arrays.asList(
     *     new Document("name", "John").append("age", 30).append("city", "NYC"),
     *     new Document("name", "Alice").append("age", 25).append("city", "LA")
     * );
     * Collection<String> fields = Arrays.asList("name", "city");
     * Dataset dataset = MDB.extractData(fields, docs, Map.class);
     *
     * // Extract all fields as String representations:
     * Dataset stringDataset = MDB.extractData(null, docs, String.class);
     *
     * // Create focused data view for reporting:
     * List<User> users = loadUsersFromDatabase();
     * Collection<String> reportFields = Arrays.asList("name", "email", "department");
     * Dataset reportData = MDB.extractData(reportFields, users, Map.class);
     * reportData.toCsv("user_report.csv");
     * }</pre>
     *
     * @param selectPropNames collection of property names to include in the Dataset (null to include all)
     * @param rowList the list of objects to extract data from
     * @param rowType the target type for each row in the resulting Dataset
     * @return a Dataset containing the extracted properties as typed rows
     * @throws IllegalArgumentException if rowList or rowType is null, or if rowType is unsupported
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
                return N.newDataset(selectPropNames, rowList);
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
     * // Create stream from find results:
     * FindIterable<Document> findResult = collection.find(Filters.eq("status", "active"));
     * Stream<Document> docStream = MDB.stream(findResult);
     *
     * // Use functional operations:
     * List<String> names = docStream
     *     .map(doc -> doc.getString("name"))
     *     .filter(name -> name.startsWith("A"))
     *     .collect(Collectors.toList());
     *
     * // Process aggregation results:
     * AggregateIterable<Document> aggResult = collection.aggregate(pipeline);
     * MDB.stream(aggResult)
     *    .forEach(doc -> processDocument(doc));
     * }</pre>
     *
     * @param iter the MongoIterable to convert to a Stream
     * @return a Stream of Document objects
     * @throws IllegalArgumentException if iter is null
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
     * // Create typed stream from find results:
     * FindIterable<Document> userDocs = collection.find();
     * Stream<User> userStream = MDB.stream(userDocs, User.class);
     *
     * // Use with typed operations:
     * List<String> emails = userStream
     *     .filter(user -> user.isActive())
     *     .map(User::getEmail)
     *     .collect(Collectors.toList());
     *
     * // Convert to Maps for flexible processing:
     * Stream<Map> mapStream = MDB.stream(userDocs, Map.class);
     * mapStream.forEach(map -> processUserMap(map));
     * }</pre>
     *
     * @param <T> the target type for stream elements
     * @param iter the MongoIterable to convert to a Stream
     * @param rowType the Class representing the target type for each stream element
     * @return a Stream of objects of the specified type
     * @throws IllegalArgumentException if iter or rowType is null, or if rowType is unsupported
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
     * // Create stream from cursor:
     * MongoCursor<Document> cursor = collection.find().iterator();
     * Stream<Document> docStream = MDB.stream(cursor);
     *
     * // Process documents lazily:
     * docStream
     *     .filter(doc -> doc.getInteger("age") > 18)
     *     .limit(100)
     *     .forEach(doc -> processAdult(doc));
     * // Cursor is automatically closed after stream operations
     *
     * // Use with try-with-resources for explicit management:
     * try (Stream<Document> stream = MDB.stream(cursor)) {
     *     return stream.map(doc -> doc.getString("name"))
     *                  .collect(Collectors.toList());
     * }
     * }</pre>
     *
     * @param cursor the MongoCursor to convert to a Stream
     * @return a Stream of Document objects with automatic cursor management
     * @throws IllegalArgumentException if cursor is null
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
     * // Create typed stream from cursor:
     * MongoCursor<Document> cursor = collection.find().iterator();
     * Stream<User> userStream = MDB.stream(cursor, User.class);
     *
     * // Process with type safety:
     * Optional<User> activeAdmin = userStream
     *     .filter(User::isActive)
     *     .filter(user -> "admin".equals(user.getRole()))
     *     .findFirst();
     * // Cursor is automatically closed
     *
     * // Convert to different types for processing:
     * MongoCursor<Document> anotherCursor = collection.find().iterator();
     * try (Stream<Map> mapStream = MDB.stream(anotherCursor, Map.class)) {
     *     return mapStream.filter(map -> "premium".equals(map.get("tier")))
     *                     .count();
     * }
     * }</pre>
     *
     * @param <T> the target type for stream elements
     * @param cursor the MongoCursor to convert to a Stream
     * @param rowType the Class representing the target type for each stream element
     * @return a Stream of objects of the specified type with automatic cursor management
     * @throws IllegalArgumentException if cursor or rowType is null, or if rowType is unsupported
     * @see Stream
     * @see MongoCursor
     * @see #stream(MongoCursor)
     * @see #stream(MongoIterable, Class)
     */
    public static <T> Stream<T> stream(final MongoCursor<Document> cursor, final Class<T> rowType) {
        return Stream.of(cursor).map(it -> readRow(it, rowType)).onClose(Fn.close(cursor));
    }

    @SuppressWarnings("deprecation")
    private static <T> Method getObjectIdSetMethod(final Class<T> rowType) {
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

            try {
                if (id instanceof String) {
                    id = new ObjectId((String) id);
                } else if (id instanceof Date) {
                    id = new ObjectId((Date) id);
                } else if (id instanceof byte[]) {
                    id = new ObjectId((byte[]) id);
                }
            } finally {
                if (id != null) {
                    doc.put(_ID, id);
                } else {
                    doc.remove(_ID);
                }
            }
        }
    }

    private static <T> void checkResultClass(final Class<T> rowType) {
        if (!(Beans.isBeanClass(rowType) || Map.class.isAssignableFrom(rowType))) {
            throw new IllegalArgumentException("The target class must be an entity class with getter/setter methods or Map.class/Document.class. But it is: "
                    + ClassUtil.getCanonicalClassName(rowType));
        }
    }

    static class GeneralCodecRegistry implements CodecRegistry {

        /** The Constant pool. */
        private static final Map<Class<?>, Codec<?>> pool = new ObjectPool<>(128);

        @Override
        public <T> Codec<T> get(final Class<T> clazz) {
            Codec<?> codec = pool.get(clazz);

            if (codec == null) {
                codec = new GeneralCodec<>(clazz);

                pool.put(clazz, codec);
            }

            return (Codec<T>) codec;
        }

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

    static class GeneralCodec<T> implements Codec<T> {

        /** The Constant documentCodec. */
        private static final DocumentCodec documentCodec = new DocumentCodec(codecRegistry, new BsonTypeClassMap());

        /** The cls. */
        private final Class<T> cls;

        /** The is entity class. */
        private final boolean isEntityClass;

        /**
         * Creates a codec for the specified Java type.
         *
         * <p>The codec serializes bean-style entity classes as BSON documents and serializes
         * non-entity values as strings using {@code N.stringOf(Object)}.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Codec<MyEntity> codec = new MDB.GeneralCodec<>(MyEntity.class);
         * }</pre>
         *
         * @param cls the target Java type to encode/decode
         */
        public GeneralCodec(final Class<T> cls) {
            this.cls = cls;
            isEntityClass = Beans.isBeanClass(cls);
        }

        @Override
        public void encode(final BsonWriter writer, final T value, final EncoderContext encoderContext) {
            if (isEntityClass) {
                documentCodec.encode(writer, toDocument(value), encoderContext);
            } else {
                writer.writeString(N.stringOf(value));
            }
        }

        @Override
        public T decode(final BsonReader reader, final DecoderContext decoderContext) {
            if (isEntityClass) {
                return readRow(documentCodec.decode(reader, decoderContext), cls);
            } else {
                return N.valueOf(reader.readString(), cls);
            }
        }

        @Override
        public Class<T> getEncoderClass() {
            return cls;
        }
    }

}
