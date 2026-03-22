
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

package com.landawn.abacus.da.aws.dynamodb;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.IntFunction;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.DeleteRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.landawn.abacus.parser.ParserUtil;
import com.landawn.abacus.parser.ParserUtil.BeanInfo;
import com.landawn.abacus.parser.ParserUtil.PropInfo;
import com.landawn.abacus.query.QueryUtil;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.Beans;
import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.Clazz;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.Fn;
import com.landawn.abacus.util.IOUtil;
import com.landawn.abacus.util.IntFunctions;
import com.landawn.abacus.util.InternalUtil;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.ObjIterator;
import com.landawn.abacus.util.RowDataset;
import com.landawn.abacus.util.SK;
import com.landawn.abacus.util.Strings;
import com.landawn.abacus.util.function.Function;
import com.landawn.abacus.util.stream.Stream;

/**
 * Synchronous DynamoDB executor providing comprehensive AWS DynamoDB operations with simplified API.
 * 
 * <p>This executor serves as a high-level wrapper around AWS DynamoDB SDK v1, offering both raw AttributeValue
 * operations and automatic object mapping for seamless interaction with DynamoDB tables. It provides
 * thread-safe operations and efficient handling of DynamoDB's unique characteristics including partition keys,
 * sort keys, and eventual consistency models.</p>
 *
 * <h2>Key Features and Concepts</h2>
 * <h3>Key Features:</h3>
 * <ul>
 * <li><b>Complete CRUD Operations</b> - Create, read, update, and delete items with support for conditional operations</li>
 * <li><b>Batch Operations</b> - Efficient batch get/write operations with automatic handling of 25-item limits</li>
 * <li><b>Query &amp; Scan</b> - Flexible querying with support for GSI/LSI, filtering, and pagination</li>
 * <li><b>Object Mapping</b> - Automatic conversion between Java objects and DynamoDB AttributeValues</li>
 * <li><b>Stream Processing</b> - Memory-efficient streaming for large result sets with automatic pagination</li>
 * <li><b>Async Support</b> - Built-in asynchronous execution via {@link #async()} method</li>
 * <li><b>Connection Management</b> - Proper resource management with AutoCloseable support</li>
 * </ul>
 * 
 * <h3>DynamoDB Concepts:</h3>
 * <ul>
 * <li><b>Partition Key</b> - Primary key component that determines item distribution across partitions</li>
 * <li><b>Sort Key</b> - Optional secondary key component for composite primary keys</li>
 * <li><b>Global Secondary Index (GSI)</b> - Alternative access patterns with different partition/sort keys</li>
 * <li><b>Local Secondary Index (LSI)</b> - Alternative sort key for same partition key</li>
 * <li><b>Eventual Consistency</b> - Default read consistency model; use consistentRead=true for strong consistency</li>
 * <li><b>Capacity Units</b> - Read/Write capacity consumption for performance and billing</li>
 * </ul>
 * 
 * <h3>Thread Safety:</h3>
 * <p>This class is thread-safe and can be safely used across multiple threads. The underlying DynamoDB client
 * maintains connection pooling and handles concurrent requests efficiently.</p>
 * 
 * <h3>Performance Considerations:</h3>
 * <ul>
 * <li>Batch operations are limited to 25 items for writes and 100 items for reads</li>
 * <li>Query operations are more efficient than Scan for targeted data retrieval</li>
 * <li>Use projection expressions to retrieve only required attributes</li>
 * <li>Consider using streams for large result sets to avoid memory issues</li>
 * </ul>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Initialize executor
 * DynamoDBExecutor executor = new DynamoDBExecutor(dynamoDBClient);
 * 
 * // Basic CRUD operations
 * User user = new User("123", "John Doe");
 * executor.mapper(User.class).putItem(user);
 * User retrieved = executor.mapper(User.class).getItem(user);
 * 
 * // Query with conditions
 * QueryRequest query = new QueryRequest()
 *     .withTableName("Users")
 *     .withKeyConditionExpression("userId = :userId")
 *     .withExpressionAttributeValues(Map.of(":userId", new AttributeValue().withS("123")));
 * List<User> users = executor.list(query, User.class);
 * }</pre>
 *
 * @see <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/">com.amazonaws.services.dynamodbv2.AmazonDynamoDB</a>
 * @see AsyncDynamoDBExecutor
 * @see <a href="https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/">DynamoDB Developer Guide</a>
 */
@SuppressWarnings("java:S1192")
public final class DynamoDBExecutor implements AutoCloseable {

    static {
        final BiFunction<AttributeValue, Class<?>, Object> converter = DynamoDBExecutor::toValue;

        N.registerConverter(AttributeValue.class, converter);
    }

    static final AsyncExecutor DEFAULT_ASYNC_EXECUTOR = new AsyncExecutor(//
            N.max(64, IOUtil.CPU_CORES * 8), // coreThreadPoolSize
            N.max(128, IOUtil.CPU_CORES * 16), // maxThreadPoolSize
            180L, TimeUnit.SECONDS);

    private final AmazonDynamoDBClient dynamoDBClient;

    private final DynamoDBMapper mapper;

    private final AsyncDynamoDBExecutor asyncDBExecutor;

    /**
     * Constructs a new DynamoDBExecutor with the specified DynamoDB client using default configuration.
     * 
     * <p>This constructor uses default DynamoDBMapperConfig and creates an internal async executor
     * with CPU-optimized thread pool settings for handling asynchronous operations. The executor
     * will use AWS SDK v1 with standard retry policies and connection pooling.</p>
     * 
     * <p><b>Default Configuration:</b></p>
     * <ul>
     * <li>Default DynamoDBMapperConfig with standard naming conventions</li>
     * <li>CPU-optimized async thread pool (max(64, CPU_CORES * 8) core threads)</li>
     * <li>Standard AWS SDK v1 retry and connection policies</li>
     * <li>Automatic resource management for connections</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AmazonDynamoDBClient dynamoClient = AmazonDynamoDBClientBuilder.standard()
     *     .withCredentials(credentialsProvider)
     *     .withRegion(Regions.US_EAST_1)
     *     .build();
     *
     * DynamoDBExecutor executor = new DynamoDBExecutor(dynamoClient);
     * }</pre>
     * 
     * @param dynamoDB the Amazon DynamoDB client to use for database operations. Must not be null.
     * @throws IllegalArgumentException if dynamoDB is null
     * @see #DynamoDBExecutor(AmazonDynamoDBClient, DynamoDBMapperConfig)
     * @see #DynamoDBExecutor(AmazonDynamoDBClient, DynamoDBMapperConfig, AsyncExecutor)
     */
    public DynamoDBExecutor(final AmazonDynamoDBClient dynamoDB) {
        this(dynamoDB, null);
    }

    /**
     * Constructs a new DynamoDBExecutor with specified DynamoDB client and mapper configuration.
     * 
     * <p>This constructor allows customization of DynamoDB mapping behavior including naming conventions,
     * table name overrides, and other mapper-specific settings while using the default async executor.
     * The configuration controls how Java objects are mapped to and from DynamoDB items.</p>
     * 
     * <p><b>Configuration Options:</b></p>
     * <ul>
     * <li>Naming policy for attribute name conversion</li>
     * <li>Table name overrides and prefixes</li>
     * <li>Consistent read settings</li>
     * <li>Pagination and batch size limits</li>
     * <li>Type converter customization</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DynamoDBMapperConfig config = DynamoDBMapperConfig.builder()
     *     .withTableNameOverride(DynamoDBMapperConfig.TableNameOverride.withTableNamePrefix("dev_"))
     *     .withConsistentReads(DynamoDBMapperConfig.ConsistentReads.CONSISTENT)
     *     .build();
     *
     * DynamoDBExecutor executor = new DynamoDBExecutor(dynamoClient, config);
     * }</pre>
     * 
     * @param dynamoDB the Amazon DynamoDB client to use for database operations. Must not be null.
     * @param config the DynamoDB mapper configuration for object mapping behavior, or null to use defaults
     * @throws IllegalArgumentException if dynamoDB is null
     * @see DynamoDBMapperConfig
     * @see #DynamoDBExecutor(AmazonDynamoDBClient)
     */
    public DynamoDBExecutor(final AmazonDynamoDBClient dynamoDB, final DynamoDBMapperConfig config) {
        this(dynamoDB, config, DEFAULT_ASYNC_EXECUTOR);
    }

    /**
     * Constructs a new DynamoDBExecutor with full customization of client, configuration, and async execution.
     * 
     * <p>This constructor provides complete control over all aspects of the executor including the underlying
     * DynamoDB client, object mapping configuration, and the async executor used for asynchronous operations.
     * The async executor should be properly configured for your application's concurrency requirements.</p>
     * 
     * <p><b>Custom Async Executor Benefits:</b></p>
     * <ul>
     * <li>Control over thread pool size and behavior</li>
     * <li>Custom task scheduling and queuing strategies</li>
     * <li>Integration with existing executor services</li>
     * <li>Resource sharing across application components</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncExecutor customAsyncExecutor = new AsyncExecutor(
     *     16,  // core threads
     *     32,  // max threads
     *     300L, TimeUnit.SECONDS  // keep alive time
     * );
     *
     * DynamoDBMapperConfig config = DynamoDBMapperConfig.builder()
     *     .withSaveBehavior(DynamoDBMapperConfig.SaveBehavior.UPDATE)
     *     .build();
     *
     * DynamoDBExecutor executor = new DynamoDBExecutor(dynamoClient, config, customAsyncExecutor);
     * }</pre>
     * 
     * @param dynamoDBClient the Amazon DynamoDB client to use for database operations. Must not be null.
     * @param config the DynamoDB mapper configuration for object mapping behavior, or null to use defaults
     * @param asyncExecutor the async executor for handling asynchronous operations. Must not be null.
     * @throws IllegalArgumentException if dynamoDBClient or asyncExecutor is null
     * @see AsyncExecutor
     * @see DynamoDBMapperConfig
     */
    public DynamoDBExecutor(final AmazonDynamoDBClient dynamoDBClient, final DynamoDBMapperConfig config, final AsyncExecutor asyncExecutor) {
        if (dynamoDBClient == null) {
            throw new IllegalArgumentException("dynamoDBClient cannot be null");
        }
        if (asyncExecutor == null) {
            throw new IllegalArgumentException("asyncExecutor cannot be null");
        }
        this.dynamoDBClient = dynamoDBClient;
        asyncDBExecutor = new AsyncDynamoDBExecutor(this, asyncExecutor);
        mapper = config == null ? new DynamoDBMapper(dynamoDBClient) : new DynamoDBMapper(dynamoDBClient, config);
    }

    /**
     * Returns the underlying Amazon DynamoDB client used by this executor.
     * 
     * <p>This provides direct access to the DynamoDB client for operations not covered by this executor
     * or for advanced configuration. Use with caution as direct client usage bypasses this executor's
     * object mapping and convenience features.</p>
     * 
     * <p><b>Direct Client Usage:</b></p>
     * <ul>
     * <li>Advanced table management operations (create, delete, describe tables)</li>
     * <li>Index management and global secondary index operations</li>
     * <li>Stream processing and change data capture</li>
     * <li>Custom request configurations not supported by executor</li>
     * <li>AWS SDK v1 specific features and optimizations</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AmazonDynamoDBClient client = executor.dynamoDBClient();
     *
     * // Direct table operations
     * ListTablesResult tables = client.listTables();
     *
     * // Advanced configuration
     * client.setTimeOffset(1000);
     * }</pre>
     * 
     * @return the AmazonDynamoDBClient instance used by this executor, never null
     * @see AmazonDynamoDBClient
     */
    public AmazonDynamoDBClient dynamoDBClient() {
        return dynamoDBClient;
    }

    /**
     * Returns the default DynamoDB mapper configured for this executor.
     * 
     * <p>The mapper handles automatic conversion between Java objects and DynamoDB AttributeValues,
     * including support for annotations like @DynamoDBTable, @DynamoDBHashKey, @DynamoDBAttribute, etc.
     * This is the same mapper used internally by the executor's entity-based operations.</p>
     * 
     * <p><b>Mapper Capabilities:</b></p>
     * <ul>
     * <li>Object-to-item mapping using annotations</li>
     * <li>Automatic type conversion for primitives, collections, and custom types</li>
     * <li>Support for inheritance and polymorphism</li>
     * <li>Batch operations with automatic pagination</li>
     * <li>Optimistic locking with version fields</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DynamoDBMapper mapper = executor.dynamoDBMapper();
     *
     * // Direct mapper operations
     * User user = mapper.load(User.class, "userId123");
     * mapper.save(user);
     *
     * // Batch operations
     * List<User> users = mapper.batchLoad(userIds);
     * }</pre>
     * 
     * @return the DynamoDBMapper instance configured with this executor's settings, never null
     * @see DynamoDBMapper
     * @see #dynamoDBMapper(DynamoDBMapperConfig)
     */
    public DynamoDBMapper dynamoDBMapper() {
        return mapper;
    }

    /**
     * Creates a new DynamoDB mapper with the specified configuration.
     * 
     * <p>This allows creating mappers with different configurations than the default one used by this executor.
     * Useful for operations requiring different naming policies, table name overrides, or other mapper-specific settings.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DynamoDBMapperConfig config = DynamoDBMapperConfig.builder()
     *     .withSaveBehavior(DynamoDBMapperConfig.SaveBehavior.CLOBBER)
     *     .build();
     * DynamoDBMapper customMapper = executor.dynamoDBMapper(config);
     * }</pre>
     * 
     * @param config the DynamoDB mapper configuration to use. Must not be null.
     * @return a new DynamoDBMapper instance configured with the specified settings, never null
     * @throws IllegalArgumentException if config is null
     */
    public DynamoDBMapper dynamoDBMapper(final DynamoDBMapperConfig config) {
        return new DynamoDBMapper(dynamoDBClient, config);
    }

    @SuppressWarnings("rawtypes")
    private final Map<Class<?>, Mapper> mapperPool = new ConcurrentHashMap<>();

    /**
     * Creates a type-safe mapper for the specified entity class with automatic table name detection.
     * 
     * <p>This method creates a cached mapper that provides type-safe operations for a specific entity class.
     * The table name is automatically derived from @Table annotations on the class. The mapper uses
     * CAMEL_CASE naming policy by default for attribute name conversion.</p>
     * 
     * <p>Entity classes must be annotated with @Table, @javax.persistence.Table, or @jakarta.persistence.Table
     * to specify the DynamoDB table name. ID fields must be annotated with appropriate key annotations.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * @Table("Users")
     * public class User {
     *     @Id private String userId;
     *     private String name;
     *     // getters/setters
     * }
     *
     * Mapper<User> userMapper = executor.mapper(User.class);
     * User user = userMapper.getItem(asKey("userId", "123"));
     * }</pre>
     * 
     * @param <T> the entity type
     * @param targetEntityClass the entity class to create mapper for. Must be annotated with @Table. Must not be null.
     * @return a cached Mapper instance for the specified entity class, never null
     * @throws IllegalArgumentException if targetEntityClass is null, not a bean class, or missing @Table annotation
     */
    public <T> Mapper<T> mapper(final Class<T> targetEntityClass) {
        @SuppressWarnings("unchecked")
        Mapper<T> result = mapperPool.computeIfAbsent(targetEntityClass, cls -> {
            final BeanInfo entityInfo = ParserUtil.getBeanInfo(cls);

            if (entityInfo.tableName.isEmpty()) {
                throw new IllegalArgumentException("Entity class " + cls
                        + " must be annotated with @Table (com.landawn.abacus.annotation, javax.persistence, or jakarta.persistence). Alternatively, use DynamoDBExecutor.mapper(String tableName, Class<T> entityClass)");
            }

            return mapper(cls, entityInfo.tableName.get(), NamingPolicy.CAMEL_CASE);
        });

        return result;
    }

    /**
     * Creates a type-safe mapper for the specified entity class with explicit table name and naming policy.
     * 
     * <p>This method creates a mapper with full customization of table name and attribute naming policy.
     * Unlike the single-parameter version, this doesn't require @Table annotations and allows complete
     * control over table mapping. Each call creates a new mapper instance (not cached).</p>
     * 
     * <p>The naming policy controls how Java property names are converted to DynamoDB attribute names:</p>
     * <ul>
     * <li>CAMEL_CASE - "userName" → "userName"</li>
     * <li>UPPER_CAMEL_CASE - "userName" → "UserName"</li>
     * <li>SNAKE_CASE - "userName" → "user_name"</li>
     * <li>SCREAMING_SNAKE_CASE - "userName" → "USER_NAME"</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Mapper<Product> productMapper = executor.mapper(
     *     Product.class, 
     *     "dev_products", 
     *     NamingPolicy.SNAKE_CASE
     * );
     * productMapper.putItem(new Product("PROD-123", "Widget"));
     * }</pre>
     * 
     * @param <T> the entity type
     * @param targetEntityClass the entity class to create mapper for. Must be a valid bean class. Must not be null.
     * @param tableName the DynamoDB table name to use for operations. Must not be null or empty.
     * @param namingPolicy the naming policy for converting property names to attribute names. Must not be null.
     * @return a new Mapper instance configured with the specified parameters, never null
     * @throws IllegalArgumentException if any parameter is null, targetEntityClass is not a bean class, or tableName is empty
     */
    public <T> Mapper<T> mapper(final Class<T> targetEntityClass, final String tableName, final NamingPolicy namingPolicy) {
        return new Mapper<>(targetEntityClass, this, tableName, namingPolicy);
    }

    /**
     * Returns an asynchronous version of this DynamoDB executor for non-blocking operations.
     * 
     * <p>The returned AsyncDynamoDBExecutor provides the same functionality as this synchronous executor
     * but returns CompletableFuture instances for all operations, allowing for asynchronous and reactive
     * programming patterns. All async operations share the same underlying DynamoDB client and configuration.</p>
     * 
     * <p>The async executor uses a dedicated thread pool configured during construction with CPU-optimized
     * settings (core threads = max(64, CPU_CORES * 8), max threads = max(128, CPU_CORES * 16)).</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CompletableFuture<User> future = executor.async().getItem(tableName, key, User.class);
     * future.thenApply(user -> {
     *     // Process user asynchronously
     *     return user.getName();
     * }).thenAccept(System.out::println);
     * }</pre>
     * 
     * @return an AsyncDynamoDBExecutor instance sharing this executor's configuration, never null
     */
    public AsyncDynamoDBExecutor async() {
        return asyncDBExecutor;
    }

    /**
     * Converts a Java object to a DynamoDB AttributeValue with automatic type detection.
     * 
     * <p>This method performs intelligent type mapping to convert Java objects into appropriate DynamoDB
     * AttributeValue instances based on the object's runtime type:</p>
     * 
     * <ul>
     * <li><b>null</b> → AttributeValue with NULL=true</li>
     * <li><b>Number types</b> (Integer, Long, Double, BigDecimal, etc.) → N (Number) attribute</li>
     * <li><b>Boolean</b> → BOOL (Boolean) attribute</li>
     * <li><b>ByteBuffer</b> → B (Binary) attribute</li>
     * <li><b>All other types</b> → S (String) attribute using string conversion</li>
     * </ul>
     * 
     * <p><b>Important:</b> This method does NOT handle complex types like Lists, Maps, or Sets.
     * For complex AttributeValue creation, use the DynamoDB SDK's AttributeValue.builder() methods.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AttributeValue stringAttr = attrValueOf("Hello World");   // S: "Hello World"
     * AttributeValue numberAttr = attrValueOf(42);              // N: "42"
     * AttributeValue boolAttr = attrValueOf(true);              // BOOL: true
     * AttributeValue nullAttr = attrValueOf(null);              // NULL: true
     * }</pre>
     * 
     * @param value the Java object to convert, can be null
     * @return an AttributeValue representing the input value with appropriate type mapping, never null
     */
    public static AttributeValue attrValueOf(final Object value) {
        final AttributeValue attrVal = new AttributeValue();

        if (value == null) {
            attrVal.withNULL(Boolean.TRUE);
        } else {
            final Type<Object> type = N.typeOf(value.getClass());

            if (type.isNumber()) {
                attrVal.setN(type.stringOf(value));
            } else if (type.isBoolean()) {
                attrVal.setBOOL((Boolean) value);
            } else if (type.isByteBuffer()) {
                attrVal.setB((ByteBuffer) value);
            } else {
                attrVal.setS(type.stringOf(value));
            }
        }

        return attrVal;
    }

    /**
     * Creates an AttributeValueUpdate with PUT action for the specified value.
     * 
     * <p>This convenience method creates an AttributeValueUpdate using the default PUT action,
     * which replaces the existing attribute value with the new value. The input value is automatically
     * converted to an AttributeValue using the same rules as {@link #attrValueOf(Object)}.</p>
     * 
     * <p>This is equivalent to calling {@code attrValueUpdateOf(value, AttributeAction.PUT)}.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValueUpdate> updates = new HashMap<>();
     * updates.put("name", attrValueUpdateOf("John Doe"));   // PUT action
     * updates.put("age", attrValueUpdateOf(30));            // PUT action
     * }</pre>
     * 
     * @param value the value to create AttributeValueUpdate for, can be null
     * @return an AttributeValueUpdate with PUT action containing the converted value, never null
     * @see #attrValueUpdateOf(Object, AttributeAction)
     * @see #attrValueOf(Object)
     */
    public static AttributeValueUpdate attrValueUpdateOf(final Object value) {
        return attrValueUpdateOf(value, AttributeAction.PUT);
    }

    /**
     * Creates an AttributeValueUpdate with the specified action and value.
     * 
     * <p>This method provides full control over AttributeValueUpdate creation by allowing specification
     * of both the value and the update action. Common actions include:</p>
     * 
     * <ul>
     * <li><b>PUT</b> - Replace the attribute value (default behavior)</li>
     * <li><b>ADD</b> - Add to numeric values or add elements to sets</li>
     * <li><b>DELETE</b> - Remove the attribute or remove elements from sets</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Replace value
     * AttributeValueUpdate put = attrValueUpdateOf("new value", AttributeAction.PUT);
     * 
     * // Add to numeric value
     * AttributeValueUpdate add = attrValueUpdateOf(5, AttributeAction.ADD);
     * 
     * // Delete attribute
     * AttributeValueUpdate delete = attrValueUpdateOf(null, AttributeAction.DELETE);
     * }</pre>
     * 
     * @param value the value for the update operation, can be null for DELETE actions
     * @param action the update action to perform. Must not be null.
     * @return an AttributeValueUpdate with the specified action and converted value, never null
     * @throws IllegalArgumentException if action is null
     * @see #attrValueOf(Object)
     */
    public static AttributeValueUpdate attrValueUpdateOf(final Object value, final AttributeAction action) {
        return new AttributeValueUpdate(attrValueOf(value), action);
    }

    /**
     * Creates a single-attribute key map for DynamoDB operations.
     * 
     * <p>This convenience method creates a key map with a single partition key, commonly used
     * for simple primary keys. The value is automatically converted to an AttributeValue.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> key = asKey("userId", "user123");
     * // Results in: {"userId": AttributeValue.builder().s("user123").build()}
     * }</pre>
     * 
     * @param keyName the name of the key attribute (usually partition key). Must not be null.
     * @param value the value for the key attribute, can be null
     * @return a Map containing the single key-value pair as AttributeValue, never null
     * @throws IllegalArgumentException if keyName is null
     * @see #asKey(String, Object, String, Object)
     */
    public static Map<String, AttributeValue> asKey(final String keyName, final Object value) {
        return asItem(keyName, value);
    }

    /**
     * Creates a composite key map with partition key and sort key for DynamoDB operations.
     * 
     * <p>This convenience method creates a key map with both partition key and sort key,
     * commonly used for composite primary keys in DynamoDB tables. Both values are
     * automatically converted to AttributeValues.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> key = asKey("userId", "user123", "timestamp", 1640995200L);
     * // Results in: {
     * //   "userId": AttributeValue.builder().s("user123").build(),
     * //   "timestamp": AttributeValue.builder().n("1640995200").build()
     * // }
     * }</pre>
     * 
     * @param keyName the name of the partition key attribute. Must not be null.
     * @param value the value for the partition key, can be null
     * @param keyName2 the name of the sort key attribute. Must not be null.
     * @param value2 the value for the sort key, can be null
     * @return a Map containing both key-value pairs as AttributeValues, never null
     * @throws IllegalArgumentException if keyName or keyName2 is null
     * @see #asKey(String, Object)
     */
    public static Map<String, AttributeValue> asKey(final String keyName, final Object value, final String keyName2, final Object value2) {
        return asItem(keyName, value, keyName2, value2);
    }

    /**
     * Creates a three-attribute key map for complex DynamoDB operations.
     * 
     * <p>This method creates a key map with three attributes, useful for complex queries
     * involving Global Secondary Indexes (GSI) or when building filter conditions.
     * All values are automatically converted to AttributeValues.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> key = asKey(
     *     "partitionKey", "value1", 
     *     "sortKey", "value2", 
     *     "gsiKey", "value3"
     * );
     * }</pre>
     * 
     * @param keyName the name of the first key attribute. Must not be null.
     * @param value the value for the first key, can be null
     * @param keyName2 the name of the second key attribute. Must not be null.
     * @param value2 the value for the second key, can be null
     * @param keyName3 the name of the third key attribute. Must not be null.
     * @param value3 the value for the third key, can be null
     * @return a Map containing all three key-value pairs as AttributeValues, never null
     * @throws IllegalArgumentException if any keyName is null
     */
    public static Map<String, AttributeValue> asKey(final String keyName, final Object value, final String keyName2, final Object value2, final String keyName3,
            final Object value3) {
        return asItem(keyName, value, keyName2, value2, keyName3, value3);
    }

    /**
     * Creates a key map from variable arguments in name-value pairs.
     * 
     * <p>This flexible method accepts alternating attribute names and values to create
     * a key map. Arguments must be provided in pairs (name1, value1, name2, value2, ...).
     * All values are automatically converted to AttributeValues.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> key = asKey(
     *     "userId", "user123",
     *     "timestamp", System.currentTimeMillis(),
     *     "type", "MESSAGE"
     * );
     * }</pre>
     * 
     * @param a variable arguments in name-value pairs. Must have even number of arguments.
     * @return a Map containing all key-value pairs as AttributeValues, never null
     * @throws IllegalArgumentException if argument count is not even (must be name-value pairs)
     */
    public static Map<String, AttributeValue> asKey(final Object... a) {
        return asItem(a);
    }

    /**
     * Creates a single-attribute item map for DynamoDB operations.
     * 
     * <p>This convenience method creates an item map with a single attribute, useful for
     * simple PutItem operations or when building items incrementally. The value is
     * automatically converted to an appropriate AttributeValue based on its type.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> item = asItem("email", "user@example.com");
     * // Results in: {"email": AttributeValue.builder().s("user@example.com").build()}
     * 
     * // Can be extended with putAll
     * item.putAll(asItem("age", 25));
     * }</pre>
     * 
     * @param attrName the name of the attribute. Must not be {@code null}.
     * @param value the value for the attribute, automatically converted to AttributeValue
     * @return a LinkedHashMap containing the single attribute-value pair, never {@code null}
     * @throws IllegalArgumentException if attrName is {@code null}
     * @see #asItem(String, Object, String, Object) for multiple attributes
     * @see #attrValueOf(Object) for value conversion rules
     */
    public static Map<String, AttributeValue> asItem(final String attrName, final Object value) {
        return N.asMap(attrName, attrValueOf(value));
    }

    /**
     * Creates a two-attribute item map for DynamoDB operations.
     * 
     * <p>This convenience method creates an item map with two attributes, commonly used
     * for items with both required attributes like partition key and sort key.
     * Values are automatically converted to appropriate AttributeValues.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> item = asItem(
     *     "userId", "user123",
     *     "email", "user@example.com"
     * );
     * // Results in: {
     * //   "userId": AttributeValue.builder().s("user123").build(),
     * //   "email": AttributeValue.builder().s("user@example.com").build()
     * // }
     * }</pre>
     * 
     * @param attrName the name of the first attribute. Must not be {@code null}.
     * @param value the value for the first attribute
     * @param attrName2 the name of the second attribute. Must not be {@code null}.
     * @param value2 the value for the second attribute
     * @return a LinkedHashMap containing both attribute-value pairs, never {@code null}
     * @throws IllegalArgumentException if any attribute name is {@code null}
     * @see #asItem(String, Object, String, Object, String, Object) for three attributes
     */
    public static Map<String, AttributeValue> asItem(final String attrName, final Object value, final String attrName2, final Object value2) {
        return N.asMap(attrName, attrValueOf(value), attrName2, attrValueOf(value2));
    }

    /**
     * Creates a three-attribute item map for DynamoDB operations.
     * 
     * <p>This convenience method creates an item map with three attributes, useful for
     * items with multiple required fields. Values are automatically converted to
     * appropriate AttributeValues based on their types.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> item = asItem(
     *     "userId", "user123",
     *     "timestamp", System.currentTimeMillis(),
     *     "status", "ACTIVE"
     * );
     * // Results in: {
     * //   "userId": AttributeValue.builder().s("user123").build(),
     * //   "timestamp": AttributeValue.builder().n("1640995200000").build(),
     * //   "status": AttributeValue.builder().s("ACTIVE").build()
     * // }
     * }</pre>
     * 
     * @param attrName the name of the first attribute. Must not be {@code null}.
     * @param value the value for the first attribute
     * @param attrName2 the name of the second attribute. Must not be {@code null}.
     * @param value2 the value for the second attribute
     * @param attrName3 the name of the third attribute. Must not be {@code null}.
     * @param value3 the value for the third attribute
     * @return a LinkedHashMap containing all three attribute-value pairs, never {@code null}
     * @throws IllegalArgumentException if any attribute name is {@code null}
     * @see #asItem(Object...) for variable number of attributes
     */
    public static Map<String, AttributeValue> asItem(final String attrName, final Object value, final String attrName2, final Object value2,
            final String attrName3, final Object value3) {
        return N.asMap(attrName, attrValueOf(value), attrName2, attrValueOf(value2), attrName3, attrValueOf(value3));
    }

    /**
     * Creates an item map from variable arguments in name-value pairs.
     * 
     * <p>This flexible method accepts alternating attribute names and values to create
     * an item map for DynamoDB operations. Arguments must be provided in pairs
     * (name1, value1, name2, value2, ...). All values are automatically converted
     * to appropriate AttributeValues based on their types.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> item = asItem(
     *     "userId", "user123",
     *     "age", 30,
     *     "premium", true,
     *     "balance", new BigDecimal("99.99")
     * );
     * // Results in: {
     * //   "userId": AttributeValue.builder().s("user123").build(),
     * //   "age": AttributeValue.builder().n("30").build(),
     * //   "premium": AttributeValue.builder().bool(true).build(),
     * //   "balance": AttributeValue.builder().n("99.99").build()
     * // }
     * }</pre>
     * 
     * @param a variable arguments in name-value pairs. Must have even number of arguments.
     *          Odd-indexed arguments must be Strings (attribute names).
     * @return a LinkedHashMap containing all attribute-value pairs, never {@code null}
     * @throws IllegalArgumentException if argument count is not even or if odd-indexed
     *         arguments are not Strings
     */
    public static Map<String, AttributeValue> asItem(final Object... a) {
        if ((a.length % 2) != 0) {
            throw new IllegalArgumentException("Parameters must be property name-value pairs, a Map, or an entity with getter/setter methods");
        }

        final Map<String, AttributeValue> item = N.newLinkedHashMap(a.length / 2);

        for (int i = 0; i < a.length; i++) {
            item.put((String) a[i], attrValueOf(a[++i]));
        }

        return item;
    }

    /**
     * Creates a single-attribute update map for DynamoDB UpdateItem operations.
     * 
     * <p>This convenience method creates an update map with a single attribute using
     * the default PUT action. The value is automatically converted to an AttributeValue
     * and wrapped in an AttributeValueUpdate.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValueUpdate> updates = asUpdateItem("lastModified", Instant.now());
     * // Results in: {
     * //   "lastModified": AttributeValueUpdate with PUT action
     * // }
     * 
     * UpdateItemResult result = executor.updateItem("Users", key, updates);
     * }</pre>
     * 
     * @param attrName the name of the attribute to update. Must not be {@code null}.
     * @param value the new value for the attribute, automatically converted
     * @return a LinkedHashMap containing the single attribute update, never {@code null}
     * @throws IllegalArgumentException if attrName is {@code null}
     * @see #asUpdateItem(String, Object, String, Object) for multiple updates
     * @see #attrValueUpdateOf(Object) for update creation
     */
    public static Map<String, AttributeValueUpdate> asUpdateItem(final String attrName, final Object value) {
        return N.asMap(attrName, attrValueUpdateOf(value));
    }

    /**
     * Creates a two-attribute update map for DynamoDB UpdateItem operations.
     * 
     * <p>This convenience method creates an update map with two attributes, both using
     * the default PUT action. Values are automatically converted to AttributeValues
     * and wrapped in AttributeValueUpdate objects.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValueUpdate> updates = asUpdateItem(
     *     "email", "newemail@example.com",
     *     "lastModified", System.currentTimeMillis()
     * );
     * // Results in: {
     * //   "email": AttributeValueUpdate with PUT action,
     * //   "lastModified": AttributeValueUpdate with PUT action
     * // }
     * 
     * UpdateItemResult result = executor.updateItem("Users", key, updates);
     * }</pre>
     * 
     * @param attrName the name of the first attribute to update. Must not be {@code null}.
     * @param value the new value for the first attribute
     * @param attrName2 the name of the second attribute to update. Must not be {@code null}.
     * @param value2 the new value for the second attribute
     * @return a LinkedHashMap containing both attribute updates, never {@code null}
     * @throws IllegalArgumentException if any attribute name is {@code null}
     * @see #asUpdateItem(String, Object, String, Object, String, Object) for three updates
     */
    public static Map<String, AttributeValueUpdate> asUpdateItem(final String attrName, final Object value, final String attrName2, final Object value2) {
        return N.asMap(attrName, attrValueUpdateOf(value), attrName2, attrValueUpdateOf(value2));
    }

    /**
     * Creates a three-attribute update map for DynamoDB UpdateItem operations.
     * 
     * <p>This convenience method creates an update map with three attributes, all using
     * the default PUT action. Values are automatically converted to AttributeValues
     * and wrapped in AttributeValueUpdate objects.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValueUpdate> updates = asUpdateItem(
     *     "status", "VERIFIED",
     *     "verifiedAt", Instant.now().toEpochMilli(),
     *     "verifiedBy", "admin123"
     * );
     * // Results in: {
     * //   "status": AttributeValueUpdate with PUT action,
     * //   "verifiedAt": AttributeValueUpdate with PUT action,
     * //   "verifiedBy": AttributeValueUpdate with PUT action
     * // }
     * 
     * UpdateItemResult result = executor.updateItem("Users", key, updates);
     * }</pre>
     * 
     * @param attrName the name of the first attribute to update. Must not be {@code null}.
     * @param value the new value for the first attribute
     * @param attrName2 the name of the second attribute to update. Must not be {@code null}.
     * @param value2 the new value for the second attribute
     * @param attrName3 the name of the third attribute to update. Must not be {@code null}.
     * @param value3 the new value for the third attribute
     * @return a LinkedHashMap containing all three attribute updates, never {@code null}
     * @throws IllegalArgumentException if any attribute name is {@code null}
     * @see #asUpdateItem(Object...) for variable number of updates
     */
    public static Map<String, AttributeValueUpdate> asUpdateItem(final String attrName, final Object value, final String attrName2, final Object value2,
            final String attrName3, final Object value3) {
        return N.asMap(attrName, attrValueUpdateOf(value), attrName2, attrValueUpdateOf(value2), attrName3, attrValueUpdateOf(value3));
    }

    /**
     * Creates an update item map from variable arguments in name-value pairs.
     * 
     * <p>This flexible method accepts alternating attribute names and values to create
     * an update map for DynamoDB UpdateItem operations. All updates use the default PUT action.
     * Arguments must be provided in pairs (name1, value1, name2, value2, ...).</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValueUpdate> updates = asUpdateItem(
     *     "status", "ACTIVE",
     *     "lastLogin", Instant.now().toString(),
     *     "loginCount", 42
     * );
     * }</pre>
     * 
     * @param a variable arguments in name-value pairs. Must have even number of arguments.
     * @return a LinkedHashMap containing all attribute updates with PUT action, never null
     * @throws IllegalArgumentException if argument count is not even
     */
    public static Map<String, AttributeValueUpdate> asUpdateItem(final Object... a) {
        if ((a.length % 2) != 0) {
            throw new IllegalArgumentException("Parameters must be property name-value pairs, a Map, or an entity with getter/setter methods");
        }

        final Map<String, AttributeValueUpdate> item = N.newLinkedHashMap(a.length / 2);

        for (int i = 0; i < a.length; i++) {
            item.put((String) a[i], attrValueUpdateOf(a[++i]));
        }

        return item;
    }

    /**
     * Converts an entity object to a DynamoDB item map using CAMEL_CASE naming policy.
     * 
     * <p>This method converts Java objects (POJOs, Maps, or Object arrays) to DynamoDB item format.
     * The conversion uses reflection for POJOs, direct mapping for Maps, and name-value pairs for arrays.
     * Null properties are excluded from the result.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User user = new User("123", "John", 30);
     * Map<String, AttributeValue> item = toItem(user);
     * // Results in: {"userId": {S: "123"}, "name": {S: "John"}, "age": {N: "30"}}
     * }</pre>
     * 
     * @param entity the entity to convert (POJO, Map, or Object array)
     * @return a Map of attribute names to AttributeValues, never null
     * @throws IllegalArgumentException if entity type is not supported
     */
    public static Map<String, AttributeValue> toItem(final Object entity) {
        return toItem(entity, NamingPolicy.CAMEL_CASE);
    }

    /**
     * Converts an entity object to a DynamoDB item map with specified naming policy.
     * 
     * <p>This method provides full control over how property names are converted to DynamoDB attribute names.
     * It supports POJOs with getter/setter methods, Maps, and Object arrays as name-value pairs.</p>
     * 
     * <p><b>Naming Policy Examples:</b></p>
     * <ul>
     * <li>CAMEL_CASE: userName → userName</li>
     * <li>UPPER_CAMEL_CASE: userName → UserName</li>
     * <li>SNAKE_CASE: userName → user_name</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Product product = new Product("PROD-123", "Widget");
     * Map<String, AttributeValue> item = toItem(product, NamingPolicy.SCREAMING_SNAKE_CASE);
     * // Results in: {"PRODUCT_ID": {S: "PROD-123"}, "PRODUCT_NAME": {S: "Widget"}}
     * }</pre>
     * 
     * @param entity the entity to convert (POJO, Map, or Object array), must not be {@code null}
     * @param namingPolicy the naming policy for attribute names, must not be {@code null}
     * @return a Map of attribute names to AttributeValues, never null
     * @throws NullPointerException if {@code entity} or {@code namingPolicy} is {@code null}
     * @throws IllegalArgumentException if entity type is not supported
     */
    public static Map<String, AttributeValue> toItem(final Object entity, final NamingPolicy namingPolicy) {
        final boolean isCamelCase = namingPolicy == NamingPolicy.CAMEL_CASE;
        final Map<String, AttributeValue> attrs = new LinkedHashMap<>();
        final Class<?> cls = entity.getClass();

        if (Beans.isBeanClass(cls)) {
            final BeanInfo entityInfo = ParserUtil.getBeanInfo(cls);
            Object propValue = null;

            for (final PropInfo propInfo : entityInfo.propInfoList) {
                propValue = propInfo.getPropValue(entity);

                if (propValue == null) {
                    continue;
                }

                attrs.put(getAttrName(propInfo, namingPolicy), attrValueOf(propValue));
            }
        } else if (Map.class.isAssignableFrom(cls)) {
            final Map<String, Object> map = (Map<String, Object>) entity;

            if (isCamelCase) {
                for (final Map.Entry<String, Object> entry : map.entrySet()) {
                    attrs.put(entry.getKey(), attrValueOf(entry.getValue()));
                }
            } else {
                for (final Map.Entry<String, Object> entry : map.entrySet()) {
                    attrs.put(namingPolicy.convert(entry.getKey()), attrValueOf(entry.getValue()));
                }
            }
        } else if (entity instanceof Object[]) {
            return toItem(toMap((Object[]) entity), namingPolicy);
        } else {
            throw new IllegalArgumentException("Unsupported type: " + ClassUtil.getCanonicalClassName(cls)
                    + ". Only Entity or Map<String, Object> classes with getter/setter methods are supported");
        }

        return attrs;
    }

    /**
     * Converts an entity to a DynamoDB update item map using CAMEL_CASE naming policy.
     * 
     * <p>Only the dirty properties will be set to the result Map if the specified entity is a dirty marker entity.
     * This method creates AttributeValueUpdate objects with PUT action for all non-null properties.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User user = new User();
     * user.setEmail("newemail@example.com");
     * Map<String, AttributeValueUpdate> updates = toUpdateItem(user);
     * }</pre>
     *
     * @param entity the entity to convert to update map
     * @return a Map of attribute names to AttributeValueUpdate objects, never null
     * @throws IllegalArgumentException if entity type is not supported
     */
    public static Map<String, AttributeValueUpdate> toUpdateItem(final Object entity) {
        return toUpdateItem(entity, NamingPolicy.CAMEL_CASE);
    }

    /**
     * Converts an entity to a DynamoDB update item map with specified naming policy.
     * 
     * <p>Only the dirty properties will be set to the result Map if the specified entity is a dirty marker entity.
     * This method provides control over attribute naming while creating update maps.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Product product = new Product();
     * product.setPrice(29.99);
     * Map<String, AttributeValueUpdate> updates = toUpdateItem(
     *     product, 
     *     NamingPolicy.SNAKE_CASE
     * );
     * // Results in: {"price": AttributeValueUpdate with PUT action}
     * }</pre>
     *
     * @param entity the entity to convert to update map, must not be {@code null}
     * @param namingPolicy the naming policy for attribute names, must not be {@code null}
     * @return a Map of attribute names to AttributeValueUpdate objects, never null
     * @throws NullPointerException if {@code entity} or {@code namingPolicy} is {@code null}
     * @throws IllegalArgumentException if entity type is not supported
     */
    public static Map<String, AttributeValueUpdate> toUpdateItem(final Object entity, final NamingPolicy namingPolicy) {
        final boolean isCamelCase = namingPolicy == NamingPolicy.CAMEL_CASE;
        final Map<String, AttributeValueUpdate> attrs = new LinkedHashMap<>();
        final Class<?> cls = entity.getClass();

        if (Beans.isBeanClass(cls)) {
            final BeanInfo entityInfo = ParserUtil.getBeanInfo(cls);
            Object propValue = null;

            for (final PropInfo propInfo : entityInfo.propInfoList) {
                propValue = propInfo.getPropValue(entity);

                if (propValue == null) {
                    continue;
                }

                attrs.put(getAttrName(propInfo, namingPolicy), attrValueUpdateOf(propValue));
            }
        } else if (Map.class.isAssignableFrom(cls)) {
            final Map<String, Object> map = (Map<String, Object>) entity;

            if (isCamelCase) {
                for (final Map.Entry<String, Object> entry : map.entrySet()) {
                    attrs.put(entry.getKey(), attrValueUpdateOf(entry.getValue()));
                }
            } else {
                for (final Map.Entry<String, Object> entry : map.entrySet()) {
                    attrs.put(namingPolicy.convert(entry.getKey()), attrValueUpdateOf(entry.getValue()));
                }
            }
        } else if (entity instanceof Object[]) {
            return toUpdateItem(toMap((Object[]) entity), namingPolicy);
        } else {
            throw new IllegalArgumentException("Unsupported type: " + ClassUtil.getCanonicalClassName(cls)
                    + ". Only Entity or Map<String, Object> classes with getter/setter methods are supported");
        }

        return attrs;
    }

    static List<Map<String, AttributeValue>> toItem(final Collection<?> entities) {
        return toItem(entities, NamingPolicy.CAMEL_CASE);
    }

    static List<Map<String, AttributeValue>> toItem(final Collection<?> entities, final NamingPolicy namingPolicy) {
        final List<Map<String, AttributeValue>> attrsList = new ArrayList<>(entities.size());

        for (final Object entity : entities) {
            attrsList.add(toItem(entity, namingPolicy));
        }

        return attrsList;
    }

    static List<Map<String, AttributeValueUpdate>> toUpdateItem(final Collection<?> entities) {
        return toUpdateItem(entities, NamingPolicy.CAMEL_CASE);
    }

    static List<Map<String, AttributeValueUpdate>> toUpdateItem(final Collection<?> entities, final NamingPolicy namingPolicy) {
        final List<Map<String, AttributeValueUpdate>> attrsList = new ArrayList<>(entities.size());

        for (final Object entity : entities) {
            attrsList.add(toUpdateItem(entity, namingPolicy));
        }

        return attrsList;
    }

    /**
     * Converts an object array of key-value pairs to a map.
     *
     * <p>The input array is interpreted as alternating key-value pairs:
     * {@code [key1, value1, key2, value2, ...]}.</p>
     *
     * @param propNameAndValues the alternating property name and value pairs
     * @return a map containing the key-value pairs, or {@code null} if input is {@code null}
     * @throws IllegalArgumentException if the array length is odd
     */
    public static Map<String, Object> toMap(final Object[] propNameAndValues) {
        if (propNameAndValues == null) {
            return null; // NOSONAR
        }

        if ((propNameAndValues.length % 2) != 0) {
            throw new IllegalArgumentException("The length of property name/value array must be even: " + propNameAndValues.length);
        }

        final Map<String, Object> props = new LinkedHashMap<>(propNameAndValues.length / 2);

        for (int i = 0, len = propNameAndValues.length; i < len; i += 2) {
            props.put(String.valueOf(propNameAndValues[i]), propNameAndValues[i + 1]);
        }

        return props;
    }

    /**
     * Converts a DynamoDB item to a Map of Java objects.
     * 
     * <p>This method converts DynamoDB AttributeValues to their corresponding Java types,
     * handling all DynamoDB data types including strings, numbers, booleans, nulls, lists, maps, and sets.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> item = // from DynamoDB
     * Map<String, Object> map = toMap(item);
     * String name = (String) map.get("name");
     * Integer age = (Integer) map.get("age");
     * }</pre>
     * 
     * @param item the DynamoDB item to convert
     * @return a Map with converted Java objects, or null if input is null
     */
    public static Map<String, Object> toMap(final Map<String, AttributeValue> item) {
        return toMap(item, IntFunctions.ofMap());
    }

    /**
     * Converts a DynamoDB item to a Map using a custom map supplier.
     * 
     * <p>This method allows you to specify the type of Map to create (e.g., HashMap, TreeMap, LinkedHashMap)
     * while converting DynamoDB AttributeValues to Java objects.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> item = // from DynamoDB
     * Map<String, Object> treeMap = toMap(item, TreeMap::new);
     * }</pre>
     * 
     * @param item the DynamoDB item to convert
     * @param mapSupplier function to create the Map instance
     * @return a Map with converted Java objects, or null if input is null
     */
    public static Map<String, Object> toMap(final Map<String, AttributeValue> item, final IntFunction<? extends Map<String, Object>> mapSupplier) {
        if (item == null) {
            return null; // NOSONAR
        }

        final Map<String, Object> map = mapSupplier.apply(item.size());

        for (final Map.Entry<String, AttributeValue> entry : item.entrySet()) {
            map.put(entry.getKey(), toValue(entry.getValue()));
        }

        return map;
    }

    /**
     * Converts a DynamoDB item (map of AttributeValues) to a specified entity class.
     * This method performs automatic mapping from DynamoDB's AttributeValue format to Java objects,
     * supporting nested properties and various data types.
     * 
     * <p>The conversion handles:</p>
     * <ul>
     * <li>Simple types (String, Number, Boolean, Binary)</li>
     * <li>Set types (String Set, Number Set, Binary Set)</li>
     * <li>Complex types (List, Map)</li>
     * <li>Nested properties using dot notation</li>
     * <li>Column name mapping via annotations</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> item = dynamoDBClient.getItem(tableName, key).getItem();
     * User user = DynamoDBExecutor.toEntity(item, User.class);
     * }</pre>
     * 
     * @param <T> the target entity type
     * @param item the DynamoDB item as a map of attribute names to AttributeValues
     * @param targetClass the class to convert the item to, must be a bean class with getter/setter methods
     * @return the converted entity instance, or null if the item is null
     * @throws IllegalArgumentException if targetClass is not a valid bean class
     */
    public static <T> T toEntity(final Map<String, AttributeValue> item, final Class<T> targetClass) {
        if (item == null) {
            return null;
        }

        final Map<String, String> column2FieldNameMap = QueryUtil.getColumn2PropNameMap(targetClass);
        final BeanInfo entityInfo = ParserUtil.getBeanInfo(targetClass);
        final Object entity = entityInfo.createBeanResult();
        PropInfo propInfo = null;
        String propName = null;
        AttributeValue propValue = null;
        String fieldName = null;

        for (final Map.Entry<String, AttributeValue> entry : item.entrySet()) {
            propName = entry.getKey();
            propValue = entry.getValue();

            propInfo = entityInfo.getPropInfo(propName);

            if (propInfo == null && (fieldName = column2FieldNameMap.get(propName)) != null) {
                propName = fieldName;
                propInfo = entityInfo.getPropInfo(propName);
            }

            if (propInfo == null) {
                if (propName.indexOf(SK._PERIOD) > 0) { //NOSONAR
                    entityInfo.setPropValue(entity, propName, toValue(propValue), true);
                }

                continue;
            }

            propInfo.setPropValue(entity, toValue(propValue, propInfo.clazz));
        }

        return entityInfo.finishBeanResult(entity);
    }

    @SuppressWarnings("rawtypes")
    private static <T> T readRow(final Map<String, AttributeValue> row, final Class<T> rowClass) {
        if (row == null) {
            return rowClass == null ? null : N.defaultValueOf(rowClass);
        }

        final Type<T> rowType = rowClass == null ? null : N.typeOf(rowClass);
        final int columnCount = row.size();

        if (rowType == null || rowType.isObjectArray()) {
            final Object[] a = rowClass == null ? new Object[columnCount] : N.newArray(rowClass.getComponentType(), columnCount);
            final Class<?> componentType = a.getClass().getComponentType();
            int idx = 0;

            for (final Map.Entry<String, AttributeValue> entry : row.entrySet()) {
                a[idx++] = toValue(entry.getValue(), componentType);
            }

            return (T) a;
        } else if (rowType.isCollection()) {
            final Collection<Object> c = N.newCollection((Class<Collection>) rowClass);

            for (final Map.Entry<String, AttributeValue> entry : row.entrySet()) {
                c.add(toValue(entry.getValue()));
            }

            return (T) c;
        } else if (rowType.isMap()) {
            return (T) toMap(row, IntFunctions.ofMap((Class<Map>) rowClass));
        } else if (rowType.isBean()) {
            return toEntity(row, rowClass);
        } else if (columnCount == 1) {
            return toValue(row.values().iterator().next(), rowClass);
        } else {
            throw new IllegalArgumentException("Unsupported row/column type: " + ClassUtil.getCanonicalClassName(rowClass));
        }
    }

    @SuppressWarnings("rawtypes")
    private static <T> Function<Map<String, AttributeValue>, T> createRowMapper(final Class<T> rowClass) {
        N.checkArgNotNull(rowClass, "rowClass");

        final Type<T> rowType = N.typeOf(rowClass);

        if (rowType.isObjectArray()) {
            return row -> {
                final int columnCount = row.size();
                @SuppressWarnings("ConstantValue")
                final Object[] a = rowClass == null ? new Object[columnCount] : N.newArray(rowClass.getComponentType(), columnCount);
                final Class<?> componentType = a.getClass().getComponentType();
                int idx = 0;

                for (final Map.Entry<String, AttributeValue> entry : row.entrySet()) {
                    a[idx++] = toValue(entry.getValue(), componentType);
                }

                return (T) a;
            };
        } else if (rowType.isCollection()) {
            return row -> {
                final Collection<Object> c = N.newCollection((Class<Collection>) rowClass);

                for (final Map.Entry<String, AttributeValue> entry : row.entrySet()) {
                    c.add(toValue(entry.getValue()));
                }

                return (T) c;
            };
        } else if (rowType.isMap()) {
            //noinspection rawtypes
            return row -> (T) toMap(row, IntFunctions.ofMap((Class<Map>) rowClass));
        } else if (rowType.isBean()) {
            return row -> toEntity(row, rowClass);
        } else {
            return row -> {
                if (row.size() != 1) {
                    throw new IllegalArgumentException("Unsupported row/column type: " + ClassUtil.getCanonicalClassName(rowClass));
                }

                return toValue(row.values().iterator().next(), rowClass);
            };
        }
    }

    static <T> T toValue(final AttributeValue x) {
        return toValue(x, null);
    }

    static <T> T toValue(final AttributeValue x, final Class<T> targetClass) {
        if (x == null || (x.getNULL() != null && x.isNULL())) {
            return targetClass == null ? null : N.defaultValueOf(targetClass);
        }

        Object ret = null;

        if (Strings.isNotEmpty(x.getS())) {
            ret = x.getS();
        } else if (Strings.isNotEmpty(x.getN())) {
            ret = x.getN();
        } else if (x.getBOOL() != null) {
            ret = x.getBOOL();
        } else if (x.getB() != null) {
            ret = x.getB();
        } else if (N.notEmpty(x.getSS())) {
            ret = x.getSS();
        } else if (N.notEmpty(x.getNS())) {
            ret = x.getNS();
        } else if (N.notEmpty(x.getBS())) {
            ret = x.getBS();
        } else if (N.notEmpty(x.getL())) {
            final List<AttributeValue> attrVals = x.getL();
            final List<Object> tmp = new ArrayList<>(attrVals.size());

            for (final AttributeValue attrVal : attrVals) {
                tmp.add(toValue(attrVal));
            }

            ret = tmp;
        } else if (N.notEmpty(x.getM())) {
            final Map<String, AttributeValue> attrMap = x.getM();
            final Map<String, Object> tmp = N.newMap(attrMap.getClass(), attrMap.size());

            for (final Map.Entry<String, AttributeValue> entry : attrMap.entrySet()) {
                tmp.put(entry.getKey(), toValue(entry.getValue()));
            }

            ret = tmp;
        } else if (x.getS() != null) {
            ret = x.getS();
        } else if (x.getN() != null) {
            ret = x.getN();
        } else if (x.getSS() != null) {
            ret = x.getSS();
        } else if (x.getNS() != null) {
            ret = x.getNS();
        } else if (x.getBS() != null) {
            ret = x.getBS();
        } else if (x.getL() != null) {
            ret = x.getL();
        } else if (x.getM() != null) {
            ret = x.getM();
        } else if (x.getNULL() != null) {
            ret = x.getNULL();
        } else {
            throw new IllegalArgumentException("Unsupported Attribute type: " + x);
        }

        if (targetClass == null || ret == null || targetClass.isAssignableFrom(ret.getClass())) {
            return (T) ret;
        }

        return N.convert(ret, targetClass);
    }

    /**
     * Converts a map of table items (from batch operations) to entity classes.
     * This method is typically used internally to process batch get results.
     *
     * @param tableItems map of table names to lists of items (AttributeValue maps)
     * @param targetClass entity classes with getter/setter methods or basic single value type (Primitive/String/Date...)
     * @param <T> the target entity type
     * @return map of table names to lists of converted entities
     */
    static <T> Map<String, List<T>> toEntities(final Map<String, List<Map<String, AttributeValue>>> tableItems, final Class<T> targetClass) {
        final Map<String, List<T>> tableEntities = new LinkedHashMap<>();

        if (N.notEmpty(tableItems)) {
            for (final Map.Entry<String, List<Map<String, AttributeValue>>> entry : tableItems.entrySet()) {
                tableEntities.put(entry.getKey(), toList(entry.getValue(), targetClass));
            }
        }

        return tableEntities;
    }

    /**
     * Converts a QueryResult to a list of entities of the specified type.
     * This method extracts all items from the query result and converts them to the target class.
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * QueryResult result = dynamoDBClient.query(queryRequest);
     * List<User> users = DynamoDBExecutor.toList(result, User.class);
     * }</pre>
     *
     * @param <T> the target entity type
     * @param queryResult the QueryResult from a DynamoDB query operation
     * @param targetClass entity classes with getter/setter methods or basic single value type (Primitive/String/Date...)
     * @return list of converted entities
     */
    public static <T> List<T> toList(final QueryResult queryResult, final Class<T> targetClass) {
        return toList(queryResult, 0, Integer.MAX_VALUE, targetClass);
    }

    /**
     * Converts a QueryResult to a list of entities with offset and count limits.
     * This method provides pagination support for query results.
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * QueryResult result = dynamoDBClient.query(queryRequest);
     * // Get items 10-19 (skip first 10, take next 10)
     * List<User> users = DynamoDBExecutor.toList(result, 10, 10, User.class);
     * }</pre>
     *
     * @param <T> the target entity type
     * @param queryResult the QueryResult from a DynamoDB query operation
     * @param offset number of items to skip from the beginning
     * @param count maximum number of items to return
     * @param targetClass entity classes with getter/setter methods or basic single value type (Primitive/String/Date...)
     * @return list of converted entities within the specified range
     * @throws IllegalArgumentException if offset or count is negative
     */
    public static <T> List<T> toList(final QueryResult queryResult, final int offset, final int count, final Class<T> targetClass) {
        return toList(queryResult.getItems(), offset, count, targetClass);
    }

    /**
     * Converts a ScanResult to a list of entities of the specified type.
     * This method extracts all items from the scan result and converts them to the target class.
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ScanResult result = dynamoDBClient.scan(scanRequest);
     * List<Product> products = DynamoDBExecutor.toList(result, Product.class);
     * }</pre>
     *
     * @param <T> the target entity type
     * @param scanResult the ScanResult from a DynamoDB scan operation
     * @param targetClass entity classes with getter/setter methods or basic single value type (Primitive/String/Date...)
     * @return list of converted entities
     */
    public static <T> List<T> toList(final ScanResult scanResult, final Class<T> targetClass) {
        return toList(scanResult, 0, Integer.MAX_VALUE, targetClass);
    }

    /**
     * Converts a ScanResult to a list of entities with offset and count limits.
     * This method provides pagination support for scan results.
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ScanResult result = dynamoDBClient.scan(scanRequest);
     * // Get items 20-39 (skip first 20, take next 20)
     * List<Product> products = DynamoDBExecutor.toList(result, 20, 20, Product.class);
     * }</pre>
     *
     * @param <T> the target entity type
     * @param scanResult the ScanResult from a DynamoDB scan operation
     * @param offset number of items to skip from the beginning
     * @param count maximum number of items to return
     * @param targetClass entity classes with getter/setter methods or basic single value type (Primitive/String/Date...)
     * @return list of converted entities within the specified range
     * @throws IllegalArgumentException if offset or count is negative
     */
    public static <T> List<T> toList(final ScanResult scanResult, final int offset, final int count, final Class<T> targetClass) {
        return toList(scanResult.getItems(), offset, count, targetClass);
    }

    /**
     * Converts a list of DynamoDB items to entities.
     * Internal method for converting raw item lists.
     *
     * @param items list of DynamoDB items as AttributeValue maps
     * @param targetClass entity classes with getter/setter methods or basic single value type (Primitive/String/Date...)
     * @param <T> the target entity type
     * @return list of converted entities
     */
    static <T> List<T> toList(final List<Map<String, AttributeValue>> items, final Class<T> targetClass) {
        return toList(items, 0, Integer.MAX_VALUE, targetClass);
    }

    /**
     * Converts a list of DynamoDB items to entities with pagination.
     * Internal method for converting raw item lists with offset and count.
     *
     * @param items list of DynamoDB items as AttributeValue maps
     * @param offset number of items to skip
     * @param count maximum number of items to convert
     * @param targetClass entity classes with getter/setter methods or basic single value type (Primitive/String/Date...)
     * @param <T> the target entity type
     * @return list of converted entities within the specified range
     */
    static <T> List<T> toList(final List<Map<String, AttributeValue>> items, final int offset, int count, final Class<T> targetClass) {
        if (offset < 0 || count < 0) {
            throw new IllegalArgumentException("Offset and count cannot be negative");
        }

        final List<T> resultList = new ArrayList<>();
        final Function<Map<String, AttributeValue>, T> mapper = createRowMapper(targetClass);

        if (N.notEmpty(items)) {
            for (int i = offset, to = items.size(); i < to && count > 0; i++, count--) {
                resultList.add(mapper.apply(items.get(i)));
            }
        }

        return resultList;
    }

    /**
     * Extracts data from a QueryResult into a Dataset for tabular operations.
     * Datasets provide column-oriented data manipulation capabilities.
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * QueryResult result = dynamoDBClient.query(queryRequest);
     * Dataset dataset = DynamoDBExecutor.extractData(result);
     * dataset.println();   // Print as table
     * }</pre>
     * 
     * @param queryResult the QueryResult to extract data from
     * @return a Dataset containing the query results in tabular format
     */
    public static Dataset extractData(final QueryResult queryResult) {
        return extractData(queryResult, 0, Integer.MAX_VALUE);
    }

    /**
     * Extracts data from a QueryResult into a Dataset with pagination.
     * Provides control over which items to include in the Dataset.
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * QueryResult result = dynamoDBClient.query(queryRequest);
     * // Extract items 5-14 (skip first 5, take next 10)
     * Dataset dataset = DynamoDBExecutor.extractData(result, 5, 10);
     * }</pre>
     * 
     * @param queryResult the QueryResult to extract data from
     * @param offset number of items to skip from the beginning
     * @param count maximum number of items to include
     * @return a Dataset containing the specified range of results
     * @throws IllegalArgumentException if offset or count is negative
     */
    public static Dataset extractData(final QueryResult queryResult, final int offset, final int count) {
        return extractData(queryResult.getItems(), offset, count);
    }

    /**
     * Extracts data from a ScanResult into a Dataset for tabular operations.
     * Datasets provide SQL-like operations and easy data export capabilities.
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ScanResult result = dynamoDBClient.scan(scanRequest);
     * Dataset dataset = DynamoDBExecutor.extractData(result);
     * dataset.toCSV(new FileWriter("data.csv"));
     * }</pre>
     * 
     * @param scanResult the ScanResult to extract data from
     * @return a Dataset containing the scan results in tabular format
     */
    public static Dataset extractData(final ScanResult scanResult) {
        return extractData(scanResult, 0, Integer.MAX_VALUE);
    }

    /**
     * Extracts data from a ScanResult into a Dataset with pagination.
     * Provides control over which items to include in the Dataset.
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ScanResult result = dynamoDBClient.scan(scanRequest);
     * // Extract first 100 items only
     * Dataset dataset = DynamoDBExecutor.extractData(result, 0, 100);
     * }</pre>
     * 
     * @param scanResult the ScanResult to extract data from
     * @param offset number of items to skip from the beginning
     * @param count maximum number of items to include
     * @return a Dataset containing the specified range of results
     * @throws IllegalArgumentException if offset or count is negative
     */
    public static Dataset extractData(final ScanResult scanResult, final int offset, final int count) {
        return extractData(scanResult.getItems(), offset, count);
    }

    static Dataset extractData(final List<Map<String, AttributeValue>> items, final int offset, final int count) {
        N.checkArgument(offset >= 0 && count >= 0, "'offset' and 'count' can't be negative: %s, %s", offset, count);

        if (N.isEmpty(items) || count == 0 || offset >= items.size()) {
            return N.newEmptyDataset();
        }

        final int rowCount = N.min(count, items.size() - offset);
        final Set<String> columnNames = N.newLinkedHashSet();

        for (int i = offset, to = offset + rowCount; i < to; i++) {
            columnNames.addAll(items.get(i).keySet());
        }

        final int columnCount = columnNames.size();
        final List<String> columnNameList = new ArrayList<>(columnNames);
        final List<List<Object>> columnList = new ArrayList<>(columnCount);

        for (int i = 0; i < columnCount; i++) {
            columnList.add(new ArrayList<>(rowCount));
        }

        for (int i = offset, to = offset + rowCount; i < to; i++) {
            final Map<String, AttributeValue> item = items.get(i);

            for (int j = 0; j < columnCount; j++) {
                columnList.get(j).add(toValue(item.get(columnNameList.get(j))));
            }
        }

        return new RowDataset(columnNameList, columnList);
    }

    static <T> void checkEntityClass(final Class<T> targetClass) {
        if (!Beans.isBeanClass(targetClass)) {
            throw new IllegalArgumentException("Unsupported type: " + ClassUtil.getCanonicalClassName(targetClass)
                    + ". Only Entity class generated by CodeGenerator with getter/setter methods are supported");
        }
    }

    private static String getAttrName(final PropInfo propInfo, final NamingPolicy namingPolicy) {
        if (propInfo.columnName.isPresent()) {
            return propInfo.columnName.get();
        } else if (namingPolicy == NamingPolicy.CAMEL_CASE) {
            return propInfo.name;
        } else {
            return namingPolicy.convert(propInfo.name);
        }
    }

    /**
     * Retrieves a single item from DynamoDB table using the specified primary key.
     * 
     * <p>This method performs a GetItem operation with eventually consistent reads by default.
     * The returned item is automatically converted from DynamoDB AttributeValues to a Map
     * of Java objects for easy manipulation.</p>
     * 
     * <p><b>Read Consistency:</b> Uses eventually consistent reads by default, which provides
     * better performance and lower cost but may not reflect the most recent write operations.
     * Use {@link #getItem(String, Map, Boolean)} with consistentRead=true for strong consistency.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> key = asKey("userId", "user123");
     * Map<String, Object> item = executor.getItem("Users", key);
     * if (item != null) {
     *     String name = (String) item.get("name");
     *     Integer age = (Integer) item.get("age");
     * }
     * }</pre>
     * 
     * @param tableName the name of the DynamoDB table. Must not be null or empty.
     * @param key the primary key of the item to retrieve. Must not be null or empty.
     * @return a Map containing the item attributes, or null if item doesn't exist
     * @throws IllegalArgumentException if tableName is null/empty or key is null/empty
     * @see #getItem(String, Map, Boolean)
     * @see #getItem(String, Map, Class)
     */
    public Map<String, Object> getItem(final String tableName, final Map<String, AttributeValue> key) {
        return getItem(tableName, key, Clazz.PROPS_MAP);
    }

    /**
     * Retrieves a single item from DynamoDB table with specified read consistency.
     * 
     * <p>This method performs a GetItem operation with configurable read consistency.
     * Strong consistent reads ensure you get the most up-to-date data but consume more
     * read capacity and have higher latency than eventually consistent reads.</p>
     * 
     * <p><b>Read Consistency Options:</b></p>
     * <ul>
     * <li><b>Eventually Consistent (false/null):</b> Default, better performance, lower cost</li>
     * <li><b>Strongly Consistent (true):</b> Most recent data, higher cost, higher latency</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> key = asKey("userId", "user123");
     * Map<String, Object> item = executor.getItem("Users", key, true);   // Strong consistency
     * }</pre>
     * 
     * @param tableName the name of the DynamoDB table. Must not be null or empty.
     * @param key the primary key of the item to retrieve. Must not be null or empty.
     * @param consistentRead true for strongly consistent reads, false/null for eventually consistent reads
     * @return a Map containing the item attributes, or null if item doesn't exist
     * @throws IllegalArgumentException if tableName is null/empty or key is null/empty
     * @see #getItem(String, Map)
     */
    public Map<String, Object> getItem(final String tableName, final Map<String, AttributeValue> key, final Boolean consistentRead) {
        return getItem(tableName, key, consistentRead, Clazz.PROPS_MAP);
    }

    /**
     * Retrieves a single item using a fully configured GetItemRequest.
     * This method provides maximum flexibility for GetItem operations.
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * GetItemRequest request = new GetItemRequest()
     *     .withTableName("Users")
     *     .withKey(asKey("userId", "user123"))
     *     .withConsistentRead(true)
     *     .withProjectionExpression("userId, name, email");
     * Map<String, Object> item = executor.getItem(request);
     * }</pre>
     * 
     * @param getItemRequest the complete request with all parameters
     * @return a Map containing the item attributes, or null if item doesn't exist
     * @throws IllegalArgumentException if getItemRequest is null
     */
    public Map<String, Object> getItem(final GetItemRequest getItemRequest) {
        return getItem(getItemRequest, Clazz.PROPS_MAP);
    }

    /**
     * Retrieves a single item from DynamoDB table and converts it to the specified type.
     * 
     * <p>This method performs a GetItem operation with eventually consistent reads and automatically
     * converts the result to the specified target class. The conversion supports entity classes
     * with getter/setter methods, Maps, Collections, arrays, and primitive types.</p>
     * 
     * <p><b>Supported Target Types:</b></p>
     * <ul>
     * <li><b>Entity Classes:</b> POJOs with getter/setter methods (automatic mapping)</li>
     * <li><b>Map Types:</b> Map&lt;String, Object&gt;, LinkedHashMap, etc.</li>
     * <li><b>Collection Types:</b> List, Set, etc. (values from all attributes)</li>
     * <li><b>Array Types:</b> Object[], String[], etc.</li>
     * <li><b>Primitive Types:</b> For single-attribute results</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Entity mapping
     * Map<String, AttributeValue> key = asKey("userId", "user123");
     * User user = executor.getItem("Users", key, User.class);
     * 
     * // Map result
     * Map<String, Object> userMap = executor.getItem("Users", key, Map.class);
     * }</pre>
     * 
     * @param <T> the target type for conversion
     * @param tableName the name of the DynamoDB table. Must not be null or empty.
     * @param key the primary key of the item to retrieve. Must not be null or empty.
     * @param targetClass the class to convert the result to. Must not be null.
     * @return an instance of targetClass containing the item data, or null if item doesn't exist
     * @throws IllegalArgumentException if any parameter is null, tableName is empty, or targetClass is unsupported
     * @see #getItem(String, Map, Boolean, Class)
     */
    public <T> T getItem(final String tableName, final Map<String, AttributeValue> key, final Class<T> targetClass) {
        return readRow(dynamoDBClient.getItem(tableName, key).getItem(), targetClass);
    }

    /**
     * Retrieves a single item from DynamoDB table with specified consistency and converts it to the target type.
     * 
     * <p>This method combines the flexibility of configurable read consistency with automatic type conversion.
     * It's the most comprehensive getItem method, providing full control over both consistency model and
     * result type conversion.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> key = asKey("userId", "user123");
     * User user = executor.getItem("Users", key, true, User.class);   // Strong consistency + entity mapping
     * }</pre>
     * 
     * @param <T> the target type for conversion
     * @param tableName the name of the DynamoDB table. Must not be null or empty.
     * @param key the primary key of the item to retrieve. Must not be null or empty.
     * @param consistentRead true for strongly consistent reads, false/null for eventually consistent reads
     * @param targetClass the class to convert the result to. Must not be null.
     * @return an instance of targetClass containing the item data, or null if item doesn't exist
     * @throws IllegalArgumentException if any parameter is null, tableName is empty, or targetClass is unsupported
     */
    public <T> T getItem(final String tableName, final Map<String, AttributeValue> key, final Boolean consistentRead, final Class<T> targetClass) {
        return readRow(dynamoDBClient.getItem(tableName, key, consistentRead).getItem(), targetClass);
    }

    /**
     * Retrieves a single item using a GetItemRequest and converts it to the specified type.
     * This method combines the flexibility of a complete request with automatic type conversion.
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * GetItemRequest request = new GetItemRequest()
     *     .withTableName("Users")
     *     .withKey(asKey("userId", "user123"));
     * User user = executor.getItem(request, User.class);
     * }</pre>
     * 
     * @param <T> the target type for conversion
     * @param getItemRequest the complete request with all parameters
     * @param targetClass the class to convert the result to
     * @return an instance of targetClass containing the item data, or null if item doesn't exist
     * @throws IllegalArgumentException if any parameter is null or targetClass is unsupported
     */
    public <T> T getItem(final GetItemRequest getItemRequest, final Class<T> targetClass) {
        return readRow(dynamoDBClient.getItem(getItemRequest).getItem(), targetClass);
    }

    /**
     * Retrieves multiple items from one or more DynamoDB tables in a single batch operation.
     * 
     * <p>This method performs a BatchGetItem operation to efficiently retrieve multiple items.
     * DynamoDB batch operations can retrieve up to 100 items and consume up to 16 MB of data.
     * If the request exceeds these limits, you'll need to make multiple batch calls.</p>
     * 
     * <p><b>Batch Operation Limits:</b></p>
     * <ul>
     * <li>Maximum 100 items per batch request</li>
     * <li>Maximum 16 MB total response size</li>
     * <li>Items can span multiple tables</li>
     * <li>Unprocessed items may be returned if limits are exceeded</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, KeysAndAttributes> requestItems = new HashMap<>();
     * 
     * // Request multiple users
     * List<Map<String, AttributeValue>> userKeys = Arrays.asList(
     *     asKey("userId", "user1"),
     *     asKey("userId", "user2")
     * );
     * requestItems.put("Users", KeysAndAttributes.builder().keys(userKeys).build());
     * 
     * Map<String, List<Map<String, Object>>> results = executor.batchGetItem(requestItems);
     * List<Map<String, Object>> users = results.get("Users");
     * }</pre>
     * 
     * @param requestItems a map of table names to KeysAndAttributes specifying which items to retrieve. Must not be null.
     * @return a map of table names to lists of retrieved items, never null
     * @throws IllegalArgumentException if requestItems is null or empty
     * @see #batchGetItem(Map, String)
     * @see #batchGetItem(Map, Class)
     */
    public Map<String, List<Map<String, Object>>> batchGetItem(final Map<String, KeysAndAttributes> requestItems) {
        return batchGetItem(requestItems, Clazz.PROPS_MAP);
    }

    /**
     * Retrieves multiple items from DynamoDB tables with consumed capacity reporting.
     * This method allows monitoring of read capacity consumption for billing and performance analysis.
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, KeysAndAttributes> requestItems = createRequestItems();
     * Map<String, List<Map<String, Object>>> results = 
     *     executor.batchGetItem(requestItems, "TOTAL");
     * // Check consumed capacity in the result
     * }</pre>
     * 
     * @param requestItems a map of table names to KeysAndAttributes specifying which items to retrieve
     * @param returnConsumedCapacity "NONE", "TOTAL", or "INDEXES" for capacity reporting
     * @return a map of table names to lists of retrieved items
     * @throws IllegalArgumentException if requestItems is null
     */
    public Map<String, List<Map<String, Object>>> batchGetItem(final Map<String, KeysAndAttributes> requestItems, final String returnConsumedCapacity) {
        return batchGetItem(requestItems, returnConsumedCapacity, Clazz.PROPS_MAP);
    }

    /**
     * Retrieves multiple items using a fully configured BatchGetItemRequest.
     * This method provides complete control over batch get operations.
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * BatchGetItemRequest request = new BatchGetItemRequest()
     *     .withRequestItems(requestItems)
     *     .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
     * Map<String, List<Map<String, Object>>> results = executor.batchGetItem(request);
     * }</pre>
     * 
     * @param batchGetItemRequest the complete batch get request
     * @return a map of table names to lists of retrieved items
     * @throws IllegalArgumentException if batchGetItemRequest is null
     */
    public Map<String, List<Map<String, Object>>> batchGetItem(final BatchGetItemRequest batchGetItemRequest) {
        return batchGetItem(batchGetItemRequest, Clazz.PROPS_MAP);
    }

    /**
     * Retrieves multiple items from DynamoDB tables and converts them to the specified type.
     * 
     * <p>This method combines the efficiency of batch operations with automatic type conversion.
     * All retrieved items are converted to the specified target class using the same mapping
     * rules as single-item operations.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, KeysAndAttributes> requestItems = new HashMap<>();
     * List<Map<String, AttributeValue>> keys = Arrays.asList(
     *     asKey("userId", "user1"),
     *     asKey("userId", "user2")
     * );
     * requestItems.put("Users", KeysAndAttributes.builder().keys(keys).build());
     * 
     * Map<String, List<User>> results = executor.batchGetItem(requestItems, User.class);
     * List<User> users = results.get("Users");
     * }</pre>
     * 
     * @param <T> the target type for conversion
     * @param requestItems a map of table names to KeysAndAttributes specifying which items to retrieve. Must not be null.
     * @param targetClass the class to convert retrieved items to. Must not be null.
     * @return a map of table names to lists of converted items, never null
     * @throws IllegalArgumentException if requestItems or targetClass is null, or targetClass is unsupported
     */
    public <T> Map<String, List<T>> batchGetItem(final Map<String, KeysAndAttributes> requestItems, final Class<T> targetClass) {
        return toEntities(dynamoDBClient.batchGetItem(requestItems).getResponses(), targetClass);
    }

    /**
     * Retrieves multiple items with consumed capacity reporting and type conversion.
     * This method combines batch operations, capacity monitoring, and automatic type conversion.
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, KeysAndAttributes> requestItems = createRequestItems();
     * Map<String, List<User>> results = 
     *     executor.batchGetItem(requestItems, "TOTAL", User.class);
     * }</pre>
     * 
     * @param <T> the target type for conversion
     * @param requestItems a map of table names to KeysAndAttributes
     * @param returnConsumedCapacity "NONE", "TOTAL", or "INDEXES" for capacity reporting
     * @param targetClass the class to convert retrieved items to
     * @return a map of table names to lists of converted items
     * @throws IllegalArgumentException if any parameter is null or invalid
     */
    public <T> Map<String, List<T>> batchGetItem(final Map<String, KeysAndAttributes> requestItems, final String returnConsumedCapacity,
            final Class<T> targetClass) {
        return toEntities(dynamoDBClient.batchGetItem(requestItems, returnConsumedCapacity).getResponses(), targetClass);
    }

    /**
     * Retrieves multiple items using a BatchGetItemRequest and converts them to the specified type.
     * This method provides complete control with automatic type conversion.
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * BatchGetItemRequest request = new BatchGetItemRequest()
     *     .withRequestItems(requestItems);
     * Map<String, List<User>> results = executor.batchGetItem(request, User.class);
     * }</pre>
     * 
     * @param <T> the target type for conversion
     * @param batchGetItemRequest the complete batch get request
     * @param targetClass the class to convert retrieved items to
     * @return a map of table names to lists of converted items
     * @throws IllegalArgumentException if any parameter is null or invalid
     */
    public <T> Map<String, List<T>> batchGetItem(final BatchGetItemRequest batchGetItemRequest, final Class<T> targetClass) {
        return toEntities(dynamoDBClient.batchGetItem(batchGetItemRequest).getResponses(), targetClass);
    }

    /**
     * Inserts or replaces an item in the specified DynamoDB table.
     * 
     * <p>This method performs a PutItem operation which creates a new item or completely replaces
     * an existing item with the same primary key. All attributes in the existing item are replaced.
     * To update only specific attributes while preserving others, use {@link #updateItem} instead.</p>
     * 
     * <p><strong>Important:</strong> PutItem is an atomic operation that either succeeds or fails
     * completely. If an item with the same primary key already exists, it will be overwritten.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> item = asItem(
     *     "userId", "user123",
     *     "name", "John Doe",
     *     "email", "john@example.com",
     *     "createdAt", System.currentTimeMillis()
     * );
     * 
     * PutItemResult result = executor.putItem("Users", item);
     * System.out.println("Consumed capacity: " + result.getConsumedCapacity());
     * }</pre>
     * 
     * @param tableName the name of the DynamoDB table. Must not be {@code null} or empty.
     * @param item the item to put, as a map of attribute names to AttributeValues. Must not be {@code null}.
     * @return a {@link PutItemResult} containing operation metadata and consumed capacity
     * @throws IllegalArgumentException if tableName is null/empty or item is null
     * @see #putItem(String, Map, String) to retrieve the replaced item
     * @see #updateItem for partial updates
     */
    public PutItemResult putItem(final String tableName, final Map<String, AttributeValue> item) {
        return dynamoDBClient.putItem(tableName, item);
    }

    /**
     * Inserts or replaces an item in DynamoDB table with return value options.
     * 
     * <p>This method performs a PutItem operation with the ability to return information
     * about the item that was replaced. This is useful for auditing or when you need to
     * know what data was overwritten.</p>
     * 
     * <p><strong>Return Values Options:</strong></p>
     * <ul>
     * <li><strong>NONE:</strong> Nothing is returned (default)</li>
     * <li><strong>ALL_OLD:</strong> Returns all attributes of the replaced item</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> item = asItem(
     *     "userId", "user123",
     *     "name", "Jane Doe",
     *     "updatedAt", Instant.now().toEpochMilli()
     * );
     * 
     * PutItemResult result = executor.putItem("Users", item, "ALL_OLD");
     * Map<String, AttributeValue> oldItem = result.getAttributes();
     * if (oldItem != null) {
     *     System.out.println("Replaced item: " + oldItem);
     * }
     * }</pre>
     * 
     * @param tableName the name of the DynamoDB table. Must not be {@code null} or empty.
     * @param item the item to put, as a map of attribute names to AttributeValues. Must not be {@code null}.
     * @param returnValues specifies what to return: "NONE" or "ALL_OLD"
     * @return a {@link PutItemResult} containing operation metadata and optionally the replaced item
     * @throws IllegalArgumentException if tableName is null/empty or item is null
     * @see com.amazonaws.services.dynamodbv2.model.ReturnValue for valid return value options
     */
    public PutItemResult putItem(final String tableName, final Map<String, AttributeValue> item, final String returnValues) {
        return dynamoDBClient.putItem(tableName, item, returnValues);
    }

    /**
     * Inserts or replaces an item using a fully configured PutItemRequest.
     * 
     * <p>This method provides complete control over the PutItem operation, allowing you to
     * specify conditional expressions, return values, consumed capacity details, and more.
     * Use this method when you need advanced features like conditional puts or custom
     * write concerns.</p>
     * 
     * <p><strong>Advanced Features:</strong></p>
     * <ul>
     * <li>Conditional expressions to prevent overwrites</li>
     * <li>Return consumed capacity for monitoring</li>
     * <li>Return item collection metrics for LSI</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * PutItemRequest request = new PutItemRequest()
     *     .withTableName("Users")
     *     .withItem(asItem("userId", "user123", "name", "John"))
     *     .withConditionExpression("attribute_not_exists(userId)") // Only if new
     *     .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
     * 
     * try {
     *     PutItemResult result = executor.putItem(request);
     *     System.out.println("Item created successfully");
     * } catch (ConditionalCheckFailedException e) {
     *     System.out.println("Item already exists");
     * }
     * }</pre>
     * 
     * @param putItemRequest the complete request with all parameters. Must not be {@code null}.
     * @return a {@link PutItemResult} containing operation metadata and optional return values
     * @throws IllegalArgumentException if putItemRequest is null
     */
    public PutItemResult putItem(final PutItemRequest putItemRequest) {
        return dynamoDBClient.putItem(putItemRequest);
    }

    // There is no too much benefit to add method for "Object entity"
    // And it may cause error because the "Object" is ambiguous to any type.
    PutItemResult putItem(final String tableName, final Object entity) {
        return putItem(tableName, toItem(entity));
    }

    PutItemResult putItem(final String tableName, final Object entity, final String returnValues) {
        return putItem(tableName, toItem(entity), returnValues);
    }

    /**
     * Performs batch write operations (puts and deletes) across multiple tables.
     * 
     * <p>This method executes multiple PutItem and DeleteItem operations in a single call,
     * significantly improving performance for bulk operations. Each batch can contain up
     * to 25 write requests with a maximum total size of 16 MB.</p>
     * 
     * <p><strong>Batch Write Limitations:</strong></p>
     * <ul>
     * <li>Maximum 25 items per batch</li>
     * <li>Maximum 16 MB total request size</li>
     * <li>Cannot use conditional expressions</li>
     * <li>Operations are not transactional</li>
     * <li>Unprocessed items may need retry</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<WriteRequest> userWrites = Arrays.asList(
     *     new WriteRequest(new PutRequest(asItem("userId", "user1", "name", "Alice"))),
     *     new WriteRequest(new PutRequest(asItem("userId", "user2", "name", "Bob"))),
     *     new WriteRequest(new DeleteRequest(asKey("userId", "user3")))
     * );
     * 
     * Map<String, List<WriteRequest>> requestItems = Map.of("Users", userWrites);
     * 
     * BatchWriteItemResult result = executor.batchWriteItem(requestItems);
     * if (!result.getUnprocessedItems().isEmpty()) {
     *     // Retry unprocessed items
     *     executor.batchWriteItem(result.getUnprocessedItems());
     * }
     * }</pre>
     * 
     * @param requestItems map of table names to lists of write requests (puts/deletes). Must not be {@code null}.
     * @return a {@link BatchWriteItemResult} containing unprocessed items and consumed capacity
     * @throws IllegalArgumentException if requestItems is null or exceeds batch limits
     * @see #batchWriteItem(BatchWriteItemRequest) for more control
     */
    public BatchWriteItemResult batchWriteItem(final Map<String, List<WriteRequest>> requestItems) {
        return dynamoDBClient.batchWriteItem(requestItems);
    }

    /**
     * Performs batch write operations using a fully configured BatchWriteItemRequest.
     * 
     * <p>This method provides complete control over batch write operations, including
     * options for consumed capacity reporting and request metrics. Use this when you
     * need detailed information about the batch operation's resource consumption.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * BatchWriteItemRequest request = new BatchWriteItemRequest()
     *     .withRequestItems(requestItems)
     *     .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
     *     .withReturnItemCollectionMetrics(ReturnItemCollectionMetrics.SIZE);
     * 
     * BatchWriteItemResult result = executor.batchWriteItem(request);
     * System.out.println("Total consumed capacity: " + result.getConsumedCapacity());
     * }</pre>
     * 
     * @param batchWriteItemRequest the complete batch write request. Must not be {@code null}.
     * @return a {@link BatchWriteItemResult} with unprocessed items and optional metrics
     * @throws IllegalArgumentException if batchWriteItemRequest is null
     */
    public BatchWriteItemResult batchWriteItem(final BatchWriteItemRequest batchWriteItemRequest) {
        return dynamoDBClient.batchWriteItem(batchWriteItemRequest);
    }

    /**
     * Updates specific attributes of an existing item in DynamoDB.
     * 
     * <p>This method performs partial updates on an item, modifying only the specified
     * attributes while leaving others unchanged. Unlike PutItem, which replaces the entire
     * item, UpdateItem allows fine-grained control over individual attributes.</p>
     * 
     * <p><strong>Update Actions:</strong></p>
     * <ul>
     * <li><strong>PUT:</strong> Sets the attribute to a new value</li>
     * <li><strong>ADD:</strong> Adds to numeric values or adds elements to sets</li>
     * <li><strong>DELETE:</strong> Removes the attribute or removes elements from sets</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> key = asKey("userId", "user123");
     * 
     * Map<String, AttributeValueUpdate> updates = new HashMap<>();
     * updates.put("lastLogin", attrValueUpdateOf(Instant.now().toString()));
     * updates.put("loginCount", new AttributeValueUpdate()
     *     .withAction(AttributeAction.ADD)
     *     .withValue(attrValueOf(1)));
     * updates.put("tempToken", new AttributeValueUpdate()
     *     .withAction(AttributeAction.DELETE));
     * 
     * UpdateItemResult result = executor.updateItem("Users", key, updates);
     * }</pre>
     * 
     * @param tableName the name of the DynamoDB table. Must not be {@code null} or empty.
     * @param key the primary key identifying the item to update. Must not be {@code null}.
     * @param attributeUpdates map of attribute names to update actions. Must not be {@code null}.
     * @return an {@link UpdateItemResult} containing operation metadata
     * @throws IllegalArgumentException if any parameter is null or tableName is empty
     * @see #updateItem(String, Map, Map, String) to retrieve updated values
     */
    public UpdateItemResult updateItem(final String tableName, final Map<String, AttributeValue> key,
            final Map<String, AttributeValueUpdate> attributeUpdates) {
        return dynamoDBClient.updateItem(tableName, key, attributeUpdates);
    }

    /**
     * Updates item attributes with options to return values.
     * 
     * <p>This method performs partial updates with the ability to retrieve attribute values
     * before or after the update operation. This is useful for audit trails, optimistic
     * locking patterns, or when you need to verify the update results.</p>
     * 
     * <p><strong>Return Values Options:</strong></p>
     * <ul>
     * <li><strong>NONE:</strong> Nothing returned (default)</li>
     * <li><strong>ALL_OLD:</strong> All attributes before update</li>
     * <li><strong>UPDATED_OLD:</strong> Only updated attributes before update</li>
     * <li><strong>ALL_NEW:</strong> All attributes after update</li>
     * <li><strong>UPDATED_NEW:</strong> Only updated attributes after update</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> key = asKey("userId", "user123");
     * Map<String, AttributeValueUpdate> updates = asUpdateItem(
     *     "email", "newemail@example.com",
     *     "updatedAt", System.currentTimeMillis()
     * );
     * 
     * UpdateItemResult result = executor.updateItem("Users", key, updates, "ALL_NEW");
     * }</pre>
     * 
     * @param tableName the name of the DynamoDB table. Must not be {@code null} or empty.
     * @param key the primary key identifying the item to update. Must not be {@code null}.
     * @param attributeUpdates map of attribute names to update actions. Must not be {@code null}.
     * @param returnValues specifies what to return (see method description)
     * @return an {@link UpdateItemResult} containing operation metadata and optional values
     * @throws IllegalArgumentException if required parameters are null or invalid
     * @see com.amazonaws.services.dynamodbv2.model.ReturnValue
     */
    public UpdateItemResult updateItem(final String tableName, final Map<String, AttributeValue> key, final Map<String, AttributeValueUpdate> attributeUpdates,
            final String returnValues) {
        return dynamoDBClient.updateItem(tableName, key, attributeUpdates, returnValues);
    }

    /**
     * Updates an item using a fully configured UpdateItemRequest.
     * 
     * <p>This method provides complete control over update operations, including conditional
     * expressions, update expressions, return values, and consumed capacity reporting.
     * Use this for advanced update scenarios requiring conditions or complex expressions.</p>
     * 
     * <p><strong>Advanced Features:</strong></p>
     * <ul>
     * <li>Conditional expressions to prevent concurrent updates</li>
     * <li>Update expressions for complex operations</li>
     * <li>Expression attribute names/values for reserved words</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * UpdateItemRequest request = new UpdateItemRequest()
     *     .withTableName("Users")
     *     .withKey(asKey("userId", "user123"))
     *     .withUpdateExpression("SET #n = :name, #c = #c + :inc")
     *     .withExpressionAttributeNames(Map.of(
     *         "#n", "name",
     *         "#c", "counter"
     *     ))
     *     .withExpressionAttributeValues(Map.of(
     *         ":name", attrValueOf("New Name"),
     *         ":inc", attrValueOf(1),
     *         ":old", attrValueOf("Old Name")
     *     ))
     *     .withConditionExpression("#n = :old")
     *     .withReturnValues("ALL_NEW");
     * 
     * UpdateItemResult result = executor.updateItem(request);
     * }</pre>
     * 
     * @param updateItemRequest the complete update request. Must not be {@code null}.
     * @return an {@link UpdateItemResult} containing operation results
     * @throws IllegalArgumentException if updateItemRequest is null
     */
    public UpdateItemResult updateItem(final UpdateItemRequest updateItemRequest) {
        return dynamoDBClient.updateItem(updateItemRequest);
    }

    /**
     * Deletes a single item from the specified DynamoDB table.
     * 
     * <p>This method performs a DeleteItem operation to remove an entire item from the table.
     * If the item doesn't exist, the operation completes successfully without error.
     * To verify deletion or retrieve the deleted item's data, use the overload with returnValues.</p>
     * 
     * <p><strong>Important:</strong> Deletion is permanent and cannot be undone. Consider
     * implementing soft deletes (marking items as deleted) if you need recovery options.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> key = asKey("userId", "user123");
     * 
     * DeleteItemResult result = executor.deleteItem("Users", key);
     * System.out.println("Item deleted. Consumed capacity: " + 
     *                    result.getConsumedCapacity());
     * }</pre>
     * 
     * @param tableName the name of the DynamoDB table. Must not be {@code null} or empty.
     * @param key the primary key of the item to delete. Must not be {@code null}.
     * @return a {@link DeleteItemResult} containing operation metadata
     * @throws IllegalArgumentException if tableName is null/empty or key is null
     * @see #deleteItem(String, Map, String) to retrieve the deleted item
     */
    public DeleteItemResult deleteItem(final String tableName, final Map<String, AttributeValue> key) {
        return dynamoDBClient.deleteItem(tableName, key);
    }

    /**
     * Deletes an item with options to return the deleted item's attributes.
     * 
     * <p>This method performs a DeleteItem operation with the ability to retrieve the
     * attributes of the deleted item. This is useful for audit logs, undo operations,
     * or confirming what was actually deleted.</p>
     * 
     * <p><strong>Return Values Options:</strong></p>
     * <ul>
     * <li><strong>NONE:</strong> Nothing returned (default)</li>
     * <li><strong>ALL_OLD:</strong> All attributes of the deleted item</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> key = asKey("userId", "user123");
     * 
     * DeleteItemResult result = executor.deleteItem("Users", key, "ALL_OLD");
     * Map<String, AttributeValue> deletedItem = result.getAttributes();
     * if (deletedItem != null) {
     *     System.out.println("Deleted user: " + deletedItem.get("name"));
     *     // Could save to audit log or archive table
     * }
     * }</pre>
     * 
     * @param tableName the name of the DynamoDB table. Must not be {@code null} or empty.
     * @param key the primary key of the item to delete. Must not be {@code null}.
     * @param returnValues "NONE" or "ALL_OLD" to get deleted item attributes
     * @return a {@link DeleteItemResult} containing metadata and optionally the deleted item
     * @throws IllegalArgumentException if tableName is null/empty or key is null
     * @see com.amazonaws.services.dynamodbv2.model.ReturnValue
     */
    public DeleteItemResult deleteItem(final String tableName, final Map<String, AttributeValue> key, final String returnValues) {
        return dynamoDBClient.deleteItem(tableName, key, returnValues);
    }

    /**
     * Deletes an item using a fully configured DeleteItemRequest.
     * 
     * <p>This method provides complete control over delete operations, including conditional
     * expressions to prevent accidental deletions, return values, and consumed capacity
     * reporting. Use this for safe deletes with conditions or when you need detailed metrics.</p>
     * 
     * <p><strong>Advanced Features:</strong></p>
     * <ul>
     * <li>Conditional expressions for safe deletes</li>
     * <li>Return deleted item attributes</li>
     * <li>Consumed capacity reporting</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DeleteItemRequest request = new DeleteItemRequest()
     *     .withTableName("Users")
     *     .withKey(asKey("userId", "user123"))
     *     .withConditionExpression("attribute_exists(userId) AND #s = :status")
     *     .withExpressionAttributeNames(Map.of("#s", "status"))
     *     .withExpressionAttributeValues(Map.of(":status", attrValueOf("INACTIVE")))
     *     .withReturnValues("ALL_OLD");
     * 
     * try {
     *     DeleteItemResult result = executor.deleteItem(request);
     *     System.out.println("Deleted inactive user: " + result.getAttributes());
     * } catch (ConditionalCheckFailedException e) {
     *     System.out.println("User not found or still active");
     * }
     * }</pre>
     * 
     * @param deleteItemRequest the complete delete request. Must not be {@code null}.
     * @return a {@link DeleteItemResult} containing operation results
     * @throws IllegalArgumentException if deleteItemRequest is null
     */
    public DeleteItemResult deleteItem(final DeleteItemRequest deleteItemRequest) {
        return dynamoDBClient.deleteItem(deleteItemRequest);
    }

    /**
     * Executes a DynamoDB query and returns results as a list of maps.
     * 
     * <p>This method performs a Query operation and returns results as a list of maps,
     * where each map represents an item with attribute names as keys and attribute values
     * as values. This is useful for generic data retrieval without specific type conversion.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * QueryRequest request = new QueryRequest()
     *     .withTableName("Sales")
     *     .withKeyConditionExpression("region = :region")
     *     .withExpressionAttributeValues(Map.of(
     *         ":region", attrValueOf("US-WEST")
     *     ));
     * 
     * List<Map<String, Object>> sales = executor.list(request);
     * sales.forEach(System.out::println);   // Print each item as a map
     * }</pre>
     * 
     * @param queryRequest the query parameters. Must not be {@code null}.
     * @return a list of maps representing all query results, never {@code null}
     */
    public List<Map<String, Object>> list(final QueryRequest queryRequest) {
        return list(queryRequest, Clazz.PROPS_MAP);
    }

    /**
     * Executes a DynamoDB query and returns results as a list of entities.
     * 
     * <p>This method performs a Query operation and converts results to a list of
     * the specified target class. It supports both maps and entity classes with
     * getter/setter methods for attribute access.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * QueryRequest request = new QueryRequest()
     *     .withTableName("Sales")
     *     .withKeyConditionExpression("region = :region")
     *     .withExpressionAttributeValues(Map.of(
     *         ":region", attrValueOf("US-WEST")
     *     ));
     * 
     * List<Sale> sales = executor.list(request, Sale.class);
     * sales.forEach(System.out::println);   // Print each sale object
     * }</pre>
     *
     * @param <T> the type of objects to return
     * @param queryRequest the query parameters. Must not be {@code null}.
     * @param targetClass the class to convert retrieved items to. Must not be {@code null}.
     * @return a list of converted items, never {@code null}
     */
    public <T> List<T> list(final QueryRequest queryRequest, final Class<T> targetClass) {
        final QueryResult queryResult = dynamoDBClient.query(queryRequest);
        final List<T> res = toList(queryResult, targetClass);

        if (N.notEmpty(queryResult.getLastEvaluatedKey()) && N.isEmpty(queryRequest.getExclusiveStartKey())) {
            final QueryRequest newQueryRequest = queryRequest.clone();
            QueryResult newQueryResult = queryResult;

            while (N.notEmpty(newQueryResult.getLastEvaluatedKey())) {
                newQueryRequest.setExclusiveStartKey(newQueryResult.getLastEvaluatedKey());
                newQueryResult = dynamoDBClient.query(newQueryRequest);
                res.addAll(toList(newQueryResult, targetClass));
            }
        }

        return res;
    }

    //    /**
    //     *
    //     * @param targetClass <code>Map</code> or entity class with getter/setter method.
    //     * @param queryRequest
    //     * @param pageOffset
    //     * @param pageCount
    //     * @return
    //     * @see <a href="http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html#Query.Pagination">Query.Pagination</a>
    //     */
    //    public <T> List<T> list(final QueryRequest queryRequest, int pageOffset, int pageCount, final Class<T> targetClass) {
    //        N.checkArgument(pageOffset >= 0 && pageCount >= 0, "'pageOffset' and 'pageCount' can't be negative");
    //
    //        final List<T> res = new ArrayList<>();
    //        QueryRequest newQueryRequest = queryRequest;
    //        QueryResult queryResult = null;
    //
    //        do {
    //            if (queryResult != null && N.notEmpty(queryResult.getLastEvaluatedKey())) {
    //                if (newQueryRequest == queryRequest) {
    //                    newQueryRequest = queryRequest.clone();
    //                }
    //
    //                newQueryRequest.setExclusiveStartKey(queryResult.getLastEvaluatedKey());
    //            }
    //
    //            queryResult = dynamoDB.query(newQueryRequest);
    //        } while (pageOffset-- > 0 && N.notEmpty(queryResult.getItems()) && N.notEmpty(queryResult.getLastEvaluatedKey()));
    //
    //        if (pageOffset >= 0 || pageCount-- <= 0) {
    //            return res;
    //        } else {
    //            res.addAll(toList(targetClass, queryResult));
    //        }
    //
    //        while (pageCount-- > 0 && N.notEmpty(queryResult.getLastEvaluatedKey())) {
    //            if (newQueryRequest == queryRequest) {
    //                newQueryRequest = queryRequest.clone();
    //            }
    //
    //            newQueryRequest.setExclusiveStartKey(queryResult.getLastEvaluatedKey());
    //            queryResult = dynamoDB.query(newQueryRequest);
    //            res.addAll(toList(targetClass, queryResult));
    //        }
    //
    //        return res;
    //    }

    /**
     * Executes a DynamoDB query and returns results as a Dataset for tabular analysis.
     * 
     * <p>This method performs a Query operation and returns results in a {@link Dataset} format,
     * which provides a tabular view with columns and rows. Datasets are ideal for analytical
     * operations, data manipulation, and when you need to work with query results in a
     * spreadsheet-like format.</p>
     * 
     * <p><strong>Dataset Benefits:</strong></p>
     * <ul>
     * <li>Column-oriented operations</li>
     * <li>Built-in aggregation functions</li>
     * <li>Easy data export (CSV, JSON, etc.)</li>
     * <li>SQL-like operations</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * QueryRequest request = new QueryRequest()
     *     .withTableName("Sales")
     *     .withKeyConditionExpression("region = :region")
     *     .withExpressionAttributeValues(Map.of(
     *         ":region", attrValueOf("US-WEST")
     *     ));
     * 
     * Dataset sales = executor.query(request);
     * 
     * // Tabular operations
     * sales.println();   // Print as table
     * double totalRevenue = sales.getColumn("revenue")
     *     .mapToDouble(Double.class::cast)
     *     .sum();
     * 
     * // Export to CSV
     * sales.toCSV(new FileWriter("sales.csv"));
     * }</pre>
     * 
     * @param queryRequest the query parameters. Must not be {@code null}.
     * @return a {@link Dataset} containing all query results in tabular format
     * @throws IllegalArgumentException if queryRequest is null
     * @see #query(QueryRequest, Class) for typed Dataset operations
     */
    public Dataset query(final QueryRequest queryRequest) {
        return query(queryRequest, Clazz.PROPS_MAP);
    }

    /**     
     * Executes a DynamoDB query and returns results as a Dataset for the specified target class.
     * 
     * <p>This method performs a Query operation and converts results to a {@link Dataset} of the
     * specified target class. It supports both maps and entity classes with getter/setter methods
     * for attribute access, allowing you to work with structured data in a tabular format.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * QueryRequest request = new QueryRequest()
     *     .withTableName("Sales")
     *     .withKeyConditionExpression("region = :region")
     *     .withExpressionAttributeValues(Map.of(
     *         ":region", attrValueOf("US-WEST")
     *     ));
     * 
     * Dataset sales = executor.query(request, Sale.class);
     * sales.println();   // Print as table
     * }</pre>
     *
     * @param queryRequest the query parameters. Must not be {@code null}.
     * @param targetClass the class to convert retrieved items to. Must not be {@code null}.
     * @return a {@link Dataset} containing all query results in tabular format
     * @throws IllegalArgumentException if queryRequest or targetClass is null
     */
    public Dataset query(final QueryRequest queryRequest, final Class<?> targetClass) {
        if (targetClass == null || Map.class.isAssignableFrom(targetClass)) {
            final QueryResult queryResult = dynamoDBClient.query(queryRequest);
            final List<Map<String, AttributeValue>> items = queryResult.getItems();

            if (N.notEmpty(queryResult.getLastEvaluatedKey()) && N.isEmpty(queryRequest.getExclusiveStartKey())) {
                final QueryRequest newQueryRequest = queryRequest.clone();
                QueryResult newQueryResult = queryResult;

                while (N.notEmpty(newQueryResult.getLastEvaluatedKey())) {
                    newQueryRequest.setExclusiveStartKey(newQueryResult.getLastEvaluatedKey());
                    newQueryResult = dynamoDBClient.query(newQueryRequest);
                    items.addAll(newQueryResult.getItems());
                }
            }

            return extractData(items, 0, items.size());
        } else {
            return N.newDataset(list(queryRequest, targetClass));
        }
    }

    //    public Dataset query(final QueryRequest queryRequest, int pageOffset, int pageCount, final Class<?> targetClass) {
    //        return N.newDataset(find(targetClass, queryRequest, pageOffset, pageCount));
    //    }

    //    public <T> List<T> scan(final ScanRequest scanRequest, int pageOffset, int pageCount, final Class<T> targetClass) {
    //        N.checkArgument(pageOffset >= 0 && pageCount >= 0, "'pageOffset' and 'pageCount' can't be negative");
    //
    //        final List<T> res = new ArrayList<>();
    //        ScanRequest newQueryRequest = scanRequest;
    //        ScanResult queryResult = null;
    //
    //        do {
    //            if (queryResult != null && N.notEmpty(queryResult.getLastEvaluatedKey())) {
    //                if (newQueryRequest == scanRequest) {
    //                    newQueryRequest = scanRequest.clone();
    //                }
    //
    //                newQueryRequest.setExclusiveStartKey(queryResult.getLastEvaluatedKey());
    //            }
    //
    //            queryResult = dynamoDB.scan(newQueryRequest);
    //        } while (pageOffset-- > 0 && N.notEmpty(queryResult.getItems()) && N.notEmpty(queryResult.getLastEvaluatedKey()));
    //
    //        if (pageOffset >= 0 || pageCount-- <= 0) {
    //            return res;
    //        } else {
    //            res.addAll(toList(targetClass, queryResult));
    //        }
    //
    //        while (pageCount-- > 0 && N.notEmpty(queryResult.getLastEvaluatedKey())) {
    //            if (newQueryRequest == scanRequest) {
    //                newQueryRequest = scanRequest.clone();
    //            }
    //
    //            newQueryRequest.setExclusiveStartKey(queryResult.getLastEvaluatedKey());
    //            queryResult = dynamoDB.scan(newQueryRequest);
    //            res.addAll(toList(targetClass, queryResult));
    //        }
    //
    //        return res;
    //    }

    /**
     * Creates a Stream of maps for DynamoDB query results with automatic pagination.
     *
     * <p>This method performs a Query operation and returns results as a lazy-evaluated {@link Stream}
     * of maps, where each map represents an item with attribute names as keys and values as objects.
     * The stream automatically handles DynamoDB pagination, fetching additional pages as needed
     * during iteration.</p>
     *
     * <p><strong>Stream Benefits:</strong></p>
     * <ul>
     * <li><strong>Memory Efficient:</strong> Items are loaded on-demand, not all at once</li>
     * <li><strong>Automatic Pagination:</strong> Transparently handles LastEvaluatedKey</li>
     * <li><strong>Lazy Evaluation:</strong> Processing only occurs during terminal operations</li>
     * <li><strong>Functional Operations:</strong> Full support for filter, map, reduce, etc.</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * QueryRequest request = new QueryRequest()
     *     .withTableName("Sales")
     *     .withKeyConditionExpression("region = :region")
     *     .withExpressionAttributeValues(Map.of(
     *         ":region", attrValueOf("US-WEST")
     *     ));
     *
     * try (Stream<Map<String, Object>> stream = executor.stream(request)) {
     *     double totalRevenue = stream
     *         .filter(item -> "COMPLETED".equals(item.get("status")))
     *         .mapToDouble(item -> ((Number) item.get("revenue")).doubleValue())
     *         .sum();
     *     System.out.println("Total revenue: " + totalRevenue);
     * }
     * }</pre>
     *
     * @param queryRequest the query parameters. Must not be {@code null}.
     * @return a {@link Stream} of maps representing query results with automatic pagination
     * @throws IllegalArgumentException if queryRequest is null
     * @see #stream(QueryRequest, Class) for typed stream operations
     */
    public Stream<Map<String, Object>> stream(final QueryRequest queryRequest) {
        return stream(queryRequest, Clazz.PROPS_MAP);
    }

    /**
     * Creates a Stream of typed objects for DynamoDB query results with automatic pagination.
     *
     * <p>This method performs a Query operation and returns results as a lazy-evaluated {@link Stream}
     * of the specified target class. The stream automatically handles DynamoDB pagination and converts
     * each item to the target type using the same mapping rules as other query methods.</p>
     *
     * <p><strong>Stream Benefits:</strong></p>
     * <ul>
     * <li><strong>Memory Efficient:</strong> Items are loaded on-demand, not all at once</li>
     * <li><strong>Automatic Pagination:</strong> Transparently handles LastEvaluatedKey</li>
     * <li><strong>Lazy Evaluation:</strong> Processing only occurs during terminal operations</li>
     * <li><strong>Functional Operations:</strong> Full support for filter, map, reduce, etc.</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * QueryRequest request = new QueryRequest()
     *     .withTableName("Users")
     *     .withKeyConditionExpression("status = :status")
     *     .withExpressionAttributeValues(Map.of(
     *         ":status", attrValueOf("ACTIVE")
     *     ));
     *
     * try (Stream<User> userStream = executor.stream(request, User.class)) {
     *     List<String> adminEmails = userStream
     *         .filter(user -> "ADMIN".equals(user.getRole()))
     *         .map(User::getEmail)
     *         .collect(Collectors.toList());
     * }
     * }</pre>
     *
     * @param <T> the target type for conversion
     * @param queryRequest the query parameters. Must not be {@code null}.
     * @param targetClass the class to convert retrieved items to. Must not be {@code null}.
     * @return a {@link Stream} of converted objects with automatic pagination
     * @throws IllegalArgumentException if queryRequest or targetClass is null or unsupported
     */
    public <T> Stream<T> stream(final QueryRequest queryRequest, final Class<T> targetClass) {
        final Iterator<List<Map<String, AttributeValue>>> iterator = new ObjIterator<>() {
            private final QueryRequest newQueryRequest = queryRequest.clone();
            private QueryResult queryResult = null;
            private List<Map<String, AttributeValue>> items = null;

            @Override
            public boolean hasNext() {
                if (items == null || items.isEmpty()) {
                    while (queryResult == null || N.notEmpty(queryResult.getLastEvaluatedKey())) {
                        if (queryResult != null && N.notEmpty(queryResult.getLastEvaluatedKey())) {
                            newQueryRequest.setExclusiveStartKey(queryResult.getLastEvaluatedKey());
                        }

                        queryResult = dynamoDBClient.query(newQueryRequest);

                        if (queryResult.getItems() != null && !queryResult.getItems().isEmpty()) {
                            items = queryResult.getItems();
                            break;
                        } else {
                            items = null;
                        }
                    }
                }

                return N.notEmpty(items);
            }

            @Override
            public List<Map<String, AttributeValue>> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException(InternalUtil.ERROR_MSG_FOR_NO_SUCH_EX);
                }

                final List<Map<String, AttributeValue>> ret = items;
                items = null;
                return ret;
            }
        };

        return Stream.of(iterator).flatmap(Fn.identity()).map(createRowMapper(targetClass));
    }

    /**
     * Creates a Stream of maps for DynamoDB scan results with automatic pagination and attributes to get.
     *
     * <p>This method performs a Scan operation with specified attributes to retrieve and returns results
     * as a lazy-evaluated {@link Stream} of maps. The stream automatically handles DynamoDB pagination,
     * fetching additional pages as needed during iteration.</p>
     *
     * <p><strong>Stream Benefits:</strong></p>
     * <ul>
     * <li><strong>Memory Efficient:</strong> Items are loaded on-demand, not all at once</li>
     * <li><strong>Automatic Pagination:</strong> Transparently handles LastEvaluatedKey</li>
     * <li><strong>Lazy Evaluation:</strong> Processing only occurs during terminal operations</li>
     * <li><strong>Functional Operations:</strong> Full support for filter, map, reduce, etc.</li>
     * </ul>
     *
     * @param tableName the name of the DynamoDB table. Must not be {@code null} or empty.
     * @param attributesToGet the list of attributes to retrieve. Must not be {@code null}.
     * @return a {@link Stream} of maps representing scan results with automatic pagination
     * @throws IllegalArgumentException if tableName is null/empty or attributesToGet is null
     */
    public Stream<Map<String, Object>> scan(final String tableName, final List<String> attributesToGet) {
        return scan(new ScanRequest().withTableName(tableName).withAttributesToGet(attributesToGet));
    }

    /**
     * Creates a Stream of maps for DynamoDB scan results with automatic pagination and filter conditions.
     *
     * <p>This method performs a Scan operation with optional filter conditions and returns results
     * as a lazy-evaluated {@link Stream} of maps. The stream automatically handles DynamoDB pagination,
     * fetching additional pages as needed during iteration.</p>
     *
     * <p><strong>Stream Benefits:</strong></p>
     * <ul>
     * <li><strong>Memory Efficient:</strong> Items are loaded on-demand, not all at once</li>
     * <li><strong>Automatic Pagination:</strong> Transparently handles LastEvaluatedKey</li>
     * <li><strong>Lazy Evaluation:</strong> Processing only occurs during terminal operations</li>
     * <li><strong>Functional Operations:</strong> Full support for filter, map, reduce, etc.</li>
     * </ul>
     *
     * @param tableName the name of the DynamoDB table. Must not be {@code null} or empty.
     * @param scanFilter the filter conditions for the scan. Must not be {@code null}.
     * @return a {@link Stream} of maps representing scan results with automatic pagination
     * @throws IllegalArgumentException if tableName is null/empty or scanFilter is null
     */
    public Stream<Map<String, Object>> scan(final String tableName, final Map<String, Condition> scanFilter) {
        return scan(new ScanRequest().withTableName(tableName).withScanFilter(scanFilter));
    }

    /**
     * Creates a Stream of maps for DynamoDB scan results with automatic pagination, attributes to get, and filter conditions.
     *
     * <p>This method performs a Scan operation with specified attributes to retrieve and optional filter conditions,
     * returning results as a lazy-evaluated {@link Stream} of maps. The stream automatically handles DynamoDB pagination,
     * fetching additional pages as needed during iteration.</p>
     *
     * <p><strong>Stream Benefits:</strong></p>
     * <ul>
     * <li><strong>Memory Efficient:</strong> Items are loaded on-demand, not all at once</li>
     * <li><strong>Automatic Pagination:</strong> Transparently handles LastEvaluatedKey</li>
     * <li><strong>Lazy Evaluation:</strong> Processing only occurs during terminal operations</li>
     * <li><strong>Functional Operations:</strong> Full support for filter, map, reduce, etc.</li>
     * </ul>
     *
     * @param tableName the name of the DynamoDB table. Must not be {@code null} or empty.
     * @param attributesToGet the list of attributes to retrieve. Must not be {@code null}.
     * @param scanFilter the filter conditions for the scan. Must not be {@code null}.
     * @return a {@link Stream} of maps representing scan results with automatic pagination
     * @throws IllegalArgumentException if tableName is null/empty, attributesToGet is null, or scanFilter is null
     */
    public Stream<Map<String, Object>> scan(final String tableName, final List<String> attributesToGet, final Map<String, Condition> scanFilter) {
        return scan(new ScanRequest().withTableName(tableName).withAttributesToGet(attributesToGet).withScanFilter(scanFilter));
    }

    /**
     * Creates a Stream of maps for DynamoDB scan results with automatic pagination.
     *
     * <p>This method performs a Scan operation and returns results as a lazy-evaluated {@link Stream}
     * of maps, where each map represents an item with attribute names as keys and values as objects.
     * The stream automatically handles DynamoDB pagination, fetching additional pages as needed
     * during iteration.</p>
     *
     * <p><strong>Stream Benefits:</strong></p>
     * <ul>
     * <li><strong>Memory Efficient:</strong> Items are loaded on-demand, not all at once</li>
     * <li><strong>Automatic Pagination:</strong> Transparently handles LastEvaluatedKey</li>
     * <li><strong>Lazy Evaluation:</strong> Processing only occurs during terminal operations</li>
     * <li><strong>Functional Operations:</strong> Full support for filter, map, reduce, etc.</li>
     * </ul>
     *
     * @param scanRequest the scan parameters. Must not be {@code null}.
     * @return a {@link Stream} of maps representing scan results with automatic pagination
     * @throws IllegalArgumentException if scanRequest is null
     */
    public Stream<Map<String, Object>> scan(final ScanRequest scanRequest) {
        return scan(scanRequest, Clazz.PROPS_MAP);
    }

    /**
     * Creates a Stream of typed objects for DynamoDB scan results with specified attributes.
     *
     * <p>This method performs a Scan operation on the specified table, retrieving only the
     * requested attributes, and returns results as a lazy-evaluated {@link Stream} of the
     * specified target class. The stream automatically handles DynamoDB pagination and converts
     * each item to the target type.</p>
     *
     * <p><strong>Stream Benefits:</strong></p>
     * <ul>
     * <li><strong>Memory Efficient:</strong> Items are loaded on-demand, not all at once</li>
     * <li><strong>Automatic Pagination:</strong> Transparently handles LastEvaluatedKey</li>
     * <li><strong>Lazy Evaluation:</strong> Processing only occurs during terminal operations</li>
     * <li><strong>Functional Operations:</strong> Full support for filter, map, reduce, etc.</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<String> attributes = Arrays.asList("userId", "name", "email");
     * 
     * try (Stream<User> userStream = executor.scan("Users", attributes, User.class)) {
     *     List<String> activeEmails = userStream
     *         .filter(user -> user.getName() != null)
     *         .map(User::getEmail)
     *         .collect(Collectors.toList());
     * }
     * }</pre>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the DynamoDB table. Must not be {@code null} or empty.
     * @param attributesToGet the list of attributes to retrieve. Must not be {@code null}.
     * @param targetClass the class to convert retrieved items to. Must not be {@code null}.
     * @return a {@link Stream} of converted objects with automatic pagination
     * @throws IllegalArgumentException if tableName is null/empty, attributesToGet is null, or targetClass is null
     */
    public <T> Stream<T> scan(final String tableName, final List<String> attributesToGet, final Class<T> targetClass) {
        return scan(new ScanRequest().withTableName(tableName).withAttributesToGet(attributesToGet), targetClass);
    }

    /**
     * Creates a Stream of typed objects for DynamoDB scan results with filter conditions.
     *
     * <p>This method performs a Scan operation with filter conditions and returns results as a
     * lazy-evaluated {@link Stream} of the specified target class. The stream automatically handles
     * DynamoDB pagination and converts each item to the target type. Filter conditions are applied
     * server-side to reduce data transfer and improve performance.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, Condition> filters = Map.of(
     *     "status", new Condition()
     *         .withComparisonOperator(ComparisonOperator.EQ)
     *         .withAttributeValueList(attrValueOf("ACTIVE"))
     * );
     * 
     * try (Stream<User> activeUsers = executor.scan("Users", filters, User.class)) {
     *     long count = activeUsers.count();
     *     System.out.println("Active users: " + count);
     * }
     * }</pre>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the DynamoDB table. Must not be {@code null} or empty.
     * @param scanFilter the filter conditions for the scan. Must not be {@code null}.
     * @param targetClass the class to convert retrieved items to. Must not be {@code null}.
     * @return a {@link Stream} of converted objects with automatic pagination
     * @throws IllegalArgumentException if tableName is null/empty, scanFilter is null, or targetClass is null
     */
    public <T> Stream<T> scan(final String tableName, final Map<String, Condition> scanFilter, final Class<T> targetClass) {
        return scan(new ScanRequest().withTableName(tableName).withScanFilter(scanFilter), targetClass);
    }

    /**
     * Creates a Stream of typed objects for DynamoDB scan results with specified attributes and filter conditions.
     *
     * <p>This method performs a Scan operation with both attribute selection and filter conditions,
     * returning results as a lazy-evaluated {@link Stream} of the specified target class. This combination
     * provides optimal performance by retrieving only needed attributes and filtering server-side.</p>
     *
     * <p><strong>Stream Benefits:</strong></p>
     * <ul>
     * <li><strong>Memory Efficient:</strong> Items are loaded on-demand, not all at once</li>
     * <li><strong>Automatic Pagination:</strong> Transparently handles LastEvaluatedKey</li>
     * <li><strong>Lazy Evaluation:</strong> Processing only occurs during terminal operations</li>
     * <li><strong>Functional Operations:</strong> Full support for filter, map, reduce, etc.</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<String> attributes = Arrays.asList("userId", "name", "department");
     * Map<String, Condition> filters = Map.of(
     *     "department", new Condition()
     *         .withComparisonOperator(ComparisonOperator.EQ)
     *         .withAttributeValueList(attrValueOf("Engineering"))
     * );
     * 
     * try (Stream<Employee> engineers = executor.scan("Employees", attributes, filters, Employee.class)) {
     *     List<String> engineerNames = engineers
     *         .map(Employee::getName)
     *         .sorted()
     *         .collect(Collectors.toList());
     * }
     * }</pre>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the DynamoDB table. Must not be {@code null} or empty.
     * @param attributesToGet the list of attributes to retrieve. Must not be {@code null}.
     * @param scanFilter the filter conditions for the scan. Must not be {@code null}.
     * @param targetClass the class to convert retrieved items to. Must not be {@code null}.
     * @return a {@link Stream} of converted objects with automatic pagination
     * @throws IllegalArgumentException if any parameter is null or tableName is empty
     */
    public <T> Stream<T> scan(final String tableName, final List<String> attributesToGet, final Map<String, Condition> scanFilter, final Class<T> targetClass) {
        return scan(new ScanRequest().withTableName(tableName).withAttributesToGet(attributesToGet).withScanFilter(scanFilter), targetClass);
    }

    /**
     * Creates a Stream of typed objects for DynamoDB scan results using a fully configured ScanRequest.
     *
     * <p>This method performs a Scan operation using a complete {@link ScanRequest} and returns results
     * as a lazy-evaluated {@link Stream} of the specified target class. The stream automatically handles
     * DynamoDB pagination by managing LastEvaluatedKey internally, providing seamless iteration over
     * large result sets without memory concerns.</p>
     *
     * <p><strong>Automatic Pagination Implementation:</strong></p>
     * <ul>
     * <li>Uses internal iterator to manage scan state</li>
     * <li>Automatically sets ExclusiveStartKey for subsequent pages</li>
     * <li>Continues until LastEvaluatedKey is null (no more items)</li>
     * <li>Lazy loading - only fetches pages as needed during iteration</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ScanRequest request = new ScanRequest()
     *     .withTableName("Products")
     *     .withFilterExpression("price BETWEEN :low AND :high")
     *     .withExpressionAttributeValues(Map.of(
     *         ":low", attrValueOf(100),
     *         ":high", attrValueOf(500)
     *     ))
     *     .withProjectionExpression("productId, name, price")
     *     .withLimit(50);   // Page size
     * 
     * try (Stream<Product> products = executor.scan(request, Product.class)) {
     *     Map<String, Double> avgPriceByCategory = products
     *         .collect(Collectors.groupingBy(
     *             Product::getCategory,
     *             Collectors.averagingDouble(Product::getPrice)
     *         ));
     * }
     * }</pre>
     *
     * @param <T> the target type for conversion
     * @param scanRequest the complete scan request with all parameters. Must not be {@code null}.
     * @param targetClass the class to convert retrieved items to. Must not be {@code null}.
     * @return a {@link Stream} of converted objects with automatic pagination
     * @throws IllegalArgumentException if scanRequest or targetClass is null or unsupported
     * @see #scan(ScanRequest) for untyped stream operations
     */
    public <T> Stream<T> scan(final ScanRequest scanRequest, final Class<T> targetClass) {
        final Iterator<List<Map<String, AttributeValue>>> iterator = new ObjIterator<>() {
            private final ScanRequest newScanRequest = scanRequest.clone();
            private ScanResult scanResult = null;
            private List<Map<String, AttributeValue>> items = null;

            @Override
            public boolean hasNext() {
                if (items == null || items.isEmpty()) {
                    while (scanResult == null || N.notEmpty(scanResult.getLastEvaluatedKey())) {
                        if (scanResult != null && N.notEmpty(scanResult.getLastEvaluatedKey())) {
                            newScanRequest.setExclusiveStartKey(scanResult.getLastEvaluatedKey());
                        }

                        scanResult = dynamoDBClient.scan(newScanRequest);

                        if (scanResult.getItems() != null && !scanResult.getItems().isEmpty()) {
                            items = scanResult.getItems();
                            break;
                        } else {
                            items = null;
                        }
                    }
                }

                return N.notEmpty(items);
            }

            @Override
            public List<Map<String, AttributeValue>> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException(InternalUtil.ERROR_MSG_FOR_NO_SUCH_EX);
                }

                final List<Map<String, AttributeValue>> ret = items;
                items = null;
                return ret;
            }
        };

        return Stream.of(iterator).flatmap(Fn.identity()).map(createRowMapper(targetClass));
    }

    /**
     * Closes the DynamoDB client and releases all resources.
     *
     * <p>This method should be called when the DynamoDBExecutor is no longer needed to
     * ensure that all resources are properly released.</p>
     */
    @Override
    public void close() {
        dynamoDBClient.shutdown();
    }

    /**
     * A generic mapper class for DynamoDB operations that provides a simplified interface
     * for common database operations on a specific entity type and table.
     * 
     * @param <T> the type of entity this mapper handles
     */
    public static class Mapper<T> {
        private final DynamoDBExecutor dynamoDBExecutor;
        private final String tableName;
        private final Class<T> targetEntityClass;
        private final BeanInfo entityInfo;
        private final List<String> keyPropNames;
        private final List<PropInfo> keyPropInfos;
        private final NamingPolicy namingPolicy;

        Mapper(final Class<T> targetEntityClass, final DynamoDBExecutor dynamoDBExecutor, final String tableName, final NamingPolicy namingPolicy) {
            N.checkArgNotNull(targetEntityClass, "targetEntityClass");
            N.checkArgNotNull(dynamoDBExecutor, "dynamoDBExecutor");
            N.checkArgNotEmpty(tableName, "tableName");

            N.checkArgument(Beans.isBeanClass(targetEntityClass), "{} is not an entity class with getter/setter method", targetEntityClass);

            @SuppressWarnings("deprecation")
            final List<String> idPropNames = QueryUtil.getIdPropNames(targetEntityClass);

            if (idPropNames.size() != 1) {
                throw new IllegalArgumentException(
                        "No or multiple ids: " + idPropNames + " defined/annotated in class: " + ClassUtil.getCanonicalClassName(targetEntityClass));
            }

            this.dynamoDBExecutor = dynamoDBExecutor;
            this.targetEntityClass = targetEntityClass;
            this.tableName = tableName;
            entityInfo = ParserUtil.getBeanInfo(targetEntityClass);
            keyPropInfos = Stream.of(idPropNames).map(entityInfo::getPropInfo).toList();
            keyPropNames = Stream.of(keyPropInfos).map(it -> Strings.isEmpty(it.columnName.orElseNull()) ? it.name : it.columnName.orElseNull()).toList();

            this.namingPolicy = namingPolicy == null ? NamingPolicy.CAMEL_CASE : namingPolicy;
        }

        /**
         * Retrieves a single item from DynamoDB using the key values extracted from the provided entity.
         *
         * <p>This method uses eventual consistency by default for better performance and lower cost.
         * The key values are automatically extracted from the entity's ID fields.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User user = new User();
         * user.setId("123");
         * User retrieved = mapper.getItem(user);
         * if (retrieved != null) {
         *     System.out.println("Found user: " + retrieved.getName());
         * }
         * }</pre>
         *
         * @param entity the entity containing the key values to search for, must not be {@code null}
         * @return the retrieved entity from DynamoDB, or {@code null} if not found
         * @throws IllegalArgumentException if entity is {@code null}
         */
        public T getItem(final T entity) {
            return dynamoDBExecutor.getItem(tableName, createKey(entity), targetEntityClass);
        }

        /**
         * Retrieves a single item from DynamoDB using the key values extracted from the provided entity.
         *
         * <p>This method allows you to specify the read consistency model. Use strongly consistent reads
         * when you need the most up-to-date data, though it will consume more capacity and have higher latency.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User user = new User();
         * user.setId("123");
         * User retrieved = mapper.getItem(user, true);   // strongly consistent read
         * if (retrieved != null) {
         *     System.out.println("Latest user data: " + retrieved.getName());
         * }
         * }</pre>
         *
         * @param entity the entity containing the key values to search for, must not be {@code null}
         * @param consistentRead if true, performs a strongly consistent read; if false or {@code null}, uses eventual consistency
         * @return the retrieved entity from DynamoDB, or {@code null} if not found
         * @throws IllegalArgumentException if entity is {@code null}
         */
        public T getItem(final T entity, final Boolean consistentRead) {
            return dynamoDBExecutor.getItem(tableName, createKey(entity), consistentRead, targetEntityClass);
        }

        /**
         * Retrieves a single item from DynamoDB using the provided key map.
         *
         * <p>This method allows direct specification of the primary key as a map of AttributeValue objects,
         * providing flexibility when the key is constructed programmatically or from external sources.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, AttributeValue> key = new HashMap<>();
         * key.put("userId", new AttributeValue().withS("123"));
         * User retrieved = mapper.getItem(key);
         * if (retrieved != null) {
         *     System.out.println("Found user: " + retrieved.getName());
         * }
         * }</pre>
         *
         * @param key a map of attribute names to AttributeValue objects representing the primary key, must not be {@code null}
         * @return the retrieved entity from DynamoDB, or {@code null} if not found
         * @throws IllegalArgumentException if key is {@code null}
         */
        public T getItem(final Map<String, AttributeValue> key) {
            return dynamoDBExecutor.getItem(tableName, key, targetEntityClass);
        }

        /**
         * Retrieves a single item from DynamoDB using a custom GetItemRequest.
         *
         * <p>This method provides full control over the GetItem operation by accepting a complete request object.
         * If no table name is specified in the request, the mapper's configured table name is used automatically.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * GetItemRequest request = new GetItemRequest()
         *     .withKey(key)
         *     .withConsistentRead(true)
         *     .withProjectionExpression("userId, name, email")
         *     .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
         * User retrieved = mapper.getItem(request);
         * }</pre>
         *
         * @param getItemRequest the GetItemRequest containing query parameters, must not be {@code null}
         * @return the retrieved entity from DynamoDB, or {@code null} if not found
         * @throws IllegalArgumentException if getItemRequest is {@code null} or specifies a different table than configured
         */
        public T getItem(final GetItemRequest getItemRequest) {
            return dynamoDBExecutor.getItem(checkItem(getItemRequest), targetEntityClass);
        }

        /**
         * Retrieves multiple items from DynamoDB in a single batch operation using key values extracted from entities.
         *
         * <p>This method efficiently retrieves multiple items in a single request (up to 100 items).
         * The key values are automatically extracted from each entity's ID fields. This is significantly
         * more efficient than making individual getItem calls for each entity.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<User> userStubs = Arrays.asList(user1, user2, user3);
         * List<User> retrieved = mapper.batchGetItem(userStubs);
         * System.out.println("Retrieved " + retrieved.size() + " users");
         * }</pre>
         *
         * @param entities collection of entities containing the key values to search for, must not be {@code null}
         * @return list of retrieved entities from DynamoDB; empty list if none found, never {@code null}
         * @throws IllegalArgumentException if entities is {@code null}
         */
        public List<T> batchGetItem(final Collection<? extends T> entities) {
            final Map<String, List<T>> map = dynamoDBExecutor.batchGetItem(createKeys(entities), targetEntityClass);

            if (N.isEmpty(map)) {
                return new ArrayList<>();
            } else {
                final List<T> result = map.values().iterator().next();
                return result != null ? result : new ArrayList<>();
            }
        }

        /**
         * Retrieves multiple items from DynamoDB in a single batch operation with specified
         * return consumed capacity settings.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<User> users = Arrays.asList(user1, user2, user3);
         * List<User> retrieved = mapper.batchGetItem(users, "TOTAL");
         * }</pre>
         *
         * @param entities collection of entities containing the key values to search for
         * @param returnConsumedCapacity specifies the level of detail about consumed capacity to return
         * @return list of retrieved entities from DynamoDB; empty list if none found
         */
        public List<T> batchGetItem(final Collection<? extends T> entities, final String returnConsumedCapacity) {
            final Map<String, List<T>> map = dynamoDBExecutor.batchGetItem(createKeys(entities), returnConsumedCapacity, targetEntityClass);

            if (N.isEmpty(map)) {
                return new ArrayList<>();
            } else {
                final List<T> result = map.values().iterator().next();
                return result != null ? result : new ArrayList<>();
            }
        }

        /**
         * Retrieves multiple items from DynamoDB using a custom BatchGetItemRequest.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * BatchGetItemRequest request = new BatchGetItemRequest()
         *     .withRequestItems(requestItems);
         * List<User> retrieved = mapper.batchGetItem(request);
         * }</pre>
         *
         * @param batchGetItemRequest the BatchGetItemRequest containing query parameters
         * @return list of retrieved entities from DynamoDB; empty list if none found
         * @throws IllegalArgumentException if the request specifies a different table than configured
         */
        public List<T> batchGetItem(final BatchGetItemRequest batchGetItemRequest) {
            final Map<String, List<T>> map = dynamoDBExecutor.batchGetItem(checkItem(batchGetItemRequest), targetEntityClass);

            if (N.isEmpty(map)) {
                return new ArrayList<>();
            } else {
                final List<T> result = map.values().iterator().next();
                return result != null ? result : new ArrayList<>();
            }
        }

        /**
         * Creates or replaces an item in DynamoDB with the provided entity.
         *
         * <p>This method converts the entity to DynamoDB format and performs a PutItem operation.
         * If an item with the same primary key exists, it will be completely replaced with the new entity.
         * All non-null attributes from the entity are written to DynamoDB.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User user = new User("123", "John Doe");
         * user.setEmail("john@example.com");
         * PutItemResult result = mapper.putItem(user);
         * System.out.println("Item saved successfully");
         * }</pre>
         *
         * @param entity the entity to save to DynamoDB, must not be {@code null}
         * @return the PutItemResult containing operation metadata, never {@code null}
         * @throws IllegalArgumentException if entity is {@code null}
         */
        public PutItemResult putItem(final T entity) {
            return dynamoDBExecutor.putItem(tableName, DynamoDBExecutor.toItem(entity, namingPolicy));
        }

        /**
         * Creates or replaces an item in DynamoDB with the provided entity,
         * optionally returning the old item values.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User user = new User("123", "John Doe");
         * PutItemResult result = mapper.putItem(user, "ALL_OLD");
         * }</pre>
         * 
         * @param entity the entity to save to DynamoDB
         * @param returnValues specifies which attributes to return (e.g., "ALL_OLD", "NONE")
         * @return the PutItemResult containing operation metadata and optionally old values
         */
        public PutItemResult putItem(final T entity, final String returnValues) {
            return dynamoDBExecutor.putItem(tableName, DynamoDBExecutor.toItem(entity, namingPolicy), returnValues);
        }

        /**
         * Creates or replaces an item in DynamoDB using a custom PutItemRequest.
         * If no table name is specified in the request, uses the mapper's configured table name.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * PutItemRequest request = new PutItemRequest()
         *     .withItem(item)
         *     .withReturnValues("ALL_OLD");
         * PutItemResult result = mapper.putItem(request);
         * }</pre>
         * 
         * @param putItemRequest the PutItemRequest containing the item and parameters
         * @return the PutItemResult containing operation metadata
         * @throws IllegalArgumentException if the request specifies a different table than configured
         */
        public PutItemResult putItem(final PutItemRequest putItemRequest) {
            return dynamoDBExecutor.putItem(checkItem(putItemRequest));
        }

        /**
         * Creates or replaces multiple items in DynamoDB in a single batch operation.
         *
         * @param entities collection of entities to save to DynamoDB
         * @return the BatchWriteItemResult containing operation metadata
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<User> users = Arrays.asList(user1, user2, user3);
         * BatchWriteItemResult result = mapper.batchPutItem(users);
         * }</pre>
         */
        public BatchWriteItemResult batchPutItem(final Collection<? extends T> entities) {
            return dynamoDBExecutor.batchWriteItem(createBatchPutRequest(entities));
        }

        /**
         * Updates an existing item in DynamoDB with the non-null values from the provided entity.
         *
         * <p>This method performs a partial update, modifying only the attributes that have non-null
         * values in the entity. Unlike putItem which replaces the entire item, this method preserves
         * any attributes not included in the entity.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User user = new User();
         * user.setId("123");
         * user.setName("Updated Name");
         * UpdateItemResult result = mapper.updateItem(user);
         * System.out.println("Item updated successfully");
         * }</pre>
         *
         * @param entity the entity containing updated values and key information, must not be {@code null}
         * @return the UpdateItemResult containing operation metadata, never {@code null}
         * @throws IllegalArgumentException if entity is {@code null}
         */
        public UpdateItemResult updateItem(final T entity) {
            return dynamoDBExecutor.updateItem(tableName, createKey(entity), DynamoDBExecutor.toUpdateItem(entity, namingPolicy));
        }

        /**
         * Updates an existing item in DynamoDB with the non-null values from the provided entity,
         * optionally returning values.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User user = new User();
         * user.setId("123");
         * user.setName("Updated Name");
         * UpdateItemResult result = mapper.updateItem(user, "ALL_NEW");
         * }</pre>
         * 
         * @param entity the entity containing updated values and key information
         * @param returnValues specifies which attributes to return (e.g., "ALL_NEW", "ALL_OLD", "UPDATED_NEW")
         * @return the UpdateItemResult containing operation metadata and optionally attribute values
         */
        public UpdateItemResult updateItem(final T entity, final String returnValues) {
            return dynamoDBExecutor.updateItem(tableName, createKey(entity), DynamoDBExecutor.toUpdateItem(entity, namingPolicy), returnValues);
        }

        /**
         * Updates an item in DynamoDB using a custom UpdateItemRequest.
         * If no table name is specified in the request, uses the mapper's configured table name.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * UpdateItemRequest request = new UpdateItemRequest()
         *     .withKey(key)
         *     .withUpdateExpression("SET #n = :val")
         *     .withExpressionAttributeNames(Map.of("#n", "name"))
         *     .withExpressionAttributeValues(Map.of(":val", new AttributeValue().withS("New Name")));
         * UpdateItemResult result = mapper.updateItem(request);
         * }</pre>
         * 
         * @param updateItemRequest the UpdateItemRequest containing update expressions and parameters
         * @return the UpdateItemResult containing operation metadata
         * @throws IllegalArgumentException if the request specifies a different table than configured
         */
        public UpdateItemResult updateItem(final UpdateItemRequest updateItemRequest) {
            return dynamoDBExecutor.updateItem(checkItem(updateItemRequest));
        }

        /**
         * Deletes an item from DynamoDB using the key values extracted from the provided entity.
         *
         * <p>This method removes the entire item from the table. The key values are automatically
         * extracted from the entity's ID fields. If the item doesn't exist, the operation completes
         * successfully without error.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User user = new User();
         * user.setId("123");
         * DeleteItemResult result = mapper.deleteItem(user);
         * System.out.println("Item deleted successfully");
         * }</pre>
         *
         * @param entity the entity containing the key values for deletion, must not be {@code null}
         * @return the DeleteItemResult containing operation metadata, never {@code null}
         * @throws IllegalArgumentException if entity is {@code null}
         */
        public DeleteItemResult deleteItem(final T entity) {
            return dynamoDBExecutor.deleteItem(tableName, createKey(entity));
        }

        /**
         * Deletes an item from DynamoDB using the key values extracted from the provided entity,
         * optionally returning the deleted item.
         *
         * <p>This method deletes an item and can optionally return the attributes of the deleted item,
         * which is useful for auditing, maintaining backups, or implementing undo functionality.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User user = new User();
         * user.setId("123");
         * DeleteItemResult result = mapper.deleteItem(user, "ALL_OLD");
         * Map<String, AttributeValue> deletedItem = result.getAttributes();
         * if (deletedItem != null) {
         *     System.out.println("Deleted user: " + deletedItem.get("name").getS());
         * }
         * }</pre>
         *
         * @param entity the entity containing the key values for deletion, must not be {@code null}
         * @param returnValues specifies which attributes to return: "ALL_OLD" returns all attributes
         *                    of the deleted item, "NONE" returns nothing (default)
         * @return the DeleteItemResult containing operation metadata and optionally the deleted item's attributes
         * @throws IllegalArgumentException if entity is {@code null}
         */
        public DeleteItemResult deleteItem(final T entity, final String returnValues) {
            return dynamoDBExecutor.deleteItem(tableName, createKey(entity), returnValues);
        }

        /**
         * Deletes an item from DynamoDB using the provided key map.
         *
         * <p>This method allows direct specification of the primary key as a map of AttributeValue objects,
         * providing flexibility when the key is constructed programmatically or from external sources.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, AttributeValue> key = new HashMap<>();
         * key.put("userId", new AttributeValue().withS("123"));
         * DeleteItemResult result = mapper.deleteItem(key);
         * System.out.println("Item deleted successfully");
         * }</pre>
         *
         * @param key a map of attribute names to AttributeValue objects representing the primary key, must not be {@code null}
         * @return the DeleteItemResult containing operation metadata
         * @throws IllegalArgumentException if key is {@code null}
         */
        public DeleteItemResult deleteItem(final Map<String, AttributeValue> key) {
            return dynamoDBExecutor.deleteItem(tableName, key);
        }

        /**
         * Deletes an item from DynamoDB using a custom DeleteItemRequest for maximum control.
         *
         * <p>This method provides complete flexibility by accepting a fully configured DeleteItemRequest,
         * allowing you to specify conditional expressions, return values, and other advanced parameters.
         * If no table name is specified in the request, the mapper's configured table name is used.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, AttributeValue> key = new HashMap<>();
         * key.put("userId", new AttributeValue().withS("123"));
         *
         * DeleteItemRequest request = new DeleteItemRequest()
         *     .withKey(key)
         *     .withConditionExpression("attribute_exists(userId)")
         *     .withReturnValues("ALL_OLD");
         *
         * try {
         *     DeleteItemResult result = mapper.deleteItem(request);
         *     System.out.println("Item deleted: " + result.getAttributes());
         * } catch (ConditionalCheckFailedException e) {
         *     System.out.println("Item does not exist");
         * }
         * }</pre>
         *
         * @param deleteItemRequest the DeleteItemRequest containing deletion parameters, must not be {@code null}
         * @return the DeleteItemResult containing operation metadata and optional return attributes
         * @throws IllegalArgumentException if the request specifies a different table than configured, or if deleteItemRequest is {@code null}
         */
        public DeleteItemResult deleteItem(final DeleteItemRequest deleteItemRequest) {
            return dynamoDBExecutor.deleteItem(checkItem(deleteItemRequest));
        }

        /**
         * Deletes multiple items from DynamoDB in a single batch operation for improved performance.
         *
         * <p>This method performs batch delete operations, which can delete up to 25 items in a single
         * network request. This is significantly more efficient than individual delete operations when
         * you need to remove multiple items.</p>
         *
         * <p><b>Batch Delete Limitations:</b></p>
         * <ul>
         * <li>Maximum 25 items per batch request</li>
         * <li>Maximum 16 MB total request size</li>
         * <li>Cannot use conditional expressions</li>
         * <li>Unprocessed items may be returned if limits are exceeded</li>
         * </ul>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User user1 = new User();
         * user1.setId("123");
         * User user2 = new User();
         * user2.setId("456");
         * User user3 = new User();
         * user3.setId("789");
         *
         * List<User> users = Arrays.asList(user1, user2, user3);
         * BatchWriteItemResult result = mapper.batchDeleteItem(users);
         *
         * // Check for unprocessed items
         * if (!result.getUnprocessedItems().isEmpty()) {
         *     System.out.println("Some items were not deleted and need retry");
         * }
         * }</pre>
         *
         * @param entities collection of entities containing the key values for deletion, must not be {@code null}
         * @return the BatchWriteItemResult containing operation metadata and any unprocessed items
         * @throws IllegalArgumentException if entities is {@code null} or exceeds batch limits (25 items)
         */
        public BatchWriteItemResult batchDeleteItem(final Collection<? extends T> entities) {
            return dynamoDBExecutor.batchWriteItem(createBatchDeleteRequest(entities));
        }

        /**
         * Performs a batch write operation (put and/or delete) using a custom BatchWriteItemRequest for full control.
         *
         * <p>This method provides complete flexibility by accepting a fully configured BatchWriteItemRequest,
         * allowing you to specify return consumed capacity, item collection metrics, and other advanced
         * parameters. If no table name is specified in the request, the mapper's configured table name is used.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<WriteRequest> writeRequests = Arrays.asList(
         *     new WriteRequest().withPutRequest(new PutRequest().withItem(item1)),
         *     new WriteRequest().withDeleteRequest(new DeleteRequest().withKey(key2))
         * );
         *
         * BatchWriteItemRequest request = new BatchWriteItemRequest()
         *     .withRequestItems(Map.of(tableName, writeRequests))
         *     .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
         *
         * BatchWriteItemResult result = mapper.batchWriteItem(request);
         * System.out.println("Consumed capacity: " + result.getConsumedCapacity());
         * }</pre>
         *
         * @param batchWriteItemRequest the BatchWriteItemRequest containing write operations, must not be {@code null}
         * @return the BatchWriteItemResult containing operation metadata and any unprocessed items
         * @throws IllegalArgumentException if the request specifies a different table than configured, or if batchWriteItemRequest is {@code null}
         */
        public BatchWriteItemResult batchWriteItem(final BatchWriteItemRequest batchWriteItemRequest) {
            return dynamoDBExecutor.batchWriteItem(checkItem(batchWriteItemRequest));
        }

        /**
         * Executes a query operation and returns all matching items as a list.
         *
         * <p>This method performs a Query operation to find items based on primary key and optionally
         * sort key conditions. All matching items are automatically loaded into memory, making this
         * suitable for results that fit comfortably in memory. For large result sets, consider using
         * the {@link #stream(QueryRequest)} method instead.</p>
         *
         * <p>If no table name is specified in the request, the mapper's configured table name is used.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * QueryRequest request = new QueryRequest()
         *     .withKeyConditionExpression("userId = :id")
         *     .withExpressionAttributeValues(Map.of(
         *         ":id", new AttributeValue().withS("123")
         *     ))
         *     .withFilterExpression("age > :minAge")
         *     .withExpressionAttributeValues(Map.of(
         *         ":minAge", new AttributeValue().withN("18")
         *     ));
         *
         * List<User> results = mapper.list(request);
         * System.out.println("Found " + results.size() + " users");
         * results.forEach(user -> System.out.println(user.getName()));
         * }</pre>
         *
         * @param queryRequest the QueryRequest containing query parameters, must not be {@code null}
         * @return list of entities matching the query conditions, never {@code null}
         * @throws IllegalArgumentException if the request specifies a different table than configured, or if queryRequest is {@code null}
         * @see #stream(QueryRequest) for memory-efficient processing of large result sets
         */
        public List<T> list(final QueryRequest queryRequest) {
            return dynamoDBExecutor.list(checkQueryRequest(queryRequest), targetEntityClass);
        }

        /**
         * Executes a query operation and returns the results as a Dataset for tabular analysis.
         *
         * <p>This method performs a Query operation and returns results in a {@link Dataset} format,
         * which provides a tabular view with columns and rows. Datasets are ideal for analytical
         * operations, aggregations, and when you need to work with query results in a structured,
         * spreadsheet-like format.</p>
         *
         * <p>If no table name is specified in the request, the mapper's configured table name is used.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * QueryRequest request = new QueryRequest()
         *     .withKeyConditionExpression("userId = :id")
         *     .withExpressionAttributeValues(Map.of(
         *         ":id", new AttributeValue().withS("123")
         *     ));
         *
         * Dataset results = mapper.query(request);
         *
         * // Print tabular view
         * results.println();
         *
         * // Perform aggregations
         * double avgAge = results.stream()
         *     .mapToDouble(row -> row.getDouble("age"))
         *     .average()
         *     .orElse(0.0);
         * System.out.println("Average age: " + avgAge);
         * }</pre>
         *
         * @param queryRequest the QueryRequest containing query parameters, must not be {@code null}
         * @return Dataset containing the query results with tabular structure, never {@code null}
         * @throws IllegalArgumentException if the request specifies a different table than configured, or if queryRequest is {@code null}
         * @see #list(QueryRequest) for list-based results
         * @see #stream(QueryRequest) for streaming results
         */
        public Dataset query(final QueryRequest queryRequest) {
            return dynamoDBExecutor.query(checkQueryRequest(queryRequest), targetEntityClass);
        }

        /**
         * Executes a query operation and returns the results as a Stream for memory-efficient processing.
         *
         * <p>This method performs a Query operation and returns results as a lazily-evaluated Stream,
         * which is ideal for processing large result sets without loading all items into memory at once.
         * The stream automatically handles pagination, seamlessly fetching additional pages as needed.</p>
         *
         * <p>If no table name is specified in the request, the mapper's configured table name is used.</p>
         *
         * <p><b>Important:</b> Streams should be closed after use to release resources. Use try-with-resources
         * for automatic resource management.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * QueryRequest request = new QueryRequest()
         *     .withKeyConditionExpression("userId = :id")
         *     .withExpressionAttributeValues(Map.of(
         *         ":id", new AttributeValue().withS("123")
         *     ));
         *
         * try (Stream<User> stream = mapper.stream(request)) {
         *     // Find first active user
         *     Optional<User> firstActive = stream
         *         .filter(u -> u.isActive())
         *         .findFirst();
         *
         *     // Or count matching items
         *     long count = stream.filter(u -> u.getAge() > 18).count();
         * }
         * }</pre>
         *
         * @param queryRequest the QueryRequest containing query parameters, must not be {@code null}
         * @return Stream of entities matching the query conditions with lazy evaluation and automatic pagination
         * @throws IllegalArgumentException if the request specifies a different table than configured, or if queryRequest is {@code null}
         * @see #list(QueryRequest) when you need all results in memory
         */
        public Stream<T> stream(final QueryRequest queryRequest) {
            return dynamoDBExecutor.stream(checkQueryRequest(queryRequest), targetEntityClass);
        }

        /**
         * Performs a full table scan with projection to retrieve only specified attributes.
         *
         * <p>This method scans every item in the table, which is an expensive operation for large tables.
         * Attribute projection reduces network transfer but does not reduce read capacity consumption.
         * Consider using Query operations with indexes instead of Scan when possible.</p>
         *
         * <p><b>Performance Warning:</b> Scan operations read every item in the table and consume
         * significant read capacity. Use with caution on large tables.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> attrs = Arrays.asList("id", "name", "email");
         * try (Stream<User> stream = mapper.scan(attrs)) {
         *     stream.limit(100)  // Process only first 100 items
         *         .forEach(System.out::println);
         * }
         * }</pre>
         *
         * @param attributesToGet list of attribute names to retrieve; {@code null} retrieves all attributes
         * @return Stream of all entities in the table with specified attributes, providing lazy evaluation
         * @see #scan(ScanRequest) for more control over scan operations
         */
        public Stream<T> scan(final List<String> attributesToGet) {
            return dynamoDBExecutor.scan(tableName, attributesToGet, targetEntityClass);
        }

        /**
         * Performs a table scan with filter conditions applied to results.
         *
         * <p>This method scans every item in the table and applies filters to determine which items
         * to return. <strong>Important:</strong> Filters are applied AFTER reading items, so you are
         * charged for reading all items in the table, not just the filtered results.</p>
         *
         * <p><b>Performance Warning:</b> Scan operations with filters still read every item in the
         * table and consume full read capacity. Use Query operations with indexes for better efficiency.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> filter = new HashMap<>();
         * filter.put("age", new Condition()
         *     .withComparisonOperator(ComparisonOperator.GT)
         *     .withAttributeValueList(new AttributeValue().withN("18")));
         *
         * try (Stream<User> stream = mapper.scan(filter)) {
         *     stream.forEach(System.out::println);
         * }
         * }</pre>
         *
         * @param scanFilter map of attribute names to {@link Condition} objects for filtering results,
         *                  must not be {@code null}
         * @return Stream of entities matching the filter conditions
         * @throws IllegalArgumentException if scanFilter is {@code null}
         * @see #scan(ScanRequest) for more efficient filtering with filter expressions
         */
        public Stream<T> scan(final Map<String, Condition> scanFilter) {
            return dynamoDBExecutor.scan(tableName, scanFilter, targetEntityClass);
        }

        /**
         * Performs a table scan with both attribute projection and filter conditions.
         *
         * <p>This method scans every item in the table, applies filters, and returns only the specified
         * attributes. Attribute projection reduces network transfer but does not reduce read capacity
         * costs, as filters are applied after reading items.</p>
         *
         * <p><b>Performance Warning:</b> Despite projection and filtering, this operation still reads
         * every item in the table and consumes full read capacity. Consider Query operations with
         * indexes for better efficiency.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> attrs = Arrays.asList("id", "name", "email");
         *
         * Map<String, Condition> filter = new HashMap<>();
         * filter.put("status", new Condition()
         *     .withComparisonOperator(ComparisonOperator.EQ)
         *     .withAttributeValueList(new AttributeValue("active")));
         *
         * try (Stream<User> stream = mapper.scan(attrs, filter)) {
         *     stream.limit(50)  // Process first 50 matching items
         *         .forEach(System.out::println);
         * }
         * }</pre>
         *
         * @param attributesToGet list of attribute names to retrieve; {@code null} retrieves all attributes
         * @param scanFilter map of attribute names to {@link Condition} objects for filtering results,
         *                  must not be {@code null}
         * @return Stream of entities with specified attributes matching the filter conditions
         * @throws IllegalArgumentException if scanFilter is {@code null}
         * @see #scan(ScanRequest) for full control with filter expressions
         */
        public Stream<T> scan(final List<String> attributesToGet, final Map<String, Condition> scanFilter) {
            return dynamoDBExecutor.scan(tableName, attributesToGet, scanFilter, targetEntityClass);
        }

        /**
         * Performs a table scan using a custom ScanRequest for maximum control.
         *
         * <p>This method provides complete flexibility by accepting a fully configured ScanRequest,
         * allowing you to specify filter expressions, projection expressions, parallel scan segments,
         * pagination settings, and other advanced parameters. If no table name is specified in the
         * request, the mapper's configured table name is used.</p>
         *
         * <p><b>Advanced Scan Features:</b></p>
         * <ul>
         * <li>Filter expressions for complex filtering logic</li>
         * <li>Projection expressions for attribute selection</li>
         * <li>Parallel scan with segment and total segments parameters</li>
         * <li>Consistent read configuration</li>
         * <li>Return consumed capacity for monitoring</li>
         * <li>Index name for scanning secondary indexes</li>
         * </ul>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * ScanRequest request = new ScanRequest()
         *     .withLimit(100)
         *     .withFilterExpression("age > :min AND #status = :active")
         *     .withExpressionAttributeNames(Map.of("#status", "status"))
         *     .withExpressionAttributeValues(Map.of(
         *         ":min", new AttributeValue().withN("18"),
         *         ":active", new AttributeValue("ACTIVE")
         *     ))
         *     .withProjectionExpression("id, name, email")
         *     .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
         *
         * try (Stream<User> stream = mapper.scan(request)) {
         *     stream.forEach(System.out::println);
         * }
         * }</pre>
         *
         * @param scanRequest the ScanRequest containing scan parameters, must not be {@code null}
         * @return Stream of entities matching the scan parameters with lazy evaluation and automatic pagination
         * @throws IllegalArgumentException if the request specifies a different table than configured, or if scanRequest is {@code null}
         * @see #scan(List, Map) for simpler scan API
         */
        public Stream<T> scan(final ScanRequest scanRequest) {
            return dynamoDBExecutor.scan(checkScanRequest(scanRequest), targetEntityClass);
        }

        private Map<String, AttributeValue> createKey(final T entity) {
            final Map<String, AttributeValue> key = new HashMap<>(keyPropNames.size());

            for (int i = 0, len = keyPropNames.size(); i < len; i++) {
                key.put(keyPropNames.get(i), attrValueOf(keyPropInfos.get(i).getPropValue(entity)));
            }

            return key;
        }

        private Map<String, KeysAndAttributes> createKeys(final Collection<? extends T> entities) {
            final List<Map<String, AttributeValue>> keys = new ArrayList<>(entities.size());

            for (final T entity : entities) {
                keys.add(createKey(entity));
            }

            return N.asMap(tableName, new KeysAndAttributes().withKeys(keys));
        }

        private Map<String, List<WriteRequest>> createBatchPutRequest(final Collection<? extends T> entities) {
            final List<WriteRequest> keys = new ArrayList<>(entities.size());

            for (final T entity : entities) {
                keys.add(new WriteRequest().withPutRequest(new PutRequest().withItem(toItem(entity))));
            }

            return N.asMap(tableName, keys);
        }

        private Map<String, List<WriteRequest>> createBatchDeleteRequest(final Collection<? extends T> entities) {
            final List<WriteRequest> keys = new ArrayList<>(entities.size());

            for (final T entity : entities) {
                keys.add(new WriteRequest().withDeleteRequest(new DeleteRequest().withKey(createKey(entity))));
            }

            return N.asMap(tableName, keys);
        }

        private GetItemRequest checkItem(final GetItemRequest item) {
            if (Strings.isEmpty(item.getTableName())) {
                item.setTableName(tableName);
            } else {
                checkTableName(item.getTableName());
            }

            return item;
        }

        private BatchGetItemRequest checkItem(final BatchGetItemRequest item) {
            if (item.getRequestItems() != null) {
                for (final String tableNameInRequest : item.getRequestItems().keySet()) {
                    checkTableName(tableNameInRequest);
                }
            }

            return item;

        }

        private BatchWriteItemRequest checkItem(final BatchWriteItemRequest item) {
            if (item.getRequestItems() != null) {
                for (final String tableNameInRequest : item.getRequestItems().keySet()) {
                    checkTableName(tableNameInRequest);
                }
            }

            return item;
        }

        private PutItemRequest checkItem(final PutItemRequest item) {
            if (Strings.isEmpty(item.getTableName())) {
                item.setTableName(tableName);
            } else {
                checkTableName(item.getTableName());
            }

            return item;
        }

        private UpdateItemRequest checkItem(final UpdateItemRequest item) {
            if (Strings.isEmpty(item.getTableName())) {
                item.setTableName(tableName);
            } else {
                checkTableName(item.getTableName());
            }

            return item;
        }

        private DeleteItemRequest checkItem(final DeleteItemRequest item) {
            if (Strings.isEmpty(item.getTableName())) {
                item.setTableName(tableName);
            } else {
                checkTableName(item.getTableName());
            }

            return item;
        }

        private QueryRequest checkQueryRequest(final QueryRequest queryRequest) {
            if (Strings.isEmpty(queryRequest.getTableName())) {
                queryRequest.setTableName(tableName);
            } else {
                checkTableName(queryRequest.getTableName());
            }

            return queryRequest;
        }

        private ScanRequest checkScanRequest(final ScanRequest scanRequest) {
            if (Strings.isEmpty(scanRequest.getTableName())) {
                scanRequest.setTableName(tableName);
            } else {
                checkTableName(scanRequest.getTableName());
            }

            return scanRequest;
        }

        private void checkTableName(final String tableNameInRequest) {
            if (!tableName.equals(tableNameInRequest)) {
                throw new IllegalArgumentException("Table name mismatch: request has '" + tableNameInRequest + "' but mapper expects '" + tableName + "'");
            }
        }
    }

    /**
     * Utility class providing factory methods for creating DynamoDB filter conditions.
     * These conditions are used in scan and query operations to filter results.
     * All methods return a Map that can be directly used with DynamoDB scan operations.
     */
    public static final class Filters {
        private Filters() {
            // singleton for Utility class
        }

        /**
         * Creates an equality condition for the specified attribute.
         * Matches items where the attribute value equals the provided value.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> filter = Filters.eq("status", "active");
         * // Matches all items where status = "active"
         * }</pre>
         *
         * @param attrName the name of the attribute to compare
         * @param attrValue the value to compare against
         * @return a Map containing the equality condition for use in DynamoDB operations
         */
        public static Map<String, Condition> eq(final String attrName, final Object attrValue) {
            return N.asMap(attrName, new Condition().withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(attrValueOf(attrValue)));
        }

        /**
         * Creates a not-equal condition for the specified attribute.
         * Matches items where the attribute value does not equal the provided value.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> filter = Filters.ne("status", "deleted");
         * // Matches all items where status != "deleted"
         * }</pre>
         *
         * @param attrName the name of the attribute to compare
         * @param attrValue the value to compare against
         * @return a Map containing the not-equal condition for use in DynamoDB operations
         */
        public static Map<String, Condition> ne(final String attrName, final Object attrValue) {
            return N.asMap(attrName, new Condition().withComparisonOperator(ComparisonOperator.NE).withAttributeValueList(attrValueOf(attrValue)));
        }

        /**
         * Creates a greater-than condition for the specified attribute.
         * Matches items where the attribute value is greater than the provided value.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> filter = Filters.gt("age", 18);
         * // Matches all items where age > 18
         * }</pre>
         *
         * @param attrName the name of the attribute to compare
         * @param attrValue the value to compare against
         * @return a Map containing the greater-than condition for use in DynamoDB operations
         */
        public static Map<String, Condition> gt(final String attrName, final Object attrValue) {
            return N.asMap(attrName, new Condition().withComparisonOperator(ComparisonOperator.GT).withAttributeValueList(attrValueOf(attrValue)));
        }

        /**
         * Creates a greater-than-or-equal condition for the specified attribute.
         * Matches items where the attribute value is greater than or equal to the provided value.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> filter = Filters.ge("score", 60);
         * // Matches all items where score >= 60
         * }</pre>
         *
         * @param attrName the name of the attribute to compare
         * @param attrValue the value to compare against
         * @return a Map containing the greater-than-or-equal condition for use in DynamoDB operations
         */
        public static Map<String, Condition> ge(final String attrName, final Object attrValue) {
            return N.asMap(attrName, new Condition().withComparisonOperator(ComparisonOperator.GE).withAttributeValueList(attrValueOf(attrValue)));
        }

        /**
         * Creates a less-than condition for the specified attribute.
         * Matches items where the attribute value is less than the provided value.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> filter = Filters.lt("price", 100);
         * // Matches all items where price < 100
         * }</pre>
         *
         * @param attrName the name of the attribute to compare
         * @param attrValue the value to compare against
         * @return a Map containing the less-than condition for use in DynamoDB operations
         */
        public static Map<String, Condition> lt(final String attrName, final Object attrValue) {
            return N.asMap(attrName, new Condition().withComparisonOperator(ComparisonOperator.LT).withAttributeValueList(attrValueOf(attrValue)));
        }

        /**
         * Creates a less-than-or-equal condition for the specified attribute.
         * Matches items where the attribute value is less than or equal to the provided value.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> filter = Filters.le("quantity", 10);
         * // Matches all items where quantity <= 10
         * }</pre>
         *
         * @param attrName the name of the attribute to compare
         * @param attrValue the value to compare against
         * @return a Map containing the less-than-or-equal condition for use in DynamoDB operations
         */
        public static Map<String, Condition> le(final String attrName, final Object attrValue) {
            return N.asMap(attrName, new Condition().withComparisonOperator(ComparisonOperator.LE).withAttributeValueList(attrValueOf(attrValue)));
        }

        /**
         * Creates a between condition for the specified attribute.
         * Matches items where the attribute value is between the two provided values (inclusive).
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> filter = Filters.bt("age", 18, 65);
         * // Matches all items where age >= 18 AND age <= 65
         * }</pre>
         *
         * @param attrName the name of the attribute to compare
         * @param minAttrValue the minimum value (inclusive)
         * @param maxAttrValue the maximum value (inclusive)
         * @return a Map containing the between condition for use in DynamoDB operations
         */
        public static Map<String, Condition> bt(final String attrName, final Object minAttrValue, final Object maxAttrValue) {
            return N.asMap(attrName, new Condition().withComparisonOperator(ComparisonOperator.BETWEEN)
                    .withAttributeValueList(attrValueOf(minAttrValue), attrValueOf(maxAttrValue)));
        }

        /**
         * Creates a null condition for the specified attribute.
         * Matches items where the attribute does not exist or has a null value.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> filter = Filters.isNull("deletedAt");
         * // Matches all items where deletedAt attribute is null or doesn't exist
         * }</pre>
         *
         * @param attrName the name of the attribute to check
         * @return a Map containing the null condition for use in DynamoDB operations
         */
        public static Map<String, Condition> isNull(final String attrName) {
            return N.asMap(attrName, new Condition().withComparisonOperator(ComparisonOperator.NULL));
        }

        /**
         * Creates a not-null condition for the specified attribute.
         * Matches items where the attribute exists and has a non-null value.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> filter = Filters.notNull("email");
         * // Matches all items where email attribute exists and is not null
         * }</pre>
         *
         * @param attrName the name of the attribute to check
         * @return a Map containing the not-null condition for use in DynamoDB operations
         */
        public static Map<String, Condition> notNull(final String attrName) {
            return N.asMap(attrName, new Condition().withComparisonOperator(ComparisonOperator.NOT_NULL));
        }

        /**
         * Creates a contains condition for the specified attribute.
         * For strings, matches items where the attribute value contains the specified substring.
         * For sets, matches items where the set contains the specified value.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> filter = Filters.contains("tags", "important");
         * // Matches items where tags contains "important"
         * }</pre>
         *
         * @param attrName the name of the attribute to check
         * @param attrValue the value or substring to search for
         * @return a Map containing the contains condition for use in DynamoDB operations
         */
        public static Map<String, Condition> contains(final String attrName, final Object attrValue) {
            return N.asMap(attrName, new Condition().withComparisonOperator(ComparisonOperator.CONTAINS).withAttributeValueList(attrValueOf(attrValue)));
        }

        /**
         * Creates a not-contains condition for the specified attribute.
         * For strings, matches items where the attribute value does not contain the specified substring.
         * For sets, matches items where the set does not contain the specified value.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> filter = Filters.notContains("description", "deprecated");
         * // Matches items where description doesn't contain "deprecated"
         * }</pre>
         *
         * @param attrName the name of the attribute to check
         * @param attrValue the value or substring to search for
         * @return a Map containing the not-contains condition for use in DynamoDB operations
         */
        public static Map<String, Condition> notContains(final String attrName, final Object attrValue) {
            return N.asMap(attrName, new Condition().withComparisonOperator(ComparisonOperator.NOT_CONTAINS).withAttributeValueList(attrValueOf(attrValue)));
        }

        /**
         * Creates a begins-with condition for the specified attribute.
         * Matches items where the string attribute value begins with the specified prefix.
         * Only applicable to String attributes.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> filter = Filters.beginsWith("email", "admin@");
         * // Matches all items where email starts with "admin@"
         * }</pre>
         *
         * @param attrName the name of the attribute to check
         * @param attrValue the prefix to match
         * @return a Map containing the begins-with condition for use in DynamoDB operations
         */
        public static Map<String, Condition> beginsWith(final String attrName, final Object attrValue) {
            return N.asMap(attrName, new Condition().withComparisonOperator(ComparisonOperator.BEGINS_WITH).withAttributeValueList(attrValueOf(attrValue)));
        }

        /**
         * Creates an IN condition for the specified attribute using varargs.
         * Matches items where the attribute value equals any of the provided values.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> filter = Filters.in("status", "active", "pending", "processing");
         * // Matches items where status is "active", "pending", or "processing"
         * }</pre>
         *
         * @param attrName the name of the attribute to check
         * @param attrValues variable number of values to match against
         * @return a Map containing the IN condition for use in DynamoDB operations
         */
        public static Map<String, Condition> in(final String attrName, final Object... attrValues) {
            final Map<String, Condition> result = new HashMap<>(1);

            in(result, attrName, attrValues);

            return result;
        }

        /**
         * Creates an IN condition for the specified attribute using a Collection.
         * Matches items where the attribute value equals any value in the collection.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> statuses = Arrays.asList("active", "pending", "processing");
         * Map<String, Condition> filter = Filters.in("status", statuses);
         * // Matches items where status is in the list
         * }</pre>
         *
         * @param attrName the name of the attribute to check
         * @param attrValues collection of values to match against
         * @return a Map containing the IN condition for use in DynamoDB operations
         */
        public static Map<String, Condition> in(final String attrName, final Collection<?> attrValues) {
            final Map<String, Condition> result = new HashMap<>(1);

            in(result, attrName, attrValues);

            return result;
        }

        static void in(final Map<String, Condition> output, final String attrName, final Object... attrValues) {
            final AttributeValue[] attributeValueList = new AttributeValue[attrValues.length];

            for (int i = 0, len = attrValues.length; i < len; i++) {
                attributeValueList[i] = attrValueOf(attrValues[i]);
            }

            final Condition cond = new Condition().withComparisonOperator(ComparisonOperator.IN).withAttributeValueList(attributeValueList);

            output.put(attrName, cond);
        }

        static void in(final Map<String, Condition> output, final String attrName, final Collection<?> attrValues) {
            final AttributeValue[] attributeValueList = new AttributeValue[attrValues.size()];

            int i = 0;
            for (final Object attrValue : attrValues) {
                attributeValueList[i++] = attrValueOf(attrValue);
            }

            final Condition cond = new Condition().withComparisonOperator(ComparisonOperator.IN).withAttributeValueList(attributeValueList);

            output.put(attrName, cond);
        }

        /**
         * Creates a new ConditionBuilder for constructing complex filter conditions.
         * The builder allows chaining multiple conditions together for a single scan or query operation.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> filters = Filters.builder()
         *     .eq("status", "active")
         *     .gt("age", 18)
         *     .notNull("email")
         *     .build();
         * }</pre>
         *
         * @return a new ConditionBuilder instance
         */
        public static ConditionBuilder builder() {
            return new ConditionBuilder();
        }
    }

    /**
     * A fluent builder class for constructing complex DynamoDB filter conditions.
     * Allows chaining multiple condition methods to create a composite filter map
     * for use in scan and query operations.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, Condition> filters = ConditionBuilder.create()
     *     .eq("status", "active")
     *     .gt("age", 18)
     *     .le("price", 100)
     *     .build();
     * }</pre>
     */
    public static final class ConditionBuilder {
        private Map<String, Condition> condMap;

        ConditionBuilder() {
            condMap = new HashMap<>();
        }

        /**
         * Creates a new ConditionBuilder instance.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> filters = ConditionBuilder.create().eq("status", "ACTIVE").build();
         * }</pre>
         *
         * @return a new ConditionBuilder instance
         * @deprecated Use {@link Filters#builder()} instead.
         */
        @Deprecated
        public static ConditionBuilder create() {
            return new ConditionBuilder();
        }

        /**
         * Adds an equality condition for the specified attribute.
         * Matches items where the attribute value equals the provided value.
         *
         * @param attrName the name of the attribute to compare
         * @param attrValue the value to compare against
         * @return this builder for method chaining
         */
        public ConditionBuilder eq(final String attrName, final Object attrValue) {
            condMap.put(attrName, new Condition().withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(attrValueOf(attrValue)));

            return this;
        }

        /**
         * Adds a not-equal condition for the specified attribute.
         * Matches items where the attribute value does not equal the provided value.
         *
         * @param attrName the name of the attribute to compare
         * @param attrValue the value to compare against
         * @return this builder for method chaining
         */
        public ConditionBuilder ne(final String attrName, final Object attrValue) {
            condMap.put(attrName, new Condition().withComparisonOperator(ComparisonOperator.NE).withAttributeValueList(attrValueOf(attrValue)));

            return this;
        }

        /**
         * Adds a greater-than condition for the specified attribute.
         * Matches items where the attribute value is greater than the provided value.
         *
         * @param attrName the name of the attribute to compare
         * @param attrValue the value to compare against
         * @return this builder for method chaining
         */
        public ConditionBuilder gt(final String attrName, final Object attrValue) {
            condMap.put(attrName, new Condition().withComparisonOperator(ComparisonOperator.GT).withAttributeValueList(attrValueOf(attrValue)));

            return this;
        }

        /**
         * Adds a greater-than-or-equal condition for the specified attribute.
         * Matches items where the attribute value is greater than or equal to the provided value.
         *
         * @param attrName the name of the attribute to compare
         * @param attrValue the value to compare against
         * @return this builder for method chaining
         */
        public ConditionBuilder ge(final String attrName, final Object attrValue) {
            condMap.put(attrName, new Condition().withComparisonOperator(ComparisonOperator.GE).withAttributeValueList(attrValueOf(attrValue)));

            return this;
        }

        /**
         * Adds a less-than condition for the specified attribute.
         * Matches items where the attribute value is less than the provided value.
         *
         * @param attrName the name of the attribute to compare
         * @param attrValue the value to compare against
         * @return this builder for method chaining
         */
        public ConditionBuilder lt(final String attrName, final Object attrValue) {
            condMap.put(attrName, new Condition().withComparisonOperator(ComparisonOperator.LT).withAttributeValueList(attrValueOf(attrValue)));

            return this;
        }

        /**
         * Adds a less-than-or-equal condition for the specified attribute.
         * Matches items where the attribute value is less than or equal to the provided value.
         *
         * @param attrName the name of the attribute to compare
         * @param attrValue the value to compare against
         * @return this builder for method chaining
         */
        public ConditionBuilder le(final String attrName, final Object attrValue) {
            condMap.put(attrName, new Condition().withComparisonOperator(ComparisonOperator.LE).withAttributeValueList(attrValueOf(attrValue)));

            return this;
        }

        /**
         * Adds a between condition for the specified attribute.
         * Matches items where the attribute value is between the two provided values (inclusive).
         *
         * @param attrName the name of the attribute to compare
         * @param minAttrValue the minimum value (inclusive)
         * @param maxAttrValue the maximum value (inclusive)
         * @return this builder for method chaining
         */
        public ConditionBuilder bt(final String attrName, final Object minAttrValue, final Object maxAttrValue) {
            condMap.put(attrName, new Condition().withComparisonOperator(ComparisonOperator.BETWEEN)
                    .withAttributeValueList(attrValueOf(minAttrValue), attrValueOf(maxAttrValue)));

            return this;
        }

        /**
         * Adds a null condition for the specified attribute.
         * Matches items where the attribute does not exist or has a null value.
         *
         * @param attrName the name of the attribute to check
         * @return this builder for method chaining
         */
        public ConditionBuilder isNull(final String attrName) {
            condMap.put(attrName, new Condition().withComparisonOperator(ComparisonOperator.NULL));

            return this;
        }

        /**
         * Adds a not-null condition for the specified attribute.
         * Matches items where the attribute exists and has a non-null value.
         *
         * @param attrName the name of the attribute to check
         * @return this builder for method chaining
         */
        public ConditionBuilder notNull(final String attrName) {
            condMap.put(attrName, new Condition().withComparisonOperator(ComparisonOperator.NOT_NULL));

            return this;
        }

        /**
         * Adds a contains condition for the specified attribute.
         * For strings, matches items where the attribute value contains the specified substring.
         * For sets, matches items where the set contains the specified value.
         *
         * @param attrName the name of the attribute to check
         * @param attrValue the value or substring to search for
         * @return this builder for method chaining
         */
        public ConditionBuilder contains(final String attrName, final Object attrValue) {
            condMap.put(attrName, new Condition().withComparisonOperator(ComparisonOperator.CONTAINS).withAttributeValueList(attrValueOf(attrValue)));

            return this;
        }

        /**
         * Adds a not-contains condition for the specified attribute.
         * For strings, matches items where the attribute value does not contain the specified substring.
         * For sets, matches items where the set does not contain the specified value.
         *
         * @param attrName the name of the attribute to check
         * @param attrValue the value or substring to search for
         * @return this builder for method chaining
         */
        public ConditionBuilder notContains(final String attrName, final Object attrValue) {
            condMap.put(attrName, new Condition().withComparisonOperator(ComparisonOperator.NOT_CONTAINS).withAttributeValueList(attrValueOf(attrValue)));

            return this;
        }

        /**
         * Adds a begins-with condition for the specified attribute.
         * Matches items where the string attribute value begins with the specified prefix.
         * Only applicable to String attributes.
         *
         * @param attrName the name of the attribute to check
         * @param attrValue the prefix to match
         * @return this builder for method chaining
         */
        public ConditionBuilder beginsWith(final String attrName, final Object attrValue) {
            condMap.put(attrName, new Condition().withComparisonOperator(ComparisonOperator.BEGINS_WITH).withAttributeValueList(attrValueOf(attrValue)));

            return this;
        }

        /**
         * Adds an IN condition for the specified attribute using varargs.
         * Matches items where the attribute value equals any of the provided values.
         *
         * @param attrName the name of the attribute to check
         * @param attrValues variable number of values to match against
         * @return this builder for method chaining
         */
        public ConditionBuilder in(final String attrName, final Object... attrValues) {
            Filters.in(condMap, attrName, attrValues);

            return this;
        }

        /**
         * Adds an IN condition for the specified attribute using a Collection.
         * Matches items where the attribute value equals any value in the collection.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> validStatuses = Arrays.asList("active", "pending");
         * builder.in("status", validStatuses)
         *        .gt("createdAt", yesterday);
         * }</pre>
         *
         * @param attrName the name of the attribute to check
         * @param attrValues collection of values to match against
         * @return this builder for method chaining
         */
        public ConditionBuilder in(final String attrName, final Collection<?> attrValues) {
            Filters.in(condMap, attrName, attrValues);

            return this;
        }

        /**
         * Builds and returns the final condition map.
         * After calling this method, the builder instance should not be reused.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> filters = Filters.builder()
         *     .eq("status", "active")
         *     .gt("age", 18)
         *     .build();
         * // Use filters in scan or query operations
         * }</pre>
         *
         * @return a Map containing all the conditions added to this builder
         */
        public Map<String, Condition> build() {
            final Map<String, Condition> result = condMap;

            condMap = null;

            return result;
        }
    }
}
