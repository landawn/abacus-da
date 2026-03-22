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

package com.landawn.abacus.da.aws.dynamodb.v2;

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
import java.util.function.BiFunction;
import java.util.function.IntFunction;

import com.landawn.abacus.da.util.AnyUtil;
import com.landawn.abacus.parser.ParserUtil;
import com.landawn.abacus.parser.ParserUtil.BeanInfo;
import com.landawn.abacus.parser.ParserUtil.PropInfo;
import com.landawn.abacus.query.QueryUtil;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.util.Beans;
import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.Clazz;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.Fn;
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

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

/**
 * Synchronous DynamoDB executor for AWS SDK v2 providing comprehensive DynamoDB operations with modern API.
 *
 * <p>This executor serves as a high-level synchronous wrapper around AWS DynamoDB SDK v2, offering both
 * raw AttributeValue operations and automatic object mapping. Built on the modern AWS SDK v2 architecture,
 * it provides improved performance, better resource management, and enhanced type safety compared to v1 implementations.</p>
 *
 * <h2>Features and Architecture</h2>
 * <h3>AWS SDK v2 Benefits:</h3>
 * <ul>
 * <li><b>Improved Performance</b> - More efficient HTTP client and connection management</li>
 * <li><b>Better Resource Management</b> - Automatic resource cleanup and connection pooling</li>
 * <li><b>Enhanced Type Safety</b> - Builder patterns and immutable request/response objects</li>
 * <li><b>Modern API Design</b> - Fluent builders and optional value handling</li>
 * <li><b>Reduced Dependencies</b> - Smaller footprint with modular architecture</li>
 * </ul>
 *
 * <h3>Key Features:</h3>
 * <ul>
 * <li><b>Complete CRUD Operations</b> - Create, read, update, and delete with conditional operation support</li>
 * <li><b>Batch Operations</b> - Efficient batch get/write operations with automatic 25/100-item limit handling</li>
 * <li><b>Query &amp; Scan</b> - Flexible querying with GSI/LSI support, filtering, and automatic pagination</li>
 * <li><b>Object Mapping</b> - Seamless conversion between Java objects and DynamoDB AttributeValues</li>
 * <li><b>Stream Processing</b> - Memory-efficient streaming for large result sets with pagination support</li>
 * <li><b>Type-Safe Mappers</b> - Entity-specific mappers with compile-time type checking</li>
 * <li><b>Thread Safety</b> - Fully thread-safe with efficient concurrent access patterns</li>
 * </ul>
 *
 * <h3>DynamoDB Core Concepts:</h3>
 * <ul>
 * <li><b>Partition Key (Hash Key)</b> - Primary key component determining data distribution</li>
 * <li><b>Sort Key (Range Key)</b> - Optional secondary key component for composite keys</li>
 * <li><b>Global Secondary Index (GSI)</b> - Alternative access patterns with different keys</li>
 * <li><b>Local Secondary Index (LSI)</b> - Alternative sort key sharing same partition key</li>
 * <li><b>Eventually Consistent Reads</b> - Default read model with better performance</li>
 * <li><b>Strongly Consistent Reads</b> - Guaranteed latest data with higher cost</li>
 * <li><b>Conditional Operations</b> - Atomic operations with condition expressions</li>
 * </ul>
 *
 * <h3>Performance Characteristics:</h3>
 * <ul>
 * <li>Batch operations: Up to 25 items for writes, 100 items for reads</li>
 * <li>Query operations: More efficient than Scan for targeted retrieval</li>
 * <li>Scan operations: Full table traversal with filtering capabilities</li>
 * <li>Projection expressions: Retrieve only required attributes for efficiency</li>
 * <li>Pagination: Automatic handling of large result sets</li>
 * </ul>
 *
 * <h3>Thread Safety &amp; Concurrency:</h3>
 * <p>This executor is fully thread-safe and optimized for concurrent access. The underlying DynamoDbClient
 * uses connection pooling and is designed for high-throughput scenarios with multiple threads.</p>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Initialize executor with v2 client
 * DynamoDbClient client = DynamoDbClient.builder()
 *     .region(Region.US_EAST_1)
 *     .build();
 * DynamoDBExecutor executor = new DynamoDBExecutor(client);
 *
 * // Entity-based operations
 * User user = new User("123", "John Doe", 30);
 * executor.mapper(User.class).putItem(user);
 * User retrieved = executor.mapper(User.class).getItem(user);
 *
 * // Raw operations with builders
 * GetItemRequest request = GetItemRequest.builder()
 *     .tableName("Users")
 *     .key(asKey("userId", "123"))
 *     .consistentRead(true)
 *     .build();
 * User result = executor.getItem(request, User.class);
 *
 * // Query with expression builders
 * QueryRequest query = QueryRequest.builder()
 *     .tableName("Users")
 *     .keyConditionExpression("userId = :userId")
 *     .expressionAttributeValues(Map.of(
 *         ":userId", AttributeValue.fromS("123")
 *     ))
 *     .build();
 * List<User> users = executor.list(query, User.class);
 * }</pre>
 *
 * <h3>Migration from v1:</h3>
 * <p>This executor maintains API compatibility with v1 while leveraging v2 SDK improvements.
 * Key differences include builder-based request construction and enhanced type safety.</p>
 *
 * @see <a href="https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/dynamodb/DynamoDbClient.html">DynamoDbClient</a>
 * @see <a href="https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/">DynamoDB Developer Guide</a>
 * @see <a href="https://aws.amazon.com/blogs/developer/aws-sdk-for-java-2-x-released/">AWS SDK v2 Release Notes</a>
 */
@SuppressWarnings("java:S1192")
public final class DynamoDBExecutor implements AutoCloseable {

    static {
        final BiFunction<AttributeValue, Class<?>, Object> converter = DynamoDBExecutor::toValue;

        N.registerConverter(AttributeValue.class, converter);
    }

    private final DynamoDbClient dynamoDBClient;

    /**
     * Constructs a new DynamoDBExecutor with the specified AWS SDK v2 DynamoDB client.
     *
     * <p>The executor will use the provided client for all DynamoDB operations, inheriting
     * its configuration including region, credentials, retry policies, endpoint configuration,
     * and HTTP client settings. The client should be properly configured before passing
     * to this constructor.</p>
     *
     * <p><b>Client Configuration Best Practices:</b></p>
     * <ul>
     * <li>Configure appropriate AWS credentials (IAM roles, profiles, or explicit credentials)</li>
     * <li>Set the correct AWS region for your DynamoDB tables</li>
     * <li>Configure retry policy and timeout settings for your use case</li>
     * <li>Optimize HTTP client settings for expected load (connection pool size, timeouts)</li>
     * <li>Enable SDK metrics and logging for monitoring and debugging</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DynamoDbClient client = DynamoDbClient.builder()
     *     .region(Region.US_EAST_1)
     *     .credentialsProvider(DefaultCredentialsProvider.create())
     *     .overrideConfiguration(ClientOverrideConfiguration.builder()
     *         .retryPolicy(RetryPolicy.builder()
     *             .numRetries(3)
     *             .build())
     *         .build())
     *     .build();
     * DynamoDBExecutor executor = new DynamoDBExecutor(client);
     * }</pre>
     *
     * @param dynamoDBClient the AWS SDK v2 DynamoDB client to use for operations. Must not be null.
     * @throws IllegalArgumentException if dynamoDBClient is null
     */
    public DynamoDBExecutor(final DynamoDbClient dynamoDBClient) {
        if (dynamoDBClient == null) {
            throw new IllegalArgumentException("dynamoDBClient cannot be null");
        }
        this.dynamoDBClient = dynamoDBClient;
    }

    /**
     * Returns the underlying DynamoDB client used by this executor.
     *
     * <p>This provides direct access to the AWS SDK v2 DynamoDbClient for operations not covered
     * by this executor or for advanced configuration. The v2 client offers improved performance
     * and additional features compared to the v1 client.</p>
     *
     * <p><b>Direct Client Capabilities:</b></p>
     * <ul>
     * <li>Table management operations (create, delete, describe, update)</li>
     * <li>Global Secondary Index management</li>
     * <li>Stream operations and change data capture</li>
     * <li>Advanced pagination with response iterators</li>
     * <li>Waiter utilities for table state changes</li>
     * <li>Enhanced error handling with specific exception types</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DynamoDbClient client = executor.dynamoDBClient();
     *
     * // Table management
     * ListTablesResponse tables = client.listTables();
     *
     * // Wait for table to become active
     * client.waiter().waitUntilTableExists(DescribeTableRequest.builder()
     *     .tableName("MyTable")
     *     .build());
     *
     * // Advanced pagination
     * ScanIterable scanResults = client.scanPaginator(ScanRequest.builder()
     *     .tableName("MyTable")
     *     .build());
     * }</pre>
     *
     * @return the DynamoDbClient instance used by this executor, never null
     * @see DynamoDbClient
     */
    public DynamoDbClient dynamoDBClient() {
        return dynamoDBClient;
    }

    @SuppressWarnings("rawtypes")
    private final Map<Class<?>, Mapper> mapperPool = new ConcurrentHashMap<>();

    /**
     * Creates a type-safe mapper for the specified entity class with automatic table name detection.
     *
     * <p>This method creates a cached mapper that provides type-safe operations for a specific
     * entity class using AWS SDK v2. The table name is automatically derived from @Table annotations
     * on the class, and the mapper uses CAMEL_CASE naming policy by default for attribute
     * name conversion.</p>
     *
     * <p><b>Entity Class Requirements:</b></p>
     * <ul>
     * <li>Must be annotated with @Table, @javax.persistence.Table, or @jakarta.persistence.Table</li>
     * <li>Must have getter/setter methods for all properties</li>
     * <li>Must have appropriate @Id annotations for primary key fields</li>
     * <li>Should have a default constructor</li>
     * </ul>
     *
     * <p><b>Mapper Features with AWS SDK v2:</b></p>
     * <ul>
     * <li>Type-safe CRUD operations with compile-time checking</li>
     * <li>Automatic conversion between Java objects and DynamoDB items</li>
     * <li>Enhanced performance with v2 SDK optimizations</li>
     * <li>Immutable request objects for better thread safety</li>
     * <li>Built-in support for batch operations</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * @Table("Users")
     * public class User {
     *     @Id
     *     private String userId;
     *     private String name;
     *     private String email;
     *     // getters and setters...
     * }
     *
     * DynamoDBExecutor.Mapper<User> userMapper = executor.mapper(User.class);
     *
     * // Type-safe operations
     * User newUser = new User();
     * newUser.setUserId("user123");
     * newUser.setName("John Doe");
     *
     * userMapper.putItem(newUser);
     * User retrieved = userMapper.getItem(newUser);
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetEntityClass the entity class to create mapper for. Must be annotated with @Table. Must not be null.
     * @return a cached Mapper instance for the specified entity class, never null
     * @throws IllegalArgumentException if targetEntityClass is null, not a bean class, or missing @Table annotation
     * @see #mapper(Class, String, NamingPolicy)
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
     * <p>This method creates a mapper with full customization of table name and attribute naming policy
     * using AWS SDK v2. Unlike the single-parameter version, this doesn't require @Table annotations and
     * allows complete control over table mapping. Each call creates a new mapper instance (not cached).</p>
     *
     * <p><b>Naming Policy Options:</b></p>
     * <ul>
     * <li><b>CAMEL_CASE</b> - "userName" → "userName" (DynamoDB standard)</li>
     * <li><b>UPPER_CAMEL_CASE</b> - "userName" → "UserName"</li>
     * <li><b>SNAKE_CASE</b> - "userName" → "user_name"</li>
     * <li><b>SCREAMING_SNAKE_CASE</b> - "userName" → "USER_NAME"</li>
     * </ul>
     *
     * <p><b>Use Cases:</b></p>
     * <ul>
     * <li>Working with existing tables without modifying entity annotations</li>
     * <li>Supporting multiple table naming conventions</li>
     * <li>Dynamic table name generation (e.g., with prefixes or suffixes)</li>
     * <li>Legacy system integration with specific naming requirements</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * public class Product {
     *     private String productId;
     *     private String productName;
     *     private BigDecimal price;
     *     // getters and setters...
     * }
     *
     * // Map to table with underscore convention
     * DynamoDBExecutor.Mapper<Product> mapper = executor.mapper(
     *     Product.class, 
     *     "product_catalog", 
     *     NamingPolicy.SNAKE_CASE
     * );
     *
     * // productName becomes "product_name" in DynamoDB
     * Product product = new Product();
     * product.setProductId("PROD-123");
     * product.setProductName("Widget");
     *
     * mapper.putItem(product);
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetEntityClass the entity class to create mapper for. Must be a valid bean class. Must not be null.
     * @param tableName the DynamoDB table name to use for operations. Must not be null or empty.
     * @param namingPolicy the naming policy for converting property names to attribute names. Must not be null.
     * @return a new Mapper instance configured with the specified parameters, never null
     * @throws IllegalArgumentException if any parameter is null, targetEntityClass is not a bean class, or tableName is empty
     * @see NamingPolicy
     * @see #mapper(Class)
     */
    public <T> Mapper<T> mapper(final Class<T> targetEntityClass, final String tableName, final NamingPolicy namingPolicy) {
        return new Mapper<>(targetEntityClass, this, tableName, namingPolicy);
    }

    /**
     * Converts a Java object to a DynamoDB AttributeValue using AWS SDK v2 with automatic type detection.
     *
     * <p>This method performs intelligent type mapping to convert Java objects into appropriate DynamoDB
     * AttributeValue instances using AWS SDK v2's builder patterns. The conversion rules are:</p>
     *
     * <ul>
     * <li><b>null</b> → AttributeValue with NULL=true using fromNul(true)</li>
     * <li><b>Number types</b> (Integer, Long, Double, BigDecimal, etc.) → N (Number) using fromN()</li>
     * <li><b>Boolean</b> → BOOL using fromBool()</li>
     * <li><b>byte[]</b> → B (Binary) using fromB() with SdkBytes</li>
     * <li><b>ByteBuffer</b> → B (Binary) using fromB() with SdkBytes</li>
     * <li><b>All other types</b> → S (String) using fromS() with string conversion</li>
     * </ul>
     *
     * <p><b>SDK v2 Improvements:</b></p>
     * <ul>
     * <li>Immutable AttributeValue objects for better thread safety</li>
     * <li>Type-safe factory methods (fromS, fromN, fromBool, etc.)</li>
     * <li>Enhanced binary data handling with SdkBytes</li>
     * <li>Better null value representation</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AttributeValue stringAttr = attrValueOf("Hello World");       // S: "Hello World"
     * AttributeValue numberAttr = attrValueOf(42);                  // N: "42"
     * AttributeValue boolAttr = attrValueOf(true);                  // BOOL: true
     * AttributeValue nullAttr = attrValueOf(null);                  // NULL: true
     * AttributeValue binaryAttr = attrValueOf("data".getBytes());   // B: binary data
     * }</pre>
     *
     * <p><b>Important:</b> This method does NOT handle complex types like Lists, Maps, or Sets.
     * For complex AttributeValue creation, use AWS SDK v2's AttributeValue builder methods directly.</p>
     *
     * @param value the Java object to convert, can be null
     * @return an AttributeValue representing the input value with appropriate type mapping, never null
     */
    public static AttributeValue attrValueOf(final Object value) {
        if (value == null) {
            return AttributeValue.fromNul(true);
        } else {
            final Type<Object> type = N.typeOf(value.getClass());

            if (type.isNumber()) {
                return AttributeValue.fromN(type.stringOf(value));
            } else if (type.isBoolean()) {
                return AttributeValue.fromBool((Boolean) value);
            } else if (value instanceof byte[]) {
                return AttributeValue.fromB(SdkBytes.fromByteArray((byte[]) value));
            } else if (type.isByteBuffer()) {
                return AttributeValue.fromB(SdkBytes.fromByteBuffer((ByteBuffer) value));
            } else {
                return AttributeValue.fromS(type.stringOf(value));
            }
        }
    }

    /**
     * Creates an AttributeValueUpdate with PUT action for the specified value using AWS SDK v2.
     *
     * <p>This convenience method creates an AttributeValueUpdate using the default PUT action,
     * which replaces the existing attribute value with the new value. The input value is automatically
     * converted to an AttributeValue using SDK v2's type-safe conversion methods.</p>
     *
     * <p>This is equivalent to calling {@code attrValueUpdateOf(value, AttributeAction.PUT)}.</p>
     *
     * <p><b>SDK v2 Benefits:</b></p>
     * <ul>
     * <li>Immutable AttributeValueUpdate objects for thread safety</li>
     * <li>Builder patterns for complex update operations</li>
     * <li>Enhanced type safety with enum-based actions</li>
     * <li>Better integration with expression-based updates</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValueUpdate> updates = new HashMap<>();
     * updates.put("name", attrValueUpdateOf("John Doe"));                      // PUT action
     * updates.put("age", attrValueUpdateOf(30));                               // PUT action
     * updates.put("lastLogin", attrValueUpdateOf(Instant.now().toString()));   // PUT action
     *
     * UpdateItemRequest request = UpdateItemRequest.builder()
     *     .tableName("Users")
     *     .key(asKey("userId", "123"))
     *     .attributeUpdates(updates)
     *     .build();
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
     * Creates an AttributeValueUpdate with the specified action and value using AWS SDK v2.
     *
     * <p>This method provides full control over AttributeValueUpdate creation by allowing specification
     * of both the value and the update action using AWS SDK v2's builder patterns. The method supports
     * all DynamoDB update actions with enhanced type safety and immutable objects.</p>
     *
     * <p><b>Available Actions:</b></p>
     * <ul>
     * <li><b>PUT</b> - Replace the attribute value completely (default behavior)</li>
     * <li><b>ADD</b> - Add to numeric values, or add elements to sets</li>
     * <li><b>DELETE</b> - Remove the attribute entirely, or remove elements from sets</li>
     * </ul>
     *
     * <p><b>SDK v2 Improvements:</b></p>
     * <ul>
     * <li>Immutable AttributeValueUpdate objects for better thread safety</li>
     * <li>Builder patterns for complex update operations</li>
     * <li>Enhanced type safety with enum-based actions</li>
     * <li>Better integration with expression-based updates</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Replace attribute value
     * AttributeValueUpdate put = attrValueUpdateOf("updated name", AttributeAction.PUT);
     *
     * // Increment a numeric counter
     * AttributeValueUpdate increment = attrValueUpdateOf(1, AttributeAction.ADD);
     *
     * // Decrement a numeric value
     * AttributeValueUpdate decrement = attrValueUpdateOf(-5, AttributeAction.ADD);
     *
     * // Delete an attribute entirely
     * AttributeValueUpdate delete = attrValueUpdateOf(null, AttributeAction.DELETE);
     *
     * // Use in UpdateItem operation
     * Map<String, AttributeValueUpdate> updates = new HashMap<>();
     * updates.put("loginCount", increment);
     * updates.put("lastLogin", attrValueUpdateOf(Instant.now().toString()));
     *
     * UpdateItemRequest request = UpdateItemRequest.builder()
     *     .tableName("Users")
     *     .key(asKey("userId", "user123"))
     *     .attributeUpdates(updates)
     *     .build();
     * }</pre>
     *
     * @param value the value for the update operation, can be null for DELETE actions
     * @param action the update action to perform using AWS SDK v2 AttributeAction enum. Must not be null.
     * @return an AttributeValueUpdate with the specified action and converted value, never null
     * @throws IllegalArgumentException if action is null
     * @see #attrValueOf(Object)
     * @see #attrValueUpdateOf(Object)
     */
    public static AttributeValueUpdate attrValueUpdateOf(final Object value, final AttributeAction action) {
        return AttributeValueUpdate.builder().value(attrValueOf(value)).action(action).build();
    }

    /**
     * Creates a single-attribute key map for DynamoDB operations using AWS SDK v2.
     *
     * <p>This convenience method creates a key map with a single partition key, commonly used
     * for simple primary keys in DynamoDB tables. The value is automatically converted to
     * an AttributeValue using AWS SDK v2's enhanced type system.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> key = asKey("userId", "user123");
     * // Results in: {"userId": AttributeValue.fromS("user123")}
     *
     * // Use with GetItem operation
     * GetItemRequest request = GetItemRequest.builder()
     *     .tableName("Users")
     *     .key(key)
     *     .build();
     * }</pre>
     *
     * @param keyName the name of the key attribute (usually partition key). Must not be null.
     * @param value the value for the key attribute, automatically converted to AttributeValue
     * @return a Map containing the single key-value pair as AttributeValue, never null
     * @throws IllegalArgumentException if keyName is null
     * @see #asKey(String, Object, String, Object)
     * @see #asItem(String, Object)
     */
    public static Map<String, AttributeValue> asKey(final String keyName, final Object value) {
        return asItem(keyName, value);
    }

    /**
     * Creates a composite key map with partition key and sort key for DynamoDB operations using AWS SDK v2.
     *
     * <p>This convenience method creates a key map with both partition key and sort key,
     * commonly used for composite primary keys in DynamoDB tables. Both values are
     * automatically converted to AttributeValues using AWS SDK v2's type-safe conversion.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> key = asKey(
     *     "userId", "user123", 
     *     "timestamp", 1640995200L
     * );
     * // Results in: {
     * //   "userId": AttributeValue.fromS("user123"),
     * //   "timestamp": AttributeValue.fromN("1640995200")
     * // }
     *
     * // Use with DeleteItem operation
     * DeleteItemRequest request = DeleteItemRequest.builder()
     *     .tableName("UserEvents")
     *     .key(key)
     *     .build();
     * }</pre>
     *
     * @param keyName the name of the partition key attribute. Must not be null.
     * @param value the value for the partition key, automatically converted
     * @param keyName2 the name of the sort key attribute. Must not be null.
     * @param value2 the value for the sort key, automatically converted
     * @return a Map containing both key-value pairs as AttributeValues, never null
     * @throws IllegalArgumentException if keyName or keyName2 is null
     * @see #asKey(String, Object)
     * @see #asItem(String, Object, String, Object)
     */
    public static Map<String, AttributeValue> asKey(final String keyName, final Object value, final String keyName2, final Object value2) {
        return asItem(keyName, value, keyName2, value2);
    }

    /**
     * Creates a composite key map with partition key, sort key, and additional attribute for DynamoDB operations using AWS SDK v2.
     *
     * <p>This convenience method creates a key map with three attributes, commonly used for
     * more complex primary keys in DynamoDB tables. All values are automatically converted
     * to AttributeValues using AWS SDK v2's type-safe conversion.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> key = asKey(
     *     "userId", "user123",
     *     "timestamp", 1640995200L,
     *     "eventType", "login"
     * );
     * // Results in: {
     * //   "userId": AttributeValue.fromS("user123"),
     * //   "timestamp": AttributeValue.fromN("1640995200"),
     * //   "eventType": AttributeValue.fromS("login")
     * // }
     *
     * // Use with UpdateItem operation
     * UpdateItemRequest request = UpdateItemRequest.builder()
     *     .tableName("UserEvents")
     *     .key(key)
     *     .build();
     * }</pre>
     *
     * @param keyName the name of the partition key attribute. Must not be null.
     * @param value the value for the partition key, automatically converted
     * @param keyName2 the name of the sort key attribute. Must not be null.
     * @param value2 the value for the sort key, automatically converted
     * @param keyName3 the name of an additional attribute. Must not be null.
     * @param value3 the value for the additional attribute, automatically converted
     * @return a Map containing all three key-value pairs as AttributeValues, never null
     * @throws IllegalArgumentException if any keyName is null
     */
    public static Map<String, AttributeValue> asKey(final String keyName, final Object value, final String keyName2, final Object value2, final String keyName3,
            final Object value3) {
        return asItem(keyName, value, keyName2, value2, keyName3, value3);
    }

    /**
     * Creates a key map for DynamoDB operations using AWS SDK v2 with an array of alternating attribute names and values.
     *
     * <p>This method allows creating a key map from an array of alternating attribute names and values,
     * which can be useful for dynamic key generation. The values are automatically converted to
     * AttributeValues using AWS SDK v2's type-safe conversion.</p>
     *
     * <p><b>Note:</b> This convenience method may be misused for non-key attributes.
     * Use {@link #asItem(Object...)} when the values represent a full item payload.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> key = asKey(
     *     "userId", "user123",
     *     "timestamp", 1640995200L
     * );
     * // Results in: {
     * //   "userId": AttributeValue.fromS("user123"),
     * //   "timestamp": AttributeValue.fromN("1640995200")
     * // }
     *
     * // Use with GetItem operation
     * GetItemRequest request = GetItemRequest.builder()
     *     .tableName("Users")
     *     .key(key)
     *     .build();
     * }</pre>
     *
     * @param a an array of alternating attribute names and values, must not be {@code null}
     * @return a Map containing the key-value pairs as AttributeValues, never null
     * @throws NullPointerException if {@code a} is {@code null}
     * @throws IllegalArgumentException if the array length is odd
     */
    public static Map<String, AttributeValue> asKey(final Object... a) {
        return asItem(a);
    }

    /**
     * Creates a single-attribute item map for DynamoDB operations using AWS SDK v2.
     *
     * <p>This convenience method creates a Map with a single attribute, commonly used for
     * simple items in DynamoDB tables. The value is automatically converted to an AttributeValue
     * using AWS SDK v2's type-safe conversion.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> item = asItem("name", "John Doe");
     * // Results in: {"name": AttributeValue.fromS("John Doe")}
     *
     * // Use with PutItem operation
     * PutItemRequest request = PutItemRequest.builder()
     *     .tableName("Users")
     *     .item(item)
     *     .build();
     * }</pre>
     *
     * @param attrName the name of the attribute. Must not be null.
     * @param value the value for the attribute, automatically converted to AttributeValue
     * @return a Map containing the single attribute as AttributeValue, never null
     * @throws IllegalArgumentException if attrName is null
     */
    public static Map<String, AttributeValue> asItem(final String attrName, final Object value) {
        return N.asMap(attrName, attrValueOf(value));
    }

    /**
     * Creates an item with two attributes for DynamoDB operations using AWS SDK v2.
     *
     * <p>This method allows creating a map with two attributes, where each attribute
     * name is paired with its corresponding value. The values are automatically converted
     * to DynamoDB `AttributeValue` objects using the `attrValueOf` method.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> item = asItem("name", "John Doe", "age", 30);
     * // Results in: {"name": AttributeValue.fromS("John Doe"), "age": AttributeValue.fromN("30")}
     * }</pre>
     *
     * @param attrName  the name of the first attribute. Must not be null.
     * @param value     the value of the first attribute, automatically converted to `AttributeValue`.
     * @param attrName2 the name of the second attribute. Must not be null.
     * @param value2    the value of the second attribute, automatically converted to `AttributeValue`.
     * @return an item containing the two attributes as `AttributeValue` objects. Never null.
     * @throws IllegalArgumentException if any attribute name is null.
     */
    public static Map<String, AttributeValue> asItem(final String attrName, final Object value, final String attrName2, final Object value2) {
        return N.asMap(attrName, attrValueOf(value), attrName2, attrValueOf(value2));
    }

    /**
     * Creates an item with three attributes for DynamoDB operations using AWS SDK v2.
     *
     * <p>This method allows creating a map with three attributes, where each attribute
     * name is paired with its corresponding value. The values are automatically converted
     * to DynamoDB `AttributeValue` objects using the `attrValueOf` method.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> item = asItem("name", "John Doe", "age", 30, "city", "New York");
     * // Results in: {"name": AttributeValue.fromS("John Doe"), "age": AttributeValue.fromN("30"), "city": AttributeValue.fromS("New York")}
     * }</pre>
     *
     * @param attrName  the name of the first attribute. Must not be null.
     * @param value     the value of the first attribute, automatically converted to `AttributeValue`.
     * @param attrName2 the name of the second attribute. Must not be null.
     * @param value2    the value of the second attribute, automatically converted to `AttributeValue`.
     * @param attrName3 the name of the third attribute. Must not be null.
     * @param value3    the value of the third attribute, automatically converted to `AttributeValue`.
     * @return an item containing the three attributes as `AttributeValue` objects. Never null.
     * @throws IllegalArgumentException if any attribute name is null.
     */
    public static Map<String, AttributeValue> asItem(final String attrName, final Object value, final String attrName2, final Object value2,
            final String attrName3, final Object value3) {
        return N.asMap(attrName, attrValueOf(value), attrName2, attrValueOf(value2), attrName3, attrValueOf(value3));
    }

    /**
     * Creates an item with an arbitrary number of attributes for DynamoDB operations using AWS SDK v2.
     *
     * <p>This method allows creating a map with an arbitrary number of attributes, where each attribute
     * name is paired with its corresponding value. The values are automatically converted to DynamoDB
     * `AttributeValue` objects using the `attrValueOf` method.</p>
     *
     * <p><b>Note:</b> This convenience method may be confused with entity conversion.
     * Use {@link #toItem(Object)} or {@link #toItem(Object, NamingPolicy)} for POJO/Map conversion.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> item = asItem("name", "John Doe", "age", 30, "city", "New York");
     * // Results in: {"name": AttributeValue.fromS("John Doe"), "age": AttributeValue.fromN("30"), "city": AttributeValue.fromS("New York")}
     * }</pre>
     *
     * @param a an array of alternating attribute names and values, must not be {@code null}
     * @return an item containing the attributes as {@code AttributeValue} objects, never null
     * @throws NullPointerException if {@code a} is {@code null}
     * @throws IllegalArgumentException if the array length is odd
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
     * Creates an AttributeValueUpdate for a single attribute with PUT action for DynamoDB operations using AWS SDK v2.
     *
     * <p>This method creates a Map with a single AttributeValueUpdate, which is commonly used
     * for updating a single attribute in DynamoDB items. The value is automatically converted
     * to an AttributeValueUpdate using the `attrValueUpdateOf` method.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValueUpdate> update = asUpdateItem("name", "John Doe");
     * // Results in: {"name": AttributeValueUpdate.builder().value(AttributeValue.fromS("John Doe")).action(AttributeAction.PUT).build()}
     *
     * // Use with UpdateItem operation
     * UpdateItemRequest request = UpdateItemRequest.builder()
     *     .tableName("Users")
     *     .key(asKey("userId", "user123"))
     *     .attributeUpdates(update)
     *     .build();
     * }</pre>
     *
     * @param attrName the name of the attribute to update. Must not be null.
     * @param value the value for the attribute, automatically converted to AttributeValueUpdate
     * @return a Map containing the single AttributeValueUpdate, never null
     * @throws IllegalArgumentException if attrName is null
     */
    public static Map<String, AttributeValueUpdate> asUpdateItem(final String attrName, final Object value) {
        return N.asMap(attrName, attrValueUpdateOf(value));
    }

    /**
     * Creates an update item with two attributes for DynamoDB operations using AWS SDK v2.
     *
     * <p>This method allows creating a map with two attributes, where each attribute
     * name is paired with its corresponding AttributeValueUpdate. The values are automatically converted
     * to AttributeValueUpdates using the `attrValueUpdateOf` method.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValueUpdate> update = asUpdateItem("name", "John Doe", "age", 30);
     * // Results in: {"name": AttributeValueUpdate.builder().value(AttributeValue.fromS("John Doe")).action(AttributeAction.PUT).build(),
     * //              "age": AttributeValueUpdate.builder().value(AttributeValue.fromN("30")).action(AttributeAction.PUT).build()}
     * }</pre>
     *
     * @param attrName  the name of the first attribute to update. Must not be null.
     * @param value     the value of the first attribute, automatically converted to `AttributeValueUpdate`.
     * @param attrName2 the name of the second attribute to update. Must not be null.
     * @param value2    the value of the second attribute, automatically converted to `AttributeValueUpdate`.
     * @return an item containing the two attributes as `AttributeValueUpdate` objects. Never null.
     * @throws IllegalArgumentException if any attribute name is null.
     */
    public static Map<String, AttributeValueUpdate> asUpdateItem(final String attrName, final Object value, final String attrName2, final Object value2) {
        return N.asMap(attrName, attrValueUpdateOf(value), attrName2, attrValueUpdateOf(value2));
    }

    /**
     * Creates an update item with three attributes for DynamoDB operations using AWS SDK v2.
     *
     * <p>This method allows creating a map with three attributes, where each attribute
     * name is paired with its corresponding AttributeValueUpdate. The values are automatically converted
     * to AttributeValueUpdates using the `attrValueUpdateOf` method.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValueUpdate> update = asUpdateItem("name", "John Doe", "age", 30, "city", "New York");
     * // Results in: {"name": AttributeValueUpdate.builder().value(AttributeValue.fromS("John Doe")).action(AttributeAction.PUT).build(),
     * //              "age": AttributeValueUpdate.builder().value(AttributeValue.fromN("30")).action(AttributeAction.PUT).build(),
     * //              "city": AttributeValueUpdate.builder().value(AttributeValue.fromS("New York")).action(AttributeAction.PUT).build()}
     * }</pre>
     *
     * @param attrName  the name of the first attribute to update. Must not be null.
     * @param value     the value of the first attribute, automatically converted to `AttributeValueUpdate`.
     * @param attrName2 the name of the second attribute to update. Must not be null.
     * @param value2    the value of the second attribute, automatically converted to `AttributeValueUpdate`.
     * @param attrName3 the name of the third attribute to update. Must not be null.
     * @param value3    the value of the third attribute, automatically converted to `AttributeValueUpdate`.
     * @return an item containing the three attributes as `AttributeValueUpdate` objects. Never null.
     * @throws IllegalArgumentException if any attribute name is null.
     */
    public static Map<String, AttributeValueUpdate> asUpdateItem(final String attrName, final Object value, final String attrName2, final Object value2,
            final String attrName3, final Object value3) {
        return N.asMap(attrName, attrValueUpdateOf(value), attrName2, attrValueUpdateOf(value2), attrName3, attrValueUpdateOf(value3));
    }

    /**
     * Creates an update item with an arbitrary number of attributes for DynamoDB operations using AWS SDK v2.
     *
     * <p>This method allows creating a map with an arbitrary number of attributes, where each attribute
     * name is paired with its corresponding AttributeValueUpdate. The values are automatically converted
     * to AttributeValueUpdates using the `attrValueUpdateOf` method.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValueUpdate> update = asUpdateItem("name", "John Doe", "age", 30, "city", "New York");
     * // Results in: {"name": AttributeValueUpdate.builder().value(AttributeValue.fromS("John Doe")).action(AttributeAction.PUT).build(),
     * //              "age": AttributeValueUpdate.builder().value(AttributeValue.fromN("30")).action(AttributeAction.PUT).build(),
     * //              "city": AttributeValueUpdate.builder().value(AttributeValue.fromS("New York")).action(AttributeAction.PUT).build()}
     * }</pre>
     *
     * <p><b>Note:</b> This convenience method may be confused with entity conversion.
     * Use {@link #toUpdateItem(Object)} or {@link #toUpdateItem(Object, NamingPolicy)} for POJO/Map conversion.</p>
     *
     * @param a an array of alternating attribute names and values, must not be {@code null}
     * @return an item containing the attributes as {@code AttributeValueUpdate} objects, never null
     * @throws NullPointerException if {@code a} is {@code null}
     * @throws IllegalArgumentException if the array length is odd
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
     * Converts a Java object to a DynamoDB item Map using AWS SDK v2 with automatic type detection.
     *
     * <p>This method converts a Java object (Entity or Map) into a DynamoDB item Map, where each property
     * is represented as an AttributeValue. The conversion respects the naming policy for attribute names.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> item = toItem(myEntity);
     * }</pre>
     *
     * @param entity the Java object to convert, can be null
     * @return a Map representing the DynamoDB item, never null
     */
    public static Map<String, AttributeValue> toItem(final Object entity) {
        return toItem(entity, NamingPolicy.CAMEL_CASE);
    }

    /**
     * Converts a Java object to a DynamoDB item Map using AWS SDK v2 with specified naming policy.
     *
     * <p>This method converts a Java object (Entity or Map) into a DynamoDB item Map, where each property
     * is represented as an AttributeValue. The conversion respects the specified naming policy for attribute names.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> item = toItem(myEntity, NamingPolicy.UPPER_CAMEL_CASE);
     * }</pre>
     *
     * @param entity the Java object to convert, must not be {@code null}
     * @param namingPolicy the naming policy to use for attribute names, must not be {@code null}
     * @return a Map representing the DynamoDB item, never null
     * @throws NullPointerException if {@code entity} or {@code namingPolicy} is {@code null}
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
            return toItem(AnyUtil.array2Props((Object[]) entity), namingPolicy);
        } else {
            throw new IllegalArgumentException("Unsupported type: " + ClassUtil.getCanonicalClassName(cls)
                    + ". Only Entity or Map<String, Object> classes with getter/setter methods are supported");
        }

        return attrs;
    }

    /**
     * Converts a Java object to a DynamoDB update item Map using AWS SDK v2 with automatic type detection.
     *
     * <p>This method converts a Java object (Entity or Map) into a DynamoDB update item Map, where each property
     * is represented as an AttributeValueUpdate. The conversion respects the default naming policy (lower camel case).</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValueUpdate> updateItem = toUpdateItem(myEntity);
     * }</pre>
     *
     * @param entity the Java object to convert, can be null
     * @return a Map representing the DynamoDB update item, never null
     */
    public static Map<String, AttributeValueUpdate> toUpdateItem(final Object entity) {
        return toUpdateItem(entity, NamingPolicy.CAMEL_CASE);
    }

    /**
     * Converts a Java object to a DynamoDB update item Map using AWS SDK v2 with specified naming policy.
     *
     * <p>This method converts a Java object (Entity or Map) into a DynamoDB update item Map, where each property
     * is represented as an AttributeValueUpdate. The conversion respects the specified naming policy for attribute names.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValueUpdate> updateItem = toUpdateItem(myEntity, NamingPolicy.UPPER_CAMEL_CASE);
     * }</pre>
     *
     * @param entity the Java object to convert, must not be {@code null}
     * @param namingPolicy the naming policy to use for attribute names, must not be {@code null}
     * @return a Map representing the DynamoDB update item, never null
     * @throws NullPointerException if {@code entity} or {@code namingPolicy} is {@code null}
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
            return toUpdateItem(AnyUtil.array2Props((Object[]) entity), namingPolicy);
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
     * Converts a DynamoDB item map to a standard Java map with default map supplier.
     *
     * @param item the DynamoDB item map with AttributeValue objects
     * @return a Map with converted Java objects, or null if item is null
     */
    public static Map<String, Object> toMap(final Map<String, AttributeValue> item) {
        return toMap(item, IntFunctions.ofMap());
    }

    /**
     * Converts a DynamoDB item map to a standard Java map with custom map supplier.
     *
     * @param item the DynamoDB item map with AttributeValue objects
     * @param mapSupplier function to create the target map instance
     * @return a Map with converted Java objects, or null if item is null
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
     * Converts a DynamoDB GetItemResponse to a Java entity of the specified class.
     *
     * <p>This method extracts the item from the GetItemResponse and converts it to an entity
     * using the specified target class. If the response does not contain an item, it returns null.</p>
     *
     * @param <T> the type of the entity to convert to
     * @param getItemResponse the GetItemResponse containing the item to convert
     * @param targetClass the class of the entity to convert to
     * @return an instance of the target class representing the item, or null if no item is present
     */
    public static <T> T toEntity(final GetItemResponse getItemResponse, final Class<T> targetClass) {
        if (getItemResponse == null || !getItemResponse.hasItem()) {
            return null;
        }

        return toEntity(getItemResponse.item(), targetClass);
    }

    /**
     * Converts a DynamoDB item Map to a Java entity of the specified class.
     *
     * <p>This method converts a Map representing a DynamoDB item into an entity of the specified class.
     * It uses reflection to set the properties of the entity based on the item attributes.</p>
     *
     * @param <T> the type of the entity to convert to
     * @param item the Map representing the DynamoDB item, can be null
     * @param targetClass the class of the entity to convert to
     * @return an instance of the target class representing the item, or null if the item is null
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

    /**
     * Reads a row from a DynamoDB GetItemResponse and converts it to an entity of the specified class.
     *
     * <p>This method extracts the item from the GetItemResponse and converts it to an entity
     * using the specified target class. If the response does not contain an item, it returns null.</p>
     *
     * @param getItemResponse the GetItemResponse containing the item to convert
     * @param rowClass the class of the entity to convert to
     * @return an instance of the target class representing the item, or null if no item is present
     */
    static <T> T readRow(final GetItemResponse getItemResponse, final Class<T> rowClass) {
        if (getItemResponse == null || !getItemResponse.hasItem()) {
            return rowClass == null ? null : N.defaultValueOf(rowClass);
        }

        return readRow(getItemResponse.item(), rowClass);
    }

    /**
     * Reads a row from a DynamoDB GetItemResponse and converts it to an entity of the specified class.
     *
     * <p>This method extracts the item from the GetItemResponse and converts it to an entity
     * using the specified target class. If the response does not contain an item, it returns null.</p>
     *
     * @param row the Map representing the DynamoDB item, can be null
     * @param rowClass the class of the entity to convert to
     * @return an instance of the target class representing the item, or null if no item is present
     */
    @SuppressWarnings("rawtypes")
    static <T> T readRow(final Map<String, AttributeValue> row, final Class<T> rowClass) {
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
    static <T> Function<Map<String, AttributeValue>, T> createRowMapper(final Class<T> rowClass) {
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

    /**
     * Converts an AttributeValue to a Java value of the specified type.
     *
     * <p>This method extracts the value from the AttributeValue and converts it to the specified target class.
     * If the AttributeValue is null or has a null value, it returns null or a default value for the target class.</p>
     *
     * @param x the AttributeValue to convert, can be null
     * @return the converted value, or null if x is null or has a null value
     */
    static <T> T toValue(final AttributeValue x) {
        return toValue(x, null);
    }

    /**
     * Converts an AttributeValue to a Java value of the specified type.
     *
     * <p>This method extracts the value from the AttributeValue and converts it to the specified target class.
     * If the AttributeValue is null or has a null value, it returns null or a default value for the target class.</p>
     *
     * @param x the AttributeValue to convert, can be null
     * @param targetClass the class of the target value, can be null
     * @return the converted value, or null if x is null or has a null value
     */
    static <T> T toValue(final AttributeValue x, final Class<T> targetClass) {
        if (x == null || N.isTrue(x.nul())) {
            return targetClass == null ? null : N.defaultValueOf(targetClass);
        }

        Object ret = null;

        if (Strings.isNotEmpty(x.s())) {
            ret = x.s();
        } else if (Strings.isNotEmpty(x.n())) {
            ret = x.n();
        } else if (x.bool() != null) {
            ret = x.bool();
        } else if (x.b() != null) {
            ret = x.b().asByteArray();
        } else if (x.hasSs()) {
            ret = x.ss();
        } else if (x.hasNs()) {
            ret = x.ns();
        } else if (x.hasBs()) {
            final List<SdkBytes> bs = x.bs();
            final List<byte[]> val = new ArrayList<>(bs.size());

            for (SdkBytes b : bs) {
                val.add(b.asByteArray());
            }

            ret = val;
        } else if (x.hasL()) {
            final List<AttributeValue> attrVals = x.l();
            final List<Object> val = new ArrayList<>(attrVals.size());

            for (final AttributeValue attrVal : attrVals) {
                val.add(toValue(attrVal));
            }

            ret = val;
        } else if (x.hasM()) {
            final Map<String, AttributeValue> attrMap = x.m();
            final Map<String, Object> val = N.newMap(attrMap.getClass(), attrMap.size());

            for (final Map.Entry<String, AttributeValue> entry : attrMap.entrySet()) {
                val.put(entry.getKey(), toValue(entry.getValue()));
            }

            ret = val;
        } else {
            throw new IllegalArgumentException("Unsupported Attribute type: " + x);
        }

        if (targetClass == null || ret == null || targetClass.isAssignableFrom(ret.getClass())) {
            return (T) ret;
        }

        return N.convert(ret, targetClass);
    }

    /**
     * Converts a BatchGetItemResponse to a Map of entity lists, where each key is the table name and the value is a list of entities.
     *
     * <p>This method extracts the responses from the BatchGetItemResponse and converts them to a Map of entity lists
     * using the specified target class. If the response does not contain any items, it returns an empty map.</p>
     *
     * @param batchGetItemResponse the BatchGetItemResponse containing the items to convert
     * @param targetClass the class of the entities to convert to
     * @return a Map where each key is the table name and the value is a list of entities, never null
     */
    static <T> Map<String, List<T>> toEntities(final BatchGetItemResponse batchGetItemResponse, final Class<T> targetClass) {
        if (batchGetItemResponse == null || !batchGetItemResponse.hasResponses()) {
            return new LinkedHashMap<>();
        }

        return toEntities(batchGetItemResponse.responses(), targetClass);
    }

    /**
     * Converts a Map of table items to a Map of entity lists, where each key is the table name and the value is a list of entities.
     *
     * <p>This method extracts the items from the provided map and converts them to a Map of entity lists
     * using the specified target class. If the map does not contain any items, it returns an empty map.</p>
     *
     * @param tableItems a Map where each key is the table name and the value is a list of items (Maps)
     * @param targetClass the class of the entities to convert to
     * @return a Map where each key is the table name and the value is a list of entities, never null
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
     * Converts a QueryResponse to a List of entities of the specified class.
     *
     * <p>This method extracts the items from the QueryResponse and converts them to a List of entities
     * using the specified target class. If the response does not contain any items, it returns an empty list.</p>
     *
     * @param <T> the type of the entities to convert to
     * @param queryResult the QueryResponse containing the items to convert
     * @param targetClass the class of the entities to convert to
     * @return a List of entities, never null
     */
    public static <T> List<T> toList(final QueryResponse queryResult, final Class<T> targetClass) {
        return toList(queryResult, 0, Integer.MAX_VALUE, targetClass);
    }

    /**
     * Converts a QueryResponse to a List of entities of the specified class with pagination support.
     *
     * <p>This method extracts the items from the QueryResponse and converts them to a List of entities
     * using the specified target class, starting from the given offset and limiting the number of items to count.
     * If the response does not contain any items, it returns an empty list.</p>
     *
     * @param <T> the type of the entities to convert to
     * @param queryResult the QueryResponse containing the items to convert
     * @param offset the starting index for pagination
     * @param count the maximum number of items to return
     * @param targetClass the class of the entities to convert to
     * @return a List of entities, never null
     */
    public static <T> List<T> toList(final QueryResponse queryResult, final int offset, final int count, final Class<T> targetClass) {
        if (queryResult == null || !queryResult.hasItems()) {
            return new ArrayList<>(0);
        }

        return toList(queryResult.items(), offset, count, targetClass);
    }

    /**
     * Converts a ScanResponse to a List of entities of the specified class.
     *
     * <p>This method extracts the items from the ScanResponse and converts them to a List of entities
     * using the specified target class. If the response does not contain any items, it returns an empty list.</p>
     *
     * @param <T> the type of the entities to convert to
     * @param scanResult the ScanResponse containing the items to convert
     * @param targetClass the class of the entities to convert to
     * @return a List of entities, never null
     */
    public static <T> List<T> toList(final ScanResponse scanResult, final Class<T> targetClass) {
        return toList(scanResult, 0, Integer.MAX_VALUE, targetClass);
    }

    /**
     * Converts a ScanResponse to a List of entities of the specified class with pagination support.
     *
     * <p>This method extracts the items from the ScanResponse and converts them to a List of entities
     * using the specified target class, starting from the given offset and limiting the number of items to count.
     * If the response does not contain any items, it returns an empty list.</p>
     *
     * @param <T> the type of the entities to convert to
     * @param scanResult the ScanResponse containing the items to convert
     * @param offset the starting index for pagination
     * @param count the maximum number of items to return
     * @param targetClass the class of the entities to convert to
     * @return a List of entities, never null
     */
    public static <T> List<T> toList(final ScanResponse scanResult, final int offset, final int count, final Class<T> targetClass) {
        if (scanResult == null || !scanResult.hasItems()) {
            return new ArrayList<>(0);
        }

        return toList(scanResult.items(), offset, count, targetClass);
    }

    /**
     * Converts a List of DynamoDB items (Maps) to a List of entities of the specified class.
     *
     * <p>This method extracts the items from the provided list and converts them to a List of entities
     * using the specified target class. If the list is empty, it returns an empty list.</p>
     *
     * @param items the List of DynamoDB items (Maps) to convert
     * @param targetClass the class of the entities to convert to
     * @return a List of entities, never null
     */
    static <T> List<T> toList(final List<Map<String, AttributeValue>> items, final Class<T> targetClass) {
        return toList(items, 0, Integer.MAX_VALUE, targetClass);
    }

    /**
     * Converts a List of DynamoDB items (Maps) to a List of entities of the specified class with pagination support.
     *
     * <p>This method extracts the items from the provided list and converts them to a List of entities
     * using the specified target class, starting from the given offset and limiting the number of items to count.
     * If the list is empty, it returns an empty list.</p>
     *
     * @param items the List of DynamoDB items (Maps) to convert
     * @param offset the starting index for pagination
     * @param count the maximum number of items to return
     * @param targetClass the class of the entities to convert to
     * @return a List of entities, never null
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
     * Extracts a Dataset from a QueryResponse.
     *
     * @param queryResult the QueryResponse containing the items to convert
     * @return a Dataset containing the extracted data, never null
     * @see #extractData(QueryResponse, int, int) for pagination support
     */
    public static Dataset extractData(final QueryResponse queryResult) {
        return extractData(queryResult, 0, Integer.MAX_VALUE);
    }

    /**
     * Extracts a Dataset from a QueryResponse with pagination support.
     *
     * <p>This method extracts the items from the QueryResponse and converts them to a Dataset,
     * starting from the given offset and limiting the number of items to count.</p>
     *
     * @param queryResult the QueryResponse containing the items to convert
     * @param offset the starting index for pagination
     * @param count the maximum number of items to return
     * @return a Dataset containing the extracted data, never null
     */
    public static Dataset extractData(final QueryResponse queryResult, final int offset, final int count) {
        return extractData(queryResult.items(), offset, count);
    }

    /**
     * Extracts a Dataset from a ScanResponse.
     *
     * @param scanResult the ScanResponse containing the items to convert
     * @return a Dataset containing the extracted data, never null
     * @see #extractData(ScanResponse, int, int) for pagination support
     */
    public static Dataset extractData(final ScanResponse scanResult) {
        return extractData(scanResult, 0, Integer.MAX_VALUE);
    }

    /**
     * Extracts a Dataset from a ScanResponse with pagination support.
     *
     * <p>This method extracts the items from the ScanResponse and converts them to a Dataset,
     * starting from the given offset and limiting the number of items to count.</p>
     *
     * @param scanResult the ScanResponse containing the items to convert
     * @param offset the starting index for pagination
     * @param count the maximum number of items to return
     * @return a Dataset containing the extracted data, never null
     */
    public static Dataset extractData(final ScanResponse scanResult, final int offset, final int count) {
        return extractData(scanResult.items(), offset, count);
    }

    /**
     * Extracts a Dataset from a List of DynamoDB items (Maps).
     *
     * <p>This method extracts the items from the provided list and converts them to a Dataset.
     * If the list is empty, it returns an empty Dataset.</p>
     *
     * @param items the List of DynamoDB items (Maps) to convert
     * @return a Dataset containing the extracted data, never null
     */
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

    static String getAttrName(final PropInfo propInfo, final NamingPolicy namingPolicy) {
        if (propInfo.columnName.isPresent()) {
            return propInfo.columnName.get();
        } else if (namingPolicy == NamingPolicy.CAMEL_CASE) {
            return propInfo.name;
        } else {
            return namingPolicy.convert(propInfo.name);
        }
    }

    /**
     * Retrieves an item from the specified DynamoDB table using AWS SDK v2.
     *
     * <p>This method performs an eventually consistent read by default and returns the item
     * as a Map of attribute names to Java objects. The method leverages AWS SDK v2's enhanced
     * performance and improved resource management for better efficiency.</p>
     *
     * <p><b>AWS SDK v2 Benefits:</b></p>
     * <ul>
     * <li>Improved performance with efficient HTTP client</li>
     * <li>Better resource management with automatic cleanup</li>
     * <li>Enhanced type safety with builder patterns</li>
     * <li>Reduced memory footprint</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> key = asKey("productId", "PROD-123");
     *
     * Map<String, Object> item = executor.getItem("Products", key);
     * if (item != null) {
     *     System.out.println("Product name: " + item.get("productName"));
     *     System.out.println("Price: $" + item.get("price"));
     * } else {
     *     System.out.println("Product not found");
     * }
     * }</pre>
     *
     * @param tableName the name of the DynamoDB table to retrieve the item from. Must not be null.
     * @param key the primary key of the item to retrieve, must include all key attributes. Must not be null.
     * @return the item as a Map of attribute names to values, or null if the item doesn't exist
     * @throws IllegalArgumentException if tableName or key is null
     * @see #getItem(String, Map, Boolean) for consistent read operations
     * @see #getItem(String, Map, Class) for type-safe retrieval
     */
    public Map<String, Object> getItem(final String tableName, final Map<String, AttributeValue> key) {
        return getItem(tableName, key, Clazz.PROPS_MAP);
    }

    /**
     * Retrieves an item from the specified DynamoDB table with configurable read consistency using AWS SDK v2.
     *
     * <p>This method performs a GetItem operation with the ability to specify read consistency level.
     * AWS SDK v2 provides enhanced performance and better resource management for this operation
     * compared to the v1 SDK.</p>
     *
     * <p><b>Read Consistency Trade-offs:</b></p>
     * <ul>
     * <li><b>Eventually Consistent (false/null):</b> Default behavior, better performance, lower cost,
     *     may not reflect most recent writes immediately</li>
     * <li><b>Strongly Consistent (true):</b> Guaranteed most recent data, higher latency,
     *     consumes more read capacity units</li>
     * </ul>
     *
     * <p><b>AWS SDK v2 Improvements:</b></p>
     * <ul>
     * <li>More efficient HTTP client with better connection pooling</li>
     * <li>Reduced memory allocation during request/response processing</li>
     * <li>Enhanced retry logic with exponential backoff</li>
     * <li>Better error handling with specific exception types</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> key = asKey("accountId", "ACC-12345");
     *
     * // Eventually consistent read (default)
     * Map<String, Object> item1 = executor.getItem("Accounts", key, false);
     *
     * // Strongly consistent read for critical data
     * Map<String, Object> item2 = executor.getItem("Accounts", key, true);
     *
     * if (item2 != null) {
     *     BigDecimal balance = (BigDecimal) item2.get("balance");
     *     System.out.println("Current balance: $" + balance);
     * }
     * }</pre>
     *
     * @param tableName the name of the DynamoDB table to retrieve from. Must not be null.
     * @param key the primary key of the item to retrieve, must include all key attributes. Must not be null.
     * @param consistentRead true for strongly consistent reads, false/null for eventually consistent reads
     * @return the item as a Map of attribute names to values, or null if the item doesn't exist
     * @throws IllegalArgumentException if tableName or key is null
     * @see #getItem(String, Map) for eventually consistent reads
     * @see #getItem(String, Map, Boolean, Class) for type-safe retrieval
     */
    public Map<String, Object> getItem(final String tableName, final Map<String, AttributeValue> key, final Boolean consistentRead) {
        return getItem(tableName, key, consistentRead, Clazz.PROPS_MAP);
    }

    /**
     * Retrieves an item using a fully configured GetItemRequest for maximum control.
     *
     * <p>This method provides complete flexibility by accepting a fully configured GetItemRequest,
     * allowing you to specify all available DynamoDB parameters including projection expressions,
     * expression attribute names, consistent read settings, and return consumed capacity options.</p>
     *
     * <p><b>Advanced Features Available:</b></p>
     * <ul>
     * <li>Projection expressions to retrieve only specific attributes</li>
     * <li>Expression attribute names for reserved word handling</li>
     * <li>Consistent read configuration per request</li>
     * <li>Return consumed capacity for monitoring read capacity usage</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * GetItemRequest request = GetItemRequest.builder()
     *     .tableName("Users")
     *     .key(Map.of("userId", AttributeValue.builder().s("user123").build()))
     *     .projectionExpression("userId, email, #name, createdAt")
     *     .expressionAttributeNames(Map.of("#name", "name"))  // 'name' is reserved
     *     .consistentRead(true)
     *     .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
     *     .build();
     *
     * Map<String, Object> item = executor.getItem(request);
     * }</pre>
     *
     * @param getItemRequest the complete request with all parameters. Must not be null.
     * @return the item as a Map of attribute names to values, or null if the item doesn't exist
     * @throws IllegalArgumentException if getItemRequest is null
     */
    public Map<String, Object> getItem(final GetItemRequest getItemRequest) {
        return getItem(getItemRequest, Clazz.PROPS_MAP);
    }

    /**
     * Retrieves an item from the specified DynamoDB table using AWS SDK v2 and converts it to a specific class.
     *
     * <p>This method performs an eventually consistent read by default and returns the item
     * as an instance of the specified target class. The method leverages AWS SDK v2's enhanced
     * performance and improved resource management for better efficiency.</p>
     *
     * @param <T> the type of the entity to convert to
     * @param tableName the name of the DynamoDB table to retrieve from. Must not be null.
     * @param key the primary key of the item to retrieve, must include all key attributes. Must not be null.
     * @param targetClass the class of the entity to convert to. Must not be null.
     * @return an instance of the target class representing the item, or null if the item doesn't exist
     * @throws IllegalArgumentException if tableName, key, or targetClass is null
     */
    public <T> T getItem(final String tableName, final Map<String, AttributeValue> key, final Class<T> targetClass) {
        final GetItemRequest getItemRequest = GetItemRequest.builder().tableName(tableName).key(key).build();

        return getItem(getItemRequest, targetClass);
    }

    /**
     * Retrieves an item from the specified DynamoDB table using AWS SDK v2 and converts it to a specific class.
     *
     * <p>This method performs a GetItem operation with configurable read consistency and converts the result
     * to an instance of the specified target class. It leverages AWS SDK v2's enhanced performance and
     * resource management for efficient operations.</p>
     *
     * <p><b>Read Consistency:</b></p>
     * <ul>
     * <li><b>Eventually Consistent (false/null):</b> Default behavior, better performance, lower cost,
     *     may not reflect most recent writes immediately.</li>
     * <li><b>Strongly Consistent (true):</b> Guaranteed most recent data, higher latency,
     *     consumes more read capacity units.</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> key = asKey("userId", "user123");
     * User user = executor.getItem("Users", key, true, User.class);
     * }</pre>
     *
     * @param <T> the type of the entity to convert to
     * @param tableName the name of the DynamoDB table to retrieve from. Must not be null.
     * @param key the primary key of the item to retrieve, must include all key attributes. Must not be null.
     * @param consistentRead true for strongly consistent reads, false/null for eventually consistent reads
     * @param targetClass the class of the entity to convert to. Must not be null.
     * @return an instance of the target class representing the item, or null if the item doesn't exist
     * @throws IllegalArgumentException if tableName, key, or targetClass is null
     */
    public <T> T getItem(final String tableName, final Map<String, AttributeValue> key, final Boolean consistentRead, final Class<T> targetClass) {
        final GetItemRequest getItemRequest = GetItemRequest.builder().tableName(tableName).key(key).consistentRead(consistentRead).build();

        return getItem(getItemRequest, targetClass);
    }

    /**
     * Retrieves an item using a GetItemRequest and converts it to the specified type.
     * This method combines the flexibility of a complete request with automatic type conversion.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * GetItemRequest request = GetItemRequest.builder()
     *     .tableName("Users")
     *     .key(asKey("userId", "user123"))
     *     .build();
     * User user = executor.getItem(request, User.class);
     * }</pre>
     *
     * @param <T> the type of the entity to convert to
     * @param getItemRequest the complete request with all parameters. Must not be null.
     * @param targetClass the class to convert the result to. Must not be null.
     * @return an instance of targetClass containing the item data, or null if the item doesn't exist
     * @throws IllegalArgumentException if any parameter is null or targetClass is unsupported
     */
    public <T> T getItem(final GetItemRequest getItemRequest, final Class<T> targetClass) {
        return readRow(dynamoDBClient.getItem(getItemRequest), targetClass);
    }

    /**
     * Performs a batch get operation to retrieve multiple items from multiple tables using AWS SDK v2.
     *
     * <p>This method can retrieve up to 100 items in a single call with a maximum total size of 16 MB.
     * AWS SDK v2 provides enhanced performance for batch operations with better connection management
     * and more efficient request processing.</p>
     *
     * <p><b>Batch Operation Benefits:</b></p>
     * <ul>
     * <li>Retrieves up to 100 items across multiple tables in one request</li>
     * <li>More efficient than individual GetItem calls</li>
     * <li>Automatic handling of unprocessed keys</li>
     * <li>Support for different consistency settings per table</li>
     * <li>Projection expressions for retrieving specific attributes only</li>
     * </ul>
     *
     * <p><b>AWS SDK v2 Enhancements:</b></p>
     * <ul>
     * <li>Better memory management for large batch requests</li>
     * <li>Improved connection pooling and reuse</li>
     * <li>Enhanced error handling for partial failures</li>
     * <li>More efficient JSON serialization/deserialization</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Batch get from multiple tables
     * KeysAndAttributes userKeys = KeysAndAttributes.builder()
     *     .keys(Arrays.asList(
     *         asKey("userId", "user1"),
     *         asKey("userId", "user2")
     *     ))
     *     .projectionExpression("userId, name, email")
     *     .consistentRead(false)
     *     .build();
     *
     * KeysAndAttributes orderKeys = KeysAndAttributes.builder()
     *     .keys(Arrays.asList(
     *         asKey("orderId", "order1"),
     *         asKey("orderId", "order2")
     *     ))
     *     .build();
     *
     * Map<String, KeysAndAttributes> requestItems = Map.of(
     *     "Users", userKeys,
     *     "Orders", orderKeys
     * );
     *
     * Map<String, List<Map<String, Object>>> results = 
     *     executor.batchGetItem(requestItems);
     *
     * List<Map<String, Object>> users = results.get("Users");
     * List<Map<String, Object>> orders = results.get("Orders");
     *
     * System.out.println("Retrieved " + users.size() + " users");
     * System.out.println("Retrieved " + orders.size() + " orders");
     * }</pre>
     *
     * @param requestItems a map where keys are table names and values are KeysAndAttributes
     *                    objects specifying the items to retrieve from each table. Must not be null.
     * @return a map of table names to lists of retrieved items, where each item is represented
     *         as a Map of attribute names to values
     * @throws IllegalArgumentException if requestItems is null or exceeds batch limits
     * @see KeysAndAttributes
     * @see #batchGetItem(Map, String) for capacity monitoring
     */
    public Map<String, List<Map<String, Object>>> batchGetItem(final Map<String, KeysAndAttributes> requestItems) {
        return batchGetItem(requestItems, Clazz.PROPS_MAP);
    }

    /**
     * Performs a batch get operation to retrieve multiple items from multiple tables using AWS SDK v2.
     *
     * <p>This method can retrieve up to 100 items in a single call with a maximum total size of 16 MB.
     * It allows specifying the return consumed capacity for monitoring purposes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, KeysAndAttributes> requestItems = Map.of(
     *     "Users", KeysAndAttributes.builder()
     *         .keys(List.of(asKey("userId", "user1"), asKey("userId", "user2")))
     *         .build()
     * );
     * Map<String, List<Map<String, Object>>> results =
     *     executor.batchGetItem(requestItems, "TOTAL");
     * }</pre>
     *
     * @param requestItems a map where keys are table names and values are KeysAndAttributes
     *                    objects specifying the items to retrieve from each table. Must not be null.
     * @param returnConsumedCapacity the level of consumed capacity to return. Can be "INDEXES", "TOTAL", or "NONE".
     * @return a map of table names to lists of retrieved items, where each item is represented
     *         as a Map of attribute names to values
     * @throws IllegalArgumentException if requestItems is null or exceeds batch limits
     */
    public Map<String, List<Map<String, Object>>> batchGetItem(final Map<String, KeysAndAttributes> requestItems, final String returnConsumedCapacity) {
        return batchGetItem(requestItems, returnConsumedCapacity, Clazz.PROPS_MAP);
    }

    /**
     * Performs a batch get operation to retrieve multiple items from multiple tables using AWS SDK v2.
     *
     * <p>This method can retrieve up to 100 items in a single call with a maximum total size of 16 MB.
     * It allows specifying the return consumed capacity for monitoring purposes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * BatchGetItemRequest batchGetItemRequest = BatchGetItemRequest.builder()
     *     .requestItems(requestItems)
     *     .returnConsumedCapacity("TOTAL")
     *     .build();
     *
     * Map<String, List<Map<String, Object>>> results =
     *     executor.batchGetItem(batchGetItemRequest);
     * }</pre>
     *
     * @param batchGetItemRequest the BatchGetItemRequest containing the request items and options. Must not be null.
     * @return a map of table names to lists of retrieved items, where each item is represented
     *         as a Map of attribute names to values
     * @throws IllegalArgumentException if batchGetItemRequest is null or exceeds batch limits
     */
    public Map<String, List<Map<String, Object>>> batchGetItem(final BatchGetItemRequest batchGetItemRequest) {
        return batchGetItem(batchGetItemRequest, Clazz.PROPS_MAP);
    }

    /**
     * Performs a batch get operation to retrieve multiple items from multiple tables using AWS SDK v2.
     *
     * <p>This method retrieves items from multiple tables in a single batch request. It converts the
     * retrieved items into a map where the keys are table names and the values are lists of entities
     * of the specified target class.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, KeysAndAttributes> requestItems = Map.of(
     *     "Users", KeysAndAttributes.builder()
     *         .keys(List.of(asKey("userId", "user1"), asKey("userId", "user2")))
     *         .build()
     * );
     * Map<String, List<User>> results = executor.batchGetItem(requestItems, User.class);
     * List<User> users = results.get("Users");
     * }</pre>
     *
     * @param <T> the type of the entities to convert to
     * @param requestItems a map where keys are table names and values are KeysAndAttributes objects
     *                     specifying the items to retrieve from each table. Must not be null.
     * @param targetClass the class of the entities to convert to. Must not be null.
     * @return a map where each key is a table name and the value is a list of entities of the specified
     *         target class. Never null.
     * @throws IllegalArgumentException if requestItems or targetClass is null
     */
    public <T> Map<String, List<T>> batchGetItem(final Map<String, KeysAndAttributes> requestItems, final Class<T> targetClass) {
        final BatchGetItemRequest batchGetItemRequest = BatchGetItemRequest.builder().requestItems(requestItems).build();

        return batchGetItem(batchGetItemRequest, targetClass);
    }

    /**
     * Performs a batch get operation to retrieve multiple items from multiple tables using AWS SDK v2.
     *
     * <p>This method retrieves items from multiple tables in a single batch request. It allows specifying
     * the return consumed capacity for monitoring purposes and converts the retrieved items into a map
     * where the keys are table names and the values are lists of entities of the specified target class.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, KeysAndAttributes> requestItems = Map.of(
     *     "Users", KeysAndAttributes.builder()
     *         .keys(List.of(asKey("userId", "user1"), asKey("userId", "user2")))
     *         .build()
     * );
     * Map<String, List<User>> results = executor.batchGetItem(requestItems, "TOTAL", User.class);
     * List<User> users = results.get("Users");
     * }</pre>
     *
     * @param <T> the type of the entities to convert to
     * @param requestItems a map where keys are table names and values are KeysAndAttributes objects
     *                     specifying the items to retrieve from each table. Must not be null.
     * @param returnConsumedCapacity the level of consumed capacity to return. Can be "INDEXES", "TOTAL", or "NONE".
     * @param targetClass the class of the entities to convert to. Must not be null.
     * @return a map where each key is a table name and the value is a list of entities of the specified
     *         target class. Never null.
     * @throws IllegalArgumentException if requestItems or targetClass is null
     */
    public <T> Map<String, List<T>> batchGetItem(final Map<String, KeysAndAttributes> requestItems, final String returnConsumedCapacity,
            final Class<T> targetClass) {
        final BatchGetItemRequest batchGetItemRequest = BatchGetItemRequest.builder()
                .requestItems(requestItems)
                .returnConsumedCapacity(returnConsumedCapacity)
                .build();

        return batchGetItem(batchGetItemRequest, targetClass);
    }

    /**
     * Executes a batch get item request and converts the results to entities of the specified type.
     *
     * <p>This method performs a batch get operation using the provided BatchGetItemRequest and converts
     * the retrieved items into a map where the keys are table names and the values are lists of entities
     * of the specified target class.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * BatchGetItemRequest request = BatchGetItemRequest.builder()
     *     .requestItems(Map.of(
     *         "Users", KeysAndAttributes.builder()
     *             .keys(List.of(asKey("userId", "user1"), asKey("userId", "user2")))
     *             .build()
     *     ))
     *     .build();
     * Map<String, List<User>> results = executor.batchGetItem(request, User.class);
     * List<User> users = results.get("Users");
     * }</pre>
     *
     * @param <T> the type of the entities to convert to
     * @param batchGetItemRequest the BatchGetItemRequest containing the request items and options. Must not be null.
     * @param targetClass the class of the entities to convert to. Must not be null.
     * @return a map where each key is a table name and the value is a list of entities of the specified
     *         target class. Never null.
     * @throws IllegalArgumentException if batchGetItemRequest or targetClass is null
     */
    public <T> Map<String, List<T>> batchGetItem(final BatchGetItemRequest batchGetItemRequest, final Class<T> targetClass) {
        return toEntities(dynamoDBClient.batchGetItem(batchGetItemRequest), targetClass);
    }

    /**
     * Inserts or replaces an item in the specified DynamoDB table using AWS SDK v2.
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
     * Map<String, AttributeValue> item = Map.of(
     *     "userId", AttributeValue.builder().s("user123").build(),
     *     "name", AttributeValue.builder().s("John Doe").build(),
     *     "email", AttributeValue.builder().s("john@example.com").build(),
     *     "createdAt", AttributeValue.builder().n(String.valueOf(System.currentTimeMillis())).build()
     * );
     *
     * PutItemResponse result = executor.putItem("Users", item);
     * System.out.println("Consumed capacity: " + result.consumedCapacity());
     * }</pre>
     *
     * @param tableName the name of the DynamoDB table. Must not be null or empty.
     * @param item the item to put, as a map of attribute names to AttributeValues. Must not be null.
     * @return a {@link PutItemResponse} containing operation metadata and consumed capacity
     * @throws IllegalArgumentException if tableName is null/empty or item is null
     * @see #putItem(String, Map, String) to retrieve the replaced item
     * @see #updateItem for partial updates
     */
    public PutItemResponse putItem(final String tableName, final Map<String, AttributeValue> item) {
        final PutItemRequest putItemRequest = PutItemRequest.builder().tableName(tableName).item(item).build();

        return putItem(putItemRequest);
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
     * Map<String, AttributeValue> item = Map.of(
     *     "userId", AttributeValue.builder().s("user123").build(),
     *     "name", AttributeValue.builder().s("Jane Doe").build(),
     *     "updatedAt", AttributeValue.builder().n(String.valueOf(Instant.now().toEpochMilli())).build()
     * );
     *
     * PutItemResponse result = executor.putItem("Users", item, "ALL_OLD");
     * Map<String, AttributeValue> oldItem = result.attributes();
     * if (!oldItem.isEmpty()) {
     *     System.out.println("Replaced item: " + oldItem);
     * }
     * }</pre>
     *
     * @param tableName the name of the DynamoDB table. Must not be null or empty.
     * @param item the item to put, as a map of attribute names to AttributeValues. Must not be null.
     * @param returnValues specifies what to return: "NONE" or "ALL_OLD"
     * @return a {@link PutItemResponse} containing operation metadata and optionally the replaced item
     * @throws IllegalArgumentException if tableName is null/empty or item is null
     * @see software.amazon.awssdk.services.dynamodb.model.ReturnValue for valid return value options
     */
    public PutItemResponse putItem(final String tableName, final Map<String, AttributeValue> item, final String returnValues) {
        final PutItemRequest putItemRequest = PutItemRequest.builder().tableName(tableName).item(item).returnValues(returnValues).build();

        return putItem(putItemRequest);
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
     * PutItemRequest request = PutItemRequest.builder()
     *     .tableName("Users")
     *     .item(Map.of(
     *         "userId", AttributeValue.builder().s("user123").build(),
     *         "name", AttributeValue.builder().s("John").build()
     *     ))
     *     .conditionExpression("attribute_not_exists(userId)") // Only if new
     *     .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
     *     .build();
     *
     * try {
     *     PutItemResponse result = executor.putItem(request);
     *     System.out.println("Item created successfully");
     * } catch (ConditionalCheckFailedException e) {
     *     System.out.println("Item already exists");
     * }
     * }</pre>
     *
     * @param putItemRequest the complete request with all parameters. Must not be null.
     * @return a {@link PutItemResponse} containing operation metadata and optional return values
     * @throws IllegalArgumentException if putItemRequest is null
     */
    public PutItemResponse putItem(final PutItemRequest putItemRequest) {
        return dynamoDBClient.putItem(putItemRequest);
    }

    /**
     * Puts an entity into the specified DynamoDB table.
     *
     * <p>This method converts the provided entity to a DynamoDB item and performs a PutItem operation.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MyEntity entity = new MyEntity("123", "John Doe");
     * PutItemResponse response = executor.putItem("MyTable", entity);
     * }</pre>
     *
     * @param tableName the name of the DynamoDB table to put the item into. Must not be null.
     * @param entity the entity to put, must be a class with getter/setter methods. Must not be null.
     * @return the response from the PutItem operation, containing metadata about the operation.
     * @throws IllegalArgumentException if tableName or entity is null.
     */
    // There is no too much benefit to add method for "Object entity"
    // And it may cause error because the "Object" is ambiguous to any type.
    PutItemResponse putItem(final String tableName, final Object entity) {
        PutItemRequest putItemRequest = PutItemRequest.builder().tableName(tableName).item(toItem(entity)).build();

        return putItem(putItemRequest);
    }

    /**
     * Puts an entity into the specified DynamoDB table with a specified return values option.
     *
     * <p>This method converts the provided entity to a DynamoDB item and performs a PutItem operation.
     * It allows specifying the return values option to control what is returned in the response.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MyEntity entity = new MyEntity("123", "John Doe");
     * PutItemResponse response = executor.putItem("MyTable", entity, "ALL_OLD");
     * }</pre>
     *
     * @param tableName the name of the DynamoDB table to put the item into. Must not be null.
     * @param entity the entity to put, must be a class with getter/setter methods. Must not be null.
     * @param returnValues specifies what values to return in the response. Can be "NONE", "ALL_OLD", "UPDATED_OLD", "ALL_NEW", or "UPDATED_NEW".
     * @return the response from the PutItem operation, containing metadata and optionally the old item.
     * @throws IllegalArgumentException if tableName or entity is null.
     */
    PutItemResponse putItem(final String tableName, final Object entity, final String returnValues) {
        PutItemRequest putItemRequest = PutItemRequest.builder().tableName(tableName).item(toItem(entity)).returnValues(returnValues).build();

        return putItem(putItemRequest);
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
     *     WriteRequest.builder()
     *         .putRequest(PutRequest.builder()
     *             .item(asItem("userId", "user1", "name", "Alice"))
     *             .build())
     *         .build(),
     *     WriteRequest.builder()
     *         .putRequest(PutRequest.builder()
     *             .item(asItem("userId", "user2", "name", "Bob"))
     *             .build())
     *         .build(),
     *     WriteRequest.builder()
     *         .deleteRequest(DeleteRequest.builder()
     *             .key(asKey("userId", "user3"))
     *             .build())
     *         .build()
     * );
     *
     * Map<String, List<WriteRequest>> requestItems = Map.of("Users", userWrites);
     *
     * BatchWriteItemResponse result = executor.batchWriteItem(requestItems);
     * if (!result.unprocessedItems().isEmpty()) {
     *     // Retry unprocessed items
     *     executor.batchWriteItem(result.unprocessedItems());
     * }
     * }</pre>
     *
     * @param requestItems map of table names to lists of write requests (puts/deletes). Must not be null.
     * @return a {@link BatchWriteItemResponse} containing unprocessed items and consumed capacity
     * @throws IllegalArgumentException if requestItems is null or exceeds batch limits
     * @see #batchWriteItem(BatchWriteItemRequest) for more control
     */
    public BatchWriteItemResponse batchWriteItem(final Map<String, List<WriteRequest>> requestItems) {
        final BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder().requestItems(requestItems).build();

        return dynamoDBClient.batchWriteItem(batchWriteItemRequest);
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
     * BatchWriteItemRequest request = BatchWriteItemRequest.builder()
     *     .requestItems(requestItems)
     *     .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
     *     .returnItemCollectionMetrics(ReturnItemCollectionMetrics.SIZE)
     *     .build();
     *
     * BatchWriteItemResponse result = executor.batchWriteItem(request);
     * System.out.println("Total consumed capacity: " + result.consumedCapacity());
     * }</pre>
     *
     * @param batchWriteItemRequest the complete batch write request. Must not be null.
     * @return a {@link BatchWriteItemResponse} with unprocessed items and optional metrics
     * @throws IllegalArgumentException if batchWriteItemRequest is null
     */
    public BatchWriteItemResponse batchWriteItem(final BatchWriteItemRequest batchWriteItemRequest) {
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
     * updates.put("loginCount", AttributeValueUpdate.builder()
     *     .action(AttributeAction.ADD)
     *     .value(attrValueOf(1))
     *     .build());
     * updates.put("tempToken", AttributeValueUpdate.builder()
     *     .action(AttributeAction.DELETE)
     *     .build());
     *
     * UpdateItemResponse result = executor.updateItem("Users", key, updates);
     * }</pre>
     *
     * @param tableName the name of the DynamoDB table. Must not be null or empty.
     * @param key the primary key identifying the item to update. Must not be null.
     * @param attributeUpdates map of attribute names to update actions. Must not be null.
     * @return an {@link UpdateItemResponse} containing operation metadata
     * @throws IllegalArgumentException if any parameter is null or tableName is empty
     * @see #updateItem(String, Map, Map, String) to retrieve updated values
     */
    public UpdateItemResponse updateItem(final String tableName, final Map<String, AttributeValue> key,
            final Map<String, AttributeValueUpdate> attributeUpdates) {
        final UpdateItemRequest updateItemRequest = UpdateItemRequest.builder().tableName(tableName).key(key).attributeUpdates(attributeUpdates).build();

        return updateItem(updateItemRequest);
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
     * UpdateItemResponse result = executor.updateItem("Users", key, updates, "ALL_NEW");
     * }</pre>
     *
     * @param tableName the name of the DynamoDB table. Must not be null or empty.
     * @param key the primary key identifying the item to update. Must not be null.
     * @param attributeUpdates map of attribute names to update actions. Must not be null.
     * @param returnValues specifies what to return (see method description)
     * @return an {@link UpdateItemResponse} containing operation metadata and optional values
     * @throws IllegalArgumentException if required parameters are null or invalid
     * @see software.amazon.awssdk.services.dynamodb.model.ReturnValue
     */
    public UpdateItemResponse updateItem(final String tableName, final Map<String, AttributeValue> key,
            final Map<String, AttributeValueUpdate> attributeUpdates, final String returnValues) {
        final UpdateItemRequest updateItemRequest = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .attributeUpdates(attributeUpdates)
                .returnValues(returnValues)
                .build();

        return updateItem(updateItemRequest);
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
     * UpdateItemRequest request = UpdateItemRequest.builder()
     *     .tableName("Users")
     *     .key(asKey("userId", "user123"))
     *     .updateExpression("SET #n = :name, #c = #c + :inc")
     *     .expressionAttributeNames(Map.of(
     *         "#n", "name",
     *         "#c", "counter"
     *     ))
     *     .expressionAttributeValues(Map.of(
     *         ":name", attrValueOf("New Name"),
     *         ":inc", attrValueOf(1),
     *         ":old", attrValueOf("Old Name")
     *     ))
     *     .conditionExpression("#n = :old")
     *     .returnValues(ReturnValue.ALL_NEW)
     *     .build();
     *
     * UpdateItemResponse result = executor.updateItem(request);
     * }</pre>
     *
     * @param updateItemRequest the complete update request. Must not be null.
     * @return an {@link UpdateItemResponse} containing operation results
     * @throws IllegalArgumentException if updateItemRequest is null
     */
    public UpdateItemResponse updateItem(final UpdateItemRequest updateItemRequest) {
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
     * DeleteItemResponse result = executor.deleteItem("Users", key);
     * System.out.println("Item deleted. Consumed capacity: " +
     *                    result.consumedCapacity());
     * }</pre>
     *
     * @param tableName the name of the DynamoDB table. Must not be null or empty.
     * @param key the primary key of the item to delete. Must not be null.
     * @return a {@link DeleteItemResponse} containing operation metadata
     * @throws IllegalArgumentException if tableName is null/empty or key is null
     * @see #deleteItem(String, Map, String) to retrieve the deleted item
     */
    public DeleteItemResponse deleteItem(final String tableName, final Map<String, AttributeValue> key) {
        DeleteItemRequest deleteItemRequest = DeleteItemRequest.builder().tableName(tableName).key(key).build();

        return deleteItem(deleteItemRequest);
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
     * DeleteItemResponse result = executor.deleteItem("Users", key, "ALL_OLD");
     * Map<String, AttributeValue> deletedItem = result.attributes();
     * if (!deletedItem.isEmpty()) {
     *     System.out.println("Deleted user: " + deletedItem.get("name"));
     *     // Could save to audit log or archive table
     * }
     * }</pre>
     *
     * @param tableName the name of the DynamoDB table. Must not be null or empty.
     * @param key the primary key of the item to delete. Must not be null.
     * @param returnValues "NONE" or "ALL_OLD" to get deleted item attributes
     * @return a {@link DeleteItemResponse} containing metadata and optionally the deleted item
     * @throws IllegalArgumentException if tableName is null/empty or key is null
     * @see software.amazon.awssdk.services.dynamodb.model.ReturnValue
     */
    public DeleteItemResponse deleteItem(final String tableName, final Map<String, AttributeValue> key, final String returnValues) {
        final DeleteItemRequest deleteItemRequest = DeleteItemRequest.builder().tableName(tableName).key(key).returnValues(returnValues).build();

        return deleteItem(deleteItemRequest);
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
     * DeleteItemRequest request = DeleteItemRequest.builder()
     *     .tableName("Users")
     *     .key(asKey("userId", "user123"))
     *     .conditionExpression("attribute_exists(userId) AND #s = :status")
     *     .expressionAttributeNames(Map.of("#s", "status"))
     *     .expressionAttributeValues(Map.of(":status", AttributeValue.builder().s("INACTIVE").build()))
     *     .returnValues("ALL_OLD")
     *     .build();
     *
     * try {
     *     DeleteItemResponse result = executor.deleteItem(request);
     *     System.out.println("Deleted inactive user: " + result.attributes());
     * } catch (ConditionalCheckFailedException e) {
     *     System.out.println("User not found or still active");
     * }
     * }</pre>
     *
     * @param deleteItemRequest the complete delete request. Must not be null.
     * @return a {@link DeleteItemResponse} containing operation results
     * @throws IllegalArgumentException if deleteItemRequest is null
     */
    public DeleteItemResponse deleteItem(final DeleteItemRequest deleteItemRequest) {
        return dynamoDBClient.deleteItem(deleteItemRequest);
    }

    /**
     * Lists items from the specified DynamoDB table using a QueryRequest.
     *
     * <p>This method performs a query operation using AWS SDK v2 and returns the results as a list of maps.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * QueryRequest queryRequest = QueryRequest.builder()
     *     .tableName("Users")
     *     .keyConditionExpression("userId = :v1")
     *     .expressionAttributeValues(Map.of(":v1", AttributeValue.builder().s("user123").build()))
     *     .build();
     * List<Map<String, Object>> results = executor.list(queryRequest);
     * }</pre>
     *
     * @param queryRequest the QueryRequest containing the table name and query parameters. Must not be null.
     * @return a list of maps representing the items retrieved by the query. Never null.
     * @throws IllegalArgumentException if queryRequest is null
     */
    public List<Map<String, Object>> list(final QueryRequest queryRequest) {
        return list(queryRequest, Clazz.PROPS_MAP);
    }

    /**
     * Lists items from the specified DynamoDB table using a QueryRequest and converts the results to entities of the specified type.
     *
     * <p>This method performs a query operation using AWS SDK v2 and returns the results as a list of entities
     * of the specified target class.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * QueryRequest queryRequest = QueryRequest.builder()
     *     .tableName("Users")
     *     .keyConditionExpression("userId = :v1")
     *     .expressionAttributeValues(Map.of(":v1", AttributeValue.builder().s("user123").build()))
     *     .build();
     * List<User> results = executor.list(queryRequest, User.class);
     * }</pre>
     *
     * @param <T> the type of the entities to convert to
     * @param queryRequest the QueryRequest containing the table name and query parameters. Must not be null.
     * @param targetClass the class of the entities to convert to. Must not be null.
     * @return a list of entities of the specified target class. Never null.
     * @throws IllegalArgumentException if queryRequest or targetClass is null
     */
    public <T> List<T> list(final QueryRequest queryRequest, final Class<T> targetClass) {
        final QueryResponse queryResult = dynamoDBClient.query(queryRequest);
        final List<T> res = toList(queryResult, targetClass);

        if (N.notEmpty(queryResult.lastEvaluatedKey()) && N.isEmpty(queryRequest.exclusiveStartKey())) {
            QueryRequest newQueryRequest = queryRequest.copy(builder -> builder.exclusiveStartKey(queryResult.lastEvaluatedKey()));
            QueryResponse newQueryResult = queryResult;

            do {
                final Map<String, AttributeValue> lastEvaluatedKey = newQueryResult.lastEvaluatedKey();
                newQueryRequest = queryRequest.copy(builder -> builder.exclusiveStartKey(lastEvaluatedKey));
                newQueryResult = dynamoDBClient.query(newQueryRequest);
                res.addAll(toList(newQueryResult, targetClass));
            } while (N.notEmpty(newQueryResult.lastEvaluatedKey()));
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
    //            if (queryResult != null && N.notEmpty(queryResult.lastEvaluatedKey())) {
    //                if (newQueryRequest == queryRequest) {
    //                    newQueryRequest = queryRequest.clone();
    //                }
    //
    //                newQueryRequest.setExclusiveStartKey(queryResult.lastEvaluatedKey());
    //            }
    //
    //            queryResult = dynamoDB.query(newQueryRequest);
    //        } while (pageOffset-- > 0 && N.notEmpty(queryResult.getItems()) && N.notEmpty(queryResult.lastEvaluatedKey()));
    //
    //        if (pageOffset >= 0 || pageCount-- <= 0) {
    //            return res;
    //        } else {
    //            res.addAll(toList(targetClass, queryResult));
    //        }
    //
    //        while (pageCount-- > 0 && N.notEmpty(queryResult.lastEvaluatedKey())) {
    //            if (newQueryRequest == queryRequest) {
    //                newQueryRequest = queryRequest.clone();
    //            }
    //
    //            newQueryRequest.setExclusiveStartKey(queryResult.lastEvaluatedKey());
    //            queryResult = dynamoDB.query(newQueryRequest);
    //            res.addAll(toList(targetClass, queryResult));
    //        }
    //
    //        return res;
    //    }

    /**
     * Queries items from the specified DynamoDB table using a QueryRequest and returns the results as a Dataset.
     *
     * <p>This method performs a query operation using AWS SDK v2 and returns the results as a Dataset.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * QueryRequest queryRequest = QueryRequest.builder()
     *     .tableName("Users")
     *     .keyConditionExpression("userId = :v1")
     *     .expressionAttributeValues(Map.of(":v1", AttributeValue.builder().s("user123").build()))
     *     .build();
     * Dataset results = executor.query(queryRequest);
     * }</pre>
     *
     * @param queryRequest the QueryRequest containing the table name and query parameters. Must not be null.
     * @return a Dataset containing the items retrieved by the query. Never null.
     * @throws IllegalArgumentException if queryRequest is null
     */
    public Dataset query(final QueryRequest queryRequest) {
        return query(queryRequest, Clazz.PROPS_MAP);
    }

    /**
     * Queries items from the specified DynamoDB table using a QueryRequest and converts the results to a Dataset.
     *
     * <p>This method performs a query operation using AWS SDK v2 and returns the results as a Dataset
     * containing entities of the specified target class.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * QueryRequest queryRequest = QueryRequest.builder()
     *     .tableName("Users")
     *     .keyConditionExpression("userId = :v1")
     *     .expressionAttributeValues(Map.of(":v1", AttributeValue.builder().s("user123").build()))
     *     .build();
     * Dataset results = executor.query(queryRequest, User.class);
     * }</pre>
     *
     * @param queryRequest the QueryRequest containing the table name and query parameters. Must not be null.
     * @param targetClass the class of the entities to convert to. Must not be null.
     * @return a Dataset containing entities of the specified target class. Never null.
     * @throws IllegalArgumentException if queryRequest or targetClass is null
     */
    public Dataset query(final QueryRequest queryRequest, final Class<?> targetClass) {
        if (targetClass == null || Map.class.isAssignableFrom(targetClass)) {
            final QueryResponse queryResult = dynamoDBClient.query(queryRequest);
            final List<Map<String, AttributeValue>> items = queryResult.items();

            if (N.notEmpty(queryResult.lastEvaluatedKey()) && N.isEmpty(queryRequest.exclusiveStartKey())) {
                QueryRequest newQueryRequest = queryRequest.copy(builder -> builder.exclusiveStartKey(queryResult.lastEvaluatedKey()));
                QueryResponse newQueryResult = queryResult;

                do {
                    final Map<String, AttributeValue> lastEvaluatedKey = newQueryResult.lastEvaluatedKey();
                    newQueryRequest = queryRequest.copy(builder -> builder.exclusiveStartKey(lastEvaluatedKey));
                    newQueryResult = dynamoDBClient.query(newQueryRequest);
                    items.addAll(newQueryResult.items());
                } while (N.notEmpty(newQueryResult.lastEvaluatedKey()));
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
    //            if (queryResult != null && N.notEmpty(queryResult.lastEvaluatedKey())) {
    //                if (newQueryRequest == scanRequest) {
    //                    newQueryRequest = scanRequest.clone();
    //                }
    //
    //                newQueryRequest.setExclusiveStartKey(queryResult.lastEvaluatedKey());
    //            }
    //
    //            queryResult = dynamoDB.scan(newQueryRequest);
    //        } while (pageOffset-- > 0 && N.notEmpty(queryResult.getItems()) && N.notEmpty(queryResult.lastEvaluatedKey()));
    //
    //        if (pageOffset >= 0 || pageCount-- <= 0) {
    //            return res;
    //        } else {
    //            res.addAll(toList(targetClass, queryResult));
    //        }
    //
    //        while (pageCount-- > 0 && N.notEmpty(queryResult.lastEvaluatedKey())) {
    //            if (newQueryRequest == scanRequest) {
    //                newQueryRequest = scanRequest.clone();
    //            }
    //
    //            newQueryRequest.setExclusiveStartKey(queryResult.lastEvaluatedKey());
    //            queryResult = dynamoDB.scan(newQueryRequest);
    //            res.addAll(toList(targetClass, queryResult));
    //        }
    //
    //        return res;
    //    }

    /**
     * Streams items from the specified DynamoDB table using a QueryRequest.
     *
     * <p>This method performs a query operation using AWS SDK v2 and returns the results as a stream of maps.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * QueryRequest queryRequest = QueryRequest.builder()
     *     .tableName("Users")
     *     .keyConditionExpression("userId = :v1")
     *     .expressionAttributeValues(Map.of(":v1", AttributeValue.builder().s("user123").build()))
     *     .build();
     * Stream<Map<String, Object>> results = executor.stream(queryRequest);
     * }</pre>
     *
     * @param queryRequest the QueryRequest containing the table name and query parameters. Must not be null.
     * @return a stream of maps representing the items retrieved by the query. Never null.
     * @throws IllegalArgumentException if queryRequest is null
     */
    public Stream<Map<String, Object>> stream(final QueryRequest queryRequest) {
        return stream(queryRequest, Clazz.PROPS_MAP);
    }

    /**
     * Streams items from the specified DynamoDB table using a QueryRequest and converts the results to entities of the specified type.
     *
     * <p>This method performs a query operation using AWS SDK v2 and returns the results as a stream of entities
     * of the specified target class.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * QueryRequest queryRequest = QueryRequest.builder()
     *     .tableName("Users")
     *     .keyConditionExpression("userId = :v1")
     *     .expressionAttributeValues(Map.of(":v1", AttributeValue.builder().s("user123").build()))
     *     .build();
     * Stream<User> results = executor.stream(queryRequest, User.class);
     * }</pre>
     *
     * @param <T> the type of the entities to convert to
     * @param queryRequest the QueryRequest containing the table name and query parameters. Must not be null.
     * @param targetClass the class of the entities to convert to. Must not be null.
     * @return a stream of entities of the specified target class. Never null.
     * @throws IllegalArgumentException if queryRequest or targetClass is null
     */
    public <T> Stream<T> stream(final QueryRequest queryRequest, final Class<T> targetClass) {

        final Iterator<List<Map<String, AttributeValue>>> iterator = new ObjIterator<>() {
            private QueryRequest newQueryRequest = queryRequest;
            private QueryResponse queryResult = null;
            private List<Map<String, AttributeValue>> items = null;

            @Override
            public boolean hasNext() {
                if (items == null || items.isEmpty()) {
                    while (queryResult == null || N.notEmpty(queryResult.lastEvaluatedKey())) {
                        if (queryResult != null && N.notEmpty(queryResult.lastEvaluatedKey())) {
                            final Map<String, AttributeValue> lastEvaluatedKey = queryResult.lastEvaluatedKey();
                            newQueryRequest = newQueryRequest.copy(builder -> builder.exclusiveStartKey(lastEvaluatedKey));
                        }

                        queryResult = dynamoDBClient.query(newQueryRequest);

                        if (queryResult.hasItems()) {
                            items = queryResult.items();
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
     * Scans items from the specified DynamoDB table and retrieves only the specified attributes.
     *
     * <p>This method performs a scan operation using AWS SDK v2 and returns the results as a stream
     * of maps, where each map represents an item with the specified attributes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<String> attributesToGet = List.of("id", "name", "status");
     * Stream<Map<String, Object>> results = executor.scan("Users", attributesToGet);
     * }</pre>
     *
     * @param tableName the name of the DynamoDB table to scan. Must not be null.
     * @param attributesToGet a list of attribute names to retrieve for each item. Must not be null or empty.
     * @return a stream of maps representing the items retrieved by the scan, containing only the specified attributes.
     * @throws IllegalArgumentException if tableName or attributesToGet is null or empty.
     */
    public Stream<Map<String, Object>> scan(final String tableName, final List<String> attributesToGet) {
        final ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).attributesToGet(attributesToGet).build();

        return scan(scanRequest);
    }

    /**
     * Scans items from the specified DynamoDB table using a ScanRequest with a scan filter.
     *
     * <p>This method performs a scan operation using AWS SDK v2 and returns the results as a list of maps.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, Condition> scanFilter = Map.of("status", Condition.builder().eq("active").build());
     * Stream<Map<String, Object>> results = executor.scan("Users", scanFilter);
     * }</pre>
     *
     * @param tableName the name of the DynamoDB table to scan. Must not be null.
     * @param scanFilter a map of conditions to filter the scan results. Must not be null.
     * @return a stream of maps representing the items retrieved by the scan. Never null.
     * @throws IllegalArgumentException if tableName or scanFilter is null
     */
    public Stream<Map<String, Object>> scan(final String tableName, final Map<String, Condition> scanFilter) {
        final ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).scanFilter(scanFilter).build();

        return scan(scanRequest);
    }

    /**
     * Scans items from the specified DynamoDB table using a ScanRequest with attributes to get and a scan filter.
     *
     * <p>This method performs a scan operation using AWS SDK v2 and returns the results as a list of maps.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<String> attributesToGet = Arrays.asList("userId", "name");
     * Map<String, Condition> scanFilter = Map.of("status", Condition.builder().eq("active").build());
     * Stream<Map<String, Object>> results = executor.scan("Users", attributesToGet, scanFilter);
     * }</pre>
     *
     * @param tableName the name of the DynamoDB table to scan. Must not be null.
     * @param attributesToGet a list of attribute names to retrieve. Must not be null.
     * @param scanFilter a map of conditions to filter the scan results. Must not be null.
     * @return a stream of maps representing the items retrieved by the scan. Never null.
     * @throws IllegalArgumentException if tableName, attributesToGet, or scanFilter is null
     */
    public Stream<Map<String, Object>> scan(final String tableName, final List<String> attributesToGet, final Map<String, Condition> scanFilter) {
        final ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).attributesToGet(attributesToGet).scanFilter(scanFilter).build();

        return scan(scanRequest);
    }

    /**
     * Scans items from the specified DynamoDB table using a ScanRequest and returns the results as a stream of maps.
     *
     * <p>This method performs a scan operation using AWS SDK v2 and returns the results as a stream of maps.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ScanRequest scanRequest = ScanRequest.builder()
     *     .tableName("Users")
     *     .build();
     * Stream<Map<String, Object>> results = executor.scan(scanRequest);
     * }</pre>
     *
     * @param scanRequest the ScanRequest containing the table name and scan parameters. Must not be null.
     * @return a stream of maps representing the items retrieved by the scan. Never null.
     * @throws IllegalArgumentException if scanRequest is null
     */
    public Stream<Map<String, Object>> scan(final ScanRequest scanRequest) {
        return scan(scanRequest, Clazz.PROPS_MAP);
    }

    /**
     * Scans items from the specified DynamoDB table using a ScanRequest and converts the results to a stream of entities of the specified type.
     *
     * <p>This method performs a scan operation using AWS SDK v2 and returns the results as a stream of entities
     * of the specified target class.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ScanRequest scanRequest = ScanRequest.builder()
     *     .tableName("Users")
     *     .build();
     * Stream<User> results = executor.scan(scanRequest, User.class);
     * }</pre>
     *
     * @param <T> the type of the entities to convert to
     * @param tableName the name of the DynamoDB table to scan. Must not be null.
     * @param attributesToGet a list of attribute names to retrieve. Must not be null.
     * @param targetClass the class of the entities to convert to. Must not be null.
     * @return a stream of entities of the specified target class. Never null.
     * @throws IllegalArgumentException if tableName, attributesToGet, or targetClass is null
     */
    public <T> Stream<T> scan(final String tableName, final List<String> attributesToGet, final Class<T> targetClass) {
        final ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).attributesToGet(attributesToGet).build();

        return scan(scanRequest, targetClass);
    }

    /**
     * Scans items from the specified DynamoDB table using a ScanRequest with a scan filter and converts the results to a stream of entities of the specified type.
     *
     * <p>This method performs a scan operation using AWS SDK v2 and returns the results as a stream of entities
     * of the specified target class.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, Condition> scanFilter = Map.of("status", Condition.builder().eq("active").build());
     * Stream<User> results = executor.scan("Users", scanFilter, User.class);
     * }</pre>
     *
     * @param <T> the type of the entities to convert to
     * @param tableName the name of the DynamoDB table to scan. Must not be null.
     * @param scanFilter a map of conditions to filter the scan results. Must not be null.
     * @param targetClass the class of the entities to convert to. Must not be null.
     * @return a stream of entities of the specified target class. Never null.
     * @throws IllegalArgumentException if tableName, scanFilter, or targetClass is null
     */
    public <T> Stream<T> scan(final String tableName, final Map<String, Condition> scanFilter, final Class<T> targetClass) {
        final ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).scanFilter(scanFilter).build();

        return scan(scanRequest, targetClass);
    }

    /**
     * Scans items from the specified DynamoDB table using a ScanRequest with attributes to get and a scan filter,
     * and converts the results to a stream of entities of the specified type.
     *
     * <p>This method performs a scan operation using AWS SDK v2 and returns the results as a stream of entities
     * of the specified target class.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<String> attributesToGet = Arrays.asList("userId", "name");
     * Map<String, Condition> scanFilter = Map.of("status", Condition.builder().eq("active").build());
     * Stream<User> results = executor.scan("Users", attributesToGet, scanFilter, User.class);
     * }</pre>
     *
     * @param <T> the type of the entities to convert to
     * @param tableName the name of the DynamoDB table to scan. Must not be null.
     * @param attributesToGet a list of attribute names to retrieve. Must not be null.
     * @param scanFilter a map of conditions to filter the scan results. Must not be null.
     * @param targetClass the class of the entities to convert to. Must not be null.
     * @return a stream of entities of the specified target class. Never null.
     * @throws IllegalArgumentException if tableName, attributesToGet, scanFilter, or targetClass is null
     */
    public <T> Stream<T> scan(final String tableName, final List<String> attributesToGet, final Map<String, Condition> scanFilter, final Class<T> targetClass) {
        final ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).attributesToGet(attributesToGet).scanFilter(scanFilter).build();

        return scan(scanRequest, targetClass);
    }

    /**
     * Scans items from the specified DynamoDB table using a ScanRequest and converts the results to a stream of entities of the specified type.
     *
     * <p>This method performs a scan operation using AWS SDK v2 and returns the results as a stream of entities
     * of the specified target class.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ScanRequest scanRequest = ScanRequest.builder()
     *     .tableName("Users")
     *     .build();
     * Stream<User> results = executor.scan(scanRequest, User.class);
     * }</pre>
     *
     * @param <T> the type of the entities to convert to
     * @param scanRequest the ScanRequest containing the table name and scan parameters. Must not be null.
     * @param targetClass the class of the entities to convert to. Must not be null.
     * @return a stream of entities of the specified target class. Never null.
     * @throws IllegalArgumentException if scanRequest or targetClass is null
     */
    public <T> Stream<T> scan(final ScanRequest scanRequest, final Class<T> targetClass) {
        final Iterator<List<Map<String, AttributeValue>>> iterator = new ObjIterator<>() {
            private ScanRequest newScanRequest = scanRequest;
            private ScanResponse scanResult = null;
            private List<Map<String, AttributeValue>> items = null;

            @Override
            public boolean hasNext() {
                if (items == null || items.isEmpty()) {
                    while (scanResult == null || N.notEmpty(scanResult.lastEvaluatedKey())) {
                        if (scanResult != null && N.notEmpty(scanResult.lastEvaluatedKey())) {
                            final Map<String, AttributeValue> lastEvaluatedKey = scanResult.lastEvaluatedKey();
                            newScanRequest = newScanRequest.copy(builder -> builder.exclusiveStartKey(lastEvaluatedKey));
                        }

                        scanResult = dynamoDBClient.scan(newScanRequest);

                        if (scanResult.hasItems()) {
                            items = scanResult.items();
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
     * Closes this DynamoDB executor and releases all associated resources using AWS SDK v2.
     *
     * <p>This method shuts down the underlying DynamoDbClient, which includes closing HTTP connections,
     * stopping background threads, and releasing system resources. AWS SDK v2 provides more efficient
     * resource cleanup compared to v1, with better handling of connection pools and NIO channels.</p>
     *
     * <p><b>Resource Management in SDK v2:</b></p>
     * <ul>
     * <li>Closes HTTP connection pools (Netty or Apache HTTP client)</li>
     * <li>Shuts down NIO event loops and worker threads</li>
     * <li>Releases direct memory buffers and native resources</li>
     * <li>Cancels any pending requests gracefully</li>
     * <li>Cleans up SSL/TLS contexts and certificate stores</li>
     * </ul>
     *
     * <p><b>Best Practices:</b></p>
     * <ul>
     * <li>Always call close() when finished with the executor</li>
     * <li>Use try-with-resources for automatic resource management</li>
     * <li>Ensure all operations complete before closing</li>
     * <li>Don't share closed executors between threads</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Try-with-resources (recommended)
     * DynamoDbClient client = DynamoDbClient.builder()
     *     .region(Region.US_EAST_1)
     *     .build();
     *
     * try (DynamoDBExecutor executor = new DynamoDBExecutor(client)) {
     *     // Use executor for operations
     *     Map<String, Object> item = executor.getItem(tableName, key);
     *     // executor.close() called automatically
     * }
     *
     * // Manual cleanup
     * DynamoDBExecutor executor = new DynamoDBExecutor(client);
     * try {
     *     // Perform operations
     * } finally {
     *     executor.close();   // Ensure cleanup
     * }
     * }</pre>
     * @see AutoCloseable#close()
     * @see DynamoDbClient#close()
     */
    @Override
    public void close() {
        if (dynamoDBClient != null) {
            dynamoDBClient.close();
        }
    }

    /**
     * A generic mapper class for performing CRUD operations on DynamoDB entities.
     * 
     * <p>This mapper provides a simplified interface for interacting with DynamoDB tables,
     * automatically handling entity-to-DynamoDB attribute conversions and supporting batch operations.
     * The mapper requires entities to have properly annotated ID fields and follow JavaBean conventions
     * with getter/setter methods.</p>
     * 
     * <p>The mapper is initialized through the DynamoDBExecutor and is tied to a specific table and entity class.
     * It handles key extraction, attribute conversion, and provides both single-item and batch operations.</p>
     * 
     * <p><b>Thread Safety:</b> Instances of this class are thread-safe and can be shared across multiple threads.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DynamoDBExecutor executor = new DynamoDBExecutor(dynamoDbClient);
     * Mapper<User> userMapper = executor.mapper(User.class);
     * 
     * // Get a single item
     * User user = userMapper.getItem(asKey("userId", "12345"));
     * 
     * // Put a new item
     * User newUser = new User("67890", "John Doe");
     * userMapper.putItem(newUser);
     * 
     * // Batch operations
     * List<User> users = Arrays.asList(user1, user2, user3);
     * userMapper.batchPutItem(users);
     * }</pre>
     *
     * @param <T> the type of the entity class this mapper handles
     * @author haiyangli
     * @since 1.0
     */
    public static class Mapper<T> {
        private final DynamoDBExecutor dynamoDBExecutor;
        private final String tableName;
        private final Class<T> targetEntityClass;
        private final BeanInfo entityInfo;
        private final List<String> keyPropNames;
        private final List<PropInfo> keyPropInfos;
        private final NamingPolicy namingPolicy;

        /**
         * Constructs a new Mapper instance for the specified entity class.
         * 
         * <p>This constructor validates that the target class is a proper entity class with
         * getter/setter methods and exactly one ID field defined. It initializes the mapper
         * with the necessary metadata for entity-to-DynamoDB conversions.</p>
         * 
         * @param targetEntityClass the class of entities this mapper will handle; must be a valid bean class with ID annotations
         * @param dynamoDBExecutor the executor to use for DynamoDB operations; must not be null
         * @param tableName the name of the DynamoDB table; must not be null or empty
         * @param namingPolicy the naming policy for attribute name conversion; uses CAMEL_CASE if null
         * @throws IllegalArgumentException if targetEntityClass is null, not a bean class, or has zero or multiple ID fields
         * @throws IllegalArgumentException if dynamoDBExecutor is null or tableName is null/empty
         */
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
         * Retrieves an item from DynamoDB using the key values extracted from the provided entity.
         * 
         * <p>This method extracts the key attributes from the entity object and uses them to
         * fetch the corresponding item from DynamoDB. The entity parameter should have its
         * key fields populated.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User keyEntity = new User();
         * keyEntity.setUserId("12345");
         * User fullUser = userMapper.getItem(keyEntity);
         * }</pre>
         * 
         * @param entity an entity instance with populated key fields
         * @return the retrieved entity with all attributes populated, or null if not found
         */
        public T getItem(final T entity) {
            return dynamoDBExecutor.getItem(tableName, createKey(entity), targetEntityClass);
        }

        /**
         * Retrieves an item from DynamoDB with optional consistent read guarantee.
         * 
         * <p>When consistentRead is true, DynamoDB returns the most recent data reflecting all writes
         * that received a successful response prior to the read. This may have higher latency and
         * lower throughput than eventually consistent reads.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User keyEntity = new User();
         * keyEntity.setUserId("12345");
         * User user = userMapper.getItem(keyEntity, true);   // Consistent read
         * }</pre>
         * 
         * @param entity an entity instance with populated key fields
         * @param consistentRead true for strongly consistent read, {@code false} for eventually consistent, null to use default
         * @return the retrieved entity with all attributes populated, or null if not found
         */
        public T getItem(final T entity, final Boolean consistentRead) {
            return dynamoDBExecutor.getItem(tableName, createKey(entity), consistentRead, targetEntityClass);
        }

        /**
         * Retrieves an item from DynamoDB using the provided key attributes.
         * 
         * <p>This method allows direct specification of key attributes without requiring an entity instance.
         * The key map should contain all required key attributes for the table (partition key and sort key if applicable).</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, AttributeValue> key = asKey("userId", "12345");
         * User user = userMapper.getItem(key);
         * }</pre>
         * 
         * @param key a map containing the key attributes (partition key and sort key if applicable)
         * @return the retrieved entity with all attributes populated, or null if not found
         */
        public T getItem(final Map<String, AttributeValue> key) {
            return dynamoDBExecutor.getItem(tableName, key, targetEntityClass);
        }

        /**
         * Retrieves an item using a fully configured GetItemRequest.
         * 
         * <p>This method provides full control over the get operation, allowing specification of
         * projection expressions, return consumed capacity, and other advanced options.
         * If the table name is not specified in the request, it will be automatically set to this mapper's table.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * GetItemRequest request = GetItemRequest.builder()
         *     .key(asKey("userId", "12345"))
         *     .projectionExpression("userId, userName, email")
         *     .build();
         * User user = userMapper.getItem(request);
         * }</pre>
         * 
         * @param getItemRequest the fully configured request object
         * @return the retrieved entity, or null if not found
         * @throws IllegalArgumentException if the request specifies a different table than this mapper's table
         */
        public T getItem(final GetItemRequest getItemRequest) {
            return dynamoDBExecutor.getItem(checkItem(getItemRequest), targetEntityClass);
        }

        /**
         * Retrieves multiple items from DynamoDB in a single batch operation.
         * 
         * <p>This method extracts keys from the provided entities and fetches all corresponding items
         * in a single batch request. This is more efficient than multiple individual getItem calls.
         * DynamoDB limits batch get operations to 100 items per request.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<User> keyEntities = Arrays.asList(
         *     new User("12345"),
         *     new User("67890"),
         *     new User("11111")
         * );
         * List<User> users = userMapper.batchGetItem(keyEntities);
         * }</pre>
         * 
         * @param entities collection of entities with populated key fields
         * @return list of retrieved entities; may be smaller than input if some items don't exist
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
         * Retrieves multiple items with optional consumed capacity information.
         * 
         * <p>This method performs a batch get operation and can return information about the
         * consumed read capacity units, useful for monitoring and optimization.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<User> users = userMapper.batchGetItem(keyEntities, "TOTAL");
         * // Check logs or response for consumed capacity information
         * }</pre>
         * 
         * @param entities collection of entities with populated key fields
         * @param returnConsumedCapacity specify "INDEXES", "TOTAL", or "NONE" for capacity details
         * @return list of retrieved entities; may be smaller than input if some items don't exist
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
         * Retrieves multiple items using a fully configured BatchGetItemRequest.
         * 
         * <p>This method provides full control over the batch get operation, including
         * attribute projections and consumed capacity settings.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * BatchGetItemRequest request = BatchGetItemRequest.builder()
         *     .requestItems(createKeys(entities))
         *     .returnConsumedCapacity("TOTAL")
         *     .build();
         * List<User> users = userMapper.batchGetItem(request);
         * }</pre>
         * 
         * @param batchGetItemRequest the fully configured batch request
         * @return list of retrieved entities
         * @throws IllegalArgumentException if the request specifies a different table than this mapper's table
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
         * Saves an entity to DynamoDB, creating a new item or replacing an existing one.
         * 
         * <p>This method converts the entity to DynamoDB attributes using the configured naming policy
         * and writes it to the table. If an item with the same key already exists, it will be completely replaced.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User user = new User("12345", "John Doe", "john@example.com");
         * PutItemResponse response = userMapper.putItem(user);
         * }</pre>
         * 
         * @param entity the entity to save; all non-null fields will be written
         * @return the response from DynamoDB containing metadata about the operation
         */
        public PutItemResponse putItem(final T entity) {
            return dynamoDBExecutor.putItem(tableName, DynamoDBExecutor.toItem(entity, namingPolicy));
        }

        /**
         * Saves an entity with optional return values specification.
         * 
         * <p>This method allows you to specify what values should be returned after the put operation,
         * such as the old item's attributes before it was replaced.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User user = new User("12345", "John Doe", "john@example.com");
         * PutItemResponse response = userMapper.putItem(user, "ALL_OLD");
         * // response will contain the previous item's attributes if it existed
         * }</pre>
         * 
         * @param entity the entity to save
         * @param returnValues specify "ALL_OLD", "NONE", etc. for what to return
         * @return the response from DynamoDB, potentially containing old item attributes
         */
        public PutItemResponse putItem(final T entity, final String returnValues) {
            return dynamoDBExecutor.putItem(tableName, DynamoDBExecutor.toItem(entity, namingPolicy), returnValues);
        }

        /**
         * Saves an item using a fully configured PutItemRequest.
         * 
         * <p>This method provides full control over the put operation, including conditional expressions,
         * return values, and other advanced options. If the table name is not specified in the request,
         * it will be automatically set to this mapper's table.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * PutItemRequest request = PutItemRequest.builder()
         *     .item(toItem(user))
         *     .conditionExpression("attribute_not_exists(userId)")
         *     .build();
         * PutItemResponse response = userMapper.putItem(request);
         * }</pre>
         * 
         * @param putItemRequest the fully configured request object
         * @return the response from DynamoDB
         * @throws IllegalArgumentException if the request specifies a different table than this mapper's table
         */
        public PutItemResponse putItem(final PutItemRequest putItemRequest) {
            return dynamoDBExecutor.putItem(checkItem(putItemRequest));
        }

        /**
         * Saves multiple entities to DynamoDB in a single batch operation.
         * 
         * <p>This method is more efficient than multiple individual putItem calls for bulk inserts.
         * DynamoDB limits batch write operations to 25 items per request. Items are written in parallel
         * and the operation is atomic per item but not for the batch as a whole.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<User> users = Arrays.asList(
         *     new User("1", "Alice"),
         *     new User("2", "Bob"),
         *     new User("3", "Charlie")
         * );
         * BatchWriteItemResponse response = userMapper.batchPutItem(users);
         * }</pre>
         * 
         * @param entities collection of entities to save
         * @return the response containing information about unprocessed items if any
         */
        public BatchWriteItemResponse batchPutItem(final Collection<? extends T> entities) {
            return dynamoDBExecutor.batchWriteItem(createBatchPutRequest(entities));
        }

        /**
         * Updates an existing item in DynamoDB with the non-null fields from the entity.
         * 
         * <p>This method performs a partial update, only modifying attributes that are non-null
         * in the provided entity. The key fields must be populated to identify the item to update.
         * Fields with null values in the entity will not be modified in DynamoDB.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User updates = new User();
         * updates.setUserId("12345");
         * updates.setEmail("newemail@example.com");
         * // Only email will be updated, other fields remain unchanged
         * UpdateItemResponse response = userMapper.updateItem(updates);
         * }</pre>
         * 
         * @param entity the entity containing updates; key fields must be populated
         * @return the response from DynamoDB containing metadata about the operation
         */
        public UpdateItemResponse updateItem(final T entity) {
            return dynamoDBExecutor.updateItem(tableName, createKey(entity), DynamoDBExecutor.toUpdateItem(entity, namingPolicy));
        }

        /**
         * Updates an item with optional return values specification.
         * 
         * <p>This method allows you to specify what values should be returned after the update,
         * such as all new attribute values or only the updated attributes.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User updates = new User();
         * updates.setUserId("12345");
         * updates.setEmail("newemail@example.com");
         * UpdateItemResponse response = userMapper.updateItem(updates, "ALL_NEW");
         * // response will contain all attributes of the item after the update
         * }</pre>
         * 
         * @param entity the entity containing updates
         * @param returnValues specify "ALL_NEW", "ALL_OLD", "UPDATED_NEW", "UPDATED_OLD", or "NONE"
         * @return the response from DynamoDB, potentially containing item attributes
         */
        public UpdateItemResponse updateItem(final T entity, final String returnValues) {
            return dynamoDBExecutor.updateItem(tableName, createKey(entity), DynamoDBExecutor.toUpdateItem(entity, namingPolicy), returnValues);
        }

        /**
         * Updates an item using a fully configured UpdateItemRequest.
         * 
         * <p>This method provides full control over the update operation, including update expressions,
         * conditional expressions, and return values. If the table name is not specified in the request,
         * it will be automatically set to this mapper's table.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * UpdateItemRequest request = UpdateItemRequest.builder()
         *     .key(asKey("userId", "12345"))
         *     .updateExpression("SET #email = :email")
         *     .expressionAttributeNames(Map.of("#email", "email"))
         *     .expressionAttributeValues(Map.of(":email", AttributeValue.builder().s("new@example.com").build()))
         *     .build();
         * UpdateItemResponse response = userMapper.updateItem(request);
         * }</pre>
         * 
         * @param updateItemRequest the fully configured request object
         * @return the response from DynamoDB
         * @throws IllegalArgumentException if the request specifies a different table than this mapper's table
         */
        public UpdateItemResponse updateItem(final UpdateItemRequest updateItemRequest) {
            return dynamoDBExecutor.updateItem(checkItem(updateItemRequest));
        }

        /**
         * Deletes an item from DynamoDB using the key values extracted from the provided entity.
         * 
         * <p>This method extracts the key attributes from the entity and deletes the corresponding
         * item from DynamoDB. Only the key fields need to be populated in the entity.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User userToDelete = new User();
         * userToDelete.setUserId("12345");
         * DeleteItemResponse response = userMapper.deleteItem(userToDelete);
         * }</pre>
         * 
         * @param entity an entity instance with populated key fields
         * @return the response from DynamoDB containing metadata about the operation
         */
        public DeleteItemResponse deleteItem(final T entity) {
            return dynamoDBExecutor.deleteItem(tableName, createKey(entity));
        }

        /**
         * Deletes an item with optional return values specification.
         * 
         * <p>This method allows you to specify that the deleted item's attributes should be
         * returned in the response, useful for logging or confirmation purposes.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User userToDelete = new User();
         * userToDelete.setUserId("12345");
         * DeleteItemResponse response = userMapper.deleteItem(userToDelete, "ALL_OLD");
         * // response will contain the deleted item's attributes
         * }</pre>
         * 
         * @param entity an entity instance with populated key fields
         * @param returnValues specify "ALL_OLD" or "NONE" for what to return
         * @return the response from DynamoDB, potentially containing the deleted item's attributes
         */
        public DeleteItemResponse deleteItem(final T entity, final String returnValues) {
            return dynamoDBExecutor.deleteItem(tableName, createKey(entity), returnValues);
        }

        /**
         * Deletes an item from DynamoDB using the provided key attributes.
         * 
         * <p>This method allows direct specification of key attributes without requiring an entity instance.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, AttributeValue> key = asKey("userId", "12345");
         * DeleteItemResponse response = userMapper.deleteItem(key);
         * }</pre>
         * 
         * @param key a map containing the key attributes (partition key and sort key if applicable)
         * @return the response from DynamoDB containing metadata about the operation
         */
        public DeleteItemResponse deleteItem(final Map<String, AttributeValue> key) {
            return dynamoDBExecutor.deleteItem(tableName, key);
        }

        /**
         * Deletes an item using a fully configured DeleteItemRequest.
         * 
         * <p>This method provides full control over the delete operation, including conditional expressions
         * and return values. If the table name is not specified in the request, it will be automatically
         * set to this mapper's table.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * DeleteItemRequest request = DeleteItemRequest.builder()
         *     .key(asKey("userId", "12345"))
         *     .conditionExpression("attribute_exists(userId)")
         *     .returnValues("ALL_OLD")
         *     .build();
         * DeleteItemResponse response = userMapper.deleteItem(request);
         * }</pre>
         * 
         * @param deleteItemRequest the fully configured request object
         * @return the response from DynamoDB
         * @throws IllegalArgumentException if the request specifies a different table than this mapper's table
         */
        public DeleteItemResponse deleteItem(final DeleteItemRequest deleteItemRequest) {
            return dynamoDBExecutor.deleteItem(checkItem(deleteItemRequest));
        }

        /**
         * Deletes multiple items from DynamoDB in a single batch operation.
         * 
         * <p>This method is more efficient than multiple individual deleteItem calls for bulk deletions.
         * DynamoDB limits batch write operations to 25 items per request. Deletions are processed
         * in parallel and are atomic per item but not for the batch as a whole.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<User> usersToDelete = Arrays.asList(
         *     new User("12345"),
         *     new User("67890"),
         *     new User("11111")
         * );
         * BatchWriteItemResponse response = userMapper.batchDeleteItem(usersToDelete);
         * }</pre>
         * 
         * @param entities collection of entities with populated key fields to delete
         * @return the response containing information about unprocessed items if any
         */
        public BatchWriteItemResponse batchDeleteItem(final Collection<? extends T> entities) {
            return dynamoDBExecutor.batchWriteItem(createBatchDeleteRequest(entities));
        }

        /**
         * Performs a batch write operation using a fully configured BatchWriteItemRequest.
         * 
         * <p>This method can handle mixed operations (puts and deletes) in a single batch request.
         * It provides full control over the batch operation parameters.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, List<WriteRequest>> requestItems = new HashMap<>();
         * requestItems.put(tableName, Arrays.asList(
         *     WriteRequest.builder().putRequest(PutRequest.builder().item(item1).build()).build(),
         *     WriteRequest.builder().deleteRequest(DeleteRequest.builder().key(key2).build()).build()
         * ));
         * BatchWriteItemRequest request = BatchWriteItemRequest.builder()
         *     .requestItems(requestItems)
         *     .build();
         * BatchWriteItemResponse response = userMapper.batchWriteItem(request);
         * }</pre>
         * 
         * @param batchWriteItemRequest the fully configured batch request
         * @return the response containing information about unprocessed items if any
         * @throws IllegalArgumentException if the request specifies a different table than this mapper's table
         */
        public BatchWriteItemResponse batchWriteItem(final BatchWriteItemRequest batchWriteItemRequest) {
            return dynamoDBExecutor.batchWriteItem(checkItem(batchWriteItemRequest));
        }

        /**
         * Executes a query and returns the results as a list of entities.
         * 
         * <p>This method performs a query operation on the table using the provided query request
         * and converts the results to entity objects. Queries are efficient for retrieving multiple
         * items with the same partition key.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * QueryRequest request = QueryRequest.builder()
         *     .keyConditionExpression("userId = :userId")
         *     .expressionAttributeValues(Map.of(":userId", AttributeValue.builder().s("12345").build()))
         *     .build();
         * List<User> users = userMapper.list(request);
         * }</pre>
         * 
         * @param queryRequest the query request with key conditions and other parameters
         * @return a list of entities matching the query; empty list if no matches found
         * @throws IllegalArgumentException if the request specifies a different table than this mapper's table
         */
        public List<T> list(final QueryRequest queryRequest) {
            return dynamoDBExecutor.list(checkQueryRequest(queryRequest), targetEntityClass);
        }

        /**
         * Executes a query and returns the results as a Dataset.
         * 
         * <p>This method provides query results in a Dataset format, which offers additional
         * functionality for data manipulation and analysis beyond a simple list.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * QueryRequest request = QueryRequest.builder()
         *     .keyConditionExpression("userId = :userId")
         *     .expressionAttributeValues(Map.of(":userId", AttributeValue.builder().s("12345").build()))
         *     .build();
         * Dataset dataset = userMapper.query(request);
         * // Use Dataset methods for further processing
         * }</pre>
         * 
         * @param queryRequest the query request with key conditions and other parameters
         * @return a Dataset containing the query results
         * @throws IllegalArgumentException if the request specifies a different table than this mapper's table
         */
        public Dataset query(final QueryRequest queryRequest) {
            return dynamoDBExecutor.query(checkQueryRequest(queryRequest), targetEntityClass);
        }

        /**
         * Executes a query and returns the results as a Stream of entities.
         * 
         * <p>This method is ideal for processing large result sets lazily. The stream will
         * automatically handle pagination, fetching additional pages as needed when elements
         * are consumed from the stream.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * QueryRequest request = QueryRequest.builder()
         *     .keyConditionExpression("userId = :userId")
         *     .expressionAttributeValues(Map.of(":userId", AttributeValue.builder().s("12345").build()))
         *     .build();
         * userMapper.stream(request)
         *     .filter(user -> user.getAge() > 18)
         *     .forEach(user -> System.out.println(user.getName()));
         * }</pre>
         * 
         * @param queryRequest the query request with key conditions and other parameters
         * @return a Stream of entities; the stream should be closed after use
         * @throws IllegalArgumentException if the request specifies a different table than this mapper's table
         */
        public Stream<T> stream(final QueryRequest queryRequest) {
            return dynamoDBExecutor.stream(checkQueryRequest(queryRequest), targetEntityClass);
        }

        /**
         * Performs a table scan with optional attribute projection.
         * 
         * <p>This method scans the entire table and returns a stream of entities. You can specify
         * which attributes to retrieve to reduce data transfer and improve performance.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> attributes = Arrays.asList("userId", "userName", "email");
         * Stream<User> users = userMapper.scan(attributes);
         * users.forEach(user -> System.out.println(user.getUserName()));
         * }</pre>
         * 
         * @param attributesToGet list of attribute names to retrieve; null to get all attributes
         * @return a Stream of entities from the scan operation
         */
        public Stream<T> scan(final List<String> attributesToGet) {
            return dynamoDBExecutor.scan(tableName, attributesToGet, targetEntityClass);
        }

        /**
         * Performs a filtered table scan.
         * 
         * <p>This method scans the table and applies the specified filter conditions to the results.
         * Note that filtering happens after items are read, so this still consumes read capacity
         * for all scanned items.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> filter = Filters.gt("age", 18);
         * Stream<User> adults = userMapper.scan(filter);
         * }</pre>
         * 
         * @param scanFilter map of attribute names to conditions for filtering results
         * @return a Stream of entities matching the filter conditions
         */
        public Stream<T> scan(final Map<String, Condition> scanFilter) {
            return dynamoDBExecutor.scan(tableName, scanFilter, targetEntityClass);
        }

        /**
         * Performs a filtered table scan with attribute projection.
         * 
         * <p>This method combines filtering and attribute projection, allowing you to both limit
         * which items are returned and which attributes are included in each item.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> attributes = Arrays.asList("userId", "userName");
         * Map<String, Condition> filter = Filters.eq("status", "ACTIVE");
         * Stream<User> activeUsers = userMapper.scan(attributes, filter);
         * }</pre>
         * 
         * @param attributesToGet list of attribute names to retrieve
         * @param scanFilter map of attribute names to conditions for filtering results
         * @return a Stream of entities matching the filter with only specified attributes
         */
        public Stream<T> scan(final List<String> attributesToGet, final Map<String, Condition> scanFilter) {
            return dynamoDBExecutor.scan(tableName, attributesToGet, scanFilter, targetEntityClass);
        }

        /**
         * Performs a scan using a fully configured ScanRequest.
         * 
         * <p>This method provides full control over the scan operation, including filter expressions,
         * projection expressions, and pagination settings. If the table name is not specified in
         * the request, it will be automatically set to this mapper's table.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * ScanRequest request = ScanRequest.builder()
         *     .filterExpression("age > :minAge")
         *     .expressionAttributeValues(Map.of(":minAge", AttributeValue.builder().n("18").build()))
         *     .projectionExpression("userId, userName, age")
         *     .build();
         * Stream<User> users = userMapper.scan(request);
         * }</pre>
         * 
         * @param scanRequest the fully configured scan request
         * @return a Stream of entities from the scan operation
         * @throws IllegalArgumentException if the request specifies a different table than this mapper's table
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

            return N.asMap(tableName, KeysAndAttributes.builder().keys(keys).build());
        }

        private Map<String, List<WriteRequest>> createBatchPutRequest(final Collection<? extends T> entities) {
            final List<WriteRequest> keys = new ArrayList<>(entities.size());

            for (final T entity : entities) {
                keys.add(WriteRequest.builder().putRequest(PutRequest.builder().item(toItem(entity)).build()).build());
            }

            return N.asMap(tableName, keys);
        }

        private Map<String, List<WriteRequest>> createBatchDeleteRequest(final Collection<? extends T> entities) {
            final List<WriteRequest> keys = new ArrayList<>(entities.size());

            for (final T entity : entities) {
                keys.add(WriteRequest.builder().deleteRequest(DeleteRequest.builder().key(createKey(entity)).build()).build());
            }

            return N.asMap(tableName, keys);
        }

        private GetItemRequest checkItem(final GetItemRequest item) {
            if (Strings.isEmpty(item.tableName())) {
                return item.copy(builder -> builder.tableName(tableName));
            } else {
                checkTableName(item.tableName());
            }

            return item;
        }

        private BatchGetItemRequest checkItem(final BatchGetItemRequest item) {
            if (item.requestItems() != null) {
                for (final String tableNameInRequest : item.requestItems().keySet()) {
                    checkTableName(tableNameInRequest);
                }
            }

            return item;

        }

        private BatchWriteItemRequest checkItem(final BatchWriteItemRequest item) {
            if (item.requestItems() != null) {
                for (final String tableNameInRequest : item.requestItems().keySet()) {
                    checkTableName(tableNameInRequest);
                }
            }

            return item;
        }

        private PutItemRequest checkItem(final PutItemRequest item) {
            if (Strings.isEmpty(item.tableName())) {
                return item.copy(builder -> builder.tableName(tableName));
            } else {
                checkTableName(item.tableName());
            }

            return item;
        }

        private UpdateItemRequest checkItem(final UpdateItemRequest item) {
            if (Strings.isEmpty(item.tableName())) {
                return item.copy(builder -> builder.tableName(tableName));
            } else {
                checkTableName(item.tableName());
            }

            return item;
        }

        private DeleteItemRequest checkItem(final DeleteItemRequest item) {
            if (Strings.isEmpty(item.tableName())) {
                return item.copy(builder -> builder.tableName(tableName));
            } else {
                checkTableName(item.tableName());
            }

            return item;
        }

        private QueryRequest checkQueryRequest(final QueryRequest queryRequest) {
            if (Strings.isEmpty(queryRequest.tableName())) {
                return queryRequest.copy(builder -> builder.tableName(tableName));
            } else {
                checkTableName(queryRequest.tableName());
            }

            return queryRequest;
        }

        private ScanRequest checkScanRequest(final ScanRequest scanRequest) {
            if (Strings.isEmpty(scanRequest.tableName())) {
                return scanRequest.copy(builder -> builder.tableName(tableName));
            } else {
                checkTableName(scanRequest.tableName());
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
     * Utility class providing static factory methods for creating DynamoDB filter conditions.
     * 
     * <p>This class offers a convenient API for building scan and query filters without directly
     * constructing Condition objects. All methods return Map objects that can be used directly
     * with scan operations or combined using the ConditionBuilder.</p>
     * 
     * <p>The class follows a fluent interface pattern and provides methods for all DynamoDB
     * comparison operators including equality, range comparisons, null checks, and string operations.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Simple equality filter
     * Map<String, Condition> filter = Filters.eq("status", "ACTIVE");
     * 
     * // Range filter
     * Map<String, Condition> ageFilter = Filters.bt("age", 18, 65);
     * 
     * // Complex filter using builder
     * Map<String, Condition> complexFilter = Filters.builder()
     *     .eq("status", "ACTIVE")
     *     .gt("age", 18)
     *     .contains("email", "@example.com")
     *     .build();
     * }</pre>
     * 
     * @author haiyangli
     * @since 1.0
     */
    public static final class Filters {
        private Filters() {
            // singleton for Utility class
        }

        /**
         * Creates an equality condition filter.
         * 
         * <p>This filter matches items where the specified attribute equals the given value.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> filter = Filters.eq("userId", "12345");
         * }</pre>
         * 
         * @param attrName the name of the attribute to compare
         * @param attrValue the value to compare against
         * @return a map containing the equality condition
         */
        public static Map<String, Condition> eq(final String attrName, final Object attrValue) {
            return N.asMap(attrName, Condition.builder().comparisonOperator(ComparisonOperator.EQ).attributeValueList(attrValueOf(attrValue)).build());
        }

        /**
         * Creates a not-equal condition filter.
         * 
         * <p>This filter matches items where the specified attribute does not equal the given value.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> filter = Filters.ne("status", "DELETED");
         * }</pre>
         * 
         * @param attrName the name of the attribute to compare
         * @param attrValue the value to compare against
         * @return a map containing the not-equal condition
         */
        public static Map<String, Condition> ne(final String attrName, final Object attrValue) {
            return N.asMap(attrName, Condition.builder().comparisonOperator(ComparisonOperator.NE).attributeValueList(attrValueOf(attrValue)).build());
        }

        /**
         * Creates a greater-than condition filter.
         * 
         * <p>This filter matches items where the specified attribute is greater than the given value.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> filter = Filters.gt("age", 18);
         * }</pre>
         * 
         * @param attrName the name of the attribute to compare
         * @param attrValue the value to compare against
         * @return a map containing the greater-than condition
         */
        public static Map<String, Condition> gt(final String attrName, final Object attrValue) {
            return N.asMap(attrName, Condition.builder().comparisonOperator(ComparisonOperator.GT).attributeValueList(attrValueOf(attrValue)).build());
        }

        /**
         * Creates a greater-than-or-equal condition filter.
         * 
         * <p>This filter matches items where the specified attribute is greater than or equal to the given value.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> filter = Filters.ge("score", 60);
         * }</pre>
         * 
         * @param attrName the name of the attribute to compare
         * @param attrValue the value to compare against
         * @return a map containing the greater-than-or-equal condition
         */
        public static Map<String, Condition> ge(final String attrName, final Object attrValue) {
            return N.asMap(attrName, Condition.builder().comparisonOperator(ComparisonOperator.GE).attributeValueList(attrValueOf(attrValue)).build());
        }

        /**
         * Creates a less-than condition filter.
         * 
         * <p>This filter matches items where the specified attribute is less than the given value.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> filter = Filters.lt("price", 100.00);
         * }</pre>
         * 
         * @param attrName the name of the attribute to compare
         * @param attrValue the value to compare against
         * @return a map containing the less-than condition
         */
        public static Map<String, Condition> lt(final String attrName, final Object attrValue) {
            return N.asMap(attrName, Condition.builder().comparisonOperator(ComparisonOperator.LT).attributeValueList(attrValueOf(attrValue)).build());
        }

        /**
         * Creates a less-than-or-equal condition filter.
         * 
         * <p>This filter matches items where the specified attribute is less than or equal to the given value.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> filter = Filters.le("quantity", 10);
         * }</pre>
         * 
         * @param attrName the name of the attribute to compare
         * @param attrValue the value to compare against
         * @return a map containing the less-than-or-equal condition
         */
        public static Map<String, Condition> le(final String attrName, final Object attrValue) {
            return N.asMap(attrName, Condition.builder().comparisonOperator(ComparisonOperator.LE).attributeValueList(attrValueOf(attrValue)).build());
        }

        /**
         * Creates a between condition filter for range queries.
         * 
         * <p>This filter matches items where the specified attribute value is between the min and max values, inclusive.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> filter = Filters.bt("age", 18, 65);
         * }</pre>
         * 
         * @param attrName the name of the attribute to compare
         * @param minAttrValue the minimum value (inclusive)
         * @param maxAttrValue the maximum value (inclusive)
         * @return a map containing the between condition
         */
        public static Map<String, Condition> bt(final String attrName, final Object minAttrValue, final Object maxAttrValue) {
            return N.asMap(attrName,
                    Condition.builder()
                            .comparisonOperator(ComparisonOperator.BETWEEN)
                            .attributeValueList(attrValueOf(minAttrValue), attrValueOf(maxAttrValue))
                            .build());
        }

        /**
         * Creates a null check condition filter.
         * 
         * <p>This filter matches items where the specified attribute does not exist or has a null value.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> filter = Filters.isNull("deletedAt");
         * }</pre>
         * 
         * @param attrName the name of the attribute to check
         * @return a map containing the null condition
         */
        public static Map<String, Condition> isNull(final String attrName) {
            return N.asMap(attrName, Condition.builder().comparisonOperator(ComparisonOperator.NULL).build());
        }

        /**
         * Creates a not-null check condition filter.
         * 
         * <p>This filter matches items where the specified attribute exists and is not null.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> filter = Filters.notNull("email");
         * }</pre>
         * 
         * @param attrName the name of the attribute to check
         * @return a map containing the not-null condition
         */
        public static Map<String, Condition> notNull(final String attrName) {
            return N.asMap(attrName, Condition.builder().comparisonOperator(ComparisonOperator.NOT_NULL).build());
        }

        /**
         * Creates a contains condition filter for substring matching.
         * 
         * <p>This filter matches items where the specified attribute contains the given value as a substring.
         * For set attributes, it checks if the set contains the specified value.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> filter = Filters.contains("description", "important");
         * }</pre>
         * 
         * @param attrName the name of the attribute to search in
         * @param attrValue the value to search for
         * @return a map containing the contains condition
         */
        public static Map<String, Condition> contains(final String attrName, final Object attrValue) {
            return N.asMap(attrName, Condition.builder().comparisonOperator(ComparisonOperator.CONTAINS).attributeValueList(attrValueOf(attrValue)).build());
        }

        /**
         * Creates a not-contains condition filter.
         * 
         * <p>This filter matches items where the specified attribute does not contain the given value.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> filter = Filters.notContains("tags", "deprecated");
         * }</pre>
         * 
         * @param attrName the name of the attribute to search in
         * @param attrValue the value that should not be present
         * @return a map containing the not-contains condition
         */
        public static Map<String, Condition> notContains(final String attrName, final Object attrValue) {
            return N.asMap(attrName,
                    Condition.builder().comparisonOperator(ComparisonOperator.NOT_CONTAINS).attributeValueList(attrValueOf(attrValue)).build());
        }

        /**
         * Creates a begins-with condition filter for prefix matching.
         * 
         * <p>This filter matches items where the specified string attribute begins with the given prefix.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> filter = Filters.beginsWith("email", "admin@");
         * }</pre>
         * 
         * @param attrName the name of the string attribute to check
         * @param attrValue the prefix to match
         * @return a map containing the begins-with condition
         */
        public static Map<String, Condition> beginsWith(final String attrName, final Object attrValue) {
            return N.asMap(attrName, Condition.builder().comparisonOperator(ComparisonOperator.BEGINS_WITH).attributeValueList(attrValueOf(attrValue)).build());
        }

        /**
         * Creates an IN condition filter with variable arguments.
         * 
         * <p>This filter matches items where the specified attribute's value is one of the provided values.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> filter = Filters.in("status", "ACTIVE", "PENDING", "PROCESSING");
         * }</pre>
         * 
         * @param attrName the name of the attribute to check
         * @param attrValues the values to match against
         * @return a map containing the IN condition
         */
        public static Map<String, Condition> in(final String attrName, final Object... attrValues) {
            final Map<String, Condition> result = new HashMap<>(1);

            in(result, attrName, attrValues);

            return result;
        }

        /**
         * Creates an IN condition filter with a collection of values.
         * 
         * <p>This filter matches items where the specified attribute's value is one of the values in the collection.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> validStatuses = Arrays.asList("ACTIVE", "PENDING", "PROCESSING");
         * Map<String, Condition> filter = Filters.in("status", validStatuses);
         * }</pre>
         * 
         * @param attrName the name of the attribute to check
         * @param attrValues collection of values to match against
         * @return a map containing the IN condition
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

            final Condition cond = Condition.builder().comparisonOperator(ComparisonOperator.IN).attributeValueList(attributeValueList).build();

            output.put(attrName, cond);
        }

        static void in(final Map<String, Condition> output, final String attrName, final Collection<?> attrValues) {
            final AttributeValue[] attributeValueList = new AttributeValue[attrValues.size()];

            int i = 0;
            for (final Object attrValue : attrValues) {
                attributeValueList[i++] = attrValueOf(attrValue);
            }

            final Condition cond = Condition.builder().comparisonOperator(ComparisonOperator.IN).attributeValueList(attributeValueList).build();

            output.put(attrName, cond);
        }

        /**
         * Creates a new ConditionBuilder for constructing complex filter conditions.
         * 
         * <p>The builder allows chaining multiple conditions together to create compound filters
         * for scan and query operations.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> filter = Filters.builder()
         *     .eq("status", "ACTIVE")
         *     .gt("age", 18)
         *     .le("age", 65)
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
     * A builder class for constructing complex DynamoDB filter conditions using a fluent API.
     * 
     * <p>This builder allows chaining multiple condition methods to create compound filters
     * for scan and query operations. Each method adds a condition for a specific attribute,
     * and the build() method returns the complete filter as a Map.</p>
     * 
     * <p>The builder follows the builder pattern and provides a more readable way to construct
     * complex filters compared to manually creating and combining Condition objects.</p>
     * 
     * <p><b>Thread Safety:</b> This builder is NOT thread-safe and should not be shared between threads.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, Condition> complexFilter = ConditionBuilder.create()
     *     .eq("status", "ACTIVE")
     *     .bt("age", 25, 65)
     *     .contains("skills", "Java")
     *     .notNull("email")
     *     .in("department", "Engineering", "Research", "Development")
     *     .build();
     * 
     * // Use with a mapper
     * Stream<Employee> employees = mapper.scan(complexFilter);
     * }</pre>
     * 
     * @author haiyangli
     * @since 1.0
     */
    public static final class ConditionBuilder {
        private Map<String, Condition> condMap;

        /**
         * Constructs a new ConditionBuilder instance.
         * 
         * <p>Initializes an empty condition map that will be populated through the builder methods.</p>
         */
        ConditionBuilder() {
            condMap = new HashMap<>();
        }

        /**
         * Creates a new ConditionBuilder instance.
         * 
         * @return a new ConditionBuilder instance
         * @deprecated Use {@link Filters#builder()} instead.
         */
        @Deprecated
        public static ConditionBuilder create() {
            return new ConditionBuilder();
        }

        /**
         * Adds an equality condition to the filter.
         * 
         * <p>The condition will match items where the specified attribute equals the given value.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * builder.eq("userId", "12345");
         * }</pre>
         * 
         * @param attrName the name of the attribute to compare
         * @param attrValue the value to compare against
         * @return this builder instance for method chaining
         */
        public ConditionBuilder eq(final String attrName, final Object attrValue) {
            condMap.put(attrName, Condition.builder().comparisonOperator(ComparisonOperator.EQ).attributeValueList(attrValueOf(attrValue)).build());

            return this;
        }

        /**
         * Adds a not-equal condition to the filter.
         * 
         * <p>The condition will match items where the specified attribute does not equal the given value.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * builder.ne("status", "DELETED");
         * }</pre>
         * 
         * @param attrName the name of the attribute to compare
         * @param attrValue the value to compare against
         * @return this builder instance for method chaining
         */
        public ConditionBuilder ne(final String attrName, final Object attrValue) {
            condMap.put(attrName, Condition.builder().comparisonOperator(ComparisonOperator.NE).attributeValueList(attrValueOf(attrValue)).build());

            return this;
        }

        /**
         * Adds a greater-than condition to the filter.
         * 
         * <p>The condition will match items where the specified attribute is greater than the given value.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * builder.gt("age", 18);
         * }</pre>
         * 
         * @param attrName the name of the attribute to compare
         * @param attrValue the value to compare against
         * @return this builder instance for method chaining
         */
        public ConditionBuilder gt(final String attrName, final Object attrValue) {
            condMap.put(attrName, Condition.builder().comparisonOperator(ComparisonOperator.GT).attributeValueList(attrValueOf(attrValue)).build());

            return this;
        }

        /**
         * Adds a greater-than-or-equal condition to the filter.
         * 
         * <p>The condition will match items where the specified attribute is greater than or equal to the given value.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * builder.ge("score", 60);
         * }</pre>
         * 
         * @param attrName the name of the attribute to compare
         * @param attrValue the value to compare against
         * @return this builder instance for method chaining
         */
        public ConditionBuilder ge(final String attrName, final Object attrValue) {
            condMap.put(attrName, Condition.builder().comparisonOperator(ComparisonOperator.GE).attributeValueList(attrValueOf(attrValue)).build());

            return this;
        }

        /**
         * Adds a less-than condition to the filter.
         * 
         * <p>The condition will match items where the specified attribute is less than the given value.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * builder.lt("price", 100.00);
         * }</pre>
         * 
         * @param attrName the name of the attribute to compare
         * @param attrValue the value to compare against
         * @return this builder instance for method chaining
         */
        public ConditionBuilder lt(final String attrName, final Object attrValue) {
            condMap.put(attrName, Condition.builder().comparisonOperator(ComparisonOperator.LT).attributeValueList(attrValueOf(attrValue)).build());

            return this;
        }

        /**
         * Adds a less-than-or-equal condition to the filter.
         * 
         * <p>The condition will match items where the specified attribute is less than or equal to the given value.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * builder.le("quantity", 10);
         * }</pre>
         * 
         * @param attrName the name of the attribute to compare
         * @param attrValue the value to compare against
         * @return this builder instance for method chaining
         */
        public ConditionBuilder le(final String attrName, final Object attrValue) {
            condMap.put(attrName, Condition.builder().comparisonOperator(ComparisonOperator.LE).attributeValueList(attrValueOf(attrValue)).build());

            return this;
        }

        /**
         * Adds a between condition to the filter for range queries.
         * 
         * <p>The condition will match items where the specified attribute value is between
         * the min and max values, inclusive.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * builder.bt("age", 18, 65);
         * }</pre>
         * 
         * @param attrName the name of the attribute to compare
         * @param minAttrValue the minimum value (inclusive)
         * @param maxAttrValue the maximum value (inclusive)
         * @return this builder instance for method chaining
         */
        public ConditionBuilder bt(final String attrName, final Object minAttrValue, final Object maxAttrValue) {
            condMap.put(attrName,
                    Condition.builder()
                            .comparisonOperator(ComparisonOperator.BETWEEN)
                            .attributeValueList(attrValueOf(minAttrValue), attrValueOf(maxAttrValue))
                            .build());

            return this;
        }

        /**
         * Adds a null check condition to the filter.
         * 
         * <p>The condition will match items where the specified attribute does not exist or has a null value.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * builder.isNull("deletedAt");
         * }</pre>
         * 
         * @param attrName the name of the attribute to check
         * @return this builder instance for method chaining
         */
        public ConditionBuilder isNull(final String attrName) {
            condMap.put(attrName, Condition.builder().comparisonOperator(ComparisonOperator.NULL).build());

            return this;
        }

        /**
         * Adds a not-null check condition to the filter.
         * 
         * <p>The condition will match items where the specified attribute exists and is not null.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * builder.notNull("email");
         * }</pre>
         * 
         * @param attrName the name of the attribute to check
         * @return this builder instance for method chaining
         */
        public ConditionBuilder notNull(final String attrName) {
            condMap.put(attrName, Condition.builder().comparisonOperator(ComparisonOperator.NOT_NULL).build());

            return this;
        }

        /**
         * Adds a contains condition to the filter for substring matching.
         * 
         * <p>The condition will match items where the specified attribute contains the given value as a substring.
         * For set attributes, it checks if the set contains the specified value.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * builder.contains("description", "important");
         * }</pre>
         * 
         * @param attrName the name of the attribute to search in
         * @param attrValue the value to search for
         * @return this builder instance for method chaining
         */
        public ConditionBuilder contains(final String attrName, final Object attrValue) {
            condMap.put(attrName, Condition.builder().comparisonOperator(ComparisonOperator.CONTAINS).attributeValueList(attrValueOf(attrValue)).build());

            return this;
        }

        /**
         * Adds a not-contains condition to the filter.
         * 
         * <p>The condition will match items where the specified attribute does not contain the given value.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * builder.notContains("tags", "deprecated");
         * }</pre>
         * 
         * @param attrName the name of the attribute to search in
         * @param attrValue the value that should not be present
         * @return this builder instance for method chaining
         */
        public ConditionBuilder notContains(final String attrName, final Object attrValue) {
            condMap.put(attrName, Condition.builder().comparisonOperator(ComparisonOperator.NOT_CONTAINS).attributeValueList(attrValueOf(attrValue)).build());

            return this;
        }

        /**
         * Adds a begins-with condition to the filter for prefix matching.
         * 
         * <p>The condition will match items where the specified string attribute begins with the given prefix.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * builder.beginsWith("email", "admin@");
         * }</pre>
         * 
         * @param attrName the name of the string attribute to check
         * @param attrValue the prefix to match
         * @return this builder instance for method chaining
         */
        public ConditionBuilder beginsWith(final String attrName, final Object attrValue) {
            condMap.put(attrName, Condition.builder().comparisonOperator(ComparisonOperator.BEGINS_WITH).attributeValueList(attrValueOf(attrValue)).build());

            return this;
        }

        /**
         * Adds an IN condition to the filter with variable arguments.
         * 
         * <p>The condition will match items where the specified attribute's value is one of the provided values.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * builder.in("status", "ACTIVE", "PENDING", "PROCESSING");
         * }</pre>
         * 
         * @param attrName the name of the attribute to check
         * @param attrValues the values to match against
         * @return this builder instance for method chaining
         */
        public ConditionBuilder in(final String attrName, final Object... attrValues) {
            Filters.in(condMap, attrName, attrValues);

            return this;
        }

        /**
         * Adds an IN condition to the filter with a collection of values.
         * 
         * <p>The condition will match items where the specified attribute's value is one of the values in the collection.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> validStatuses = Arrays.asList("ACTIVE", "PENDING", "PROCESSING");
         * builder.in("status", validStatuses);
         * }</pre>
         * 
         * @param attrName the name of the attribute to check
         * @param attrValues collection of values to match against
         * @return this builder instance for method chaining
         */
        public ConditionBuilder in(final String attrName, final Collection<?> attrValues) {
            Filters.in(condMap, attrName, attrValues);

            return this;
        }

        /**
         * Builds the final filter condition map.
         * 
         * <p>Returns a map containing all the conditions added to this builder.
         * After calling this method, the builder cannot be reused.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> filter = builder.build();
         * }</pre>
         * 
         * @return a map containing all the conditions built by this builder
         */
        public Map<String, Condition> build() {
            final Map<String, Condition> result = condMap;

            condMap = null;

            return result;
        }
    }
}
