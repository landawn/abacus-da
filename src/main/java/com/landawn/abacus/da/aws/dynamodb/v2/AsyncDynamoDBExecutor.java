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

import static com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.attrValueOf;
import static com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.createRowMapper;
import static com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.extractData;
import static com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.readRow;
import static com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntities;
import static com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toItem;
import static com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toList;
import static com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toUpdateItem;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import com.landawn.abacus.parser.ParserUtil;
import com.landawn.abacus.parser.ParserUtil.BeanInfo;
import com.landawn.abacus.parser.ParserUtil.PropInfo;
import com.landawn.abacus.query.QueryUtil;
import com.landawn.abacus.util.Beans;
import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.Clazz;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.ExceptionUtil;
import com.landawn.abacus.util.Fn;
import com.landawn.abacus.util.InternalUtil;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.ObjIterator;
import com.landawn.abacus.util.Strings;
import com.landawn.abacus.util.stream.Stream;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
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
 * Asynchronous DynamoDB executor providing non-blocking AWS DynamoDB operations with CompletableFuture support.
 * 
 * <p>This executor serves as a high-level asynchronous wrapper around AWS DynamoDB SDK v2, offering
 * CompletableFuture-based operations for all DynamoDB interactions. It enables reactive programming
 * patterns and efficient handling of concurrent database operations without blocking threads.</p>
 *
 * <h2>Key Features and Architecture</h2>
 * <h3>Key Features:</h3>
 * <ul>
 * <li><b>Non-blocking Operations</b> - All methods return CompletableFuture for asynchronous execution</li>
 * <li><b>Complete CRUD Support</b> - Async versions of create, read, update, and delete operations</li>
 * <li><b>Batch Operations</b> - Efficient async batch get/write with proper limit handling</li>
 * <li><b>Query &amp; Scan</b> - Asynchronous querying with automatic pagination support</li>
 * <li><b>Stream Processing</b> - CompletableFuture&lt;Stream&gt; for memory-efficient large result processing</li>
 * <li><b>Object Mapping</b> - Automatic conversion between Java objects and DynamoDB AttributeValues</li>
 * <li><b>Type Safety</b> - Generic type support with Mapper&lt;T&gt; for entity-specific operations</li>
 * </ul>
 * 
 * <h3>DynamoDB v2 SDK Integration:</h3>
 * <p>Built on AWS SDK v2's DynamoDbAsyncClient, this executor benefits from improved performance,
 * better resource management, and enhanced async capabilities compared to v1 SDK implementations.</p>
 * 
 * <h3>Thread Safety &amp; Performance:</h3>
 * <p>This class is fully thread-safe and optimized for high-concurrency scenarios. The underlying
 * DynamoDbAsyncClient uses NIO-based networking with efficient connection pooling and automatic
 * retry mechanisms with exponential backoff.</p>
 * 
 * <h3>CompletableFuture Usage Patterns:</h3>
 * <ul>
 * <li><b>Async Chaining:</b> Chain operations using thenCompose(), thenApply(), thenAccept()</li>
 * <li><b>Parallel Execution:</b> Combine multiple operations with CompletableFuture.allOf()</li>
 * <li><b>Error Handling:</b> Use exceptionally(), handle(), or whenComplete() for robust error handling</li>
 * <li><b>Timeout Control:</b> Apply timeouts using orTimeout() or completeOnTimeout()</li>
 * </ul>
 * 
 * <h3>Resource Management:</h3>
 * <p>The executor implements AutoCloseable and should be closed when no longer needed to ensure
 * proper cleanup of underlying resources including connection pools and thread pools.</p>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Initialize async executor
 * AsyncDynamoDBExecutor executor = new AsyncDynamoDBExecutor(dynamoDbAsyncClient);
 * 
 * // Basic async operations
 * CompletableFuture<User> userFuture = executor.getItem("Users", key, User.class);
 * userFuture.thenApply(User::getName)
 *           .thenAccept(System.out::println)
 *           .exceptionally(ex -> {
 *               logger.error("Failed to get user", ex);
 *               return null;
 *           });
 * 
 * // Parallel operations
 * CompletableFuture<User> user1 = executor.getItem("Users", key1, User.class);
 * CompletableFuture<User> user2 = executor.getItem("Users", key2, User.class);
 * CompletableFuture<List<User>> bothUsers = user1.thenCombine(user2, Arrays::asList);
 * 
 * // Batch operations
 * CompletableFuture<Map<String, List<User>>> batchResult = 
 *     executor.batchGetItem(requestItems, User.class);
 * }</pre>
 * 
 * <h3>Error Handling:</h3>
 * <p>All CompletableFuture results may complete exceptionally with DynamoDbException or its subclasses.
 * Common exceptions include ResourceNotFoundException, ConditionalCheckFailedException, and
 * ProvisionedThroughputExceededException. Implement proper error handling using CompletableFuture's
 * exception handling methods.</p>
 *
 * @see <a href="https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/dynamodb/DynamoDbAsyncClient.html">DynamoDbAsyncClient</a>
 * @see <a href="https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/">DynamoDB Developer Guide</a>
 * @see CompletableFuture
 */
@SuppressWarnings("java:S1192")
public final class AsyncDynamoDBExecutor implements AutoCloseable {

    private final DynamoDbAsyncClient dynamoDBClient;

    /**
     * Constructs a new AsyncDynamoDBExecutor with the specified DynamoDB async client.
     * 
     * <p>The executor will use the provided async client for all DynamoDB operations,
     * inheriting its configuration including region, credentials, retry policies, and
     * connection settings. The client should be properly configured before passing
     * to this constructor.</p>
     *
     * <p><b>Client Configuration:</b> Ensure the DynamoDbAsyncClient is configured with:</p>
     * <ul>
     * <li>Appropriate AWS credentials</li>
     * <li>Correct AWS region</li>
     * <li>Suitable retry policy and timeout settings</li>
     * <li>Proper connection pool settings for your use case</li>
     * </ul>
     * 
     * @param dynamoDBClient the DynamoDB async client to use for operations. Must not be null.
     * @throws IllegalArgumentException if dynamoDBClient is null
     */
    public AsyncDynamoDBExecutor(final DynamoDbAsyncClient dynamoDBClient) {
        if (dynamoDBClient == null) {
            throw new IllegalArgumentException("dynamoDBClient cannot be null");
        }
        this.dynamoDBClient = dynamoDBClient;
    }

    /**
     * Returns the underlying DynamoDB async client used by this executor.
     * 
     * <p>This provides direct access to the async client for operations not covered by this executor
     * or for advanced configuration. Use with caution as direct client usage bypasses this executor's
     * object mapping and convenience features.</p>
     * 
     * <p>The returned client is the same instance used internally and should not be closed
     * separately from this executor.</p>
     * 
     * @return the DynamoDbAsyncClient instance used by this executor, never null
     */
    public DynamoDbAsyncClient dynamoDBAsyncClient() {
        return dynamoDBClient;
    }

    @SuppressWarnings("rawtypes")
    private final Map<Class<?>, Mapper> mapperPool = new ConcurrentHashMap<>();

    /**
     * Creates a type-safe async mapper for the specified entity class with automatic table name detection.
     * 
     * <p>This method creates a cached async mapper that provides type-safe asynchronous operations for a specific 
     * entity class. The table name is automatically derived from @Table annotations on the class. The mapper 
     * uses CAMEL_CASE naming policy by default for attribute name conversion.</p>
     * 
     * <p>Entity classes must be annotated with @Table, @javax.persistence.Table, or @jakarta.persistence.Table
     * to specify the DynamoDB table name. ID fields must be annotated with appropriate key annotations.</p>
     * 
     * <p><b>Async Benefits:</b></p>
     * <ul>
     * <li>All mapper operations return CompletableFuture for non-blocking execution</li>
     * <li>Supports reactive programming patterns and async composition</li>
     * <li>Efficient resource utilization for high-concurrency scenarios</li>
     * <li>Compatible with timeout and error handling mechanisms</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * @Table("Users")
     * public class User {
     *     @Id
     *     private String userId;
     *     private String name;
     *     // getters and setters...
     * }
     * 
     * AsyncDynamoDBExecutor.Mapper<User> userMapper = executor.mapper(User.class);
     * CompletableFuture<User> userFuture = userMapper.getItem(user);
     * }</pre>
     * 
     * @param <T> the entity type
     * @param targetEntityClass the entity class to create mapper for. Must be annotated with @Table. Must not be null.
     * @return a cached async Mapper instance for the specified entity class, never null
     * @throws IllegalArgumentException if targetEntityClass is null, not a bean class, or missing @Table annotation
     */
    public <T> Mapper<T> mapper(final Class<T> targetEntityClass) {
        @SuppressWarnings("rawtypes")
        Mapper result = mapperPool.get(targetEntityClass);

        if (result == null) {
            final BeanInfo entityInfo = ParserUtil.getBeanInfo(targetEntityClass);

            if (entityInfo.tableName.isEmpty()) {
                throw new IllegalArgumentException("Entity class " + targetEntityClass
                        + " must be annotated with @Table (com.landawn.abacus.annotation, javax.persistence, or jakarta.persistence). Alternatively, use AsyncDynamoDBExecutor.mapper(String tableName, Class<T> entityClass)");
            }

            result = mapper(targetEntityClass, entityInfo.tableName.get(), NamingPolicy.CAMEL_CASE);

            mapperPool.put(targetEntityClass, result);
        }

        return result;
    }

    /**
     * Creates a type-safe async mapper for the specified entity class with explicit table name and naming policy.
     * 
     * <p>This method creates an async mapper with full customization of table name and attribute naming policy.
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
     * public class Product {
     *     private String productId;
     *     private String name;
     *     // getters and setters...
     * }
     * 
     * AsyncDynamoDBExecutor.Mapper<Product> mapper = 
     *     executor.mapper(Product.class, "ProductTable", NamingPolicy.SNAKE_CASE);
     * 
     * CompletableFuture<Product> future = mapper.getItem(product);
     * }</pre>
     * 
     * @param <T> the entity type
     * @param targetEntityClass the entity class to create mapper for. Must be a valid bean class. Must not be null.
     * @param tableName the DynamoDB table name to use for operations. Must not be null or empty.
     * @param namingPolicy the naming policy for converting property names to attribute names. Must not be null.
     * @return a new async Mapper instance configured with the specified parameters, never null
     * @throws IllegalArgumentException if any parameter is null, targetEntityClass is not a bean class, or tableName is empty
     */
    public <T> Mapper<T> mapper(final Class<T> targetEntityClass, final String tableName, final NamingPolicy namingPolicy) {
        return new Mapper<>(targetEntityClass, this, tableName, namingPolicy);
    }

    /**
     * Asynchronously retrieves an item from the specified DynamoDB table.
     * 
     * <p>This method performs an eventually consistent read by default and returns the item
     * as a Map of attribute names to Java objects. The CompletableFuture will complete with
     * null if the item doesn't exist, or complete exceptionally if the operation fails.</p>
     * 
     * <p><b>Async Operation Benefits:</b></p>
     * <ul>
     * <li>Non-blocking - frees up calling thread for other operations</li>
     * <li>Composable - can be chained with other async operations using thenCompose(), thenApply()</li>
     * <li>Error handling - use exceptionally() or handle() for robust error management</li>
     * <li>Timeout support - apply timeouts using orTimeout() methods</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> key = Map.of("userId", AttributeValue.builder().s("user123").build());
     *
     * CompletableFuture<Map<String, Object>> itemFuture = executor.getItem("Users", key);
     *
     * itemFuture.thenAccept(item -> {
     *     if (item != null) {
     *         System.out.println("User name: " + item.get("name"));
     *     } else {
     *         System.out.println("User not found");
     *     }
     * }).exceptionally(ex -> {
     *     logger.error("Failed to get user", ex);
     *     return null;
     * });
     * }</pre>
     * 
     * @param tableName the name of the DynamoDB table to retrieve the item from. Must not be null or empty.
     * @param key the primary key of the item to retrieve. Must include all key attributes. Must not be null.
     * @return a CompletableFuture containing the item as a Map of attribute names to values, 
     *         or null if the item doesn't exist
     * @throws IllegalArgumentException if tableName is null/empty or key is null
     * @see #getItem(String, Map, Boolean) for consistent read operations
     * @see DynamoDbAsyncClient#getItem(GetItemRequest)
     */
    public CompletableFuture<Map<String, Object>> getItem(final String tableName, final Map<String, AttributeValue> key) {
        return getItem(tableName, key, Clazz.PROPS_MAP);
    }

    /**
     * Asynchronously retrieves an item from the specified table with optional consistent read.
     * 
     * <p>This method allows you to control the read consistency level while maintaining all benefits
     * of asynchronous execution. Strongly consistent reads ensure you get the most recent item data
     * but consume more read capacity and may have slightly higher latency.</p>
     * 
     * <p><b>Read Consistency Trade-offs:</b></p>
     * <ul>
     * <li><b>Eventually Consistent (false/null):</b> Lower latency, better throughput, lower cost</li>
     * <li><b>Strongly Consistent (true):</b> Guaranteed latest data, higher resource consumption</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> key = Map.of("accountId", AttributeValue.builder().s("ACC-456").build());
     * 
     * // Use strong consistency for financial data
     * CompletableFuture<Map<String, Object>> accountFuture =
     *     executor.getItem("Accounts", key, true);
     *
     * accountFuture.orTimeout(5, TimeUnit.SECONDS)
     *              .thenAccept(account -> {
     *                  if (account != null) {
     *                      processAccount(account);
     *                  }
     *              })
     *              .exceptionally(ex -> {
     *                  if (ex instanceof TimeoutException) {
     *                      logger.warn("Account lookup timed out");
     *                  }
     *                  return null;
     *              });
     * }</pre>
     * 
     * @param tableName the name of the table to get the item from. Must not be null or empty.
     * @param key the primary key of the item to retrieve. Must include all key attributes. Must not be null.
     * @param consistentRead whether to perform a consistent read (true) or eventually consistent read (false/null)
     * @return a CompletableFuture containing the item as a Map of attribute names to values,
     *         or null if not found
     * @throws IllegalArgumentException if tableName is null/empty or key is null
     */
    public CompletableFuture<Map<String, Object>> getItem(final String tableName, final Map<String, AttributeValue> key, final Boolean consistentRead) {
        return getItem(tableName, key, consistentRead, Clazz.PROPS_MAP);
    }

    /**
     * Asynchronously retrieves an item using a GetItemRequest.
     * 
     * <p>This method provides complete control over the get operation, allowing you to specify
     * all parameters including projection expressions, return consumed capacity, and more.
     * This is the most flexible way to retrieve items from DynamoDB asynchronously.</p>
     * 
     * <p><b>Advanced Features Available:</b></p>
     * <ul>
     * <li>Projection expressions for retrieving specific attributes</li>
     * <li>Expression attribute names for reserved words</li>
     * <li>Return consumed capacity for monitoring</li>
     * <li>Consistent read configuration</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * GetItemRequest request = GetItemRequest.builder()
     *     .tableName("Products")
     *     .key(Map.of("productId", AttributeValue.builder().s("PROD-789").build()))
     *     .projectionExpression("productName, price, inStock")
     *     .consistentRead(true)
     *     .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
     *     .build();
     * 
     * CompletableFuture<Map<String, Object>> productFuture = executor.getItem(request);
     *
     * productFuture.thenAccept(item -> {
     *     if (item != null) {
     *         System.out.println("Product: " + item.get("productName"));
     *         System.out.println("Price: " + item.get("price"));
     *     }
     * });
     * }</pre>
     * 
     * @param getItemRequest the complete GetItemRequest with all parameters configured. Must not be null.
     * @return a CompletableFuture containing the item as a Map of attribute names to values,
     *         or null if not found
     * @throws IllegalArgumentException if getItemRequest is null
     */
    public CompletableFuture<Map<String, Object>> getItem(final GetItemRequest getItemRequest) {
        return getItem(getItemRequest, Clazz.PROPS_MAP);
    }

    /**
     * Asynchronously retrieves a single item from DynamoDB table and converts it to the specified type.
     * 
     * <p>This method performs an async GetItem operation with eventually consistent reads by default.
     * The returned CompletableFuture will complete with the converted item when the operation succeeds,
     * or complete exceptionally if the operation fails.</p>
     * 
     * <p><b>Async Operation Benefits:</b></p>
     * <ul>
     * <li>Non-blocking - doesn't tie up calling thread</li>
     * <li>Composable - can chain with other async operations</li>
     * <li>Efficient - uses NIO-based networking</li>
     * <li>Scalable - supports high concurrency</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CompletableFuture<User> userFuture = executor.getItem("Users", key, User.class);
     * 
     * userFuture.thenApply(user -> {
     *     // Transform the user
     *     return user.getName().toUpperCase();
     * }).thenAccept(name -> {
     *     System.out.println("User name: " + name);
     * }).exceptionally(ex -> {
     *     logger.error("Failed to get user", ex);
     *     return null;
     * });
     * }</pre>
     * 
     * @param <T> the target type for conversion
     * @param tableName the name of the DynamoDB table. Must not be null or empty.
     * @param key the primary key of the item to retrieve. Must not be null or empty.
     * @param targetClass the class to convert the result to. Must not be null.
     * @return a CompletableFuture that completes with the converted item, or null if item doesn't exist
     * @throws IllegalArgumentException if any parameter is null, tableName is empty, or targetClass is unsupported
     * @see DynamoDbAsyncClient#getItem(GetItemRequest)
     * @see <a href="https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html">GetItem API Reference</a>
     */
    public <T> CompletableFuture<T> getItem(final String tableName, final Map<String, AttributeValue> key, final Class<T> targetClass) {
        final GetItemRequest getItemRequest = GetItemRequest.builder().tableName(tableName).key(key).build();

        return getItem(getItemRequest, targetClass);
    }

    /**
     * Asynchronously retrieves a single item with specified read consistency and converts it to the target type.
     * 
     * <p>This method provides full control over read consistency while maintaining the benefits of asynchronous
     * execution. Strongly consistent reads ensure the most up-to-date data but consume more read capacity
     * and may have slightly higher latency.</p>
     * 
     * <p><b>Read Consistency Impact on Async Operations:</b></p>
     * <ul>
     * <li><b>Eventually Consistent:</b> Lower latency, better throughput, lower cost</li>
     * <li><b>Strongly Consistent:</b> Guaranteed latest data, higher resource consumption</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Strong consistency for critical reads
     * CompletableFuture<User> userFuture =
     *     executor.getItem("Users", key, true, User.class);
     *
     * userFuture.orTimeout(5, TimeUnit.SECONDS)
     *           .thenAccept(user -> processUser(user))
     *           .exceptionally(ex -> {
     *               if (ex instanceof TimeoutException) {
     *                   logger.warn("Get item timed out");
     *               }
     *               return null;
     *           });
     * }</pre>
     * 
     * @param <T> the target type for conversion
     * @param tableName the name of the DynamoDB table. Must not be null or empty.
     * @param key the primary key of the item to retrieve. Must not be null or empty.
     * @param consistentRead true for strongly consistent reads, false/null for eventually consistent reads
     * @param targetClass the class to convert the result to. Must not be null.
     * @return a CompletableFuture that completes with the converted item, or null if item doesn't exist
     * @throws IllegalArgumentException if any parameter is null, tableName is empty, or targetClass is unsupported
     * @see DynamoDbAsyncClient#getItem(GetItemRequest)
     * @see <a href="https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html">GetItem API Reference</a>
     */
    public <T> CompletableFuture<T> getItem(final String tableName, final Map<String, AttributeValue> key, final Boolean consistentRead,
            final Class<T> targetClass) {
        final GetItemRequest getItemRequest = GetItemRequest.builder().tableName(tableName).key(key).consistentRead(consistentRead).build();

        return getItem(getItemRequest, targetClass);
    }

    /**
     * Asynchronously retrieves an item using a GetItemRequest and converts it to the target type.
     * 
     * <p>This method provides the most flexibility by combining full request control with
     * automatic type conversion. You can use all DynamoDB features while maintaining type safety
     * and asynchronous execution benefits.</p>
     * 
     * <p><b>Advanced Usage Benefits:</b></p>
     * <ul>
     * <li>Complete control over DynamoDB request parameters</li>
     * <li>Automatic type conversion with compile-time safety</li>
     * <li>Non-blocking execution with CompletableFuture</li>
     * <li>Support for all DynamoDB v2 SDK features</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * public class ProductSummary {
     *     private String productId;
     *     private String name;
     *     private Boolean inStock;
     *     // getters and setters...
     * }
     * 
     * GetItemRequest request = GetItemRequest.builder()
     *     .tableName("Products")
     *     .key(Map.of("productId", AttributeValue.builder().s("PROD-123").build()))
     *     .projectionExpression("productId, productName, inStock")
     *     .expressionAttributeNames(Map.of("#name", "productName"))
     *     .consistentRead(false)
     *     .build();
     * 
     * CompletableFuture<ProductSummary> productFuture = 
     *     executor.getItem(request, ProductSummary.class);
     * 
     * productFuture.thenCompose(product -> {
     *     if (product != null && product.getInStock()) {
     *         return processAvailableProduct(product);
     *     } else {
     *         return CompletableFuture.completedFuture(null);
     *     }
     * });
     * }</pre>
     * 
     * @param <T> the type to convert the item to
     * @param getItemRequest the complete GetItemRequest with all parameters configured. Must not be null.
     * @param targetClass the class to convert the item to. Must have a default constructor. Must not be null.
     * @return a CompletableFuture containing the item converted to type T, or null if not found
     * @throws IllegalArgumentException if getItemRequest or targetClass is null
     */
    public <T> CompletableFuture<T> getItem(final GetItemRequest getItemRequest, final Class<T> targetClass) {
        return dynamoDBClient.getItem(getItemRequest).thenApply(getItemResponse -> readRow(getItemResponse, targetClass));
    }

    /**
     * Asynchronously performs a batch get operation to retrieve multiple items from multiple tables.
     * 
     * <p>This method can retrieve up to 100 items in a single async call, with a maximum total size
     * of 16 MB. If any requested items are not found, they will simply be omitted from the
     * results. The operation performs eventually consistent reads by default and returns a
     * CompletableFuture for non-blocking execution.</p>
     * 
     * <p><b>Batch Operation Benefits:</b></p>
     * <ul>
     * <li>Retrieves multiple items in a single network round-trip</li>
     * <li>Non-blocking execution reduces thread pool pressure</li>
     * <li>Automatic handling of unprocessed items through pagination</li>
     * <li>Cost-efficient compared to individual GetItem calls</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * KeysAndAttributes userKeys = KeysAndAttributes.builder()
     *     .keys(Arrays.asList(
     *         Map.of("userId", AttributeValue.builder().s("user1").build()),
     *         Map.of("userId", AttributeValue.builder().s("user2").build())
     *     ))
     *     .projectionExpression("userId, name, email")
     *     .build();
     * 
     * Map<String, KeysAndAttributes> requestItems = Map.of("Users", userKeys);
     * 
     * CompletableFuture<Map<String, List<Map<String, Object>>>> batchFuture =
     *     executor.batchGetItem(requestItems);
     *
     * batchFuture.thenAccept(results -> {
     *     List<Map<String, Object>> users = results.get("Users");
     *     System.out.println("Retrieved " + users.size() + " users");
     *     users.forEach(user -> System.out.println(user.get("name")));
     * }).exceptionally(ex -> {
     *     logger.error("Batch get failed", ex);
     *     return null;
     * });
     * }</pre>
     * 
     * @param requestItems a map where keys are table names and values are KeysAndAttributes
     *                    objects specifying the items to retrieve from each table. Must not be null.
     * @return a CompletableFuture containing a map of table names to lists of retrieved items,
     *         where each item is represented as a Map of attribute names to values
     * @throws IllegalArgumentException if requestItems is null or exceeds batch limits
     * @see #batchGetItem(Map, String) to include consumed capacity information
     */
    public CompletableFuture<Map<String, List<Map<String, Object>>>> batchGetItem(final Map<String, KeysAndAttributes> requestItems) {
        return batchGetItem(requestItems, Clazz.PROPS_MAP);
    }

    /**
     * Asynchronously performs a batch get operation with consumed capacity reporting.
     * 
     * <p>This method extends the basic batch get by allowing you to track the read capacity
     * consumed by the operation asynchronously. This is useful for monitoring and optimizing
     * your DynamoDB usage and costs in high-throughput async applications.</p>
     * 
     * <p><b>Capacity Monitoring Benefits:</b></p>
     * <ul>
     * <li>Real-time capacity consumption tracking for cost optimization</li>
     * <li>Performance monitoring for auto-scaling decisions</li>
     * <li>Debugging high-consumption operations</li>
     * <li>Compliance with capacity budget constraints</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * KeysAndAttributes productKeys = KeysAndAttributes.builder()
     *     .keys(Arrays.asList(
     *         Map.of("productId", AttributeValue.builder().s("PROD-1").build()),
     *         Map.of("productId", AttributeValue.builder().s("PROD-2").build())
     *     ))
     *     .consistentRead(true) // Higher capacity consumption
     *     .build();
     * 
     * Map<String, KeysAndAttributes> requestItems = Map.of("Products", productKeys);
     * 
     * CompletableFuture<Map<String, List<Map<String, Object>>>> future =
     *     executor.batchGetItem(requestItems, "TOTAL");
     *
     * future.thenAccept(results -> {
     *     System.out.println("Retrieved " + results.get("Products").size() + " products");
     *     // Note: Consumed capacity info available in underlying response metadata
     * });
     * }</pre>
     * 
     * @param requestItems a map of table names to KeysAndAttributes specifying the items to retrieve.
     *                    Must not be null.
     * @param returnConsumedCapacity determines the level of detail about consumed capacity returned:
     *                              "INDEXES" - returns capacity for table and indexes,
     *                              "TOTAL" - returns only total consumed capacity,
     *                              "NONE" - no capacity details returned
     * @return a CompletableFuture containing a map of table names to lists of retrieved items
     * @throws IllegalArgumentException if requestItems is null
     */
    public CompletableFuture<Map<String, List<Map<String, Object>>>> batchGetItem(final Map<String, KeysAndAttributes> requestItems,
            final String returnConsumedCapacity) {
        return batchGetItem(requestItems, returnConsumedCapacity, Clazz.PROPS_MAP);
    }

    /**
     * Asynchronously performs a batch get operation using a BatchGetItemRequest.
     * 
     * <p>This method provides complete control over the batch get operation, allowing you
     * to specify all parameters including projection expressions, consistent reads per table,
     * and return consumed capacity settings. All operations execute asynchronously with
     * CompletableFuture support.</p>
     * 
     * <p><b>Advanced Configuration Options:</b></p>
     * <ul>
     * <li>Per-table projection expressions and attribute filtering</li>
     * <li>Mixed consistency requirements across different tables</li>
     * <li>Detailed capacity consumption reporting</li>
     * <li>Custom retry and timeout configurations</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * BatchGetItemRequest request = BatchGetItemRequest.builder()
     *     .requestItems(Map.of(
     *         "Orders", KeysAndAttributes.builder()
     *             .keys(Arrays.asList(
     *                 Map.of("orderId", AttributeValue.builder().s("ORD-001").build()),
     *                 Map.of("orderId", AttributeValue.builder().s("ORD-002").build())
     *             ))
     *             .projectionExpression("orderId, customerId, total, status")
     *             .consistentRead(true)
     *             .build(),
     *         "OrderItems", KeysAndAttributes.builder()
     *             .keys(Arrays.asList(
     *                 Map.of("orderId", AttributeValue.builder().s("ORD-001").build(),
     *                       "itemId", AttributeValue.builder().s("ITEM-A").build())
     *             ))
     *             .build()
     *     ))
     *     .returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
     *     .build();
     * 
     * CompletableFuture<Map<String, List<Map<String, Object>>>> future =
     *     executor.batchGetItem(request);
     *
     * future.thenAccept(results -> {
     *     results.forEach((table, items) -> {
     *         System.out.println(table + ": " + items.size() + " items");
     *     });
     * });
     * }</pre>
     * 
     * @param batchGetItemRequest the complete BatchGetItemRequest with all parameters configured.
     *                           Must not be null.
     * @return a CompletableFuture containing a map of table names to lists of retrieved items
     * @throws IllegalArgumentException if batchGetItemRequest is null or exceeds batch limits
     */
    public CompletableFuture<Map<String, List<Map<String, Object>>>> batchGetItem(final BatchGetItemRequest batchGetItemRequest) {
        return batchGetItem(batchGetItemRequest, Clazz.PROPS_MAP);
    }

    /**
     * Asynchronously retrieves multiple items from DynamoDB in a batch operation.
     * 
     * <p>This method performs a batch get operation to retrieve items from one or more tables
     * efficiently in a single request. The items are converted to the specified target class type.
     * This is more efficient than multiple individual getItem calls.</p>
     * 
     * <p><b>Batch Limits:</b></p>
     * <ul>
     * <li>Maximum 100 items per request across all tables</li>
     * <li>Maximum 16 MB total response size</li>
     * <li>Eventually consistent reads only</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, KeysAndAttributes> requestItems = new HashMap<>();
     * List<Map<String, AttributeValue>> keys = Arrays.asList(
     *     Map.of("userId", AttributeValue.builder().s("user1").build()),
     *     Map.of("userId", AttributeValue.builder().s("user2").build())
     * );
     * requestItems.put("Users", KeysAndAttributes.builder().keys(keys).build());
     * 
     * executor.batchGetItem(requestItems, User.class)
     *     .thenAccept(results -> {
     *         List<User> users = results.get("Users");
     *         System.out.println("Retrieved " + users.size() + " users");
     *     });
     * }</pre>
     * 
     * @param <T> the type of objects to return
     * @param requestItems map of table names to keys and attributes to retrieve. Must not be null.
     * @param targetClass the class to convert results to. Must not be null.
     * @return a CompletableFuture containing a map of table names to lists of retrieved items
     * @throws IllegalArgumentException if requestItems or targetClass is null
     */
    public <T> CompletableFuture<Map<String, List<T>>> batchGetItem(final Map<String, KeysAndAttributes> requestItems, final Class<T> targetClass) {
        final BatchGetItemRequest batchGetItemRequest = BatchGetItemRequest.builder().requestItems(requestItems).build();

        return batchGetItem(batchGetItemRequest, targetClass);
    }

    /**
     * Asynchronously retrieves multiple items with consumed capacity reporting.
     * 
     * <p>This method is similar to {@link #batchGetItem(Map, Class)} but includes
     * information about the read capacity consumed by the operation. This is useful
     * for monitoring and optimizing DynamoDB costs and performance.</p>
     * 
     * <p><b>Consumed Capacity Options:</b></p>
     * <ul>
     * <li><b>NONE</b> - No consumed capacity info returned (default)</li>
     * <li><b>TOTAL</b> - Returns total consumed capacity</li>
     * <li><b>INDEXES</b> - Returns consumed capacity for each table and index</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, KeysAndAttributes> requestItems = createBatchKeys(userIds);
     * 
     * executor.batchGetItem(requestItems, "TOTAL", User.class)
     *     .thenAccept(results -> {
     *         List<User> users = results.get("Users");
     *         System.out.println("Retrieved " + users.size() + " users");
     *         // Response includes consumed capacity information
     *     });
     * }</pre>
     * 
     * @param <T> the type of objects to return
     * @param requestItems map of table names to keys and attributes to retrieve. Must not be null.
     * @param returnConsumedCapacity specifies consumed capacity detail level: "NONE", "TOTAL", or "INDEXES"
     * @param targetClass the class to convert results to. Must not be null.
     * @return a CompletableFuture containing a map of table names to lists of retrieved items
     * @throws IllegalArgumentException if requestItems or targetClass is null
     */
    public <T> CompletableFuture<Map<String, List<T>>> batchGetItem(final Map<String, KeysAndAttributes> requestItems, final String returnConsumedCapacity,
            final Class<T> targetClass) {
        final BatchGetItemRequest batchGetItemRequest = BatchGetItemRequest.builder()
                .requestItems(requestItems)
                .returnConsumedCapacity(returnConsumedCapacity)
                .build();

        return batchGetItem(batchGetItemRequest, targetClass);
    }

    /**
     * Asynchronously retrieves multiple items using a custom BatchGetItemRequest.
     * 
     * <p>This method provides full control over the batch get operation, allowing
     * specification of all DynamoDB batch get parameters including projection expressions,
     * consistency settings, and consumed capacity reporting across multiple tables.</p>
     * 
     * <p><b>Advanced Features:</b></p>
     * <ul>
     * <li>Projection expressions to retrieve specific attributes</li>
     * <li>Consistent read options per table</li>
     * <li>Expression attribute names for reserved words</li>
     * <li>Consumed capacity and metrics reporting</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, KeysAndAttributes> requestItems = new HashMap<>();
     * requestItems.put("Users", KeysAndAttributes.builder()
     *     .keys(userKeys)
     *     .projectionExpression("userId, email, #s")
     *     .expressionAttributeNames(Map.of("#s", "status"))
     *     .consistentRead(true)
     *     .build());
     * 
     * BatchGetItemRequest request = BatchGetItemRequest.builder()
     *     .requestItems(requestItems)
     *     .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
     *     .build();
     * 
     * executor.batchGetItem(request, User.class)
     *     .thenAccept(results -> processResults(results));
     * }</pre>
     * 
     * @param <T> the type of objects to return
     * @param batchGetItemRequest the complete BatchGetItemRequest. Must not be null.
     * @param targetClass the class to convert results to. Must not be null.
     * @return a CompletableFuture containing a map of table names to lists of retrieved items
     * @throws IllegalArgumentException if batchGetItemRequest or targetClass is null
     */
    public <T> CompletableFuture<Map<String, List<T>>> batchGetItem(final BatchGetItemRequest batchGetItemRequest, final Class<T> targetClass) {
        return dynamoDBClient.batchGetItem(batchGetItemRequest).thenApply(batchGetItemResponse -> toEntities(batchGetItemResponse, targetClass));
    }

    /**
     * Asynchronously puts an item into the specified DynamoDB table using AWS SDK v2.
     * 
     * <p>This method creates a new item or replaces an existing item with the same primary key
     * asynchronously. The operation uses AWS SDK v2's non-blocking async client for improved
     * performance and resource utilization. By default, no information about the previous
     * item is returned.</p>
     * 
     * <p><b>Async Operation Benefits:</b></p>
     * <ul>
     * <li>Non-blocking execution frees up calling thread</li>
     * <li>Better resource utilization in high-concurrency scenarios</li>
     * <li>Composable with other async operations using CompletableFuture</li>
     * <li>Enhanced error handling with exception propagation</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> item = new HashMap<>();
     * item.put("userId", AttributeValue.fromS("user123"));
     * item.put("name", AttributeValue.fromS("John Doe"));
     * item.put("email", AttributeValue.fromS("john@example.com"));
     * item.put("createdAt", AttributeValue.fromN(String.valueOf(Instant.now().toEpochMilli())));
     * 
     * CompletableFuture<PutItemResponse> future = executor.putItem("Users", item);
     *
     * future.thenAccept(response -> {
     *         System.out.println("Item saved successfully");
     *         System.out.println("Consumed capacity: " + response.consumedCapacity());
     *     })
     *     .exceptionally(ex -> {
     *         if (ex.getCause() instanceof ConditionalCheckFailedException) {
     *             System.err.println("Item already exists");
     *         } else {
     *             logger.error("Failed to save item", ex);
     *         }
     *         return null;
     *     });
     * }</pre>
     * 
     * @param tableName the name of the DynamoDB table to put the item into. Must not be null.
     * @param item the item to put, represented as a map of attribute names to AttributeValue objects.
     *            Must include all required attributes. Must not be null.
     * @return a CompletableFuture containing the PutItemResponse with operation metadata
     * @throws IllegalArgumentException if tableName or item is null
     * @see #putItem(String, Map, String) to retrieve old item values
     * @see PutItemResponse
     */
    public CompletableFuture<PutItemResponse> putItem(final String tableName, final Map<String, AttributeValue> item) {
        final PutItemRequest putItemRequest = PutItemRequest.builder().tableName(tableName).item(item).build();

        return putItem(putItemRequest);
    }

    /**
     * Asynchronously puts an item into DynamoDB table with return value specification using AWS SDK v2.
     * 
     * <p>This method creates a new item or replaces an existing item while allowing you to specify
     * what values should be returned after the operation completes. This is useful for retrieving
     * the old item values or confirming the operation success with specific attributes.</p>
     * 
     * <p><b>Return Value Options:</b></p>
     * <ul>
     * <li><b>NONE</b> - Nothing is returned (default, best performance)</li>
     * <li><b>ALL_OLD</b> - Returns all attributes of the old item, if it existed</li>
     * <li><b>UPDATED_OLD</b> - Returns only updated attributes of the old item</li>
     * <li><b>ALL_NEW</b> - Returns all attributes of the new item</li>
     * <li><b>UPDATED_NEW</b> - Returns only updated attributes of the new item</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> newItem = Map.of(
     *     "userId", AttributeValue.fromS("user123"),
     *     "name", AttributeValue.fromS("John Updated"),
     *     "version", AttributeValue.fromN("2")
     * );
     * 
     * CompletableFuture<PutItemResponse> future =
     *     executor.putItem("Users", newItem, "ALL_OLD");
     *
     * future.thenAccept(response -> {
     *         Map<String, AttributeValue> oldAttributes = response.attributes();
     *         if (oldAttributes != null && !oldAttributes.isEmpty()) {
     *             System.out.println("Previous name: " + oldAttributes.get("name").s());
     *         } else {
     *             System.out.println("Item was newly created");
     *         }
     *     })
     *     .exceptionally(ex -> {
     *         logger.error("Put item failed", ex);
     *         return null;
     *     });
     * }</pre>
     * 
     * @param tableName the name of the DynamoDB table to put the item into. Must not be null.
     * @param item the item to put, as a map of attribute names to AttributeValue objects. Must not be null.
     * @param returnValues specifies what values to return: "NONE", "ALL_OLD", "UPDATED_OLD", "ALL_NEW", "UPDATED_NEW"
     * @return a CompletableFuture containing the PutItemResponse with requested return values
     * @throws IllegalArgumentException if tableName or item is null
     * @see #putItem(String, Map)
     */
    public CompletableFuture<PutItemResponse> putItem(final String tableName, final Map<String, AttributeValue> item, final String returnValues) {
        final PutItemRequest putItemRequest = PutItemRequest.builder().tableName(tableName).item(item).returnValues(returnValues).build();

        return putItem(putItemRequest);
    }

    /**
     * Asynchronously puts an item using a complete PutItemRequest with AWS SDK v2.
     * 
     * <p>This method provides maximum flexibility by accepting a fully configured PutItemRequest.
     * You can specify all DynamoDB PutItem parameters including condition expressions,
     * expression attribute names/values, return value specifications, and capacity monitoring.</p>
     * 
     * <p><b>Advanced Features Available:</b></p>
     * <ul>
     * <li>Conditional puts with condition expressions</li>
     * <li>Expression attribute names for reserved words</li>
     * <li>Expression attribute values for dynamic conditions</li>
     * <li>Return consumed capacity for monitoring</li>
     * <li>Return item collection metrics</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * PutItemRequest request = PutItemRequest.builder()
     *     .tableName("Users")
     *     .item(Map.of(
     *         "userId", AttributeValue.fromS("user123"),
     *         "email", AttributeValue.fromS("user@example.com"),
     *         "version", AttributeValue.fromN("1")
     *     ))
     *     .conditionExpression("attribute_not_exists(userId)")  // Only create if new
     *     .returnValues(ReturnValue.ALL_OLD)
     *     .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
     *     .build();
     * 
     * CompletableFuture<PutItemResponse> future = executor.putItem(request);
     *
     * future.thenAccept(response -> {
     *         System.out.println("Consumed capacity: " + response.consumedCapacity().capacityUnits());
     *         if (response.attributes() != null) {
     *             System.out.println("Replaced existing item");
     *         }
     *     })
     *     .exceptionally(ex -> {
     *         if (ex.getCause() instanceof ConditionalCheckFailedException) {
     *             System.err.println("User already exists");
     *         }
     *         return null;
     *     });
     * }</pre>
     * 
     * @param putItemRequest the complete PutItemRequest with all parameters configured. Must not be null.
     * @return a CompletableFuture containing the PutItemResponse with operation results
     * @throws IllegalArgumentException if putItemRequest is null
     * @see PutItemRequest
     * @see PutItemResponse
     */
    public CompletableFuture<PutItemResponse> putItem(final PutItemRequest putItemRequest) {
        return dynamoDBClient.putItem(putItemRequest);
    }

    /**
     * Asynchronously puts an entity object into the specified DynamoDB table.
     *
     * <p>This method converts a Java entity object to DynamoDB item format and performs an async
     * put operation. The entity is automatically converted to AttributeValue objects using the
     * executor's object mapping capabilities. This provides a convenient way to store POJOs directly
     * without manual AttributeValue conversion.</p>
     *
     * <p><b>Entity Conversion Process:</b></p>
     * <ul>
     * <li>Bean properties are converted to DynamoDB attributes</li>
     * <li>Null values are typically omitted from the item</li>
     * <li>Collection and complex types are serialized appropriately</li>
     * <li>Naming policies are applied for attribute name conversion</li>
     * <li>Type-specific converters handle Java types to AttributeValue mapping</li>
     * </ul>
     *
     * <p><b>Supported Entity Types:</b></p>
     * <ul>
     * <li>JavaBean objects with getter/setter methods</li>
     * <li>Records and simple data classes</li>
     * <li>Objects with proper constructor and field access</li>
     * <li>Maps and other collection-based objects</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * public class User {
     *     private String userId;
     *     private String name;
     *     private String email;
     *     private Long createdAt;
     *     // getters and setters...
     * }
     *
     * User user = new User();
     * user.setUserId("user123");
     * user.setName("John Doe");
     * user.setEmail("john@example.com");
     * user.setCreatedAt(Instant.now().toEpochMilli());
     *
     * CompletableFuture<PutItemResponse> future = executor.putItem("Users", user);
     *
     * future.thenAccept(response -> {
     *     System.out.println("User saved successfully");
     * }).exceptionally(ex -> {
     *     logger.error("Failed to save user", ex);
     *     return null;
     * });
     * }</pre>
     *
     * @param tableName the name of the DynamoDB table to put the item into. Must not be null or empty.
     * @param entity the entity object to convert and store. Must not be null.
     * @return a CompletableFuture containing the PutItemResponse with operation metadata
     * @throws IllegalArgumentException if tableName is null/empty, entity is null, or entity cannot be converted
     * @see #putItem(String, Object, String) for operations with return values
     * @see #putItem(String, Map) for direct AttributeValue operations
     */
    CompletableFuture<PutItemResponse> putItem(final String tableName, final Object entity) {
        // There is no too much benefit to add method for "Object entity"
        // And it may cause error because the "Object" is ambiguous to any type.
        final PutItemRequest putItemRequest = PutItemRequest.builder().tableName(tableName).item(toItem(entity)).build();

        return putItem(putItemRequest);
    }

    /**
     * Asynchronously puts an entity object into DynamoDB table with return value specification.
     *
     * <p>This method converts a Java entity object to DynamoDB item format and performs an async
     * put operation with the specified return value configuration. The entity is automatically
     * converted to AttributeValue objects using the executor's object mapping capabilities.</p>
     *
     * <p><b>Entity Conversion:</b></p>
     * <ul>
     * <li>Bean properties are converted to DynamoDB attributes</li>
     * <li>Null values are typically omitted from the item</li>
     * <li>Collection and complex types are serialized appropriately</li>
     * <li>Naming policies are applied for attribute name conversion</li>
     * </ul>
     *
     * <p><b>Return Value Options:</b></p>
     * <ul>
     * <li><b>NONE</b> - Nothing is returned (best performance)</li>
     * <li><b>ALL_OLD</b> - Returns all attributes of the old item</li>
     * <li><b>UPDATED_OLD</b> - Returns only updated attributes of the old item</li>
     * <li><b>ALL_NEW</b> - Returns all attributes of the new item</li>
     * <li><b>UPDATED_NEW</b> - Returns only updated attributes of the new item</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User user = new User();
     * user.setUserId("user123");
     * user.setName("John Doe Updated");
     * user.setEmail("john.updated@example.com");
     * user.setVersion(2);
     *
     * CompletableFuture<PutItemResponse> future =
     *     executor.putItem("Users", user, "ALL_OLD");
     *
     * future.thenAccept(response -> {
     *     Map<String, AttributeValue> oldAttributes = response.attributes();
     *     if (oldAttributes != null && !oldAttributes.isEmpty()) {
     *         System.out.println("Updated existing user");
     *     } else {
     *         System.out.println("Created new user");
     *     }
     * }).exceptionally(ex -> {
     *     logger.error("Failed to save user", ex);
     *     return null;
     * });
     * }</pre>
     *
     * @param tableName the name of the DynamoDB table to put the item into. Must not be null or empty.
     * @param entity the entity object to convert and store. Must not be null.
     * @param returnValues specifies what values to return: "NONE", "ALL_OLD", "UPDATED_OLD", "ALL_NEW", "UPDATED_NEW"
     * @return a CompletableFuture containing the PutItemResponse with requested return values
     * @throws IllegalArgumentException if tableName is null/empty, entity is null, or entity cannot be converted
     * @see #putItem(String, Object) for operations without return values
     * @see #putItem(String, Map, String) for direct AttributeValue operations
     */
    CompletableFuture<PutItemResponse> putItem(final String tableName, final Object entity, final String returnValues) {
        final PutItemRequest putItemRequest = PutItemRequest.builder().tableName(tableName).item(toItem(entity)).returnValues(returnValues).build();

        return putItem(putItemRequest);
    }

    /**
     * Asynchronously performs batch write operations on DynamoDB tables.
     * 
     * <p>This method efficiently writes or deletes multiple items across one or more tables
     * in a single request. Each table can have a mix of put and delete operations.
     * Batch writes are atomic at the item level but not at the batch level.</p>
     * 
     * <p><b>Batch Write Limits:</b></p>
     * <ul>
     * <li>Maximum 25 write requests per batch</li>
     * <li>Maximum 16 MB total request size</li>
     * <li>Maximum 400 KB per individual item</li>
     * <li>No conditional expressions supported</li>
     * <li>No return values for individual operations</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, List<WriteRequest>> requestItems = new HashMap<>();
     * List<WriteRequest> userWrites = new ArrayList<>();
     * 
     * // Add put requests
     * userWrites.add(WriteRequest.builder()
     *     .putRequest(PutRequest.builder()
     *         .item(Map.of(
     *             "userId", AttributeValue.fromS("user1"),
     *             "name", AttributeValue.fromS("Alice")
     *         ))
     *         .build())
     *     .build());
     * 
     * // Add delete requests
     * userWrites.add(WriteRequest.builder()
     *     .deleteRequest(DeleteRequest.builder()
     *         .key(Map.of("userId", AttributeValue.fromS("user2")))
     *         .build())
     *     .build());
     * 
     * requestItems.put("Users", userWrites);
     * 
     * executor.batchWriteItem(requestItems)
     *     .thenAccept(response -> {
     *         if (response.unprocessedItems().isEmpty()) {
     *             System.out.println("All items processed successfully");
     *         } else {
     *             System.out.println("Some items were not processed");
     *             // Retry unprocessed items
     *         }
     *     });
     * }</pre>
     * 
     * @param requestItems map of table names to lists of write requests. Must not be null.
     * @return a CompletableFuture containing BatchWriteItemResponse with unprocessed items if any
     * @throws IllegalArgumentException if requestItems is null or exceeds batch limits
     */
    public CompletableFuture<BatchWriteItemResponse> batchWriteItem(final Map<String, List<WriteRequest>> requestItems) {
        final BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder().requestItems(requestItems).build();

        return dynamoDBClient.batchWriteItem(batchWriteItemRequest);
    }

    /**
     * Asynchronously performs batch write operations using a custom BatchWriteItemRequest.
     * 
     * <p>This method provides full control over batch write operations, allowing specification
     * of all parameters including return consumed capacity and metrics. Useful for complex
     * batch operations across multiple tables with detailed monitoring requirements.</p>
     * 
     * <p><b>Advanced Features:</b></p>
     * <ul>
     * <li>Mixed put and delete operations</li>
     * <li>Operations across multiple tables</li>
     * <li>Consumed capacity reporting</li>
     * <li>Item collection metrics for local secondary indexes</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * BatchWriteItemRequest request = BatchWriteItemRequest.builder()
     *     .requestItems(createBatchWriteRequests())
     *     .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
     *     .returnItemCollectionMetrics(ReturnItemCollectionMetrics.SIZE)
     *     .build();
     * 
     * executor.batchWriteItem(request)
     *     .thenAccept(response -> {
     *         System.out.println("Total consumed capacity: " +
     *             response.consumedCapacity().stream()
     *                 .mapToDouble(c -> c.capacityUnits())
     *                 .sum());
     *
     *         if (!response.unprocessedItems().isEmpty()) {
     *             // Retry logic for unprocessed items
     *             retryUnprocessedItems(response.unprocessedItems());
     *         }
     *     });
     * }</pre>
     * 
     * @param batchWriteItemRequest the complete BatchWriteItemRequest. Must not be null.
     * @return a CompletableFuture containing BatchWriteItemResponse with operation results
     * @throws IllegalArgumentException if batchWriteItemRequest is null
     */
    public CompletableFuture<BatchWriteItemResponse> batchWriteItem(final BatchWriteItemRequest batchWriteItemRequest) {
        return dynamoDBClient.batchWriteItem(batchWriteItemRequest);
    }

    /**
     * Asynchronously updates an item in DynamoDB table using attribute updates with AWS SDK v2.
     * 
     * <p>This method modifies an existing item by specifying which attributes to update using
     * AttributeValueUpdate objects. Each update can use different actions (PUT, ADD, DELETE)
     * to modify attributes in various ways. If the item doesn't exist, the operation will fail.</p>
     * 
     * <p><b>Update Actions:</b></p>
     * <ul>
     * <li><b>PUT</b> - Set attribute to new value (replace existing)</li>
     * <li><b>ADD</b> - Add to numeric values or add elements to sets</li>
     * <li><b>DELETE</b> - Remove attribute or remove elements from sets</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> key = asKey("userId", "user123");
     * 
     * Map<String, AttributeValueUpdate> updates = new HashMap<>();
     * updates.put("loginCount", AttributeValueUpdate.builder()
     *     .value(AttributeValue.fromN("1"))
     *     .action(AttributeAction.ADD)
     *     .build());
     * updates.put("lastLogin", AttributeValueUpdate.builder()
     *     .value(AttributeValue.fromS(Instant.now().toString()))
     *     .action(AttributeAction.PUT)
     *     .build());
     * 
     * CompletableFuture<UpdateItemResponse> future =
     *     executor.updateItem("Users", key, updates);
     *
     * future.thenAccept(response -> {
     *         System.out.println("Item updated successfully");
     *     })
     *     .exceptionally(ex -> {
     *         logger.error("Update failed", ex);
     *         return null;
     *     });
     * }</pre>
     * 
     * @param tableName the name of the DynamoDB table containing the item to update. Must not be null.
     * @param key the primary key of the item to update, must include all key attributes. Must not be null.
     * @param attributeUpdates a map of attribute names to AttributeValueUpdate objects specifying the updates. Must not be null.
     * @return a CompletableFuture containing the UpdateItemResponse with operation metadata
     * @throws IllegalArgumentException if tableName, key, or attributeUpdates is null
     * @see AttributeValueUpdate
     */
    public CompletableFuture<UpdateItemResponse> updateItem(final String tableName, final Map<String, AttributeValue> key,
            final Map<String, AttributeValueUpdate> attributeUpdates) {
        final UpdateItemRequest updateItemRequest = UpdateItemRequest.builder().tableName(tableName).key(key).attributeUpdates(attributeUpdates).build();

        return updateItem(updateItemRequest);
    }

    /**
     * Asynchronously updates an item with return value specification.
     * 
     * <p>This method is similar to {@link #updateItem(String, Map, Map)} but allows
     * specifying which values to return after the update. This is useful for retrieving
     * the updated values or the old values for audit purposes.</p>
     * 
     * <p><b>Return Value Options:</b></p>
     * <ul>
     * <li><b>NONE</b> - No values returned (default)</li>
     * <li><b>ALL_OLD</b> - All attributes before the update</li>
     * <li><b>UPDATED_OLD</b> - Only updated attributes before the update</li>
     * <li><b>ALL_NEW</b> - All attributes after the update</li>
     * <li><b>UPDATED_NEW</b> - Only updated attributes after the update</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> key = asKey("userId", "user123");
     * Map<String, AttributeValueUpdate> updates = createUpdates();
     * 
     * executor.updateItem("Users", key, updates, "ALL_NEW")
     *     .thenAccept(response -> {
     *         Map<String, AttributeValue> newAttributes = response.attributes();
     *         System.out.println("Updated user: " + newAttributes);
     *     });
     * }</pre>
     * 
     * @param tableName the name of the DynamoDB table. Must not be null.
     * @param key the primary key of the item to update. Must not be null.
     * @param attributeUpdates map of updates to apply. Must not be null.
     * @param returnValues specifies which values to return
     * @return a CompletableFuture containing UpdateItemResponse with requested values
     * @throws IllegalArgumentException if any parameter is null
     */
    public CompletableFuture<UpdateItemResponse> updateItem(final String tableName, final Map<String, AttributeValue> key,
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
     * Asynchronously updates an item using a complete UpdateItemRequest with AWS SDK v2.
     * 
     * <p>This method provides maximum flexibility for update operations by accepting a fully
     * configured UpdateItemRequest. You can use modern expression-based updates, conditional
     * updates, and access all advanced DynamoDB update features.</p>
     * 
     * <p><b>Advanced Update Features:</b></p>
     * <ul>
     * <li>Update expressions for modern, efficient updates</li>
     * <li>Condition expressions for conditional updates</li>
     * <li>Expression attribute names and values</li>
     * <li>Atomic counters and set operations</li>
     * <li>Return value specifications</li>
     * <li>Capacity consumption monitoring</li>
     * </ul>
     * 
     * <p><b>Usage Example with Update Expressions:</b></p>
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * UpdateItemRequest request = UpdateItemRequest.builder()
     *     .tableName("Users")
     *     .key(asKey("userId", "user123"))
     *     .updateExpression("SET #name = :name, #updated = :time ADD #loginCount :inc")
     *     .conditionExpression("attribute_exists(userId)")  // Only update if exists
     *     .expressionAttributeNames(Map.of(
     *         "#name", "userName",
     *         "#updated", "lastUpdated",
     *         "#loginCount", "loginCount"
     *     ))
     *     .expressionAttributeValues(Map.of(
     *         ":name", AttributeValue.fromS("John Updated"),
     *         ":time", AttributeValue.fromN(String.valueOf(Instant.now().toEpochMilli())),
     *         ":inc", AttributeValue.fromN("1")
     *     ))
     *     .returnValues(ReturnValue.UPDATED_NEW)
     *     .build();
     * 
     * CompletableFuture<UpdateItemResponse> future = executor.updateItem(request);
     * 
     * future.thenAccept(response -> {
     *         Map<String, AttributeValue> updatedAttrs = response.attributes();
     *         System.out.println("Updated attributes: " + updatedAttrs);
     *     })
     *     .exceptionally(ex -> {
     *         if (ex.getCause() instanceof ConditionalCheckFailedException) {
     *             System.err.println("Item doesn't exist for update");
     *         }
     *         return null;
     *     });
     * }</pre>
     * 
     * @param updateItemRequest the complete UpdateItemRequest with all parameters configured. Must not be null.
     * @return a CompletableFuture containing the UpdateItemResponse with operation results
     * @throws IllegalArgumentException if updateItemRequest is null
     * @see UpdateItemRequest
     * @see UpdateItemResponse
     */
    public CompletableFuture<UpdateItemResponse> updateItem(final UpdateItemRequest updateItemRequest) {
        return dynamoDBClient.updateItem(updateItemRequest);
    }

    /**
     * Asynchronously deletes an item from DynamoDB table.
     * 
     * <p>This method removes an item from the specified table using its primary key.
     * The operation is idempotent - deleting a non-existent item doesn't cause an error.
     * By default, no information about the deleted item is returned.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> key = Map.of(
     *     "userId", AttributeValue.fromS("user123")
     * );
     * 
     * executor.deleteItem("Users", key)
     *     .thenAccept(response -> {
     *         System.out.println("Item deleted successfully");
     *     })
     *     .exceptionally(ex -> {
     *         logger.error("Delete failed", ex);
     *         return null;
     *     });
     * }</pre>
     * 
     * @param tableName the name of the DynamoDB table. Must not be null.
     * @param key the primary key of the item to delete. Must include all key attributes. Must not be null.
     * @return a CompletableFuture containing the DeleteItemResponse with operation metadata
     * @throws IllegalArgumentException if tableName or key is null
     */
    public CompletableFuture<DeleteItemResponse> deleteItem(final String tableName, final Map<String, AttributeValue> key) {
        DeleteItemRequest deleteItemRequest = DeleteItemRequest.builder().tableName(tableName).key(key).build();

        return deleteItem(deleteItemRequest);
    }

    /**
     * Asynchronously deletes an item with return value specification.
     * 
     * <p>This method deletes an item and optionally returns the attributes of the deleted item.
     * This is useful for audit trails or confirming what was actually deleted.</p>
     * 
     * <p><b>Return Value Options:</b></p>
     * <ul>
     * <li><b>NONE</b> - No values returned (default)</li>
     * <li><b>ALL_OLD</b> - All attributes of the deleted item</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> key = asKey("userId", "user123");
     * 
     * executor.deleteItem("Users", key, "ALL_OLD")
     *     .thenAccept(response -> {
     *         Map<String, AttributeValue> deletedItem = response.attributes();
     *         if (deletedItem != null && !deletedItem.isEmpty()) {
     *             System.out.println("Deleted user: " + deletedItem.get("name").s());
     *             archiveDeletedUser(deletedItem);
     *         } else {
     *             System.out.println("Item didn't exist");
     *         }
     *     });
     * }</pre>
     * 
     * @param tableName the name of the DynamoDB table. Must not be null.
     * @param key the primary key of the item to delete. Must not be null.
     * @param returnValues specifies whether to return the deleted item: "NONE" or "ALL_OLD"
     * @return a CompletableFuture containing DeleteItemResponse with requested values
     * @throws IllegalArgumentException if tableName or key is null
     */
    public CompletableFuture<DeleteItemResponse> deleteItem(final String tableName, final Map<String, AttributeValue> key, final String returnValues) {
        final DeleteItemRequest deleteItemRequest = DeleteItemRequest.builder().tableName(tableName).key(key).returnValues(returnValues).build();

        return deleteItem(deleteItemRequest);
    }

    /**
     * Asynchronously deletes an item using a custom DeleteItemRequest.
     * 
     * <p>This method provides full control over the delete operation, allowing specification
     * of conditional expressions, return values, and consumed capacity reporting.</p>
     * 
     * <p><b>Advanced Features:</b></p>
     * <ul>
     * <li>Conditional deletes with condition expressions</li>
     * <li>Return deleted item attributes</li>
     * <li>Consumed capacity reporting</li>
     * <li>Expression attribute names and values</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DeleteItemRequest request = DeleteItemRequest.builder()
     *     .tableName("Users")
     *     .key(asKey("userId", "user123"))
     *     .conditionExpression("attribute_exists(userId) AND #status = :inactive")
     *     .expressionAttributeNames(Map.of("#status", "status"))
     *     .expressionAttributeValues(Map.of(
     *         ":inactive", AttributeValue.fromS("INACTIVE")
     *     ))
     *     .returnValues(ReturnValue.ALL_OLD)
     *     .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
     *     .build();
     * 
     * executor.deleteItem(request)
     *     .thenAccept(response -> {
     *         System.out.println("Deleted inactive user");
     *         System.out.println("Consumed capacity: " + response.consumedCapacity());
     *     })
     *     .exceptionally(ex -> {
     *         if (ex.getCause() instanceof ConditionalCheckFailedException) {
     *             System.err.println("User is not inactive or doesn't exist");
     *         }
     *         return null;
     *     });
     * }</pre>
     * 
     * @param deleteItemRequest the complete DeleteItemRequest. Must not be null.
     * @return a CompletableFuture containing DeleteItemResponse with operation results
     * @throws IllegalArgumentException if deleteItemRequest is null
     */
    public CompletableFuture<DeleteItemResponse> deleteItem(final DeleteItemRequest deleteItemRequest) {
        return dynamoDBClient.deleteItem(deleteItemRequest);
    }

    /**
     * Asynchronously executes a query and returns all matching items as a list of Maps.
     * 
     * <p>This method performs a Query operation that retrieves items with the same partition key
     * and applies any specified filter conditions. It automatically handles pagination to return
     * ALL matching items in a single list, which is convenient but may consume significant memory
     * for large result sets.</p>
     * 
     * <p><b>Important Notes:</b></p>
     * <ul>
     * <li>Automatically handles pagination - retrieves ALL results</li>
     * <li>May perform multiple API calls for large result sets</li>
     * <li>Results are loaded entirely into memory</li>
     * <li>Consider using stream() for large datasets</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * QueryRequest queryRequest = QueryRequest.builder()
     *     .tableName("Orders")
     *     .keyConditionExpression("customerId = :customerId")
     *     .expressionAttributeValues(Map.of(
     *         ":customerId", AttributeValue.fromS("CUSTOMER123")
     *     ))
     *     .build();
     * 
     * CompletableFuture<List<Map<String, Object>>> future = executor.list(queryRequest);
     * future.thenAccept(orders -> {
     *     System.out.println("Found " + orders.size() + " orders");
     *     orders.forEach(order -> 
     *         System.out.println("Order: " + order.get("orderId")));
     * });
     * }</pre>
     * 
     * @param queryRequest the QueryRequest with all parameters configured. Must not be null.
     * @return a CompletableFuture containing a list of all matching items as Maps
     * @throws IllegalArgumentException if queryRequest is null
     * @see #list(QueryRequest, Class) for type-safe results
     * @see #stream(QueryRequest) for memory-efficient processing
     */
    public CompletableFuture<List<Map<String, Object>>> list(final QueryRequest queryRequest) {
        return list(queryRequest, Clazz.PROPS_MAP);
    }

    /**
     * Asynchronously executes a query and returns all matching items as a list of typed objects.
     * 
     * <p>This method performs a Query operation and converts the results to instances of the
     * specified target class. It automatically handles pagination to retrieve all matching items.
     * This provides type safety and automatic object mapping for entity classes.</p>
     * 
     * <p><b>Pagination Behavior:</b></p>
     * <ul>
     * <li>Automatically fetches all pages of results</li>
     * <li>Continues until no more items are available</li>
     * <li>All results loaded into memory at once</li>
     * <li>May timeout for very large result sets</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * QueryRequest queryRequest = QueryRequest.builder()
     *     .tableName("Orders")
     *     .indexName("customer-date-index")
     *     .keyConditionExpression("customerId = :id AND orderDate > :date")
     *     .expressionAttributeValues(Map.of(
     *         ":id", AttributeValue.fromS("CUSTOMER123"),
     *         ":date", AttributeValue.fromS("2024-01-01")
     *     ))
     *     .scanIndexForward(false)  // Most recent first
     *     .build();
     * 
     * CompletableFuture<List<Order>> future = executor.list(queryRequest, Order.class);
     * future.thenAccept(orders -> {
     *     System.out.println("Found " + orders.size() + " recent orders");
     *     orders.forEach(order -> processOrder(order));
     * });
     * }</pre>
     * 
     * @param <T> the type of objects to return
     * @param queryRequest the QueryRequest with query parameters. Must not be null.
     * @param targetClass the class to convert results to. Must not be null.
     * @return a CompletableFuture containing a list of all matching items as typed objects
     * @throws IllegalArgumentException if queryRequest or targetClass is null
     */
    public <T> CompletableFuture<List<T>> list(final QueryRequest queryRequest, final Class<T> targetClass) {
        final CompletableFuture<QueryResponse> queryResultFuture = dynamoDBClient.query(queryRequest);

        return queryResultFuture.thenApplyAsync(queryResult -> {
            final List<T> res = toList(queryResult, targetClass);

            if (N.notEmpty(queryResult.lastEvaluatedKey()) && N.isEmpty(queryRequest.exclusiveStartKey())) {
                QueryRequest newQueryRequest = queryRequest.copy(builder -> builder.exclusiveStartKey(queryResult.lastEvaluatedKey()));
                QueryResponse newQueryResult = queryResult;

                try {
                    do {
                        final Map<String, AttributeValue> lastEvaluatedKey = newQueryResult.lastEvaluatedKey();
                        newQueryRequest = queryRequest.copy(builder -> builder.exclusiveStartKey(lastEvaluatedKey));
                        newQueryResult = dynamoDBClient.query(newQueryRequest).get();
                        res.addAll(toList(newQueryResult, targetClass));
                    } while (N.notEmpty(newQueryResult.lastEvaluatedKey()));
                } catch (final InterruptedException | ExecutionException e) {
                    throw ExceptionUtil.toRuntimeException(e, true);
                }
            }

            return res;
        });
    }

    /**
     * Asynchronously executes a query and returns results as a Dataset.
     * 
     * <p>This method performs a Query operation and returns the results in a Dataset format,
     * which provides rich functionality for data manipulation, filtering, grouping, and
     * aggregation operations. Datasets are particularly useful for analytical operations.</p>
     * 
     * <p><b>Dataset Features:</b></p>
     * <ul>
     * <li>Column-based operations</li>
     * <li>Grouping and aggregation</li>
     * <li>Filtering and transformation</li>
     * <li>SQL-like operations on results</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * QueryRequest queryRequest = QueryRequest.builder()
     *     .tableName("Sales")
     *     .keyConditionExpression("storeId = :storeId")
     *     .expressionAttributeValues(Map.of(
     *         ":storeId", AttributeValue.fromS("STORE001")
     *     ))
     *     .build();
     * 
     * executor.query(queryRequest)
     *     .thenAccept(dataset -> {
     *         // Group sales by product and sum amounts
     *         Dataset grouped = dataset.groupBy("productId")
     *             .aggregate("amount", Collectors.summingDouble(Double::doubleValue));
     *         
     *         System.out.println("Sales by product:");
     *         grouped.forEach(row -> 
     *             System.out.println(row.get("productId") + ": $" + row.get("amount")));
     *     });
     * }</pre>
     * 
     * @param queryRequest the QueryRequest with query parameters. Must not be null.
     * @return a CompletableFuture containing a Dataset with all query results
     * @throws IllegalArgumentException if queryRequest is null
     * @see #query(QueryRequest, Class) for typed Dataset results
     */
    public CompletableFuture<Dataset> query(final QueryRequest queryRequest) {
        return query(queryRequest, Map.class);
    }

    /**
     * Asynchronously executes a query and returns results as a typed Dataset.
     * 
     * <p>This method performs a Query operation and returns results in a Dataset format
     * with type conversion to the specified class. This combines the benefits of Dataset
     * operations with type safety for entity classes.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * QueryRequest queryRequest = QueryRequest.builder()
     *     .tableName("Products")
     *     .indexName("category-price-index")
     *     .keyConditionExpression("category = :category")
     *     .filterExpression("price > :minPrice")
     *     .expressionAttributeValues(Map.of(
     *         ":category", AttributeValue.fromS("Electronics"),
     *         ":minPrice", AttributeValue.fromN("100")
     *     ))
     *     .build();
     * 
     * executor.query(queryRequest, Product.class)
     *     .thenAccept(dataset -> {
     *         // Work with typed Product objects in Dataset
     *         double avgPrice = dataset.stream()
     *             .mapToDouble(product -> product.getPrice())
     *             .average()
     *             .orElse(0.0);
     *         
     *         System.out.println("Average price: $" + avgPrice);
     *     });
     * }</pre>
     * 
     * @param queryRequest the QueryRequest with query parameters. Must not be null.
     * @param targetClass the class to convert results to, or Map.class for raw results
     * @return a CompletableFuture containing a typed Dataset with query results
     * @throws IllegalArgumentException if queryRequest is null
     */
    public CompletableFuture<Dataset> query(final QueryRequest queryRequest, final Class<?> targetClass) {
        if (targetClass == null || Map.class.isAssignableFrom(targetClass)) {
            final CompletableFuture<QueryResponse> queryResultFuture = dynamoDBClient.query(queryRequest);

            return queryResultFuture.thenApplyAsync(queryResult -> {
                final List<Map<String, AttributeValue>> items = queryResult.items();

                if (N.notEmpty(queryResult.lastEvaluatedKey()) && N.isEmpty(queryRequest.exclusiveStartKey())) {
                    QueryRequest newQueryRequest = queryRequest.copy(builder -> builder.exclusiveStartKey(queryResult.lastEvaluatedKey()));
                    QueryResponse newQueryResult = queryResult;

                    try {
                        do {
                            final Map<String, AttributeValue> lastEvaluatedKey = newQueryResult.lastEvaluatedKey();
                            newQueryRequest = queryRequest.copy(builder -> builder.exclusiveStartKey(lastEvaluatedKey));
                            newQueryResult = dynamoDBClient.query(newQueryRequest).get();
                            items.addAll(newQueryResult.items());
                        } while (N.notEmpty(newQueryResult.lastEvaluatedKey()));
                    } catch (final InterruptedException | ExecutionException e) {
                        throw ExceptionUtil.toRuntimeException(e, true);
                    }
                }

                return extractData(items, 0, items.size());
            });
        } else {
            return list(queryRequest, targetClass).thenApplyAsync(N::newDataset);
        }
    }

    /**
     * Asynchronously executes a query and returns matching items as a memory-efficient Stream.
     * 
     * <p>This method creates a lazy-loading Stream that fetches query results on-demand using
     * pagination. This is the preferred approach for processing large result sets as it minimizes
     * memory usage and allows for efficient processing of millions of items.</p>
     * 
     * <p><b>Stream Benefits:</b></p>
     * <ul>
     * <li>Memory-efficient - loads data in pages as needed</li>
     * <li>Lazy evaluation - only fetches what you process</li>
     * <li>Functional processing with Stream API</li>
     * <li>Automatic pagination handling</li>
     * <li>Can be interrupted or limited easily</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * QueryRequest queryRequest = QueryRequest.builder()
     *     .tableName("Events")
     *     .keyConditionExpression("userId = :userId AND #timestamp > :since")
     *     .expressionAttributeNames(Map.of("#timestamp", "timestamp"))
     *     .expressionAttributeValues(Map.of(
     *         ":userId", AttributeValue.fromS("user123"),
     *         ":since", AttributeValue.fromN(String.valueOf(Instant.now().minus(30, ChronoUnit.DAYS).toEpochMilli()))
     *     ))
     *     .build();
     * 
     * CompletableFuture<Stream<Map<String, Object>>> future = executor.stream(queryRequest);
     * future.thenAccept(eventStream -> {
     *     long recentEvents = eventStream
     *         .filter(event -> "ERROR".equals(event.get("level")))
     *         .limit(100)  // Process only first 100 errors
     *         .peek(event -> processErrorEvent(event))
     *         .count();
     *     System.out.println("Processed " + recentEvents + " error events");
     * });
     * }</pre>
     * 
     * @param queryRequest the QueryRequest with all parameters configured. Must not be null.
     * @return a CompletableFuture containing a Stream of matching items as Maps
     * @throws IllegalArgumentException if queryRequest is null
     * @see #stream(QueryRequest, Class) for type-safe streaming
     * @see #list(QueryRequest) for loading all results into memory
     */
    public CompletableFuture<Stream<Map<String, Object>>> stream(final QueryRequest queryRequest) {
        return stream(queryRequest, Clazz.PROPS_MAP);
    }

    /**
     * Asynchronously executes a query and returns matching items as a typed Stream.
     * 
     * <p>This method creates a lazy-loading Stream that fetches query results on-demand
     * and converts them to instances of the specified target class. Combines memory
     * efficiency with type safety for large result sets.</p>
     * 
     * <p><b>Pagination and Streaming:</b></p>
     * <ul>
     * <li>Pages are fetched as the stream is consumed</li>
     * <li>Each page requires a network call to DynamoDB</li>
     * <li>Stream can be interrupted at any point</li>
     * <li>Ideal for processing large datasets incrementally</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * QueryRequest queryRequest = QueryRequest.builder()
     *     .tableName("Transactions")
     *     .keyConditionExpression("accountId = :accountId")
     *     .filterExpression("amount > :threshold")
     *     .expressionAttributeValues(Map.of(
     *         ":accountId", AttributeValue.fromS("ACC123"),
     *         ":threshold", AttributeValue.fromN("1000")
     *     ))
     *     .build();
     * 
     * CompletableFuture<Stream<Transaction>> future = 
     *     executor.stream(queryRequest, Transaction.class);
     * 
     * future.thenAccept(transactionStream -> {
     *     // Process large transactions efficiently
     *     Map<String, Double> totals = transactionStream
     *         .collect(Collectors.groupingBy(
     *             Transaction::getCategory,
     *             Collectors.summingDouble(Transaction::getAmount)
     *         ));
     *     
     *     totals.forEach((category, total) -> 
     *         System.out.println(category + ": $" + total));
     * });
     * }</pre>
     * 
     * @param <T> the type of objects in the stream
     * @param queryRequest the QueryRequest with query parameters. Must not be null.
     * @param targetClass the class to convert results to. Must not be null.
     * @return a CompletableFuture containing a Stream of typed objects
     * @throws IllegalArgumentException if queryRequest or targetClass is null
     */
    public <T> CompletableFuture<Stream<T>> stream(final QueryRequest queryRequest, final Class<T> targetClass) {

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

                        try {
                            queryResult = dynamoDBClient.query(newQueryRequest).get();
                        } catch (final InterruptedException | ExecutionException e) {
                            throw ExceptionUtil.toRuntimeException(e, true);
                        }

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

        return CompletableFuture.supplyAsync(() -> Stream.of(iterator).flatmap(Fn.identity()).map(createRowMapper(targetClass)));
    }

    /**
     * Asynchronously scans a table and returns specified attributes as a Stream.
     * 
     * <p>This method performs a table scan operation, retrieving only the specified attributes
     * from all items in the table. Scans are expensive operations that read every item
     * in the table regardless of any filter conditions.</p>
     * 
     * <p><b>Performance Warning:</b> Scans consume read capacity for every item in the table.
     * Consider using Query operations with appropriate indexes for better performance.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<String> attributes = Arrays.asList("userId", "email", "status");
     * 
     * executor.scan("Users", attributes)
     *     .thenAccept(stream -> {
     *         long activeUsers = stream
     *             .filter(user -> "ACTIVE".equals(user.get("status")))
     *             .count();
     *         System.out.println("Active users: " + activeUsers);
     *     });
     * }</pre>
     * 
     * @param tableName the name of the DynamoDB table to scan. Must not be null.
     * @param attributesToGet list of attribute names to retrieve, null for all attributes
     * @return a CompletableFuture containing a Stream of items as Maps
     * @throws IllegalArgumentException if tableName is null
     */
    public CompletableFuture<Stream<Map<String, Object>>> scan(final String tableName, final List<String> attributesToGet) {
        final ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).attributesToGet(attributesToGet).build();

        return scan(scanRequest);
    }

    /**
     * Asynchronously scans a table with filter conditions.
     * 
     * <p>This method performs a table scan with specified filter conditions. Note that
     * filters are applied AFTER items are read, so this still consumes read capacity
     * for all items in the table. The filter only reduces the amount of data transferred.</p>
     * 
     * <p><b>Important:</b> Scan filters don't reduce read costs - they only reduce network transfer.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, Condition> scanFilter = new HashMap<>();
     * scanFilter.put("age", Condition.builder()
     *     .comparisonOperator(ComparisonOperator.GE)
     *     .attributeValueList(AttributeValue.builder().n("18").build())
     *     .build());
     * scanFilter.put("country", Condition.builder()
     *     .comparisonOperator(ComparisonOperator.EQ)
     *     .attributeValueList(AttributeValue.builder().s("USA").build())
     *     .build());
     * 
     * executor.scan("Users", scanFilter)
     *     .thenAccept(stream -> {
     *         stream.forEach(user -> processEligibleUser(user));
     *     });
     * }</pre>
     * 
     * @param tableName the name of the DynamoDB table to scan. Must not be null.
     * @param scanFilter map of attribute names to filter conditions
     * @return a CompletableFuture containing a Stream of filtered items
     * @throws IllegalArgumentException if tableName is null
     */
    public CompletableFuture<Stream<Map<String, Object>>> scan(final String tableName, final Map<String, Condition> scanFilter) {
        final ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).scanFilter(scanFilter).build();

        return scan(scanRequest);
    }

    /**
     * Asynchronously scans a table with both attribute projection and filtering.
     * 
     * <p>This method combines attribute projection and filtering in a single scan operation.
     * Only specified attributes are retrieved, and results are filtered according to the
     * provided conditions. Remember that filtering happens after reading items.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<String> attributes = Arrays.asList("userId", "email", "lastLogin");
     * Map<String, Condition> filter = Map.of(
     *     "lastLogin", Condition.builder()
     *         .comparisonOperator(ComparisonOperator.GT)
     *         .attributeValueList(AttributeValue.builder()
     *             .n(String.valueOf(Instant.now().minus(30, ChronoUnit.DAYS).toEpochMilli()))
     *             .build())
     *         .build()
     * );
     * 
     * executor.scan("Users", attributes, filter)
     *     .thenAccept(stream -> {
     *         List<String> recentUserEmails = stream
     *             .map(user -> (String) user.get("email"))
     *             .collect(Collectors.toList());
     *         sendReengagementEmails(recentUserEmails);
     *     });
     * }</pre>
     * 
     * @param tableName the name of the DynamoDB table to scan. Must not be null.
     * @param attributesToGet list of attribute names to retrieve, null for all
     * @param scanFilter map of attribute names to filter conditions
     * @return a CompletableFuture containing a Stream of filtered items with specified attributes
     * @throws IllegalArgumentException if tableName is null
     */
    public CompletableFuture<Stream<Map<String, Object>>> scan(final String tableName, final List<String> attributesToGet,
            final Map<String, Condition> scanFilter) {
        final ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).attributesToGet(attributesToGet).scanFilter(scanFilter).build();

        return scan(scanRequest);
    }

    /**
     * Asynchronously scans a table using a custom ScanRequest.
     * 
     * <p>This method provides full control over the scan operation through a complete
     * ScanRequest. Returns results as a Stream of Maps for memory-efficient processing
     * of large tables.</p>
     * 
     * <p><b>Advanced Scan Features:</b></p>
     * <ul>
     * <li>Filter expressions for complex conditions</li>
     * <li>Projection expressions for attribute selection</li>
     * <li>Parallel scans for large tables</li>
     * <li>Consistent reads (expensive for scans)</li>
     * <li>Pagination control with limit and exclusiveStartKey</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ScanRequest scanRequest = ScanRequest.builder()
     *     .tableName("Products")
     *     .filterExpression("price BETWEEN :min AND :max AND inStock = :true")
     *     .projectionExpression("productId, productName, price")
     *     .expressionAttributeValues(Map.of(
     *         ":min", AttributeValue.builder().n("10").build(),
     *         ":max", AttributeValue.builder().n("100").build(),
     *         ":true", AttributeValue.builder().bool(true).build()
     *     ))
     *     .limit(100)  // Process in batches of 100
     *     .build();
     * 
     * executor.scan(scanRequest)
     *     .thenAccept(stream -> {
     *         stream.forEach(product -> displayProduct(product));
     *     });
     * }</pre>
     * 
     * @param scanRequest the complete ScanRequest with all parameters. Must not be null.
     * @return a CompletableFuture containing a Stream of items as Maps
     * @throws IllegalArgumentException if scanRequest is null
     * @see #scan(ScanRequest, Class) for type-safe results
     */
    public CompletableFuture<Stream<Map<String, Object>>> scan(final ScanRequest scanRequest) {
        return scan(scanRequest, Clazz.PROPS_MAP);
    }

    /**
     * Asynchronously scans a table and returns specified attributes as a typed Stream.
     * 
     * <p>This method performs a table scan operation with attribute projection and
     * converts results to instances of the specified target class. Provides type safety
     * while maintaining memory efficiency through streaming.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<String> attributes = Arrays.asList("userId", "name", "email");
     * 
     * executor.scan("Users", attributes, User.class)
     *     .thenAccept(userStream -> {
     *         List<User> users = userStream
     *             .filter(user -> user.getEmail() != null)
     *             .collect(Collectors.toList());
     *         System.out.println("Found " + users.size() + " users with email");
     *     });
     * }</pre>
     * 
     * @param <T> the type of objects in the stream
     * @param tableName the name of the DynamoDB table to scan. Must not be null.
     * @param attributesToGet list of attribute names to retrieve, null for all attributes
     * @param targetClass the class to convert results to. Must not be null.
     * @return a CompletableFuture containing a Stream of typed objects
     * @throws IllegalArgumentException if tableName or targetClass is null
     */
    public <T> CompletableFuture<Stream<T>> scan(final String tableName, final List<String> attributesToGet, final Class<T> targetClass) {
        final ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).attributesToGet(attributesToGet).build();

        return scan(scanRequest, targetClass);
    }

    /**
     * Asynchronously scans a table with filter conditions and returns results as a typed Stream.
     * 
     * <p>This method performs a table scan with specified filter conditions and converts
     * the results to instances of the specified target class. This provides type safety
     * while allowing for filtering of results.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, Condition> scanFilter = new HashMap<>();
     * scanFilter.put("status", Condition.builder()
     *     .comparisonOperator(ComparisonOperator.EQ)
     *     .attributeValueList(AttributeValue.builder().s("ACTIVE").build())
     *     .build());
     * 
     * executor.scan("Users", scanFilter, User.class)
     *     .thenAccept(userStream -> {
     *         userStream.forEach(user -> processActiveUser(user));
     *     });
     * }</pre>
     * 
     * @param <T> the type of objects in the stream
     * @param tableName the name of the DynamoDB table to scan. Must not be null.
     * @param scanFilter map of attribute names to filter conditions
     * @param targetClass the class to convert results to. Must not be null.
     * @return a CompletableFuture containing a Stream of typed objects
     * @throws IllegalArgumentException if tableName or targetClass is null
     */
    public <T> CompletableFuture<Stream<T>> scan(final String tableName, final Map<String, Condition> scanFilter, final Class<T> targetClass) {
        final ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).scanFilter(scanFilter).build();

        return scan(scanRequest, targetClass);
    }

    /**
     * Asynchronously scans a table with attribute projection and filter conditions,
     * returning results as a typed Stream.
     * 
     * <p>This method combines attribute projection and filtering in a single scan operation,
     * converting results to instances of the specified target class. This provides type safety
     * while allowing for efficient processing of large datasets.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<String> attributes = Arrays.asList("userId", "email", "status");
     * Map<String, Condition> scanFilter = new HashMap<>();
     * scanFilter.put("status", Condition.builder()
     *     .comparisonOperator(ComparisonOperator.EQ)
     *     .attributeValueList(AttributeValue.builder().s("ACTIVE").build())
     *     .build());
     * 
     * executor.scan("Users", attributes, scanFilter, User.class)
     *     .thenAccept(userStream -> {
     *         userStream.forEach(user -> processActiveUser(user));
     *     });
     * }</pre>
     * 
     * @param <T> the type of objects in the stream
     * @param tableName the name of the DynamoDB table to scan. Must not be null.
     * @param attributesToGet list of attribute names to retrieve, null for all attributes
     * @param scanFilter map of attribute names to filter conditions
     * @param targetClass the class to convert results to. Must not be null.
     * @return a CompletableFuture containing a Stream of typed objects
     * @throws IllegalArgumentException if tableName or targetClass is null
     */
    public <T> CompletableFuture<Stream<T>> scan(final String tableName, final List<String> attributesToGet, final Map<String, Condition> scanFilter,
            final Class<T> targetClass) {
        final ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).attributesToGet(attributesToGet).scanFilter(scanFilter).build();

        return scan(scanRequest, targetClass);
    }

    /**
     * Asynchronously scans a table using a custom ScanRequest and returns results as a typed Stream.
     * 
     * <p>This method provides full control over the scan operation through a complete ScanRequest
     * and converts results to instances of the specified target class. This allows for type-safe
     * processing of large datasets while maintaining memory efficiency through streaming.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ScanRequest scanRequest = ScanRequest.builder()
     *     .tableName("Orders")
     *     .filterExpression("status = :status")
     *     .expressionAttributeValues(Map.of(
     *         ":status", AttributeValue.fromS("PENDING")
     *     ))
     *     .build();
     * 
     * CompletableFuture<Stream<Order>> future = executor.scan(scanRequest, Order.class);
     * future.thenAccept(orderStream -> {
     *     orderStream.forEach(order -> processPendingOrder(order));
     * });
     * }</pre>
     * 
     * @param <T> the type of objects in the stream
     * @param scanRequest the complete ScanRequest with all parameters. Must not be null.
     * @param targetClass the class to convert results to. Must not be null.
     * @return a CompletableFuture containing a Stream of typed objects
     * @throws IllegalArgumentException if scanRequest or targetClass is null
     */
    public <T> CompletableFuture<Stream<T>> scan(final ScanRequest scanRequest, final Class<T> targetClass) {

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

                        try {
                            scanResult = dynamoDBClient.scan(newScanRequest).get();
                        } catch (final InterruptedException | ExecutionException e) {
                            throw ExceptionUtil.toRuntimeException(e, true);
                        }

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

        return CompletableFuture.supplyAsync(() -> Stream.of(iterator).flatmap(Fn.identity()).map(createRowMapper(targetClass)));
    }

    /**
     * Closes this async executor and releases all associated resources.
     * 
     * <p>This method shuts down the underlying DynamoDbAsyncClient, which includes closing
     * HTTP connections, stopping background threads, and releasing any other system resources.
     * After calling this method, no further operations should be performed on this executor.</p>
     * 
     * <p><b>Resource Management:</b></p>
     * <ul>
     * <li>Closes all HTTP connections in the connection pool</li>
     * <li>Shuts down internal async thread pools</li>
     * <li>Releases NIO channels and buffers</li>
     * <li>Cancels any pending requests (they will fail)</li>
     * </ul>
     * 
     * <p><b>Best Practices:</b></p>
     * <ul>
     * <li>Always call close() when finished with the executor</li>
     * <li>Use try-with-resources for automatic cleanup</li>
     * <li>Don't call close() while operations are still pending</li>
     * <li>Consider graceful shutdown by waiting for pending futures</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Try-with-resources (recommended)
     * try (AsyncDynamoDBExecutor executor = new AsyncDynamoDBExecutor(client)) {
     *     // Use executor for operations
     *     CompletableFuture<User> future = executor.getItem(tableName, key, User.class);
     *     User user = future.get();
     * } // Automatically closed
     * 
     * // Manual cleanup
     * AsyncDynamoDBExecutor executor = new AsyncDynamoDBExecutor(client);
     * try {
     *     // Use executor
     * } finally {
     *     executor.close();
     * }
     * }</pre>
     * 
     * @see AutoCloseable#close()
     */
    @Override
    public void close() {
        if (dynamoDBClient != null) {
            dynamoDBClient.close();
        }
    }

    /**
     * A generic mapper class for asynchronous DynamoDB operations on entity objects.
     * 
     * <p>This class provides a high-level abstraction for common DynamoDB operations including
     * CRUD operations (Create, Read, Update, Delete), batch operations, queries, and scans.
     * All operations are asynchronous and return CompletableFuture instances for non-blocking execution.</p>
     * 
     * <p>The mapper automatically handles:</p>
     * <ul>
     *   <li>Entity-to-DynamoDB item conversion using configurable naming policies</li>
     *   <li>Primary key extraction from entities using @Id annotations</li>
     *   <li>Type conversions between Java objects and DynamoDB AttributeValues</li>
     *   <li>Batch operation management with DynamoDB limits</li>
     * </ul>
     * 
     * <p><b>Thread Safety:</b> Instances of this class are thread-safe and can be shared
     * across multiple threads.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Create a mapper for User entities
     * Mapper<User> userMapper = new Mapper<>(
     *     User.class, 
     *     dynamoDBExecutor, 
     *     "users-table",
     *     NamingPolicy.CAMEL_CASE
     * );
     * 
     * // Use the mapper for operations
     * User user = new User();
     * user.setUserId("123");
     * user.setName("John Doe");
     * 
     * userMapper.putItem(user)
     *     .thenAccept(response -> System.out.println("User saved"));
     * }</pre>
     * 
     * @param <T> the type of entity this mapper handles. Must be a valid bean class with getter/setter methods
     *            and at least one field annotated with @Id.
     */
    public static class Mapper<T> {
        private final AsyncDynamoDBExecutor dynamoDBExecutor;
        private final String tableName;
        private final Class<T> targetEntityClass;
        private final BeanInfo entityInfo;
        private final List<String> keyPropNames;
        private final List<PropInfo> keyPropInfos;
        private final NamingPolicy namingPolicy;

        /**
         * Constructs a new Mapper instance for the specified entity class and DynamoDB table.
         * 
         * <p>This constructor validates that the entity class is a proper bean class with
         * getter/setter methods and exactly one field annotated with @Id for the primary key.
         * The naming policy determines how Java property names are converted to DynamoDB attribute names.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Mapper<Product> productMapper = new Mapper<>(
         *     Product.class,
         *     dynamoDBExecutor,
         *     "products-table",
         *     NamingPolicy.SCREAMING_SNAKE_CASE
         * );
         * }</pre>
         * 
         * @param targetEntityClass the class of entities this mapper will handle. Must not be null
         *                         and must be a valid bean class with exactly one @Id field.
         * @param dynamoDBExecutor the async executor for DynamoDB operations. Must not be null.
         * @param tableName the name of the DynamoDB table. Must not be null or empty.
         * @param namingPolicy the policy for converting property names to attribute names.
         *                    If null, defaults to CAMEL_CASE.
         * @throws IllegalArgumentException if targetEntityClass is null, not a bean class,
         *                                 or doesn't have exactly one @Id field; if dynamoDBExecutor
         *                                 is null; or if tableName is null or empty.
         */
        Mapper(final Class<T> targetEntityClass, final AsyncDynamoDBExecutor dynamoDBExecutor, final String tableName, final NamingPolicy namingPolicy) {
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
         * Asynchronously retrieves an item using an entity instance as the key.
         * 
         * <p>This method extracts the primary key values from the provided entity instance
         * and uses them to retrieve the complete item from DynamoDB. The entity must have
         * the key attributes properly set (usually the @Id annotated fields).</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User userKey = new User();
         * userKey.setUserId("user123");   // Primary key
         * 
         * CompletableFuture<User> future = userMapper.getItem(userKey);
         * future.thenAccept(user -> {
         *     if (user != null) {
         *         System.out.println("Found user: " + user.getName());
         *     }
         * });
         * }</pre>
         * 
         * @param entity the entity instance with key attributes set. Must not be null.
         * @return a CompletableFuture containing the retrieved entity, or null if not found
         * @throws IllegalArgumentException if entity is null or missing key attributes
         */
        public CompletableFuture<T> getItem(final T entity) {
            return dynamoDBExecutor.getItem(tableName, createKey(entity), targetEntityClass);
        }

        /**
         * Asynchronously retrieves an item using an entity instance as the key with read consistency control.
         * 
         * <p>This method is similar to {@link #getItem(Object)} but allows specification of read consistency.
         * Strongly consistent reads return the most recent data but may have higher latency and consume
         * more read capacity units than eventually consistent reads.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User userKey = new User();
         * userKey.setUserId("user123");
         * 
         * // Strongly consistent read
         * userMapper.getItem(userKey, true)
         *     .thenAccept(user -> System.out.println("Latest user data: " + user));
         * }</pre>
         * 
         * @param entity the entity instance with key attributes set. Must not be null.
         * @param consistentRead true for strongly consistent reads, {@code false} for eventually consistent,
         *                      null to use default behavior
         * @return a CompletableFuture containing the retrieved entity, or null if not found
         * @throws IllegalArgumentException if entity is null or missing key attributes
         */
        public CompletableFuture<T> getItem(final T entity, final Boolean consistentRead) {
            return dynamoDBExecutor.getItem(tableName, createKey(entity), consistentRead, targetEntityClass);
        }

        /**
         * Asynchronously retrieves an item using a DynamoDB key map.
         * 
         * <p>This method provides direct control over the key specification using DynamoDB's
         * AttributeValue format. This is useful when working with composite keys or when
         * the key is already in AttributeValue format.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, AttributeValue> key = new HashMap<>();
         * key.put("userId", AttributeValue.builder().s("user123").build());
         * 
         * userMapper.getItem(key)
         *     .thenAccept(user -> System.out.println("Found: " + user));
         * }</pre>
         * 
         * @param key a map of attribute names to AttributeValues representing the primary key.
         *           Must not be null and must contain all key attributes.
         * @return a CompletableFuture containing the retrieved entity, or null if not found
         * @throws IllegalArgumentException if key is null or incomplete
         */
        public CompletableFuture<T> getItem(final Map<String, AttributeValue> key) {
            return dynamoDBExecutor.getItem(tableName, key, targetEntityClass);
        }

        /**
         * Asynchronously retrieves an item using a custom GetItemRequest.
         * 
         * <p>This method provides full control over the get operation, allowing specification
         * of projection expressions, return consumed capacity, and other advanced options.
         * If the table name is not specified in the request, it will be automatically set
         * to this mapper's table.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * GetItemRequest request = GetItemRequest.builder()
         *     .key(createKey("user123"))
         *     .projectionExpression("userId, email, lastLogin")
         *     .build();
         * 
         * userMapper.getItem(request)
         *     .thenAccept(user -> System.out.println("User email: " + user.getEmail()));
         * }</pre>
         * 
         * @param getItemRequest the complete GetItemRequest. Must not be null.
         * @return a CompletableFuture containing the retrieved entity, or null if not found
         * @throws IllegalArgumentException if the request specifies a different table name
         *                                 than this mapper's table
         */
        public CompletableFuture<T> getItem(final GetItemRequest getItemRequest) {
            return dynamoDBExecutor.getItem(checkItem(getItemRequest), targetEntityClass);
        }

        /**
         * Asynchronously retrieves multiple items using a collection of entity instances as keys.
         * 
         * <p>This method extracts primary keys from the provided entity instances and performs
         * a batch get operation to retrieve up to 100 items efficiently. This is much more
         * efficient than individual getItem calls when retrieving multiple items.</p>
         * 
         * <p><b>Batch Limits and Performance:</b></p>
         * <ul>
         * <li>Maximum 100 items per batch request</li>
         * <li>Maximum 16 MB total request size</li>
         * <li>Single network round-trip for multiple items</li>
         * <li>Eventually consistent reads by default</li>
         * </ul>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<User> userKeys = Arrays.asList(
         *     createUserKey("user1"),
         *     createUserKey("user2"),
         *     createUserKey("user3")
         * );
         * 
         * CompletableFuture<List<User>> future = userMapper.batchGetItem(userKeys);
         * future.thenAccept(users -> {
         *     System.out.println("Retrieved " + users.size() + " users");
         *     users.forEach(user -> System.out.println(user.getName()));
         * });
         * }</pre>
         * 
         * @param entities collection of entity instances with key attributes set. Must not be null.
         * @return a CompletableFuture containing a list of retrieved entities (may be fewer than requested)
         * @throws IllegalArgumentException if entities is null or contains invalid key entities
         */
        public CompletableFuture<List<T>> batchGetItem(final Collection<? extends T> entities) {
            return dynamoDBExecutor.batchGetItem(createKeys(entities), targetEntityClass).thenApply(batchGetItemResponse -> {
                if (N.isEmpty(batchGetItemResponse)) {
                    return new ArrayList<>();
                } else {
                    final List<T> result = batchGetItemResponse.values().iterator().next();
                    return result != null ? result : new ArrayList<>();
                }
            });
        }

        /**
         * Asynchronously retrieves multiple items with consumed capacity reporting.
         * 
         * <p>This method is similar to {@link #batchGetItem(Collection)} but allows requesting
         * information about the read capacity units consumed by the operation. This is useful
         * for monitoring and optimizing DynamoDB usage costs.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<User> userKeys = createUserKeys(userIds);
         * 
         * userMapper.batchGetItem(userKeys, "TOTAL")
         *     .thenAccept(users -> {
         *         System.out.println("Retrieved " + users.size() + " users");
         *         // Consumed capacity info available in response
         *     });
         * }</pre>
         * 
         * @param entities collection of entity instances with key attributes set. Must not be null.
         * @param returnConsumedCapacity specifies the level of detail for consumed capacity.
         *                              Valid values: "INDEXES", "TOTAL", "NONE"
         * @return a CompletableFuture containing a list of retrieved entities
         * @throws IllegalArgumentException if entities is null or contains invalid key entities
         */
        public CompletableFuture<List<T>> batchGetItem(final Collection<? extends T> entities, final String returnConsumedCapacity) {
            return dynamoDBExecutor.batchGetItem(createKeys(entities), returnConsumedCapacity, targetEntityClass).thenApply(batchGetItemResponse -> {

                if (N.isEmpty(batchGetItemResponse)) {
                    return new ArrayList<>();
                } else {
                    final List<T> result = batchGetItemResponse.values().iterator().next();
                    return result != null ? result : new ArrayList<>();
                }
            });
        }

        /**
         * Asynchronously retrieves multiple items using a custom BatchGetItemRequest.
         * 
         * <p>This method provides full control over the batch get operation, allowing
         * specification of multiple tables, projection expressions, and other advanced options.
         * The response will only include items from this mapper's table.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * BatchGetItemRequest request = BatchGetItemRequest.builder()
         *     .requestItems(createBatchKeys(userIds))
         *     .returnConsumedCapacity("TOTAL")
         *     .build();
         * 
         * userMapper.batchGetItem(request)
         *     .thenAccept(users -> processUsers(users));
         * }</pre>
         * 
         * @param batchGetItemRequest the complete BatchGetItemRequest. Must not be null.
         * @return a CompletableFuture containing a list of retrieved entities from this mapper's table
         * @throws IllegalArgumentException if the request specifies a different table name
         */
        public CompletableFuture<List<T>> batchGetItem(final BatchGetItemRequest batchGetItemRequest) {
            return dynamoDBExecutor.batchGetItem(checkItem(batchGetItemRequest), targetEntityClass).thenApply(batchGetItemResponse -> {

                if (N.isEmpty(batchGetItemResponse)) {
                    return new ArrayList<>();
                } else {
                    final List<T> result = batchGetItemResponse.values().iterator().next();
                    return result != null ? result : new ArrayList<>();
                }
            });
        }

        /**
         * Asynchronously puts an entity into the DynamoDB table.
         * 
         * <p>This method converts the entity to a DynamoDB item and stores it in the table.
         * If an item with the same primary key already exists, it will be completely replaced.
         * The conversion follows the configured naming policy and handles all supported data types.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User newUser = new User();
         * newUser.setUserId("user123");
         * newUser.setName("John Doe");
         * newUser.setEmail("john@example.com");
         * 
         * CompletableFuture<PutItemResponse> future = userMapper.putItem(newUser);
         * future.thenAccept(response -> {
         *     System.out.println("User saved successfully");
         * });
         * }</pre>
         * 
         * @param entity the entity to save. Must not be null and must have all required attributes.
         * @return a CompletableFuture containing the PutItemResponse with operation metadata
         * @throws IllegalArgumentException if entity is null or invalid
         */
        public CompletableFuture<PutItemResponse> putItem(final T entity) {
            return dynamoDBExecutor.putItem(tableName, toItem(entity, namingPolicy));
        }

        /**
         * Asynchronously puts an entity into the table with return values specification.
         * 
         * <p>This method allows retrieving the old item values when replacing an existing item.
         * This is useful for audit trails or when you need to know what was replaced.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User updatedUser = new User();
         * updatedUser.setUserId("user123");
         * updatedUser.setName("Jane Doe");
         * 
         * userMapper.putItem(updatedUser, "ALL_OLD")
         *     .thenAccept(response -> {
         *         // response.attributes() contains the old item if it existed
         *         System.out.println("Replaced user data");
         *     });
         * }</pre>
         * 
         * @param entity the entity to save. Must not be null.
         * @param returnValues specifies which attributes to return. Valid values:
         *                    "NONE" (default), "ALL_OLD"
         * @return a CompletableFuture containing the PutItemResponse with specified return values
         * @throws IllegalArgumentException if entity is null or returnValues is invalid
         */
        public CompletableFuture<PutItemResponse> putItem(final T entity, final String returnValues) {
            return dynamoDBExecutor.putItem(tableName, toItem(entity, namingPolicy), returnValues);
        }

        /**
         * Asynchronously puts an item using a custom PutItemRequest.
         * 
         * <p>This method provides full control over the put operation, allowing specification
         * of conditional expressions, return values, and other advanced options. If the table
         * name is not specified in the request, it will be automatically set.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * PutItemRequest request = PutItemRequest.builder()
         *     .item(toItem(user))
         *     .conditionExpression("attribute_not_exists(userId)")
         *     .build();
         * 
         * userMapper.putItem(request)
         *     .thenAccept(response -> System.out.println("New user created"))
         *     .exceptionally(ex -> {
         *         System.out.println("User already exists");
         *         return null;
         *     });
         * }</pre>
         * 
         * @param putItemRequest the complete PutItemRequest. Must not be null.
         * @return a CompletableFuture containing the PutItemResponse
         * @throws IllegalArgumentException if the request specifies a different table name
         */
        public CompletableFuture<PutItemResponse> putItem(final PutItemRequest putItemRequest) {
            return dynamoDBExecutor.putItem(checkItem(putItemRequest));
        }

        /**
         * Asynchronously performs a batch put operation for multiple entities.
         * 
         * <p>This method efficiently writes multiple items to DynamoDB in a single request.
         * It's much more efficient than individual putItem calls when saving multiple items.
         * Note that batch operations do not support conditional expressions.</p>
         * 
         * <p><b>Batch Limits:</b></p>
         * <ul>
         * <li>Maximum 25 items per batch request</li>
         * <li>Maximum 16 MB total request size</li>
         * <li>Maximum 400 KB per individual item</li>
         * <li>No conditional expressions supported</li>
         * </ul>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<User> newUsers = Arrays.asList(
         *     createUser("user1", "Alice"),
         *     createUser("user2", "Bob"),
         *     createUser("user3", "Charlie")
         * );
         * 
         * userMapper.batchPutItem(newUsers)
         *     .thenAccept(response -> {
         *         if (response.unprocessedItems().isEmpty()) {
         *             System.out.println("All users saved successfully");
         *         }
         *     });
         * }</pre>
         * 
         * @param entities collection of entities to save. Must not be null or exceed batch limits.
         * @return a CompletableFuture containing the BatchWriteItemResponse with unprocessed items if any
         * @throws IllegalArgumentException if entities is null or exceeds DynamoDB limits
         */
        public CompletableFuture<BatchWriteItemResponse> batchPutItem(final Collection<? extends T> entities) {
            return dynamoDBExecutor.batchWriteItem(createBatchPutRequest(entities));
        }

        /**
         * Asynchronously updates an item using an entity instance.
         * 
         * <p>This method extracts the primary key from the entity and creates update operations
         * for all non-null properties. Only changed attributes are included in the update operation,
         * making it efficient for partial updates. The key attributes are used to identify the item.</p>
         * 
         * <p><b>Update Behavior:</b></p>
         * <ul>
         * <li>Only non-null entity properties are updated</li>
         * <li>Uses PUT action for all attribute updates</li>
         * <li>Null properties are ignored (not deleted)</li>
         * <li>Key attributes are used for item identification only</li>
         * </ul>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User updateUser = new User();
         * updateUser.setUserId("user123");          // Key for identification
         * updateUser.setEmail("new@example.com");   // Only this will be updated
         * updateUser.setLastLogin(Instant.now());
         * // name and other properties remain unchanged
         * 
         * CompletableFuture<UpdateItemResponse> future = userMapper.updateItem(updateUser);
         * future.thenAccept(response -> {
         *     System.out.println("User updated successfully");
         * });
         * }</pre>
         * 
         * @param entity the entity instance with key and updated attributes set. Must not be null.
         * @return a CompletableFuture containing the UpdateItemResponse with operation metadata
         * @throws IllegalArgumentException if entity is null or missing key attributes
         */
        public CompletableFuture<UpdateItemResponse> updateItem(final T entity) {
            return dynamoDBExecutor.updateItem(tableName, createKey(entity), toUpdateItem(entity, namingPolicy));
        }

        /**
         * Asynchronously updates an item with return values specification.
         * 
         * <p>This method is similar to {@link #updateItem(Object)} but allows specifying
         * which values to return after the update. This is useful for retrieving the updated
         * values or the old values for audit purposes.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User updateUser = new User();
         * updateUser.setUserId("user123");
         * updateUser.setEmail("updated@example.com");
         * 
         * userMapper.updateItem(updateUser, "ALL_NEW")
         *     .thenAccept(response -> {
         *         // response.attributes() contains all attributes after update
         *         System.out.println("Updated user: " + response.attributes());
         *     });
         * }</pre>
         * 
         * @param entity the entity instance with key and updated attributes set. Must not be null.
         * @param returnValues specifies which attributes to return. Valid values:
         *                    "NONE", "ALL_OLD", "UPDATED_OLD", "ALL_NEW", "UPDATED_NEW"
         * @return a CompletableFuture containing the UpdateItemResponse with specified return values
         * @throws IllegalArgumentException if entity is null or returnValues is invalid
         */
        public CompletableFuture<UpdateItemResponse> updateItem(final T entity, final String returnValues) {
            return dynamoDBExecutor.updateItem(tableName, createKey(entity), toUpdateItem(entity, namingPolicy), returnValues);
        }

        /**
         * Asynchronously updates an item using a custom UpdateItemRequest.
         * 
         * <p>This method provides full control over the update operation, allowing specification
         * of update expressions, conditional expressions, and other advanced options. If the table
         * name is not specified in the request, it will be automatically set.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * UpdateItemRequest request = UpdateItemRequest.builder()
         *     .key(createKey("user123"))
         *     .updateExpression("SET #email = :email, #count = #count + :inc")
         *     .expressionAttributeNames(Map.of("#email", "email", "#count", "loginCount"))
         *     .expressionAttributeValues(Map.of(
         *         ":email", AttributeValue.builder().s("new@example.com").build(),
         *         ":inc", AttributeValue.builder().n("1").build()
         *     ))
         *     .build();
         * 
         * userMapper.updateItem(request)
         *     .thenAccept(response -> System.out.println("User updated"));
         * }</pre>
         * 
         * @param updateItemRequest the complete UpdateItemRequest. Must not be null.
         * @return a CompletableFuture containing the UpdateItemResponse
         * @throws IllegalArgumentException if the request specifies a different table name
         */
        public CompletableFuture<UpdateItemResponse> updateItem(final UpdateItemRequest updateItemRequest) {
            return dynamoDBExecutor.updateItem(checkItem(updateItemRequest));
        }

        /**
         * Asynchronously deletes an item using an entity instance as the key.
         * 
         * <p>This method extracts the primary key from the entity and deletes the corresponding
         * item from DynamoDB. Only the key attributes need to be set in the entity; other
         * attributes are ignored.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User userToDelete = new User();
         * userToDelete.setUserId("user123");   // Only key needed
         * 
         * userMapper.deleteItem(userToDelete)
         *     .thenAccept(response -> System.out.println("User deleted"));
         * }</pre>
         * 
         * @param entity the entity instance with key attributes set. Must not be null.
         * @return a CompletableFuture containing the DeleteItemResponse with operation metadata
         * @throws IllegalArgumentException if entity is null or missing key attributes
         */
        public CompletableFuture<DeleteItemResponse> deleteItem(final T entity) {
            return dynamoDBExecutor.deleteItem(tableName, createKey(entity));
        }

        /**
         * Asynchronously deletes an item with return values specification.
         * 
         * <p>This method is similar to {@link #deleteItem(Object)} but allows retrieving
         * the deleted item's attributes. This is useful for audit trails or confirming
         * what was actually deleted.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User userToDelete = new User();
         * userToDelete.setUserId("user123");
         * 
         * userMapper.deleteItem(userToDelete, "ALL_OLD")
         *     .thenAccept(response -> {
         *         if (!response.attributes().isEmpty()) {
         *             System.out.println("Deleted user: " + response.attributes());
         *         }
         *     });
         * }</pre>
         * 
         * @param entity the entity instance with key attributes set. Must not be null.
         * @param returnValues specifies which attributes to return. Valid values:
         *                    "NONE" (default), "ALL_OLD"
         * @return a CompletableFuture containing the DeleteItemResponse with specified return values
         * @throws IllegalArgumentException if entity is null or returnValues is invalid
         */
        public CompletableFuture<DeleteItemResponse> deleteItem(final T entity, final String returnValues) {
            return dynamoDBExecutor.deleteItem(tableName, createKey(entity), returnValues);
        }

        /**
         * Asynchronously deletes an item using a DynamoDB key map.
         * 
         * <p>This method provides direct control over the key specification using DynamoDB's
         * AttributeValue format. This is useful when working with composite keys or when
         * the key is already in AttributeValue format.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, AttributeValue> key = new HashMap<>();
         * key.put("userId", AttributeValue.builder().s("user123").build());
         * 
         * userMapper.deleteItem(key)
         *     .thenAccept(response -> System.out.println("Item deleted"));
         * }</pre>
         * 
         * @param key a map of attribute names to AttributeValues representing the primary key.
         *           Must not be null and must contain all key attributes.
         * @return a CompletableFuture containing the DeleteItemResponse
         * @throws IllegalArgumentException if key is null or incomplete
         */
        public CompletableFuture<DeleteItemResponse> deleteItem(final Map<String, AttributeValue> key) {
            return dynamoDBExecutor.deleteItem(tableName, key);
        }

        /**
         * Asynchronously deletes an item using a custom DeleteItemRequest.
         * 
         * <p>This method provides full control over the delete operation, allowing specification
         * of conditional expressions and return values. If the table name is not specified
         * in the request, it will be automatically set.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * DeleteItemRequest request = DeleteItemRequest.builder()
         *     .key(createKey("user123"))
         *     .conditionExpression("attribute_exists(userId)")
         *     .returnValues("ALL_OLD")
         *     .build();
         * 
         * userMapper.deleteItem(request)
         *     .thenAccept(response -> System.out.println("Deleted: " + response.attributes()))
         *     .exceptionally(ex -> {
         *         System.out.println("Item not found");
         *         return null;
         *     });
         * }</pre>
         * 
         * @param deleteItemRequest the complete DeleteItemRequest. Must not be null.
         * @return a CompletableFuture containing the DeleteItemResponse
         * @throws IllegalArgumentException if the request specifies a different table name
         */
        public CompletableFuture<DeleteItemResponse> deleteItem(final DeleteItemRequest deleteItemRequest) {
            return dynamoDBExecutor.deleteItem(checkItem(deleteItemRequest));
        }

        /**
         * Asynchronously performs a batch delete operation for multiple entities.
         * 
         * <p>This method efficiently deletes multiple items from DynamoDB in a single request.
         * It's much more efficient than individual deleteItem calls when removing multiple items.
         * Note that batch operations do not support conditional expressions.</p>
         * 
         * <p><b>Batch Limits:</b></p>
         * <ul>
         * <li>Maximum 25 items per batch request</li>
         * <li>No conditional expressions supported</li>
         * <li>No return values for deleted items</li>
         * </ul>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<User> usersToDelete = Arrays.asList(
         *     createUserKey("user1"),
         *     createUserKey("user2"),
         *     createUserKey("user3")
         * );
         * 
         * userMapper.batchDeleteItem(usersToDelete)
         *     .thenAccept(response -> {
         *         if (response.unprocessedItems().isEmpty()) {
         *             System.out.println("All users deleted successfully");
         *         }
         *     });
         * }</pre>
         * 
         * @param entities collection of entities with key attributes set. Must not be null.
         * @return a CompletableFuture containing the BatchWriteItemResponse with unprocessed items if any
         * @throws IllegalArgumentException if entities is null or exceeds DynamoDB limits
         */
        public CompletableFuture<BatchWriteItemResponse> batchDeleteItem(final Collection<? extends T> entities) {
            return dynamoDBExecutor.batchWriteItem(createBatchDeleteRequest(entities));
        }

        /**
         * Asynchronously performs a batch write operation using a custom BatchWriteItemRequest.
         * 
         * <p>This method provides full control over batch write operations, allowing mixed
         * put and delete requests in a single batch. This is the most flexible batch operation
         * but requires manual request construction.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, List<WriteRequest>> requestItems = new HashMap<>();
         * List<WriteRequest> writeRequests = new ArrayList<>();
         * 
         * // Add put requests
         * writeRequests.add(WriteRequest.builder()
         *     .putRequest(PutRequest.builder().item(toItem(newUser)).build())
         *     .build());
         * 
         * // Add delete requests
         * writeRequests.add(WriteRequest.builder()
         *     .deleteRequest(DeleteRequest.builder().key(createKey(oldUser)).build())
         *     .build());
         * 
         * requestItems.put("users-table", writeRequests);
         * BatchWriteItemRequest request = BatchWriteItemRequest.builder()
         *     .requestItems(requestItems)
         *     .build();
         * 
         * userMapper.batchWriteItem(request)
         *     .thenAccept(response -> processResponse(response));
         * }</pre>
         * 
         * @param batchWriteItemRequest the complete BatchWriteItemRequest. Must not be null.
         * @return a CompletableFuture containing the BatchWriteItemResponse
         * @throws IllegalArgumentException if the request specifies a different table name
         */
        public CompletableFuture<BatchWriteItemResponse> batchWriteItem(final BatchWriteItemRequest batchWriteItemRequest) {
            return dynamoDBExecutor.batchWriteItem(checkItem(batchWriteItemRequest));
        }

        /**
         * Asynchronously executes a query and returns results as a list.
         * 
         * <p>This method performs a query operation on the table using the specified query request
         * and returns all matching items as a list. Queries are efficient for retrieving items
         * with a specific partition key value and optional sort key conditions.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * QueryRequest request = QueryRequest.builder()
         *     .keyConditionExpression("userId = :userId")
         *     .expressionAttributeValues(Map.of(
         *         ":userId", AttributeValue.builder().s("user123").build()
         *     ))
         *     .build();
         * 
         * userMapper.list(request)
         *     .thenAccept(users -> {
         *         System.out.println("Found " + users.size() + " matching users");
         *     });
         * }</pre>
         * 
         * @param queryRequest the QueryRequest specifying query parameters. Must not be null.
         * @return a CompletableFuture containing a list of all matching entities
         * @throws IllegalArgumentException if the request specifies a different table name
         */
        public CompletableFuture<List<T>> list(final QueryRequest queryRequest) {
            return dynamoDBExecutor.list(checkQueryRequest(queryRequest), targetEntityClass);
        }

        /**
         * Asynchronously executes a query and returns results as a Dataset.
         * 
         * <p>This method performs a query operation and returns results in a Dataset format,
         * which provides additional functionality for data manipulation and analysis beyond
         * a simple list. Datasets support operations like filtering, mapping, and aggregation.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * QueryRequest request = QueryRequest.builder()
         *     .indexName("email-index")
         *     .keyConditionExpression("email = :email")
         *     .expressionAttributeValues(Map.of(
         *         ":email", AttributeValue.builder().s("user@example.com").build()
         *     ))
         *     .build();
         * 
         * userMapper.query(request)
         *     .thenAccept(dataset -> {
         *         // Use Dataset operations for analysis
         *         dataset.groupBy("department").forEach((dept, users) -> {
         *             System.out.println(dept + ": " + users.size() + " users");
         *         });
         *     });
         * }</pre>
         * 
         * @param queryRequest the QueryRequest specifying query parameters. Must not be null.
         * @return a CompletableFuture containing a Dataset of matching entities
         * @throws IllegalArgumentException if the request specifies a different table name
         */
        public CompletableFuture<Dataset> query(final QueryRequest queryRequest) {
            return dynamoDBExecutor.query(checkQueryRequest(queryRequest), targetEntityClass);
        }

        /**
         * Asynchronously executes a query and returns results as a Stream.
         * 
         * <p>This method performs a query operation and returns results as a Stream for
         * efficient processing of large result sets. The Stream allows lazy evaluation
         * and can handle pagination automatically for large queries.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * QueryRequest request = QueryRequest.builder()
         *     .keyConditionExpression("status = :status")
         *     .expressionAttributeValues(Map.of(
         *         ":status", AttributeValue.builder().s("ACTIVE").build()
         *     ))
         *     .limit(100)  // Process in batches
         *     .build();
         * 
         * userMapper.stream(request)
         *     .thenAccept(stream -> {
         *         stream.filter(user -> user.getAge() > 18)
         *               .map(User::getEmail)
         *               .forEach(email -> sendNotification(email));
         *     });
         * }</pre>
         * 
         * @param queryRequest the QueryRequest specifying query parameters. Must not be null.
         * @return a CompletableFuture containing a Stream of matching entities
         * @throws IllegalArgumentException if the request specifies a different table name
         */
        public CompletableFuture<Stream<T>> stream(final QueryRequest queryRequest) {
            return dynamoDBExecutor.stream(checkQueryRequest(queryRequest), targetEntityClass);
        }

        /**
         * Asynchronously performs a table scan with specified attributes to retrieve.
         * 
         * <p>This method scans the entire table and returns all items, optionally projecting
         * only the specified attributes. Scans are expensive operations that read every item
         * in the table and should be used sparingly.</p>
         * 
         * <p><b>Performance Warning:</b> Scans consume read capacity for every item in the table,
         * regardless of whether they match any filter criteria.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> attributes = Arrays.asList("userId", "email", "lastLogin");
         * 
         * userMapper.scan(attributes)
         *     .thenAccept(stream -> {
         *         stream.forEach(user -> System.out.println(user.getEmail()));
         *     });
         * }</pre>
         * 
         * @param attributesToGet list of attribute names to retrieve, null for all attributes
         * @return a CompletableFuture containing a Stream of all entities in the table
         */
        public CompletableFuture<Stream<T>> scan(final List<String> attributesToGet) {
            return dynamoDBExecutor.scan(tableName, attributesToGet, targetEntityClass);
        }

        /**
         * Asynchronously performs a table scan with filter conditions.
         * 
         * <p>This method scans the entire table with specified filter conditions. Note that
         * filters are applied after the scan reads items, so this still consumes read capacity
         * for all items in the table. Consider using queries with indexes for better performance.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Condition> scanFilter = new HashMap<>();
         * scanFilter.put("age", Condition.builder()
         *     .comparisonOperator(ComparisonOperator.GT)
         *     .attributeValueList(AttributeValue.builder().n("18").build())
         *     .build());
         * 
         * userMapper.scan(scanFilter)
         *     .thenAccept(stream -> {
         *         stream.forEach(user -> processAdultUser(user));
         *     });
         * }</pre>
         * 
         * @param scanFilter map of attribute names to filter conditions
         * @return a CompletableFuture containing a Stream of entities matching the filter
         */
        public CompletableFuture<Stream<T>> scan(final Map<String, Condition> scanFilter) {
            return dynamoDBExecutor.scan(tableName, scanFilter, targetEntityClass);
        }

        /**
         * Asynchronously performs a table scan with both projection and filter.
         * 
         * <p>This method combines attribute projection and filtering in a single scan operation.
         * This is useful when you need specific attributes from items matching certain criteria,
         * but remember that scans are expensive operations.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> attributes = Arrays.asList("userId", "email");
         * Map<String, Condition> filter = createAgeFilter(18);
         * 
         * userMapper.scan(attributes, filter)
         *     .thenAccept(stream -> {
         *         List<String> emails = stream.map(User::getEmail)
         *                                     .collect(Collectors.toList());
         *         sendBulkEmail(emails);
         *     });
         * }</pre>
         * 
         * @param attributesToGet list of attribute names to retrieve, null for all attributes
         * @param scanFilter map of attribute names to filter conditions
         * @return a CompletableFuture containing a Stream of filtered entities
         */
        public CompletableFuture<Stream<T>> scan(final List<String> attributesToGet, final Map<String, Condition> scanFilter) {
            return dynamoDBExecutor.scan(tableName, attributesToGet, scanFilter, targetEntityClass);
        }

        /**
         * Asynchronously performs a scan using a custom ScanRequest.
         * 
         * <p>This method provides full control over the scan operation, allowing specification
         * of filter expressions, pagination, parallel scans, and other advanced options.
         * If the table name is not specified in the request, it will be automatically set.</p>
         * 
         * <p><b>Advanced Features:</b></p>
         * <ul>
         * <li>Filter expressions for complex conditions</li>
         * <li>Pagination with exclusiveStartKey</li>
         * <li>Parallel scans for large tables</li>
         * <li>Consistent reads (Note: expensive for scans)</li>
         * </ul>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * ScanRequest request = ScanRequest.builder()
         *     .filterExpression("age > :minAge AND status = :status")
         *     .expressionAttributeValues(Map.of(
         *         ":minAge", AttributeValue.builder().n("21").build(),
         *         ":status", AttributeValue.builder().s("ACTIVE").build()
         *     ))
         *     .limit(1000)
         *     .build();
         * 
         * userMapper.scan(request)
         *     .thenAccept(stream -> processActiveAdults(stream));
         * }</pre>
         * 
         * @param scanRequest the complete ScanRequest. Must not be null.
         * @return a CompletableFuture containing a Stream of entities from the scan
         * @throws IllegalArgumentException if the request specifies a different table name
         */
        public CompletableFuture<Stream<T>> scan(final ScanRequest scanRequest) {
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
}
