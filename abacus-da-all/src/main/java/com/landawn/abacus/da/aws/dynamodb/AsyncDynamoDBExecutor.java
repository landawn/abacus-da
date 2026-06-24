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

import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.stream.Stream;

/**
 * Asynchronous wrapper around {@link DynamoDBExecutor} providing non-blocking operations against the
 * AWS SDK <b>v1</b> ({@code com.amazonaws.services.dynamodbv2}) DynamoDB client.
 *
 * <p>This class wraps a synchronous {@link DynamoDBExecutor} and submits each call to a backing
 * {@link AsyncExecutor}. Every method returns a {@link ContinuableFuture} whose payload is exactly
 * the value the corresponding synchronous method on {@link DynamoDBExecutor} would have returned
 * (for example, {@code Map<String, Object>}, {@code PutItemResult}, or {@code Stream<T>}).</p>
 *
 * <p><strong>Note:</strong> This class targets AWS SDK v1, which is a different driver than the
 * v2 wrappers in the {@code com.landawn.abacus.da.aws.dynamodb.v2} subpackage. The two are not
 * interchangeable; the v1 model classes (e.g. {@link com.amazonaws.services.dynamodbv2.model.GetItemRequest})
 * are referenced throughout.</p>
 *
 * <p><strong>Threading model:</strong></p>
 * <ul>
 *   <li>Each method submits a task to the {@link AsyncExecutor} supplied at construction
 *       (typically derived from {@link DynamoDBExecutor#async()}).</li>
 *   <li>The submitted task invokes the corresponding blocking call on the underlying
 *       {@link DynamoDBExecutor}; the calling thread is never blocked.</li>
 *   <li>Continuations attached via {@link ContinuableFuture#thenRunAsync} and related methods
 *       run on the same {@link AsyncExecutor} unless an explicit executor is supplied.</li>
 *   <li>For methods that return a {@link Stream} (queries/scans), the stream itself is created
 *       asynchronously but is consumed lazily on whichever thread iterates it; the underlying
 *       SDK calls used to fetch additional pages happen synchronously during iteration.</li>
 * </ul>
 *
 * <p><strong>Key Features:</strong></p>
 * <ul>
 *   <li>Non-blocking analogues of every public {@link DynamoDBExecutor} operation.</li>
 *   <li>Automatic type conversion between DynamoDB {@link AttributeValue}s and Java objects.</li>
 *   <li>Support for batch operations, queries, and scans.</li>
 *   <li>Memory-efficient streaming for large result sets (automatic pagination).</li>
 *   <li>Seamless interop with the {@link ContinuableFuture} API for composition and chaining.</li>
 * </ul>
 *
 * <p><strong>Thread Safety:</strong> This class is thread-safe and may be shared across multiple
 * threads; both the wrapped {@link DynamoDBExecutor} and the {@link AsyncExecutor} are themselves
 * thread-safe.</p>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Create an async executor
 * AsyncDynamoDBExecutor asyncExecutor = new DynamoDBExecutor(dynamoDBClient).async();
 *
 * // Perform async operations
 * Map<String, AttributeValue> key = Map.of("id", new AttributeValue("123"));
 * asyncExecutor.getItem("MyTable", key)
 *     .thenRunAsync(item -> System.out.println("Found: " + item));
 *
 * // Or block for the result:
 * Map<String, Object> item = asyncExecutor.getItem("MyTable", key).get();
 * }</pre>
 *
 * @see DynamoDBExecutor
 * @see ContinuableFuture
 * @see AsyncExecutor
 */
public final class AsyncDynamoDBExecutor {

    private final DynamoDBExecutor dbExecutor;

    private final AsyncExecutor asyncExecutor;

    /**
     * Constructs an AsyncDynamoDBExecutor with the specified synchronous executor and async executor.
     *
     * <p>This constructor is package-private and is typically invoked indirectly through
     * {@link DynamoDBExecutor#async()} to create an asynchronous wrapper around a synchronous
     * executor. Every asynchronous operation delegates the corresponding synchronous method on
     * {@code dbExecutor} to a task submitted to {@code asyncExecutor}.</p>
     *
     * @param dbExecutor the synchronous DynamoDB executor to wrap (must not be {@code null})
     * @param asyncExecutor the executor used to schedule each asynchronous operation
     *                      (must not be {@code null})
     */
    AsyncDynamoDBExecutor(final DynamoDBExecutor dbExecutor, final AsyncExecutor asyncExecutor) {
        this.dbExecutor = dbExecutor;
        this.asyncExecutor = asyncExecutor;
    }

    /**
     * Returns the underlying synchronous DynamoDBExecutor.
     *
     * <p>This method provides access to the wrapped synchronous executor, allowing you to
     * perform blocking operations when needed. This is useful when you need to mix
     * synchronous and asynchronous operations in your application.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncDynamoDBExecutor asyncExecutor = ...;
     *
     * // Switch to synchronous operations when needed
     * DynamoDBExecutor syncExecutor = asyncExecutor.sync();            // returns the wrapped (non-null) executor instance
     * Map<String, Object> item = syncExecutor.getItem("MyTable", key); // blocking call on the same client
     *
     * // The returned instance is the very one this wrapper delegates to:
     * boolean same = (asyncExecutor.sync() == syncExecutor); // returns true (identity, never a copy)
     * }</pre>
     *
     * @return the underlying synchronous {@link DynamoDBExecutor} instance
     */
    public DynamoDBExecutor sync() {
        return dbExecutor;
    }

    /**
     * Asynchronously retrieves an item from the specified DynamoDB table.
     *
     * <p>This method performs an eventually consistent read by default. The returned
     * map contains attribute names as keys and their corresponding values as Java objects.
     * If the item doesn't exist, the future will complete with {@code null}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> key = Map.of("userId", new AttributeValue("12345"));
     *
     * // Typical: react to the item on a callback (non-blocking)
     * asyncExecutor.getItem("Users", key)
     *     .thenRunAsync(item -> {
     *         if (item != null) {
     *             System.out.println("User found: " + item.get("name"));
     *         }
     *     }); // returns ContinuableFuture<Map<String, Object>>; callback receives the item map or null
     *
     * // Typical: block for the result
     * Map<String, Object> item = asyncExecutor.getItem("Users", key).get(); // returns the attribute map, e.g. {userId=12345, name=...}
     *
     * // Edge: a key that matches no row completes with a null payload
     * Map<String, AttributeValue> missing = Map.of("userId", new AttributeValue("does-not-exist"));
     * Map<String, Object> none = asyncExecutor.getItem("Users", missing).get(); // returns null
     * }</pre>
     *
     * @param tableName the name of the DynamoDB table to retrieve the item from, must not be {@code null}
     * @param key the primary key of the item to retrieve, must include all key attributes, must not be {@code null}
     * @return a {@link ContinuableFuture} containing the item as a Map of attribute names to values,
     *         or {@code null} if the item doesn't exist
     * @throws IllegalArgumentException if tableName or key is {@code null}
     * @see #getItem(String, Map, Boolean)
     */
    public ContinuableFuture<Map<String, Object>> getItem(final String tableName, final Map<String, AttributeValue> key) {
        return asyncExecutor.execute(() -> dbExecutor.getItem(tableName, key));
    }

    /**
     * Asynchronously retrieves an item from the specified DynamoDB table with configurable read consistency.
     *
     * <p>This method allows you to control the read consistency level for the operation. Strongly consistent
     * reads ensure you get the most up-to-date data, while eventually consistent reads offer better performance
     * and lower cost but might return slightly outdated data.</p>
     *
     * <p><b>Read Consistency Trade-offs:</b></p>
     * <ul>
     * <li><b>Eventually Consistent (false/null):</b> Lower latency, better throughput, half the read capacity cost</li>
     * <li><b>Strongly Consistent (true):</b> Guaranteed latest data, higher latency, full read capacity consumption</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> key = Map.of("accountId", new AttributeValue("ACC-789"));
     *
     * // Typical: strong consistency for critical operations
     * asyncExecutor.getItem("Accounts", key, true)
     *     .thenRunAsync(account -> {
     *         if (account != null) {
     *             System.out.println("Account balance: " + account.get("balance"));
     *         }
     *     }); // returns ContinuableFuture<Map<String, Object>>; payload is the item map or null
     *
     * // Typical: eventual consistency (Boolean.FALSE) for non-critical reads
     * Map<String, Object> product = asyncExecutor.getItem("Products", productKey, false).get(); // returns the item map, or null if absent
     *
     * // Edge: a null consistentRead is treated as an eventually consistent read (same as false)
     * Map<String, Object> acct = asyncExecutor.getItem("Accounts", key, (Boolean) null).get(); // returns the item map, or null if absent
     * }</pre>
     *
     * @param tableName the name of the DynamoDB table to retrieve the item from, must not be {@code null}
     * @param key the primary key of the item to retrieve, must include all key attributes, must not be {@code null}
     * @param consistentRead {@code Boolean.TRUE} for a strongly consistent read;
     *                       {@code Boolean.FALSE} or {@code null} for an eventually consistent read
     * @return a {@link ContinuableFuture} whose payload is a {@code Map<String, Object>} of attribute
     *         names to values, or {@code null} if the item doesn't exist
     * @throws IllegalArgumentException if tableName or key is {@code null}
     * @see #getItem(String, Map)
     * @see #getItem(String, Map, Boolean, Class)
     */
    public ContinuableFuture<Map<String, Object>> getItem(final String tableName, final Map<String, AttributeValue> key, final Boolean consistentRead) {
        return asyncExecutor.execute(() -> dbExecutor.getItem(tableName, key, consistentRead));
    }

    /**
     * Asynchronously retrieves an item using a complete GetItemRequest for maximum control.
     *
     * <p>This method provides the most flexibility by accepting a fully configured GetItemRequest,
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
     * GetItemRequest request = new GetItemRequest()
     *     .withTableName("Users")
     *     .withKey(Map.of("userId", new AttributeValue("user-456")))
     *     .withProjectionExpression("userId, email, #name, createdAt")
     *     .withExpressionAttributeNames(Map.of("#name", "name"))  // 'name' is reserved
     *     .withConsistentRead(true)
     *     .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
     *
     * // Typical: callback that also handles failures via the (result, exception) form
     * asyncExecutor.getItem(request)
     *     .thenRunAsync((user, ex) -> {
     *         if (ex != null) {
     *             logger.error("Failed to retrieve user", ex);
     *         } else if (user != null) {
     *             System.out.println("User: " + user.get("email"));
     *         } else {
     *             System.out.println("User not found");
     *         }
     *     }); // returns ContinuableFuture<Map<String, Object>>; payload is the projected item map or null
     *
     * // Typical: block for the result
     * Map<String, Object> user = asyncExecutor.getItem(request).get(); // returns the projected attribute map, or null if absent
     *
     * // Edge: a request whose key matches nothing completes with a null payload
     * GetItemRequest miss = new GetItemRequest().withTableName("Users")
     *     .withKey(Map.of("userId", new AttributeValue("nope")));
     * Map<String, Object> none = asyncExecutor.getItem(miss).get(); // returns null
     * }</pre>
     *
     * @param getItemRequest the complete GetItemRequest with all parameters configured, must not be {@code null}
     * @return a ContinuableFuture containing the item as a Map of attribute names to values,
     *         or {@code null} if the item doesn't exist
     * @throws IllegalArgumentException if getItemRequest is {@code null}
     * @see GetItemRequest
     * @see #getItem(GetItemRequest, Class)
     */
    public ContinuableFuture<Map<String, Object>> getItem(final GetItemRequest getItemRequest) {
        return asyncExecutor.execute(() -> dbExecutor.getItem(getItemRequest));
    }

    /**
     * Asynchronously retrieves an item from the specified DynamoDB table and converts it to the target type.
     *
     * <p>This method performs an eventually consistent read by default and automatically converts the
     * DynamoDB item to the specified Java type. The conversion handles primitive types, collections,
     * nested objects, and custom POJOs with getter/setter methods.</p>
     *
     * <p><b>Type Conversion Features:</b></p>
     * <ul>
     * <li>Automatic mapping from DynamoDB AttributeValues to Java types</li>
     * <li>Support for primitives, wrappers, String, and numeric types</li>
     * <li>Handling of complex types like Lists, Sets, and Maps</li>
     * <li>Null-safe conversion for missing or null attributes</li>
     * <li>Custom POJO mapping using reflection</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> key = Map.of("orderId", new AttributeValue("ORDER-123"));
     *
     * // Typical: callback with type conversion to a POJO
     * asyncExecutor.getItem("Orders", key, Order.class)
     *     .thenRunAsync((order, ex) -> {
     *         if (ex != null) {
     *             logger.error("Failed to retrieve order", ex);
     *         } else if (order != null) {
     *             System.out.println("Order total: $" + order.getTotal());
     *             System.out.println("Status: " + order.getStatus());
     *         } else {
     *             System.out.println("Order not found");
     *         }
     *     }); // returns ContinuableFuture<Order>; payload is the converted Order or null
     *
     * // Typical: block for the converted entity
     * Order order = asyncExecutor.getItem("Orders", key, Order.class).get(); // returns an Order instance, or null if absent
     *
     * // Edge: a missing key completes with a null payload (no exception)
     * Map<String, AttributeValue> miss = Map.of("orderId", new AttributeValue("nope"));
     * Order none = asyncExecutor.getItem("Orders", miss, Order.class).get(); // returns null
     * }</pre>
     *
     * @param <T> the type to convert the item to
     * @param tableName the name of the DynamoDB table to retrieve the item from, must not be {@code null}
     * @param key the primary key of the item to retrieve, must include all key attributes, must not be {@code null}
     * @param targetClass the class to convert the item to, must have a default constructor, must not be {@code null}
     * @return a {@link ContinuableFuture} containing the item converted to type T, or {@code null} if not found
     * @throws IllegalArgumentException if any parameter is {@code null}
     * @see #getItem(String, Map, Boolean, Class)
     */
    public <T> ContinuableFuture<T> getItem(final String tableName, final Map<String, AttributeValue> key, final Class<T> targetClass) {
        return asyncExecutor.execute(() -> dbExecutor.getItem(tableName, key, targetClass));
    }

    /**
     * Asynchronously retrieves an item from the specified table with optional consistent read and converts it to the target type.
     *
     * <p>This method provides full control over read consistency while maintaining the benefits of asynchronous
     * execution and automatic type conversion. Strongly consistent reads ensure the most up-to-date data
     * but consume more read capacity and may have slightly higher latency.</p>
     *
     * <p><b>Read Consistency Impact:</b></p>
     * <ul>
     * <li><b>Eventually Consistent (false/null):</b> Lower latency, better throughput, lower cost</li>
     * <li><b>Strongly Consistent (true):</b> Guaranteed latest data, higher resource consumption</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> key = Map.of("accountId", new AttributeValue("ACC-456"));
     *
     * // Typical: strong consistency for financial data, converted to a POJO
     * executor.getItem("Accounts", key, true, Account.class)
     *     .thenRunAsync((account, ex) -> {
     *         if (ex != null) {
     *             logger.error("Failed to get account", ex);
     *         } else if (account != null) {
     *             System.out.println("Balance: $" + account.getBalance());
     *         }
     *     }); // returns ContinuableFuture<Account>; payload is the converted Account or null
     *
     * // Typical: block for the converted entity using an eventually consistent read
     * Account acct = executor.getItem("Accounts", key, false, Account.class).get(); // returns an Account instance, or null if absent
     *
     * // Edge: a null consistentRead means eventual consistency; a missing key yields null
     * Map<String, AttributeValue> miss = Map.of("accountId", new AttributeValue("nope"));
     * Account none = executor.getItem("Accounts", miss, (Boolean) null, Account.class).get(); // returns null
     * }</pre>
     *
     * @param <T> the type to convert the item to
     * @param tableName the name of the table to get the item from, must not be {@code null}
     * @param key the primary key of the item to retrieve, must include all key attributes, must not be {@code null}
     * @param consistentRead {@code Boolean.TRUE} to perform a strongly consistent read;
     *                       {@code Boolean.FALSE} or {@code null} for an eventually consistent read
     * @param targetClass the class to convert the item to, must not be {@code null}
     * @return a {@link ContinuableFuture} whose payload is the item converted to type {@code T},
     *         or {@code null} if the item does not exist
     * @throws IllegalArgumentException if tableName, key, or targetClass is {@code null}
     */
    public <T> ContinuableFuture<T> getItem(final String tableName, final Map<String, AttributeValue> key, final Boolean consistentRead,
            final Class<T> targetClass) {
        return asyncExecutor.execute(() -> dbExecutor.getItem(tableName, key, consistentRead, targetClass));
    }

    /**
     * Asynchronously retrieves an item using a GetItemRequest and converts it to the target type.
     *
     * <p>This method provides the most flexibility by combining full request control with automatic type conversion.
     * You can use all DynamoDB features including projection expressions, consistent read options, and expression
     * attribute names while maintaining type safety and asynchronous execution benefits.</p>
     *
     * <p><b>Advanced Features Available:</b></p>
     * <ul>
     * <li>Projection expressions for retrieving specific attributes only</li>
     * <li>Expression attribute names for reserved words handling</li>
     * <li>Return consumed capacity for cost monitoring</li>
     * <li>Consistent read configuration per request</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * GetItemRequest request = new GetItemRequest()
     *     .withTableName("Products")
     *     .withKey(Map.of("productId", new AttributeValue("PROD-123")))
     *     .withProjectionExpression("productId, productName, price, inStock")
     *     .withConsistentRead(true)
     *     .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
     *
     * // Typical: callback that converts the projected item to a POJO
     * executor.getItem(request, ProductSummary.class)
     *     .thenRunAsync((product, ex) -> {
     *         if (ex != null) {
     *             logger.error("Product lookup failed", ex);
     *         } else if (product != null && product.isInStock()) {
     *             processAvailableProduct(product);
     *         }
     *     }); // returns ContinuableFuture<ProductSummary>; payload is the converted item or null
     *
     * // Typical: block for the converted entity
     * ProductSummary product = executor.getItem(request, ProductSummary.class).get(); // returns a ProductSummary, or null if absent
     *
     * // Edge: a request whose key matches nothing completes with a null payload
     * GetItemRequest miss = new GetItemRequest().withTableName("Products")
     *     .withKey(Map.of("productId", new AttributeValue("nope")));
     * ProductSummary none = executor.getItem(miss, ProductSummary.class).get(); // returns null
     * }</pre>
     *
     * @param <T> the type to convert the item to
     * @param getItemRequest the complete GetItemRequest with all parameters configured, must not be {@code null}
     * @param targetClass the class to convert the item to, must have a default constructor, must not be {@code null}
     * @return a ContinuableFuture containing the item converted to type T, or null if not found
     * @throws IllegalArgumentException if getItemRequest or targetClass is {@code null}
     */
    public <T> ContinuableFuture<T> getItem(final GetItemRequest getItemRequest, final Class<T> targetClass) {
        return asyncExecutor.execute(() -> dbExecutor.getItem(getItemRequest, targetClass));
    }

    /**
     * Asynchronously performs a batch get operation to retrieve multiple items from multiple tables.
     *
     * <p>This method can retrieve up to 100 items in a single call, with a maximum total size
     * of 16 MB. If any requested items are not found, they will simply be omitted from the
     * results. The operation performs eventually consistent reads by default.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * KeysAndAttributes keysAndAttrs = new KeysAndAttributes()
     *     .withKeys(Arrays.asList(
     *         Map.of("id", new AttributeValue("item1")),
     *         Map.of("id", new AttributeValue("item2"))
     *     ));
     *
     * Map<String, KeysAndAttributes> requestItems = Map.of(
     *     "Products", keysAndAttrs
     * );
     *
     * // Typical: callback over the per-table results
     * asyncExecutor.batchGetItem(requestItems)
     *     .thenRunAsync(results -> {
     *         List<Map<String, Object>> products = results.get("Products");
     *         products.forEach(System.out::println);
     *     }); // returns ContinuableFuture<Map<String, List<Map<String, Object>>>>; keyed by table name
     *
     * // Typical: block for the result map
     * Map<String, List<Map<String, Object>>> results = asyncExecutor.batchGetItem(requestItems).get(); // returns e.g. {Products=[{id=item1}, {id=item2}]}
     *
     * // Edge: keys that match no rows are simply omitted; a table with no hits maps to an empty list
     * List<Map<String, Object>> products = results.get("Products"); // returns a (possibly empty) list, never null for a requested table
     * }</pre>
     *
     * @param requestItems a map where keys are table names and values are {@link KeysAndAttributes}
     *                    objects specifying the items to retrieve from each table, must not be {@code null}
     * @return a {@link ContinuableFuture} containing a map of table names to lists of retrieved items,
     *         where each item is represented as a Map of attribute names to values
     * @throws IllegalArgumentException if requestItems is {@code null} or exceeds batch limits
     * @see #batchGetItem(Map, String)
     */
    public ContinuableFuture<Map<String, List<Map<String, Object>>>> batchGetItem(final Map<String, KeysAndAttributes> requestItems) {
        return asyncExecutor.execute(() -> dbExecutor.batchGetItem(requestItems));
    }

    /**
     * Asynchronously performs a batch get operation with consumed capacity reporting.
     *
     * <p>This method extends the basic batch get by allowing you to track the read capacity consumed
     * by the operation asynchronously. This is particularly useful for monitoring and optimizing
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
     * <p><b>Return Consumed Capacity Options:</b></p>
     * <ul>
     * <li><b>INDEXES</b> - Returns capacity consumed by table and all indexes</li>
     * <li><b>TOTAL</b> - Returns only the total consumed capacity units</li>
     * <li><b>NONE</b> - No capacity details returned (default)</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * KeysAndAttributes userKeys = new KeysAndAttributes()
     *     .withKeys(Arrays.asList(
     *         Map.of("userId", new AttributeValue("user1")),
     *         Map.of("userId", new AttributeValue("user2"))
     *     ))
     *     .withConsistentRead(true);   // Higher capacity consumption
     *
     * Map<String, KeysAndAttributes> requestItems = Map.of("Users", userKeys);
     *
     * // Typical: callback over the per-table results
     * executor.batchGetItem(requestItems, "TOTAL")
     *     .thenRunAsync((results, ex) -> {
     *         if (ex != null) {
     *             logger.error("Batch get with capacity tracking failed", ex);
     *             return;
     *         }
     *         List<Map<String, Object>> users = results.get("Users");
     *         System.out.println("Retrieved " + users.size() + " users");
     *         // Note: this overload extracts only the per-table results. To inspect
     *         // ConsumedCapacity, use batchGetItem(BatchGetItemRequest) instead.
     *     }); // returns ContinuableFuture<Map<String, List<Map<String, Object>>>> (capacity not exposed by this overload)
     *
     * // Typical: block for the result map ("NONE" disables capacity reporting on the wire)
     * Map<String, List<Map<String, Object>>> results = executor.batchGetItem(requestItems, "NONE").get(); // returns e.g. {Users=[...]}
     *
     * // Edge: keys with no matching rows are omitted, so the per-table list may be empty
     * boolean empty = results.getOrDefault("Users", List.of()).isEmpty(); // returns true when nothing matched
     * }</pre>
     *
     * @param requestItems a map of table names to KeysAndAttributes specifying the items to retrieve, must not be {@code null}
     * @param returnConsumedCapacity determines the level of detail about consumed capacity returned:
     *                              "INDEXES" - returns capacity for table and indexes,
     *                              "TOTAL" - returns only total consumed capacity,
     *                              "NONE" - no capacity details returned
     * @return a ContinuableFuture containing a map of table names to lists of retrieved items
     * @throws IllegalArgumentException if requestItems is {@code null} or exceeds batch limits (100 items)
     */
    public ContinuableFuture<Map<String, List<Map<String, Object>>>> batchGetItem(final Map<String, KeysAndAttributes> requestItems,
            final String returnConsumedCapacity) {
        return asyncExecutor.execute(() -> dbExecutor.batchGetItem(requestItems, returnConsumedCapacity));
    }

    /**
     * Asynchronously performs a batch get operation using a complete BatchGetItemRequest for maximum control.
     *
     * <p>This method provides full control over batch get operations by accepting a fully configured
     * BatchGetItemRequest. You can retrieve up to 100 items across multiple tables with a single
     * asynchronous operation, with full control over consistency, projections, and capacity monitoring.</p>
     *
     * <p><b>Advanced Batch Features:</b></p>
     * <ul>
     * <li>Up to 100 items per request (16 MB limit) across multiple tables</li>
     * <li>Per-table projection expressions for attribute selection</li>
     * <li>Consistent read configuration per table</li>
     * <li>Return consumed capacity for monitoring and optimization</li>
     * <li>Expression attribute names for reserved word handling</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * KeysAndAttributes userKeys = new KeysAndAttributes()
     *     .withKeys(Arrays.asList(
     *         Map.of("userId", new AttributeValue("user1")),
     *         Map.of("userId", new AttributeValue("user2"))
     *     ))
     *     .withProjectionExpression("userId, name, email")
     *     .withConsistentRead(true);
     *
     * KeysAndAttributes orderKeys = new KeysAndAttributes()
     *     .withKeys(Arrays.asList(
     *         Map.of("orderId", new AttributeValue("order1")),
     *         Map.of("orderId", new AttributeValue("order2"))
     *     ))
     *     .withProjectionExpression("orderId, status, total");
     *
     * BatchGetItemRequest request = new BatchGetItemRequest()
     *     .withRequestItems(Map.of(
     *         "Users", userKeys,
     *         "Orders", orderKeys
     *     ))
     *     .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
     *
     * // Typical: callback over results from multiple tables
     * asyncExecutor.batchGetItem(request)
     *     .thenRunAsync((results, ex) -> {
     *         if (ex != null) {
     *             logger.error("Batch get failed", ex);
     *             return;
     *         }
     *         List<Map<String, Object>> users = results.get("Users");
     *         List<Map<String, Object>> orders = results.get("Orders");
     *         System.out.println("Retrieved " + users.size() + " users and " + orders.size() + " orders");
     *     }); // returns ContinuableFuture<Map<String, List<Map<String, Object>>>> keyed by table name
     *
     * // Typical: block for the result map
     * Map<String, List<Map<String, Object>>> results = asyncExecutor.batchGetItem(request).get(); // returns e.g. {Users=[...], Orders=[...]}
     *
     * // Edge: a request whose keys all miss yields empty per-table lists
     * boolean noUsers = results.getOrDefault("Users", List.of()).isEmpty(); // returns true when no Users matched
     * }</pre>
     *
     * @param batchGetItemRequest the complete BatchGetItemRequest with all parameters configured, must not be {@code null}
     * @return a {@link ContinuableFuture} containing a map of table names to lists of retrieved items
     * @throws IllegalArgumentException if batchGetItemRequest is {@code null} or exceeds batch limits (100 items)
     * @see BatchGetItemRequest
     * @see #batchGetItem(BatchGetItemRequest, Class)
     */
    public ContinuableFuture<Map<String, List<Map<String, Object>>>> batchGetItem(final BatchGetItemRequest batchGetItemRequest) {
        return asyncExecutor.execute(() -> dbExecutor.batchGetItem(batchGetItemRequest));
    }

    /**
     * Asynchronously performs a batch get operation with automatic type conversion.
     *
     * <p>This method retrieves multiple items from one or more tables and automatically converts
     * each item to the specified target type. This provides type safety and eliminates the need
     * for manual conversion from DynamoDB's AttributeValue format.</p>
     *
     * <p><b>Type Conversion Benefits:</b></p>
     * <ul>
     * <li>Compile-time type safety with generic return types</li>
     * <li>Automatic mapping from DynamoDB AttributeValues to Java objects</li>
     * <li>Support for all standard Java types and custom POJOs</li>
     * <li>Null handling for missing attributes</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * KeysAndAttributes orderKeys = new KeysAndAttributes()
     *     .withKeys(Arrays.asList(
     *         Map.of("orderId", new AttributeValue("ORDER-001")),
     *         Map.of("orderId", new AttributeValue("ORDER-002"))
     *     ))
     *     .withProjectionExpression("orderId, customerId, total, status");
     *
     * Map<String, KeysAndAttributes> requestItems = Map.of("Orders", orderKeys);
     *
     * // Typical: callback over per-table lists of converted POJOs
     * executor.batchGetItem(requestItems, Order.class)
     *     .thenRunAsync((results, ex) -> {
     *         if (ex != null) {
     *             logger.error("Batch get with type conversion failed", ex);
     *             return;
     *         }
     *         List<Order> orders = results.get("Orders");
     *         orders.forEach(order -> {
     *             System.out.println("Order " + order.getOrderId() +
     *                              ": $" + order.getTotal());
     *         });
     *     }); // returns ContinuableFuture<Map<String, List<Order>>> keyed by table name
     *
     * // Typical: block for the converted result map
     * Map<String, List<Order>> results = executor.batchGetItem(requestItems, Order.class).get(); // returns e.g. {Orders=[Order@.., Order@..]}
     *
     * // Edge: a table whose keys all miss maps to an empty list
     * boolean none = results.getOrDefault("Orders", List.of()).isEmpty(); // returns true when no orders matched
     * }</pre>
     *
     * @param <T> the type to convert retrieved items to
     * @param requestItems a map of table names to KeysAndAttributes specifying items to retrieve, must not be {@code null}
     * @param targetClass the class to convert each retrieved item to, must not be {@code null}
     * @return a ContinuableFuture containing a map of table names to lists of converted items
     * @throws IllegalArgumentException if requestItems or targetClass is {@code null}
     */
    public <T> ContinuableFuture<Map<String, List<T>>> batchGetItem(final Map<String, KeysAndAttributes> requestItems, final Class<T> targetClass) {
        return asyncExecutor.execute(() -> dbExecutor.batchGetItem(requestItems, targetClass));
    }

    /**
     * Asynchronously performs a batch get operation with type conversion and capacity monitoring.
     *
     * <p>This method combines automatic type conversion with capacity consumption tracking,
     * providing both type safety and performance monitoring in a single operation. This is
     * ideal for production applications that need both strong typing and cost optimization.</p>
     *
     * <p><b>Combined Benefits:</b></p>
     * <ul>
     * <li>Type-safe retrieval with automatic conversion</li>
     * <li>Capacity consumption monitoring for cost control</li>
     * <li>Performance metrics for scaling decisions</li>
     * <li>Production-ready error handling and logging</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * KeysAndAttributes productKeys = new KeysAndAttributes()
     *     .withKeys(productIds.stream()
     *         .map(id -> Map.of("productId", new AttributeValue(id)))
     *         .collect(Collectors.toList()))
     *     .withConsistentRead(false);   // Use eventually consistent for cost savings
     *
     * Map<String, KeysAndAttributes> requestItems = Map.of("Products", productKeys);
     *
     * // Typical: callback over converted POJOs while requesting capacity reporting
     * executor.batchGetItem(requestItems, "TOTAL", Product.class)
     *     .thenRunAsync((results, ex) -> {
     *         if (ex != null) {
     *             logger.error("Batch product lookup failed", ex);
     *             return;
     *         }
     *         List<Product> products = results.get("Products");
     *         logger.info("Retrieved {} products", products.size());
     *         products.forEach(this::processProduct);
     *     }); // returns ContinuableFuture<Map<String, List<Product>>> (capacity not exposed by this overload)
     *
     * // Typical: block for the converted result map
     * Map<String, List<Product>> results = executor.batchGetItem(requestItems, "NONE", Product.class).get(); // returns e.g. {Products=[Product@..]}
     *
     * // Edge: a table whose keys all miss maps to an empty list
     * boolean none = results.getOrDefault("Products", List.of()).isEmpty(); // returns true when no products matched
     * }</pre>
     *
     * @param <T> the type to convert retrieved items to
     * @param requestItems a map of table names to KeysAndAttributes specifying items to retrieve, must not be {@code null}
     * @param returnConsumedCapacity the level of capacity details to return ("INDEXES", "TOTAL", or "NONE")
     * @param targetClass the class to convert each retrieved item to, must not be {@code null}
     * @return a ContinuableFuture containing a map of table names to lists of converted items
     * @throws IllegalArgumentException if requestItems or targetClass is {@code null}
     */
    public <T> ContinuableFuture<Map<String, List<T>>> batchGetItem(final Map<String, KeysAndAttributes> requestItems, final String returnConsumedCapacity,
            final Class<T> targetClass) {
        return asyncExecutor.execute(() -> dbExecutor.batchGetItem(requestItems, returnConsumedCapacity, targetClass));
    }

    /**
     * Asynchronously performs a batch get operation using a {@link BatchGetItemRequest} with type conversion.
     *
     * <p>This method allows you to retrieve multiple items from one or more tables and convert
     * each item to the specified target type using a fully configured BatchGetItemRequest.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * BatchGetItemRequest request = new BatchGetItemRequest()
     *     .withRequestItems(Map.of(
     *         "Users", new KeysAndAttributes()
     *             .withKeys(Map.of("userId", new AttributeValue("user123")))
     *             .withProjectionExpression("userId, name, age")
     *     ));
     *
     * // Typical: callback over per-table lists of converted POJOs
     * asyncExecutor.batchGetItem(request, User.class)
     *     .thenRunAsync(results -> {
     *         List<User> users = results.get("Users");
     *         users.forEach(user -> System.out.println(user.getName()));
     *     }); // returns ContinuableFuture<Map<String, List<User>>> keyed by table name
     *
     * // Typical: block for the converted result map
     * Map<String, List<User>> results = asyncExecutor.batchGetItem(request, User.class).get(); // returns e.g. {Users=[User@..]}
     *
     * // Edge: tables not in the request are absent from the map (not present as empty)
     * boolean hasOrders = results.containsKey("Orders"); // returns false when "Orders" was not requested
     * }</pre>
     *
     * @param <T> the type to convert retrieved items to
     * @param batchGetItemRequest the complete BatchGetItemRequest with all parameters configured, must not be {@code null}
     * @param targetClass the class to convert each retrieved item to, must not be {@code null}
     * @return a {@link ContinuableFuture} whose payload is a map of table names to lists of items
     *         converted to type {@code T}; tables not in the request are absent from the map
     * @throws IllegalArgumentException if {@code batchGetItemRequest} or {@code targetClass} is {@code null}
     */
    public <T> ContinuableFuture<Map<String, List<T>>> batchGetItem(final BatchGetItemRequest batchGetItemRequest, final Class<T> targetClass) {
        return asyncExecutor.execute(() -> dbExecutor.batchGetItem(batchGetItemRequest, targetClass));
    }

    /**
     * Asynchronously puts an item into the specified DynamoDB table.
     *
     * <p>This method creates a new item or replaces an existing item with the same primary key.
     * By default, no information about the previous item is returned. Use the overloaded method
     * with returnValues parameter to retrieve the old item attributes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> item = new HashMap<>();
     * item.put("userId", new AttributeValue("12345"));
     * item.put("name", new AttributeValue("John Doe"));
     * item.put("age", new AttributeValue().withN("30"));
     *
     * // Typical: fire-and-react callback
     * asyncExecutor.putItem("Users", item)
     *     .thenRunAsync(result -> System.out.println("Item saved successfully")); // returns ContinuableFuture<PutItemResult>
     *
     * // Typical: block until the write completes
     * PutItemResult result = asyncExecutor.putItem("Users", item).get(); // returns a non-null PutItemResult
     *
     * // Edge: without a returnValues setting, no previous attributes are reported
     * Map<String, AttributeValue> old = result.getAttributes(); // returns null (use the returnValues overload for ALL_OLD)
     * }</pre>
     *
     * @param tableName the name of the DynamoDB table to put the item into, must not be {@code null}
     * @param item the item to put, represented as a map of attribute names to {@link AttributeValue} objects,
     *            must include all required attributes, must not be {@code null}
     * @return a {@link ContinuableFuture} containing the {@link PutItemResult} with operation metadata
     * @throws IllegalArgumentException if tableName or item is {@code null}
     * @see #putItem(String, Map, String)
     */
    public ContinuableFuture<PutItemResult> putItem(final String tableName, final Map<String, AttributeValue> item) {
        return asyncExecutor.execute(() -> dbExecutor.putItem(tableName, item));
    }

    /**
     * Asynchronously puts an item into the specified DynamoDB table with return value specification.
     *
     * <p>This method creates a new item or replaces an existing item with the same primary key.
     * The returnValues parameter allows you to retrieve information about the replaced item,
     * which is useful for tracking changes, implementing optimistic locking, or maintaining audit logs.</p>
     *
     * <p><b>Return Value Options:</b></p>
     * <ul>
     * <li><b>NONE</b> - No values returned (default, most efficient)</li>
     * <li><b>ALL_OLD</b> - Returns all attributes of the old item, if it existed</li>
     * <li><b>UPDATED_OLD</b> - Not applicable for PutItem (use UpdateItem instead)</li>
     * <li><b>ALL_NEW</b> - Not applicable for PutItem (use UpdateItem instead)</li>
     * <li><b>UPDATED_NEW</b> - Not applicable for PutItem (use UpdateItem instead)</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> newItem = Map.of(
     *     "userId", new AttributeValue("user123"),
     *     "name", new AttributeValue("Jane Doe"),
     *     "version", new AttributeValue().withN("2")
     * );
     *
     * // Typical: request the previous attributes via "ALL_OLD"
     * asyncExecutor.putItem("Users", newItem, "ALL_OLD")
     *     .thenRunAsync((result, ex) -> {
     *         if (ex != null) {
     *             logger.error("Put item failed", ex);
     *             return;
     *         }
     *         Map<String, AttributeValue> oldItem = result.getAttributes();
     *         if (oldItem != null && !oldItem.isEmpty()) {
     *             System.out.println("Replaced existing user: " + oldItem.get("name").getS());
     *             System.out.println("Old version: " + oldItem.get("version").getN());
     *         } else {
     *             System.out.println("Created new user");
     *         }
     *     }); // returns ContinuableFuture<PutItemResult>; getAttributes() holds the prior item when it existed
     *
     * // Typical: block and inspect the prior attributes
     * PutItemResult result = asyncExecutor.putItem("Users", newItem, "ALL_OLD").get(); // returns a non-null PutItemResult
     *
     * // Edge: with "NONE" the result carries no prior attributes
     * Map<String, AttributeValue> old = asyncExecutor.putItem("Users", newItem, "NONE").get().getAttributes(); // returns null
     * }</pre>
     *
     * @param tableName the name of the DynamoDB table to put the item into, must not be {@code null}
     * @param item the item to put, represented as a map of attribute names to {@link AttributeValue} objects,
     *            must include all required attributes, must not be {@code null}
     * @param returnValues specifies what values to return: "NONE" (default) or "ALL_OLD" for PutItem operations
     * @return a {@link ContinuableFuture} containing the {@link PutItemResult} with operation metadata
     *         and optionally the old item's attributes if returnValues is "ALL_OLD"
     * @throws IllegalArgumentException if tableName or item is {@code null}
     * @see #putItem(String, Map)
     * @see #putItem(PutItemRequest)
     */
    public ContinuableFuture<PutItemResult> putItem(final String tableName, final Map<String, AttributeValue> item, final String returnValues) {
        return asyncExecutor.execute(() -> dbExecutor.putItem(tableName, item, returnValues));
    }

    /**
     * Asynchronously puts an item using a complete PutItemRequest with AWS SDK v1.
     *
     * <p>This method provides maximum flexibility by accepting a fully configured PutItemRequest.
     * You can specify all DynamoDB PutItem parameters including condition expressions,
     * expected values, return value specifications, and capacity monitoring.</p>
     *
     * <p><b>Advanced Features Available:</b></p>
     * <ul>
     * <li>Conditional puts with expected value conditions</li>
     * <li>Return value specifications for retrieving old item values</li>
     * <li>Return consumed capacity for monitoring and optimization</li>
     * <li>Return item collection metrics for tables with local secondary indexes</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * PutItemRequest request = new PutItemRequest()
     *     .withTableName("Users")
     *     .withItem(Map.of(
     *         "userId", new AttributeValue("user123"),
     *         "email", new AttributeValue("user@example.com"),
     *         "version", new AttributeValue().withN("1")
     *     ))
     *     .withExpected(Map.of(
     *         "userId", new ExpectedAttributeValue(false)  // Only create if new
     *     ))
     *     .withReturnValues(ReturnValue.ALL_OLD)
     *     .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
     *
     * ContinuableFuture<PutItemResult> future = executor.putItem(request); // returns immediately; work runs on the AsyncExecutor
     *
     * // Typical: handle success or a failed conditional check on a callback
     * future.thenRunAsync((result, ex) -> {
     *         if (ex != null) {
     *             if (ex.getCause() instanceof ConditionalCheckFailedException) {
     *                 System.err.println("User already exists");
     *             }
     *             return;
     *         }
     *         System.out.println("Consumed capacity: " + result.getConsumedCapacity().getCapacityUnits());
     *         if (result.getAttributes() != null) {
     *             System.out.println("Replaced existing item");
     *         }
     *     }); // returns ContinuableFuture<Void>; runs after the put completes
     *
     * // Typical: block for the result
     * PutItemResult result = future.get(); // returns a non-null PutItemResult (the request used ReturnValue.ALL_OLD)
     * }</pre>
     *
     * @param putItemRequest the complete PutItemRequest with all parameters configured. Must not be null.
     * @return a ContinuableFuture containing the PutItemResult with operation results
     * @throws IllegalArgumentException if putItemRequest is null
     * @see PutItemRequest
     * @see PutItemResult
     */
    public ContinuableFuture<PutItemResult> putItem(final PutItemRequest putItemRequest) {
        return asyncExecutor.execute(() -> dbExecutor.putItem(putItemRequest));
    }

    /**
     * Asynchronously puts an entity (POJO) into the specified table.
     *
     * <p>The entity is converted to a map of {@link AttributeValue}s by the underlying
     * {@link DynamoDBExecutor} before being written. This method is package-private
     * because the {@code Object} parameter would overload-clash with the
     * {@link #putItem(String, Map)} entry point and create resolution ambiguity for
     * callers passing {@code Map}-typed entities; public callers should serialize their
     * entity to a {@code Map<String, AttributeValue>} and use the public overload, or
     * obtain the wrapper through other API entry points.</p>
     *
     * @param tableName the name of the table to put the entity into (must not be null)
     * @param entity the entity object to put (must not be null)
     * @return a {@link ContinuableFuture} whose payload is the {@link PutItemResult}
     *         returned by the underlying synchronous operation
     */
    ContinuableFuture<PutItemResult> putItem(final String tableName, final Object entity) {
        return asyncExecutor.execute(() -> dbExecutor.putItem(tableName, entity));
    }

    /**
     * Asynchronously puts an entity (POJO) into the specified table with return value specification.
     *
     * <p>The entity is converted to a map of {@link AttributeValue}s by the underlying
     * {@link DynamoDBExecutor} before being written. This method is package-private for the
     * same reason as {@link #putItem(String, Object)} — the {@code Object} parameter would
     * overload-clash with the {@code Map}-accepting public entry point.</p>
     *
     * @param tableName the name of the table to put the entity into (must not be null)
     * @param entity the entity object to put (must not be null)
     * @param returnValues specifies what values to return: "NONE" (default) or "ALL_OLD" for PutItem operations
     * @return a {@link ContinuableFuture} whose payload is the {@link PutItemResult}
     *         returned by the underlying synchronous operation; when {@code returnValues}
     *         is {@code "ALL_OLD"} the result's {@code getAttributes()} contains the previous item
     */
    ContinuableFuture<PutItemResult> putItem(final String tableName, final Object entity, final String returnValues) {
        return asyncExecutor.execute(() -> dbExecutor.putItem(tableName, entity, returnValues));
    }

    /**
     * Asynchronously performs a batch write operation to put or delete multiple items across one or more tables.
     *
     * <p>This method allows you to perform up to 25 put or delete operations in a single call, with a
     * maximum total request size of 16 MB. Batch write operations provide better throughput than individual
     * write operations and are useful for bulk data loading or deletion.</p>
     *
     * <p><b>Key Features:</b></p>
     * <ul>
     * <li>Up to 25 put or delete requests in a single batch</li>
     * <li>Can write to multiple tables in one operation</li>
     * <li>Automatic handling of unprocessed items in the result</li>
     * <li>More cost-effective than individual writes</li>
     * </ul>
     *
     * <p><b>Important Notes:</b></p>
     * <ul>
     * <li>Individual item writes are atomic, but the batch as a whole is not</li>
     * <li>Some items may fail while others succeed</li>
     * <li>Check result for unprocessed items and retry if needed</li>
     * <li>No condition expressions or return values supported</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<WriteRequest> writeRequests = Arrays.asList(
     *     new WriteRequest(new PutRequest(Map.of(
     *         "userId", new AttributeValue("user1"),
     *         "name", new AttributeValue("Alice")
     *     ))),
     *     new WriteRequest(new DeleteRequest(Map.of(
     *         "userId", new AttributeValue("user2")
     *     )))
     * );
     *
     * Map<String, List<WriteRequest>> requestItems = Map.of("Users", writeRequests);
     *
     * // Typical: callback that checks for unprocessed items
     * executor.batchWriteItem(requestItems)
     *     .thenRunAsync((result, ex) -> {
     *         if (ex != null) {
     *             logger.error("Batch write failed", ex);
     *             return;
     *         }
     *         if (result.getUnprocessedItems() != null && !result.getUnprocessedItems().isEmpty()) {
     *             System.out.println("Some items were not processed, retry needed");
     *         } else {
     *             System.out.println("All items processed successfully");
     *         }
     *     }); // returns ContinuableFuture<BatchWriteItemResult>
     *
     * // Typical: block for the result
     * BatchWriteItemResult result = executor.batchWriteItem(requestItems).get(); // returns a non-null BatchWriteItemResult
     *
     * // Edge: when everything was processed, the unprocessed-items map is empty (or null)
     * Map<String, List<WriteRequest>> unprocessed = result.getUnprocessedItems(); // returns an empty map (or null) on full success
     * }</pre>
     *
     * @param requestItems a map where keys are table names and values are lists of {@link WriteRequest} objects
     *                     (containing either PutRequest or DeleteRequest), must not be {@code null}
     * @return a {@link ContinuableFuture} containing the {@link BatchWriteItemResult} with information about
     *         consumed capacity and any unprocessed items that need to be retried
     * @throws IllegalArgumentException if requestItems is {@code null} or exceeds 25 write requests
     * @see #batchWriteItem(BatchWriteItemRequest)
     */
    public ContinuableFuture<BatchWriteItemResult> batchWriteItem(final Map<String, List<WriteRequest>> requestItems) {
        return asyncExecutor.execute(() -> dbExecutor.batchWriteItem(requestItems));
    }

    /**
     * Asynchronously performs a batch write operation using a complete BatchWriteItemRequest with AWS SDK v1.
     *
     * <p>This method provides full control over batch write operations by accepting a fully configured
     * BatchWriteItemRequest. You can specify all batch write parameters including return consumed capacity
     * and return item collection metrics for monitoring and optimization.</p>
     *
     * <p><b>Advanced Features Available:</b></p>
     * <ul>
     * <li>Return consumed capacity for cost monitoring and optimization</li>
     * <li>Return item collection metrics for tables with local secondary indexes</li>
     * <li>Full control over request items across multiple tables</li>
     * <li>Custom request timeout and retry configurations</li>
     * </ul>
     *
     * <p><b>Unprocessed Items Handling:</b></p>
     * <p>DynamoDB may not process all items in a single request due to throughput limits or
     * other constraints. Always check the response for unprocessed items and implement
     * an exponential backoff retry strategy for production applications.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<WriteRequest> userWrites = Arrays.asList(
     *     new WriteRequest(new PutRequest(Map.of(
     *         "userId", new AttributeValue("user1"),
     *         "email", new AttributeValue("user1@example.com"),
     *         "status", new AttributeValue("ACTIVE")
     *     ))),
     *     new WriteRequest(new PutRequest(Map.of(
     *         "userId", new AttributeValue("user2"),
     *         "email", new AttributeValue("user2@example.com"),
     *         "status", new AttributeValue("ACTIVE")
     *     )))
     * );
     *
     * BatchWriteItemRequest request = new BatchWriteItemRequest()
     *     .withRequestItems(Map.of("Users", userWrites))
     *     .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
     *     .withReturnItemCollectionMetrics(ReturnItemCollectionMetrics.SIZE);
     *
     * ContinuableFuture<BatchWriteItemResult> future = executor.batchWriteItem(request); // returns immediately; work runs on the AsyncExecutor
     *
     * // Typical: callback that sums capacity and retries unprocessed items
     * future.thenRunAsync((result, ex) -> {
     *         if (ex != null) {
     *             logger.error("Batch write failed", ex);
     *             return;
     *         }
     *         System.out.println("Consumed capacity: " +
     *             result.getConsumedCapacity().stream()
     *                 .mapToDouble(c -> c.getCapacityUnits())
     *                 .sum());
     *
     *         // Handle unprocessed items with exponential backoff
     *         Map<String, List<WriteRequest>> unprocessed = result.getUnprocessedItems();
     *         if (unprocessed != null && !unprocessed.isEmpty()) {
     *             System.out.println("Retrying " + unprocessed.size() + " unprocessed items");
     *             retryWithBackoff(unprocessed);
     *         }
     *     }); // returns ContinuableFuture<Void>; runs after the batch write completes
     *
     * // Typical: block for the result
     * BatchWriteItemResult result = future.get(); // returns a non-null BatchWriteItemResult
     * }</pre>
     *
     * @param batchWriteItemRequest the complete BatchWriteItemRequest with all parameters configured. Must not be null.
     * @return a ContinuableFuture containing the BatchWriteItemResult with consumed capacity,
     *         item collection metrics, and any unprocessed items
     * @throws IllegalArgumentException if batchWriteItemRequest is null or contains more than 25 write requests
     * @see BatchWriteItemRequest
     * @see BatchWriteItemResult
     * @see #batchWriteItem(Map)
     */
    public ContinuableFuture<BatchWriteItemResult> batchWriteItem(final BatchWriteItemRequest batchWriteItemRequest) {
        return asyncExecutor.execute(() -> dbExecutor.batchWriteItem(batchWriteItemRequest));
    }

    /**
     * Asynchronously updates specific attributes of an item in the specified DynamoDB table.
     *
     * <p>This method performs partial updates on an existing item, modifying only the specified
     * attributes while leaving other attributes unchanged. If the item doesn't exist, a new item
     * is created with the specified attributes (unless a conditional expression prevents it).</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> key = Map.of("userId", new AttributeValue("12345"));
     *
     * Map<String, AttributeValueUpdate> updates = new HashMap<>();
     * updates.put("lastLogin", new AttributeValueUpdate()
     *     .withAction(AttributeAction.PUT)
     *     .withValue(new AttributeValue(Instant.now().toString())));
     * updates.put("loginCount", new AttributeValueUpdate()
     *     .withAction(AttributeAction.ADD)
     *     .withValue(new AttributeValue().withN("1")));
     *
     * // Typical: fire-and-react callback
     * asyncExecutor.updateItem("Users", key, updates)
     *     .thenRunAsync(result -> System.out.println("User updated")); // returns ContinuableFuture<UpdateItemResult>
     *
     * // Typical: block until the update completes
     * UpdateItemResult result = asyncExecutor.updateItem("Users", key, updates).get(); // returns a non-null UpdateItemResult
     *
     * // Edge: without a returnValues setting, no attributes are reported back
     * Map<String, AttributeValue> changed = result.getAttributes(); // returns null (use the returnValues overload to fetch them)
     * }</pre>
     *
     * @param tableName the name of the DynamoDB table containing the item to update, must not be {@code null}
     * @param key the primary key identifying the item to update, must include all key attributes,
     *           must not be {@code null}
     * @param attributeUpdates a map of attribute names to {@link AttributeValueUpdate} objects specifying
     *                        the update actions, must not be {@code null}
     * @return a {@link ContinuableFuture} containing the {@link UpdateItemResult} with operation metadata
     * @throws IllegalArgumentException if any parameter is {@code null}
     * @see #updateItem(String, Map, Map, String)
     */
    public ContinuableFuture<UpdateItemResult> updateItem(final String tableName, final Map<String, AttributeValue> key,
            final Map<String, AttributeValueUpdate> attributeUpdates) {
        return asyncExecutor.execute(() -> dbExecutor.updateItem(tableName, key, attributeUpdates));
    }

    /**
     * Asynchronously updates specific attributes of an item in the specified DynamoDB table with return value specification.
     *
     * <p>This method performs partial updates on an existing item, modifying only the specified attributes
     * while leaving other attributes unchanged. The returnValues parameter controls what data is returned
     * after the update, which is useful for retrieving the updated values without an additional read operation.</p>
     *
     * <p><b>Return Value Options:</b></p>
     * <ul>
     * <li><b>NONE</b> - No values returned (default, most efficient)</li>
     * <li><b>ALL_OLD</b> - Returns all attributes before the update</li>
     * <li><b>UPDATED_OLD</b> - Returns only the updated attributes before the update</li>
     * <li><b>ALL_NEW</b> - Returns all attributes after the update</li>
     * <li><b>UPDATED_NEW</b> - Returns only the updated attributes after the update</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> key = Map.of("productId", new AttributeValue("PROD-456"));
     *
     * Map<String, AttributeValueUpdate> updates = new HashMap<>();
     * updates.put("price", new AttributeValueUpdate()
     *     .withAction(AttributeAction.PUT)
     *     .withValue(new AttributeValue().withN("29.99")));
     * updates.put("stock", new AttributeValueUpdate()
     *     .withAction(AttributeAction.ADD)
     *     .withValue(new AttributeValue().withN("-1")));  // Decrement by 1
     *
     * // Typical: request the post-update state via "ALL_NEW"
     * asyncExecutor.updateItem("Products", key, updates, "ALL_NEW")
     *     .thenRunAsync((result, ex) -> {
     *         if (ex != null) {
     *             logger.error("Update failed", ex);
     *             return;
     *         }
     *         Map<String, AttributeValue> updatedItem = result.getAttributes();
     *         System.out.println("New price: $" + updatedItem.get("price").getN());
     *         System.out.println("Remaining stock: " + updatedItem.get("stock").getN());
     *     }); // returns ContinuableFuture<UpdateItemResult>; getAttributes() holds all post-update attributes
     *
     * // Typical: block and read the post-update attributes
     * UpdateItemResult result = asyncExecutor.updateItem("Products", key, updates, "ALL_NEW").get(); // returns a non-null UpdateItemResult
     *
     * // Edge: with "NONE" the result carries no attributes
     * Map<String, AttributeValue> attrs = asyncExecutor.updateItem("Products", key, updates, "NONE").get().getAttributes(); // returns null
     * }</pre>
     *
     * @param tableName the name of the DynamoDB table containing the item to update, must not be {@code null}
     * @param key the primary key identifying the item to update, must include all key attributes, must not be {@code null}
     * @param attributeUpdates a map of attribute names to {@link AttributeValueUpdate} objects specifying
     *                        the update actions (PUT, ADD, DELETE), must not be {@code null}
     * @param returnValues specifies what values to return: "NONE", "ALL_OLD", "UPDATED_OLD", "ALL_NEW", or "UPDATED_NEW"
     * @return a {@link ContinuableFuture} containing the {@link UpdateItemResult} with operation metadata
     *         and optionally the item's attributes based on the returnValues parameter
     * @throws IllegalArgumentException if any parameter is {@code null}
     * @see #updateItem(String, Map, Map)
     * @see #updateItem(UpdateItemRequest)
     */
    public ContinuableFuture<UpdateItemResult> updateItem(final String tableName, final Map<String, AttributeValue> key,
            final Map<String, AttributeValueUpdate> attributeUpdates, final String returnValues) {
        return asyncExecutor.execute(() -> dbExecutor.updateItem(tableName, key, attributeUpdates, returnValues));
    }

    /**
     * Asynchronously updates an item using a complete UpdateItemRequest with AWS SDK v1.
     *
     * <p>This method provides maximum flexibility for update operations by accepting a fully
     * configured UpdateItemRequest. You can use attribute-based updates, conditional updates,
     * and access all DynamoDB update features available in SDK v1.</p>
     *
     * <p><b>Advanced Update Features:</b></p>
     * <ul>
     * <li>Attribute updates with PUT, ADD, and DELETE actions</li>
     * <li>Conditional updates with expected attribute values</li>
     * <li>Return value specifications for retrieving old/new values</li>
     * <li>Atomic counters and set operations</li>
     * <li>Capacity consumption monitoring</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * UpdateItemRequest request = new UpdateItemRequest()
     *     .withTableName("Users")
     *     .withKey(asKey("userId", "user123"))
     *     .withAttributeUpdates(Map.of(
     *         "loginCount", new AttributeValueUpdate(
     *             new AttributeValue().withN("1"),
     *             AttributeAction.ADD
     *         ),
     *         "lastLogin", new AttributeValueUpdate(
     *             new AttributeValue(Instant.now().toString()),
     *             AttributeAction.PUT
     *         )
     *     ))
     *     .withExpected(Map.of(
     *         "userId", new ExpectedAttributeValue(true)  // Only update if exists
     *     ))
     *     .withReturnValues(ReturnValue.UPDATED_NEW);
     *
     * ContinuableFuture<UpdateItemResult> future = executor.updateItem(request); // returns immediately; work runs on the AsyncExecutor
     *
     * // Typical: handle success or a failed conditional check on a callback
     * future.thenRunAsync((result, ex) -> {
     *         if (ex != null) {
     *             if (ex.getCause() instanceof ConditionalCheckFailedException) {
     *                 System.err.println("Item doesn't exist for update");
     *             }
     *             return;
     *         }
     *         Map<String, AttributeValue> updatedAttrs = result.getAttributes();
     *         System.out.println("Updated attributes: " + updatedAttrs);
     *     }); // returns ContinuableFuture<Void>; getAttributes() holds the updated attributes (ReturnValue.UPDATED_NEW)
     *
     * // Typical: block for the result
     * UpdateItemResult result = future.get(); // returns a non-null UpdateItemResult
     * }</pre>
     *
     * @param updateItemRequest the complete UpdateItemRequest with all parameters configured. Must not be null.
     * @return a ContinuableFuture containing the UpdateItemResult with operation results
     * @throws IllegalArgumentException if updateItemRequest is null
     * @see UpdateItemRequest
     * @see UpdateItemResult
     */
    public ContinuableFuture<UpdateItemResult> updateItem(final UpdateItemRequest updateItemRequest) {
        return asyncExecutor.execute(() -> dbExecutor.updateItem(updateItemRequest));
    }

    /**
     * Asynchronously deletes an item from the specified DynamoDB table.
     *
     * <p>This method removes an entire item from the table. If the item doesn't exist,
     * the operation completes successfully without error. By default, no information
     * about the deleted item is returned.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> key = Map.of(
     *     "userId", new AttributeValue("12345")
     * );
     *
     * // Typical: fire-and-react callback
     * asyncExecutor.deleteItem("Users", key)
     *     .thenRunAsync((result, ex) -> {
     *         if (ex != null) {
     *             System.err.println("Delete failed: " + ex.getMessage());
     *         } else {
     *             System.out.println("Item deleted");
     *         }
     *     }); // returns ContinuableFuture<DeleteItemResult>
     *
     * // Typical: block until the delete completes
     * DeleteItemResult result = asyncExecutor.deleteItem("Users", key).get(); // returns a non-null DeleteItemResult
     *
     * // Edge: deleting a key that does not exist still completes normally (no error)
     * Map<String, AttributeValue> absent = Map.of("userId", new AttributeValue("does-not-exist"));
     * DeleteItemResult none = asyncExecutor.deleteItem("Users", absent).get(); // returns a non-null DeleteItemResult; getAttributes() is null
     * }</pre>
     *
     * @param tableName the name of the DynamoDB table to delete the item from, must not be {@code null}
     * @param key the primary key identifying the item to delete, must include all key attributes,
     *           must not be {@code null}
     * @return a {@link ContinuableFuture} containing the {@link DeleteItemResult} with operation metadata
     * @throws IllegalArgumentException if tableName or key is {@code null}
     * @see #deleteItem(String, Map, String)
     */
    public ContinuableFuture<DeleteItemResult> deleteItem(final String tableName, final Map<String, AttributeValue> key) {
        return asyncExecutor.execute(() -> dbExecutor.deleteItem(tableName, key));
    }

    /**
     * Asynchronously deletes an item from the specified DynamoDB table with return value specification.
     *
     * <p>This method removes an entire item from the table. If the item doesn't exist, the operation
     * completes successfully without error. The returnValues parameter allows you to retrieve the
     * deleted item's attributes, which is useful for maintaining audit logs, implementing undo
     * functionality, or tracking deleted records.</p>
     *
     * <p><b>Return Value Options:</b></p>
     * <ul>
     * <li><b>NONE</b> - No values returned (default, most efficient)</li>
     * <li><b>ALL_OLD</b> - Returns all attributes of the deleted item, if it existed</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, AttributeValue> key = Map.of(
     *     "orderId", new AttributeValue("ORDER-789")
     * );
     *
     * // Typical: request the deleted attributes via "ALL_OLD"
     * asyncExecutor.deleteItem("Orders", key, "ALL_OLD")
     *     .thenRunAsync((result, ex) -> {
     *         if (ex != null) {
     *             logger.error("Delete failed", ex);
     *             return;
     *         }
     *         Map<String, AttributeValue> deletedItem = result.getAttributes();
     *         if (deletedItem != null && !deletedItem.isEmpty()) {
     *             // Archive the deleted order
     *             archiveOrder(deletedItem);
     *             System.out.println("Deleted order: " + deletedItem.get("orderId").getS());
     *         } else {
     *             System.out.println("Order did not exist");
     *         }
     *     }); // returns ContinuableFuture<DeleteItemResult>; getAttributes() holds the deleted item when it existed
     *
     * // Typical: block and inspect the deleted attributes
     * DeleteItemResult result = asyncExecutor.deleteItem("Orders", key, "ALL_OLD").get(); // returns a non-null DeleteItemResult
     *
     * // Edge: deleting a missing key with "ALL_OLD" returns a result whose getAttributes() is null
     * Map<String, AttributeValue> miss = Map.of("orderId", new AttributeValue("nope"));
     * Map<String, AttributeValue> old = asyncExecutor.deleteItem("Orders", miss, "ALL_OLD").get().getAttributes(); // returns null
     * }</pre>
     *
     * @param tableName the name of the DynamoDB table to delete the item from, must not be {@code null}
     * @param key the primary key identifying the item to delete, must include all key attributes, must not be {@code null}
     * @param returnValues specifies what values to return: "NONE" (default) or "ALL_OLD" to retrieve the deleted item
     * @return a {@link ContinuableFuture} containing the {@link DeleteItemResult} with operation metadata
     *         and optionally the deleted item's attributes if returnValues is "ALL_OLD"
     * @throws IllegalArgumentException if tableName or key is {@code null}
     * @see #deleteItem(String, Map)
     * @see #deleteItem(DeleteItemRequest)
     */
    public ContinuableFuture<DeleteItemResult> deleteItem(final String tableName, final Map<String, AttributeValue> key, final String returnValues) {
        return asyncExecutor.execute(() -> dbExecutor.deleteItem(tableName, key, returnValues));
    }

    /**
     * Asynchronously deletes an item using a complete DeleteItemRequest with AWS SDK v1.
     *
     * <p>This method provides maximum flexibility for delete operations by accepting a fully
     * configured DeleteItemRequest. You can use conditional deletes, return value specifications,
     * and monitor capacity consumption.</p>
     *
     * <p><b>Advanced Delete Features:</b></p>
     * <ul>
     * <li>Conditional deletes with expected attribute values</li>
     * <li>Return value specifications for retrieving deleted item values</li>
     * <li>Capacity consumption monitoring</li>
     * <li>Item collection metrics for tables with local secondary indexes</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DeleteItemRequest request = new DeleteItemRequest()
     *     .withTableName("Users")
     *     .withKey(asKey("userId", "user123"))
     *     .withExpected(Map.of(
     *         "status", new ExpectedAttributeValue(
     *             new AttributeValue("INACTIVE")  // Only delete if inactive
     *         )
     *     ))
     *     .withReturnValues(ReturnValue.ALL_OLD)
     *     .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
     *
     * ContinuableFuture<DeleteItemResult> future = executor.deleteItem(request); // returns immediately; work runs on the AsyncExecutor
     *
     * // Typical: handle success or a failed conditional check on a callback
     * future.thenRunAsync((result, ex) -> {
     *         if (ex != null) {
     *             if (ex.getCause() instanceof ConditionalCheckFailedException) {
     *                 System.err.println("Cannot delete - user is not inactive");
     *             }
     *             return;
     *         }
     *         if (result.getAttributes() != null) {
     *             System.out.println("Deleted user: " + result.getAttributes().get("name").getS());
     *         } else {
     *             System.out.println("User was already deleted");
     *         }
     *         System.out.println("Consumed capacity: " + result.getConsumedCapacity().getCapacityUnits());
     *     }); // returns ContinuableFuture<Void>; getAttributes() holds the deleted item (ReturnValue.ALL_OLD)
     *
     * // Typical: block for the result
     * DeleteItemResult result = future.get(); // returns a non-null DeleteItemResult
     * }</pre>
     *
     * @param deleteItemRequest the complete DeleteItemRequest with all parameters configured. Must not be null.
     * @return a ContinuableFuture containing the DeleteItemResult with operation results
     * @throws IllegalArgumentException if deleteItemRequest is null
     * @see DeleteItemRequest
     * @see DeleteItemResult
     */
    public ContinuableFuture<DeleteItemResult> deleteItem(final DeleteItemRequest deleteItemRequest) {
        return asyncExecutor.execute(() -> dbExecutor.deleteItem(deleteItemRequest));
    }

    /**
     * Asynchronously executes a DynamoDB query and returns all matching items as a list.
     *
     * <p>This method performs a query operation using the specified key conditions and optional
     * filter expressions. Query operations are efficient for retrieving items with a specific
     * partition key value and optional sort key conditions. All matching items are collected
     * into a list, handling pagination automatically.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * QueryRequest request = new QueryRequest()
     *     .withTableName("Orders")
     *     .withKeyConditionExpression("customerId = :custId")
     *     .withExpressionAttributeValues(Map.of(
     *         ":custId", new AttributeValue("12345")
     *     ))
     *     .withScanIndexForward(false);   // Sort descending
     *
     * // Typical: callback over all matching rows (pagination handled internally)
     * asyncExecutor.list(request)
     *     .thenRunAsync(orders -> {
     *         System.out.println("Found " + orders.size() + " orders");
     *         orders.forEach(order -> System.out.println(order.get("orderId")));
     *     }); // returns ContinuableFuture<List<Map<String, Object>>>
     *
     * // Typical: block for the full list
     * List<Map<String, Object>> orders = asyncExecutor.list(request).get(); // returns a list of attribute maps
     *
     * // Edge: a query that matches nothing yields an empty list (never null)
     * List<Map<String, Object>> empty = asyncExecutor.list(noMatchRequest).get(); // returns an empty list
     * }</pre>
     *
     * @param queryRequest the {@link QueryRequest} specifying table name, key conditions,
     *                    filter expressions, and other query parameters, must not be {@code null}
     * @return a {@link ContinuableFuture} containing a list of all items matching the query,
     *         where each item is represented as a Map of attribute names to values
     * @throws IllegalArgumentException if queryRequest is {@code null}
     * @see #stream(QueryRequest)
     */
    public ContinuableFuture<List<Map<String, Object>>> list(final QueryRequest queryRequest) {
        return asyncExecutor.execute(() -> dbExecutor.list(queryRequest));
    }

    /**
     * Asynchronously executes a query and returns all matching items as a typed list.
     *
     * <p>This method performs a Query operation that retrieves items with the same partition key,
     * applies any specified filter conditions, and converts each item to the specified target type.
     * It automatically handles pagination to return ALL matching items in a single list.</p>
     *
     * <p><b>Type Conversion Benefits:</b></p>
     * <ul>
     * <li>Compile-time type safety with generic return types</li>
     * <li>Automatic conversion from DynamoDB AttributeValues to Java objects</li>
     * <li>Support for primitive types, collections, and custom POJOs</li>
     * <li>Null-safe handling of missing or null attributes</li>
     * </ul>
     *
     * <p><b>Important Notes:</b></p>
     * <ul>
     * <li>Automatically handles pagination - retrieves ALL results</li>
     * <li>May perform multiple synchronous API calls internally</li>
     * <li>Results are loaded entirely into memory</li>
     * <li>Consider using stream() for very large result sets</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * QueryRequest queryRequest = new QueryRequest()
     *     .withTableName("Orders")
     *     .withKeyConditionExpression("customerId = :customerId")
     *     .withExpressionAttributeValues(Map.of(
     *         ":customerId", new AttributeValue("CUSTOMER123")
     *     ))
     *     .withFilterExpression("#status = :status")
     *     .withExpressionAttributeNames(Map.of("#status", "status"))
     *     .withExpressionAttributeValues(Map.of(
     *         ":status", new AttributeValue("SHIPPED")
     *     ));
     *
     * ContinuableFuture<List<Order>> future = executor.list(queryRequest, Order.class); // returns immediately; work runs on the AsyncExecutor
     *
     * // Typical: callback over the converted list
     * future.thenRunAsync((orders, ex) -> {
     *         if (ex != null) {
     *             logger.error("Query failed", ex);
     *             return;
     *         }
     *         System.out.println("Found " + orders.size() + " shipped orders");
     *         orders.forEach(order ->
     *             System.out.println("Order " + order.getOrderId() + ": $" + order.getTotal()));
     *     }); // returns ContinuableFuture<Void>; runs after the query completes
     *
     * // Typical: block for the converted list
     * List<Order> orders = future.get(); // returns a list of Order instances
     *
     * // Edge: a query that matches nothing yields an empty list (never null)
     * List<Order> empty = executor.list(noMatchRequest, Order.class).get(); // returns an empty list
     * }</pre>
     *
     * @param <T> the type to convert each item to
     * @param queryRequest the QueryRequest with all parameters configured. Must not be null.
     * @param targetClass the class to convert each item to. Must not be null.
     * @return a ContinuableFuture containing a list of all matching items converted to type T
     * @throws IllegalArgumentException if queryRequest or targetClass is null
     * @see #list(QueryRequest)
     * @see #stream(QueryRequest, Class)
     */
    public <T> ContinuableFuture<List<T>> list(final QueryRequest queryRequest, final Class<T> targetClass) {
        return asyncExecutor.execute(() -> dbExecutor.list(queryRequest, targetClass));
    }

    /**
     * Asynchronously executes a DynamoDB query and returns the results as a Dataset.
     *
     * <p>This method performs a query operation and converts the results into a {@link Dataset},
     * which provides a tabular view of the data with additional analytical capabilities.
     * The Dataset preserves column order and provides methods for data manipulation and analysis.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * QueryRequest request = new QueryRequest()
     *     .withTableName("Products")
     *     .withIndexName("CategoryIndex")
     *     .withKeyConditionExpression("category = :cat")
     *     .withExpressionAttributeValues(Map.of(
     *         ":cat", new AttributeValue("Electronics")
     *     ));
     *
     * // Typical: callback over the resulting Dataset
     * asyncExecutor.query(request)
     *     .thenRunAsync(dataset -> {
     *         dataset.println();   // Print tabular view
     *         double avgPrice = dataset.<String>getColumn("price").stream()
     *             .mapToDouble(Double::parseDouble)
     *             .average().orElse(0);
     *         System.out.println("Average price: " + avgPrice);
     *     }); // returns ContinuableFuture<Dataset>
     *
     * // Typical: block for the Dataset
     * Dataset ds = asyncExecutor.query(request).get(); // returns a non-null Dataset (rows may be 0)
     *
     * // Edge: a query that matches nothing yields an empty (non-null) Dataset
     * boolean empty = asyncExecutor.query(noMatchRequest).get().isEmpty(); // returns true
     * }</pre>
     *
     * @param queryRequest the {@link QueryRequest} specifying table name, key conditions,
     *                    filter expressions, and other query parameters, must not be {@code null}
     * @return a {@link ContinuableFuture} containing a {@link Dataset} with all query results
     *         organized in a tabular format for easy analysis
     * @throws IllegalArgumentException if queryRequest is {@code null}
     * @see #list(QueryRequest)
     */
    public ContinuableFuture<Dataset> query(final QueryRequest queryRequest) {
        return asyncExecutor.execute(() -> dbExecutor.query(queryRequest));
    }

    /**
     * Asynchronously executes a DynamoDB query and returns the results as a Dataset with type information.
     *
     * <p>This method performs a query operation and returns the results in a Dataset format
     * with the specified target class for type conversion and validation. The Dataset provides
     * a tabular view of the data with additional analytical capabilities and type safety.</p>
     *
     * <p><b>Dataset Benefits:</b></p>
     * <ul>
     * <li>Tabular data representation with column names and types</li>
     * <li>Type-safe column access and manipulation</li>
     * <li>Built-in data analysis and aggregation operations</li>
     * <li>Easy conversion to other formats (CSV, JSON, etc.)</li>
     * <li>Filtering, sorting, and transformation operations</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * QueryRequest request = new QueryRequest()
     *     .withTableName("Sales")
     *     .withIndexName("DateIndex")
     *     .withKeyConditionExpression("saleDate = :date")
     *     .withExpressionAttributeValues(Map.of(
     *         ":date", new AttributeValue("2024-01-15")
     *     ));
     *
     * // Typical: callback that analyzes the typed Dataset
     * asyncExecutor.query(request, Sale.class)
     *     .thenRunAsync((dataset, ex) -> {
     *         if (ex != null) {
     *             logger.error("Query failed", ex);
     *             return;
     *         }
     *         // Print tabular view
     *         dataset.println();
     *
     *         // Perform analysis on a numeric column
     *         double totalRevenue = dataset.<Number>getColumn("amount").stream()
     *             .mapToDouble(Number::doubleValue)
     *             .sum();
     *         System.out.println("Total revenue: $" + totalRevenue);
     *
     *         // Stream rows as the Sale type
     *         long highValueCount = dataset.stream(Sale.class)
     *             .filter(sale -> sale.getAmount() > 1000)
     *             .count();
     *         System.out.println("High-value sales: " + highValueCount);
     *     }); // returns ContinuableFuture<Dataset>
     *
     * // Typical: block for the Dataset
     * Dataset ds = asyncExecutor.query(request, Sale.class).get(); // returns a non-null Dataset
     *
     * // Edge: a query that matches nothing yields an empty (non-null) Dataset
     * boolean empty = asyncExecutor.query(noMatchRequest, Sale.class).get().isEmpty(); // returns true
     * }</pre>
     *
     * @param queryRequest the QueryRequest specifying the query parameters including key conditions,
     *                    filter expressions, and projection, must not be {@code null}
     * @param targetClass the class to associate with the Dataset for type operations; if {@code null}
     *                    or a {@link Map} type, results are extracted as raw attribute maps
     * @return a {@link ContinuableFuture} containing a {@link Dataset} with the query results
     *         and associated type information for type-safe operations
     * @throws IllegalArgumentException if queryRequest is {@code null}
     * @see #query(QueryRequest)
     * @see #list(QueryRequest, Class)
     */
    public ContinuableFuture<Dataset> query(final QueryRequest queryRequest, final Class<?> targetClass) {
        return asyncExecutor.execute(() -> dbExecutor.query(queryRequest, targetClass));
    }

    /**
     * Asynchronously executes a DynamoDB query and returns the results as a Stream.
     *
     * <p>This method provides memory-efficient processing of query results by returning a
     * {@link Stream} that lazily fetches items as needed. This is ideal for processing
     * large result sets without loading all items into memory at once. The stream handles
     * pagination automatically.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * QueryRequest request = new QueryRequest()
     *     .withTableName("Logs")
     *     .withKeyConditionExpression("userId = :uid AND timestamp > :ts")
     *     .withExpressionAttributeValues(Map.of(
     *         ":uid", new AttributeValue("user123"),
     *         ":ts", new AttributeValue().withN("1609459200")
     *     ));
     *
     * // Typical: callback that consumes the lazy stream
     * asyncExecutor.stream(request)
     *     .thenRunAsync(stream -> {
     *         long errorCount = stream
     *             .filter(log -> "ERROR".equals(log.get("level")))
     *             .count();
     *         System.out.println("Error logs: " + errorCount);
     *     }); // returns ContinuableFuture<Stream<Map<String, Object>>>
     *
     * // Typical: block for the stream, then run a single terminal op (a stream is consumed once)
     * long total = asyncExecutor.stream(request).get().count(); // returns the number of matching rows
     *
     * // Edge: a query that matches nothing yields an empty stream
     * long none = asyncExecutor.stream(noMatchRequest).get().count(); // returns 0
     * }</pre>
     *
     * @param queryRequest the {@link QueryRequest} specifying table name, key conditions,
     *                    filter expressions, and other query parameters, must not be {@code null}
     * @return a {@link ContinuableFuture} containing a {@link Stream} of items matching the query,
     *         providing lazy evaluation and automatic pagination
     * @throws IllegalArgumentException if queryRequest is {@code null}
     * @see #list(QueryRequest)
     */
    public ContinuableFuture<Stream<Map<String, Object>>> stream(final QueryRequest queryRequest) {
        return asyncExecutor.execute(() -> dbExecutor.stream(queryRequest));
    }

    /**
     * Asynchronously executes a DynamoDB query and returns the results as a typed Stream with automatic pagination.
     *
     * <p>This method performs a query operation and returns the results as a Stream with automatic
     * type conversion, combining the benefits of memory-efficient streaming, lazy evaluation, and
     * compile-time type safety. The stream automatically handles pagination, making it ideal for
     * processing large query results without loading everything into memory.</p>
     *
     * <p><b>Stream Benefits:</b></p>
     * <ul>
     * <li>Memory-efficient processing - items loaded lazily as consumed</li>
     * <li>Automatic pagination - seamlessly fetches additional pages</li>
     * <li>Type-safe operations with generics</li>
     * <li>Functional programming support (map, filter, reduce, etc.)</li>
     * <li>Parallel processing capabilities</li>
     * <li>Early termination support (limit, findFirst, etc.)</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * QueryRequest request = new QueryRequest()
     *     .withTableName("Transactions")
     *     .withIndexName("UserTransactionsIndex")
     *     .withKeyConditionExpression("userId = :userId AND #date >= :startDate")
     *     .withExpressionAttributeNames(Map.of("#date", "date"))
     *     .withExpressionAttributeValues(Map.of(
     *         ":userId", new AttributeValue("user123"),
     *         ":startDate", new AttributeValue("2024-01-01")
     *     ))
     *     .withScanIndexForward(false);  // Most recent first
     *
     * // Typical: callback that consumes the typed lazy stream
     * asyncExecutor.stream(request, Transaction.class)
     *     .thenRunAsync((stream, ex) -> {
     *         if (ex != null) {
     *             logger.error("Query streaming failed", ex);
     *             return;
     *         }
     *         // Streams can only be consumed once; pick one terminal operation
     *         double total = stream
     *             .filter(t -> "COMPLETED".equals(t.getStatus()))
     *             .mapToDouble(Transaction::getAmount)
     *             .sum();
     *         System.out.println("Total completed: $" + total);
     *     }); // returns ContinuableFuture<Stream<Transaction>>
     *
     * // Typical: block for the typed stream, then run one terminal op
     * long count = asyncExecutor.stream(request, Transaction.class).get().count(); // returns the number of matching rows
     *
     * // Edge: a query that matches nothing yields an empty stream
     * long none = asyncExecutor.stream(noMatchRequest, Transaction.class).get().count(); // returns 0
     * }</pre>
     *
     * @param <T> the type to convert each query result item to
     * @param queryRequest the QueryRequest specifying the query parameters including key conditions,
     *                    filter expressions, and projection, must not be {@code null}
     * @param targetClass the class to convert each result item to, must have a default constructor, must not be {@code null}
     * @return a {@link ContinuableFuture} containing a {@link Stream} of items matching the query,
     *         each automatically converted to type {@code T} with lazy evaluation and automatic pagination
     * @throws IllegalArgumentException if any parameter is {@code null}
     * @see #stream(QueryRequest)
     * @see #list(QueryRequest, Class)
     */
    public <T> ContinuableFuture<Stream<T>> stream(final QueryRequest queryRequest, final Class<T> targetClass) {
        return asyncExecutor.execute(() -> dbExecutor.stream(queryRequest, targetClass));
    }

    /**
     * Asynchronously performs a scan operation on the specified DynamoDB table with attribute projection.
     *
     * <p>This method scans the entire table and returns a stream of items. Scan operations
     * examine every item in the table and are less efficient than query operations.
     * Use scans sparingly and consider adding appropriate filters to reduce data transfer.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<String> attributes = Arrays.asList("userId", "name", "email");
     *
     * // Typical: callback that iterates projected items
     * asyncExecutor.scan("Users", attributes)
     *     .thenRunAsync(stream -> {
     *         stream.forEach(user -> {
     *             System.out.println(user.get("name") + ": " + user.get("email"));
     *         });
     *     }); // returns ContinuableFuture<Stream<Map<String, Object>>>
     *
     * // Typical: block for the stream, then run one terminal op
     * long total = asyncExecutor.scan("Users", attributes).get().count(); // returns the number of rows scanned
     *
     * // Edge: passing null projects all attributes (rather than none)
     * long all = asyncExecutor.scan("Users", (List<String>) null).get().count(); // returns the full row count
     * }</pre>
     *
     * @param tableName the name of the DynamoDB table to scan, must not be {@code null}
     * @param attributesToGet list of attribute names to retrieve, or {@code null} to retrieve all attributes.
     *                       Projecting specific attributes reduces data transfer costs
     * @return a {@link ContinuableFuture} containing a {@link Stream} of all items in the table,
     *         with automatic pagination and lazy evaluation
     * @throws IllegalArgumentException if tableName is {@code null}
     * @see #scan(String, Map)
     */
    public ContinuableFuture<Stream<Map<String, Object>>> scan(final String tableName, final List<String> attributesToGet) {
        return asyncExecutor.execute(() -> dbExecutor.scan(tableName, attributesToGet));
    }

    /**
     * Asynchronously performs a filtered scan operation on the specified DynamoDB table.
     *
     * <p>This method scans the entire table but only returns items that match the specified
     * filter conditions. Note that filtering is applied after reading items, so you're still
     * charged for reading all items in the table. Consider using Query operations when possible.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, Condition> scanFilter = new HashMap<>();
     * scanFilter.put("status", new Condition()
     *     .withComparisonOperator(ComparisonOperator.EQ)
     *     .withAttributeValueList(new AttributeValue("active")));
     * scanFilter.put("age", new Condition()
     *     .withComparisonOperator(ComparisonOperator.GT)
     *     .withAttributeValueList(new AttributeValue().withN("18")));
     *
     * // Typical: callback over the filtered stream
     * asyncExecutor.scan("Users", scanFilter)
     *     .thenRunAsync(stream -> {
     *         long count = stream.count();
     *         System.out.println("Active adult users: " + count);
     *     }); // returns ContinuableFuture<Stream<Map<String, Object>>>
     *
     * // Typical: block for the stream, then count the matches
     * long count = asyncExecutor.scan("Users", scanFilter).get().count(); // returns the number of matching rows
     *
     * // Edge: a filter that matches nothing yields an empty stream
     * long none = asyncExecutor.scan("Users", noMatchFilter).get().count(); // returns 0
     * }</pre>
     *
     * @param tableName the name of the DynamoDB table to scan, must not be {@code null}
     * @param scanFilter map of attribute names to {@link Condition} objects for filtering results;
     *                  may be {@code null} to apply no filter. Multiple conditions are combined with AND logic
     * @return a {@link ContinuableFuture} containing a {@link Stream} of items matching all filter conditions
     * @throws IllegalArgumentException if tableName is {@code null}
     * @see #scan(String, List, Map)
     */
    public ContinuableFuture<Stream<Map<String, Object>>> scan(final String tableName, final Map<String, Condition> scanFilter) {
        return asyncExecutor.execute(() -> dbExecutor.scan(tableName, scanFilter));
    }

    /**
     * Asynchronously performs a scan operation with both attribute projection and filter conditions.
     *
     * <p>This method combines attribute projection with filtering to scan the table and return
     * only the specified attributes from items that match the filter conditions. Note that filters
     * are applied after reading items, so you're still charged for reading all items. Consider
     * using Query operations when possible for better efficiency.</p>
     *
     * <p><b>Important Performance Notes:</b></p>
     * <ul>
     * <li>Scan reads every item in the table - expensive for large tables</li>
     * <li>Filters are applied AFTER reading, so you pay for all read items</li>
     * <li>Projection reduces network transfer but not read cost</li>
     * <li>Use Query with indexes instead of Scan when possible</li>
     * <li>Consider parallel scans for large tables</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<String> attributes = Arrays.asList("productId", "name", "price", "category");
     *
     * Map<String, Condition> filter = new HashMap<>();
     * filter.put("category", new Condition()
     *     .withComparisonOperator(ComparisonOperator.EQ)
     *     .withAttributeValueList(new AttributeValue("Electronics")));
     * filter.put("price", new Condition()
     *     .withComparisonOperator(ComparisonOperator.LT)
     *     .withAttributeValueList(new AttributeValue().withN("100")));
     *
     * // Typical: callback that takes the first 50 matches
     * asyncExecutor.scan("Products", attributes, filter)
     *     .thenRunAsync((stream, ex) -> {
     *         if (ex != null) {
     *             logger.error("Scan failed", ex);
     *             return;
     *         }
     *         List<Map<String, Object>> products = stream
     *             .limit(50)  // Process first 50 matches
     *             .toList();
     *         System.out.println("Found " + products.size() + " affordable electronics");
     *     }); // returns ContinuableFuture<Stream<Map<String, Object>>>
     *
     * // Typical: block for the stream, then count the matches
     * long count = asyncExecutor.scan("Products", attributes, filter).get().count(); // returns the number of matching rows
     *
     * // Edge: a filter that matches nothing yields an empty stream
     * long none = asyncExecutor.scan("Products", attributes, noMatchFilter).get().count(); // returns 0
     * }</pre>
     *
     * @param tableName the name of the DynamoDB table to scan, must not be {@code null}
     * @param attributesToGet list of attribute names to retrieve, or {@code null} to retrieve all attributes.
     *                       Projecting reduces network transfer but not read cost
     * @param scanFilter map of attribute names to {@link Condition} objects for filtering results;
     *                  may be {@code null} to apply no filter. Multiple conditions are combined with AND logic.
     * @return a {@link ContinuableFuture} containing a {@link Stream} of items matching all filter conditions
     *         with only specified attributes, providing lazy evaluation and automatic pagination
     * @throws IllegalArgumentException if tableName is {@code null}
     * @see #scan(String, List)
     * @see #scan(String, Map)
     * @see #scan(ScanRequest)
     */
    public ContinuableFuture<Stream<Map<String, Object>>> scan(final String tableName, final List<String> attributesToGet,
            final Map<String, Condition> scanFilter) {
        return asyncExecutor.execute(() -> dbExecutor.scan(tableName, attributesToGet, scanFilter));
    }

    /**
     * Asynchronously performs a scan operation using a complete ScanRequest for maximum control.
     *
     * <p>This method provides the most flexibility for scan operations by accepting a fully configured
     * ScanRequest. You can specify all available DynamoDB scan parameters including filter expressions,
     * projection expressions, parallel scan segments, pagination settings, and capacity monitoring.</p>
     *
     * <p><b>Advanced Scan Features Available:</b></p>
     * <ul>
     * <li>Filter expressions for complex filtering logic</li>
     * <li>Projection expressions for attribute selection</li>
     * <li>Expression attribute names and values for reserved words</li>
     * <li>Parallel scan with segment and total segments parameters</li>
     * <li>Consistent read configuration</li>
     * <li>Return consumed capacity for monitoring</li>
     * <li>Index name for scanning secondary indexes</li>
     * <li>Limit and exclusive start key for pagination control</li>
     * </ul>
     *
     * <p><b>Parallel Scan Pattern:</b></p>
     * <p>For large tables, use parallel scans by dividing the table into segments.
     * Each segment can be scanned concurrently to improve throughput.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Simple scan with filter expression
     * ScanRequest request = new ScanRequest()
     *     .withTableName("Users")
     *     .withFilterExpression("#status = :active AND #age > :minAge")
     *     .withExpressionAttributeNames(Map.of("#status", "status", "#age", "age"))
     *     .withExpressionAttributeValues(Map.of(
     *         ":active", new AttributeValue("ACTIVE"),
     *         ":minAge", new AttributeValue().withN("18")
     *     ))
     *     .withProjectionExpression("userId, name, email, age")
     *     .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
     *
     * // Typical: callback over the filtered scan
     * asyncExecutor.scan(request)
     *     .thenRunAsync(stream -> {
     *         long count = stream.count();
     *         System.out.println("Active adult users: " + count);
     *     }); // returns ContinuableFuture<Stream<Map<String, Object>>>
     *
     * // Typical: block for the stream, then count
     * long count = asyncExecutor.scan(request).get().count(); // returns the number of matching rows
     *
     * // Parallel scan example (segment 1 of 4)
     * ScanRequest parallelRequest = new ScanRequest()
     *     .withTableName("LargeTable")
     *     .withSegment(0)        // This segment number (0-based)
     *     .withTotalSegments(4); // Total number of segments
     *
     * asyncExecutor.scan(parallelRequest)
     *     .thenRunAsync(stream -> processSegment(stream)); // returns ContinuableFuture<Stream<Map<String, Object>>> for segment 0
     * }</pre>
     *
     * @param scanRequest the complete ScanRequest with all parameters configured, must not be {@code null}
     * @return a {@link ContinuableFuture} containing a {@link Stream} of all items from the scan,
     *         providing lazy evaluation and automatic pagination
     * @throws IllegalArgumentException if scanRequest is {@code null}
     * @see ScanRequest
     * @see #scan(ScanRequest, Class)
     * @see #scan(String, List, Map)
     */
    public ContinuableFuture<Stream<Map<String, Object>>> scan(final ScanRequest scanRequest) {
        return asyncExecutor.execute(() -> dbExecutor.scan(scanRequest));
    }

    /**
     * Asynchronously performs a scan operation with attribute projection and type conversion.
     *
     * <p>This method scans the table, projects specific attributes, and converts each result
     * item to the specified target type, providing type-safe scan operations.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<String> attributes = Arrays.asList("userId", "name", "email");
     *
     * // Typical: callback that iterates converted POJOs
     * asyncExecutor.scan("Users", attributes, User.class)
     *     .thenRunAsync(stream -> {
     *         stream.forEach(user -> {
     *             System.out.println(user.getName() + ": " + user.getEmail());
     *         });
     *     }); // returns ContinuableFuture<Stream<User>>
     *
     * // Typical: block for the typed stream, then count
     * long total = asyncExecutor.scan("Users", attributes, User.class).get().count(); // returns the number of rows scanned
     *
     * // Edge: passing null projects all attributes (rather than none)
     * long all = asyncExecutor.scan("Users", (List<String>) null, User.class).get().count(); // returns the full row count
     * }</pre>
     *
     * @param <T> the type to convert the scan results to
     * @param tableName the name of the DynamoDB table to scan, must not be {@code null}
     * @param attributesToGet list of attribute names to retrieve, or {@code null} to retrieve all attributes
     * @param targetClass the class to convert each result item to, must not be {@code null}
     * @return a {@link ContinuableFuture} containing a {@link Stream} of items from the scan,
     *         each converted to type {@code T}
     * @throws IllegalArgumentException if tableName or targetClass is {@code null}
     */
    public <T> ContinuableFuture<Stream<T>> scan(final String tableName, final List<String> attributesToGet, final Class<T> targetClass) {
        return asyncExecutor.execute(() -> dbExecutor.scan(tableName, attributesToGet, targetClass));
    }

    /**
     * Asynchronously performs a scan operation with filter conditions and type conversion.
     *
     * <p>This method scans the table with filter conditions and converts each matching result
     * item to the specified target type, combining filtering with type safety.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, Condition> filter = new HashMap<>();
     * filter.put("status", new Condition()
     *     .withComparisonOperator(ComparisonOperator.EQ)
     *     .withAttributeValueList(new AttributeValue("active")));
     *
     * // Typical: callback over the filtered, converted stream
     * asyncExecutor.scan("Users", filter, User.class)
     *     .thenRunAsync(stream -> {
     *         long count = stream.count();
     *         System.out.println("Active users: " + count);
     *     }); // returns ContinuableFuture<Stream<User>>
     *
     * // Typical: block for the typed stream, then count
     * long count = asyncExecutor.scan("Users", filter, User.class).get().count(); // returns the number of matching rows
     *
     * // Edge: a filter that matches nothing yields an empty stream
     * long none = asyncExecutor.scan("Users", noMatchFilter, User.class).get().count(); // returns 0
     * }</pre>
     *
     * @param <T> the type to convert the scan results to
     * @param tableName the name of the DynamoDB table to scan, must not be {@code null}
     * @param scanFilter map of attribute names to {@link Condition} objects for filtering results;
     *                  may be {@code null} to apply no filter
     * @param targetClass the class to convert each result item to, must not be {@code null}
     * @return a {@link ContinuableFuture} containing a {@link Stream} of items matching the filter conditions,
     *         each converted to type {@code T}
     * @throws IllegalArgumentException if tableName or targetClass is {@code null}
     */
    public <T> ContinuableFuture<Stream<T>> scan(final String tableName, final Map<String, Condition> scanFilter, final Class<T> targetClass) {
        return asyncExecutor.execute(() -> dbExecutor.scan(tableName, scanFilter, targetClass));
    }

    /**
     * Asynchronously performs a scan operation with projection, filtering, and type conversion.
     *
     * <p>This method provides the most comprehensive scan operation, combining attribute projection,
     * filter conditions, and automatic type conversion for maximum flexibility and type safety.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<String> attributes = Arrays.asList("productId", "name", "price");
     * Map<String, Condition> filter = new HashMap<>();
     * filter.put("category", new Condition()
     *     .withComparisonOperator(ComparisonOperator.EQ)
     *     .withAttributeValueList(new AttributeValue("Electronics")));
     *
     * // Typical: callback that iterates projected, filtered, converted POJOs
     * asyncExecutor.scan("Products", attributes, filter, Product.class)
     *     .thenRunAsync(stream -> {
     *         stream.forEach(p -> System.out.println(p.getName() + ": $" + p.getPrice()));
     *     }); // returns ContinuableFuture<Stream<Product>>
     *
     * // Typical: block for the typed stream, then count
     * long count = asyncExecutor.scan("Products", attributes, filter, Product.class).get().count(); // returns the number of matching rows
     *
     * // Edge: a filter that matches nothing yields an empty stream
     * long none = asyncExecutor.scan("Products", attributes, noMatchFilter, Product.class).get().count(); // returns 0
     * }</pre>
     *
     * @param <T> the type to convert the scan results to
     * @param tableName the name of the DynamoDB table to scan, must not be {@code null}
     * @param attributesToGet list of attribute names to retrieve, or {@code null} to retrieve all attributes
     * @param scanFilter map of attribute names to {@link Condition} objects for filtering results;
     *                  may be {@code null} to apply no filter
     * @param targetClass the class to convert each result item to, must not be {@code null}
     * @return a {@link ContinuableFuture} containing a {@link Stream} of filtered items with specified attributes,
     *         each converted to type {@code T}
     * @throws IllegalArgumentException if tableName or targetClass is {@code null}
     */
    public <T> ContinuableFuture<Stream<T>> scan(final String tableName, final List<String> attributesToGet, final Map<String, Condition> scanFilter,
            final Class<T> targetClass) {
        return asyncExecutor.execute(() -> dbExecutor.scan(tableName, attributesToGet, scanFilter, targetClass));
    }

    /**
     * Asynchronously performs a scan operation using a ScanRequest with type conversion.
     *
     * <p>This method provides full scan operation control with automatic type conversion,
     * allowing you to use all advanced scan features while maintaining type safety.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ScanRequest request = new ScanRequest().withTableName("Users");
     *
     * // Typical: callback that iterates converted POJOs
     * asyncExecutor.scan(request, User.class)
     *     .thenRunAsync(stream -> {
     *         stream.forEach(user -> System.out.println(user.getName()));
     *     }); // returns ContinuableFuture<Stream<User>>
     *
     * // Typical: block for the typed stream, then count
     * long total = asyncExecutor.scan(request, User.class).get().count(); // returns the number of rows scanned
     *
     * // Edge: scanning an empty table yields an empty stream
     * ScanRequest empty = new ScanRequest().withTableName("EmptyTable");
     * long none = asyncExecutor.scan(empty, User.class).get().count(); // returns 0
     * }</pre>
     *
     * @param <T> the type to convert the scan results to
     * @param scanRequest the complete ScanRequest with all parameters configured, must not be {@code null}
     * @param targetClass the class to convert each result item to, must not be {@code null}
     * @return a {@link ContinuableFuture} containing a {@link Stream} of items from the scan,
     *         each converted to type {@code T}
     * @throws IllegalArgumentException if any parameter is {@code null}
     */
    public <T> ContinuableFuture<Stream<T>> scan(final ScanRequest scanRequest, final Class<T> targetClass) {
        return asyncExecutor.execute(() -> dbExecutor.scan(scanRequest, targetClass));
    }
}
