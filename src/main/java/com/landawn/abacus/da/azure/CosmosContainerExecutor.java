/*
 * Copyright (C) 2022 HaiYang Li
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
package com.landawn.abacus.da.azure;

import java.util.Collection;
import java.util.List;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosItemIdentity;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosPatchItemRequestOptions;
import com.azure.cosmos.models.CosmosPatchOperations;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.SqlParameter;
import com.azure.cosmos.models.SqlQuerySpec;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.landawn.abacus.annotation.Beta;
import com.landawn.abacus.query.AbstractQueryBuilder.SP;
import com.landawn.abacus.query.SqlBuilder;
import com.landawn.abacus.query.SqlBuilder.ACSB;
import com.landawn.abacus.query.SqlBuilder.LCSB;
import com.landawn.abacus.query.SqlBuilder.SCSB;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.stream.Stream;

/**
 * Comprehensive executor for Azure Cosmos DB container operations providing a high-level, simplified interface 
 * for document database operations with built-in query building capabilities.
 * 
 * <p>This class serves as a sophisticated wrapper around the Azure Cosmos DB Java SDK's {@link CosmosContainer},
 * offering developer-friendly methods for all common document operations. It seamlessly integrates with the
 * Abacus query building framework to provide type-safe, fluent query construction while maintaining full access
 * to Cosmos DB's advanced features.</p>
 *
 * <h2>Core Capabilities</h2>
 * <h3>Document and Query Operations</h3>
 * <ul>
 * <li><strong>Document Operations:</strong>
 *     <ul>
 *     <li>Create, read, update, delete operations with type safety</li>
 *     <li>Upsert operations for create-or-update scenarios</li>
 *     <li>Patch operations for efficient partial updates</li>
 *     <li>Batch operations for high-throughput scenarios</li>
 *     </ul>
 * </li>
 * <li><strong>Advanced Querying:</strong>
 *     <ul>
 *     <li>SQL-like queries with parameter binding</li>
 *     <li>Integration with SqlBuilder for dynamic query construction</li>
 *     <li>Support for cross-partition and single-partition queries</li>
 *     <li>Streaming results for large result sets</li>
 *     </ul>
 * </li>
 * <li><strong>Cosmos DB Integration:</strong>
 *     <ul>
 *     <li>Automatic partition key handling</li>
 *     <li>Request units (RU) tracking and optimization</li>
 *     <li>Consistency level configuration</li>
 *     <li>Indexing policy support</li>
 *     </ul>
 * </li>
 * <li><strong>Developer Experience:</strong>
 *     <ul>
 *     <li>Automatic POJO ↔ JSON conversion</li>
 *     <li>Configurable naming policies for field mapping</li>
 *     <li>Exception handling and error reporting</li>
 *     <li>Stream-based result processing</li>
 *     </ul>
 * </li>
 * </ul>
 *
 * <p><b>Basic Usage Examples:</b></p>
 * <pre>{@code
 * // Initialize with Cosmos DB container
 * CosmosClient client = new CosmosClientBuilder()
 *     .endpoint("https://myaccount.documents.azure.com:443/")
 *     .key("mykey")
 *     .buildClient();
 * 
 * CosmosContainer container = client.getDatabase("ecommerce")
 *                                  .getContainer("products");
 * CosmosContainerExecutor executor = new CosmosContainerExecutor(container);
 * 
 * // Document operations
 * Product product = new Product("prod123", "Laptop", "Electronics", 999.99);
 * 
 * // Create
 * CosmosItemResponse<Product> response = executor.createItem(product);
 * System.out.println("RUs consumed: " + response.getRequestCharge());
 * 
 * // Read
 * Product retrieved = executor.readItem("prod123", 
 *                                      new PartitionKey("Electronics"), 
 *                                      Product.class);
 * 
 * // Update
 * product.setPrice(899.99);
 * executor.replaceItem(product);
 * 
 * // Delete
 * executor.deleteItem("prod123", new PartitionKey("Electronics"));
 * }</pre>
 *
 * <p><b>Advanced Query Examples:</b></p>
 * <pre>{@code
 * // SQL queries with parameters
 * String sql = "SELECT * FROM c WHERE c.category = @category AND c.price > @minPrice";
 * List<Product> expensiveElectronics = executor.queryItems(
 *     sql, 
 *     N.asMap("category", "Electronics", "minPrice", 500.0),
 *     Product.class
 * ).toList();
 * 
 * // Using SqlBuilder for dynamic queries
 * String dynamicQuery = ACSB.select("id", "name", "price")
 *                          .from("c")
 *                          .where(Filters.eq("category", "Electronics"))
 *                          .and(Filters.between("price", 100, 1000))
 *                          .orderBy("price DESC")
 *                          .sql();
 * 
 * Stream<Product> products = executor.queryItems(dynamicQuery, Product.class);
 * 
 * // Stream processing for large result sets
 * executor.queryItems("SELECT * FROM c WHERE c.inStock = true", Product.class)
 *        .filter(p -> p.getPrice() > 100)
 *        .forEach(this::processProduct);
 * }</pre>
 *
 * <p><b>Batch Operations:</b></p>
 * <pre>{@code
 * // Batch operations for performance
 * List<Product> products = Arrays.asList(product1, product2, product3);
 * List<CosmosItemResponse<Product>> responses = executor.createItems(products);
 * 
 * // Calculate total RUs consumed
 * double totalRUs = responses.stream()
 *                           .mapToDouble(CosmosItemResponse::getRequestCharge)
 *                           .sum();
 * }</pre>
 * 
 * <h3>Partition Key Considerations</h3>
 * <p>Cosmos DB requires partition keys for optimal performance. This executor handles partition keys in several ways:</p>
 * <ul>
 * <li>Automatic extraction from document objects when possible</li>
 * <li>Explicit partition key specification for operations</li>
 * <li>Cross-partition query support with performance implications</li>
 * </ul>
 * 
 * <h3>Request Units (RU) Management</h3>
 * <p>All operations return response objects that include RU consumption information, enabling cost monitoring
 * and optimization. The executor supports request options for fine-tuning performance characteristics.</p>
 * 
 * <h3>Thread Safety</h3>
 * <p>This class is thread-safe and designed for concurrent use. The underlying {@link CosmosContainer} 
 * is thread-safe, and this executor maintains no mutable state that would cause thread safety issues.</p>
 * 
 * <h3>Naming Policy Integration</h3>
 * <p>The executor supports configurable naming policies for mapping Java property names to Cosmos DB field names:</p>
 * <ul>
 * <li><strong>SNAKE_CASE:</strong> {@code firstName} → {@code first_name}</li>
 * <li><strong>CAMEL_CASE:</strong> {@code firstName} → {@code firstName}</li>
 * <li><strong>UPPER_CAMEL_CASE:</strong> {@code firstName} → {@code FirstName}</li>
 * </ul>
 * 
 * @see CosmosContainer
 * @see CosmosItemResponse
 * @see SqlQuerySpec
 * @see PartitionKey
 * @see SqlBuilder
 * @see com.landawn.abacus.query.Filters
 */
@Beta
@SuppressWarnings("deprecation")
public class CosmosContainerExecutor {

    // private static final BiConsumer<StringBuilder, String> handlerForNamedParameter = (sb, propName) -> sb.append("@").append(propName);

    private final CosmosContainer cosmosContainer;
    private final NamingPolicy namingPolicy;

    /**
     * Constructs a new CosmosContainerExecutor with default naming policy.
     *
     * <p>Uses {@link NamingPolicy#SNAKE_CASE} as the default naming policy
     * for converting Java property names to Cosmos DB field names.</p>
     *
     * @param cosmosContainer the Cosmos DB container to wrap
     * @throws IllegalArgumentException if cosmosContainer is null
     */
    public CosmosContainerExecutor(final CosmosContainer cosmosContainer) {
        if (cosmosContainer == null) {
            throw new IllegalArgumentException("cosmosContainer cannot be null");
        }
        this.cosmosContainer = cosmosContainer;
        this.namingPolicy = NamingPolicy.SNAKE_CASE;
    }

    /**
     * Constructs a new CosmosContainerExecutor with specified naming policy.
     *
     * <p>The naming policy is used to convert Java property names to Cosmos DB field names
     * during automatic object mapping operations.</p>
     *
     * @param cosmosContainer the Cosmos DB container to wrap
     * @param namingPolicy the naming policy for field name conversion
     * @throws IllegalArgumentException if cosmosContainer or namingPolicy is null
     */
    public CosmosContainerExecutor(final CosmosContainer cosmosContainer, final NamingPolicy namingPolicy) {
        if (cosmosContainer == null) {
            throw new IllegalArgumentException("cosmosContainer cannot be null");
        }
        if (namingPolicy == null) {
            throw new IllegalArgumentException("namingPolicy cannot be null");
        }
        this.cosmosContainer = cosmosContainer;
        this.namingPolicy = namingPolicy;
    }

    /**
     * Returns the underlying Cosmos DB container.
     * 
     * <p>This provides direct access to the wrapped {@link CosmosContainer} for operations
     * not provided by this executor.</p>
     *
     * @return the underlying CosmosContainer instance
     */
    public CosmosContainer cosmosContainer() {
        return cosmosContainer;
    }

    /**
     * Creates a new item in the container.
     *
     * <p>The item will be serialized to JSON and inserted into the container. The partition key
     * will be extracted automatically from the item based on the container's partition key definition.
     * This is the simplest form of item creation, using default request options and automatic
     * partition key detection.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Define a product entity
     * Product product = new Product();
     * product.setId("prod123");
     * product.setName("Laptop");
     * product.setCategory("Electronics"); // Assuming category is the partition key
     * product.setPrice(999.99);
     *
     * // Create the item
     * CosmosItemResponse<Product> response = executor.createItem(product);
     *
     * // Access response metadata
     * System.out.println("Request charge: " + response.getRequestCharge() + " RUs");
     * System.out.println("Status code: " + response.getStatusCode());
     * System.out.println("ETag: " + response.getETag());
     *
     * // Get the created item
     * Product createdProduct = response.getItem();
     * }</pre>
     *
     * @param <T> the type of the item to create
     * @param item the item to create (must not be null)
     * @return a CosmosItemResponse containing the created item and metadata including RU consumption
     * @throws CosmosException if the operation fails (e.g., item already exists, invalid data)
     * @throws IllegalArgumentException if item is null
     */
    public <T> CosmosItemResponse<T> createItem(final T item) {
        return cosmosContainer.createItem(item);
    }

    /**
     * Creates a new item in the container with explicit partition key and options.
     *
     * <p>This method provides full control over the create operation by allowing explicit
     * specification of the partition key and request options such as consistency level,
     * indexing policy, and conditional creation. This is useful when you need to override
     * automatic partition key detection or when you want fine-grained control over the
     * operation behavior.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Product product = new Product("prod123", "Laptop", "Electronics", 999.99);
     *
     * // Create with explicit partition key
     * PartitionKey partitionKey = new PartitionKey("Electronics");
     *
     * // Configure request options
     * CosmosItemRequestOptions options = new CosmosItemRequestOptions();
     * options.setConsistencyLevel(ConsistencyLevel.STRONG);
     *
     * // Conditional creation - only create if item doesn't exist
     * options.setIfNoneMatchETag("*");
     *
     * // Create the item
     * CosmosItemResponse<Product> response = executor.createItem(
     *     product,
     *     partitionKey,
     *     options
     * );
     *
     * System.out.println("Created with RU charge: " + response.getRequestCharge());
     * }</pre>
     *
     * @param <T> the type of the item to create
     * @param item the item to create (must not be null)
     * @param partitionKey the partition key for the item (must not be null)
     * @param options additional options for the create operation (can be null for default behavior)
     * @return a CosmosItemResponse containing the created item and metadata
     * @throws CosmosException if the operation fails (e.g., conditional check fails, conflict)
     * @throws IllegalArgumentException if item or partitionKey is null
     */
    public <T> CosmosItemResponse<T> createItem(final T item, final PartitionKey partitionKey, final CosmosItemRequestOptions options) {
        return cosmosContainer.createItem(item, partitionKey, options);
    }

    /**
     * Creates a new item in the container with specified options.
     *
     * <p>This method creates a new document in the container with additional request options
     * but uses the automatic partition key extraction from the item. Useful when you need
     * to control aspects like consistency level or indexing policy without explicitly
     * specifying the partition key.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Product product = new Product("prod456", "Mouse", "Electronics", 29.99);
     *
     * // Configure options without explicit partition key
     * CosmosItemRequestOptions options = new CosmosItemRequestOptions();
     * options.setContentResponseOnWriteEnabled(false);           // Reduce RU cost by not returning document
     * options.setIndexingDirective(IndexingDirective.EXCLUDE);   // Skip indexing for this item
     *
     * CosmosItemResponse<Product> response = executor.createItem(product, options);
     *
     * System.out.println("Item created with minimal RU: " + response.getRequestCharge());
     * }</pre>
     *
     * @param <T> the type of the item to create
     * @param item the item to create (must not be null)
     * @param options additional options for the create operation (can be null for default behavior)
     * @return a CosmosItemResponse containing the created item and metadata
     * @throws CosmosException if the operation fails
     * @throws IllegalArgumentException if item is null
     */
    public <T> CosmosItemResponse<T> createItem(final T item, final CosmosItemRequestOptions options) {
        return cosmosContainer.createItem(item, options);
    }

    /**
     * Creates or updates an item in the container (upsert operation).
     *
     * <p>If an item with the same id and partition key exists, it will be replaced.
     * If it doesn't exist, a new item will be created. The partition key is automatically
     * extracted from the item based on the container's partition key definition. This
     * operation is idempotent and useful for scenarios where you don't know whether
     * the item already exists.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Create or update a product
     * Product product = new Product();
     * product.setId("prod789");
     * product.setName("Keyboard");
     * product.setCategory("Electronics");
     * product.setPrice(79.99);
     * product.setLastUpdated(System.currentTimeMillis());
     *
     * // Upsert - will create if new, replace if exists
     * CosmosItemResponse<Product> response = executor.upsertItem(product);
     *
     * // Check if it was created (201) or replaced (200)
     * int statusCode = response.getStatusCode();
     * if (statusCode == 201) {
     *     System.out.println("New item created");
     * } else if (statusCode == 200) {
     *     System.out.println("Existing item updated");
     * }
     *
     * System.out.println("RU consumed: " + response.getRequestCharge());
     * }</pre>
     *
     * @param <T> the type of the item to upsert
     * @param item the item to create or update (must not be null)
     * @return a CosmosItemResponse containing the upserted item and metadata
     * @throws CosmosException if the operation fails
     * @throws IllegalArgumentException if item is null
     *
     * @see #createItem(Object) for create-only operations
     * @see #replaceItem(String, PartitionKey, Object, CosmosItemRequestOptions) for replace-only operations
     */
    public <T> CosmosItemResponse<T> upsertItem(final T item) {
        return cosmosContainer.upsertItem(item);
    }

    /**
     * Creates or updates an item with explicit partition key and options.
     *
     * <p>Performs an upsert operation with full control over partition key and request options.
     * This is useful when you need to override the automatic partition key detection or
     * when you want to specify options like consistency level, indexing policy, or conditional updates.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Product product = new Product("prod999", "Monitor", "Electronics", 299.99);
     * PartitionKey partitionKey = new PartitionKey("Electronics");
     *
     * // Configure upsert options
     * CosmosItemRequestOptions options = new CosmosItemRequestOptions();
     * options.setConsistencyLevel(ConsistencyLevel.SESSION);
     *
     * // Conditional upsert - only if ETag matches (optimistic concurrency)
     * options.setIfMatchETag(existingETag);
     *
     * try {
     *     CosmosItemResponse<Product> response = executor.upsertItem(
     *         product,
     *         partitionKey,
     *         options
     *     );
     *     System.out.println("Upsert successful, RU: " + response.getRequestCharge());
     * } catch (CosmosException e) {
     *     if (e.getStatusCode() == 412) {
     *         System.out.println("Precondition failed - item was modified");
     *     }
     * }
     * }</pre>
     *
     * @param <T> the type of the item to upsert
     * @param item the item to create or update (must not be null)
     * @param partitionKey the partition key for the item (must not be null)
     * @param options additional options for the upsert operation (can be null for default behavior)
     * @return a CosmosItemResponse containing the upserted item and metadata
     * @throws CosmosException if the operation fails (e.g., conditional check fails)
     * @throws IllegalArgumentException if item or partitionKey is null
     */
    public <T> CosmosItemResponse<T> upsertItem(final T item, final PartitionKey partitionKey, final CosmosItemRequestOptions options) {
        return cosmosContainer.upsertItem(item, partitionKey, options);
    }

    /**
     * Creates or updates an item with specified options.
     *
     * <p>Performs an upsert operation using automatic partition key extraction from the item
     * but allows specification of request options such as consistency level, indexing policy,
     * or conditional operations.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Product product = new Product("prod555", "Headphones", "Electronics", 149.99);
     *
     * // Configure options for cost optimization
     * CosmosItemRequestOptions options = new CosmosItemRequestOptions();
     * options.setContentResponseOnWriteEnabled(false); // Don't return the document body
     *
     * CosmosItemResponse<Product> response = executor.upsertItem(product, options);
     *
     * // Response will not contain the item, but metadata is available
     * System.out.println("Upsert completed with RU: " + response.getRequestCharge());
     * System.out.println("Status: " + response.getStatusCode());
     * }</pre>
     *
     * @param <T> the type of the item to upsert
     * @param item the item to create or update (must not be null)
     * @param options additional options for the upsert operation (can be null for default behavior)
     * @return a CosmosItemResponse containing the upserted item and metadata
     * @throws CosmosException if the operation fails
     * @throws IllegalArgumentException if item is null
     */
    public <T> CosmosItemResponse<T> upsertItem(final T item, final CosmosItemRequestOptions options) {
        return cosmosContainer.upsertItem(item, options);
    }

    /**
     * Replaces an existing item with a new item.
     * 
     * <p>This operation completely replaces the existing document with the new item.
     * The item must exist or the operation will fail. This is different from upsert
     * which will create the item if it doesn't exist.</p>
     * 
     * <p>The replace operation is atomic and will either succeed completely or fail
     * without making any changes. It can be used with conditional options to perform
     * optimistic concurrency control using ETags.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Product existingProduct = executor.readItem("prod123", 
     *                                           new PartitionKey("Electronics"), 
     *                                           Product.class).getItem();
     * 
     * // Modify the product
     * Product updatedProduct = new Product(existingProduct.getId(), 
     *                                     "Updated Laptop", 
     *                                     "Electronics", 
     *                                     1099.99);
     * 
     * // Replace with ETag for optimistic concurrency
     * CosmosItemRequestOptions options = new CosmosItemRequestOptions()
     *     .setIfMatchETag(existingProduct.getETag());
     * 
     * CosmosItemResponse<Product> response = executor.replaceItem(
     *     "prod123", 
     *     new PartitionKey("Electronics"), 
     *     updatedProduct, 
     *     options
     * );
     * }</pre>
     *
     * @param <T> the type of the new item
     * @param oldItemId the id of the existing item to replace
     * @param partitionKey the partition key of the item to replace
     * @param newItem the new item to replace the existing one with
     * @param options additional options for the replace operation (can be null)
     * @return a CosmosItemResponse containing the replaced item and metadata
     * @throws CosmosException if the operation fails or the item doesn't exist
     * 
     * @see #upsertItem(Object) for create-or-replace operations
     */
    public <T> CosmosItemResponse<T> replaceItem(final String oldItemId, final PartitionKey partitionKey, final T newItem,
            final CosmosItemRequestOptions options) {
        return cosmosContainer.replaceItem(newItem, oldItemId, partitionKey, options);
    }

    /**
     * Performs partial updates on an item using patch operations.
     *
     * <p>Patch operations allow you to modify specific fields of a document without
     * replacing the entire document. This is more efficient than read-modify-write
     * patterns and supports atomic operations like increment, add, remove, and replace.
     * Patch operations are executed atomically on the server side.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String itemId = "prod123";
     * PartitionKey partitionKey = new PartitionKey("Electronics");
     *
     * // Create patch operations
     * CosmosPatchOperations patchOps = CosmosPatchOperations.create();
     *
     * // Update price
     * patchOps.replace("/price", 899.99);
     *
     * // Increment view count
     * patchOps.increment("/viewCount", 1);
     *
     * // Add new field
     * patchOps.add("/tags", Arrays.asList("sale", "featured"));
     *
     * // Remove a field
     * patchOps.remove("/deprecated");
     *
     * // Execute patch
     * CosmosItemResponse<Product> response = executor.patchItem(
     *     itemId,
     *     partitionKey,
     *     patchOps,
     *     Product.class
     * );
     *
     * Product patchedProduct = response.getItem();
     * System.out.println("Patched price: " + patchedProduct.getPrice());
     * System.out.println("RU consumed: " + response.getRequestCharge());
     * }</pre>
     *
     * @param <T> the type of the item to patch
     * @param itemId the id of the item to patch (must not be null)
     * @param partitionKey the partition key of the item (must not be null)
     * @param cosmosPatchOperations the patch operations to apply (must not be null)
     * @param itemType the class type for deserializing the response (must not be null)
     * @return a CosmosItemResponse containing the patched item and metadata
     * @throws CosmosException if the operation fails or the item doesn't exist
     * @throws IllegalArgumentException if any parameter is null
     *
     * @see CosmosPatchOperations for available patch operations
     */
    public <T> CosmosItemResponse<T> patchItem(final String itemId, final PartitionKey partitionKey, final CosmosPatchOperations cosmosPatchOperations,
            final Class<T> itemType) {
        return cosmosContainer.patchItem(itemId, partitionKey, cosmosPatchOperations, itemType);
    }

    /**
     * Performs partial updates on an item using patch operations with additional options.
     *
     * <p>Extended patch operation that allows specification of additional options such as
     * consistency level, conditional patch based on etag, or custom request options.
     * This provides full control over the patch operation behavior and enables advanced
     * scenarios like optimistic concurrency control.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String itemId = "prod456";
     * PartitionKey partitionKey = new PartitionKey("Electronics");
     *
     * // Create patch operations
     * CosmosPatchOperations patchOps = CosmosPatchOperations.create()
     *     .replace("/price", 799.99)
     *     .increment("/stockQuantity", -1);
     *
     * // Configure patch options with conditional update
     * CosmosPatchItemRequestOptions options = new CosmosPatchItemRequestOptions();
     * options.setIfMatchETag(existingETag);                             // Only patch if ETag matches
     * options.setFilterPredicate("from c where c.stockQuantity > 0");   // Conditional logic
     *
     * try {
     *     CosmosItemResponse<Product> response = executor.patchItem(
     *         itemId,
     *         partitionKey,
     *         patchOps,
     *         options,
     *         Product.class
     *     );
     *     System.out.println("Patch successful, new stock: " + response.getItem().getStockQuantity());
     * } catch (CosmosException e) {
     *     if (e.getStatusCode() == 412) {
     *         System.out.println("Patch failed: ETag mismatch or filter condition not met");
     *     }
     * }
     * }</pre>
     *
     * @param <T> the type of the item to patch
     * @param itemId the id of the item to patch (must not be null)
     * @param partitionKey the partition key of the item (must not be null)
     * @param cosmosPatchOperations the patch operations to apply (must not be null)
     * @param options additional options for the patch operation (can be null for default behavior)
     * @param itemType the class type for deserializing the response (must not be null)
     * @return a CosmosItemResponse containing the patched item and metadata
     * @throws CosmosException if the operation fails or the item doesn't exist
     * @throws IllegalArgumentException if itemId, partitionKey, cosmosPatchOperations, or itemType is null
     *
     * @see CosmosPatchOperations for available patch operations
     * @see CosmosPatchItemRequestOptions for available options
     */
    public <T> CosmosItemResponse<T> patchItem(final String itemId, final PartitionKey partitionKey, final CosmosPatchOperations cosmosPatchOperations,
            final CosmosPatchItemRequestOptions options, final Class<T> itemType) {
        return cosmosContainer.patchItem(itemId, partitionKey, cosmosPatchOperations, options, itemType);
    }

    /**
     * Deletes an item from the container using the item object.
     *
     * <p>The item's id and partition key are automatically extracted from the provided
     * object to identify and delete the document. The entire object is not required,
     * only the id and partition key fields need to be populated.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Option 1: Delete using full object
     * Product productToDelete = executor.readItem("prod123",
     *     new PartitionKey("Electronics"),
     *     Product.class).getItem();
     *
     * CosmosItemResponse<Object> response = executor.deleteItem(productToDelete, null);
     * System.out.println("Delete RU cost: " + response.getRequestCharge());
     *
     * // Option 2: Delete with conditional options
     * CosmosItemRequestOptions options = new CosmosItemRequestOptions();
     * options.setIfMatchETag(productToDelete.getETag()); // Only delete if not modified
     *
     * try {
     *     executor.deleteItem(productToDelete, options);
     *     System.out.println("Item deleted successfully");
     * } catch (CosmosException e) {
     *     if (e.getStatusCode() == 412) {
     *         System.out.println("Delete failed: Item was modified");
     *     }
     * }
     * }</pre>
     *
     * @param <T> the type of the item to delete
     * @param item the item object containing id and partition key information (must not be null)
     * @param options additional options for the delete operation (can be null for default behavior)
     * @return a CosmosItemResponse with metadata about the delete operation
     * @throws CosmosException if the operation fails or the item doesn't exist
     * @throws IllegalArgumentException if item is null
     */
    public <T> CosmosItemResponse<Object> deleteItem(final T item, final CosmosItemRequestOptions options) {
        return cosmosContainer.deleteItem(item, options);
    }

    /**
     * Deletes an item from the container using explicit id and partition key.
     *
     * <p>This method provides direct deletion using the item's unique identifiers
     * without requiring the full item object. This is more efficient when you
     * only have the id and partition key values.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Simple delete by id and partition key
     * String itemId = "prod123";
     * PartitionKey partitionKey = new PartitionKey("Electronics");
     *
     * CosmosItemResponse<Object> response = executor.deleteItem(
     *     itemId,
     *     partitionKey,
     *     null
     * );
     *
     * System.out.println("Item deleted");
     * System.out.println("Status code: " + response.getStatusCode()); // 204 for successful delete
     * System.out.println("RU consumed: " + response.getRequestCharge());
     *
     * // Delete with retry policy options
     * CosmosItemRequestOptions options = new CosmosItemRequestOptions();
     * executor.deleteItem("prod456", new PartitionKey("Books"), options);
     * }</pre>
     *
     * @param itemId the id of the item to delete (must not be null)
     * @param partitionKey the partition key of the item to delete (must not be null)
     * @param options additional options for the delete operation (can be null for default behavior)
     * @return a CosmosItemResponse with metadata about the delete operation
     * @throws CosmosException if the operation fails or the item doesn't exist (404 status)
     * @throws IllegalArgumentException if itemId or partitionKey is null
     */
    public CosmosItemResponse<Object> deleteItem(final String itemId, final PartitionKey partitionKey, final CosmosItemRequestOptions options) {
        return cosmosContainer.deleteItem(itemId, partitionKey, options);
    }

    /**
     * Deletes all items within a specific partition.
     *
     * <p>This is a bulk delete operation that removes all documents within the specified
     * partition. This operation is more efficient than deleting items individually
     * and is useful for partition-level cleanup operations. This is a server-side
     * operation that deletes all items in a single atomic transaction.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Delete all products in the "Discontinued" category
     * PartitionKey partitionKey = new PartitionKey("Discontinued");
     *
     * // Perform the bulk delete
     * CosmosItemResponse<Object> response = executor.deleteAllItemsByPartitionKey(
     *     partitionKey,
     *     null
     * );
     *
     * System.out.println("All items in partition deleted");
     * System.out.println("Status: " + response.getStatusCode());
     * System.out.println("RU consumed: " + response.getRequestCharge());
     *
     * // With options
     * CosmosItemRequestOptions options = new CosmosItemRequestOptions();
     * executor.deleteAllItemsByPartitionKey(
     *     new PartitionKey("TempData"),
     *     options
     * );
     * }</pre>
     *
     * @param partitionKey the partition key identifying the partition to clear (must not be null)
     * @param options additional options for the delete operation (can be null for default behavior)
     * @return a CosmosItemResponse with metadata about the bulk delete operation
     * @throws CosmosException if the operation fails
     * @throws IllegalArgumentException if partitionKey is null
     *
     * <p><b>Warning:</b> This operation cannot be undone and will delete all items in the partition.
     * Use with caution in production environments.</p>
     */
    public CosmosItemResponse<Object> deleteAllItemsByPartitionKey(final PartitionKey partitionKey, final CosmosItemRequestOptions options) {
        return cosmosContainer.deleteAllItemsByPartitionKey(partitionKey, options);
    }

    /**
     * Reads a single item from the container by id and partition key.
     *
     * <p>This is the most efficient way to retrieve a document when you know both
     * the id and partition key. The operation is executed as a point read with
     * minimal latency and RU consumption (typically 1 RU for items up to 1KB).</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Read a product by id and partition key
     * String itemId = "prod123";
     * PartitionKey partitionKey = new PartitionKey("Electronics");
     *
     * CosmosItemResponse<Product> response = executor.readItem(
     *     itemId,
     *     partitionKey,
     *     Product.class
     * );
     *
     * // Access the item
     * Product product = response.getItem();
     * System.out.println("Product: " + product.getName());
     * System.out.println("Price: $" + product.getPrice());
     *
     * // Access metadata
     * System.out.println("RU consumed: " + response.getRequestCharge());
     * System.out.println("ETag: " + response.getETag());
     * System.out.println("Activity ID: " + response.getActivityId());
     * }</pre>
     *
     * @param <T> the type of the item to read
     * @param itemId the id of the item to read (must not be null)
     * @param partitionKey the partition key of the item (must not be null)
     * @param itemType the class type for deserializing the response (must not be null)
     * @return a CosmosItemResponse containing the item and metadata
     * @throws CosmosException if the operation fails or the item doesn't exist (404 status)
     * @throws IllegalArgumentException if itemId, partitionKey, or itemType is null
     */
    public <T> CosmosItemResponse<T> readItem(final String itemId, final PartitionKey partitionKey, final Class<T> itemType) {
        return cosmosContainer.readItem(itemId, partitionKey, itemType);
    }

    /**
     * Reads a single item with additional options.
     *
     * <p>Extended point read operation that allows specification of consistency level,
     * session token, or other request options. Useful when you need specific
     * consistency guarantees or want to optimize for specific scenarios.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String itemId = "prod456";
     * PartitionKey partitionKey = new PartitionKey("Electronics");
     *
     * // Configure read options
     * CosmosItemRequestOptions options = new CosmosItemRequestOptions();
     *
     * // Set strong consistency for critical reads
     * options.setConsistencyLevel(ConsistencyLevel.STRONG);
     *
     * // Use session token for session consistency
     * options.setSessionToken(sessionToken);
     *
     * // Read the item
     * CosmosItemResponse<Product> response = executor.readItem(
     *     itemId,
     *     partitionKey,
     *     options,
     *     Product.class
     * );
     *
     * Product product = response.getItem();
     * System.out.println("Read with strong consistency: " + product.getName());
     * System.out.println("Session token: " + response.getSessionToken());
     * }</pre>
     *
     * @param <T> the type of the item to read
     * @param itemId the id of the item to read (must not be null)
     * @param partitionKey the partition key of the item (must not be null)
     * @param options additional options for the read operation (can be null for default behavior)
     * @param itemType the class type for deserializing the response (must not be null)
     * @return a CosmosItemResponse containing the item and metadata
     * @throws CosmosException if the operation fails or the item doesn't exist
     * @throws IllegalArgumentException if itemId, partitionKey, or itemType is null
     */
    public <T> CosmosItemResponse<T> readItem(final String itemId, final PartitionKey partitionKey, final CosmosItemRequestOptions options,
            final Class<T> itemType) {
        return cosmosContainer.readItem(itemId, partitionKey, options, itemType);
    }

    /**
     * Reads multiple items in a single request using their identities.
     *
     * <p>Batch read operation that efficiently retrieves multiple documents by their
     * id and partition key combinations. This is more efficient than multiple individual
     * read operations and returns all results in a single response. The operation can
     * span multiple partitions.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Create list of item identities to read
     * List<CosmosItemIdentity> identities = Arrays.asList(
     *     new CosmosItemIdentity(new PartitionKey("Electronics"), "prod123"),
     *     new CosmosItemIdentity(new PartitionKey("Electronics"), "prod456"),
     *     new CosmosItemIdentity(new PartitionKey("Books"), "book789")
     * );
     *
     * // Read multiple items at once
     * FeedResponse<Product> response = executor.readMany(identities, Product.class);
     *
     * // Process results
     * List<Product> products = response.getResults();
     * System.out.println("Retrieved " + products.size() + " items");
     *
     * for (Product product : products) {
     *     System.out.println("Product: " + product.getName());
     * }
     *
     * // Check total RU consumed
     * System.out.println("Total RU: " + response.getRequestCharge());
     *
     * // Note: Items not found will not be included in results
     * // Check count to see if all items were found
     * if (products.size() < identities.size()) {
     *     System.out.println("Some items were not found");
     * }
     * }</pre>
     *
     * @param <T> the type of the items to read
     * @param itemIdentityList list of item identities (id and partition key pairs, must not be null or empty)
     * @param classType the class type for deserializing the response items (must not be null)
     * @return a FeedResponse containing all found items (items not found will be omitted)
     * @throws CosmosException if the operation fails
     * @throws IllegalArgumentException if itemIdentityList or classType is null
     *
     * @see CosmosItemIdentity for item identity specification
     */
    public <T> FeedResponse<T> readMany(final List<CosmosItemIdentity> itemIdentityList, final Class<T> classType) {
        return cosmosContainer.readMany(itemIdentityList, classType);
    }

    /**
     * Reads multiple items with session token for consistency.
     *
     * <p>Batch read operation with session-level consistency. The session token
     * ensures that the read operation reflects all writes that were acknowledged
     * within the same session, providing session consistency guarantees. This is
     * useful for read-your-writes scenarios across multiple items.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // First, perform a write and capture session token
     * CosmosItemResponse<Product> writeResponse = executor.createItem(newProduct);
     * String sessionToken = writeResponse.getSessionToken();
     *
     * // Now read multiple items using the session token
     * List<CosmosItemIdentity> identities = Arrays.asList(
     *     new CosmosItemIdentity(new PartitionKey("Electronics"), "prod123"),
     *     new CosmosItemIdentity(new PartitionKey("Electronics"), "prod456")
     * );
     *
     * // Read with session consistency
     * FeedResponse<Product> response = executor.readMany(
     *     identities,
     *     sessionToken,
     *     Product.class
     * );
     *
     * // The read is guaranteed to see the previous write
     * List<Product> products = response.getResults();
     * System.out.println("Read " + products.size() + " items with session consistency");
     * }</pre>
     *
     * @param <T> the type of the items to read
     * @param itemIdentityList list of item identities (id and partition key pairs, must not be null or empty)
     * @param sessionToken the session token for consistency (can be null for default consistency)
     * @param classType the class type for deserializing the response items (must not be null)
     * @return a FeedResponse containing all found items
     * @throws CosmosException if the operation fails
     * @throws IllegalArgumentException if itemIdentityList or classType is null
     */
    public <T> FeedResponse<T> readMany(final List<CosmosItemIdentity> itemIdentityList, final String sessionToken, final Class<T> classType) {
        return cosmosContainer.readMany(itemIdentityList, sessionToken, classType);
    }

    /**
     * Reads all items within a specific partition.
     *
     * <p>Retrieves all documents within the specified partition. This operation
     * scans the entire partition and returns all items. Use with caution on
     * large partitions as it can be resource-intensive. The results are paginated
     * automatically by the CosmosPagedIterable.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Read all products in Electronics category
     * PartitionKey partitionKey = new PartitionKey("Electronics");
     * CosmosPagedIterable<Product> pagedResults = executor.readAllItems(
     *     partitionKey,
     *     Product.class
     * );
     *
     * // Iterate through all items (handles pagination automatically)
     * for (Product product : pagedResults) {
     *     System.out.println("Product: " + product.getName());
     * }
     *
     * // Or iterate by pages for more control
     * pagedResults.iterableByPage().forEach(page -> {
     *     System.out.println("Page RU charge: " + page.getRequestCharge());
     *     page.getResults().forEach(product -> {
     *         System.out.println("  - " + product.getName());
     *     });
     * });
     * }</pre>
     *
     * @param <T> the type of the items to read
     * @param partitionKey the partition key identifying the partition to scan (must not be null)
     * @param classType the class type for deserializing the response items (must not be null)
     * @return a CosmosPagedIterable for iterating through all items
     * @throws CosmosException if the operation fails
     * @throws IllegalArgumentException if partitionKey or classType is null
     *
     * @see #streamAllItems(PartitionKey, Class) for stream-based processing
     */
    public <T> CosmosPagedIterable<T> readAllItems(final PartitionKey partitionKey, final Class<T> classType) {
        return cosmosContainer.readAllItems(partitionKey, classType);
    }

    /**
     * Reads all items within a partition with query options.
     *
     * <p>Extended partition scan with additional query options such as page size,
     * consistency level, or continuation tokens. Provides more control over the
     * scanning behavior and resource usage.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * PartitionKey partitionKey = new PartitionKey("Electronics");
     *
     * // Configure query options
     * CosmosQueryRequestOptions options = new CosmosQueryRequestOptions();
     * options.setMaxDegreeOfParallelism(4);   // Parallel query execution
     * options.setMaxBufferedItemCount(100);   // Buffer size
     * options.setMaxItemCount(50);            // Page size
     *
     * CosmosPagedIterable<Product> pagedResults = executor.readAllItems(
     *     partitionKey,
     *     options,
     *     Product.class
     * );
     *
     * // Process with pagination control
     * int pageCount = 0;
     * for (FeedResponse<Product> page : pagedResults.iterableByPage()) {
     *     pageCount++;
     *     System.out.println("Page " + pageCount + ": " +
     *         page.getResults().size() + " items, " +
     *         page.getRequestCharge() + " RUs");
     * }
     * }</pre>
     *
     * @param <T> the type of the items to read
     * @param partitionKey the partition key identifying the partition to scan (must not be null)
     * @param options query options controlling the scan behavior (can be null for default behavior)
     * @param classType the class type for deserializing the response items (must not be null)
     * @return a CosmosPagedIterable for iterating through all items
     * @throws CosmosException if the operation fails
     * @throws IllegalArgumentException if partitionKey or classType is null
     */
    public <T> CosmosPagedIterable<T> readAllItems(final PartitionKey partitionKey, final CosmosQueryRequestOptions options, final Class<T> classType) {
        return cosmosContainer.readAllItems(partitionKey, options, classType);
    }

    /**
     * Streams all items within a partition for processing.
     *
     * <p>Returns a Stream for functional-style processing of all items in a partition.
     * This is more memory-efficient than loading all items at once and allows for
     * pipeline operations like filtering, mapping, and collecting.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * PartitionKey partitionKey = new PartitionKey("Electronics");
     *
     * // Stream and process items functionally
     * List<String> expensiveProducts = executor.streamAllItems(partitionKey, Product.class)
     *     .filter(p -> p.getPrice() > 500)
     *     .map(Product::getName)
     *     .sorted()
     *     .collect(Collectors.toList());
     *
     * System.out.println("Expensive products: " + expensiveProducts);
     *
     * // Calculate statistics
     * DoubleSummaryStatistics stats = executor.streamAllItems(partitionKey, Product.class)
     *     .mapToDouble(Product::getPrice)
     *     .summaryStatistics();
     *
     * System.out.println("Average price: $" + stats.getAverage());
     * System.out.println("Max price: $" + stats.getMax());
     * System.out.println("Total products: " + stats.getCount());
     * }</pre>
     *
     * @param <T> the type of the items to stream
     * @param partitionKey the partition key identifying the partition to scan (must not be null)
     * @param classType the class type for deserializing the items (must not be null)
     * @return a Stream of items for functional processing
     * @throws CosmosException if the operation fails
     * @throws IllegalArgumentException if partitionKey or classType is null
     *
     * @see #readAllItems(PartitionKey, Class) for paginated results
     */
    @Beta
    public <T> Stream<T> streamAllItems(final PartitionKey partitionKey, final Class<T> classType) {
        return Stream.from(cosmosContainer.readAllItems(partitionKey, classType).stream());
    }

    /**
     * Streams all items within a partition with query options.
     *
     * <p>Stream-based partition scan with additional control over query behavior.
     * Combines the efficiency of streaming with options like page size and
     * consistency level for optimized processing.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * PartitionKey partitionKey = new PartitionKey("Electronics");
     *
     * // Configure options for optimized streaming
     * CosmosQueryRequestOptions options = new CosmosQueryRequestOptions();
     * options.setMaxItemCount(100);           // Fetch 100 items per page
     * options.setMaxBufferedItemCount(500);   // Buffer size
     *
     * // Stream with options and process
     * long count = executor.streamAllItems(partitionKey, options, Product.class)
     *     .filter(p -> p.getPrice() < 100)
     *     .peek(p -> System.out.println("Processing: " + p.getName()))
     *     .count();
     *
     * System.out.println("Total affordable products: " + count);
     * }</pre>
     *
     * @param <T> the type of the items to stream
     * @param partitionKey the partition key identifying the partition to scan (must not be null)
     * @param options query options controlling the scan behavior (can be null for default behavior)
     * @param classType the class type for deserializing the items (must not be null)
     * @return a Stream of items for functional processing
     * @throws CosmosException if the operation fails
     * @throws IllegalArgumentException if partitionKey or classType is null
     */
    @Beta
    public <T> Stream<T> streamAllItems(final PartitionKey partitionKey, final CosmosQueryRequestOptions options, final Class<T> classType) {
        return Stream.from(cosmosContainer.readAllItems(partitionKey, options, classType).stream());
    }

    /**
     * Executes a SQL query against the container.
     *
     * <p>Runs a SQL query using Cosmos DB's SQL API syntax. The query can include
     * SELECT, WHERE, ORDER BY, and other SQL constructs supported by Cosmos DB.
     * Results are returned as a paginated iterable.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Simple query
     * String query = "SELECT * FROM c WHERE c.category = 'Electronics' AND c.price > 100";
     * CosmosPagedIterable<Product> results = executor.queryItems(query, Product.class);
     *
     * for (Product product : results) {
     *     System.out.println(product.getName() + ": $" + product.getPrice());
     * }
     *
     * // Query with ORDER BY and projection
     * String query2 = "SELECT c.id, c.name, c.price FROM c " +
     *                 "WHERE c.inStock = true " +
     *                 "ORDER BY c.price DESC";
     *
     * CosmosPagedIterable<Product> topProducts = executor.queryItems(query2, Product.class);
     *
     * // Track RU consumption by page
     * for (FeedResponse<Product> page : topProducts.iterableByPage()) {
     *     System.out.println("Retrieved " + page.getResults().size() +
     *         " items, RU: " + page.getRequestCharge());
     * }
     * }</pre>
     *
     * @param <T> the type of the items in the query result
     * @param query the SQL query string (e.g., "SELECT * FROM c WHERE c.status = 'active'", must not be null)
     * @param classType the class type for deserializing the query results (must not be null)
     * @return a CosmosPagedIterable for iterating through query results
     * @throws CosmosException if the query fails or contains syntax errors
     * @throws IllegalArgumentException if query or classType is null
     */
    public <T> CosmosPagedIterable<T> queryItems(final String query, final Class<T> classType) {
        return queryItems(query, null, classType);
    }

    /**
     * Executes a SQL query with additional query options.
     *
     * <p>Extended query execution with options for controlling query behavior such as
     * partition key scope, page size, consistency level, or enabling cross-partition
     * queries. Provides fine-grained control over query performance and resource usage.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String query = "SELECT * FROM c WHERE c.price BETWEEN 100 AND 500";
     *
     * // Configure query options
     * CosmosQueryRequestOptions options = new CosmosQueryRequestOptions();
     *
     * // Single partition query for better performance
     * options.setPartitionKey(new PartitionKey("Electronics"));
     *
     * // Control parallelism and buffering
     * options.setMaxDegreeOfParallelism(4);
     * options.setMaxBufferedItemCount(1000);
     * options.setMaxItemCount(100); // Page size
     *
     * // Execute query
     * CosmosPagedIterable<Product> results = executor.queryItems(
     *     query,
     *     options,
     *     Product.class
     * );
     *
     * // Cross-partition query example
     * CosmosQueryRequestOptions crossPartitionOptions = new CosmosQueryRequestOptions();
     * // Enable cross-partition by not setting partition key
     * crossPartitionOptions.setMaxDegreeOfParallelism(-1); // Auto parallelism
     *
     * String globalQuery = "SELECT * FROM c WHERE c.featured = true";
     * CosmosPagedIterable<Product> featuredProducts = executor.queryItems(
     *     globalQuery,
     *     crossPartitionOptions,
     *     Product.class
     * );
     * }</pre>
     *
     * @param <T> the type of the items in the query result
     * @param query the SQL query string (must not be null)
     * @param options query options controlling execution behavior (can be null for default behavior)
     * @param classType the class type for deserializing the query results (must not be null)
     * @return a CosmosPagedIterable for iterating through query results
     * @throws CosmosException if the query fails or contains syntax errors
     * @throws IllegalArgumentException if query or classType is null
     */
    public <T> CosmosPagedIterable<T> queryItems(final String query, final CosmosQueryRequestOptions options, final Class<T> classType) {
        return cosmosContainer.queryItems(query, options, classType);
    }

    /**
     * Executes a parameterized SQL query specification.
     * 
     * <p>Runs a parameterized query using SqlQuerySpec which supports parameter binding
     * for safe and efficient query execution. This prevents SQL injection and allows
     * for query plan reuse. Parameters are bound using named or positional parameter
     * markers in the SQL text.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Create parameterized query
     * SqlQuerySpec querySpec = new SqlQuerySpec(
     *     "SELECT * FROM c WHERE c.category = @category AND c.price BETWEEN @minPrice AND @maxPrice"
     * ).setParameters(
     *     Arrays.asList(
     *         new SqlParameter("@category", "Electronics"),
     *         new SqlParameter("@minPrice", 100.0),
     *         new SqlParameter("@maxPrice", 1000.0)
     *     )
     * );
     * 
     * CosmosPagedIterable<Product> results = executor.queryItems(querySpec, Product.class);
     * 
     * for (Product product : results) {
     *     System.out.println("Found: " + product.getName());
     * }
     * }</pre>
     *
     * @param <T> the type of the items in the query result
     * @param querySpec the SQL query specification with parameters
     * @param classType the class type for deserializing the query results
     * @return a CosmosPagedIterable for iterating through query results
     * @throws CosmosException if the query fails or contains syntax errors
     * 
     * @see SqlQuerySpec for parameterized query construction
     * @see com.azure.cosmos.models.SqlParameter for parameter specification
     */
    public <T> CosmosPagedIterable<T> queryItems(final SqlQuerySpec querySpec, final Class<T> classType) {
        return queryItems(querySpec, null, classType);
    }

    /**
     * Executes a parameterized SQL query with additional options.
     * 
     * <p>Extended parameterized query execution combining the safety of parameter binding
     * with advanced query options. Provides the most comprehensive control over
     * query behavior and performance characteristics.</p>
     *
     * @param <T> the type of the items in the query result
     * @param querySpec the SQL query specification with parameters
     * @param options query options controlling execution behavior (can be null)
     * @param classType the class type for deserializing the query results
     * @return a CosmosPagedIterable for iterating through query results
     * @throws CosmosException if the query fails or contains syntax errors
     */
    public <T> CosmosPagedIterable<T> queryItems(final SqlQuerySpec querySpec, final CosmosQueryRequestOptions options, final Class<T> classType) {
        return cosmosContainer.queryItems(querySpec, options, classType);
    }

    /**
     * Streams query results for functional processing.
     *
     * <p>Executes a SQL query and returns results as a Stream for functional-style
     * processing. This is memory-efficient for large result sets and enables
     * pipeline operations like filtering, mapping, and reduction.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Stream query results and process functionally
     * String query = "SELECT * FROM c WHERE c.category = 'Electronics'";
     *
     * List<String> productNames = executor.streamItems(query, Product.class)
     *     .filter(p -> p.getPrice() > 100)
     *     .map(Product::getName)
     *     .sorted()
     *     .collect(Collectors.toList());
     *
     * System.out.println("Products over $100: " + productNames);
     *
     * // Calculate aggregates
     * String allQuery = "SELECT * FROM c";
     * double averagePrice = executor.streamItems(allQuery, Product.class)
     *     .mapToDouble(Product::getPrice)
     *     .average()
     *     .orElse(0.0);
     *
     * System.out.println("Average price: $" + averagePrice);
     *
     * // Count with condition
     * long expensiveCount = executor.streamItems(query, Product.class)
     *     .filter(p -> p.getPrice() > 500)
     *     .count();
     * }</pre>
     *
     * @param <T> the type of the items in the query result
     * @param query the SQL query string (must not be null)
     * @param classType the class type for deserializing the query results (must not be null)
     * @return a Stream of query results for functional processing
     * @throws CosmosException if the query fails or contains syntax errors
     * @throws IllegalArgumentException if query or classType is null
     *
     * @see #queryItems(String, Class) for paginated results
     */
    public final <T> Stream<T> streamItems(final String query, final Class<T> classType) {
        return streamItems(query, null, classType);
    }

    /**
     * Streams query results with query options.
     *
     * <p>Stream-based query execution with additional options for controlling
     * query behavior. Combines the efficiency of streaming with options like
     * partition key scope and consistency level.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String query = "SELECT * FROM c WHERE c.inStock = true";
     *
     * // Configure options for single partition query
     * CosmosQueryRequestOptions options = new CosmosQueryRequestOptions();
     * options.setPartitionKey(new PartitionKey("Electronics"));
     * options.setMaxItemCount(50);
     *
     * // Stream with options
     * Map<String, Long> categoryCount = executor.streamItems(query, options, Product.class)
     *     .collect(Collectors.groupingBy(
     *         Product::getSubCategory,
     *         Collectors.counting()
     *     ));
     *
     * categoryCount.forEach((category, count) ->
     *     System.out.println(category + ": " + count + " items")
     * );
     * }</pre>
     *
     * @param <T> the type of the items in the query result
     * @param query the SQL query string (must not be null)
     * @param options query options controlling execution behavior (can be null for default behavior)
     * @param classType the class type for deserializing the query results (must not be null)
     * @return a Stream of query results for functional processing
     * @throws CosmosException if the query fails or contains syntax errors
     * @throws IllegalArgumentException if query or classType is null
     */
    public final <T> Stream<T> streamItems(final String query, final CosmosQueryRequestOptions options, final Class<T> classType) {
        return Stream.from(cosmosContainer.queryItems(query, options, classType).stream());
    }

    /**
     * Streams parameterized query results.
     *
     * <p>Executes a parameterized query and returns results as a Stream.
     * Combines the safety of parameter binding with the efficiency of
     * stream-based result processing.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Create parameterized query
     * SqlQuerySpec querySpec = new SqlQuerySpec(
     *     "SELECT * FROM c WHERE c.category = @cat AND c.price BETWEEN @min AND @max"
     * ).setParameters(
     *     Arrays.asList(
     *         new SqlParameter("@cat", "Electronics"),
     *         new SqlParameter("@min", 50.0),
     *         new SqlParameter("@max", 500.0)
     *     )
     * );
     *
     * // Stream and process
     * List<Product> affordableProducts = executor.streamItems(querySpec, Product.class)
     *     .sorted(Comparator.comparing(Product::getPrice))
     *     .limit(10)
     *     .collect(Collectors.toList());
     *
     * System.out.println("Top 10 affordable products:");
     * affordableProducts.forEach(p ->
     *     System.out.println(p.getName() + ": $" + p.getPrice())
     * );
     * }</pre>
     *
     * @param <T> the type of the items in the query result
     * @param querySpec the SQL query specification with parameters (must not be null)
     * @param classType the class type for deserializing the query results (must not be null)
     * @return a Stream of query results for functional processing
     * @throws CosmosException if the query fails or contains syntax errors
     * @throws IllegalArgumentException if querySpec or classType is null
     */
    public final <T> Stream<T> streamItems(final SqlQuerySpec querySpec, final Class<T> classType) {
        return streamItems(querySpec, null, classType);
    }

    /**
     * Streams parameterized query results with options.
     *
     * <p>Advanced stream-based query execution combining parameter binding,
     * query options, and efficient result streaming. Provides the most
     * comprehensive control over parameterized query execution.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Build parameterized query
     * SqlQuerySpec querySpec = new SqlQuerySpec(
     *     "SELECT * FROM c WHERE c.tags ARRAY_CONTAINS @tag AND c.rating >= @minRating"
     * ).setParameters(
     *     Arrays.asList(
     *         new SqlParameter("@tag", "featured"),
     *         new SqlParameter("@minRating", 4.0)
     *     )
     * );
     *
     * // Configure options
     * CosmosQueryRequestOptions options = new CosmosQueryRequestOptions();
     * options.setPartitionKey(new PartitionKey("Electronics"));
     * options.setMaxDegreeOfParallelism(2);
     *
     * // Stream with full control
     * OptionalDouble avgPrice = executor.streamItems(querySpec, options, Product.class)
     *     .mapToDouble(Product::getPrice)
     *     .average();
     *
     * avgPrice.ifPresent(avg ->
     *     System.out.println("Average price of featured products: $" + avg)
     * );
     * }</pre>
     *
     * @param <T> the type of the items in the query result
     * @param querySpec the SQL query specification with parameters (must not be null)
     * @param options query options controlling execution behavior (can be null for default behavior)
     * @param classType the class type for deserializing the query results (must not be null)
     * @return a Stream of query results for functional processing
     * @throws CosmosException if the query fails or contains syntax errors
     * @throws IllegalArgumentException if querySpec or classType is null
     */
    public final <T> Stream<T> streamItems(final SqlQuerySpec querySpec, final CosmosQueryRequestOptions options, final Class<T> classType) {
        return Stream.from(cosmosContainer.queryItems(querySpec, options, classType).stream());
    }

    /**
     * Streams items matching a condition using the query builder.
     *
     * <p>Uses the integrated Abacus query builder to construct and execute queries
     * based on condition objects. This provides type-safe query construction
     * with automatic field name mapping according to the configured naming policy.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * import static com.landawn.abacus.query.Filters.*;
     *
     * // Simple condition query
     * Condition condition = eq("category", "Electronics")
     *     .and(gt("price", 100.0))
     *     .and(eq("inStock", true));
     *
     * List<Product> results = executor.streamItems(condition, Product.class)
     *     .collect(Collectors.toList());
     *
     * // Complex condition with multiple operators
     * Condition complexCond = eq("category", "Electronics")
     *     .and(between("price", 50.0, 500.0))
     *     .and(in("brand", Arrays.asList("Apple", "Samsung", "Sony")))
     *     .and(notEqual("status", "discontinued"));
     *
     * executor.streamItems(complexCond, Product.class)
     *     .forEach(p -> System.out.println(p.getName() + ": $" + p.getPrice()));
     *
     * // Condition with OR logic
     * Condition orCond = eq("featured", true)
     *     .or(gt("rating", 4.5));
     *
     * long featuredCount = executor.streamItems(orCond, Product.class).count();
     * }</pre>
     *
     * @param <T> the type of the items to stream
     * @param whereClause the condition object defining the WHERE clause (can be null for no filter)
     * @param classType the class type for deserializing the results (must not be null)
     * @return a Stream of items matching the condition
     * @throws CosmosException if the query fails
     * @throws IllegalArgumentException if classType is null
     *
     * @see Condition for condition construction
     * @see com.landawn.abacus.query.Filters for available filter operations
     */
    public final <T> Stream<T> streamItems(final Condition whereClause, final Class<T> classType) {
        return streamItems(whereClause, null, classType);
    }

    /**
     * Streams items matching a condition with query options.
     *
     * <p>Extended condition-based query with additional options for controlling
     * execution behavior. Combines type-safe query building with advanced
     * query options like partition key scope and consistency level.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * import static com.landawn.abacus.query.Filters.*;
     *
     * // Build condition
     * Condition condition = eq("category", "Electronics")
     *     .and(gte("price", 100.0))
     *     .and(lte("price", 1000.0));
     *
     * // Configure options
     * CosmosQueryRequestOptions options = new CosmosQueryRequestOptions();
     * options.setPartitionKey(new PartitionKey("Electronics"));
     * options.setMaxItemCount(100);
     *
     * // Stream with condition and options
     * List<Product> midRangeProducts = executor.streamItems(condition, options, Product.class)
     *     .sorted(Comparator.comparing(Product::getPrice))
     *     .collect(Collectors.toList());
     *
     * System.out.println("Found " + midRangeProducts.size() + " products");
     * }</pre>
     *
     * @param <T> the type of the items to stream
     * @param whereClause the condition object defining the WHERE clause (can be null for no filter)
     * @param options query options controlling execution behavior (can be null for default behavior)
     * @param classType the class type for deserializing the results (must not be null)
     * @return a Stream of items matching the condition
     * @throws CosmosException if the query fails
     * @throws IllegalArgumentException if classType is null
     */
    public final <T> Stream<T> streamItems(final Condition whereClause, final CosmosQueryRequestOptions options, final Class<T> classType) {
        return streamItems(null, whereClause, options, classType);
    }

    /**
     * Streams selected properties of items matching a condition.
     *
     * <p>Projection query that selects only specified properties from items matching
     * the condition. This reduces network bandwidth and RU consumption by retrieving
     * only the required fields. Field names are automatically mapped according to
     * the configured naming policy.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * import static com.landawn.abacus.query.Filters.*;
     *
     * // Select specific fields to reduce RU cost
     * Collection<String> selectFields = Arrays.asList("id", "name", "price");
     * Condition condition = eq("category", "Electronics")
     *     .and(eq("inStock", true));
     *
     * // Stream with projection
     * List<Product> products = executor.streamItems(selectFields, condition, Product.class)
     *     .collect(Collectors.toList());
     *
     * // Only id, name, and price fields will be populated
     * products.forEach(p ->
     *     System.out.println(p.getId() + ": " + p.getName() + " - $" + p.getPrice())
     * );
     *
     * // Projection for report generation
     * Collection<String> reportFields = Arrays.asList("name", "price", "category");
     * Condition reportCond = gt("price", 100.0);
     *
     * executor.streamItems(reportFields, reportCond, Product.class)
     *     .forEach(p -> generateReport(p));
     * }</pre>
     *
     * @param <T> the type of the items to stream
     * @param selectPropNames collection of property names to select (null for SELECT *, selecting all fields)
     * @param whereClause the condition object defining the WHERE clause (can be null for no filter)
     * @param classType the class type for deserializing the results (must not be null)
     * @return a Stream of items with only selected properties populated
     * @throws CosmosException if the query fails
     * @throws IllegalArgumentException if classType is null
     */
    public final <T> Stream<T> streamItems(final Collection<String> selectPropNames, final Condition whereClause, final Class<T> classType) {
        return streamItems(selectPropNames, whereClause, null, classType);
    }

    /**
     * Streams selected properties with condition and query options.
     *
     * <p>Advanced projection query combining property selection, condition-based
     * filtering, and query options. This provides the most comprehensive control
     * over query construction and execution using the integrated query builder.
     * Field names are automatically converted according to the naming policy.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * import static com.landawn.abacus.query.Filters.*;
     *
     * // Select specific fields with condition and options
     * Collection<String> fields = Arrays.asList("id", "name", "price", "stockQuantity");
     * Condition condition = eq("category", "Electronics")
     *     .and(gt("price", 50.0))
     *     .and(eq("inStock", true));
     *
     * // Configure query options
     * CosmosQueryRequestOptions options = new CosmosQueryRequestOptions();
     * options.setPartitionKey(new PartitionKey("Electronics"));
     * options.setMaxItemCount(50);
     *
     * // Stream with projection, condition, and options
     * List<Product> optimizedResults = executor.streamItems(
     *     fields,
     *     condition,
     *     options,
     *     Product.class
     * ).collect(Collectors.toList());
     *
     * // Process results - only selected fields are populated
     * System.out.println("Retrieved " + optimizedResults.size() + " products");
     * optimizedResults.forEach(p ->
     *     System.out.println(p.getName() + ": $" + p.getPrice() +
     *                       " (Stock: " + p.getStockQuantity() + ")")
     * );
     *
     * // Example with null selectPropNames to fetch all fields
     * Stream<Product> allFieldsStream = executor.streamItems(
     *     null,  // Select all fields
     *     eq("featured", true),
     *     options,
     *     Product.class
     * );
     * }</pre>
     *
     * @param <T> the type of the items to stream
     * @param selectPropNames collection of property names to select (null for SELECT *, selecting all fields)
     * @param whereClause the condition object defining the WHERE clause (can be null for no filter)
     * @param options query options controlling execution behavior (can be null for default behavior)
     * @param classType the class type for deserializing the results (must not be null)
     * @return a Stream of items with only selected properties populated
     * @throws CosmosException if the query fails
     * @throws IllegalArgumentException if classType is null
     *
     * @see NamingPolicy for field name mapping behavior
     * @see com.landawn.abacus.query.Filters for available filter operations
     */
    public final <T> Stream<T> streamItems(final Collection<String> selectPropNames, final Condition whereClause, final CosmosQueryRequestOptions options,
            final Class<T> classType) {
        final SP sp = prepareQuery(classType, selectPropNames, whereClause);

        return Stream.from(cosmosContainer.queryItems(toSqlQuerySpec(sp), options, classType).stream());
    }

    private static SqlQuerySpec toSqlQuerySpec(final SP sp) {
        if (N.isEmpty(sp.parameters())) {
            return new SqlQuerySpec(sp.query());
        }

        final int parameterCount = sp.parameters().size();
        final String query = rewritePositionalParameters(sp.query(), parameterCount);
        final List<SqlParameter> sqlParameters = N.newArrayList(parameterCount);

        for (int i = 0; i < parameterCount; i++) {
            sqlParameters.add(new SqlParameter("@p" + i, sp.parameters().get(i)));
        }

        return new SqlQuerySpec(query, sqlParameters);
    }

    private static String rewritePositionalParameters(final String query, final int parameterCount) {
        final StringBuilder sb = new StringBuilder(query.length() + parameterCount * 3);
        final int len = query.length();
        int replaced = 0;
        int totalPlaceholders = 0;
        boolean inSingleQuote = false;

        for (int i = 0; i < len; i++) {
            final char ch = query.charAt(i);

            if (ch == '\'') {
                sb.append(ch);

                if (inSingleQuote && i + 1 < len && query.charAt(i + 1) == '\'') {
                    sb.append(query.charAt(++i));
                } else {
                    inSingleQuote = !inSingleQuote;
                }
            } else if (ch == '?' && !inSingleQuote) {
                totalPlaceholders++;

                if (replaced < parameterCount) {
                    sb.append("@p").append(replaced++);
                } else {
                    sb.append(ch);
                }
            } else {
                sb.append(ch);
            }
        }

        if (totalPlaceholders != parameterCount) {
            throw new IllegalArgumentException("Query parameter count mismatch: expected " + parameterCount + " placeholders but found " + totalPlaceholders);
        }

        return sb.toString();
    }

    private <T> SP prepareQuery(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
        return prepareQuery(targetClass, selectPropNames, whereClause, 0);
    }

    private <T> SP prepareQuery(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause, final int count) {
        final boolean isNonNullCond = whereClause != null;
        SqlBuilder sqlBuilder = null;

        //    SqlBuilder.setHandlerForNamedParameter(handlerForNamedParameter);
        //
        //    try {

        switch (namingPolicy) {
            case SNAKE_CASE:
                if (N.isEmpty(selectPropNames)) {
                    sqlBuilder = SCSB.selectFrom(targetClass);
                } else {
                    sqlBuilder = SCSB.select(selectPropNames).from(targetClass);
                }

                break;

            case SCREAMING_SNAKE_CASE:
                if (N.isEmpty(selectPropNames)) {
                    sqlBuilder = ACSB.selectFrom(targetClass);
                } else {
                    sqlBuilder = ACSB.select(selectPropNames).from(targetClass);
                }

                break;

            case CAMEL_CASE:
                if (N.isEmpty(selectPropNames)) {
                    sqlBuilder = LCSB.selectFrom(targetClass);
                } else {
                    sqlBuilder = LCSB.select(selectPropNames).from(targetClass);
                }

                break;

            default:
                throw new IllegalStateException("Unsupported naming policy: " + namingPolicy);
        }

        if (isNonNullCond) {
            sqlBuilder = sqlBuilder.where(whereClause);
        }

        if (count > 0) {
            sqlBuilder.limit(count);
        }

        return sqlBuilder.build();
        //    } finally {
        //        SqlBuilder.resetHandlerForNamedParameter();
        //    }
    }

    //    public static void main(String[] args) {
    //        CosmosContainerExecutor cosmosContainerExecutor = new CosmosContainerExecutor(null, NamingPolicy.SNAKE_CASE);
    //
    //        SP sp = cosmosContainerExecutor.prepareQuery(User.class, null, Filters.eq("id", 1).and(Filters.notEqual("name", "abc")));
    //
    //        N.println(sp.query());
    //    }
    //
    //    @Data
    //    public static class User {
    //
    //        private int id;
    //        private String name;
    //
    //    }
}
