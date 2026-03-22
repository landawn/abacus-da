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

package com.landawn.abacus.da.hbase;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.stream.Stream;

/**
 * Asynchronous wrapper for HBase database operations that provides non-blocking access to Apache HBase.
 * This executor returns {@code ContinuableFuture} instances for all operations, enabling high-performance,
 * concurrent access patterns and reactive programming models.
 *
 * <p>All methods in this class are asynchronous counterparts to the synchronous methods in {@link HBaseExecutor}.
 * Operations are executed on a configurable thread pool, allowing the calling thread to continue processing
 * while HBase operations complete in the background.</p>
 *
 * <h2>Key Features</h2>
 * <ul>
 * <li><strong>Non-blocking Operations</strong>: All methods return immediately with a {@code ContinuableFuture}</li>
 * <li><strong>Thread Pool Management</strong>: Configurable executor for controlling concurrency</li>
 * <li><strong>Row Key Operations</strong>: Async existence checks, gets, puts, deletes with row key support</li>
 * <li><strong>Batch Operations</strong>: Async batch gets, puts, and deletes for high throughput</li>
 * <li><strong>Scanning</strong>: Async table scans with column family and qualifier filtering</li>
 * <li><strong>Entity Mapping</strong>: Automatic object-relational mapping with async support</li>
 * <li><strong>Coprocessor Support</strong>: Async coprocessor execution and batch processing</li>
 * <li><strong>Atomic Operations</strong>: Async increments, appends, and row mutations</li>
 * </ul>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Get executor and perform async operations
 * AsyncHBaseExecutor async = hbaseExecutor.async();
 *
 * // Async existence check
 * ContinuableFuture<Boolean> existsFuture = async.exists("users", AnyGet.of("user123"));
 *
 * // Async get with entity mapping
 * ContinuableFuture<User> userFuture = async.get("users", "user123", User.class);
 *
 * // Async batch operations
 * List<AnyPut> puts = Arrays.asList(
 *     AnyPut.of("user1").addColumn("info", "name", "Alice"),
 *     AnyPut.of("user2").addColumn("info", "name", "Bob")
 * );
 * ContinuableFuture<Void> putFuture = async.put("users", puts);
 *
 * // Async scanning with filtering
 * ContinuableFuture<Stream<Result>> scanFuture =
 *     async.scan("users", AnyScan.of().setStartRow("user1").setStopRow("user2"));
 *
 * // Chain async operations
 * async.get("users", "user123", User.class)
 *      .thenCallAsync(user -> { user.setLastLogin(new Date()); return user; })
 *      .thenCompose(user -> async.put("users", AnyPut.of(user)))
 *      .thenRunAsync(() -> System.out.println("User updated"));
 * }</pre>
 *
 * <h3>Performance Considerations:</h3>
 * <ul>
 * <li><strong>Thread Pool Sizing</strong>: Default pool size scales with CPU cores (8-16x)</li>
 * <li><strong>Connection Sharing</strong>: Shares HBase connection with synchronous executor</li>
 * <li><strong>Memory Management</strong>: Results are processed asynchronously to avoid blocking</li>
 * <li><strong>Error Handling</strong>: Exceptions are wrapped in the returned futures</li>
 * </ul>
 *
 * @see HBaseExecutor
 * @see ContinuableFuture
 * @see <a href="http://hbase.apache.org/devapidocs/index.html">Apache HBase Java API Documentation</a>
 * @see org.apache.hadoop.hbase.client.Table
 */
public final class AsyncHBaseExecutor {

    private final HBaseExecutor hbaseExecutor;

    private final AsyncExecutor asyncExecutor;

    AsyncHBaseExecutor(final HBaseExecutor hbaseExecutor, final AsyncExecutor asyncExecutor) {
        this.hbaseExecutor = hbaseExecutor;
        this.asyncExecutor = asyncExecutor;
    }

    /**
     * Returns the underlying synchronous HBase executor for blocking operations.
     *
     * <p>This method provides access to the synchronous executor that this async wrapper
     * delegates to. Use this when you need to perform blocking operations or when integrating
     * with synchronous code paths.</p>
     *
     * @return the synchronous HBase executor instance
     * @see HBaseExecutor
     */
    public HBaseExecutor sync() {
        return hbaseExecutor;
    }

    /**
     * Asynchronously checks if a row exists in the specified HBase table.
     *
     * <p>This is a server-side operation that checks for row existence without transferring
     * any actual data to the client, making it efficient for existence checks.</p>
     *
     * @param tableName the name of the HBase table to check
     * @param get the Get operation specifying the row to check for existence
     * @return a ContinuableFuture that completes with {@code true} if the row exists, {@code false} otherwise
     * @see Get
     */
    public ContinuableFuture<Boolean> exists(final String tableName, final Get get) {
        return asyncExecutor.execute(() -> hbaseExecutor.exists(tableName, get));
    }

    /**
     * Asynchronously checks the existence of multiple rows in the specified HBase table.
     *
     * <p>Performs batch existence checks for multiple rows. This is a server-side operation
     * that efficiently checks for row existence without transferring data to the client.
     * The returned list will have the same order as the input list.</p>
     *
     * @param tableName the name of the HBase table to check
     * @param gets the list of Get operations specifying the rows to check
     * @return a ContinuableFuture containing a list of boolean values indicating existence
     * @see Get
     */
    public ContinuableFuture<List<Boolean>> exists(final String tableName, final List<Get> gets) {
        return asyncExecutor.execute(() -> hbaseExecutor.exists(tableName, gets));
    }

    //    /**
    //     * Test for the existence of columns in the table, as specified by the Gets.
    //     * This will return an array of booleans. Each value will be true if the related Get matches
    //     * one or more keys, {@code false} if not.
    //     * This is a server-side call so it prevents any data from being transferred to
    //     * the client.
    //     *
    //     * @param tableName
    //     * @param gets
    //     * @return Array of boolean.  True if the specified Get matches one or more keys, {@code false} if not.
    //     * @deprecated since 2.0 version and will be removed in 3.0 version.
    //     *             use {@code exists(List)}
    //     */
    //    @SuppressWarnings("deprecation")
    //    @Deprecated
    //    public ContinuableFuture<List<Boolean>> existsAll(final String tableName, final List<Get> gets) {
    //        return asyncExecutor.execute((Callable<List<Boolean>>) () -> hbaseExecutor.existsAll(tableName, gets));
    //    }

    /**
     * Asynchronously checks if a row exists using an AnyGet operation builder.
     *
     * <p>This method provides a fluent API for existence checks using the AnyGet builder,
     * which allows for more readable and maintainable code when specifying row keys,
     * column families, and qualifiers.</p>
     *
     * @param tableName the name of the HBase table to check
     * @param anyGet the AnyGet operation builder specifying the row to check
     * @return a ContinuableFuture that completes with {@code true} if the row exists, {@code false} otherwise
     * @see AnyGet
     */
    public ContinuableFuture<Boolean> exists(final String tableName, final AnyGet anyGet) {
        return asyncExecutor.execute(() -> hbaseExecutor.exists(tableName, anyGet));
    }

    /**
     * Asynchronously checks the existence of multiple rows using AnyGet operation builders.
     *
     * <p>Performs batch existence checks using a collection of AnyGet builders. This method
     * provides the convenience of the fluent API while maintaining the efficiency of batch
     * operations for multiple existence checks.</p>
     *
     * @param tableName the name of the HBase table to check
     * @param anyGets the collection of AnyGet builders specifying the rows to check
     * @return a ContinuableFuture containing a list of boolean values indicating existence
     * @see AnyGet
     */
    public ContinuableFuture<List<Boolean>> exists(final String tableName, final Collection<AnyGet> anyGets) {
        return asyncExecutor.execute(() -> hbaseExecutor.exists(tableName, anyGets));
    }

    //    @SuppressWarnings("deprecation")
    //    @Deprecated
    //    public ContinuableFuture<List<Boolean>> existsAll(final String tableName, final Collection<AnyGet> anyGets) {
    //        return asyncExecutor.execute((Callable<List<Boolean>>) () -> hbaseExecutor.existsAll(tableName, anyGets));
    //    }

    /**
     * Asynchronously retrieves a single row from the specified HBase table.
     *
     * <p>Performs a get operation to retrieve data from a specific row. The returned Result
     * object contains all the cells that match the Get operation's criteria, including
     * column family and qualifier specifications, time ranges, and version limits.</p>
     *
     * @param tableName the name of the HBase table to retrieve from
     * @param get the Get operation specifying the row and columns to retrieve
     * @return a ContinuableFuture containing the Result object with the retrieved data
     * @see Get
     * @see Result
     */
    public ContinuableFuture<Result> get(final String tableName, final Get get) {
        return asyncExecutor.execute(() -> hbaseExecutor.get(tableName, get));
    }

    /**
     * Asynchronously retrieves multiple rows from the specified HBase table.
     *
     * <p>Performs batch get operations to retrieve data from multiple rows efficiently.
     * This method is optimized for retrieving many rows in a single round-trip to the
     * HBase region servers. The returned list maintains the same order as the input list.</p>
     *
     * @param tableName the name of the HBase table to retrieve from
     * @param gets the list of Get operations specifying the rows and columns to retrieve
     * @return a ContinuableFuture containing a list of Result objects with the retrieved data
     * @see Get
     * @see Result
     */
    public ContinuableFuture<List<Result>> get(final String tableName, final List<Get> gets) {
        return asyncExecutor.execute(() -> hbaseExecutor.get(tableName, gets));
    }

    /**
     * Asynchronously retrieves a single row using an AnyGet operation builder.
     *
     * @param tableName the name of the HBase table to retrieve from
     * @param anyGet the AnyGet operation builder specifying the row and columns
     * @return a ContinuableFuture containing the Result object with the retrieved data
     * @see AnyGet
     */
    public ContinuableFuture<Result> get(final String tableName, final AnyGet anyGet) {
        return asyncExecutor.execute(() -> hbaseExecutor.get(tableName, anyGet));
    }

    /**
     * Asynchronously retrieves multiple rows using AnyGet operation builders.
     *
     * @param tableName the name of the HBase table to retrieve from
     * @param anyGets the collection of AnyGet builders specifying the rows to retrieve
     * @return a ContinuableFuture containing a list of Result objects
     * @see AnyGet
     */
    public ContinuableFuture<List<Result>> get(final String tableName, final Collection<AnyGet> anyGets) {
        return asyncExecutor.execute(() -> hbaseExecutor.get(tableName, anyGets));
    }

    /**
     * Asynchronously retrieves a row and converts it to the specified type.
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table to retrieve from
     * @param get the Get operation specifying the row to retrieve
     * @param targetClass the class to convert the result to
     * @return a ContinuableFuture containing the converted object
     * @see Get
     */
    public <T> ContinuableFuture<T> get(final String tableName, final Get get, final Class<T> targetClass) {
        return asyncExecutor.execute(() -> hbaseExecutor.get(tableName, get, targetClass));
    }

    /**
     * Asynchronously retrieves multiple rows and converts them to the specified type.
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table to retrieve from
     * @param gets the list of Get operations specifying the rows to retrieve
     * @param targetClass the class to convert each result to
     * @return a ContinuableFuture containing a list of converted objects
     * @see Get
     */
    public <T> ContinuableFuture<List<T>> get(final String tableName, final List<Get> gets, final Class<T> targetClass) {
        return asyncExecutor.execute(() -> hbaseExecutor.get(tableName, gets, targetClass));
    }

    /**
     * Asynchronously retrieves a row using AnyGet and converts it to the specified type.
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table to retrieve from
     * @param anyGet the AnyGet operation builder specifying the row
     * @param targetClass the class to convert the result to
     * @return a ContinuableFuture containing the converted object
     * @see AnyGet
     */
    public <T> ContinuableFuture<T> get(final String tableName, final AnyGet anyGet, final Class<T> targetClass) {
        return asyncExecutor.execute(() -> hbaseExecutor.get(tableName, anyGet, targetClass));
    }

    /**
     * Asynchronously retrieves multiple rows using AnyGet builders and converts them to the specified type.
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table to retrieve from
     * @param anyGets the collection of AnyGet builders specifying the rows
     * @param targetClass the class to convert each result to
     * @return a ContinuableFuture containing a list of converted objects
     * @see AnyGet
     */
    public <T> ContinuableFuture<List<T>> get(final String tableName, final Collection<AnyGet> anyGets, final Class<T> targetClass) {
        return asyncExecutor.execute(() -> hbaseExecutor.get(tableName, anyGets, targetClass));
    }

    /**
     * Asynchronously scans the specified HBase table for all rows in a column family.
     *
     * <p>Performs a full table scan retrieving all cells from the specified column family.
     * The returned ContinuableFuture completes with a Stream of Result objects that can be
     * processed using functional programming constructs. The stream must be closed after use.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.scan("users", "info")
     *      .thenRunAsync(stream -> stream.forEach(result -> System.out.println(result)));
     * }</pre>
     *
     * @param tableName the name of the HBase table to scan
     * @param family the column family name to retrieve (as String)
     * @return a ContinuableFuture containing a Stream of Result objects from the scan
     * @see Scan
     * @see Result
     */
    public ContinuableFuture<Stream<Result>> scan(final String tableName, final String family) {
        return asyncExecutor.execute(() -> hbaseExecutor.scan(tableName, family));
    }

    /**
     * Asynchronously scans the specified HBase table for all rows with a specific column.
     *
     * <p>Performs a full table scan retrieving only the specified column (family:qualifier).
     * This is more efficient than scanning the entire family when only specific columns are needed.
     * The returned stream must be closed after use.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.scan("users", "info", "name")
     *      .thenCallAsync(stream -> stream.map(r -> Bytes.toString(r.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")))).toList());
     * }</pre>
     *
     * @param tableName the name of the HBase table to scan
     * @param family the column family name (as String)
     * @param qualifier the column qualifier name (as String)
     * @return a ContinuableFuture containing a Stream of Result objects with the specified column
     * @see Scan
     * @see Result
     */
    public ContinuableFuture<Stream<Result>> scan(final String tableName, final String family, final String qualifier) {
        return asyncExecutor.execute(() -> hbaseExecutor.scan(tableName, family, qualifier));
    }

    /**
     * Asynchronously scans the specified HBase table for all rows in a column family.
     *
     * <p>Performs a full table scan retrieving all cells from the specified column family.
     * This method accepts the family name as a byte array for efficiency when the bytes
     * are already available. The returned stream must be closed after use.</p>
     *
     * @param tableName the name of the HBase table to scan
     * @param family the column family name as a byte array
     * @return a ContinuableFuture containing a Stream of Result objects from the scan
     * @see Scan
     * @see Result
     */
    public ContinuableFuture<Stream<Result>> scan(final String tableName, final byte[] family) {
        return asyncExecutor.execute(() -> hbaseExecutor.scan(tableName, family));
    }

    /**
     * Asynchronously scans the specified HBase table for all rows with a specific column.
     *
     * <p>Performs a full table scan retrieving only the specified column (family:qualifier).
     * This method accepts both family and qualifier as byte arrays for efficiency when the
     * bytes are already available. The returned stream must be closed after use.</p>
     *
     * @param tableName the name of the HBase table to scan
     * @param family the column family name as a byte array
     * @param qualifier the column qualifier name as a byte array
     * @return a ContinuableFuture containing a Stream of Result objects with the specified column
     * @see Scan
     * @see Result
     */
    public ContinuableFuture<Stream<Result>> scan(final String tableName, final byte[] family, final byte[] qualifier) {
        return asyncExecutor.execute(() -> hbaseExecutor.scan(tableName, family, qualifier));
    }

    /**
     * Asynchronously scans the specified HBase table using a fluent AnyScan builder.
     *
     * <p>Performs a table scan with the criteria specified in the AnyScan builder. AnyScan provides
     * a fluent API for configuring scan parameters including start/stop rows, filters, column families,
     * time ranges, and result limits. The returned stream must be closed after use.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.scan("users", AnyScan.of().setStartRow("user1").setStopRow("user9").setLimit(100))
     *      .thenRunAsync(stream -> stream.forEach(System.out::println));
     * }</pre>
     *
     * @param tableName the name of the HBase table to scan
     * @param anyScan the AnyScan builder specifying scan criteria
     * @return a ContinuableFuture containing a Stream of Result objects matching the scan criteria
     * @see AnyScan
     * @see Result
     */
    public ContinuableFuture<Stream<Result>> scan(final String tableName, final AnyScan anyScan) {
        return asyncExecutor.execute(() -> hbaseExecutor.scan(tableName, anyScan));
    }

    /**
     * Asynchronously scans the specified HBase table using a native Scan object.
     *
     * <p>Performs a table scan with the criteria specified in the HBase Scan object. This method
     * provides direct access to all HBase scan capabilities including filters, column selection,
     * time ranges, caching, and batching. The returned stream must be closed after use.</p>
     *
     * @param tableName the name of the HBase table to scan
     * @param scan the HBase Scan object specifying scan criteria
     * @return a ContinuableFuture containing a Stream of Result objects matching the scan criteria
     * @see Scan
     * @see Result
     */
    public ContinuableFuture<Stream<Result>> scan(final String tableName, final Scan scan) {
        return asyncExecutor.execute(() -> hbaseExecutor.scan(tableName, scan));
    }

    /**
     * Asynchronously scans the table and converts results to the specified target type.
     *
     * <p>Performs a full table scan of the specified column family and automatically converts each
     * Result to the target type. For entity classes, performs object-relational mapping. For
     * primitive types, extracts the single cell value. The returned stream must be closed after use.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.scan("users", "info", User.class)
     *      .thenRunAsync(stream -> stream.forEach(user -> System.out.println(user.getName())));
     * }</pre>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table to scan
     * @param family the column family name (as String)
     * @param targetClass the class to convert each result to
     * @return a ContinuableFuture containing a Stream of converted objects
     * @see Scan
     */
    public <T> ContinuableFuture<Stream<T>> scan(final String tableName, final String family, final Class<T> targetClass) {
        return asyncExecutor.execute(() -> hbaseExecutor.scan(tableName, family, targetClass));
    }

    /**
     * Asynchronously scans the table for a specific column and converts results to the target type.
     *
     * <p>Performs a full table scan retrieving only the specified column (family:qualifier) and
     * automatically converts each Result to the target type. This is efficient for retrieving
     * single-valued entities. The returned stream must be closed after use.</p>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table to scan
     * @param family the column family name (as String)
     * @param qualifier the column qualifier name (as String)
     * @param targetClass the class to convert each result to
     * @return a ContinuableFuture containing a Stream of converted objects
     * @see Scan
     */
    public <T> ContinuableFuture<Stream<T>> scan(final String tableName, final String family, final String qualifier, final Class<T> targetClass) {
        return asyncExecutor.execute(() -> hbaseExecutor.scan(tableName, family, qualifier, targetClass));
    }

    /**
     * Asynchronously scans the table and converts results to the specified target type.
     *
     * <p>Performs a full table scan of the specified column family and automatically converts each
     * Result to the target type. This method accepts the family name as a byte array for efficiency.
     * The returned stream must be closed after use.</p>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table to scan
     * @param family the column family name as a byte array
     * @param targetClass the class to convert each result to
     * @return a ContinuableFuture containing a Stream of converted objects
     * @see Scan
     */
    public <T> ContinuableFuture<Stream<T>> scan(final String tableName, final byte[] family, final Class<T> targetClass) {
        return asyncExecutor.execute(() -> hbaseExecutor.scan(tableName, family, targetClass));
    }

    /**
     * Asynchronously scans the table for a specific column and converts results to the target type.
     *
     * <p>Performs a full table scan retrieving only the specified column (family:qualifier) and
     * automatically converts each Result to the target type. This method accepts both family and
     * qualifier as byte arrays for efficiency. The returned stream must be closed after use.</p>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table to scan
     * @param family the column family name as a byte array
     * @param qualifier the column qualifier name as a byte array
     * @param targetClass the class to convert each result to
     * @return a ContinuableFuture containing a Stream of converted objects
     * @see Scan
     */
    public <T> ContinuableFuture<Stream<T>> scan(final String tableName, final byte[] family, final byte[] qualifier, final Class<T> targetClass) {
        return asyncExecutor.execute(() -> hbaseExecutor.scan(tableName, family, qualifier, targetClass));
    }

    /**
     * Asynchronously scans the table using AnyScan and converts results to the target type.
     *
     * <p>Performs a table scan with the criteria specified in the AnyScan builder and automatically
     * converts each Result to the target type. This combines the flexibility of AnyScan configuration
     * with automatic object mapping. The returned stream must be closed after use.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.scan("users", AnyScan.of().setStartRow("user1").setLimit(50), User.class)
     *      .thenRunAsync(stream -> stream.forEach(user -> processUser(user)));
     * }</pre>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table to scan
     * @param anyScan the AnyScan builder specifying scan criteria
     * @param targetClass the class to convert each result to
     * @return a ContinuableFuture containing a Stream of converted objects
     * @see AnyScan
     */
    public <T> ContinuableFuture<Stream<T>> scan(final String tableName, final AnyScan anyScan, final Class<T> targetClass) {
        return asyncExecutor.execute(() -> hbaseExecutor.scan(tableName, anyScan, targetClass));
    }

    /**
     * Asynchronously scans the table using a native Scan object and converts results to the target type.
     *
     * <p>Performs a table scan with the criteria specified in the HBase Scan object and automatically
     * converts each Result to the target type. This provides full access to HBase scan capabilities
     * with automatic object mapping. The returned stream must be closed after use.</p>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table to scan
     * @param scan the HBase Scan object specifying scan criteria
     * @param targetClass the class to convert each result to
     * @return a ContinuableFuture containing a Stream of converted objects
     * @see Scan
     */
    public <T> ContinuableFuture<Stream<T>> scan(final String tableName, final Scan scan, final Class<T> targetClass) {
        return asyncExecutor.execute(() -> hbaseExecutor.scan(tableName, scan, targetClass));
    }

    /**
     * Asynchronously inserts or updates a single row in the specified HBase table.
     *
     * <p>Performs a put operation that stores cells from the Put object into HBase. If the row
     * already exists, this operation updates the specified cells. The ContinuableFuture completes
     * with {@code null} when the operation finishes successfully.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Put put = new Put(Bytes.toBytes("user123"))
     *     .addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("John"));
     * async.put("users", put).thenRunAsync(() -> System.out.println("Put complete"));
     * }</pre>
     *
     * @param tableName the name of the HBase table to put data into
     * @param put the Put operation containing the row key and cells to store
     * @return a ContinuableFuture that completes with {@code null} when the put operation finishes
     * @see Put
     */
    public ContinuableFuture<Void> put(final String tableName, final Put put) {
        return asyncExecutor.execute(() -> {
            hbaseExecutor.put(tableName, put);

            return null;
        });
    }

    /**
     * Asynchronously inserts or updates multiple rows in the specified HBase table.
     *
     * <p>Performs batch put operations to efficiently store multiple rows in a single operation.
     * This is optimized for high throughput when inserting or updating many rows. The
     * ContinuableFuture completes with {@code null} when all put operations finish successfully.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Put> puts = Arrays.asList(
     *     new Put(Bytes.toBytes("user1")).addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("Alice")),
     *     new Put(Bytes.toBytes("user2")).addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("Bob"))
     * );
     * async.put("users", puts);
     * }</pre>
     *
     * @param tableName the name of the HBase table to put data into
     * @param puts the list of Put operations to execute
     * @return a ContinuableFuture that completes with {@code null} when all put operations finish
     * @see Put
     */
    public ContinuableFuture<Void> put(final String tableName, final List<Put> puts) {
        return asyncExecutor.execute(() -> {
            hbaseExecutor.put(tableName, puts);

            return null;
        });
    }

    /**
     * Asynchronously inserts or updates a single row using a fluent AnyPut builder.
     *
     * <p>Performs a put operation using the AnyPut builder which provides a fluent API for
     * constructing put operations. AnyPut simplifies the creation of Put objects with a more
     * readable syntax. The ContinuableFuture completes with {@code null} when the operation finishes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.put("users", AnyPut.of("user123").addColumn("info", "name", "John"))
     *      .thenRunAsync(() -> System.out.println("User saved"));
     * }</pre>
     *
     * @param tableName the name of the HBase table to put data into
     * @param anyPut the AnyPut builder specifying the row and cells to store
     * @return a ContinuableFuture that completes with {@code null} when the put operation finishes
     * @see AnyPut
     */
    public ContinuableFuture<Void> put(final String tableName, final AnyPut anyPut) {
        return asyncExecutor.execute(() -> {
            hbaseExecutor.put(tableName, anyPut);

            return null;
        });
    }

    /**
     * Asynchronously inserts or updates multiple rows using AnyPut builders.
     *
     * <p>Performs batch put operations using a collection of AnyPut builders. This combines
     * the efficiency of batch operations with the convenience of the fluent AnyPut API. The
     * ContinuableFuture completes with {@code null} when all put operations finish successfully.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<AnyPut> puts = Arrays.asList(
     *     AnyPut.of("user1").addColumn("info", "name", "Alice"),
     *     AnyPut.of("user2").addColumn("info", "name", "Bob")
     * );
     * async.put("users", puts);
     * }</pre>
     *
     * @param tableName the name of the HBase table to put data into
     * @param anyPuts the collection of AnyPut builders specifying the rows to store
     * @return a ContinuableFuture that completes with {@code null} when all put operations finish
     * @see AnyPut
     */
    public ContinuableFuture<Void> put(final String tableName, final Collection<AnyPut> anyPuts) {
        return asyncExecutor.execute(() -> {
            hbaseExecutor.put(tableName, anyPuts);

            return null;
        });
    }

    /**
     * Asynchronously deletes a row or specific cells from the specified HBase table.
     *
     * <p>Performs a delete operation that removes the row or specific cells identified by the
     * Delete object. The delete can target an entire row, specific column families, or individual
     * cells. The ContinuableFuture completes with {@code null} when the operation finishes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Delete delete = new Delete(Bytes.toBytes("user123"));
     * async.delete("users", delete).thenRunAsync(() -> System.out.println("Delete complete"));
     * }</pre>
     *
     * @param tableName the name of the HBase table to delete from
     * @param delete the Delete operation specifying the row and cells to delete
     * @return a ContinuableFuture that completes with {@code null} when the delete operation finishes
     * @see Delete
     */
    public ContinuableFuture<Void> delete(final String tableName, final Delete delete) {
        return asyncExecutor.execute(() -> {
            hbaseExecutor.delete(tableName, delete);

            return null;
        });
    }

    /**
     * Asynchronously deletes multiple rows or cells from the specified HBase table.
     *
     * <p>Performs batch delete operations to efficiently remove multiple rows or cells in a single
     * operation. This is optimized for high throughput when deleting many rows. The ContinuableFuture
     * completes with {@code null} when all delete operations finish successfully.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Delete> deletes = Arrays.asList(
     *     new Delete(Bytes.toBytes("user1")),
     *     new Delete(Bytes.toBytes("user2"))
     * );
     * async.delete("users", deletes);
     * }</pre>
     *
     * @param tableName the name of the HBase table to delete from
     * @param deletes the list of Delete operations to execute
     * @return a ContinuableFuture that completes with {@code null} when all delete operations finish
     * @see Delete
     */
    public ContinuableFuture<Void> delete(final String tableName, final List<Delete> deletes) {
        return asyncExecutor.execute(() -> {
            hbaseExecutor.delete(tableName, deletes);

            return null;
        });
    }

    /**
     * Asynchronously deletes a row or specific cells using a fluent AnyDelete builder.
     *
     * <p>Performs a delete operation using the AnyDelete builder which provides a fluent API for
     * constructing delete operations. AnyDelete simplifies the creation of Delete objects with
     * a more readable syntax. The ContinuableFuture completes with {@code null} when the operation finishes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.delete("users", AnyDelete.of("user123"))
     *      .thenRunAsync(() -> System.out.println("User deleted"));
     * }</pre>
     *
     * @param tableName the name of the HBase table to delete from
     * @param anyDelete the AnyDelete builder specifying the row and cells to delete
     * @return a ContinuableFuture that completes with {@code null} when the delete operation finishes
     * @see AnyDelete
     */
    public ContinuableFuture<Void> delete(final String tableName, final AnyDelete anyDelete) {
        return asyncExecutor.execute(() -> {
            hbaseExecutor.delete(tableName, anyDelete);

            return null;
        });
    }

    /**
     * Asynchronously deletes multiple rows or cells using AnyDelete builders.
     *
     * <p>Performs batch delete operations using a collection of AnyDelete builders. This combines
     * the efficiency of batch operations with the convenience of the fluent AnyDelete API. The
     * ContinuableFuture completes with {@code null} when all delete operations finish successfully.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Collection<AnyDelete> deletes = Arrays.asList(
     *     AnyDelete.of("user1"),
     *     AnyDelete.of("user2").addColumn("info", "email")
     * );
     * async.delete("users", deletes);
     * }</pre>
     *
     * @param tableName the name of the HBase table to delete from
     * @param anyDeletes the collection of AnyDelete builders specifying the rows to delete
     * @return a ContinuableFuture that completes with {@code null} when all delete operations finish
     * @see AnyDelete
     */
    public ContinuableFuture<Void> delete(final String tableName, final Collection<AnyDelete> anyDeletes) {
        return asyncExecutor.execute(() -> {
            hbaseExecutor.delete(tableName, anyDeletes);

            return null;
        });
    }

    /**
     * Asynchronously performs atomic mutations on a single row using a fluent builder.
     *
     * <p>Executes multiple mutations (puts and/or deletes) on a single row atomically. All mutations
     * either succeed together or fail together, ensuring data consistency. This is useful for
     * implementing complex update logic that requires atomic guarantees. The ContinuableFuture
     * completes with {@code null} when the operation finishes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyRowMutations rm = AnyRowMutations.of("user123")
     *     .add(AnyPut.of("user123").addColumn("info", "name", "John").val())
     *     .add(AnyDelete.of("user123").addColumn("info", "oldEmail").val());
     * async.mutateRow("users", rm);
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param rm the AnyRowMutations builder containing the atomic mutations
     * @return a ContinuableFuture that completes with {@code null} when the mutations finish
     * @see AnyRowMutations
     */
    public ContinuableFuture<Void> mutateRow(final String tableName, final AnyRowMutations rm) {
        return asyncExecutor.execute(() -> {
            hbaseExecutor.mutateRow(tableName, rm);

            return null;
        });
    }

    /**
     * Asynchronously performs atomic mutations on a single row using a native RowMutations object.
     *
     * <p>Executes multiple mutations (puts and/or deletes) on a single row atomically using the
     * HBase native RowMutations API. All mutations either succeed together or fail together,
     * ensuring data consistency. The ContinuableFuture completes with {@code null} when the
     * operation finishes.</p>
     *
     * @param tableName the name of the HBase table
     * @param rm the RowMutations object containing the atomic mutations
     * @return a ContinuableFuture that completes with {@code null} when the mutations finish
     * @see RowMutations
     */
    public ContinuableFuture<Void> mutateRow(final String tableName, final RowMutations rm) {
        return asyncExecutor.execute(() -> {
            hbaseExecutor.mutateRow(tableName, rm);

            return null;
        });
    }

    /**
     * Asynchronously appends data to one or more columns in a row using a fluent builder.
     *
     * <p>Atomically appends values to the end of existing cell values. This is useful for
     * maintaining counters or logs. If the cell doesn't exist, it's created with the appended
     * value. The ContinuableFuture completes with a Result containing the new cell values after
     * the append operation.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.append("users", AnyAppend.of("user123").addColumn("info", "log", "Entry1"))
     *      .thenRunAsync(result -> System.out.println("New value: " + result));
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param append the AnyAppend builder specifying the row and values to append
     * @return a ContinuableFuture containing the Result with new cell values after the append
     * @see AnyAppend
     * @see Result
     */
    public ContinuableFuture<Result> append(final String tableName, final AnyAppend append) {
        return asyncExecutor.execute(() -> hbaseExecutor.append(tableName, append));
    }

    /**
     * Asynchronously appends data to one or more columns in a row using a native Append object.
     *
     * <p>Atomically appends values to the end of existing cell values using the HBase native
     * Append API. If the cell doesn't exist, it's created with the appended value. The
     * ContinuableFuture completes with a Result containing the new cell values after the append.</p>
     *
     * @param tableName the name of the HBase table
     * @param append the Append object specifying the row and values to append
     * @return a ContinuableFuture containing the Result with new cell values after the append
     * @see Append
     * @see Result
     */
    public ContinuableFuture<Result> append(final String tableName, final Append append) {
        return asyncExecutor.execute(() -> hbaseExecutor.append(tableName, append));
    }

    /**
     * Asynchronously increments column values in a row using a fluent builder.
     *
     * <p>Atomically increments one or more column values by the specified amounts. This is ideal
     * for maintaining counters without race conditions. If the cell doesn't exist, it's initialized
     * to the increment value. The ContinuableFuture completes with a Result containing the new
     * cell values after the increment.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.increment("users", AnyIncrement.of("user123").addColumn("stats", "loginCount", 1))
     *      .thenRunAsync(result -> System.out.println("New count: " + Bytes.toLong(result.getValue(...))));
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param increment the AnyIncrement builder specifying the row and increment amounts
     * @return a ContinuableFuture containing the Result with new cell values after the increment
     * @see AnyIncrement
     * @see Result
     */
    public ContinuableFuture<Result> increment(final String tableName, final AnyIncrement increment) {
        return asyncExecutor.execute(() -> hbaseExecutor.increment(tableName, increment));
    }

    /**
     * Asynchronously increments column values in a row using a native Increment object.
     *
     * <p>Atomically increments one or more column values by the specified amounts using the HBase
     * native Increment API. If the cell doesn't exist, it's initialized to the increment value.
     * The ContinuableFuture completes with a Result containing the new cell values after the increment.</p>
     *
     * @param tableName the name of the HBase table
     * @param increment the Increment object specifying the row and increment amounts
     * @return a ContinuableFuture containing the Result with new cell values after the increment
     * @see Increment
     * @see Result
     */
    public ContinuableFuture<Result> increment(final String tableName, final Increment increment) {
        return asyncExecutor.execute(() -> hbaseExecutor.increment(tableName, increment));
    }

    /**
     * Asynchronously increments a single column value by a specified amount.
     *
     * <p>Atomically increments a specific column's long value by the given amount. This is a
     * convenience method for incrementing a single counter without creating an Increment object.
     * If the cell doesn't exist, it's initialized to the amount value. The ContinuableFuture
     * completes with the new value after the increment.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * async.incrementColumnValue("users", "user123", "stats", "loginCount", 1)
     *      .thenRunAsync(newCount -> System.out.println("Login count: " + newCount));
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param rowKey the row key (will be converted to bytes)
     * @param family the column family name (as String)
     * @param qualifier the column qualifier name (as String)
     * @param amount the amount to increment by (can be negative for decrement)
     * @return a ContinuableFuture containing the new value after the increment
     */
    public ContinuableFuture<Long> incrementColumnValue(final String tableName, final Object rowKey, final String family, final String qualifier,
            final long amount) {
        return asyncExecutor.execute(() -> hbaseExecutor.incrementColumnValue(tableName, rowKey, family, qualifier, amount));
    }

    /**
     * Asynchronously increments a single column value with specified durability.
     *
     * <p>Atomically increments a specific column's long value by the given amount with control
     * over write durability. The durability setting determines how data is persisted (e.g.,
     * SYNC_WAL, ASYNC_WAL, SKIP_WAL). The ContinuableFuture completes with the new value.</p>
     *
     * @param tableName the name of the HBase table
     * @param rowKey the row key (will be converted to bytes)
     * @param family the column family name (as String)
     * @param qualifier the column qualifier name (as String)
     * @param amount the amount to increment by (can be negative for decrement)
     * @param durability the durability level for this operation
     * @return a ContinuableFuture containing the new value after the increment
     * @see Durability
     */
    public ContinuableFuture<Long> incrementColumnValue(final String tableName, final Object rowKey, final String family, final String qualifier,
            final long amount, final Durability durability) {
        return asyncExecutor.execute(() -> hbaseExecutor.incrementColumnValue(tableName, rowKey, family, qualifier, amount, durability));
    }

    /**
     * Asynchronously increments a single column value by a specified amount.
     *
     * <p>Atomically increments a specific column's long value by the given amount. This method
     * accepts family and qualifier as byte arrays for efficiency. If the cell doesn't exist,
     * it's initialized to the amount value. The ContinuableFuture completes with the new value.</p>
     *
     * @param tableName the name of the HBase table
     * @param rowKey the row key (will be converted to bytes)
     * @param family the column family name as a byte array
     * @param qualifier the column qualifier name as a byte array
     * @param amount the amount to increment by (can be negative for decrement)
     * @return a ContinuableFuture containing the new value after the increment
     */
    public ContinuableFuture<Long> incrementColumnValue(final String tableName, final Object rowKey, final byte[] family, final byte[] qualifier,
            final long amount) {
        return asyncExecutor.execute(() -> hbaseExecutor.incrementColumnValue(tableName, rowKey, family, qualifier, amount));
    }

    /**
     * Asynchronously increments a single column value with specified durability.
     *
     * <p>Atomically increments a specific column's long value by the given amount with control
     * over write durability. This method accepts family and qualifier as byte arrays for efficiency.
     * The ContinuableFuture completes with the new value after the increment.</p>
     *
     * @param tableName the name of the HBase table
     * @param rowKey the row key (will be converted to bytes)
     * @param family the column family name as a byte array
     * @param qualifier the column qualifier name as a byte array
     * @param amount the amount to increment by (can be negative for decrement)
     * @param durability the durability level for this operation
     * @return a ContinuableFuture containing the new value after the increment
     * @see Durability
     */
    public ContinuableFuture<Long> incrementColumnValue(final String tableName, final Object rowKey, final byte[] family, final byte[] qualifier,
            final long amount, final Durability durability) {
        return asyncExecutor.execute(() -> hbaseExecutor.incrementColumnValue(tableName, rowKey, family, qualifier, amount, durability));
    }

    /**
     * Asynchronously creates an RPC channel to a coprocessor for the specified row.
     *
     * <p>Returns a coprocessor RPC channel that can be used to execute custom server-side logic
     * on the region server hosting the specified row. This enables extending HBase functionality
     * with custom operations that run close to the data. The ContinuableFuture completes with
     * the channel that can be used for coprocessor method invocation.</p>
     *
     * @param tableName the name of the HBase table
     * @param rowKey the row key to determine which region server to connect to
     * @return a ContinuableFuture containing the CoprocessorRpcChannel for the row's region
     * @see CoprocessorRpcChannel
     */
    public ContinuableFuture<CoprocessorRpcChannel> coprocessorService(final String tableName, final Object rowKey) {
        return asyncExecutor.execute(() -> hbaseExecutor.coprocessorService(tableName, rowKey));
    }

    /**
     * Asynchronously executes a coprocessor call across a range of rows.
     *
     * <p>Invokes a coprocessor service method across all regions that overlap with the specified
     * row range. The callable is executed on each region server, and results are collected in a
     * map keyed by region start key. This enables distributed processing across multiple regions.
     * The ContinuableFuture completes with a map of results from each region.</p>
     *
     * @param <T> the coprocessor service type
     * @param <R> the return type from the coprocessor call
     * @param tableName the name of the HBase table
     * @param service the coprocessor service class
     * @param startRowKey the starting row key (inclusive) for the range
     * @param endRowKey the ending row key (exclusive) for the range
     * @param callable the callable to execute on each region
     * @return a ContinuableFuture containing a map of region start keys to results
     * @see Batch.Call
     */
    public <T extends Service, R> ContinuableFuture<Map<byte[], R>> coprocessorService(final String tableName, final Class<T> service, final Object startRowKey,
            final Object endRowKey, final Batch.Call<T, R> callable) {
        return asyncExecutor.execute(() -> hbaseExecutor.coprocessorService(tableName, service, startRowKey, endRowKey, callable));
    }

    /**
     * Asynchronously executes a coprocessor call across a range of rows with a callback.
     *
     * <p>Invokes a coprocessor service method across all regions that overlap with the specified
     * row range, with results delivered via the provided callback as they become available. This
     * enables streaming processing of results rather than collecting them all in memory. The
     * callback is invoked for each region's result. The ContinuableFuture completes with {@code null}
     * when all coprocessor calls finish.</p>
     *
     * @param <T> the coprocessor service type
     * @param <R> the return type from the coprocessor call
     * @param tableName the name of the HBase table
     * @param service the coprocessor service class
     * @param startRowKey the starting row key (inclusive) for the range
     * @param endRowKey the ending row key (exclusive) for the range
     * @param callable the callable to execute on each region
     * @param callback the callback to receive results from each region
     * @return a ContinuableFuture that completes with {@code null} when all calls finish
     * @see Batch.Call
     * @see Batch.Callback
     */
    public <T extends Service, R> ContinuableFuture<Void> coprocessorService(final String tableName, final Class<T> service, final Object startRowKey,
            final Object endRowKey, final Batch.Call<T, R> callable, final Batch.Callback<R> callback) {
        return asyncExecutor.execute(() -> {
            hbaseExecutor.coprocessorService(tableName, service, startRowKey, endRowKey, callable, callback);

            return null;
        });
    }

    /**
     * Asynchronously executes a batch coprocessor call across a range of rows.
     *
     * <p>Invokes a coprocessor method defined by the method descriptor across all regions that
     * overlap with the specified row range using Protocol Buffer messages. This is useful for
     * executing predefined coprocessor operations in batch mode. The ContinuableFuture completes
     * with a map of region start keys to response messages.</p>
     *
     * @param <R> the response message type
     * @param tableName the name of the HBase table
     * @param methodDescriptor the Protocol Buffer method descriptor for the coprocessor method
     * @param request the request message to send to each region
     * @param startRowKey the starting row key (inclusive) for the range
     * @param endRowKey the ending row key (exclusive) for the range
     * @param responsePrototype the prototype instance for parsing responses
     * @return a ContinuableFuture containing a map of region start keys to response messages
     * @see Descriptors.MethodDescriptor
     */
    public <R extends Message> ContinuableFuture<Map<byte[], R>> batchCoprocessorService(final String tableName,
            final Descriptors.MethodDescriptor methodDescriptor, final Message request, final Object startRowKey, final Object endRowKey,
            final R responsePrototype) {
        return asyncExecutor
                .execute(() -> hbaseExecutor.batchCoprocessorService(tableName, methodDescriptor, request, startRowKey, endRowKey, responsePrototype));
    }

    /**
     * Asynchronously executes a batch coprocessor call across a range of rows with a callback.
     *
     * <p>Invokes a coprocessor method defined by the method descriptor across all regions that
     * overlap with the specified row range using Protocol Buffer messages, with results delivered
     * via the provided callback as they become available. This enables streaming processing of
     * results. The callback is invoked for each region's response. The ContinuableFuture completes
     * with {@code null} when all coprocessor calls finish.</p>
     *
     * @param <R> the response message type
     * @param tableName the name of the HBase table
     * @param methodDescriptor the Protocol Buffer method descriptor for the coprocessor method
     * @param request the request message to send to each region
     * @param startRowKey the starting row key (inclusive) for the range
     * @param endRowKey the ending row key (exclusive) for the range
     * @param responsePrototype the prototype instance for parsing responses
     * @param callback the callback to receive responses from each region
     * @return a ContinuableFuture that completes with {@code null} when all calls finish
     * @see Descriptors.MethodDescriptor
     * @see Batch.Callback
     */
    public <R extends Message> ContinuableFuture<Void> batchCoprocessorService(final String tableName, final Descriptors.MethodDescriptor methodDescriptor,
            final Message request, final Object startRowKey, final Object endRowKey, final R responsePrototype, final Batch.Callback<R> callback) {
        return asyncExecutor.execute(() -> {
            hbaseExecutor.batchCoprocessorService(tableName, methodDescriptor, request, startRowKey, endRowKey, responsePrototype, callback);

            return null;
        });
    }
}
