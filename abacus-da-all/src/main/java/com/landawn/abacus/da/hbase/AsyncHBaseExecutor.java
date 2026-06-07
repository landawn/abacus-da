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
 * This executor returns {@link ContinuableFuture} instances for all operations, enabling high-performance,
 * concurrent access patterns and reactive programming models.
 *
 * <p>All methods in this class are asynchronous counterparts to the synchronous methods in {@link HBaseExecutor}.
 * Each method submits a task that invokes the corresponding synchronous method on the {@link AsyncExecutor}
 * supplied at construction time (or, when none is supplied, on {@link HBaseExecutor#DEFAULT_ASYNC_EXECUTOR},
 * a thread pool sized to {@code max(64, CPU_CORES * 8)} core threads and {@code max(128, CPU_CORES * 16)}
 * maximum threads, with a 180-second keep-alive). Every method in this class uses that same executor,
 * so the threading model is uniform across operations.</p>
 *
 * <p><b>Ordering guarantees:</b> tasks are submitted to the executor in the order calls are made,
 * but completion order depends on the executor's scheduling and on how long each individual HBase
 * operation takes; concurrent submissions are not serialised. Each returned {@code ContinuableFuture}
 * completes when its single underlying synchronous call returns. A synchronous transform created via
 * {@link ContinuableFuture#map(com.landawn.abacus.util.Throwables.Function)} runs on the completing
 * thread, whereas the {@code Async} continuations ({@link ContinuableFuture#thenRunAsync},
 * {@link ContinuableFuture#thenCallAsync}, etc.) are dispatched back to the same {@code AsyncExecutor}.</p>
 *
 * <h2>Key Features</h2>
 * <ul>
 * <li><strong>Non-blocking Operations</strong>: All methods return immediately with a {@code ContinuableFuture}</li>
 * <li><strong>Thread Pool Management</strong>: A single, configurable {@link AsyncExecutor} backs every method</li>
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
 * ContinuableFuture<User> userFuture = async.get("users", AnyGet.of("user123"), User.class);
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
 *     async.scan("users", AnyScan.create().withStartRow("user1").withStopRow("user2"));
 *
 * // Chain async operations: each step is dispatched back to the same AsyncExecutor.
 * async.get("users", AnyGet.of("user123"), User.class)
 *      .thenCallAsync(user -> { user.setLastLogin(new Date()); return user; })
 *      .thenRunAsync(user -> async.sync().put("users", AnyPut.create(user)))
 *      .thenRunAsync(() -> System.out.println("User updated"));
 * }</pre>
 *
 * <h3>Performance Considerations:</h3>
 * <ul>
 * <li><strong>Thread Pool Sizing</strong>: Default pool size scales with CPU cores (8-16x)</li>
 * <li><strong>Connection Sharing</strong>: Shares the underlying HBase {@code Connection} with the wrapped {@link HBaseExecutor}</li>
 * <li><strong>Memory Management</strong>: Streams returned by {@code scan} are lazy and own the underlying HBase {@code ResultScanner}; close them after use</li>
 * <li><strong>Error Handling</strong>: Exceptions thrown by the underlying call are propagated through the returned {@code ContinuableFuture}</li>
 * </ul>
 *
 * @see HBaseExecutor
 * @see HBaseExecutor#async()
 * @see ContinuableFuture
 * @see AsyncExecutor
 * @see <a href="http://hbase.apache.org/devapidocs/index.html">Apache HBase Java API Documentation</a>
 * @see org.apache.hadoop.hbase.client.Table
 */
public final class AsyncHBaseExecutor {

    private final HBaseExecutor hbaseExecutor;

    private final AsyncExecutor asyncExecutor;

    /**
     * Package-private constructor. Instances are created by {@link HBaseExecutor} and obtained via
     * {@link HBaseExecutor#async()} rather than constructed directly.
     *
     * @param hbaseExecutor the synchronous executor this wrapper delegates to; must not be null
     * @param asyncExecutor the thread pool on which every operation in this wrapper is submitted;
     *                      must not be null
     */
    AsyncHBaseExecutor(final HBaseExecutor hbaseExecutor, final AsyncExecutor asyncExecutor) {
        this.hbaseExecutor = hbaseExecutor;
        this.asyncExecutor = asyncExecutor;
    }

    /**
     * Returns the underlying synchronous HBase executor that this async wrapper delegates to.
     *
     * <p>Every async method in this class submits a task that invokes the corresponding method on
     * the returned instance. Use this when you need to perform blocking operations directly or when
     * integrating with synchronous code paths.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     * HBaseExecutor sync = async.sync();             // returns the wrapped synchronous executor
     * boolean same = (sync == hbaseExecutor);        // returns true: it is the same instance
     *
     * // The returned reference is stable across calls:
     * boolean stable = (async.sync() == async.sync()); // returns true
     * }</pre>
     *
     * @return the wrapped {@link HBaseExecutor} instance; never null
     * @see HBaseExecutor
     */
    public HBaseExecutor sync() {
        return hbaseExecutor;
    }

    /**
     * Asynchronously checks if a row exists in the specified HBase table.
     *
     * <p>This is a server-side operation that checks for row existence without transferring
     * any actual cell value to the client, making it efficient for existence checks.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Typical: check an existing row
     * Boolean present = async.exists("users", new Get(Bytes.toBytes("user123"))).get(); // returns Boolean.TRUE if any cell matches
     *
     * // Typical: chain a follow-up action only when present
     * async.exists("users", new Get(Bytes.toBytes("user123")))
     *      .thenRunAsync(exists -> { if (exists) load("user123"); }); // returns ContinuableFuture<Void>
     *
     * // Edge: a row that does not exist yields false
     * Boolean missing = async.exists("users", new Get(Bytes.toBytes("nope"))).get(); // returns Boolean.FALSE
     *
     * // Negative: any exception raised by the underlying synchronous call is re-thrown,
     * // wrapped in an ExecutionException, when the future is resolved via get().
     * async.exists("badTable", new Get(Bytes.toBytes("k"))).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param tableName the name of the HBase table to check
     * @param get the Get operation specifying the row (and optionally column filters) to check for existence
     * @return a {@link ContinuableFuture} that completes with {@code true} if the Get matches one or
     *         more cells, {@code false} otherwise. Wraps {@link HBaseExecutor#exists(String, Get)}.
     * @see HBaseExecutor#exists(String, Get)
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
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Typical: batch existence check; result order matches input order
     * List<Get> gets = Arrays.asList(new Get(Bytes.toBytes("a")), new Get(Bytes.toBytes("b")));
     * List<Boolean> flags = async.exists("users", gets).get(); // returns e.g. [true, false]; flags.size() == 2
     *
     * // Typical: react to the batch result asynchronously
     * async.exists("users", gets)
     *      .thenRunAsync(results -> results.forEach(System.out::println)); // returns ContinuableFuture<Void>
     *
     * // Edge: an empty input list yields an empty result list
     * List<Boolean> none = async.exists("users", Collections.<Get>emptyList()).get(); // returns an empty list
     *
     * // Negative: any exception from the underlying call surfaces wrapped in ExecutionException
     * async.exists("badTable", gets).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param tableName the name of the HBase table to check
     * @param gets the list of Get operations specifying the rows to check
     * @return a ContinuableFuture whose value is a {@code List<Boolean>} in the same order as
     *         {@code gets}; the i-th entry is {@code true} if the i-th Get would match one or
     *         more cells, {@code false} otherwise. Wraps {@link HBaseExecutor#exists(String, List)}.
     * @see HBaseExecutor#exists(String, List)
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
     * Asynchronously checks if a row exists using an {@link AnyGet} operation builder.
     *
     * <p>Equivalent to {@link #exists(String, Get)} but using the fluent {@link AnyGet} wrapper,
     * which allows row keys, column families, and qualifiers to be specified without manual
     * byte-array conversion.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Typical: row-level existence check
     * Boolean present = async.exists("users", AnyGet.of("user123")).get(); // returns Boolean.TRUE if the row has any cell
     *
     * // Typical: narrow the check to a single column
     * Boolean hasEmail = async.exists("users", AnyGet.of("user123").addColumn("info", "email")).get(); // returns Boolean.TRUE only if that column exists
     *
     * // Edge: a non-existent row yields false
     * Boolean missing = async.exists("users", AnyGet.of("nope")).get(); // returns Boolean.FALSE
     *
     * // Negative: exceptions from the underlying call surface wrapped in ExecutionException
     * async.exists("badTable", AnyGet.of("k")).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param tableName the name of the HBase table to check
     * @param anyGet the AnyGet operation builder specifying the row to check
     * @return a {@link ContinuableFuture} that completes with {@code true} if the row exists,
     *         {@code false} otherwise. Wraps {@link HBaseExecutor#exists(String, AnyGet)}.
     * @see HBaseExecutor#exists(String, AnyGet)
     * @see AnyGet
     */
    public ContinuableFuture<Boolean> exists(final String tableName, final AnyGet anyGet) {
        return asyncExecutor.execute(() -> hbaseExecutor.exists(tableName, anyGet));
    }

    /**
     * Asynchronously checks the existence of multiple rows using {@link AnyGet} operation builders.
     *
     * <p>Performs batch existence checks using a collection of AnyGet builders. The returned list
     * preserves the iteration order of {@code anyGets} so that the i-th entry corresponds to the
     * i-th AnyGet.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Typical: batch existence check; result order matches iteration order
     * List<AnyGet> gets = Arrays.asList(AnyGet.of("a"), AnyGet.of("b"));
     * List<Boolean> flags = async.exists("users", gets).get(); // returns e.g. [true, false]; flags.size() == 2
     *
     * // Typical: act on the result asynchronously
     * async.exists("users", gets)
     *      .thenRunAsync(results -> System.out.println(results)); // returns ContinuableFuture<Void>
     *
     * // Edge: an empty collection yields an empty result list
     * List<Boolean> none = async.exists("users", Collections.<AnyGet>emptyList()).get(); // returns an empty list
     *
     * // Negative: exceptions from the underlying call surface wrapped in ExecutionException
     * async.exists("badTable", gets).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param tableName the name of the HBase table to check
     * @param anyGets the collection of AnyGet builders specifying the rows to check
     * @return a {@link ContinuableFuture} whose value is a {@code List<Boolean>} in the iteration
     *         order of {@code anyGets}; each entry is {@code true} if the corresponding AnyGet
     *         matches one or more cells, {@code false} otherwise. Wraps
     *         {@link HBaseExecutor#exists(String, Collection)}.
     * @see HBaseExecutor#exists(String, Collection)
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
     * <p>Performs a get operation to retrieve data from a specific row. The returned {@link Result}
     * contains all cells that match the Get operation's criteria (column family and qualifier
     * specifications, time ranges, and version limits). If the row does not exist, the Result will
     * be {@linkplain Result#isEmpty() empty}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Typical: fetch a row and read a cell
     * Result row = async.get("users", new Get(Bytes.toBytes("user123"))).get(); // returns the row's Result (empty if absent)
     * byte[] name = row.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"));
     *
     * // Typical: process the Result asynchronously
     * async.get("users", new Get(Bytes.toBytes("user123")))
     *      .thenRunAsync(r -> { if (!r.isEmpty()) handle(r); }); // returns ContinuableFuture<Void>
     *
     * // Edge: a missing row resolves to an empty Result, not null
     * Result missing = async.get("users", new Get(Bytes.toBytes("nope"))).get(); // returns a Result where isEmpty() == true
     *
     * // Negative: exceptions from the underlying call surface wrapped in ExecutionException
     * async.get("badTable", new Get(Bytes.toBytes("k"))).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param tableName the name of the HBase table to retrieve from
     * @param get the Get operation specifying the row and columns to retrieve
     * @return a {@link ContinuableFuture} containing the Result object with the retrieved data
     *         (possibly empty). Wraps {@link HBaseExecutor#get(String, Get)}.
     * @see HBaseExecutor#get(String, Get)
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
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Typical: batch fetch; result list is parallel to the input list
     * List<Get> gets = Arrays.asList(new Get(Bytes.toBytes("a")), new Get(Bytes.toBytes("b")));
     * List<Result> rows = async.get("users", gets).get(); // returns a list with rows.size() == 2
     *
     * // Edge: missing rows are present as empty Results (positions preserved, none dropped)
     * boolean secondMissing = rows.get(1).isEmpty(); // returns true when row "b" does not exist
     *
     * // Edge: an empty input list yields an empty result list
     * List<Result> none = async.get("users", Collections.<Get>emptyList()).get(); // returns an empty list
     *
     * // Negative: exceptions from the underlying call surface wrapped in ExecutionException
     * async.get("badTable", gets).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param tableName the name of the HBase table to retrieve from
     * @param gets the list of Get operations specifying the rows and columns to retrieve
     * @return a ContinuableFuture whose value is a {@code List<Result>} in the same order as
     *         {@code gets}; rows that do not exist are represented by {@linkplain Result#isEmpty() empty}
     *         Result entries. Wraps {@link HBaseExecutor#get(String, List)}.
     * @see HBaseExecutor#get(String, List)
     * @see Get
     * @see Result
     */
    public ContinuableFuture<List<Result>> get(final String tableName, final List<Get> gets) {
        return asyncExecutor.execute(() -> hbaseExecutor.get(tableName, gets));
    }

    /**
     * Asynchronously retrieves a single row using an {@link AnyGet} operation builder.
     *
     * <p>Equivalent to {@link #get(String, Get)} but using the fluent {@link AnyGet} wrapper. If
     * the row does not exist, the Result will be {@linkplain Result#isEmpty() empty}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Typical: fetch the whole row
     * Result row = async.get("users", AnyGet.of("user123")).get(); // returns the row's Result (empty if absent)
     *
     * // Typical: restrict to one column
     * Result emailOnly = async.get("users", AnyGet.of("user123").addColumn("info", "email")).get(); // returns a Result holding only info:email
     *
     * // Edge: a missing row resolves to an empty Result, not null
     * Result missing = async.get("users", AnyGet.of("nope")).get(); // returns a Result where isEmpty() == true
     *
     * // Negative: exceptions from the underlying call surface wrapped in ExecutionException
     * async.get("badTable", AnyGet.of("k")).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param tableName the name of the HBase table to retrieve from
     * @param anyGet the AnyGet operation builder specifying the row and columns
     * @return a {@link ContinuableFuture} containing the Result object with the retrieved data
     *         (possibly empty). Wraps {@link HBaseExecutor#get(String, AnyGet)}.
     * @see HBaseExecutor#get(String, AnyGet)
     * @see AnyGet
     */
    public ContinuableFuture<Result> get(final String tableName, final AnyGet anyGet) {
        return asyncExecutor.execute(() -> hbaseExecutor.get(tableName, anyGet));
    }

    /**
     * Asynchronously retrieves multiple rows using {@link AnyGet} operation builders.
     *
     * <p>The returned list has the same size and order as {@code anyGets}; rows that do not exist
     * are represented by {@linkplain Result#isEmpty() empty} Result entries.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Typical: batch fetch; result list is parallel to the input
     * List<AnyGet> gets = Arrays.asList(AnyGet.of("a"), AnyGet.of("b"));
     * List<Result> rows = async.get("users", gets).get(); // returns a list with rows.size() == 2
     *
     * // Edge: missing rows are present as empty Results (positions preserved)
     * boolean firstMissing = rows.get(0).isEmpty(); // returns true when row "a" does not exist
     *
     * // Edge: an empty collection yields an empty result list
     * List<Result> none = async.get("users", Collections.<AnyGet>emptyList()).get(); // returns an empty list
     *
     * // Negative: exceptions from the underlying call surface wrapped in ExecutionException
     * async.get("badTable", gets).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param tableName the name of the HBase table to retrieve from
     * @param anyGets the collection of AnyGet builders specifying the rows to retrieve
     * @return a {@link ContinuableFuture} containing a list of Result objects in the iteration
     *         order of {@code anyGets}. Wraps {@link HBaseExecutor#get(String, Collection)}.
     * @see HBaseExecutor#get(String, Collection)
     * @see AnyGet
     */
    public ContinuableFuture<List<Result>> get(final String tableName, final Collection<AnyGet> anyGets) {
        return asyncExecutor.execute(() -> hbaseExecutor.get(tableName, anyGets));
    }

    /**
     * Asynchronously retrieves a row and converts it to the specified target type.
     *
     * <p>The {@link Result} returned by HBase is converted to {@code T} using the entity-mapping
     * facilities of {@link HBaseExecutor}. If the row does not exist, the resulting object may be
     * {@code null} or an empty instance depending on the target type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Typical: fetch and map a row to an entity
     * User user = async.get("users", new Get(Bytes.toBytes("user123")), User.class).get(); // returns a mapped User
     *
     * // Typical: continue processing on the async pool
     * async.get("users", new Get(Bytes.toBytes("user123")), User.class)
     *      .thenRunAsync(u -> { if (u != null) cache(u); }); // returns ContinuableFuture<Void>
     *
     * // Edge: a missing row maps to null (for bean target types)
     * User missing = async.get("users", new Get(Bytes.toBytes("nope")), User.class).get(); // returns null
     *
     * // Negative: exceptions from the underlying call surface wrapped in ExecutionException
     * async.get("badTable", new Get(Bytes.toBytes("k")), User.class).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table to retrieve from
     * @param get the Get operation specifying the row to retrieve
     * @param targetClass the class to convert the result to
     * @return a {@link ContinuableFuture} containing the converted object. Wraps
     *         {@link HBaseExecutor#get(String, Get, Class)}.
     * @see HBaseExecutor#get(String, Get, Class)
     * @see Get
     */
    public <T> ContinuableFuture<T> get(final String tableName, final Get get, final Class<T> targetClass) {
        return asyncExecutor.execute(() -> hbaseExecutor.get(tableName, get, targetClass));
    }

    /**
     * Asynchronously retrieves multiple rows and converts them to the specified type.
     *
     * <p>Unlike {@link #get(String, List)}, rows that do not exist (empty Results) are
     * <i>skipped</i>, so the resulting list may contain fewer elements than {@code gets}
     * and does not necessarily correspond positionally to it.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Typical: batch fetch and map; only existing rows appear in the result
     * List<Get> gets = Arrays.asList(new Get(Bytes.toBytes("a")), new Get(Bytes.toBytes("b")));
     * List<User> users = async.get("users", gets, User.class).get(); // returns mapped Users; size <= gets.size()
     *
     * // Edge: if only "a" exists, the result holds a single element (missing rows dropped, not nulls)
     * // users.size() == 1 in that case
     *
     * // Edge: an empty input list yields an empty result list
     * List<User> none = async.get("users", Collections.<Get>emptyList(), User.class).get(); // returns an empty list
     *
     * // Negative: exceptions from the underlying call surface wrapped in ExecutionException
     * async.get("badTable", gets, User.class).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table to retrieve from
     * @param gets the list of Get operations specifying the rows to retrieve
     * @param targetClass the class to convert each non-empty result to
     * @return a ContinuableFuture whose value is a {@code List<T>} of converted objects,
     *         with empty/missing rows skipped. Wraps {@link HBaseExecutor#get(String, List, Class)}.
     * @see HBaseExecutor#get(String, List, Class)
     * @see Get
     */
    public <T> ContinuableFuture<List<T>> get(final String tableName, final List<Get> gets, final Class<T> targetClass) {
        return asyncExecutor.execute(() -> hbaseExecutor.get(tableName, gets, targetClass));
    }

    /**
     * Asynchronously retrieves a row using {@link AnyGet} and converts it to the specified target type.
     *
     * <p>Equivalent to {@link #get(String, Get, Class)} but using the fluent {@link AnyGet} wrapper.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Typical: fetch and map a single row to an entity
     * User user = async.get("users", AnyGet.of("user123"), User.class).get(); // returns a mapped User
     *
     * // Typical: continue on the async pool
     * async.get("users", AnyGet.of("user123"), User.class)
     *      .thenRunAsync(u -> { if (u != null) cache(u); }); // returns ContinuableFuture<Void>
     *
     * // Edge: a missing row maps to null (for bean target types)
     * User missing = async.get("users", AnyGet.of("nope"), User.class).get(); // returns null
     *
     * // Negative: exceptions from the underlying call surface wrapped in ExecutionException
     * async.get("badTable", AnyGet.of("k"), User.class).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table to retrieve from
     * @param anyGet the AnyGet operation builder specifying the row
     * @param targetClass the class to convert the result to
     * @return a {@link ContinuableFuture} containing the converted object. Wraps
     *         {@link HBaseExecutor#get(String, AnyGet, Class)}.
     * @see HBaseExecutor#get(String, AnyGet, Class)
     * @see AnyGet
     */
    public <T> ContinuableFuture<T> get(final String tableName, final AnyGet anyGet, final Class<T> targetClass) {
        return asyncExecutor.execute(() -> hbaseExecutor.get(tableName, anyGet, targetClass));
    }

    /**
     * Asynchronously retrieves multiple rows using AnyGet builders and converts them to the specified type.
     *
     * <p>Unlike {@link #get(String, Collection)}, rows that do not exist (empty Results) are
     * <i>skipped</i>, so the resulting list may contain fewer elements than {@code anyGets}
     * and does not necessarily correspond positionally to it.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Typical: batch fetch and map; only existing rows appear in the result
     * List<AnyGet> gets = Arrays.asList(AnyGet.of("a"), AnyGet.of("b"));
     * List<User> users = async.get("users", gets, User.class).get(); // returns mapped Users; size <= gets.size()
     *
     * // Edge: missing rows are dropped (not represented as null entries)
     * // if only "a" exists, users.size() == 1
     *
     * // Edge: an empty collection yields an empty result list
     * List<User> none = async.get("users", Collections.<AnyGet>emptyList(), User.class).get(); // returns an empty list
     *
     * // Negative: exceptions from the underlying call surface wrapped in ExecutionException
     * async.get("badTable", gets, User.class).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table to retrieve from
     * @param anyGets the collection of AnyGet builders specifying the rows
     * @param targetClass the class to convert each non-empty result to
     * @return a ContinuableFuture whose value is a {@code List<T>} of converted objects,
     *         with empty/missing rows skipped. Wraps {@link HBaseExecutor#get(String, Collection, Class)}.
     * @see HBaseExecutor#get(String, Collection, Class)
     * @see AnyGet
     */
    public <T> ContinuableFuture<List<T>> get(final String tableName, final Collection<AnyGet> anyGets, final Class<T> targetClass) {
        return asyncExecutor.execute(() -> hbaseExecutor.get(tableName, anyGets, targetClass));
    }

    /**
     * Asynchronously scans the specified HBase table for all rows in a column family.
     *
     * <p>Performs a full table scan retrieving all cells from the specified column family.
     * The returned {@link ContinuableFuture} completes with a lazy {@link Stream} of {@link Result}
     * objects backed by an open HBase {@code ResultScanner}.</p>
     *
     * <p><b>Important:</b> the returned Stream owns an open HBase scanner and must be closed
     * (e.g., via try-with-resources or by exhausting it with a terminal operation that closes it)
     * to release server-side resources.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Typical: consume the stream on the async pool (thenRunAsync takes the resolved value).
     * // Close the stream after use; it owns the underlying scanner.
     * async.scan("users", "info")
     *      .thenRunAsync(stream -> { try (stream) { stream.forEach(System.out::println); } }); // returns ContinuableFuture<Void>
     *
     * // Typical: block for the stream, then count rows
     * try (Stream<Result> s = async.scan("users", "info").get()) {
     *     long n = s.count(); // returns the number of rows having a cell in family "info"
     * }                       // throws InterruptedException, ExecutionException
     *
     * // Edge: scanning a family with no rows yields an empty (but still closeable) stream
     * try (Stream<Result> empty = async.scan("emptyTable", "info").get()) {
     *     long n = empty.count(); // returns 0
     * }                           // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param tableName the name of the HBase table to scan
     * @param family the column family name to retrieve (as String)
     * @return a {@link ContinuableFuture} containing a {@code Stream<Result>} of all rows in the
     *         specified family. Wraps {@link HBaseExecutor#scan(String, String)}.
     * @see HBaseExecutor#scan(String, String)
     * @see Scan
     * @see Result
     */
    public ContinuableFuture<Stream<Result>> scan(final String tableName, final String family) {
        return asyncExecutor.execute(() -> hbaseExecutor.scan(tableName, family));
    }

    /**
     * Asynchronously scans the specified HBase table for all rows with a specific column.
     *
     * <p>Performs a full table scan retrieving only the specified column (family:qualifier). This
     * is more efficient than scanning the entire family when only specific columns are needed.</p>
     *
     * <p><b>Important:</b> the returned Stream owns an open HBase scanner and must be closed
     * after use to release server-side resources.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Typical: map each row's single column to a value on the async pool.
     * // toList() is a terminal op that closes the underlying scanner.
     * List<String> names = async.scan("users", "info", "name")
     *      .thenCallAsync(stream -> stream.map(r -> Bytes.toString(r.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")))).toList())
     *      .get(); // returns the list of names // throws InterruptedException, ExecutionException
     *
     * // Edge: a column with no matching rows yields an empty stream
     * try (Stream<Result> empty = async.scan("users", "info", "missingCol").get()) {
     *     long n = empty.count(); // returns 0
     * }                           // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param tableName the name of the HBase table to scan
     * @param family the column family name (as String)
     * @param qualifier the column qualifier name (as String)
     * @return a {@link ContinuableFuture} containing a {@code Stream<Result>} for the specified
     *         column. Wraps {@link HBaseExecutor#scan(String, String, String)}.
     * @see HBaseExecutor#scan(String, String, String)
     * @see Scan
     * @see Result
     */
    public ContinuableFuture<Stream<Result>> scan(final String tableName, final String family, final String qualifier) {
        return asyncExecutor.execute(() -> hbaseExecutor.scan(tableName, family, qualifier));
    }

    /**
     * Asynchronously scans the specified HBase table for all rows in a column family, using a
     * byte-array family identifier.
     *
     * <p>Performs a full table scan retrieving all cells from the specified column family. The
     * byte-array variant avoids string-to-bytes conversion when the bytes are already available.</p>
     *
     * <p><b>Important:</b> the returned Stream owns an open HBase scanner and must be closed
     * after use.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     * byte[] family = Bytes.toBytes("info");
     *
     * // Typical: block for the stream, then iterate (close it afterwards)
     * try (Stream<Result> s = async.scan("users", family).get()) {
     *     long n = s.count(); // returns the number of rows having a cell in family "info"
     * }                       // throws InterruptedException, ExecutionException
     *
     * // Typical: consume on the async pool
     * async.scan("users", family)
     *      .thenRunAsync(stream -> { try (stream) { stream.forEach(System.out::println); } }); // returns ContinuableFuture<Void>
     *
     * // Negative: the scan stream is lazy, so get() returns a stream successfully even for a bad
     * // table; the underlying table is opened on consumption, surfacing an UncheckedIOException then.
     * try (Stream<Result> s = async.scan("badTable", family).get()) {
     *     s.count(); // throws UncheckedIOException (table opened on consumption)
     * }              // the get() above only throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param tableName the name of the HBase table to scan
     * @param family the column family name as a byte array
     * @return a {@link ContinuableFuture} containing a {@code Stream<Result>} from the scan. Wraps
     *         {@link HBaseExecutor#scan(String, byte[])}.
     * @see HBaseExecutor#scan(String, byte[])
     * @see Scan
     * @see Result
     */
    public ContinuableFuture<Stream<Result>> scan(final String tableName, final byte[] family) {
        return asyncExecutor.execute(() -> hbaseExecutor.scan(tableName, family));
    }

    /**
     * Asynchronously scans the specified HBase table for all rows with a specific column, using
     * byte-array identifiers.
     *
     * <p>Performs a full table scan retrieving only the specified column (family:qualifier). The
     * byte-array variant avoids string-to-bytes conversion when the bytes are already available.</p>
     *
     * <p><b>Important:</b> the returned Stream owns an open HBase scanner and must be closed
     * after use.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     * byte[] family = Bytes.toBytes("info");
     * byte[] qualifier = Bytes.toBytes("name");
     *
     * // Typical: block for the column-restricted stream, then count
     * try (Stream<Result> s = async.scan("users", family, qualifier).get()) {
     *     long n = s.count(); // returns the number of rows having info:name
     * }                       // throws InterruptedException, ExecutionException
     *
     * // Edge: a qualifier that no row has yields an empty stream
     * try (Stream<Result> empty = async.scan("users", family, Bytes.toBytes("missing")).get()) {
     *     long n = empty.count(); // returns 0
     * }                           // throws InterruptedException, ExecutionException
     *
     * // Negative: the scan stream is lazy, so get() returns a stream successfully even for a bad
     * // table; the underlying table is opened on consumption, surfacing an UncheckedIOException then.
     * try (Stream<Result> s = async.scan("badTable", family, qualifier).get()) {
     *     s.count(); // throws UncheckedIOException (table opened on consumption)
     * }              // the get() above only throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param tableName the name of the HBase table to scan
     * @param family the column family name as a byte array
     * @param qualifier the column qualifier name as a byte array
     * @return a {@link ContinuableFuture} containing a {@code Stream<Result>} for the specified
     *         column. Wraps {@link HBaseExecutor#scan(String, byte[], byte[])}.
     * @see HBaseExecutor#scan(String, byte[], byte[])
     * @see Scan
     * @see Result
     */
    public ContinuableFuture<Stream<Result>> scan(final String tableName, final byte[] family, final byte[] qualifier) {
        return asyncExecutor.execute(() -> hbaseExecutor.scan(tableName, family, qualifier));
    }

    /**
     * Asynchronously scans the specified HBase table using a fluent {@link AnyScan} builder.
     *
     * <p>Performs a table scan with the criteria specified in the AnyScan builder, which exposes
     * a fluent API for configuring start/stop rows, filters, column families, time ranges, and
     * result limits.</p>
     *
     * <p><b>Important:</b> the returned Stream owns an open HBase scanner and must be closed
     * after use.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Typical: a bounded scan consumed on the async pool (close the stream after use)
     * async.scan("users", AnyScan.create().withStartRow("user1").withStopRow("user9").setLimit(100))
     *      .thenRunAsync(stream -> { try (stream) { stream.forEach(System.out::println); } }); // returns ContinuableFuture<Void>
     *
     * // Typical: block for the stream, then collect
     * try (Stream<Result> s = async.scan("users", AnyScan.create().setLimit(10)).get()) {
     *     List<Result> first10 = s.toList(); // returns up to 10 rows
     * }                                      // throws InterruptedException, ExecutionException
     *
     * // Negative: the scan stream is lazy, so get() returns a stream successfully even for a bad
     * // table; the underlying table is opened on consumption, surfacing an UncheckedIOException then.
     * try (Stream<Result> s = async.scan("badTable", AnyScan.create()).get()) {
     *     s.count(); // throws UncheckedIOException (table opened on consumption)
     * }              // the get() above only throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param tableName the name of the HBase table to scan
     * @param anyScan the AnyScan builder specifying scan criteria
     * @return a {@link ContinuableFuture} containing a {@code Stream<Result>} matching the scan
     *         criteria. Wraps {@link HBaseExecutor#scan(String, AnyScan)}.
     * @see HBaseExecutor#scan(String, AnyScan)
     * @see AnyScan
     * @see Result
     */
    public ContinuableFuture<Stream<Result>> scan(final String tableName, final AnyScan anyScan) {
        return asyncExecutor.execute(() -> hbaseExecutor.scan(tableName, anyScan));
    }

    /**
     * Asynchronously scans the specified HBase table using a native HBase {@link Scan} object.
     *
     * <p>Performs a table scan with the criteria specified in the HBase {@link Scan} object,
     * providing direct access to all HBase scan capabilities including filters, column selection,
     * time ranges, caching, and batching.</p>
     *
     * <p><b>Important:</b> the returned Stream owns an open HBase scanner and must be closed
     * after use.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Typical: a native Scan with caching, consumed after blocking for the stream
     * Scan scan = new Scan().withStartRow(Bytes.toBytes("user1")).setCaching(500);
     * try (Stream<Result> s = async.scan("users", scan).get()) {
     *     long n = s.count(); // returns the number of rows in [user1, end)
     * }                       // throws InterruptedException, ExecutionException
     *
     * // Typical: consume on the async pool
     * async.scan("users", new Scan())
     *      .thenRunAsync(stream -> { try (stream) { stream.forEach(System.out::println); } }); // returns ContinuableFuture<Void>
     *
     * // Negative: the scan stream is lazy, so get() returns a stream successfully even for a bad
     * // table; the underlying table is opened on consumption, surfacing an UncheckedIOException then.
     * try (Stream<Result> s = async.scan("badTable", new Scan()).get()) {
     *     s.count(); // throws UncheckedIOException (table opened on consumption)
     * }              // the get() above only throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param tableName the name of the HBase table to scan
     * @param scan the HBase Scan object specifying scan criteria
     * @return a {@link ContinuableFuture} containing a {@code Stream<Result>} matching the scan
     *         criteria. Wraps {@link HBaseExecutor#scan(String, Scan)}.
     * @see HBaseExecutor#scan(String, Scan)
     * @see Scan
     * @see Result
     */
    public ContinuableFuture<Stream<Result>> scan(final String tableName, final Scan scan) {
        return asyncExecutor.execute(() -> hbaseExecutor.scan(tableName, scan));
    }

    /**
     * Asynchronously scans the table and converts each Result to the specified target type.
     *
     * <p>Performs a full table scan of the specified column family and automatically converts each
     * {@link Result} to {@code T} using {@link HBaseExecutor}'s entity-mapping facilities.</p>
     *
     * <p><b>Important:</b> the returned Stream owns an open HBase scanner and must be closed
     * after use.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Typical: map every row to an entity and process on the async pool (close the stream).
     * async.scan("users", "info", User.class)
     *      .thenRunAsync(stream -> { try (stream) { stream.forEach(u -> System.out.println(u.getName())); } }); // returns ContinuableFuture<Void>
     *
     * // Typical: block for the stream, then collect entities
     * try (Stream<User> s = async.scan("users", "info", User.class).get()) {
     *     List<User> users = s.toList(); // returns the mapped User entities
     * }                                  // throws InterruptedException, ExecutionException
     *
     * // Edge: scanning a family with no rows yields an empty stream
     * try (Stream<User> empty = async.scan("emptyTable", "info", User.class).get()) {
     *     long n = empty.count(); // returns 0
     * }                           // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table to scan
     * @param family the column family name (as String)
     * @param targetClass the class to convert each result to
     * @return a {@link ContinuableFuture} containing a {@code Stream<T>} of converted objects.
     *         Wraps {@link HBaseExecutor#scan(String, String, Class)}.
     * @see HBaseExecutor#scan(String, String, Class)
     * @see Scan
     */
    public <T> ContinuableFuture<Stream<T>> scan(final String tableName, final String family, final Class<T> targetClass) {
        return asyncExecutor.execute(() -> hbaseExecutor.scan(tableName, family, targetClass));
    }

    /**
     * Asynchronously scans the table for a specific column and converts each Result to the
     * specified target type.
     *
     * <p>Performs a full table scan retrieving only the specified column (family:qualifier) and
     * automatically converts each Result to {@code T}. This is efficient when each row maps to a
     * single value.</p>
     *
     * <p><b>Important:</b> the returned Stream owns an open HBase scanner and must be closed
     * after use.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Typical: scan a single column and map each row to a value
     * try (Stream<String> s = async.scan("users", "info", "name", String.class).get()) {
     *     List<String> names = s.toList(); // returns the per-row mapped values
     * }                                    // throws InterruptedException, ExecutionException
     *
     * // Edge: a column that no row has yields an empty stream
     * try (Stream<String> empty = async.scan("users", "info", "missingCol", String.class).get()) {
     *     long n = empty.count(); // returns 0
     * }                           // throws InterruptedException, ExecutionException
     *
     * // Negative: the scan stream is lazy, so get() returns a stream successfully even for a bad
     * // table; the underlying table is opened on consumption, surfacing an UncheckedIOException then.
     * try (Stream<String> s = async.scan("badTable", "info", "name", String.class).get()) {
     *     s.count(); // throws UncheckedIOException (table opened on consumption)
     * }              // the get() above only throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table to scan
     * @param family the column family name (as String)
     * @param qualifier the column qualifier name (as String)
     * @param targetClass the class to convert each result to
     * @return a {@link ContinuableFuture} containing a {@code Stream<T>} of converted objects.
     *         Wraps {@link HBaseExecutor#scan(String, String, String, Class)}.
     * @see HBaseExecutor#scan(String, String, String, Class)
     * @see Scan
     */
    public <T> ContinuableFuture<Stream<T>> scan(final String tableName, final String family, final String qualifier, final Class<T> targetClass) {
        return asyncExecutor.execute(() -> hbaseExecutor.scan(tableName, family, qualifier, targetClass));
    }

    /**
     * Asynchronously scans the table and converts each Result to the specified target type, using
     * a byte-array family identifier.
     *
     * <p>The byte-array variant avoids string-to-bytes conversion when the bytes are already
     * available.</p>
     *
     * <p><b>Important:</b> the returned Stream owns an open HBase scanner and must be closed
     * after use.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     * byte[] family = Bytes.toBytes("info");
     *
     * // Typical: map each row to an entity (close the stream after use)
     * try (Stream<User> s = async.scan("users", family, User.class).get()) {
     *     List<User> users = s.toList(); // returns the mapped User entities
     * }                                  // throws InterruptedException, ExecutionException
     *
     * // Edge: an empty table yields an empty stream
     * try (Stream<User> empty = async.scan("emptyTable", family, User.class).get()) {
     *     long n = empty.count(); // returns 0
     * }                           // throws InterruptedException, ExecutionException
     *
     * // Negative: the scan stream is lazy, so get() returns a stream successfully even for a bad
     * // table; the underlying table is opened on consumption, surfacing an UncheckedIOException then.
     * try (Stream<User> s = async.scan("badTable", family, User.class).get()) {
     *     s.count(); // throws UncheckedIOException (table opened on consumption)
     * }              // the get() above only throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table to scan
     * @param family the column family name as a byte array
     * @param targetClass the class to convert each result to
     * @return a {@link ContinuableFuture} containing a {@code Stream<T>} of converted objects.
     *         Wraps {@link HBaseExecutor#scan(String, byte[], Class)}.
     * @see HBaseExecutor#scan(String, byte[], Class)
     * @see Scan
     */
    public <T> ContinuableFuture<Stream<T>> scan(final String tableName, final byte[] family, final Class<T> targetClass) {
        return asyncExecutor.execute(() -> hbaseExecutor.scan(tableName, family, targetClass));
    }

    /**
     * Asynchronously scans the table for a specific column and converts each Result to the
     * specified target type, using byte-array identifiers.
     *
     * <p>The byte-array variant avoids string-to-bytes conversion when the bytes are already
     * available.</p>
     *
     * <p><b>Important:</b> the returned Stream owns an open HBase scanner and must be closed
     * after use.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     * byte[] family = Bytes.toBytes("info");
     * byte[] qualifier = Bytes.toBytes("name");
     *
     * // Typical: scan a single column and map each row to a value
     * try (Stream<String> s = async.scan("users", family, qualifier, String.class).get()) {
     *     List<String> names = s.toList(); // returns the per-row mapped values
     * }                                    // throws InterruptedException, ExecutionException
     *
     * // Edge: a qualifier no row has yields an empty stream
     * try (Stream<String> empty = async.scan("users", family, Bytes.toBytes("missing"), String.class).get()) {
     *     long n = empty.count(); // returns 0
     * }                           // throws InterruptedException, ExecutionException
     *
     * // Negative: the scan stream is lazy, so get() returns a stream successfully even for a bad
     * // table; the underlying table is opened on consumption, surfacing an UncheckedIOException then.
     * try (Stream<String> s = async.scan("badTable", family, qualifier, String.class).get()) {
     *     s.count(); // throws UncheckedIOException (table opened on consumption)
     * }              // the get() above only throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table to scan
     * @param family the column family name as a byte array
     * @param qualifier the column qualifier name as a byte array
     * @param targetClass the class to convert each result to
     * @return a {@link ContinuableFuture} containing a {@code Stream<T>} of converted objects.
     *         Wraps {@link HBaseExecutor#scan(String, byte[], byte[], Class)}.
     * @see HBaseExecutor#scan(String, byte[], byte[], Class)
     * @see Scan
     */
    public <T> ContinuableFuture<Stream<T>> scan(final String tableName, final byte[] family, final byte[] qualifier, final Class<T> targetClass) {
        return asyncExecutor.execute(() -> hbaseExecutor.scan(tableName, family, qualifier, targetClass));
    }

    /**
     * Asynchronously scans the table using a fluent {@link AnyScan} builder and converts each
     * Result to the specified target type.
     *
     * <p>Combines the flexibility of {@link AnyScan} configuration (start/stop rows, filters,
     * column families, time ranges, limits) with automatic object mapping.</p>
     *
     * <p><b>Important:</b> the returned Stream owns an open HBase scanner and must be closed
     * after use.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Typical: a bounded scan mapped to entities and processed on the async pool (close the stream).
     * async.scan("users", AnyScan.create().withStartRow("user1").setLimit(50), User.class)
     *      .thenRunAsync(stream -> { try (stream) { stream.forEach(user -> processUser(user)); } }); // returns ContinuableFuture<Void>
     *
     * // Typical: block for the stream, then collect entities
     * try (Stream<User> s = async.scan("users", AnyScan.create().setLimit(50), User.class).get()) {
     *     List<User> users = s.toList(); // returns up to 50 mapped Users
     * }                                  // throws InterruptedException, ExecutionException
     *
     * // Negative: the scan stream is lazy, so get() returns a stream successfully even for a bad
     * // table; the underlying table is opened on consumption, surfacing an UncheckedIOException then.
     * try (Stream<User> s = async.scan("badTable", AnyScan.create(), User.class).get()) {
     *     s.count(); // throws UncheckedIOException (table opened on consumption)
     * }              // the get() above only throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table to scan
     * @param anyScan the AnyScan builder specifying scan criteria
     * @param targetClass the class to convert each result to
     * @return a {@link ContinuableFuture} containing a {@code Stream<T>} of converted objects.
     *         Wraps {@link HBaseExecutor#scan(String, AnyScan, Class)}.
     * @see HBaseExecutor#scan(String, AnyScan, Class)
     * @see AnyScan
     */
    public <T> ContinuableFuture<Stream<T>> scan(final String tableName, final AnyScan anyScan, final Class<T> targetClass) {
        return asyncExecutor.execute(() -> hbaseExecutor.scan(tableName, anyScan, targetClass));
    }

    /**
     * Asynchronously scans the table using a native HBase {@link Scan} object and converts each
     * Result to the specified target type.
     *
     * <p>Provides full access to HBase scan capabilities (filters, column selection, time ranges,
     * caching, batching) combined with automatic object mapping.</p>
     *
     * <p><b>Important:</b> the returned Stream owns an open HBase scanner and must be closed
     * after use.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Typical: a native Scan mapped to entities, consumed after blocking
     * Scan scan = new Scan().withStartRow(Bytes.toBytes("user1")).setCaching(500);
     * try (Stream<User> s = async.scan("users", scan, User.class).get()) {
     *     List<User> users = s.toList(); // returns the mapped User entities
     * }                                  // throws InterruptedException, ExecutionException
     *
     * // Typical: process on the async pool
     * async.scan("users", new Scan(), User.class)
     *      .thenRunAsync(stream -> { try (stream) { stream.forEach(u -> processUser(u)); } }); // returns ContinuableFuture<Void>
     *
     * // Negative: the scan stream is lazy, so get() returns a stream successfully even for a bad
     * // table; the underlying table is opened on consumption, surfacing an UncheckedIOException then.
     * try (Stream<User> s = async.scan("badTable", new Scan(), User.class).get()) {
     *     s.count(); // throws UncheckedIOException (table opened on consumption)
     * }              // the get() above only throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table to scan
     * @param scan the HBase Scan object specifying scan criteria
     * @param targetClass the class to convert each result to
     * @return a {@link ContinuableFuture} containing a {@code Stream<T>} of converted objects.
     *         Wraps {@link HBaseExecutor#scan(String, Scan, Class)}.
     * @see HBaseExecutor#scan(String, Scan, Class)
     * @see Scan
     */
    public <T> ContinuableFuture<Stream<T>> scan(final String tableName, final Scan scan, final Class<T> targetClass) {
        return asyncExecutor.execute(() -> hbaseExecutor.scan(tableName, scan, targetClass));
    }

    /**
     * Asynchronously inserts or updates a single row in the specified HBase table.
     *
     * <p>Performs a put operation that stores cells from the {@link Put} object into HBase. Cells
     * are written per (row, family, qualifier, timestamp) tuple: an explicit-timestamp cell with
     * the same key as an existing cell overwrites it, while cells with different timestamps
     * produce additional versions (subject to the column family's {@code VERSIONS} setting). The
     * returned {@link ContinuableFuture} completes with {@code null} when the operation finishes
     * successfully.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     * Put put = new Put(Bytes.toBytes("user123")).addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("John"));
     *
     * // Typical: block until the put completes
     * Object done = async.put("users", put).get(); // returns null on success
     *
     * // Typical: run a follow-up once the put completes, on the async pool
     * async.put("users", put).thenRunAsync(() -> System.out.println("Put complete")); // returns ContinuableFuture<Void>
     *
     * // Negative: exceptions from the underlying call surface wrapped in ExecutionException
     * async.put("badTable", put).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param tableName the name of the HBase table to put data into
     * @param put the Put operation containing the row key and cells to store
     * @return a {@link ContinuableFuture} that completes with {@code null} when the put operation
     *         finishes. Wraps {@link HBaseExecutor#put(String, Put)}.
     * @see HBaseExecutor#put(String, Put)
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
     * <p>Performs batch put operations to efficiently store multiple rows. The same per-cell
     * overwrite-vs-version semantics described for {@link #put(String, Put)} apply to each
     * individual Put in the list. The returned {@link ContinuableFuture} completes with
     * {@code null} when all put operations finish successfully.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     * List<Put> puts = Arrays.asList(
     *     new Put(Bytes.toBytes("user1")).addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("Alice")),
     *     new Put(Bytes.toBytes("user2")).addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("Bob"))
     * );
     *
     * // Typical: block until all puts complete
     * Object done = async.put("users", puts).get(); // returns null on success
     *
     * // Edge: an empty list is a no-op that still completes successfully
     * Object none = async.put("users", Collections.<Put>emptyList()).get(); // returns null
     *
     * // Negative: exceptions from the underlying call surface wrapped in ExecutionException
     * async.put("badTable", puts).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param tableName the name of the HBase table to put data into
     * @param puts the list of Put operations to execute
     * @return a {@link ContinuableFuture} that completes with {@code null} when all put operations
     *         finish. Wraps {@link HBaseExecutor#put(String, List)}.
     * @see HBaseExecutor#put(String, List)
     * @see Put
     */
    public ContinuableFuture<Void> put(final String tableName, final List<Put> puts) {
        return asyncExecutor.execute(() -> {
            hbaseExecutor.put(tableName, puts);

            return null;
        });
    }

    /**
     * Asynchronously inserts or updates a single row using a fluent {@link AnyPut} builder.
     *
     * <p>Equivalent to {@link #put(String, Put)} but using the fluent {@link AnyPut} wrapper. See
     * that method for the per-cell overwrite-vs-version semantics. The returned
     * {@link ContinuableFuture} completes with {@code null} when the operation finishes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Typical: block until the put completes
     * Object done = async.put("users", AnyPut.of("user123").addColumn("info", "name", "John")).get(); // returns null on success
     *
     * // Typical: run a follow-up once the put completes, on the async pool
     * async.put("users", AnyPut.of("user123").addColumn("info", "name", "John"))
     *      .thenRunAsync(() -> System.out.println("User saved")); // returns ContinuableFuture<Void>
     *
     * // Negative: exceptions from the underlying call surface wrapped in ExecutionException
     * async.put("badTable", AnyPut.of("user123").addColumn("info", "name", "John")).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param tableName the name of the HBase table to put data into
     * @param anyPut the AnyPut builder specifying the row and cells to store
     * @return a {@link ContinuableFuture} that completes with {@code null} when the put operation
     *         finishes. Wraps {@link HBaseExecutor#put(String, AnyPut)}.
     * @see HBaseExecutor#put(String, AnyPut)
     * @see AnyPut
     */
    public ContinuableFuture<Void> put(final String tableName, final AnyPut anyPut) {
        return asyncExecutor.execute(() -> {
            hbaseExecutor.put(tableName, anyPut);

            return null;
        });
    }

    /**
     * Asynchronously inserts or updates multiple rows using {@link AnyPut} builders.
     *
     * <p>Performs batch put operations using a collection of AnyPut builders. The same per-cell
     * overwrite-vs-version semantics described for {@link #put(String, Put)} apply to each
     * individual AnyPut. The returned {@link ContinuableFuture} completes with {@code null} when
     * all put operations finish successfully.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     * Collection<AnyPut> puts = Arrays.asList(
     *     AnyPut.of("user1").addColumn("info", "name", "Alice"),
     *     AnyPut.of("user2").addColumn("info", "name", "Bob")
     * );
     *
     * // Typical: block until all puts complete
     * Object done = async.put("users", puts).get(); // returns null on success
     *
     * // Edge: an empty collection is a no-op that still completes successfully
     * Object none = async.put("users", Collections.<AnyPut>emptyList()).get(); // returns null
     *
     * // Negative: exceptions from the underlying call surface wrapped in ExecutionException
     * async.put("badTable", puts).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param tableName the name of the HBase table to put data into
     * @param anyPuts the collection of AnyPut builders specifying the rows to store
     * @return a {@link ContinuableFuture} that completes with {@code null} when all put operations
     *         finish. Wraps {@link HBaseExecutor#put(String, Collection)}.
     * @see HBaseExecutor#put(String, Collection)
     * @see AnyPut
     */
    public ContinuableFuture<Void> put(final String tableName, final Collection<AnyPut> anyPuts) {
        return asyncExecutor.execute(() -> {
            hbaseExecutor.put(tableName, anyPuts);

            return null;
        });
    }

    /**
     * Asynchronously deletes a row, column family, column, or specific cell version from the
     * specified HBase table.
     *
     * <p>The granularity of the delete is determined by the {@link Delete} object: it may target
     * an entire row, specific column families, specific qualifiers, or individual versions. The
     * returned {@link ContinuableFuture} completes with {@code null} when the operation finishes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     * Delete delete = new Delete(Bytes.toBytes("user123"));
     *
     * // Typical: block until the delete completes
     * Object done = async.delete("users", delete).get(); // returns null on success
     *
     * // Typical: run a follow-up once the delete completes, on the async pool
     * async.delete("users", delete).thenRunAsync(() -> System.out.println("Delete complete")); // returns ContinuableFuture<Void>
     *
     * // Edge: deleting a non-existent row still succeeds (no error)
     * Object missing = async.delete("users", new Delete(Bytes.toBytes("nope"))).get(); // returns null
     *
     * // Negative: exceptions from the underlying call surface wrapped in ExecutionException
     * async.delete("badTable", delete).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param tableName the name of the HBase table to delete from
     * @param delete the Delete operation specifying the row and cells to delete
     * @return a {@link ContinuableFuture} that completes with {@code null} when the delete
     *         operation finishes. Wraps {@link HBaseExecutor#delete(String, Delete)}.
     * @see HBaseExecutor#delete(String, Delete)
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
     * <p>Performs batch delete operations to efficiently remove multiple rows or cells. The
     * returned {@link ContinuableFuture} completes with {@code null} when all delete operations
     * finish successfully.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     * List<Delete> deletes = Arrays.asList(
     *     new Delete(Bytes.toBytes("user1")),
     *     new Delete(Bytes.toBytes("user2"))
     * );
     *
     * // Typical: block until all deletes complete
     * Object done = async.delete("users", deletes).get(); // returns null on success
     *
     * // Edge: an empty list is a no-op that still completes successfully
     * Object none = async.delete("users", Collections.<Delete>emptyList()).get(); // returns null
     *
     * // Negative: exceptions from the underlying call surface wrapped in ExecutionException
     * async.delete("badTable", deletes).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param tableName the name of the HBase table to delete from
     * @param deletes the list of Delete operations to execute
     * @return a {@link ContinuableFuture} that completes with {@code null} when all delete
     *         operations finish. Wraps {@link HBaseExecutor#delete(String, List)}.
     * @see HBaseExecutor#delete(String, List)
     * @see Delete
     */
    public ContinuableFuture<Void> delete(final String tableName, final List<Delete> deletes) {
        return asyncExecutor.execute(() -> {
            hbaseExecutor.delete(tableName, deletes);

            return null;
        });
    }

    /**
     * Asynchronously deletes a row, column family, column, or specific cell version using a
     * fluent {@link AnyDelete} builder.
     *
     * <p>Equivalent to {@link #delete(String, Delete)} but using the fluent {@link AnyDelete}
     * wrapper. The returned {@link ContinuableFuture} completes with {@code null} when the
     * operation finishes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Typical: block until the delete completes
     * Object done = async.delete("users", AnyDelete.of("user123")).get(); // returns null on success
     *
     * // Typical: run a follow-up once the delete completes, on the async pool
     * async.delete("users", AnyDelete.of("user123"))
     *      .thenRunAsync(() -> System.out.println("User deleted")); // returns ContinuableFuture<Void>
     *
     * // Negative: exceptions from the underlying call surface wrapped in ExecutionException
     * async.delete("badTable", AnyDelete.of("user123")).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param tableName the name of the HBase table to delete from
     * @param anyDelete the AnyDelete builder specifying the row and cells to delete
     * @return a {@link ContinuableFuture} that completes with {@code null} when the delete
     *         operation finishes. Wraps {@link HBaseExecutor#delete(String, AnyDelete)}.
     * @see HBaseExecutor#delete(String, AnyDelete)
     * @see AnyDelete
     */
    public ContinuableFuture<Void> delete(final String tableName, final AnyDelete anyDelete) {
        return asyncExecutor.execute(() -> {
            hbaseExecutor.delete(tableName, anyDelete);

            return null;
        });
    }

    /**
     * Asynchronously deletes multiple rows or cells using {@link AnyDelete} builders.
     *
     * <p>Performs batch delete operations using a collection of {@link AnyDelete} builders. The
     * returned {@link ContinuableFuture} completes with {@code null} when all delete operations
     * finish successfully.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     * Collection<AnyDelete> deletes = Arrays.asList(
     *     AnyDelete.of("user1"),
     *     AnyDelete.of("user2").addColumn("info", "email")
     * );
     *
     * // Typical: block until all deletes complete
     * Object done = async.delete("users", deletes).get(); // returns null on success
     *
     * // Edge: an empty collection is a no-op that still completes successfully
     * Object none = async.delete("users", Collections.<AnyDelete>emptyList()).get(); // returns null
     *
     * // Negative: exceptions from the underlying call surface wrapped in ExecutionException
     * async.delete("badTable", deletes).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param tableName the name of the HBase table to delete from
     * @param anyDeletes the collection of AnyDelete builders specifying the rows to delete
     * @return a {@link ContinuableFuture} that completes with {@code null} when all delete
     *         operations finish. Wraps {@link HBaseExecutor#delete(String, Collection)}.
     * @see HBaseExecutor#delete(String, Collection)
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
     * <p>Executes multiple mutations (puts and/or deletes) on a single row atomically. All
     * mutations target the same row key and either all succeed or all fail, ensuring per-row
     * consistency. The returned {@link ContinuableFuture} completes with {@code null} when the
     * operation finishes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Setup: a put and a delete on the same row (AnyRowMutations.add throws IOException)
     * AnyRowMutations rm = AnyRowMutations.of("user123")
     *     .add(AnyPut.of("user123").addColumn("info", "name", "John").val())
     *     .add(AnyDelete.of("user123").addColumn("info", "oldEmail").val()); // build the atomic put+delete set
     *
     * // Typical: apply both mutations atomically
     * Object done = async.mutateRow("users", rm).get(); // returns null on success
     *
     * // Typical: run a follow-up only after the mutation completes
     * async.mutateRow("users", rm).thenRunAsync(() -> System.out.println("Row mutated")); // returns ContinuableFuture<Void>
     *
     * // Negative: exceptions from the underlying call surface wrapped in ExecutionException
     * async.mutateRow("badTable", rm).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param rm the AnyRowMutations builder containing the atomic mutations
     * @return a {@link ContinuableFuture} that completes with {@code null} when the mutations
     *         finish. Wraps {@link HBaseExecutor#mutateRow(String, AnyRowMutations)}.
     * @see HBaseExecutor#mutateRow(String, AnyRowMutations)
     * @see AnyRowMutations
     */
    public ContinuableFuture<Void> mutateRow(final String tableName, final AnyRowMutations rm) {
        return asyncExecutor.execute(() -> {
            hbaseExecutor.mutateRow(tableName, rm);

            return null;
        });
    }

    /**
     * Asynchronously performs atomic mutations on a single row using a native {@link RowMutations}
     * object.
     *
     * <p>Executes multiple mutations (puts and/or deletes) on a single row atomically using the
     * HBase native {@link RowMutations} API. All mutations target the same row key and either all
     * succeed or all fail. The returned {@link ContinuableFuture} completes with {@code null} when
     * the operation finishes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Setup: a put and a delete on the same row (RowMutations.add throws IOException)
     * RowMutations rm = new RowMutations(Bytes.toBytes("user123"));
     * rm.add(new Put(Bytes.toBytes("user123")).addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("John"))); // stage a put on the row
     * rm.add(new Delete(Bytes.toBytes("user123")).addColumns(Bytes.toBytes("info"), Bytes.toBytes("oldEmail")));                // stage a delete on the same row
     *
     * // Typical: apply both mutations atomically
     * Object done = async.mutateRow("users", rm).get(); // returns null on success
     *
     * // Typical: run a follow-up only after the mutation completes
     * async.mutateRow("users", rm).thenRunAsync(() -> System.out.println("Row mutated")); // returns ContinuableFuture<Void>
     *
     * // Negative: exceptions from the underlying call surface wrapped in ExecutionException
     * async.mutateRow("badTable", rm).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param rm the RowMutations object containing the atomic mutations
     * @return a {@link ContinuableFuture} that completes with {@code null} when the mutations
     *         finish. Wraps {@link HBaseExecutor#mutateRow(String, RowMutations)}.
     * @see HBaseExecutor#mutateRow(String, RowMutations)
     * @see RowMutations
     */
    public ContinuableFuture<Void> mutateRow(final String tableName, final RowMutations rm) {
        return asyncExecutor.execute(() -> {
            hbaseExecutor.mutateRow(tableName, rm);

            return null;
        });
    }

    /**
     * Asynchronously appends data to one or more columns in a row using a fluent {@link AnyAppend}
     * builder.
     *
     * <p>Atomically concatenates the supplied bytes to the end of each targeted cell's existing
     * value. If a target cell does not yet exist, it is created with the supplied value. This is
     * useful for maintaining log-like or counter-like columns. The returned
     * {@link ContinuableFuture} completes with a {@link Result} containing the new (post-append)
     * cell values.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Typical: append to a log column and read back the post-append value
     * Result r = async.append("users", AnyAppend.of("user123").addColumn("info", "log", "Entry1")).get(); // returns the Result with the new cell values
     * byte[] log = r.getValue(Bytes.toBytes("info"), Bytes.toBytes("log"));
     *
     * // Typical: handle the post-append Result on the async pool
     * async.append("users", AnyAppend.of("user123").addColumn("info", "log", "Entry2"))
     *      .thenRunAsync(result -> System.out.println("New value: " + result)); // returns ContinuableFuture<Void>
     *
     * // Negative: exceptions from the underlying call surface wrapped in ExecutionException
     * async.append("badTable", AnyAppend.of("user123").addColumn("info", "log", "x")).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param append the AnyAppend builder specifying the row and values to append
     * @return a {@link ContinuableFuture} containing the Result with new cell values after the
     *         append. Wraps {@link HBaseExecutor#append(String, AnyAppend)}.
     * @see HBaseExecutor#append(String, AnyAppend)
     * @see AnyAppend
     * @see Result
     */
    public ContinuableFuture<Result> append(final String tableName, final AnyAppend append) {
        return asyncExecutor.execute(() -> hbaseExecutor.append(tableName, append));
    }

    /**
     * Asynchronously appends data to one or more columns in a row using a native {@link Append}
     * object.
     *
     * <p>Atomically concatenates the supplied bytes to the end of each targeted cell's existing
     * value via the HBase native {@link Append} API. If a target cell does not yet exist, it is
     * created with the supplied value. The returned {@link ContinuableFuture} completes with a
     * {@link Result} containing the new (post-append) cell values.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Typical: append to a log column using the native Append API
     * Append append = new Append(Bytes.toBytes("user123")).addColumn(Bytes.toBytes("info"), Bytes.toBytes("log"), Bytes.toBytes("Entry1"));
     * Result r = async.append("users", append).get(); // returns the Result with the new cell values
     *
     * // Typical: handle the post-append Result on the async pool
     * async.append("users", append).thenRunAsync(result -> System.out.println(result)); // returns ContinuableFuture<Void>
     *
     * // Negative: exceptions from the underlying call surface wrapped in ExecutionException
     * async.append("badTable", append).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param append the Append object specifying the row and values to append
     * @return a {@link ContinuableFuture} containing the Result with new cell values after the
     *         append. Wraps {@link HBaseExecutor#append(String, Append)}.
     * @see HBaseExecutor#append(String, Append)
     * @see Append
     * @see Result
     */
    public ContinuableFuture<Result> append(final String tableName, final Append append) {
        return asyncExecutor.execute(() -> hbaseExecutor.append(tableName, append));
    }

    /**
     * Asynchronously increments column values in a row using a fluent {@link AnyIncrement}
     * builder.
     *
     * <p>Atomically increments one or more long-valued cells by the specified amounts. This is
     * ideal for maintaining counters without race conditions. If a target cell does not yet exist,
     * it is initialised to the increment amount. The returned {@link ContinuableFuture} completes
     * with a {@link Result} containing the post-increment cell values.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Typical: bump a counter and read the post-increment value from the Result
     * Result r = async.increment("users", AnyIncrement.of("user123").addColumn("stats", "loginCount", 1)).get(); // returns the Result with the new counter value
     * long newCount = Bytes.toLong(r.getValue(Bytes.toBytes("stats"), Bytes.toBytes("loginCount")));
     *
     * // Typical: handle the post-increment Result on the async pool
     * async.increment("users", AnyIncrement.of("user123").addColumn("stats", "loginCount", 1))
     *      .thenRunAsync(result -> System.out.println("New value: " + result)); // returns ContinuableFuture<Void>
     *
     * // Negative: exceptions from the underlying call surface wrapped in ExecutionException
     * async.increment("badTable", AnyIncrement.of("user123").addColumn("stats", "loginCount", 1)).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param increment the AnyIncrement builder specifying the row and increment amounts
     * @return a {@link ContinuableFuture} containing the Result with new cell values after the
     *         increment. Wraps {@link HBaseExecutor#increment(String, AnyIncrement)}.
     * @see HBaseExecutor#increment(String, AnyIncrement)
     * @see AnyIncrement
     * @see Result
     */
    public ContinuableFuture<Result> increment(final String tableName, final AnyIncrement increment) {
        return asyncExecutor.execute(() -> hbaseExecutor.increment(tableName, increment));
    }

    /**
     * Asynchronously increments column values in a row using a native {@link Increment} object.
     *
     * <p>Atomically increments one or more long-valued cells by the specified amounts using the
     * HBase native {@link Increment} API. If a target cell does not yet exist, it is initialised
     * to the increment amount. The returned {@link ContinuableFuture} completes with a
     * {@link Result} containing the post-increment cell values.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Typical: bump a counter via the native Increment API and read it back
     * Increment increment = new Increment(Bytes.toBytes("user123")).addColumn(Bytes.toBytes("stats"), Bytes.toBytes("loginCount"), 1L);
     * Result r = async.increment("users", increment).get();                                       // returns the Result with the new counter value
     * long count = Bytes.toLong(r.getValue(Bytes.toBytes("stats"), Bytes.toBytes("loginCount"))); // decode the post-increment value
     *
     * // Typical: handle the post-increment Result on the async pool
     * async.increment("users", increment).thenRunAsync(result -> System.out.println(result)); // returns ContinuableFuture<Void>
     *
     * // Negative: exceptions from the underlying call surface wrapped in ExecutionException
     * async.increment("badTable", increment).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param increment the Increment object specifying the row and increment amounts
     * @return a {@link ContinuableFuture} containing the Result with new cell values after the
     *         increment. Wraps {@link HBaseExecutor#increment(String, Increment)}.
     * @see HBaseExecutor#increment(String, Increment)
     * @see Increment
     * @see Result
     */
    public ContinuableFuture<Result> increment(final String tableName, final Increment increment) {
        return asyncExecutor.execute(() -> hbaseExecutor.increment(tableName, increment));
    }

    /**
     * Asynchronously increments a single column value by a specified amount.
     *
     * <p>Atomically increments a specific column's long-encoded value by the given amount. This is
     * a convenience method for incrementing a single counter without constructing an
     * {@link Increment}. If the cell does not yet exist, it is initialised to {@code amount}. The
     * returned {@link ContinuableFuture} completes with the new value after the increment.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Typical: increment a counter and read the new value directly
     * Long newCount = async.incrementColumnValue("users", "user123", "stats", "loginCount", 1).get(); // returns the post-increment value, e.g. 5L
     *
     * // Typical: handle the new value on the async pool
     * async.incrementColumnValue("users", "user123", "stats", "loginCount", 1)
     *      .thenRunAsync(count -> System.out.println("Login count: " + count)); // returns ContinuableFuture<Void>
     *
     * // Edge: a negative amount decrements the counter
     * Long afterDecrement = async.incrementColumnValue("users", "user123", "stats", "loginCount", -1).get(); // returns the decremented value
     *
     * // Negative: exceptions from the underlying call surface wrapped in ExecutionException
     * async.incrementColumnValue("badTable", "user123", "stats", "loginCount", 1).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param rowKey the row key (will be converted to bytes)
     * @param family the column family name (as String)
     * @param qualifier the column qualifier name (as String)
     * @param amount the amount to increment by (can be negative for decrement)
     * @return a {@link ContinuableFuture} containing the new value after the increment. Wraps
     *         {@link HBaseExecutor#incrementColumnValue(String, Object, String, String, long)}.
     * @see HBaseExecutor#incrementColumnValue(String, Object, String, String, long)
     */
    public ContinuableFuture<Long> incrementColumnValue(final String tableName, final Object rowKey, final String family, final String qualifier,
            final long amount) {
        return asyncExecutor.execute(() -> hbaseExecutor.incrementColumnValue(tableName, rowKey, family, qualifier, amount));
    }

    /**
     * Asynchronously increments a single column value with the specified write durability.
     *
     * <p>Atomically increments a specific column's long-encoded value by the given amount with
     * control over write durability. The durability setting determines how the WAL is flushed
     * (e.g., {@link Durability#SYNC_WAL}, {@link Durability#ASYNC_WAL}, {@link Durability#SKIP_WAL}).
     * The returned {@link ContinuableFuture} completes with the new value.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Typical: increment with durable WAL write and read the new value
     * Long newCount = async.incrementColumnValue("users", "user123", "stats", "loginCount", 1, Durability.SYNC_WAL).get(); // returns the post-increment value
     *
     * // Typical: trade durability for throughput (skip the WAL)
     * Long fast = async.incrementColumnValue("users", "user123", "stats", "loginCount", 1, Durability.SKIP_WAL).get(); // returns the post-increment value
     *
     * // Negative: exceptions from the underlying call surface wrapped in ExecutionException
     * async.incrementColumnValue("badTable", "user123", "stats", "loginCount", 1, Durability.SYNC_WAL).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param rowKey the row key (will be converted to bytes)
     * @param family the column family name (as String)
     * @param qualifier the column qualifier name (as String)
     * @param amount the amount to increment by (can be negative for decrement)
     * @param durability the durability level for this operation
     * @return a {@link ContinuableFuture} containing the new value after the increment. Wraps
     *         {@link HBaseExecutor#incrementColumnValue(String, Object, String, String, long, Durability)}.
     * @see HBaseExecutor#incrementColumnValue(String, Object, String, String, long, Durability)
     * @see Durability
     */
    public ContinuableFuture<Long> incrementColumnValue(final String tableName, final Object rowKey, final String family, final String qualifier,
            final long amount, final Durability durability) {
        return asyncExecutor.execute(() -> hbaseExecutor.incrementColumnValue(tableName, rowKey, family, qualifier, amount, durability));
    }

    /**
     * Asynchronously increments a single column value by a specified amount, using byte-array
     * family and qualifier identifiers.
     *
     * <p>Atomically increments a specific column's long-encoded value by the given amount. If the
     * cell does not yet exist, it is initialised to {@code amount}. The returned
     * {@link ContinuableFuture} completes with the new value.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     * byte[] family = Bytes.toBytes("stats");
     * byte[] qualifier = Bytes.toBytes("loginCount");
     *
     * // Typical: increment a counter using byte-array identifiers
     * Long newCount = async.incrementColumnValue("users", "user123", family, qualifier, 1).get(); // returns the post-increment value
     *
     * // Edge: a negative amount decrements the counter
     * Long afterDecrement = async.incrementColumnValue("users", "user123", family, qualifier, -1).get(); // returns the decremented value
     *
     * // Negative: exceptions from the underlying call surface wrapped in ExecutionException
     * async.incrementColumnValue("badTable", "user123", family, qualifier, 1).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param rowKey the row key (will be converted to bytes)
     * @param family the column family name as a byte array
     * @param qualifier the column qualifier name as a byte array
     * @param amount the amount to increment by (can be negative for decrement)
     * @return a {@link ContinuableFuture} containing the new value after the increment. Wraps
     *         {@link HBaseExecutor#incrementColumnValue(String, Object, byte[], byte[], long)}.
     * @see HBaseExecutor#incrementColumnValue(String, Object, byte[], byte[], long)
     */
    public ContinuableFuture<Long> incrementColumnValue(final String tableName, final Object rowKey, final byte[] family, final byte[] qualifier,
            final long amount) {
        return asyncExecutor.execute(() -> hbaseExecutor.incrementColumnValue(tableName, rowKey, family, qualifier, amount));
    }

    /**
     * Asynchronously increments a single column value with the specified write durability, using
     * byte-array family and qualifier identifiers.
     *
     * <p>Atomically increments a specific column's long-encoded value by the given amount with
     * control over WAL durability. The returned {@link ContinuableFuture} completes with the new
     * value.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     * byte[] family = Bytes.toBytes("stats");
     * byte[] qualifier = Bytes.toBytes("loginCount");
     *
     * // Typical: durable increment using byte-array identifiers
     * Long newCount = async.incrementColumnValue("users", "user123", family, qualifier, 1, Durability.SYNC_WAL).get(); // returns the post-increment value
     *
     * // Typical: skip the WAL for higher throughput
     * Long fast = async.incrementColumnValue("users", "user123", family, qualifier, 1, Durability.SKIP_WAL).get(); // returns the post-increment value
     *
     * // Negative: exceptions from the underlying call surface wrapped in ExecutionException
     * async.incrementColumnValue("badTable", "user123", family, qualifier, 1, Durability.SYNC_WAL).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param rowKey the row key (will be converted to bytes)
     * @param family the column family name as a byte array
     * @param qualifier the column qualifier name as a byte array
     * @param amount the amount to increment by (can be negative for decrement)
     * @param durability the durability level for this operation
     * @return a {@link ContinuableFuture} containing the new value after the increment. Wraps
     *         {@link HBaseExecutor#incrementColumnValue(String, Object, byte[], byte[], long, Durability)}.
     * @see HBaseExecutor#incrementColumnValue(String, Object, byte[], byte[], long, Durability)
     * @see Durability
     */
    public ContinuableFuture<Long> incrementColumnValue(final String tableName, final Object rowKey, final byte[] family, final byte[] qualifier,
            final long amount, final Durability durability) {
        return asyncExecutor.execute(() -> hbaseExecutor.incrementColumnValue(tableName, rowKey, family, qualifier, amount, durability));
    }

    /**
     * Asynchronously obtains an RPC channel for invoking coprocessor endpoints on the region that
     * contains the specified row.
     *
     * <p>The returned {@link CoprocessorRpcChannel} can be used to execute custom server-side
     * logic registered on the region server hosting {@code rowKey}, enabling computation close to
     * the data. The returned {@link ContinuableFuture} completes with the channel that can then be
     * used for coprocessor method invocation.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Typical: obtain the channel for the region hosting "user123"
     * CoprocessorRpcChannel channel = async.coprocessorService("users", "user123").get(); // returns the RPC channel for that row's region
     * // ... build a service stub on the channel and invoke an endpoint method ...
     *
     * // Typical: use the channel once it is available, on the async pool
     * async.coprocessorService("users", "user123")
     *      .thenRunAsync(ch -> invokeEndpoint(ch)); // returns ContinuableFuture<Void>
     *
     * // Negative: exceptions from the underlying call surface wrapped in ExecutionException
     * async.coprocessorService("badTable", "user123").get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param rowKey the row key used to locate the region server / region whose channel is returned
     * @return a {@link ContinuableFuture} containing the {@code CoprocessorRpcChannel} for the
     *         row's region. Wraps {@link HBaseExecutor#coprocessorService(String, Object)}.
     * @see HBaseExecutor#coprocessorService(String, Object)
     * @see CoprocessorRpcChannel
     */
    public ContinuableFuture<CoprocessorRpcChannel> coprocessorService(final String tableName, final Object rowKey) {
        return asyncExecutor.execute(() -> hbaseExecutor.coprocessorService(tableName, rowKey));
    }

    /**
     * Asynchronously executes a coprocessor call across all regions in a row range.
     *
     * <p>Invokes a coprocessor service method on every region whose row range overlaps
     * {@code [startRowKey, endRowKey)}. The {@code callable} is executed on each region server,
     * and results are collected into a map keyed by region name bytes. This enables
     * distributed server-side processing across multiple regions. The returned
     * {@link ContinuableFuture} completes with the aggregated map once every region has
     * responded.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Typical: run a coprocessor endpoint on every region in [user1, user9) and collect per-region results.
     * // MyService is a generated protobuf Service; the Batch.Call invokes one of its methods on each region.
     * Batch.Call<MyService, Long> call = service -> service.countRows();
     * ContinuableFuture<Map<byte[], Long>> future = async.coprocessorService("users", MyService.class, "user1", "user9", call); // returns a ContinuableFuture of the per-region map
     * Map<byte[], Long> perRegion = future.get();                                                                               // returns one entry per region in the range
     *
     * // Typical: sum the per-region results once they arrive, on the async pool
     * async.coprocessorService("users", MyService.class, "user1", "user9", call)
     *      .thenCallAsync(map -> map.values().stream().mapToLong(Long::longValue).sum()); // returns ContinuableFuture<Long>
     *
     * // Negative: exceptions from the underlying call surface wrapped in ExecutionException
     * async.coprocessorService("badTable", MyService.class, "user1", "user9", call).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param <T> the coprocessor service type
     * @param <R> the return type from the coprocessor call
     * @param tableName the name of the HBase table
     * @param service the coprocessor service class
     * @param startRowKey the starting row key (inclusive) for the range
     * @param endRowKey the ending row key (exclusive) for the range
     * @param callable the callable to execute on each region
     * @return a {@link ContinuableFuture} containing a map from region name bytes to the result
     *         returned by {@code callable} for that region. Wraps
     *         {@link HBaseExecutor#coprocessorService(String, Class, Object, Object, Batch.Call)}.
     * @see HBaseExecutor#coprocessorService(String, Class, Object, Object, Batch.Call)
     * @see Batch.Call
     */
    public <T extends Service, R> ContinuableFuture<Map<byte[], R>> coprocessorService(final String tableName, final Class<T> service, final Object startRowKey,
            final Object endRowKey, final Batch.Call<T, R> callable) {
        return asyncExecutor.execute(() -> hbaseExecutor.coprocessorService(tableName, service, startRowKey, endRowKey, callable));
    }

    /**
     * Asynchronously executes a coprocessor call across all regions in a row range, delivering
     * each region's result to a callback as it becomes available.
     *
     * <p>Invokes a coprocessor service method on every region whose row range overlaps
     * {@code [startRowKey, endRowKey)}, with each region's result delivered to {@code callback} as
     * soon as it returns. This enables streaming processing of results rather than collecting them
     * all in memory. The returned {@link ContinuableFuture} completes with {@code null} once every
     * coprocessor invocation has finished.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Typical: stream each region's result into a callback as it arrives
     * Batch.Call<MyService, Long> call = service -> service.countRows();
     * AtomicLong total = new AtomicLong();
     * Batch.Callback<Long> callback = (region, result) -> total.addAndGet(result);
     * Object done = async.coprocessorService("users", MyService.class, "user1", "user9", call, callback).get(); // returns null when all regions have responded
     *
     * // Typical: run a follow-up only after every region has been processed
     * async.coprocessorService("users", MyService.class, "user1", "user9", call, callback)
     *      .thenRunAsync(() -> System.out.println("Total = " + total.get())); // returns ContinuableFuture<Void>
     *
     * // Negative: exceptions from the underlying call surface wrapped in ExecutionException
     * async.coprocessorService("badTable", MyService.class, "user1", "user9", call, callback).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param <T> the coprocessor service type
     * @param <R> the return type from the coprocessor call
     * @param tableName the name of the HBase table
     * @param service the coprocessor service class
     * @param startRowKey the starting row key (inclusive) for the range
     * @param endRowKey the ending row key (exclusive) for the range
     * @param callable the callable to execute on each region
     * @param callback the callback that receives each region's result
     * @return a {@link ContinuableFuture} that completes with {@code null} when all calls finish.
     *         Wraps {@link HBaseExecutor#coprocessorService(String, Class, Object, Object, Batch.Call, Batch.Callback)}.
     * @see HBaseExecutor#coprocessorService(String, Class, Object, Object, Batch.Call, Batch.Callback)
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
     * Asynchronously executes a batch coprocessor call across all regions in a row range using
     * Protocol Buffer messages.
     *
     * <p>Invokes the coprocessor method identified by {@code methodDescriptor} on every region
     * whose row range overlaps {@code [startRowKey, endRowKey)}, passing the supplied
     * {@code request} message and parsing each response against {@code responsePrototype}. The
     * returned {@link ContinuableFuture} completes with a map of region names (byte arrays) to their
     * corresponding response messages once every region has responded.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Typical: invoke a protobuf coprocessor method on every region in [user1, user9) and collect responses.
     * // methodDescriptor / request / responsePrototype come from your generated protobuf service.
     * Descriptors.MethodDescriptor methodDescriptor = MyService.getDescriptor().findMethodByName("count");
     * Message request = CountRequest.getDefaultInstance();
     * CountResponse responsePrototype = CountResponse.getDefaultInstance();
     * ContinuableFuture<Map<byte[], CountResponse>> future = async.batchCoprocessorService("users", methodDescriptor, request, "user1", "user9", responsePrototype); // returns a ContinuableFuture of the per-region response map
     * Map<byte[], CountResponse> perRegion = future.get();                                                                                                           // returns one response message per region in the range
     *
     * // Negative: exceptions from the underlying call surface wrapped in ExecutionException
     * async.batchCoprocessorService("badTable", methodDescriptor, request, "user1", "user9", responsePrototype).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param <R> the response message type
     * @param tableName the name of the HBase table
     * @param methodDescriptor the Protocol Buffer method descriptor for the coprocessor method
     * @param request the request message to send to each region
     * @param startRowKey the starting row key (inclusive) for the range
     * @param endRowKey the ending row key (exclusive) for the range
     * @param responsePrototype the prototype instance used to parse responses
     * @return a {@link ContinuableFuture} containing a map of region names (byte arrays) to their
     *         corresponding response messages. Wraps {@link HBaseExecutor#batchCoprocessorService(String, Descriptors.MethodDescriptor, Message, Object, Object, Message)}.
     * @see HBaseExecutor#batchCoprocessorService(String, Descriptors.MethodDescriptor, Message, Object, Object, Message)
     * @see Descriptors.MethodDescriptor
     */
    public <R extends Message> ContinuableFuture<Map<byte[], R>> batchCoprocessorService(final String tableName,
            final Descriptors.MethodDescriptor methodDescriptor, final Message request, final Object startRowKey, final Object endRowKey,
            final R responsePrototype) {
        return asyncExecutor
                .execute(() -> hbaseExecutor.batchCoprocessorService(tableName, methodDescriptor, request, startRowKey, endRowKey, responsePrototype));
    }

    /**
     * Asynchronously executes a batch coprocessor call across all regions in a row range with a
     * callback, using Protocol Buffer messages.
     *
     * <p>Invokes the coprocessor method identified by {@code methodDescriptor} on every region
     * whose row range overlaps {@code [startRowKey, endRowKey)}, with each region's response
     * delivered to {@code callback} as soon as it is received. This enables streaming processing
     * of results. The returned {@link ContinuableFuture} completes with {@code null} once every
     * coprocessor invocation has finished.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncHBaseExecutor async = hbaseExecutor.async();
     *
     * // Typical: stream each region's protobuf response into a callback as it arrives
     * Descriptors.MethodDescriptor methodDescriptor = MyService.getDescriptor().findMethodByName("count");
     * Message request = CountRequest.getDefaultInstance();
     * CountResponse responsePrototype = CountResponse.getDefaultInstance();
     * Batch.Callback<CountResponse> callback = (region, response) -> accumulate(response);
     * Object done = async.batchCoprocessorService("users", methodDescriptor, request, "user1", "user9", responsePrototype, callback).get(); // returns null when all regions have responded
     *
     * // Typical: run a follow-up only after every region has been processed
     * async.batchCoprocessorService("users", methodDescriptor, request, "user1", "user9", responsePrototype, callback)
     *      .thenRunAsync(() -> System.out.println("done")); // returns ContinuableFuture<Void>
     *
     * // Negative: exceptions from the underlying call surface wrapped in ExecutionException
     * async.batchCoprocessorService("badTable", methodDescriptor, request, "user1", "user9", responsePrototype, callback).get(); // throws InterruptedException, ExecutionException
     * }</pre>
     *
     * @param <R> the response message type
     * @param tableName the name of the HBase table
     * @param methodDescriptor the Protocol Buffer method descriptor for the coprocessor method
     * @param request the request message to send to each region
     * @param startRowKey the starting row key (inclusive) for the range
     * @param endRowKey the ending row key (exclusive) for the range
     * @param responsePrototype the prototype instance used to parse responses
     * @param callback the callback that receives each region's response
     * @return a {@link ContinuableFuture} that completes with {@code null} when all calls finish.
     *         Wraps {@link HBaseExecutor#batchCoprocessorService(String, Descriptors.MethodDescriptor, Message, Object, Object, Message, Batch.Callback)}.
     * @see HBaseExecutor#batchCoprocessorService(String, Descriptors.MethodDescriptor, Message, Object, Object, Message, Batch.Callback)
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
