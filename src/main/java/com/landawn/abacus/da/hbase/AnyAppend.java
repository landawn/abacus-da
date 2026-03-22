/*
 * Copyright (C) 2020 HaiYang Li
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

import static com.landawn.abacus.da.hbase.HBaseExecutor.toFamilyQualifierBytes;
import static com.landawn.abacus.da.hbase.HBaseExecutor.toRowBytes;
import static com.landawn.abacus.da.hbase.HBaseExecutor.toValueBytes;

import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.io.TimeRange;

import com.landawn.abacus.annotation.SuppressFBWarnings;

/**
 * A fluent builder wrapper for HBase {@link Append} operations that provides atomic value concatenation
 * capabilities with automatic type conversion and performance optimization. This class simplifies append
 * operations by handling byte array conversions and providing advanced features like time range filtering
 * and result control for efficient counter implementations and log aggregation scenarios.
 *
 * <p>HBase Append operations are atomic server-side operations that concatenate byte arrays to existing
 * cell values, making them ideal for implementing counters, logs, and other append-only data structures.
 * Unlike client-side read-modify-write operations, Append operations guarantee atomicity even under
 * concurrent access.</p>
 *
 * <h2>Usage Examples</h2>
 *
 * <h3>Basic Value Appending</h3>
 * <pre>{@code
 * // Append string values to create logs
 * AnyAppend logAppend = AnyAppend.of("user123")
 *                                .addColumn("logs", "activity", "login at " + new Date());
 *
 * // Append binary data for counters
 * AnyAppend counterAppend = AnyAppend.of("stats:daily")
 *                                    .addColumn("metrics", "pageviews", 1L)
 *                                    .addColumn("metrics", "sessions", 1L);
 *
 * // Multiple column appends in single operation
 * AnyAppend multiAppend = AnyAppend.of("user123")
 *                                  .addColumn("timeline", "posts", "New post at " + timestamp)
 *                                  .addColumn("stats", "post_count", 1L);
 * }</pre>
 *
 * <h3>Performance-Optimized Appends</h3>
 * <pre>{@code
 * // Disable result return for better performance
 * AnyAppend performanceAppend = AnyAppend.of("high_volume_key")
 *                                        .setReturnResults(false)
 *                                        .addColumn("data", "stream", newData);
 *
 * // Time-range filtering for partitioned data
 * long startOfDay = System.currentTimeMillis() / 86400000 * 86400000;
 * long endOfDay = startOfDay + 86400000;
 * AnyAppend timeRangeAppend = AnyAppend.of("daily_counter")
 *                                      .setTimeRange(startOfDay, endOfDay)
 *                                      .addColumn("metrics", "events", 1L);
 * }</pre>
 *
 * <h3>Advanced Construction Patterns</h3>
 * <pre>{@code
 * // Create from byte array for performance
 * byte[] rowKeyBytes = Bytes.toBytes("high_perf_row");
 * AnyAppend byteAppend = AnyAppend.of(rowKeyBytes)
 *                                 .addColumn("data", "values", newBytes);
 *
 * // Row key slicing for composite keys
 * AnyAppend sliceAppend = AnyAppend.of(compositeKey, 0, prefixLength)
 *                                  .addColumn("counters", "hits", 1L);
 *
 * // Copy and modify existing append
 * AnyAppend copiedAppend = AnyAppend.of(existingAppend.val())
 *                                   .addColumn("additional", "data", moreData);
 * }</pre>
 *
 * <h3>Key Features:</h3>
 * <ul>
 * <li><strong>Atomic Operations</strong>: Server-side atomic append operations for thread-safe data modification</li>
 * <li><strong>Type Safety</strong>: Automatic conversion from Java objects to byte arrays</li>
 * <li><strong>Performance Control</strong>: Optional result return for high-throughput scenarios</li>
 * <li><strong>Time Range Filtering</strong>: Efficient time-based append operations for partitioned data</li>
 * <li><strong>Counter Support</strong>: Ideal for implementing distributed counters and metrics</li>
 * <li><strong>Log Aggregation</strong>: Perfect for append-only log data structures</li>
 * <li><strong>Memory Efficiency</strong>: Server-side operation eliminates client-side data retrieval</li>
 * </ul>
 *
 * <h3>Common Use Cases:</h3>
 * <ul>
 * <li><strong>Distributed Counters</strong>: Page views, user sessions, event counting</li>
 * <li><strong>Activity Logs</strong>: User activity timelines, audit trails, system logs</li>
 * <li><strong>Message Queues</strong>: Append-only message accumulation</li>
 * <li><strong>Time Series Data</strong>: Sensor readings, metrics collection, monitoring data</li>
 * <li><strong>Social Feeds</strong>: News feeds, notification streams, activity updates</li>
 * <li><strong>Configuration Merging</strong>: Append-based configuration updates</li>
 * </ul>
 *
 * <h3>Performance Considerations:</h3>
 * <ul>
 * <li><strong>Result Control</strong>: Use {@code setReturnResults(false)} for write-heavy scenarios</li>
 * <li><strong>Time Range Optimization</strong>: Use time ranges for partitioned counter scenarios</li>
 * <li><strong>Batch Operations</strong>: Combine multiple appends in single operations when possible</li>
 * <li><strong>Row Key Design</strong>: Design keys to minimize hot-spotting in high-throughput scenarios</li>
 * <li><strong>Value Size</strong>: Keep appended values reasonably sized to avoid large cell accumulation</li>
 * <li><strong>Compaction Impact</strong>: Consider HBase compaction patterns for long-running append scenarios</li>
 * </ul>
 *
 * <h3>HBase Append Semantics:</h3>
 * <ul>
 * <li><strong>Atomicity</strong>: Operations are atomic at the server level, safe for concurrent access</li>
 * <li><strong>Ordering</strong>: Appends preserve order within a single column</li>
 * <li><strong>Existence</strong>: Creates new cells if they don't exist, appends to existing cells</li>
 * <li><strong>Versioning</strong>: Each append operation creates a new version with current timestamp</li>
 * <li><strong>Consistency</strong>: Immediately consistent within a region, eventually consistent across replicas</li>
 * </ul>
 *
 * @see Append
 * @see AnyMutation
 * @see HBaseExecutor#append(String, AnyAppend) 
 * @see <a href="http://hbase.apache.org/devapidocs/index.html">Apache HBase Java API Documentation</a>
 * @see org.apache.hadoop.hbase.client.Append
 */
public final class AnyAppend extends AnyMutation<AnyAppend> {

    private final Append append;

    AnyAppend(final Object rowKey) {
        super(new Append(toRowBytes(rowKey)));
        append = (Append) mutation;
    }

    AnyAppend(final byte[] rowKey) {
        super(new Append(rowKey));
        append = (Append) mutation;
    }

    AnyAppend(final byte[] rowKey, final int rowOffset, final int rowLength) {
        super(new Append(rowKey, rowOffset, rowLength));
        append = (Append) mutation;
    }

    AnyAppend(final byte[] rowKey, final long timestamp, final NavigableMap<byte[], List<Cell>> familyMap) {
        super(new Append(rowKey, timestamp, familyMap));
        append = (Append) mutation;
    }

    AnyAppend(final Append appendToCopy) {
        super(new Append(appendToCopy));
        append = (Append) mutation;
    }

    /**
     * Creates a new AnyAppend instance for the specified row key.
     *
     * <p>This factory method creates an append operation for the given row key. The row key
     * will be converted to bytes using HBase's standard conversion mechanisms. This is the
     * most commonly used factory method for creating append operations.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyAppend append = AnyAppend.of("user123");
     * append.addColumn("cf", "counter", 1L);
     * }</pre>
     *
     * @param rowKey the row key to append data to, will be converted to bytes automatically
     * @return a new AnyAppend instance configured for the specified row
     * @throws IllegalArgumentException if rowKey is null
     * @see #addColumn(String, String, Object)
     */
    public static AnyAppend of(final Object rowKey) {
        return new AnyAppend(rowKey);
    }

    /**
     * Creates a new AnyAppend instance for the specified byte array row key.
     *
     * <p>This factory method creates an append operation using a pre-computed byte array
     * as the row key. This is more efficient than the Object version when you already
     * have the row key in byte array format, as it avoids the conversion overhead.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * byte[] keyBytes = Bytes.toBytes("user123");
     * AnyAppend append = AnyAppend.of(keyBytes);
     * append.addColumn("cf", "counter", 1L);
     * }</pre>
     *
     * @param rowKey the row key as a byte array, must not be null or empty
     * @return a new AnyAppend instance configured for the specified row
     * @throws IllegalArgumentException if rowKey is null or empty
     * @see #of(Object)
     */
    public static AnyAppend of(final byte[] rowKey) {
        return new AnyAppend(rowKey);
    }

    /**
     * Creates a new AnyAppend instance using a portion of the specified byte array as the row key.
     *
     * <p>This factory method creates an append operation using a subset of a byte array
     * as the row key. This is useful when the row key is part of a larger byte array,
     * allowing you to specify exactly which portion to use without creating a new array.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * byte[] data = "prefix:user123:suffix".getBytes();
     * AnyAppend append = AnyAppend.of(data, 7, 7);   // Extract "user123"
     * append.addColumn("cf", "counter", 1L);
     * }</pre>
     *
     * @param rowKey the source byte array containing the row key
     * @param offset the starting position within the byte array (0-based)
     * @param length the number of bytes to use for the row key
     * @return a new AnyAppend instance configured for the specified row key portion
     * @throws IllegalArgumentException if rowKey is null, offset is negative, length is negative, or offset+length exceeds array bounds
     * @see #of(byte[])
     */
    public static AnyAppend of(final byte[] rowKey, final int offset, final int length) {
        return new AnyAppend(rowKey, offset, length);
    }

    /**
     * Creates a new AnyAppend instance with pre-configured data and timestamp.
     *
     * <p>This advanced factory method creates an append operation with a specific timestamp
     * and pre-built family map containing the data to append. This is typically used for
     * bulk operations or when reconstructing append operations from existing data structures.
     * The family map contains column families as keys and lists of cells as values.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * NavigableMap<byte[], List<Cell>> familyMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
     * List<Cell> cells = Arrays.asList(CellUtil.createCell(Bytes.toBytes("row"), ...));
     * familyMap.put(Bytes.toBytes("cf"), cells);
     *
     * AnyAppend append = AnyAppend.of(Bytes.toBytes("user123"), System.currentTimeMillis(), familyMap);
     * }</pre>
     *
     * @param rowKey the row key as a byte array, must not be null or empty
     * @param timestamp the timestamp for all append operations (milliseconds since epoch)
     * @param familyMap a map of column family names to lists of cells to append
     * @return a new AnyAppend instance configured with the specified data
     * @throws IllegalArgumentException if rowKey is null/empty, timestamp is negative, or familyMap is null
     * @see #of(Object)
     * @see org.apache.hadoop.hbase.Cell
     */
    public static AnyAppend of(final byte[] rowKey, final long timestamp, final NavigableMap<byte[], List<Cell>> familyMap) {
        return new AnyAppend(rowKey, timestamp, familyMap);
    }

    /**
     * Creates a new AnyAppend instance by copying an existing HBase Append operation.
     *
     * <p>This factory method creates a new AnyAppend instance that wraps a copy of the
     * provided HBase Append object. All configuration, data, and attributes from the
     * original append are preserved. This is useful for converting existing HBase
     * Append objects to the AnyAppend wrapper for additional functionality.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Append existingAppend = new Append(Bytes.toBytes("user123"));
     * existingAppend.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("counter"), Bytes.toBytes(1L));
     *
     * AnyAppend append = AnyAppend.of(existingAppend);
     * append.addColumn("cf", "visits", 1L);   // Add more data
     * }</pre>
     *
     * @param appendToCopy the existing HBase Append object to copy
     * @return a new AnyAppend instance that wraps a copy of the specified append
     * @throws IllegalArgumentException if appendToCopy is null
     * @see org.apache.hadoop.hbase.client.Append
     */
    public static AnyAppend of(final Append appendToCopy) {
        return new AnyAppend(appendToCopy);
    }

    /**
     * Returns the underlying HBase Append object for direct access to native HBase operations.
     *
     * <p>This method provides access to the wrapped HBase Append instance, allowing for advanced
     * operations not directly exposed by the AnyAppend fluent API. Use this method when you need
     * to access HBase-specific functionality or when integrating with existing HBase code.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyAppend anyAppend = AnyAppend.of("user123")
     *                                .addColumn("stats", "count", 1L);
     * Append hbaseAppend = anyAppend.val();
     * table.append(hbaseAppend);   // Use with native HBase API
     * }</pre>
     *
     * @return the underlying HBase Append object
     * @see Append
     */
    public Append val() {
        return append;
    }

    /**
     * Sets the TimeRange to be used on the Get operation for this append.
     * <p>
     * This is useful for when you have counters that only last for specific
     * periods of time (i.e., counters that are partitioned by time).  By setting
     * the range of valid times for this append, you can potentially gain
     * some performance with a more optimal Get operation.
     * Be careful adding the time range to this class as you will update the old cell if the
     * time range doesn't include the latest cells.
     * </p>
     * <p>
     * This range is used as [minStamp, maxStamp), meaning minStamp is inclusive
     * and maxStamp is exclusive.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Set time range for hourly partitioned counters
     * long hourStart = System.currentTimeMillis() / 3600000 * 3600000;
     * long hourEnd = hourStart + 3600000;
     * AnyAppend append = AnyAppend.of("hourly_counter")
     *                             .setTimeRange(hourStart, hourEnd)
     *                             .addColumn("stats", "events", 1L);
     * }</pre>
     *
     * @param minStamp minimum timestamp value, inclusive
     * @param maxStamp maximum timestamp value, exclusive
     * @return this AnyAppend instance for method chaining
     * @throws IllegalArgumentException if minStamp is negative, maxStamp is negative, or minStamp >= maxStamp
     * @see #getTimeRange()
     * @see org.apache.hadoop.hbase.io.TimeRange
     */
    public AnyAppend setTimeRange(final long minStamp, final long maxStamp) {
        append.setTimeRange(minStamp, maxStamp);

        return this;
    }

    /**
     * Returns the time range configuration for this append operation.
     *
     * <p>The time range specifies the minimum and maximum timestamps for cells that
     * should be considered during the append operation. Only cells with timestamps
     * within this range will be affected. If no time range has been set, this
     * method returns the default time range (all timestamps).</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyAppend append = AnyAppend.of("user123");
     * append.setTimeRange(startTime, endTime);
     * TimeRange range = append.getTimeRange();
     * }</pre>
     *
     * @return the configured TimeRange, or the default TimeRange if no specific range is set
     * @see #setTimeRange(long, long)
     * @see org.apache.hadoop.hbase.io.TimeRange
     */
    public TimeRange getTimeRange() {
        return append.getTimeRange();
    }

    /**
     * Sets whether to return results after the append operation.
     *
     * <p>By default, HBase Append operations return the updated values. However, in high-throughput
     * scenarios where the results are not needed, you can disable result return for better performance.
     * This is particularly useful for counters or log aggregation where you only care about the operation's
     * success and not the resulting values.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Disable result return for better performance
     * AnyAppend highThroughput = AnyAppend.of("counter_key")
     *                                     .addColumn("stats", "count", 1L)
     *                                     .setReturnResults(false);
     *
     * // Enable result return to verify the new value
     * AnyAppend withResults = AnyAppend.of("important_key")
     *                                  .addColumn("data", "value", someData)
     *                                  .setReturnResults(true);
     * }</pre>
     *
     * @param returnResults true to return results, {@code false} to disable result return
     * @return this AnyAppend instance for method chaining
     * @see #isReturnResults()
     */
    public AnyAppend setReturnResults(final boolean returnResults) {
        append.setReturnResults(returnResults);

        return this;
    }

    /**
     * Checks whether this append operation is configured to return results.
     *
     * <p>This method returns {@code true} if the append operation is set to return the updated values
     * after the operation. If false, the operation will not return results, optimizing performance
     * for scenarios where results are not needed.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyAppend append = AnyAppend.of("user123")
     *                             .addColumn("stats", "count", 1L)
     *                             .setReturnResults(false);
     * boolean returnsResults = append.isReturnResults();   // returns false
     * }</pre>
     *
     * @return {@code true} if results will be returned, {@code false} otherwise
     * @see #setReturnResults(boolean)
     */
    // This method makes public the superclass's protected method.
    public boolean isReturnResults() {
        return append.isReturnResults();
    }

    //    /**
    //     * Add the specified column and value to this Append operation.
    //     * @param family family name
    //     * @param qualifier column qualifier
    //     * @param value value to append to specified column
    //     * @return this
    //     * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
    //     *             Use {@link #addColumn(byte[], byte[], byte[])} instead
    //     */
    //    @Deprecated
    //    public AnyAppend add(byte[] family, byte[] qualifier, byte[] value) {
    //        append.add(family, qualifier, value);
    //
    //        return this;
    //    }

    /**
     * Adds the specified column and value to this append operation using byte array identifiers.
     *
     * <p>This method provides direct byte array access for appending data to a specific column.
     * The value will be atomically concatenated to the existing cell value at the server side.
     * This is more efficient than the String version when you already have the data in byte
     * array format, as it avoids conversion overhead.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * byte[] familyBytes = Bytes.toBytes("logs");
     * byte[] qualifierBytes = Bytes.toBytes("activity");
     * byte[] valueBytes = Bytes.toBytes("login_event");
     *
     * AnyAppend append = AnyAppend.of("user123")
     *                             .addColumn(familyBytes, qualifierBytes, valueBytes);
     * }</pre>
     *
     * @param family the column family name as a byte array, must not be null
     * @param qualifier the column qualifier as a byte array, must not be null
     * @param value the value to append to the specified column as a byte array
     * @return this AnyAppend instance for method chaining
     * @throws IllegalArgumentException if family or qualifier is null
     * @see #addColumn(String, String, Object)
     */
    public AnyAppend addColumn(final byte[] family, final byte[] qualifier, final byte[] value) {
        append.addColumn(family, qualifier, value);

        return this;
    }

    /**
     * Adds the specified column and value to this append operation using string identifiers.
     *
     * <p>This convenience method allows you to specify the column family, qualifier, and value
     * using strings and objects rather than byte arrays. The method automatically handles
     * the conversion from string/object to bytes using HBase's standard conversion mechanisms.
     * This is the most commonly used method for adding data to append operations.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyAppend append = AnyAppend.of("user123");
     * append.addColumn("stats", "login_count", 1L);
     * append.addColumn("profile", "last_login", new Date());
     * append.addColumn("settings", "theme", "dark");
     * }</pre>
     *
     * @param family the column family name as a string, must not be null or empty
     * @param qualifier the column qualifier as a string, must not be null or empty
     * @param value the value to append, will be automatically converted to bytes (can be null for delete)
     * @return this AnyAppend instance for method chaining
     * @throws IllegalArgumentException if family or qualifier is null or empty
     * @see #addColumn(byte[], byte[], byte[])
     */
    public AnyAppend addColumn(final String family, final String qualifier, final Object value) {
        append.addColumn(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier), toValueBytes(value));

        return this;
    }

    /**
     * Adds a pre-constructed Cell to this append operation.
     *
     * <p>This method allows you to add a fully constructed HBase Cell object to the
     * append operation. This provides maximum flexibility when you need precise control
     * over cell attributes such as timestamp, type, or when working with existing Cell
     * objects from other HBase operations. The cell must have the same row key as this
     * append operation.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyAppend append = AnyAppend.of("user123");
     *
     * Cell cell = CellUtil.createCell(
     *     Bytes.toBytes("user123"),
     *     Bytes.toBytes("stats"),
     *     Bytes.toBytes("counter"),
     *     System.currentTimeMillis(),
     *     Cell.Type.Put,
     *     Bytes.toBytes(1L)
     * );
     *
     * append.add(cell);
     * }</pre>
     *
     * @param cell the Cell object to add to this append operation, must not be null
     * @return this AnyAppend instance for method chaining
     * @throws IllegalArgumentException if cell is null or has a different row key
     * @see org.apache.hadoop.hbase.Cell
     * @see #addColumn(String, String, Object)
     */
    public AnyAppend add(final Cell cell) {
        append.add(cell);

        return this;
    }

    /**
     * Sets a custom attribute for this append operation.
     *
     * <p>Attributes are key-value pairs that can be attached to HBase operations
     * for custom processing, monitoring, or metadata purposes. These attributes
     * are available to coprocessors and custom filters during operation execution.
     * Common uses include operation tracing, custom validation, or feature flags.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyAppend append = AnyAppend.of("user123");
     * append.setAttribute("trace_id", Bytes.toBytes("trace-12345"));
     * append.setAttribute("priority", Bytes.toBytes("high"));
     * }</pre>
     *
     * @param name the attribute name/key, must not be null or empty
     * @param value the attribute value as a byte array, can be null
     * @return this AnyAppend instance for method chaining
     * @throws IllegalArgumentException if name is null or empty
     * @see #getAttribute(String)
     */
    public AnyAppend setAttribute(final String name, final byte[] value) {
        append.setAttribute(name, value);

        return this;
    }

    /**
     * Returns the hash code value for this AnyAppend instance.
     *
     * <p>The hash code is based on the underlying HBase Append object and is consistent
     * with the {@link #equals(Object)} method.</p>
     *
     * @return the hash code value for this AnyAppend
     * @see #equals(Object)
     */
    @Override
    public int hashCode() {
        return append.hashCode();
    }

    /**
     * Compares this AnyAppend instance with another object for equality.
     *
     * <p>Two AnyAppend instances are considered equal if they wrap equivalent HBase Append
     * operations. This comparison is based on the underlying Append object's equality.</p>
     *
     * @param obj the object to compare with
     * @return {@code true} if the specified object represents an equivalent append operation, {@code false} otherwise
     * @see #hashCode()
     */
    @SuppressFBWarnings
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof final AnyAppend other) {
            return append.equals(other.append);
        }

        return false;
    }

    /**
     * Returns a string representation of this AnyAppend instance.
     *
     * <p>The string representation is delegated to the underlying HBase Append object
     * and includes information about the row key, column families, qualifiers,
     * and other configuration settings.</p>
     *
     * @return a string representation of the append operation
     */
    @Override
    public String toString() {
        return append.toString();
    }
}
