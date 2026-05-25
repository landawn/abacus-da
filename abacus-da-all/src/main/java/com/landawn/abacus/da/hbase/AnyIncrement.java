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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.io.TimeRange;

import com.landawn.abacus.annotation.SuppressFBWarnings;

/**
 * Wrapper around HBase's {@link Increment} that exposes the same fluent / type-converting API as
 * the other {@code AnyMutation} subclasses. Each {@link #addColumn} call queues an atomic counter
 * increment that the region server applies in a single read-modify-write — without needing the
 * client to hold a lock.
 *
 * <p>Key features:</p>
 * <ul>
 *   <li>Atomic server-side counter increments (or decrements when a negative amount is supplied)</li>
 *   <li>String family / qualifier names converted to bytes via {@link HBaseExecutor}</li>
 *   <li>Optional {@link TimeRange} for the read that precedes the increment</li>
 *   <li>Optional {@link #setReturnResults(boolean)} to skip returning the post-increment values</li>
 *   <li>Fluent API: every setter returns {@code this}</li>
 * </ul>
 *
 * <h2>Usage Examples</h2>
 * <pre>{@code
 * AnyIncrement increment = AnyIncrement.of("user123")
 *     .addColumn("stats", "page_views", 1L)
 *     .addColumn("stats", "login_count", 1L)
 *     .setReturnResults(true);
 * }</pre>
 *
 * @see <a href="http://hbase.apache.org/devapidocs/index.html">Apache HBase Java API Documentation</a>
 * @see org.apache.hadoop.hbase.client.Increment
 * @see AnyMutation
 */
public final class AnyIncrement extends AnyMutation<AnyIncrement> {

    private final Increment increment;

    /**
     * Package-private constructor: prefer {@link #of(Object)}. Wraps a new HBase {@link Increment}
     * for the given row key. The row key is converted to bytes via
     * {@link HBaseExecutor#toRowBytes(Object)}.
     *
     * @param rowKey the row key for the increment operation
     * @throws IllegalArgumentException if {@code rowKey} is {@code null}
     */
    AnyIncrement(final Object rowKey) {
        super(new Increment(toRowBytes(rowKey)));
        increment = (Increment) mutation;
    }

    /**
     * Package-private constructor: prefer {@link #of(byte[])}. Wraps a new HBase {@link Increment}
     * for the given byte-array row key.
     *
     * @param rowKey the row key for the increment operation, as a byte array
     */
    AnyIncrement(final byte[] rowKey) {
        super(new Increment(rowKey));
        increment = (Increment) mutation;
    }

    /**
     * Package-private constructor: prefer {@link #of(byte[], int, int)}. Wraps a new HBase
     * {@link Increment} that uses a slice of {@code rowKey} as its row key.
     *
     * @param rowKey the byte array containing the row key data
     * @param offset the starting position within {@code rowKey} (0-based)
     * @param length the number of bytes to use from {@code rowKey}
     */
    AnyIncrement(final byte[] rowKey, final int offset, final int length) {
        super(new Increment(rowKey, offset, length));
        increment = (Increment) mutation;
    }

    /**
     * Package-private constructor: prefer {@link #of(byte[], long, NavigableMap)}. Wraps a new
     * HBase {@link Increment} pre-populated with the given family map and timestamp.
     *
     * @param rowKey the row key as a byte array
     * @param timestamp the timestamp to apply to every cell in this increment
     * @param familyMap a pre-populated map of column families to their cells
     */
    AnyIncrement(final byte[] rowKey, final long timestamp, final NavigableMap<byte[], List<Cell>> familyMap) {
        super(new Increment(rowKey, timestamp, familyMap));
        increment = (Increment) mutation;
    }

    /**
     * Package-private constructor: prefer {@link #of(Increment)}. Wraps a fresh copy of an
     * existing HBase {@link Increment}, so subsequent modifications do not touch the original.
     *
     * @param incrementToCopy the existing {@link Increment} to copy
     */
    AnyIncrement(final Increment incrementToCopy) {
        super(new Increment(incrementToCopy));
        increment = (Increment) mutation;
    }

    /**
     * Creates a new AnyIncrement instance for the specified row key.
     * <p>
     * This is the primary factory method for creating increment operations. The row key
     * will be automatically converted to the appropriate byte array format for HBase operations.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Create increment for page view counter
     * AnyIncrement pageViews = AnyIncrement.of("user123")
     *                                      .addColumn("stats", "page_views", 1L);
     *
     * // Create increment for multiple counters
     * AnyIncrement metrics = AnyIncrement.of("daily_stats")
     *                                    .addColumn("counters", "logins", 1L)
     *                                    .addColumn("counters", "signups", 1L);
     * }</pre>
     *
     * @param rowKey the row key for the increment operation; automatically converted to bytes
     * @return a new AnyIncrement instance configured for the specified row
     * @throws IllegalArgumentException if rowKey is null
     * @see #of(byte[])
     * @see #addColumn(String, String, long)
     */
    public static AnyIncrement of(final Object rowKey) {
        return new AnyIncrement(rowKey);
    }

    /**
     * Creates a new AnyIncrement instance for the specified byte array row key.
     * <p>
     * Use this method when you already have the row key as a byte array and want
     * to avoid additional conversion overhead. This is more efficient for high-performance
     * scenarios where row keys are pre-converted.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * byte[] keyBytes = Bytes.toBytes("counter_row_123");
     * AnyIncrement increment = AnyIncrement.of(keyBytes)
     *                                      .addColumn("metrics", "hits", 1L);
     * }</pre>
     *
     * @param rowKey the row key for the increment operation as a byte array
     * @return a new AnyIncrement instance configured for the specified row
     * @throws IllegalArgumentException if rowKey is null
     * @see #of(Object)
     */
    public static AnyIncrement of(final byte[] rowKey) {
        return new AnyIncrement(rowKey);
    }

    /**
     * Creates a new AnyIncrement instance for a subset of the specified byte array row key.
     * <p>
     * This method is useful for composite row keys where you only want to use a portion
     * of the byte array as the actual row key. This can be helpful for prefix-based
     * row key schemes or when working with fixed-width row key formats.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Extract user ID from composite key
     * byte[] compositeKey = buildCompositeKey("user123", "session456");
     * AnyIncrement increment = AnyIncrement.of(compositeKey, 0, 7)  // "user123"
     *                                      .addColumn("counters", "sessions", 1L);
     * }</pre>
     *
     * @param rowKey the byte array containing the row key data
     * @param offset the starting position within the rowKey array (0-based)
     * @param length the number of bytes to use from the rowKey array
     * @return a new AnyIncrement instance configured for the partial row key
     * @throws IllegalArgumentException if rowKey is null, offset is negative, or length is invalid
     * @see #of(byte[])
     */
    public static AnyIncrement of(final byte[] rowKey, final int offset, final int length) {
        return new AnyIncrement(rowKey, offset, length);
    }

    /**
     * Creates a new AnyIncrement instance with a specific timestamp and pre-populated family map.
     * <p>
     * This advanced factory method is useful for reconstructing increment operations from
     * existing data or for scenarios where you need precise control over the increment
     * structure and timing. The family map should contain the column families and their
     * respective Cell objects with the increment values.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * NavigableMap<byte[], List<Cell>> familyMap = buildIncrementFamilyMap();
     * long timestamp = System.currentTimeMillis();
     * AnyIncrement increment = AnyIncrement.of(
     *     Bytes.toBytes("counter_row"),
     *     timestamp,
     *     familyMap
     * );
     * }</pre>
     *
     * @param rowKey the row key for the increment operation as a byte array
     * @param timestamp the timestamp to apply to all cells in this increment operation
     * @param familyMap a pre-populated NavigableMap of column families to their Cell lists
     * @return a new AnyIncrement instance with the specified configuration
     * @throws IllegalArgumentException if rowKey is null
     * @see #of(Increment)
     */
    public static AnyIncrement of(final byte[] rowKey, final long timestamp, final NavigableMap<byte[], List<Cell>> familyMap) {
        return new AnyIncrement(rowKey, timestamp, familyMap);
    }

    /**
     * Creates a new AnyIncrement instance by copying an existing HBase Increment object.
     * <p>
     * Delegates to {@link Increment#Increment(Increment)}, which copies the row, timestamp,
     * time range, and the family-to-cells map structure (the map and per-family
     * {@code List<Cell>} are new collections, but the {@link Cell} instances themselves are
     * shared with the source). Subsequent {@code addColumn} calls on the returned wrapper
     * therefore do not mutate the source increment.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Increment existingIncrement = buildStandardIncrement();
     * AnyIncrement extended = AnyIncrement.of(existingIncrement)
     *                                     .addColumn("additional", "counter", 5L)
     *                                     .setReturnResults(true);
     * }</pre>
     *
     * @param incrementToCopy the HBase Increment object to copy; must not be {@code null}
     * @return a new AnyIncrement instance backed by a fresh Increment copied from {@code incrementToCopy}
     * @throws NullPointerException if {@code incrementToCopy} is {@code null}
     * @see Increment
     */
    public static AnyIncrement of(final Increment incrementToCopy) {
        return new AnyIncrement(incrementToCopy);
    }

    /**
     * Returns the underlying HBase Increment object for direct access to native HBase operations.
     *
     * <p>This method provides access to the wrapped HBase Increment instance, allowing for advanced
     * operations not directly exposed by the AnyIncrement fluent API. Use this method when you need
     * to access HBase-specific functionality or when integrating with existing HBase code.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyIncrement anyIncrement = AnyIncrement.of("user123")
     *                                         .addColumn("stats", "count", 1L);
     * Increment hbaseIncrement = anyIncrement.val();
     * table.increment(hbaseIncrement);   // Use with native HBase API
     * }</pre>
     *
     * @return the underlying HBase Increment object
     * @see Increment
     */
    public Increment val() {
        return increment;
    }

    /**
     * Adds a pre-constructed {@link Cell} carrying the per-column increment amount. Useful when
     * an existing {@code Cell} (e.g. from an {@link org.apache.hadoop.hbase.CellScanner}) already
     * encodes the desired increment; for ordinary use prefer
     * {@link #addColumn(String, String, long)}.
     *
     * <p>The cell's row key must match this increment's row key; HBase's {@link Increment#add(Cell)}
     * throws an {@link IOException} otherwise.</p>
     *
     * @param cell the {@link Cell} to add
     * @return this AnyIncrement instance, to allow fluent method chaining
     * @throws IOException if the cell's row key does not match this increment's row key, or if
     *         the cell is otherwise rejected by HBase
     * @see Cell
     * @see #addColumn(String, String, long)
     */
    public AnyIncrement add(final Cell cell) throws IOException {
        increment.add(cell);

        return this;
    }

    /**
     * Adds a column increment operation using byte array identifiers.
     * <p>
     * This method specifies an increment operation for a specific column identified by
     * its family and qualifier as byte arrays. The specified amount will be atomically
     * added to the existing value in the column. If the column doesn't exist, it will
     * be created with the increment amount as its initial value.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * byte[] familyBytes = Bytes.toBytes("counters");
     * byte[] qualifierBytes = Bytes.toBytes("page_views");
     *
     * AnyIncrement increment = AnyIncrement.of("user123")
     *                                      .addColumn(familyBytes, qualifierBytes, 1L);
     * }</pre>
     *
     * @param family the column-family name as a byte array; must not be {@code null}
     * @param qualifier the column-qualifier name as a byte array
     * @param amount the long delta to apply to the existing cell value (negative for decrement)
     * @return this AnyIncrement instance, to allow fluent method chaining
     * @throws IllegalArgumentException if {@code family} is {@code null}
     * @see #addColumn(String, String, long)
     */
    public AnyIncrement addColumn(final byte[] family, final byte[] qualifier, final long amount) {
        increment.addColumn(family, qualifier, amount);

        return this;
    }

    /**
     * Adds a column increment operation using string identifiers.
     * <p>
     * This is the most commonly used method for adding increment operations. The family
     * and qualifier strings are automatically converted to byte arrays. The specified amount
     * will be atomically added to the existing value in the column, making this ideal for
     * implementing counters, statistics, and other numeric aggregations.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Increment page view counter
     * anyIncrement.addColumn("stats", "pageViews", 1L);
     *
     * // Increment multiple counters
     * anyIncrement.addColumn("metrics", "sessions", 1L)
     *            .addColumn("metrics", "events", 5L)
     *            .addColumn("counters", "clicks", 3L);
     *
     * // Decrement (negative increment)
     * anyIncrement.addColumn("inventory", "stock", -1L);
     * }</pre>
     *
     * @param family the column-family name; converted to bytes via
     *               {@link HBaseExecutor#toFamilyQualifierBytes(String)}
     * @param qualifier the column-qualifier name; converted to bytes via
     *                  {@link HBaseExecutor#toFamilyQualifierBytes(String)}
     * @param amount the long delta to apply to the existing cell value (negative for decrement)
     * @return this AnyIncrement instance, to allow fluent method chaining
     * @see #addColumn(byte[], byte[], long)
     */
    public AnyIncrement addColumn(final String family, final String qualifier, final long amount) {
        increment.addColumn(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier), amount);

        return this;
    }

    /**
     * Returns the TimeRange currently set for this increment operation.
     * <p>
     * The TimeRange specifies the time window that will be used during the Get operation
     * that precedes the increment. This allows for time-partitioned counter scenarios
     * where you only want to increment based on existing values within a specific time range.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyIncrement increment = AnyIncrement.of("counter_row")
     *                                      .setTimeRange(startTime, endTime);
     * TimeRange range = increment.getTimeRange();
     * long min = range.getMin();
     * long max = range.getMax();
     * }</pre>
     *
     * @return the current TimeRange for this increment, or the default TimeRange if no specific range is set
     * @see #setTimeRange(long, long)
     * @see TimeRange
     */
    public TimeRange getTimeRange() {
        return increment.getTimeRange();
    }

    /**
     * Sets the TimeRange used by the pre-increment Get that HBase performs to read the existing
     * cell value before applying the delta. Useful for time-partitioned counters where only
     * recent values should be considered.
     *
     * <p><strong>Caution:</strong> if the time range does not cover the latest cell, the server
     * will compute the new value from an older version, effectively basing the increment on
     * stale data.</p>
     *
     * <p>The range is half-open: {@code [minStamp, maxStamp)} — {@code minStamp} is inclusive
     * and {@code maxStamp} is exclusive.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Increment counter for current hour only
     * long hourStart = System.currentTimeMillis() / 3600000 * 3600000;
     * long hourEnd = hourStart + 3600000;
     *
     * AnyIncrement hourlyCounter = AnyIncrement.of("hourly_stats")
     *                                          .setTimeRange(hourStart, hourEnd)
     *                                          .addColumn("metrics", "events", 1L);
     * }</pre>
     *
     * @param minStamp minimum timestamp value, inclusive
     * @param maxStamp maximum timestamp value, exclusive
     * @return this AnyIncrement instance, to allow fluent method chaining
     * @throws IllegalArgumentException if {@code minStamp} or {@code maxStamp} is negative or if
     *         {@code minStamp >= maxStamp} (wraps the underlying {@link IOException})
     * @see #getTimeRange()
     * @see TimeRange
     */
    public AnyIncrement setTimeRange(final long minStamp, final long maxStamp) {
        try {
            increment.setTimeRange(minStamp, maxStamp);
        } catch (final IOException e) {
            throw new IllegalArgumentException(e);
        }

        return this;
    }

    /**
     * Configures whether the increment operation should return the updated cell values.
     *
     * <p>By default, HBase increment operations return a Result containing the new values after
     * the increment is applied. For high-throughput scenarios where you don't need the result values,
     * setting this to false can improve performance by reducing network bandwidth and server-side
     * processing overhead.</p>
     *
     * <p><strong>Performance Benefits of returnResults=false:</strong></p>
     * <ul>
     * <li>Reduced network bandwidth usage</li>
     * <li>Lower server-side memory consumption</li>
     * <li>Faster operation completion</li>
     * <li>Better performance for write-heavy counter scenarios</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // High-throughput counter without result retrieval
     * AnyIncrement fastCounter = AnyIncrement.of("counter_key")
     *                                        .addColumn("stats", "hits", 1L)
     *                                        .setReturnResults(false);
     *
     * // Standard increment with result retrieval for verification
     * AnyIncrement verifiableIncrement = AnyIncrement.of("important_counter")
     *                                                .addColumn("metrics", "value", 5L)
     *                                                .setReturnResults(true);
     * }</pre>
     *
     * @param returnResults {@code true} to return the post-increment values (HBase's historical
     *                      default for {@link Increment}); {@code false} to skip the response
     *                      payload and improve throughput
     * @return this AnyIncrement instance, to allow fluent method chaining
     * @see #isReturnResults()
     */
    public AnyIncrement setReturnResults(final boolean returnResults) {
        increment.setReturnResults(returnResults);

        return this;
    }

    /**
     * Returns whether this increment operation is configured to return results.
     * <p>
     * When return results is enabled (true), the increment operation will return
     * a Result containing the new values after the increment. When disabled (false),
     * the operation returns faster by skipping the result retrieval, which can
     * improve performance for write-heavy scenarios where you don't need the result values.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyIncrement increment = AnyIncrement.of("counter")
     *                                      .addColumn("stats", "count", 1L)
     *                                      .setReturnResults(false);
     * boolean returnsResults = increment.isReturnResults();   // returns false
     * }</pre>
     *
     * @return {@code true} if the increment will return results, {@code false} otherwise
     * @see #setReturnResults(boolean)
     */
    public boolean isReturnResults() {

        return increment.isReturnResults();
    }

    /**
     * Checks if this increment operation has any column families specified.
     * <p>
     * Returns true if at least one column family has been added to this increment through
     * the {@code addColumn} methods. An increment operation must have at least one column
     * family specified to be valid for execution.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyIncrement increment1 = AnyIncrement.of("row1");
     * boolean has1 = increment1.hasFamilies();   // returns false
     *
     * AnyIncrement increment2 = AnyIncrement.of("row2")
     *                                       .addColumn("stats", "count", 1L);
     * boolean has2 = increment2.hasFamilies();   // returns true
     * }</pre>
     *
     * @return {@code true} if column families have been added to this increment, {@code false} otherwise
     * @see #addColumn(String, String, long)
     * @see #getFamilyMapOfLongs()
     */
    public boolean hasFamilies() {
        return increment.hasFamilies();
    }

    /**
     * Returns a map representation of this increment operation organized by families and qualifiers.
     * <p>
     * This method provides access to the increment data in a structured format where
     * the outer map keys are column family names (as byte arrays) and the values are
     * NavigableMap instances containing qualifier-to-increment-value mappings.
     * This is useful for inspecting or manipulating the increment data programmatically.
     * </p>
     *
     * <p><b>Structure:</b></p>
     * <pre>{@code
     * Map<family_bytes, Map<qualifier_bytes, Long_increment_value>>
     * }</pre>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyIncrement increment = AnyIncrement.of("user123")
     *                                      .addColumn("stats", "views", 5L)
     *                                      .addColumn("stats", "clicks", 3L);
     *
     * Map<byte[], NavigableMap<byte[], Long>> familyMap = increment.getFamilyMapOfLongs();
     * // Process the increment data programmatically
     * for (Map.Entry<byte[], NavigableMap<byte[], Long>> entry : familyMap.entrySet()) {
     *     String family = Bytes.toString(entry.getKey());
     *     for (Map.Entry<byte[], Long> colEntry : entry.getValue().entrySet()) {
     *         String qualifier = Bytes.toString(colEntry.getKey());
     *         Long amount = colEntry.getValue();
     *         System.out.println(family + ":" + qualifier + " += " + amount);
     *     }
     * }
     * }</pre>
     *
     * @return a Map of column families to their qualifier-value mappings; never null but may be empty
     * @see #hasFamilies()
     * @see #addColumn(String, String, long)
     */
    public Map<byte[], NavigableMap<byte[], Long>> getFamilyMapOfLongs() {
        return increment.getFamilyMapOfLongs();
    }

    /**
     * Sets a custom attribute on this increment using a raw byte-array value. This overload
     * bypasses {@link HBaseExecutor#toValueBytes(Object)} and stores the supplied array directly
     * on the wrapped HBase {@link Increment}; a {@code null} value clears the attribute.
     *
     * <p>For a {@code byte[]} argument, this is observationally equivalent to
     * {@link AnyOperationWithAttributes#setAttribute(String, Object)} (since {@code toValueBytes}
     * returns {@code byte[]} arguments as-is), but its presence avoids the {@code Object}-overload
     * dispatch path and makes the byte-array intent explicit at the call site.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyIncrement increment = AnyIncrement.of("user123")
     *                                      .addColumn("stats", "count", 1L)
     *                                      .setAttribute("trace_id", Bytes.toBytes("trace-456"))
     *                                      .setAttribute("priority", Bytes.toBytes("high"));
     * }</pre>
     *
     * @param name the attribute name
     * @param value the attribute value as a raw byte array; may be {@code null} to clear it
     * @return this AnyIncrement instance, to allow fluent method chaining
     * @see #getAttribute(String)
     * @see #getAttributesMap()
     * @see AnyOperationWithAttributes#setAttribute(String, Object)
     */
    public AnyIncrement setAttribute(final String name, final byte[] value) {
        increment.setAttribute(name, value);

        return this;
    }

    /**
     * Returns the hash code value for this AnyIncrement instance.
     *
     * <p>The hash code is based on the underlying HBase Increment object and is consistent
     * with the {@link #equals(Object)} method.</p>
     *
     * @return the hash code value for this AnyIncrement
     * @see #equals(Object)
     */
    @SuppressWarnings("deprecation")
    @Override
    public int hashCode() {
        return increment.hashCode();
    }

    /**
     * Compares this AnyIncrement instance with another object for equality.
     *
     * <p>Two AnyIncrement instances are considered equal if they wrap equivalent HBase Increment
     * operations. This comparison is based on the underlying Increment object's equality.</p>
     *
     * @param obj the object to compare with
     * @return {@code true} if the specified object represents an equivalent increment operation, {@code false} otherwise
     * @see #hashCode()
     */
    @SuppressFBWarnings
    @SuppressWarnings("deprecation")
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof final AnyIncrement other) {
            return increment.equals(other.increment);
        }

        return false;
    }

    /**
     * Returns a string representation of this AnyIncrement instance.
     *
     * <p>The string representation is delegated to the underlying HBase Increment object
     * and includes information about the row key, column families, qualifiers,
     * and other configuration settings.</p>
     *
     * @return a string representation of the increment operation
     */
    @Override
    public String toString() {
        return increment.toString();
    }
}
