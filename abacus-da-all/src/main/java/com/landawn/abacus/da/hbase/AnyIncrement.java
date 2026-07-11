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
import static com.landawn.abacus.da.hbase.HBaseExecutor.toRowKeyBytes;

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
     * {@link HBaseExecutor#toRowKeyBytes(Object)}.
     *
     * @param rowKey the row key for the increment operation
     * @throws NullPointerException if {@code rowKey} is {@code null} (its converted row bytes are
     *         {@code null}, which the underlying {@link Increment} constructor rejects)
     */
    AnyIncrement(final Object rowKey) {
        super(new Increment(toRowKeyBytes(rowKey)));
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
     * @param rowOffset the starting position within {@code rowKey} (0-based)
     * @param rowLength the number of bytes to use from {@code rowKey}
     */
    AnyIncrement(final byte[] rowKey, final int rowOffset, final int rowLength) {
        super(new Increment(rowKey, rowOffset, rowLength));
        increment = (Increment) mutation;
    }

    /**
     * Package-private constructor: prefer {@link #of(Object, int, int)}. Wraps a new HBase
     * {@link Increment} that uses a slice of the row key's byte representation as its row key. The
     * row key is converted to bytes via {@link HBaseExecutor#toRowKeyBytes(Object)} before slicing.
     *
     * @param rowKey the row key object whose byte representation will be sliced
     * @param rowOffset the starting position within the converted row-key bytes (0-based)
     * @param rowLength the number of bytes to use from the converted row-key bytes
     */
    AnyIncrement(final Object rowKey, final int rowOffset, final int rowLength) {
        super(new Increment(toRowKeyBytes(rowKey), rowOffset, rowLength));
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
     * pageViews.getRow();      // returns Bytes.toBytes("user123")
     * pageViews.hasFamilies(); // returns true
     *
     * // Create increment for multiple counters (same family => one family entry)
     * AnyIncrement metrics = AnyIncrement.of("daily_stats")
     *                                    .addColumn("counters", "logins", 1L)
     *                                    .addColumn("counters", "signups", 1L);
     * metrics.getFamilyMapOfLongs().size(); // returns 1
     *
     * // Edge: null row key is rejected by the underlying HBase Increment constructor
     * AnyIncrement.of((Object) null);       // throws NullPointerException
     * }</pre>
     *
     * @param rowKey the row key for the increment operation; automatically converted to bytes
     * @return a new AnyIncrement instance configured for the specified row
     * @throws NullPointerException if {@code rowKey} is {@code null} (its byte conversion yields
     *         {@code null}, which the {@link Increment} constructor rejects)
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
     * increment.getRow(); // returns keyBytes ("counter_row_123")
     *
     * // Edge: a null row key is rejected by the HBase Increment constructor
     * AnyIncrement.of((byte[]) null); // throws NullPointerException
     *
     * // Edge: an empty (zero-length) row key is rejected with IllegalArgumentException
     * AnyIncrement.of(new byte[0]);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param rowKey the row key for the increment operation as a byte array
     * @return a new AnyIncrement instance configured for the specified row
     * @throws NullPointerException if {@code rowKey} is {@code null}
     * @throws IllegalArgumentException if {@code rowKey} is empty (zero-length)
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
     * // Extract user ID from a composite key (use the first 7 bytes -> "user123")
     * byte[] compositeKey = Bytes.toBytes("user123session456");
     * AnyIncrement increment = AnyIncrement.of(compositeKey, 0, 7)
     *                                      .addColumn("counters", "sessions", 1L);
     * increment.getRow(); // returns Bytes.toBytes("user123")
     *
     * // Edge: a null row key is rejected with IllegalArgumentException ("Row buffer is null")
     * AnyIncrement.of((byte[]) null, 0, 1);   // throws IllegalArgumentException
     *
     * // Edge: a negative offset overflows the slice copy
     * AnyIncrement.of(compositeKey, -1, 3);   // throws ArrayIndexOutOfBoundsException
     * }</pre>
     *
     * @param rowKey the byte array containing the row key data
     * @param rowOffset the starting position within the rowKey array (0-based)
     * @param rowLength the number of bytes to use from the rowKey array
     * @return a new AnyIncrement instance configured for the partial row key
     * @throws IllegalArgumentException if {@code rowKey} is {@code null}, or if the resulting
     *         slice is empty or exceeds HBase's maximum row-key length
     * @throws ArrayIndexOutOfBoundsException if {@code rowOffset} or {@code rowLength} addresses bytes
     *         outside {@code rowKey} (for example a negative {@code rowOffset})
     * @see #of(byte[])
     */
    public static AnyIncrement of(final byte[] rowKey, final int rowOffset, final int rowLength) {
        return new AnyIncrement(rowKey, rowOffset, rowLength);
    }

    /**
     * Creates a new AnyIncrement instance using a portion of the row key object's byte representation.
     *
     * <p>Mirrors {@link AnyGet#of(Object, int, int)} / {@link AnyPut#of(Object, int, int)} /
     * {@link AnyDelete#of(Object, int, int)}: the row key is first converted to bytes via
     * {@link HBaseExecutor#toRowKeyBytes(Object)}, then the {@code [rowOffset, rowOffset + rowLength)}
     * slice of those bytes is used as the row key. This is useful for composite or fixed-width
     * row-key schemes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Extract user ID from a composite key (use the first 7 bytes -> "user123")
     * AnyIncrement increment = AnyIncrement.of("user123session456", 0, 7)
     *                                      .addColumn("counters", "sessions", 1L);
     * increment.getRow(); // returns Bytes.toBytes("user123")
     *
     * AnyIncrement.of((Object) null, 0, 1);   // throws IllegalArgumentException ("Row buffer is null")
     * AnyIncrement.of("abc", -1, 3);          // throws ArrayIndexOutOfBoundsException (negative offset)
     * }</pre>
     *
     * @param rowKey the row key object whose byte representation will be sliced
     * @param rowOffset the starting position within the converted row-key bytes (0-based)
     * @param rowLength the number of bytes to use for the row key
     * @return a new AnyIncrement instance configured for the partial row key
     * @throws IllegalArgumentException if {@code rowKey} converts to a {@code null} byte array, or if the
     *         resulting slice is empty or exceeds HBase's maximum row-key length
     * @throws ArrayIndexOutOfBoundsException if {@code rowOffset} or {@code rowLength} addresses bytes
     *         outside the converted row-key bytes (for example a negative {@code rowOffset})
     * @see #of(byte[], int, int)
     */
    public static AnyIncrement of(final Object rowKey, final int rowOffset, final int rowLength) {
        return new AnyIncrement(rowKey, rowOffset, rowLength);
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
     * NavigableMap<byte[], List<Cell>> familyMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
     * long timestamp = 1234L;
     * AnyIncrement increment = AnyIncrement.of(Bytes.toBytes("counter_row"), timestamp, familyMap);
     * increment.getRow();       // returns Bytes.toBytes("counter_row")
     * increment.getTimestamp(); // returns 1234L
     *
     * // Edge: an empty (zero-length) row key is rejected
     * AnyIncrement.of(new byte[0], timestamp, familyMap);                  // throws IllegalArgumentException
     *
     * // Edge: a null row key or null family map is rejected
     * AnyIncrement.of((byte[]) null, timestamp, familyMap);                // throws NullPointerException
     * AnyIncrement.of(Bytes.toBytes("r"), timestamp, (NavigableMap) null); // throws NullPointerException
     * }</pre>
     *
     * @param rowKey the row key for the increment operation as a byte array
     * @param timestamp the timestamp to apply to all cells in this increment operation
     * @param familyMap a pre-populated NavigableMap of column families to their Cell lists
     * @return a new AnyIncrement instance with the specified configuration
     * @throws IllegalArgumentException if {@code rowKey} is empty (zero-length)
     * @throws NullPointerException if {@code rowKey} or {@code familyMap} is {@code null}
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
     * Increment existingIncrement = new Increment(Bytes.toBytes("r"));
     * existingIncrement.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q"), 1L);
     *
     * AnyIncrement extended = AnyIncrement.of(existingIncrement)
     *                                     .addColumn("additional", "counter", 5L)
     *                                     .setReturnResults(true);
     * extended.val() == existingIncrement;            // false (a fresh copy is wrapped)
     * extended.getFamilyMapOfLongs().size();          // returns 2
     * existingIncrement.getFamilyMapOfLongs().size(); // returns 1 (source map is unchanged)
     *
     * // Edge: a null source increment is rejected
     * AnyIncrement.of((Increment) null);            // throws NullPointerException
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
     * hbaseIncrement.getRow();                 // returns Bytes.toBytes("user123")
     * anyIncrement.val() == hbaseIncrement;    // true (same wrapped instance every call)
     * table.increment(hbaseIncrement);         // Use with native HBase API
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
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyIncrement increment = AnyIncrement.of("row1");
     * Cell cell = CellUtil.createCell(Bytes.toBytes("row1"), Bytes.toBytes("cf"),
     *         Bytes.toBytes("q"), 0L, KeyValue.Type.Put.getCode(), Bytes.toBytes(1L));
     * AnyIncrement same = increment.add(cell); // succeeds: cell row matches this increment's row
     * same == increment;                       // true (returns this for chaining)
     * increment.hasFamilies();                 // returns true
     *
     * // Edge: a cell whose row key differs from this increment's row is rejected
     * Cell badCell = CellUtil.createCell(Bytes.toBytes("OTHER"), Bytes.toBytes("cf"),
     *         Bytes.toBytes("q"), 0L, KeyValue.Type.Put.getCode(), Bytes.toBytes(1L));
     * AnyIncrement.of("row1").add(badCell);    // throws IOException
     * }</pre>
     *
     * @param cell the {@link Cell} to add
     * @return this AnyIncrement instance, to allow fluent method chaining
     * @throws IOException if the cell's row key does not match this increment's row key
     * @throws IllegalArgumentException if the cell's family is null or empty
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
     * AnyIncrement increment = AnyIncrement.of("user123");
     * AnyIncrement same = increment.addColumn(familyBytes, qualifierBytes, 1L);
     * same == increment;         // true (returns this for chaining)
     * increment.hasFamilies();   // returns true
     *
     * // Edge: a null family is rejected
     * AnyIncrement.of("user123").addColumn((byte[]) null, qualifierBytes, 1L); // throws IllegalArgumentException
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
     * AnyIncrement anyIncrement = AnyIncrement.of("user123");
     *
     * // Increment page view counter (returns the same builder for chaining)
     * AnyIncrement same = anyIncrement.addColumn("stats", "pageViews", 1L);
     * same == anyIncrement; // true
     *
     * // Increment multiple counters
     * anyIncrement.addColumn("metrics", "sessions", 1L)
     *            .addColumn("metrics", "events", 5L)
     *            .addColumn("counters", "clicks", 3L);
     *
     * // Decrement: a negative amount is stored as-is
     * anyIncrement.addColumn("inventory", "stock", -1L);
     * anyIncrement.getFamilyMapOfLongs()
     *             .get(Bytes.toBytes("inventory"))
     *             .get(Bytes.toBytes("stock")); // returns -1L
     * }</pre>
     *
     * @param family the column-family name; converted to bytes via
     *               {@link HBaseExecutor#toFamilyQualifierBytes(String)}
     * @param qualifier the column-qualifier name; converted to bytes via
     *                  {@link HBaseExecutor#toFamilyQualifierBytes(String)}
     * @param amount the long delta to apply to the existing cell value (negative for decrement)
     * @return this AnyIncrement instance, to allow fluent method chaining
     * @throws IllegalArgumentException if {@code family} is null
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
     *                                      .setTimeRange(100L, 200L);
     * TimeRange range = increment.getTimeRange();
     * range.getMin(); // returns 100L
     * range.getMax(); // returns 200L
     *
     * // Edge: when no range is set, the default spans all timestamps
     * TimeRange def = AnyIncrement.of("row").getTimeRange();
     * def.getMin(); // returns 0L
     * def.getMax(); // returns Long.MAX_VALUE
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
     * // Increment counter for one hour only (returns the same builder for chaining)
     * long hourStart = 3600000L;
     * long hourEnd = hourStart + 3600000L;
     *
     * AnyIncrement hourlyCounter = AnyIncrement.of("hourly_stats")
     *                                          .setTimeRange(hourStart, hourEnd)
     *                                          .addColumn("metrics", "events", 1L);
     * hourlyCounter.getTimeRange().getMin(); // returns 3600000L
     * hourlyCounter.getTimeRange().getMax(); // returns 7200000L
     *
     * // Edge: maxStamp < minStamp is rejected
     * AnyIncrement.of("row").setTimeRange(200L, 100L); // throws IllegalArgumentException
     *
     * // Edge: a negative timestamp is rejected
     * AnyIncrement.of("row").setTimeRange(-1L, 100L);  // throws IllegalArgumentException
     * }</pre>
     *
     * @param minStamp minimum timestamp value, inclusive
     * @param maxStamp maximum timestamp value, exclusive
     * @return this AnyIncrement instance, to allow fluent method chaining
     * @throws IllegalArgumentException if {@code minStamp} or {@code maxStamp} is negative or if
     *         {@code maxStamp < minStamp}
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
     * fastCounter.isReturnResults(); // returns false
     *
     * // Standard increment with result retrieval for verification
     * AnyIncrement verifiableIncrement = AnyIncrement.of("important_counter")
     *                                                .addColumn("metrics", "value", 5L)
     *                                                .setReturnResults(true);
     * verifiableIncrement.isReturnResults(); // returns true
     *
     * // Returns the same builder for chaining
     * AnyIncrement inc = AnyIncrement.of("row");
     * inc.setReturnResults(true) == inc;     // true
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
     *
     * // After re-enabling, the flag reflects the latest setting
     * increment.setReturnResults(true);
     * increment.isReturnResults();                            // returns true
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
     * This is useful for inspecting the queued increment data programmatically.
     * The returned map is a snapshot rebuilt on each call; modifying it does not
     * affect this increment.
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
     * familyMap.size();                                                   // returns 1 (one family: "stats")
     * familyMap.get(Bytes.toBytes("stats")).get(Bytes.toBytes("views"));  // returns 5L
     * familyMap.get(Bytes.toBytes("stats")).get(Bytes.toBytes("clicks")); // returns 3L
     *
     * // Process the increment data programmatically
     * for (Map.Entry<byte[], NavigableMap<byte[], Long>> entry : familyMap.entrySet()) {
     *     String family = Bytes.toString(entry.getKey());
     *     for (Map.Entry<byte[], Long> colEntry : entry.getValue().entrySet()) {
     *         String qualifier = Bytes.toString(colEntry.getKey());
     *         Long amount = colEntry.getValue();
     *         System.out.println(family + ":" + qualifier + " += " + amount);
     *     }
     * }
     *
     * // Edge: an increment with no columns yields a non-null, empty map
     * AnyIncrement.of("row").getFamilyMapOfLongs().size();             // returns 0
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
     * increment.getAttribute("trace_id"); // returns Bytes.toBytes("trace-456")
     * increment.getAttribute("priority"); // returns Bytes.toBytes("high")
     *
     * // Edge: passing a null value clears (removes) the attribute
     * increment.setAttribute("priority", (byte[]) null);
     * increment.getAttribute("priority"); // returns null
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
     * <p>The hash code is derived from the underlying HBase Increment object &mdash; which HBase
     * computes from the <b>row key only</b> &mdash; and is therefore consistent with the
     * {@link #equals(Object)} method.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyIncrement increment = AnyIncrement.of("row");
     * increment.hashCode() == increment.val().hashCode(); // true (delegates to the wrapped Increment)
     *
     * // Increments with the same row key share the same hash code (columns are not considered)
     * AnyIncrement a = AnyIncrement.of("row").addColumn("cf", "q", 1L);
     * AnyIncrement b = AnyIncrement.of("row").addColumn("cf", "q", 1L);
     * a.equals(b);                         // true
     * a.hashCode() == b.hashCode();        // true
     * }</pre>
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
     * <p>Two AnyIncrement instances are considered equal if their underlying {@link Increment}
     * objects are equal. HBase's {@code Increment.equals(Object)} compares the <b>row key only</b>
     * &mdash; the queued column families, qualifiers, and increment amounts are <em>not</em>
     * considered &mdash; so two increments on the same row are equal even when they hold different
     * column increments, and two on different rows are unequal even when their columns are identical.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyIncrement increment = AnyIncrement.of("row");
     * increment.equals(increment);                 // returns true (same instance)
     *
     * AnyIncrement a = AnyIncrement.of("row").addColumn("cf", "q", 1L);
     * AnyIncrement b = AnyIncrement.of("row").addColumn("cf", "q", 1L);
     * a.equals(b);                                 // returns true (same row key; columns are ignored)
     *
     * // Edge: a non-AnyIncrement object and null are never equal
     * increment.equals("not an AnyIncrement");     // returns false
     * increment.equals(null);                      // returns false
     * }</pre>
     *
     * @param obj the object to compare with
     * @return {@code true} if the specified object is an {@code AnyIncrement} whose underlying {@code Increment} is equal (same row key), {@code false} otherwise
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
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyIncrement increment = AnyIncrement.of("row").addColumn("cf", "q", 1L);
     * String text = increment.toString();
     * text.equals(increment.val().toString()); // returns true (delegates to the wrapped Increment)
     * // text contains the row key, families and qualifiers, e.g. "row=row, ..."
     * }</pre>
     *
     * @return a string representation of the increment operation
     */
    @Override
    public String toString() {
        return increment.toString();
    }
}
