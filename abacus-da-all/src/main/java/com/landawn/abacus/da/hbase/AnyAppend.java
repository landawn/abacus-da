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
 * Fluent builder wrapper for HBase {@link Append} operations. Each {@link #addColumn} call queues
 * a byte-array concatenation that the region server applies atomically as a single
 * read-modify-write — the supplied bytes are appended verbatim to the end of the cell's existing
 * value, with no parsing, addition, or other interpretation.
 *
 * <p><strong>Append vs. Increment:</strong> {@code Append} performs a bytewise concatenation; it
 * is <em>not</em> a counter operation. To atomically add to a numeric value use
 * {@link AnyIncrement} instead. For example, appending {@code Bytes.toBytes(1L)} to a cell that
 * already contains {@code Bytes.toBytes(1L)} produces a 16-byte value whose first 8 bytes decode to
 * {@code 1} — it does not produce {@code 2}.</p>
 *
 * <h2>Usage Examples</h2>
 *
 * <h3>Basic Value Appending</h3>
 * <pre>{@code
 * // Append string fragments to build a log entry
 * AnyAppend logAppend = AnyAppend.of("user123")
 *                                .addColumn("logs", "activity", "login at " + new Date());
 *
 * // Append multiple columns in a single round-trip
 * AnyAppend multiAppend = AnyAppend.of("user123")
 *                                  .addColumn("timeline", "posts", "New post at " + timestamp)
 *                                  .addColumn("audit", "trail", "ACTION:POST_CREATED;");
 * }</pre>
 *
 * <h3>Performance-Optimized Appends</h3>
 * <pre>{@code
 * // Skip the response payload for higher throughput
 * AnyAppend performanceAppend = AnyAppend.of("high_volume_key")
 *                                        .setReturnResults(false)
 *                                        .addColumn("data", "stream", newData);
 *
 * // Restrict the pre-append read to a time range
 * long startOfDay = System.currentTimeMillis() / 86400000 * 86400000;
 * long endOfDay = startOfDay + 86400000;
 * AnyAppend timeRangeAppend = AnyAppend.of("daily_log")
 *                                      .setTimeRange(startOfDay, endOfDay)
 *                                      .addColumn("logs", "events", "evt;");
 * }</pre>
 *
 * <h3>Advanced Construction Patterns</h3>
 * <pre>{@code
 * // Construct from a pre-computed byte-array row key
 * byte[] rowKeyBytes = Bytes.toBytes("high_perf_row");
 * AnyAppend byteAppend = AnyAppend.of(rowKeyBytes)
 *                                 .addColumn("data", "values", newBytes);
 *
 * // Use only a slice of a composite key
 * AnyAppend sliceAppend = AnyAppend.of(compositeKey, 0, prefixLength)
 *                                  .addColumn("logs", "hits", "h;");
 *
 * // Copy an existing Append and extend it
 * AnyAppend copiedAppend = AnyAppend.of(existingAppend.val())
 *                                   .addColumn("additional", "data", moreData);
 * }</pre>
 *
 * <h3>Key features</h3>
 * <ul>
 * <li><strong>Per-row atomicity</strong>: the server applies all queued cells in one
 *     read-modify-write against the target row</li>
 * <li><strong>Type conversion</strong>: Java objects are encoded to bytes via {@link HBaseExecutor}</li>
 * <li><strong>Performance control</strong>: {@link #setReturnResults(boolean)} can suppress the
 *     post-append result payload</li>
 * <li><strong>Time-range filtering</strong>: {@link #setTimeRange(long, long)} narrows the read
 *     that precedes the append</li>
 * </ul>
 *
 * <h3>Typical use cases</h3>
 * <ul>
 * <li><strong>Activity logs / audit trails</strong>: appending event strings to a per-user cell</li>
 * <li><strong>Stream accumulation</strong>: appending serialized records to an append-only cell</li>
 * <li><strong>Configuration merging</strong>: appending additional configuration fragments</li>
 * </ul>
 * <p>For numeric counters (page views, session counts, hit counts) use {@link AnyIncrement}, not
 * {@code AnyAppend}.</p>
 *
 * <h3>HBase Append semantics</h3>
 * <ul>
 * <li><strong>Atomicity</strong>: single-row atomic; safe under concurrent access</li>
 * <li><strong>Ordering</strong>: bytes are appended in the order {@code addColumn} is called</li>
 * <li><strong>Existence</strong>: a new cell is created when the column did not previously exist</li>
 * <li><strong>Versioning</strong>: each append creates a new version stamped with the server time
 *     (or the timestamp set via {@link #setTimestamp(long)} on the wrapped mutation)</li>
 * </ul>
 *
 * @see Append
 * @see AnyMutation
 * @see AnyIncrement
 * @see HBaseExecutor#append(String, AnyAppend)
 * @see <a href="http://hbase.apache.org/devapidocs/index.html">Apache HBase Java API Documentation</a>
 * @see org.apache.hadoop.hbase.client.Append
 */
public final class AnyAppend extends AnyMutation<AnyAppend> {

    private final Append append;

    /**
     * Package-private constructor: prefer {@link #of(Object)}. Wraps a new HBase {@link Append}
     * for the given row key, which is converted to bytes via {@link HBaseExecutor#toRowBytes(Object)}.
     *
     * @param rowKey the row key for the append operation
     * @throws IllegalArgumentException if {@code rowKey} is {@code null}
     */
    AnyAppend(final Object rowKey) {
        super(new Append(toRowBytes(rowKey)));
        append = (Append) mutation;
    }

    /**
     * Package-private constructor: prefer {@link #of(byte[])}. Wraps a new HBase {@link Append}
     * for the given byte-array row key.
     *
     * @param rowKey the row key as a byte array; must not be {@code null}
     */
    AnyAppend(final byte[] rowKey) {
        super(new Append(rowKey));
        append = (Append) mutation;
    }

    /**
     * Package-private constructor: prefer {@link #of(byte[], int, int)}. Wraps a new HBase
     * {@link Append} that uses a slice of {@code rowKey} as its row key.
     *
     * @param rowKey the byte array containing the row key data
     * @param rowOffset the starting position within {@code rowKey} (0-based)
     * @param rowLength the number of bytes to use from {@code rowKey}
     */
    AnyAppend(final byte[] rowKey, final int rowOffset, final int rowLength) {
        super(new Append(rowKey, rowOffset, rowLength));
        append = (Append) mutation;
    }

    /**
     * Package-private constructor: prefer {@link #of(byte[], long, NavigableMap)}. Wraps a new
     * HBase {@link Append} pre-populated with the given family map and timestamp.
     *
     * @param rowKey the row key as a byte array
     * @param timestamp the timestamp to apply to every cell in this append
     * @param familyMap a pre-populated map of column families to their cells
     */
    AnyAppend(final byte[] rowKey, final long timestamp, final NavigableMap<byte[], List<Cell>> familyMap) {
        super(new Append(rowKey, timestamp, familyMap));
        append = (Append) mutation;
    }

    /**
     * Package-private constructor: prefer {@link #of(Append)}. Wraps a fresh copy of an existing
     * HBase {@link Append}, so subsequent modifications do not touch the original.
     *
     * @param appendToCopy the existing {@link Append} to copy
     */
    AnyAppend(final Append appendToCopy) {
        super(new Append(appendToCopy));
        append = (Append) mutation;
    }

    /**
     * Creates a new AnyAppend instance for the specified row key.
     *
     * <p>This factory method creates an append operation for the given row key. The row key
     * will be converted to bytes via {@link HBaseExecutor#toRowBytes(Object)}. This is the
     * most commonly used factory method for creating append operations.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyAppend append = AnyAppend.of("user123");
     * append.addColumn("logs", "activity", "login;");
     * }</pre>
     *
     * @param rowKey the row key to append data to; converted to bytes automatically
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
     * append.addColumn("logs", "activity", "login;");
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
     * append.addColumn("logs", "activity", "login;");
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
     * @throws IllegalArgumentException if {@code rowKey} is empty (zero-length)
     * @throws NullPointerException if {@code rowKey} or {@code familyMap} is {@code null}
     * @see #of(Object)
     * @see org.apache.hadoop.hbase.Cell
     */
    public static AnyAppend of(final byte[] rowKey, final long timestamp, final NavigableMap<byte[], List<Cell>> familyMap) {
        return new AnyAppend(rowKey, timestamp, familyMap);
    }

    /**
     * Creates a new AnyAppend instance by copying an existing HBase Append operation.
     *
     * <p>Delegates to {@link Append#Append(Append)}, which copies the row, timestamp, time
     * range, and the family-to-cells map structure (the map and per-family {@code List<Cell>}
     * are new collections, but the {@link Cell} instances themselves are shared with the source).
     * Subsequent {@code addColumn} calls on the returned wrapper therefore do not mutate the
     * source append.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Append existingAppend = new Append(Bytes.toBytes("user123"));
     * existingAppend.addColumn(Bytes.toBytes("logs"), Bytes.toBytes("activity"),
     *                          Bytes.toBytes("login;"));
     *
     * AnyAppend append = AnyAppend.of(existingAppend);
     * append.addColumn("logs", "activity", "logout;");   // Add more data
     * }</pre>
     *
     * @param appendToCopy the existing HBase {@link Append} to copy; must not be {@code null}
     * @return a new AnyAppend instance backed by a fresh Append copied from {@code appendToCopy}
     * @throws NullPointerException if {@code appendToCopy} is {@code null}
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
     *                                .addColumn("logs", "activity", "login;");
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
     * Sets the TimeRange used by the Get that HBase performs before the append. Because the
     * server reads the existing cell value (or the lack of one) before concatenating, restricting
     * the read to a specific time range lets you target time-partitioned cells.
     * <p>
     * <strong>Caution:</strong> if the time range does not cover the latest cells the server will
     * append against an older version, effectively rewriting it rather than the current value.
     * </p>
     * <p>
     * The range is half-open: {@code [minStamp, maxStamp)} — {@code minStamp} is inclusive and
     * {@code maxStamp} is exclusive.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Append into the cell version recorded during the current hour
     * long hourStart = System.currentTimeMillis() / 3600000 * 3600000;
     * long hourEnd = hourStart + 3600000;
     * AnyAppend append = AnyAppend.of("hourly_log")
     *                             .setTimeRange(hourStart, hourEnd)
     *                             .addColumn("logs", "events", "evt;");
     * }</pre>
     *
     * @param minStamp minimum timestamp value, inclusive
     * @param maxStamp maximum timestamp value, exclusive
     * @return this AnyAppend instance, to allow fluent method chaining
     * @throws IllegalArgumentException if {@code minStamp} or {@code maxStamp} is negative or if
     *         {@code maxStamp < minStamp}
     * @see #getTimeRange()
     * @see org.apache.hadoop.hbase.io.TimeRange
     */
    public AnyAppend setTimeRange(final long minStamp, final long maxStamp) {
        append.setTimeRange(minStamp, maxStamp);

        return this;
    }

    /**
     * Returns the time range used by the pre-append read for this append operation.
     *
     * <p>The time range specifies the minimum and maximum timestamps for cells considered
     * by the {@code Get} that HBase performs before concatenating the new bytes. If no time
     * range has been set, this method returns the underlying {@link Append}'s default
     * {@link TimeRange} (covering all timestamps).</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyAppend append = AnyAppend.of("user123");
     * append.setTimeRange(startTime, endTime);
     * TimeRange range = append.getTimeRange();
     * }</pre>
     *
     * @return the configured {@link TimeRange}, or the default {@link TimeRange} if no specific
     *         range is set; never {@code null}
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
     * scenarios where the results are not needed, you can disable result return for better
     * performance. This is particularly useful for log aggregation where you only care about the
     * operation's success and not the resulting concatenated value.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Disable result return for better performance
     * AnyAppend highThroughput = AnyAppend.of("log_key")
     *                                     .addColumn("logs", "events", "evt;")
     *                                     .setReturnResults(false);
     *
     * // Enable result return to inspect the concatenated value
     * AnyAppend withResults = AnyAppend.of("important_key")
     *                                  .addColumn("data", "value", someData)
     *                                  .setReturnResults(true);
     * }</pre>
     *
     * @param returnResults {@code true} to return the post-append values (HBase's historical
     *                      default for {@link Append}); {@code false} to skip the response
     *                      payload and improve throughput
     * @return this AnyAppend instance, to allow fluent method chaining
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
     *                             .addColumn("logs", "activity", "login;")
     *                             .setReturnResults(false);
     * boolean returnsResults = append.isReturnResults();   // returns false
     * }</pre>
     *
     * @return {@code true} if results will be returned, {@code false} otherwise
     * @see #setReturnResults(boolean)
     */
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
     * @param family the column-family name as a byte array; must not be null or empty
     * @param qualifier the column-qualifier name as a byte array; may be {@code null} to denote an
     *                  empty qualifier
     * @param value the byte array to append to the existing cell value; may be {@code null}
     * @return this AnyAppend instance, to allow fluent method chaining
     * @throws IllegalArgumentException if {@code family} is null or empty (validated by the
     *         underlying {@link Append})
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
     * using strings and objects rather than byte arrays. The method automatically converts the
     * family/qualifier via {@link HBaseExecutor#toFamilyQualifierBytes(String)} and the value via
     * {@link HBaseExecutor#toValueBytes(Object)} before delegating to the wrapped
     * {@link Append}. The encoded bytes are then concatenated to the end of the cell's existing
     * value on the server — they are <em>not</em> parsed or summed, so this is not a counter
     * operation. Use {@link AnyIncrement} for atomic numeric counters.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyAppend append = AnyAppend.of("user123");
     * append.addColumn("logs", "activity", "login;");
     * append.addColumn("audit", "trail", "ACTION:LOGIN;");
     * append.addColumn("settings", "history", "theme=dark;");
     * }</pre>
     *
     * @param family the column-family name; converted via {@link HBaseExecutor#toFamilyQualifierBytes(String)}
     * @param qualifier the column-qualifier name; converted via {@link HBaseExecutor#toFamilyQualifierBytes(String)}
     * @param value the value whose encoded bytes will be appended; converted via
     *              {@link HBaseExecutor#toValueBytes(Object)}
     * @return this AnyAppend instance, to allow fluent method chaining
     * @see #addColumn(byte[], byte[], byte[])
     * @see AnyIncrement
     */
    public AnyAppend addColumn(final String family, final String qualifier, final Object value) {
        append.addColumn(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier), toValueBytes(value));

        return this;
    }

    /**
     * Adds a pre-constructed {@link Cell} carrying bytes to concatenate. Useful when an existing
     * cell (for example one read out of an {@link org.apache.hadoop.hbase.CellScanner}) already
     * encodes the value to append; for ordinary use prefer
     * {@link #addColumn(String, String, Object)}.
     *
     * <p>The cell's row key should match this append's row key. The wrapped
     * {@link Append#add(Cell)} logs and swallows the row-mismatch {@code IOException} rather
     * than propagating it (kept for backwards compatibility), so callers must enforce row
     * consistency themselves to avoid silently dropping the cell.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyAppend append = AnyAppend.of("user123");
     *
     * Cell cell = CellUtil.createCell(
     *     Bytes.toBytes("user123"),
     *     Bytes.toBytes("logs"),
     *     Bytes.toBytes("activity"),
     *     System.currentTimeMillis(),
     *     KeyValue.Type.Put.getCode(),
     *     Bytes.toBytes("login;")
     * );
     *
     * append.add(cell);
     * }</pre>
     *
     * @param cell the {@link Cell} to add to this append operation
     * @return this AnyAppend instance, to allow fluent method chaining
     * @see org.apache.hadoop.hbase.Cell
     * @see #addColumn(String, String, Object)
     */
    public AnyAppend add(final Cell cell) {
        append.add(cell);

        return this;
    }

    /**
     * Sets a custom attribute on this append using a raw byte-array value. This overload bypasses
     * {@link HBaseExecutor#toValueBytes(Object)} and stores the supplied array directly on the
     * wrapped HBase {@link Append}; a {@code null} value clears the attribute.
     *
     * <p>For a {@code byte[]} argument, this is observationally equivalent to
     * {@link AnyOperationWithAttributes#setAttribute(String, Object)} (since {@code toValueBytes}
     * returns {@code byte[]} arguments as-is), but its presence avoids the {@code Object}-overload
     * dispatch path and makes the byte-array intent explicit at the call site.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyAppend append = AnyAppend.of("user123");
     * append.setAttribute("trace_id", Bytes.toBytes("trace-12345"));
     * append.setAttribute("priority", Bytes.toBytes("high"));
     * }</pre>
     *
     * @param name the attribute name
     * @param value the attribute value as a raw byte array; may be {@code null} to clear it
     * @return this AnyAppend instance, to allow fluent method chaining
     * @see #getAttribute(String)
     * @see AnyOperationWithAttributes#setAttribute(String, Object)
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
