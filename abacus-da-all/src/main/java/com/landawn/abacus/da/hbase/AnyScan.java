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

import static com.landawn.abacus.da.hbase.HBaseExecutor.toFamilyQualifierBytes;
import static com.landawn.abacus.da.hbase.HBaseExecutor.toRowKeyBytes;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableSet;

import org.apache.hadoop.hbase.client.Cursor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Scan.ReadType;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;

import com.landawn.abacus.annotation.SuppressFBWarnings;
import com.landawn.abacus.util.N;

/**
 * A comprehensive wrapper around HBase's {@link Scan} class that provides simplified scanning
 * operations with automatic type conversion and a fluent API design.
 *
 * <p>This class extends {@link AnyQuery} and exposes all the functionality of HBase's native
 * {@link Scan} class while reducing the complexity of working with byte arrays. Every mutator
 * returns {@code this}, enabling method chaining. The underlying {@link Scan} instance is
 * accessible through {@link #val()} for interoperation with native HBase APIs.</p>
 *
 * <h2>Scan range semantics</h2>
 * <ul>
 *   <li><b>Start row</b> is <i>inclusive</i> by default and can be flipped with
 *       {@link #withStartRow(Object, boolean)}; if it does not exist the scanner advances to the
 *       next lexicographically greater row.</li>
 *   <li><b>Stop row</b> is <i>exclusive</i> by default and can be flipped with
 *       {@link #withStopRow(Object, boolean)}.</li>
 *   <li><b>Prefix scans</b> can be configured via {@link #setStartStopRowForPrefixScan(byte[])}
 *       which translates the prefix into appropriate start/stop bounds without server-side filtering.</li>
 *   <li><b>Reversed scans</b> ({@link #setReversed(boolean)}) swap the natural traversal order: the
 *       scanner starts at the start row (now treated as the upper bound) and moves backwards toward
 *       the stop row.</li>
 * </ul>
 *
 * <h2>Throughput tuning</h2>
 * <ul>
 *   <li>{@link #setCaching(int)} controls how many <em>rows</em> are pre-fetched per RPC.</li>
 *   <li>{@link #setBatch(int)} controls how many <em>columns</em> are returned per RPC for
 *       wide rows (use with {@link #setAllowPartialResults(boolean)} for very wide rows).</li>
 *   <li>{@link #setMaxResultSize(long)} caps the total bytes transferred per RPC, providing an
 *       OOM safeguard independent of row/column counts.</li>
 *   <li>{@link #setLimit(int)} bounds the total number of rows returned by the scan.</li>
 *   <li>{@link #setAsyncPrefetch(boolean)} lets the client overlap I/O with processing.</li>
 * </ul>
 *
 * <h2>Time range and versions</h2>
 * <p>{@link #setTimeRange(long, long)} restricts the scan to cells whose timestamp falls within
 * {@code [minStamp, maxStamp)}. {@link #setTimestamp(long)} is a convenience for the single-point
 * range {@code [timestamp, timestamp + 1)}. By default at most one version per cell is returned;
 * use {@link #readVersions(int)} or {@link #readAllVersions()} to retrieve more.</p>
 *
 * <h2>Filters and raw scans</h2>
 * <p>Server-side {@link Filter} chains are inherited from {@link AnyQuery#setFilter(Filter)}.
 * {@link #setRaw(boolean)} enables a raw scan that surfaces delete markers (tombstones) for
 * administrative or debugging use.</p>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * AnyScan scan = AnyScan.create()
 *     .withStartRow("user_001")
 *     .withStopRow("user_999")
 *     .addFamily("data")
 *     .setLimit(100)
 *     .setCaching(10);
 * }</pre>
 *
 * <p>Key features:</p>
 * <ul>
 *   <li>Automatic conversion between Java values and byte arrays for row keys / families / qualifiers</li>
 *   <li>Fluent API for method chaining</li>
 *   <li>Support for all HBase scan operations and filters</li>
 *   <li>Easy configuration of scan parameters like caching, batching, and limits</li>
 * </ul>
 *
 * @see <a href="http://hbase.apache.org/devapidocs/index.html">Apache HBase Java API Documentation</a>
 * @see org.apache.hadoop.hbase.client.Scan
 * @see AnyQuery
 */
public final class AnyScan extends AnyQuery<AnyScan> {

    /**
     * The underlying HBase {@link Scan} instance that backs this wrapper. Mutator methods on
     * {@link AnyScan} forward to this object, and {@link #val()} exposes it for interoperation
     * with native HBase APIs. Also held in the inherited {@code query} field.
     */
    private final Scan scan;

    /**
     * Constructs a new AnyScan with default configuration.
     *
     * <p>Creates an empty scan that will scan all rows and columns in a table unless further
     * configured with row ranges, families, or filters. Package-private; callers should use the
     * {@link #create()} factory method.</p>
     */
    AnyScan() {
        super(new Scan());
        scan = (Scan) query;
    }

    /**
     * Constructs a new AnyScan that starts scanning at the specified row.
     *
     * <p>If the specified row does not exist, the scanner starts from the next lexicographically
     * greater row. Package-private; callers should use {@link #create()} chained with
     * {@link #withStartRow(Object)}.</p>
     *
     * @param startRow row to start scanner at or after; converted via {@link HBaseExecutor#toRowKeyBytes(Object)}
     * @deprecated Use {@code AnyScan.create().withStartRow(startRow)} instead.
     */
    @Deprecated
    AnyScan(final Object startRow) {
        super(new Scan(toRowKeyBytes(startRow)));
        scan = (Scan) query;
    }

    /**
     * Constructs a new AnyScan for the range of rows specified. Package-private; callers should use
     * {@link #create()} chained with {@link #withStartRow(Object)} and {@link #withStopRow(Object)}.
     *
     * @param startRow row to start scanner at or after (inclusive)
     * @param stopRow row to stop scanner before (exclusive)
     * @deprecated Use {@code AnyScan.create().withStartRow(startRow).withStopRow(stopRow)} instead.
     */
    @Deprecated
    AnyScan(final Object startRow, final Object stopRow) {
        super(new Scan(toRowKeyBytes(startRow), toRowKeyBytes(stopRow)));
        scan = (Scan) query;
    }

    /**
     * Constructs a new AnyScan that starts scanning at the specified row with a filter. Package-private;
     * callers should use {@link #create()} chained with {@link #withStartRow(Object)} and
     * {@link AnyQuery#setFilter(Filter)}.
     *
     * @param startRow row to start scanner at or after
     * @param filter the {@link Filter} to apply to the scan
     * @deprecated Use {@code AnyScan.create().withStartRow(startRow).setFilter(filter)} instead.
     */
    @Deprecated
    AnyScan(final Object startRow, final Filter filter) {
        super(new Scan(toRowKeyBytes(startRow), filter));
        scan = (Scan) query;
    }

    /**
     * Constructs a new AnyScan wrapping an existing HBase {@link Scan} object.
     *
     * <p>The supplied {@link Scan} is wrapped by reference, not copied. Passing {@code null} will
     * result in an {@link IllegalArgumentException}. Package-private; callers should use
     * {@link #of(Scan)}.</p>
     *
     * @param scan the HBase {@link Scan} object to wrap; must not be {@code null}
     */
    AnyScan(final Scan scan) {
        super(scan);
        this.scan = (Scan) query;
    }

    /**
     * Constructs a new AnyScan from an existing {@link Get} operation.
     *
     * <p>This constructor converts a point lookup into a single-row scan, preserving the families,
     * qualifiers, time range and other settings configured on the {@link Get}. The conversion is
     * delegated to the underlying {@link Scan#Scan(Get)} constructor. No null-check is performed;
     * passing {@code null} will result in a {@link NullPointerException} from that constructor.
     * Package-private; callers should use {@link #of(Get)}.</p>
     *
     * @param get the {@link Get} operation to convert to a {@link Scan}; must not be {@code null}
     */
    AnyScan(final Get get) {
        this(new Scan(get));
    }

    /**
     * Creates a new AnyScan instance with default configuration.
     * <p>
     * This is a factory method that provides a convenient way to create new AnyScan instances.
     * The returned scan will include all rows and columns unless further configured.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Create a basic scan
     * AnyScan scan = AnyScan.create();
     * boolean families = scan.hasFamilies();   // returns false (scans all families)
     *
     * // Create and configure a scan with method chaining
     * AnyScan configuredScan = AnyScan.create()
     *                                 .withStartRow("user_100")
     *                                 .withStopRow("user_999")
     *                                 .addFamily("data")
     *                                 .setLimit(100)
     *                                 .setCaching(10);
     * int limit = configuredScan.getLimit();   // returns 100
     * }</pre>
     *
     * @return a new AnyScan instance with default configuration
     */
    public static AnyScan create() {
        return new AnyScan();
    }

    /**
     * Creates a new AnyScan instance from a cursor position.
     * <p>
     * This method allows resuming a scan from a previously saved cursor position,
     * which is useful for implementing pagination or resuming interrupted scans.
     * The cursor must have been obtained from a previous scan that had cursor
     * results enabled via {@link #setNeedCursorResult(boolean)}.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // First scan: enable cursor results
     * AnyScan initialScan = AnyScan.create()
     *                              .setNeedCursorResult(true)
     *                              .setLimit(100);
     *
     * ResultScanner scanner = table.getScanner(initialScan.val());
     * Cursor lastCursor = null;
     * for (Result result : scanner) {
     *     // Process result
     *     if (result.getCursor() != null) {
     *         lastCursor = result.getCursor();
     *     }
     * }
     *
     * // Resume from cursor
     * if (lastCursor != null) {
     *     AnyScan resumedScan = AnyScan.createScanFromCursor(lastCursor);
     *     // Continue scanning from where we left off
     * }
     *
     * // A null cursor is rejected.
     * AnyScan.createScanFromCursor((Cursor) null);   // throws NullPointerException
     * }</pre>
     *
     * @param cursor the cursor position from which to resume scanning; must not be null
     * @return a new AnyScan instance configured to start from the cursor position
     * @throws NullPointerException if {@code cursor} is null (raised by the wrapped {@link Scan#createScanFromCursor(Cursor)})
     * @see #setNeedCursorResult(boolean)
     */
    public static AnyScan createScanFromCursor(final Cursor cursor) {
        return new AnyScan(Scan.createScanFromCursor(cursor));
    }

    /**
     * Creates a Scan operation starting at the specified row.
     *
     * <p>If the specified row does not exist, the Scanner will start from the
     * next closest row after the specified row.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.of("user_001");
     * byte[] start = scan.getStartRow();   // returns the bytes of "user_001"
     *
     * // A null start row is tolerated: the resulting scan has a null start row.
     * AnyScan nullStart = AnyScan.of((Object) null);   // no exception
     * byte[] sr = nullStart.getStartRow();             // returns null
     *
     * // An empty byte[] start row is also tolerated.
     * AnyScan emptyStart = AnyScan.of(new byte[0]);    // no exception
     * int len = emptyStart.getStartRow().length;       // returns 0
     * }</pre>
     *
     * @param startRow row to start scanner at or after
     * @return a new AnyScan instance configured with the specified start row
     * @deprecated Use {@code AnyScan.create().withStartRow(startRow)} instead.
     */
    @Deprecated
    public static AnyScan of(final Object startRow) {
        return new AnyScan(startRow);
    }

    /**
     * Creates a Scan operation for the range of rows specified.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.of("a", "z");
     * byte[] start = scan.getStartRow();   // returns the bytes of "a"
     * byte[] stop = scan.getStopRow();     // returns the bytes of "z" (exclusive)
     *
     * // A null start row is tolerated; the resulting start row is null.
     * AnyScan nullStart = AnyScan.of((Object) null, "z");   // no exception
     * byte[] sr = nullStart.getStartRow();                  // returns null
     * }</pre>
     *
     * @param startRow row to start scanner at or after (inclusive)
     * @param stopRow row to stop scanner before (exclusive)
     * @return a new AnyScan instance configured with the specified start and stop rows
     * @deprecated Use {@code AnyScan.create().withStartRow(startRow).withStopRow(stopRow)} instead.
     */
    @Deprecated
    public static AnyScan of(final Object startRow, final Object stopRow) {
        return new AnyScan(startRow, stopRow);
    }

    /**
     * Creates a Scan operation starting at the specified row with a filter.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Filter filter = new PrefixFilter(Bytes.toBytes("user_"));
     * AnyScan scan = AnyScan.of("user_001", filter);
     * byte[] start = scan.getStartRow();   // returns the bytes of "user_001"
     * boolean has = scan.hasFilter();      // returns true
     * Filter f = scan.getFilter();         // returns the same filter instance
     * }</pre>
     *
     * @param startRow row to start scanner at or after
     * @param filter the filter to apply to the scan
     * @return a new AnyScan instance configured with the specified start row and filter
     * @deprecated Use {@code AnyScan.create().withStartRow(startRow).setFilter(filter)} instead.
     */
    @Deprecated
    public static AnyScan of(final Object startRow, final Filter filter) {
        return new AnyScan(startRow, filter);
    }

    /**
     * Creates a new AnyScan that wraps (does not copy) an existing HBase Scan object.
     *
     * <p>The returned AnyScan stores the supplied Scan by reference. Subsequent mutations
     * performed through the returned AnyScan are applied to the same underlying Scan and
     * are therefore visible to any other code that retains a reference to the original.
     * If isolation is required, copy the Scan before calling this method (for example via
     * {@code new Scan(originalScan)}).</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Scan raw = new Scan();
     * raw.setCaching(99);
     * AnyScan scan = AnyScan.of(raw);
     * Scan backing = scan.val();        // returns the same Scan instance (raw == backing)
     * int caching = scan.getCaching();  // returns 99
     *
     * // A null Scan is rejected.
     * AnyScan.of((Scan) null);          // throws IllegalArgumentException
     * }</pre>
     *
     * @param scan the HBase Scan object to wrap; must not be null
     * @return a new AnyScan instance that wraps the provided Scan by reference
     * @throws IllegalArgumentException if {@code scan} is null
     */
    public static AnyScan of(final Scan scan) {
        return new AnyScan(scan);
    }

    /**
     * Creates a new AnyScan instance from an existing Get operation.
     * <p>
     * This factory method converts a Get operation into a Scan operation,
     * preserving all the settings from the original Get.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Get get = new Get(Bytes.toBytes("row1"));
     * AnyScan scan = AnyScan.of(get);
     * byte[] start = scan.getStartRow();   // returns the bytes of "row1"
     * boolean getScan = scan.isGetScan();  // returns true (single-row scan)
     *
     * // A null Get is rejected by the underlying Scan constructor.
     * AnyScan.of((Get) null);              // throws NullPointerException
     * }</pre>
     *
     * @param get the Get operation to convert to a Scan; must not be null
     * @return a new AnyScan instance created from the Get operation
     * @throws NullPointerException if {@code get} is null (raised by the wrapped HBase {@link Scan} constructor)
     */
    public static AnyScan of(final Get get) {
        return new AnyScan(get);
    }

    /**
     * Returns the underlying HBase {@link Scan} object.
     *
     * <p>This method provides access to the native HBase {@link Scan} instance, which can be useful
     * when you need to interact directly with HBase APIs that expect the native type. Mutations
     * performed on the returned object are visible to this {@link AnyScan} and vice versa.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create().setCaching(50);
     * Scan native_ = scan.val();                  // returns the backing Scan; never null
     * int caching = native_.getCaching();         // returns 50 (mutations are shared)
     * boolean same = (scan.val() == scan.val());  // returns true (same instance)
     * }</pre>
     *
     * @return the underlying {@link Scan} object; never {@code null}
     */
    public Scan val() {
        return scan;
    }

    /**
     * Checks if this scan is effectively a Get operation.
     * <p>
     * Returns true if this scan has been configured to retrieve a single row,
     * making it equivalent to a Get operation in terms of behavior.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create();
     * boolean get = scan.isGetScan();   // returns false (open-ended scan)
     *
     * // A scan derived from a Get behaves like a Get.
     * AnyScan fromGet = AnyScan.of(new Get(Bytes.toBytes("row1")));
     * boolean isGet = fromGet.isGetScan();   // returns true
     * }</pre>
     *
     * @return {@code true} if this scan behaves like a Get operation; {@code false} otherwise
     */
    public boolean isGetScan() {
        return scan.isGetScan();
    }

    /**
     * Checks if this scan has any column families specified.
     * <p>
     * Returns true if at least one column family has been added to this scan.
     * If no families are specified, the scan will include all families in the table.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create();
     * boolean before = scan.hasFamilies();   // returns false (scans all families)
     *
     * scan.addFamily("cf");
     * boolean after = scan.hasFamilies();    // returns true
     * }</pre>
     *
     * @return {@code true} if column families have been specified; {@code false} otherwise
     */
    public boolean hasFamilies() {
        return scan.hasFamilies();
    }

    /**
     * Returns the number of column families that have been specified for this scan.
     * <p>
     * This count includes all column families that have been explicitly added using
     * {@link #addFamily(String)} or {@link #addColumn(String, String)} methods.
     * If no families are specified, this returns 0 and the scan will include all families.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create();
     * int empty = scan.numFamilies();   // returns 0
     *
     * scan.addFamily("cf1").addFamily("cf2");
     * int count = scan.numFamilies();   // returns 2
     * }</pre>
     *
     * @return the number of column families specified for this scan
     * @see #hasFamilies()
     * @see #addFamily(String)
     */
    public int numFamilies() {
        return scan.numFamilies();
    }

    /**
     * Returns an array of all column family names specified for this scan.
     *
     * <p>This method provides access to all column families that have been added to the scan.
     * The returned array contains byte arrays representing the family names. Returns {@code null}
     * if no families have been added (in which case the scan covers every family in the table).</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create();
     * byte[][] none = scan.getFamilies();   // returns null (no families added)
     *
     * scan.addFamily("cf1").addFamily("cf2");
     * byte[][] families = scan.getFamilies();   // returns a length-2 array
     * }</pre>
     *
     * @return an array of column family names as byte arrays; {@code null} if no families specified
     * @see #addFamily(String)
     * @see #numFamilies()
     */
    public byte[][] getFamilies() {
        return scan.getFamilies();
    }

    /**
     * Adds a column family to this scan to retrieve all columns from that family.
     * <p>
     * When a family is added without specific qualifiers, all columns within that
     * family will be retrieved. This is useful for getting all data associated with
     * a particular column family in HBase.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create()
     *                      .addFamily("userdata")     // returns this scan
     *                      .addFamily("preferences"); // returns this scan
     * int count = scan.numFamilies();                 // returns 2
     * }</pre>
     *
     * @param family the column family name to retrieve; the value is converted to bytes and forwarded
     *               to the underlying {@link Scan} (an empty or {@code null} family is tolerated)
     * @return this AnyScan instance for method chaining
     * @see #addColumn(String, String)
     * @see #addFamily(byte[])
     */
    public AnyScan addFamily(final String family) {
        scan.addFamily(toFamilyQualifierBytes(family));

        return this;
    }

    /**
     * Adds a column family to this scan using a byte array identifier.
     * <p>
     * This method provides the same functionality as {@link #addFamily(String)} but
     * accepts a pre-converted byte array. Use this when you already have the family
     * name as bytes to avoid conversion overhead.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * byte[] familyBytes = Bytes.toBytes("userdata");
     * AnyScan scan = AnyScan.create().addFamily(familyBytes);   // returns this scan
     * int count = scan.numFamilies();                           // returns 1
     * }</pre>
     *
     * @param family the column family name as byte array; forwarded to the underlying {@link Scan}
     *               (an empty or {@code null} array is tolerated)
     * @return this AnyScan instance for method chaining
     * @see #addFamily(String)
     */
    public AnyScan addFamily(final byte[] family) {
        scan.addFamily(family);

        return this;
    }

    /**
     * Returns the complete family-to-qualifiers mapping for this scan.
     * <p>
     * This method provides access to the internal structure of the scan,
     * organized as a Map where keys are column family names (as byte arrays)
     * and values are NavigableSet objects containing the specific qualifiers
     * within each family. If a family has no specific qualifiers, it means
     * all qualifiers in that family will be scanned.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create();
     * Map<byte[], NavigableSet<byte[]>> empty = scan.getFamilyMap();   // returns an empty (non-null) map
     *
     * scan.addColumn("cf", "q");
     * Map<byte[], NavigableSet<byte[]>> map = scan.getFamilyMap();     // returns a map of size 1
     * }</pre>
     *
     * @return a Map of column families to their qualifier sets; never null
     * @see #addFamily(String)
     * @see #addColumn(String, String)
     */
    public Map<byte[], NavigableSet<byte[]>> getFamilyMap() {
        return scan.getFamilyMap();
    }

    /**
     * Sets the complete family-to-qualifiers mapping for this scan.
     * <p>
     * This advanced method allows setting the entire scan structure in one operation.
     * The provided map should contain column family names as keys and NavigableSet
     * objects containing the specific qualifiers to scan within each family.
     * An empty NavigableSet for a family means all qualifiers in that family will be scanned.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<byte[], NavigableSet<byte[]>> familyMap = new HashMap<>();
     *
     * // Add specific qualifiers for "info" family
     * NavigableSet<byte[]> infoQualifiers = new TreeSet<>(Bytes.BYTES_COMPARATOR);
     * infoQualifiers.add(Bytes.toBytes("name"));
     * infoQualifiers.add(Bytes.toBytes("email"));
     * familyMap.put(Bytes.toBytes("info"), infoQualifiers);
     *
     * // Add all qualifiers for "data" family (empty set)
     * familyMap.put(Bytes.toBytes("data"), new TreeSet<>(Bytes.BYTES_COMPARATOR));
     *
     * AnyScan scan = AnyScan.create().setFamilyMap(familyMap);   // returns this scan
     * int count = scan.numFamilies();                            // returns 2 ("info" and "data")
     *
     * // A null map is rejected fast.
     * AnyScan.create().setFamilyMap((Map<byte[], NavigableSet<byte[]>>) null);   // throws IllegalArgumentException
     * }</pre>
     *
     * <p>The supplied map is validated to be non-{@code null} before being passed to
     * {@link Scan#setFamilyMap(Map)}, which simply stores the reference.</p>
     *
     * @param familyMap the complete family-to-qualifiers mapping; must not be {@code null}
     * @return this {@link AnyScan} instance for method chaining
     * @throws IllegalArgumentException if {@code familyMap} is {@code null}
     * @see #getFamilyMap()
     */
    public AnyScan setFamilyMap(final Map<byte[], NavigableSet<byte[]>> familyMap) {
        N.checkArgNotNull(familyMap, "familyMap");

        scan.setFamilyMap(familyMap);

        return this;
    }

    /**
     * Sets a specific time range for a column family.
     * <p>
     * This method allows different time ranges to be applied to different column families
     * within the same scan. Only cells within the specified timestamp range for the given
     * family will be included in the results. This is useful when different column families
     * have different temporal characteristics or when you want to query different time
     * windows for different families.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long yesterday = System.currentTimeMillis() - 86400000;
     * long now = System.currentTimeMillis();
     *
     * AnyScan scan = AnyScan.create()
     *                      .addFamily("recent_data")
     *                      .addFamily("historical_data")
     *                      .setColumnFamilyTimeRange("recent_data", yesterday, now)        // returns this scan
     *                      .setColumnFamilyTimeRange("historical_data", 0, yesterday);     // returns this scan
     *
     * // An inverted range (max < min) is rejected.
     * AnyScan.create().setColumnFamilyTimeRange("cf", 200L, 100L);   // throws IllegalArgumentException
     *
     * // A negative timestamp is also rejected.
     * AnyScan.create().setColumnFamilyTimeRange("cf", -1L, 100L);    // throws IllegalArgumentException
     * }</pre>
     *
     * @param family the column family name
     * @param minTimestamp the minimum timestamp (inclusive)
     * @param maxTimestamp the maximum timestamp (exclusive)
     * @return this AnyScan instance for method chaining
     * @throws IllegalArgumentException if {@code minTimestamp} or {@code maxTimestamp} is negative,
     *         or if {@code maxTimestamp} is smaller than {@code minTimestamp}
     * @see #setTimeRange(long, long)
     */
    @Override
    public AnyScan setColumnFamilyTimeRange(final String family, final long minTimestamp, final long maxTimestamp) {
        scan.setColumnFamilyTimeRange(toFamilyQualifierBytes(family), minTimestamp, maxTimestamp);

        return this;
    }

    /**
     * Sets a specific time range for a column family using byte array identifier.
     * <p>
     * This method provides the same functionality as {@link #setColumnFamilyTimeRange(String, long, long)}
     * but accepts a pre-converted byte array for the family name.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * byte[] family = Bytes.toBytes("data");
     * AnyScan scan = AnyScan.create().setColumnFamilyTimeRange(family, 0L, 1000L);   // returns this scan
     *
     * // An inverted range (max < min) is rejected.
     * AnyScan.create().setColumnFamilyTimeRange(family, 200L, 100L);   // throws IllegalArgumentException
     *
     * // A negative timestamp is also rejected.
     * AnyScan.create().setColumnFamilyTimeRange(family, -1L, 100L);    // throws IllegalArgumentException
     * }</pre>
     *
     * @param family the column family name as byte array
     * @param minTimestamp the minimum timestamp (inclusive)
     * @param maxTimestamp the maximum timestamp (exclusive)
     * @return this AnyScan instance for method chaining
     * @throws IllegalArgumentException if {@code minTimestamp} or {@code maxTimestamp} is negative,
     *         or if {@code maxTimestamp} is smaller than {@code minTimestamp}
     * @see #setColumnFamilyTimeRange(String, long, long)
     */
    @Override
    public AnyScan setColumnFamilyTimeRange(final byte[] family, final long minTimestamp, final long maxTimestamp) {
        scan.setColumnFamilyTimeRange(family, minTimestamp, maxTimestamp);

        return this;
    }

    /**
     * Adds a specific column (family:qualifier combination) to this scan.
     * <p>
     * This method allows for precise column selection, scanning only the specified
     * column from the HBase table. This is more efficient than scanning entire families
     * when you only need specific columns. Multiple calls to this method will add
     * additional columns to the scan.
     * </p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create()
     *                      .addColumn("userinfo", "name")      // returns this scan
     *                      .addColumn("userinfo", "email")     // returns this scan
     *                      .addColumn("preferences", "theme"); // returns this scan
     * int count = scan.numFamilies();                          // returns 2 ("userinfo" and "preferences")
     * }</pre>
     *
     * @param family the column family name; converted to bytes and forwarded to the underlying
     *               {@link Scan} (an empty or {@code null} family is tolerated)
     * @param qualifier the column qualifier name; converted to bytes (an empty or {@code null}
     *               qualifier is tolerated)
     * @return this AnyScan instance for method chaining
     * @see #addFamily(String)
     * @see #addColumn(byte[], byte[])
     */
    public AnyScan addColumn(final String family, final String qualifier) {
        scan.addColumn(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier));

        return this;
    }

    /**
     * Adds a specific column using byte array identifiers.
     * <p>
     * This method provides the same functionality as {@link #addColumn(String, String)}
     * but accepts pre-converted byte arrays. Use this when you already have the family
     * and qualifier names as bytes to avoid conversion overhead.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * byte[] family = Bytes.toBytes("userinfo");
     * byte[] qualifier = Bytes.toBytes("email");
     * AnyScan scan = AnyScan.create().addColumn(family, qualifier);   // returns this scan
     * int count = scan.numFamilies();                                 // returns 1
     * }</pre>
     *
     * @param family the column family name as byte array; forwarded to the underlying {@link Scan}
     *               (an empty or {@code null} array is tolerated)
     * @param qualifier the column qualifier name as byte array (an empty or {@code null} array is tolerated)
     * @return this AnyScan instance for method chaining
     * @see #addColumn(String, String)
     */
    public AnyScan addColumn(final byte[] family, final byte[] qualifier) {
        scan.addColumn(family, qualifier);

        return this;
    }

    /**
     * Returns the time range for this scan operation.
     * <p>
     * The time range specifies which versions of cells to include based on their timestamps.
     * By default, all versions within the configured time range will be considered.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create();
     * TimeRange def = scan.getTimeRange();   // returns the full default range
     * long min = def.getMin();               // returns 0
     * long max = def.getMax();               // returns Long.MAX_VALUE
     *
     * scan.setTimeRange(100L, 200L);
     * TimeRange tr = scan.getTimeRange();    // min == 100, max == 200
     * }</pre>
     *
     * @return the TimeRange object specifying the minimum and maximum timestamps for this scan
     * @see #setTimeRange(long, long)
     * @see #setTimestamp(long)
     */
    public TimeRange getTimeRange() {
        return scan.getTimeRange();
    }

    /**
     * Sets the time range for this scan to retrieve cells within the specified timestamp range.
     * <p>
     * Only cells with timestamps greater than or equal to minStamp and less than maxStamp
     * will be included in the scan results. This is useful for temporal queries or retrieving
     * historical data within a specific time window.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long yesterday = System.currentTimeMillis() - 86400000;
     * long now = System.currentTimeMillis();
     * AnyScan scan = AnyScan.create().setTimeRange(yesterday, now);   // returns this scan
     * long min = scan.getTimeRange().getMin();                        // returns yesterday
     *
     * // An inverted range (max < min) is rejected.
     * AnyScan.create().setTimeRange(200L, 100L);   // throws IllegalArgumentException
     *
     * // A negative timestamp is also rejected.
     * AnyScan.create().setTimeRange(-1L, 100L);    // throws IllegalArgumentException
     * }</pre>
     *
     * @param minStamp the minimum timestamp (inclusive)
     * @param maxStamp the maximum timestamp (exclusive)
     * @return this AnyScan instance for method chaining
     * @throws IllegalArgumentException if either stamp is negative or {@code maxStamp} is smaller
     *         than {@code minStamp} (the underlying {@link IOException} is wrapped as such)
     * @see #getTimeRange()
     * @see #setTimestamp(long)
     */
    public AnyScan setTimeRange(final long minStamp, final long maxStamp) {
        try {
            scan.setTimeRange(minStamp, maxStamp);
        } catch (final IOException e) {
            throw new IllegalArgumentException(e);
        }

        return this;
    }

    /**
     * Restricts the scan to cells with exactly the specified timestamp.
     *
     * <p>Delegates to {@link Scan#setTimestamp(long)}, which configures the time range as
     * {@code [timestamp, timestamp + 1)} — i.e. only cells stamped with {@code timestamp} are
     * returned. The default maximum versions returned is 1; if you need every version that
     * happens to share this timestamp, also call {@link #readVersions(int)}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create().setTimestamp(1234567890L).readVersions(5);   // returns this scan
     * long min = scan.getTimeRange().getMin();                                     // returns 1234567890
     * long max = scan.getTimeRange().getMax();                                     // returns 1234567891 (timestamp + 1)
     *
     * // A negative timestamp is rejected.
     * AnyScan.create().setTimestamp(-1L);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param timestamp the exact timestamp to filter cells by
     * @return this AnyScan instance for method chaining
     * @throws IllegalArgumentException if {@code timestamp} is negative
     * @see #setTimeRange(long, long)
     * @see #readVersions(int)
     */
    public AnyScan setTimestamp(final long timestamp) {
        scan.setTimestamp(timestamp);
        return this;
    }

    //    /**
    //     * Get versions of columns with the specified timestamp. Note, default maximum
    //     * versions to return is 1.  If your time range spans more than one version
    //     * and you want all versions returned, up the number of versions beyond the default.
    //     *
    //     * @param timestamp version timestamp
    //     * @return this
    //     * @see Scan#setMaxVersions()
    //     * @see Scan#setMaxVersions(int)
    //     * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
    //     *             Use {@code setTimestamp(long)} instead
    //     */
    //    @Deprecated
    //    public AnyScan setTimeStamp(long timestamp) {
    //        try {
    //            scan.setTimeStamp(timestamp);
    //        } catch (IOException e) {
    //            throw new IllegalArgumentException(e);
    //        }
    //
    //        return this;
    //    }

    /**
     * Returns whether the start row is included in this scan.
     * <p>
     * By default, the start row is inclusive, meaning if a row exactly matches
     * the start row key, it will be included in the scan results. This method
     * returns the current inclusion setting for the start row.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create();
     * boolean def = scan.includeStartRow();   // returns true (inclusive by default)
     *
     * scan.withStartRow("a", false);
     * boolean excl = scan.includeStartRow();  // returns false
     * }</pre>
     *
     * @return {@code true} if the start row is included in the scan, {@code false} otherwise
     * @see #withStartRow(Object, boolean)
     * @see #getStartRow()
     */
    public boolean includeStartRow() {
        return scan.includeStartRow();
    }

    /**
     * Returns the start row key for this scan.
     * <p>
     * The start row defines the beginning of the scan range. Rows that are
     * lexicographically greater than or equal to (if inclusive) or greater than
     * (if exclusive) this row key will be considered for scanning.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create();
     * byte[] def = scan.getStartRow();   // returns an empty (length 0) array
     *
     * scan.withStartRow("a");
     * byte[] sr = scan.getStartRow();    // returns the bytes of "a"
     * }</pre>
     *
     * @return the start row key as byte array, or empty array if scanning from table beginning
     * @see #withStartRow(Object)
     * @see #includeStartRow()
     */
    public byte[] getStartRow() {
        return scan.getStartRow();
    }

    //    /**
    //     * Set the start row of the scan.
    //     * <p>
    //     * If the specified row does not exist, the Scanner will start from the next closest row after the
    //     * specified row.
    //     * @param startRow row to start scanner at or after
    //     * @return this
    //     * @throws IllegalArgumentException if startRow does not meet criteria for a row key (when length
    //     *           exceeds {@link HConstants#MAX_ROW_LENGTH})
    //     * @deprecated use {@code withStartRow(byte[])} instead. This method may change the inclusive of
    //     *             the stop row to keep compatible with the old behavior.
    //     */
    //    @Deprecated
    //    public AnyScan setStartRow(final Object startRow) {
    //        scan.setStartRow(toRowKeyBytes(startRow));
    //
    //        return this;
    //    }

    /**
     * Sets the start row for the scan with inclusive behavior by default.
     * <p>
     * The scan will start from this row key. If the exact row doesn't exist,
     * the scan will start from the next closest row that is lexicographically
     * greater than the specified start row.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create().withStartRow("user_100");   // returns this scan
     * byte[] sr = scan.getStartRow();                             // returns the bytes of "user_100"
     * boolean incl = scan.includeStartRow();                      // returns true (inclusive)
     *
     * // An empty byte[] start row is tolerated (resets to an open lower bound).
     * AnyScan empty = AnyScan.create().withStartRow(new byte[0]);   // no exception
     * int len = empty.getStartRow().length;                         // returns 0
     * }</pre>
     *
     * @param startRow the row key to start scanning from (inclusive by default)
     * @return this AnyScan instance for method chaining
     * @see #withStartRow(Object, boolean)
     * @see #includeStartRow()
     */
    public AnyScan withStartRow(final Object startRow) {
        scan.withStartRow(toRowKeyBytes(startRow));

        return this;
    }

    /**
     * Sets the start row for the scan with explicit inclusive/exclusive behavior.
     * <p>
     * This method allows fine-grained control over whether the start row itself
     * is included in the scan results. By default (when using {@link #withStartRow(Object)}),
     * the start row is inclusive.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Exclude the start row itself
     * AnyScan scan = AnyScan.create().withStartRow("user_100", false);   // returns this scan
     * byte[] sr = scan.getStartRow();                                    // returns the bytes of "user_100"
     * boolean incl = scan.includeStartRow();                             // returns false (exclusive)
     * }</pre>
     *
     * @param startRow the row key to start scanning from
     * @param inclusive {@code true} to include the start row, {@code false} to exclude it
     * @return this AnyScan instance for method chaining
     * @see #withStartRow(Object)
     * @see #includeStartRow()
     */
    public AnyScan withStartRow(final Object startRow, final boolean inclusive) {
        scan.withStartRow(toRowKeyBytes(startRow), inclusive);

        return this;
    }

    /**
     * Returns whether the stop row is included in this scan.
     * <p>
     * By default, the stop row is exclusive, meaning if a row exactly matches
     * the stop row key, it will not be included in the scan results. This method
     * returns the current inclusion setting for the stop row.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create();
     * boolean def = scan.includeStopRow();   // returns false (exclusive by default)
     *
     * scan.withStopRow("z", true);
     * boolean incl = scan.includeStopRow();  // returns true
     * }</pre>
     *
     * @return {@code true} if the stop row is included in the scan, {@code false} otherwise
     * @see #withStopRow(Object, boolean)
     * @see #getStopRow()
     */
    public boolean includeStopRow() {
        return scan.includeStopRow();
    }

    /**
     * Returns the stop row key for this scan.
     * <p>
     * The stop row defines the end of the scan range. Rows that are
     * lexicographically less than (if exclusive) or less than or equal to
     * (if inclusive) this row key will be considered for scanning.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create();
     * byte[] def = scan.getStopRow();   // returns an empty (length 0) array
     *
     * scan.withStopRow("z");
     * byte[] sr = scan.getStopRow();    // returns the bytes of "z"
     * }</pre>
     *
     * @return the stop row key as byte array, or empty array if scanning to table end
     * @see #withStopRow(Object)
     * @see #includeStopRow()
     */
    public byte[] getStopRow() {
        return scan.getStopRow();
    }

    //    /**
    //     * Set the stop row of the scan.
    //     * <p>
    //     * The scan will include rows that are lexicographically less than the provided stopRow.
    //     * <p>
    //     * <b>Note:</b> When doing a filter for a rowKey <u>Prefix</u> use
    //     * {@code setRowPrefixFilter(byte[])}. The 'trailing 0' will not yield the desired result.
    //     * </p>
    //     * @param stopRow row to end at (exclusive)
    //     * @return this
    //     * @throws IllegalArgumentException if stopRow does not meet criteria for a row key (when length
    //     *           exceeds {@link HConstants#MAX_ROW_LENGTH})
    //     * @deprecated use {@code withStopRow(byte[])} instead. This method may change the inclusive of
    //     *             the stop row to keep compatible with the old behavior.
    //     */
    //    @Deprecated
    //    public AnyScan setStopRow(final Object stopRow) {
    //        scan.setStopRow(toRowKeyBytes(stopRow));
    //
    //        return this;
    //    }

    /**
     * Sets the stop row for the scan with exclusive behavior by default.
     * <p>
     * The scan will stop before reaching this row key. Rows that are lexicographically
     * less than the stop row will be included in the scan results.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create().withStopRow("user_999");   // returns this scan
     * byte[] sr = scan.getStopRow();                             // returns the bytes of "user_999"
     * boolean incl = scan.includeStopRow();                      // returns false (exclusive)
     * }</pre>
     *
     * @param stopRow the row key to stop scanning before (exclusive by default)
     * @return this AnyScan instance for method chaining
     * @see #withStopRow(Object, boolean)
     * @see #includeStopRow()
     */
    public AnyScan withStopRow(final Object stopRow) {
        scan.withStopRow(toRowKeyBytes(stopRow));

        return this;
    }

    /**
     * Sets the stop row for the scan with explicit inclusive/exclusive behavior.
     * <p>
     * This method allows fine-grained control over whether the stop row itself
     * is included in the scan results. By default (when using {@link #withStopRow(Object)}),
     * the stop row is exclusive.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Include the stop row itself
     * AnyScan scan = AnyScan.create().withStopRow("user_999", true);   // returns this scan
     * byte[] sr = scan.getStopRow();                                   // returns the bytes of "user_999"
     * boolean incl = scan.includeStopRow();                            // returns true (inclusive)
     * }</pre>
     *
     * @param stopRow the row key to stop scanning at
     * @param inclusive {@code true} to include the stop row, {@code false} to exclude it
     * @return this AnyScan instance for method chaining
     * @see #withStopRow(Object)
     * @see #includeStopRow()
     */
    public AnyScan withStopRow(final Object stopRow, final boolean inclusive) {
        scan.withStopRow(toRowKeyBytes(stopRow), inclusive);

        return this;
    }

    /**
     * Sets the row prefix bounds via start/stop rows (the name is a misnomer &mdash; no
     * {@link Filter} is installed).
     *
     * <p>Despite the name, no server-side {@link Filter} is registered: this method delegates to
     * {@link Scan#setRowPrefixFilter(byte[])}, which configures startRow and stopRow to bracket
     * all keys beginning with the given prefix.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create().setRowPrefixFilter("user_");   // returns this scan
     * byte[] sr = scan.getStartRow();                                // returns the bytes of "user_"
     *
     * // A null prefix is tolerated and clears the range.
     * AnyScan cleared = AnyScan.create().setRowPrefixFilter((Object) null);   // no exception
     * int len = cleared.getStartRow().length;                                 // returns 0
     * }</pre>
     *
     * @param rowPrefix the row prefix; converted to bytes via {@link HBaseExecutor#toRowKeyBytes(Object)}
     * @return this {@link AnyScan} instance for method chaining
     * @deprecated Since HBase 2.5.0, scheduled for removal in 4.0.0. The name is misleading because
     *             no {@link Filter} is used. Use {@link #setStartStopRowForPrefixScan(byte[])} instead.
     */
    @Deprecated
    public AnyScan setRowPrefixFilter(final Object rowPrefix) {
        scan.setRowPrefixFilter(toRowKeyBytes(rowPrefix));

        return this;
    }

    /**
     * Sets the start and stop rows to scan every row whose key begins with the given prefix.
     *
     * <p>This is an optimized prefix scan that translates the supplied prefix into an
     * inclusive start row and a lexicographically computed exclusive stop row, avoiding the
     * cost of a server-side filter. Delegates to {@link Scan#setStartStopRowForPrefixScan(byte[])}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * byte[] prefix = Bytes.toBytes("user_");
     * AnyScan scan = AnyScan.create().setStartStopRowForPrefixScan(prefix);   // returns this scan
     * byte[] sr = scan.getStartRow();                                         // returns the bytes of "user_" (inclusive lower bound)
     * }</pre>
     *
     * @param rowPrefix the row key prefix; passed through {@link HBaseExecutor#toRowKeyBytes(Object)} (a {@code byte[]} input is returned as-is)
     * @return this AnyScan instance for method chaining
     * @see Scan#setStartStopRowForPrefixScan(byte[])
     */
    public AnyScan setStartStopRowForPrefixScan(final byte[] rowPrefix) {
        scan.setStartStopRowForPrefixScan(toRowKeyBytes(rowPrefix));

        return this;
    }

    /**
     * Returns the maximum number of versions to retrieve for each column.
     * <p>
     * HBase stores multiple versions of each cell value. This setting controls
     * how many historical versions will be returned. The default is 1 (only the
     * latest version). A value of Integer.MAX_VALUE indicates that all available
     * versions should be retrieved.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create();
     * int def = scan.getMaxVersions();   // returns 1 (latest version only)
     *
     * scan.readVersions(5);
     * int five = scan.getMaxVersions();  // returns 5
     *
     * scan.readAllVersions();
     * int all = scan.getMaxVersions();   // returns Integer.MAX_VALUE
     * }</pre>
     *
     * @return the maximum number of versions to retrieve per column
     * @see #readVersions(int)
     * @see #readAllVersions()
     */
    public int getMaxVersions() {
        return scan.getMaxVersions();
    }

    //    /**
    //     * Get all available versions.
    //     *
    //     * @param maxVersions
    //     * @return this
    //     * @deprecated It is easy to misunderstand with column family's max versions, so use
    //     *             {@code readAllVersions()} instead.
    //     */
    //    @Deprecated
    //    public AnyScan setMaxVersions(int maxVersions) {
    //        scan.setMaxVersions(maxVersions);
    //
    //        return this;
    //    }
    //
    //    /**
    //     * Get all available versions.
    //     * @return this
    //     * @deprecated It is easy to misunderstand with column family's max versions, so use
    //     *             {@code readAllVersions()} instead.
    //     */
    //    @Deprecated
    //    public AnyScan setMaxVersions() {
    //        scan.setMaxVersions();
    //
    //        return this;
    //    }

    /**
     * Sets the maximum number of versions to retrieve for each column.
     * <p>
     * By default, HBase returns only the latest version of each cell. Use this method
     * to retrieve multiple historical versions. Each version has a timestamp indicating
     * when it was written.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create().readVersions(5);   // returns this scan (last 5 versions)
     * int max = scan.getMaxVersions();                   // returns 5
     *
     * // Zero or negative values are stored as-is by this HBase version (no validation here).
     * AnyScan zero = AnyScan.create().readVersions(0);
     * int v = zero.getMaxVersions();     // returns 0
     * }</pre>
     *
     * <p>Unlike {@link AnyGet#readVersions(int)}, this does not reject values &lt; 1 (mirrors the HBase Scan client).</p>
     *
     * @param maxVersions the maximum number of versions to retrieve per column
     * @return this AnyScan instance for method chaining
     * @see #readAllVersions()
     * @see #getMaxVersions()
     */
    public AnyScan readVersions(final int maxVersions) {
        scan.readVersions(maxVersions);

        return this;
    }

    /**
     * Configures the scan to retrieve all available versions for each column.
     * <p>
     * This is equivalent to calling {@code readVersions(Integer.MAX_VALUE)}.
     * Use with caution as this may return a large amount of data if many versions exist.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create().readAllVersions();   // returns this scan
     * int max = scan.getMaxVersions();                     // returns Integer.MAX_VALUE
     * }</pre>
     *
     * @return this AnyScan instance for method chaining
     * @see #readVersions(int)
     */
    public AnyScan readAllVersions() {
        scan.readAllVersions();

        return this;
    }

    /**
     * Returns the batch size for this scan operation.
     * <p>
     * The batch size controls how many columns are retrieved per RPC call.
     * This is different from caching, which controls how many rows are retrieved.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create();
     * int def = scan.getBatch();   // returns -1 (no batch limit by default)
     *
     * scan.setBatch(100);
     * int batch = scan.getBatch(); // returns 100
     * }</pre>
     *
     * @return the current batch size
     * @see #setBatch(int)
     * @see #getCaching()
     */
    public int getBatch() {
        return scan.getBatch();
    }

    /**
     * Sets the batch size for this scan operation.
     * <p>
     * The batch size determines the maximum number of columns to retrieve per RPC call.
     * This can help manage memory usage when scanning rows with many columns.
     * Set to -1 for unlimited batch size (default).
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create().setBatch(100);   // returns this scan (max 100 columns per RPC)
     * int batch = scan.getBatch();                     // returns 100
     *
     * // -1 means unlimited (the default).
     * AnyScan unlimited = AnyScan.create().setBatch(-1);
     * int b = unlimited.getBatch();  // returns -1
     * }</pre>
     *
     * @param batch the batch size (number of columns per RPC)
     * @return this AnyScan instance for method chaining
     * @see #getBatch()
     * @see #setCaching(int)
     */
    public AnyScan setBatch(final int batch) {
        scan.setBatch(batch);

        return this;
    }

    /**
     * Returns the maximum number of results to return per column family.
     * <p>
     * This setting limits how many cells are returned for each column family,
     * which can be useful for controlling result set size.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create();
     * int def = scan.getMaxResultsPerColumnFamily();   // returns -1 (no limit)
     *
     * scan.setMaxResultsPerColumnFamily(10);
     * int limit = scan.getMaxResultsPerColumnFamily(); // returns 10
     * }</pre>
     *
     * @return the maximum number of results per column family, or -1 if no limit is set
     * @see #setMaxResultsPerColumnFamily(int)
     */
    public int getMaxResultsPerColumnFamily() {
        return scan.getMaxResultsPerColumnFamily();
    }

    /**
     * Sets the maximum number of results to return per column family.
     * <p>
     * This setting limits how many cells are returned for each column family within a row.
     * Setting this can help reduce the amount of data transferred and improve performance
     * when you only need a subset of columns from large column families.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Limit to 10 most recent columns per family
     * AnyScan scan = AnyScan.create()
     *                      .setMaxResultsPerColumnFamily(10)   // returns this scan
     *                      .readVersions(1);
     * int limit = scan.getMaxResultsPerColumnFamily();   // returns 10
     *
     * // Pagination: Get columns 10-20 from each family
     * AnyScan paginatedScan = AnyScan.create()
     *                                .setRowOffsetPerColumnFamily(10)
     *                                .setMaxResultsPerColumnFamily(10);
     * }</pre>
     *
     * @param limit the maximum number of results per column family
     * @return this AnyScan instance for method chaining
     * @see #getMaxResultsPerColumnFamily()
     * @see #setRowOffsetPerColumnFamily(int)
     */
    public AnyScan setMaxResultsPerColumnFamily(final int limit) {
        scan.setMaxResultsPerColumnFamily(limit);

        return this;
    }

    /**
     * Returns the row offset per column family.
     * <p>
     * This offset specifies how many cells to skip at the beginning of each
     * column family before returning results.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create();
     * int def = scan.getRowOffsetPerColumnFamily();   // returns 0 (no offset)
     *
     * scan.setRowOffsetPerColumnFamily(20);
     * int offset = scan.getRowOffsetPerColumnFamily(); // returns 20
     * }</pre>
     *
     * @return the current row offset per column family, or 0 if no offset is set
     * @see #setRowOffsetPerColumnFamily(int)
     */
    public int getRowOffsetPerColumnFamily() {
        return scan.getRowOffsetPerColumnFamily();
    }

    /**
     * Sets the row offset per column family.
     * <p>
     * This setting specifies how many cells to skip at the beginning of each column family.
     * Combined with {@link #setMaxResultsPerColumnFamily(int)}, this enables pagination
     * within column families.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Skip first 20 columns and get next 10 from each family
     * AnyScan scan = AnyScan.create()
     *                      .setRowOffsetPerColumnFamily(20)   // returns this scan
     *                      .setMaxResultsPerColumnFamily(10);
     * int offset = scan.getRowOffsetPerColumnFamily();   // returns 20
     *
     * // Pagination logic
     * int pageSize = 10;
     * int pageNumber = 3;
     * AnyScan pageThree = AnyScan.create()
     *                            .setRowOffsetPerColumnFamily(pageNumber * pageSize)
     *                            .setMaxResultsPerColumnFamily(pageSize);
     * }</pre>
     *
     * @param offset the number of cells to skip per column family (a negative value is stored as-is
     *               by this HBase version, with no validation)
     * @return this AnyScan instance for method chaining
     * @see #getRowOffsetPerColumnFamily()
     * @see #setMaxResultsPerColumnFamily(int)
     */
    public AnyScan setRowOffsetPerColumnFamily(final int offset) {
        scan.setRowOffsetPerColumnFamily(offset);

        return this;
    }

    /**
     * Returns the caching size for this scan operation.
     * <p>
     * Caching determines how many rows are fetched from the server at once.
     * Higher values improve throughput but use more memory.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create();
     * int def = scan.getCaching();   // returns -1 (server/connection default is used)
     *
     * scan.setCaching(1000);
     * int caching = scan.getCaching(); // returns 1000
     * }</pre>
     *
     * @return the current caching size (number of rows)
     * @see #setCaching(int)
     * @see #getBatch()
     */
    public int getCaching() {
        return scan.getCaching();
    }

    /**
     * Sets the caching size for this scan operation.
     * <p>
     * Caching controls how many rows are prefetched from the server in a single RPC call.
     * Higher caching values improve throughput for large scans but increase memory usage.
     * The default is usually sufficient, but you may want to increase it for full table scans.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create().setCaching(1000);   // returns this scan (fetch 1000 rows at a time)
     * int caching = scan.getCaching();                    // returns 1000
     * }</pre>
     *
     * @param caching the number of rows to cache per RPC call
     * @return this AnyScan instance for method chaining
     * @see #getCaching()
     * @see #setBatch(int)
     */
    public AnyScan setCaching(final int caching) {
        scan.setCaching(caching);

        return this;
    }

    /**
     * Returns whether block caching is enabled for this scan.
     * <p>
     * Block caching stores frequently accessed data blocks in memory to improve
     * read performance. For full table scans, you may want to disable this to
     * avoid evicting frequently-used blocks from the cache.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create();
     * boolean def = scan.getCacheBlocks();   // returns true (enabled by default)
     *
     * scan.setCacheBlocks(false);
     * boolean off = scan.getCacheBlocks();   // returns false
     * }</pre>
     *
     * @return {@code true} if block caching is enabled, {@code false} otherwise
     * @see #setCacheBlocks(boolean)
     */
    public boolean getCacheBlocks() { // NOSONAR
        return scan.getCacheBlocks();
    }

    /**
     * Sets whether to cache blocks for this scan operation.
     * <p>
     * Block caching can improve performance for repeated scans of the same data,
     * but for one-time full table scans, it's often better to disable caching
     * to avoid polluting the block cache and evicting more frequently-used data.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Disable caching for a full table scan
     * AnyScan scan = AnyScan.create().setCacheBlocks(false);   // returns this scan
     * boolean enabled = scan.getCacheBlocks();                 // returns false
     * }</pre>
     *
     * @param cacheBlocks {@code true} to enable block caching, {@code false} to disable
     * @return this AnyScan instance for method chaining
     * @see #getCacheBlocks()
     */
    public AnyScan setCacheBlocks(final boolean cacheBlocks) {
        scan.setCacheBlocks(cacheBlocks);

        return this;
    }

    /**
     * Returns the maximum result size in bytes for this scan.
     * <p>
     * This setting limits the total amount of data that can be returned by a single
     * RPC call, helping to prevent out-of-memory errors for large scans.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create();
     * long def = scan.getMaxResultSize();   // returns -1 (no explicit limit)
     *
     * scan.setMaxResultSize(1048576L);
     * long size = scan.getMaxResultSize();  // returns 1048576
     * }</pre>
     *
     * @return the maximum result size in bytes
     * @see #setMaxResultSize(long)
     */
    public long getMaxResultSize() {
        return scan.getMaxResultSize();
    }

    /**
     * Sets the maximum result size in bytes for this scan.
     * <p>
     * This setting provides a safeguard against retrieving too much data in a single
     * RPC call. The scan will return fewer rows if necessary to stay under this limit.
     * This is particularly useful for preventing out-of-memory errors when scanning
     * tables with very large rows.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Limit result size to 10MB per RPC call
     * AnyScan scan = AnyScan.create()
     *                      .setMaxResultSize(10 * 1024 * 1024);   // returns this scan
     * long size = scan.getMaxResultSize();                        // returns 10485760
     *
     * // For tables with large rows, use smaller result size
     * AnyScan safeScan = AnyScan.create()
     *                           .setMaxResultSize(5 * 1024 * 1024)
     *                           .setCaching(100);
     * }</pre>
     *
     * @param maxResultSize the maximum result size in bytes
     * @return this AnyScan instance for method chaining
     * @see #getMaxResultSize()
     */
    public AnyScan setMaxResultSize(final long maxResultSize) {
        scan.setMaxResultSize(maxResultSize);

        return this;
    }

    /**
     * Returns the maximum number of rows to return from this scan.
     * <p>
     * This limit caps the total number of rows that will be returned by the scan,
     * regardless of how many rows match the scan criteria.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create();
     * int def = scan.getLimit();   // returns -1 (no limit)
     *
     * scan.setLimit(100);
     * int limit = scan.getLimit(); // returns 100
     * }</pre>
     *
     * @return the maximum number of rows to return
     * @see #setLimit(int)
     * @see #setOneRowLimit()
     */
    public int getLimit() {
        return scan.getLimit();
    }

    /**
     * Sets the maximum number of rows to return from this scan.
     * <p>
     * This method limits the total number of rows returned by the scan, which can
     * improve performance when you only need the first N matching rows. The scan
     * will automatically close after returning this many rows.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create().setLimit(100);   // returns this scan (at most 100 rows)
     * int limit = scan.getLimit();                     // returns 100
     *
     * // A limit of 0 is stored as-is (an immediately-exhausted scan).
     * AnyScan none = AnyScan.create().setLimit(0);
     * int zero = none.getLimit();    // returns 0
     * }</pre>
     *
     * @param limit the maximum number of rows to return
     * @return this AnyScan instance for method chaining
     * @see #getLimit()
     * @see #setOneRowLimit()
     */
    public AnyScan setLimit(final int limit) {
        scan.setLimit(limit);

        return this;
    }

    /**
     * Configures the scan to return at most one row.
     * <p>
     * This is a convenience method equivalent to {@code setLimit(1)}.
     * It's useful when you know you only need a single row and want to
     * optimize the scan accordingly.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create().setOneRowLimit();   // returns this scan
     * int limit = scan.getLimit();                        // returns 1
     * }</pre>
     *
     * @return this AnyScan instance for method chaining
     * @see #setLimit(int)
     */
    public AnyScan setOneRowLimit() {
        scan.setOneRowLimit();

        return this;
    }

    /**
     * Returns whether a {@link Filter} has been set for this scan.
     *
     * <p>Filters allow for server-side filtering of rows and columns, reducing the amount of data
     * transferred to the client. The filter itself is configured via the inherited
     * {@link AnyQuery#setFilter(Filter)} method.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create();
     * boolean none = scan.hasFilter();   // returns false
     *
     * scan.setFilter(new PrefixFilter(Bytes.toBytes("user_")));
     * boolean has = scan.hasFilter();    // returns true
     * }</pre>
     *
     * @return {@code true} if a filter has been set, {@code false} otherwise
     * @see AnyQuery#setFilter(Filter)
     */
    public boolean hasFilter() {
        return scan.hasFilter();
    }

    /**
     * Returns whether this scan will proceed in reverse (descending) row-key order.
     *
     * <p>In a reversed scan the start row is interpreted as the higher bound and the stop row as
     * the lower bound; the scanner walks rows from high keys to low keys. This can be useful for
     * retrieving the most recent data first in time-series applications keyed by ascending time.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create();
     * boolean def = scan.isReversed();   // returns false (forward scan)
     *
     * scan.setReversed(true);
     * boolean rev = scan.isReversed();   // returns true
     * }</pre>
     *
     * @return {@code true} if the scan is reversed, {@code false} otherwise
     * @see #setReversed(boolean)
     */
    public boolean isReversed() {
        return scan.isReversed();
    }

    /**
     * Sets whether this scan should proceed in reverse (descending) row-key order.
     *
     * <p>When set to {@code true}, the start row becomes the upper bound and the stop row becomes
     * the lower bound, and rows are visited in descending lexicographic order. This is useful for
     * time-series data where the most recent entries are wanted first. Reversed scans typically
     * have slightly lower performance than forward scans because HBase storage is optimized for
     * ascending iteration.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Reversed: start at the high key (Dec 31), stop before the low key (Jan 01).
     * AnyScan scan = AnyScan.create()
     *                      .withStartRow("2024-12-31")
     *                      .withStopRow("2024-01-01")
     *                      .setReversed(true);   // returns this scan
     * boolean rev = scan.isReversed();           // returns true
     * }</pre>
     *
     * @param reversed {@code true} to scan in reverse order, {@code false} for normal order
     * @return this {@link AnyScan} instance for method chaining
     * @see #isReversed()
     */
    public AnyScan setReversed(final boolean reversed) {
        scan.setReversed(reversed);

        return this;
    }

    /**
     * Returns whether partial results are allowed for this scan.
     * <p>
     * When enabled, the server may return partial results (rows split across multiple
     * responses) to better manage memory and prevent timeouts for rows with many columns.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create();
     * boolean def = scan.getAllowPartialResults();   // returns false (disabled by default)
     *
     * scan.setAllowPartialResults(true);
     * boolean allowed = scan.getAllowPartialResults(); // returns true
     * }</pre>
     *
     * @return {@code true} if partial results are allowed, {@code false} otherwise
     * @see #setAllowPartialResults(boolean)
     */
    public boolean getAllowPartialResults() { // NOSONAR
        return scan.getAllowPartialResults();
    }

    /**
     * Sets whether to allow partial results for this scan.
     * <p>
     * When set to true, HBase may split large rows across multiple Result objects
     * to avoid memory issues and timeouts. This is useful for rows with many columns
     * or large cell values. You'll need to handle reassembling the partial results
     * in your application code.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Enable partial results for tables with very wide rows
     * AnyScan scan = AnyScan.create()
     *                      .addFamily("wideFamily")
     *                      .setAllowPartialResults(true);   // returns this scan
     * boolean allowed = scan.getAllowPartialResults();      // returns true
     *
     * // Process partial results
     * ResultScanner scanner = table.getScanner(scan.val());
     * for (Result result : scanner) {
     *     // Handle potentially partial result
     *     // Check result.mayHaveMoreCellsInRow() to detect partials
     * }
     * }</pre>
     *
     * @param allowPartialResults {@code true} to allow partial results, {@code false} otherwise
     * @return this AnyScan instance for method chaining
     * @see #getAllowPartialResults()
     */
    public AnyScan setAllowPartialResults(final boolean allowPartialResults) {
        scan.setAllowPartialResults(allowPartialResults);

        return this;
    }

    /**
     * Returns whether this is a raw scan that includes delete markers.
     * <p>
     * Raw scans include all data including cells marked for deletion (tombstones).
     * This is primarily useful for debugging or implementing custom compaction logic.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create();
     * boolean def = scan.isRaw();   // returns false (normal scan)
     *
     * scan.setRaw(true);
     * boolean raw = scan.isRaw();    // returns true
     * }</pre>
     *
     * @return {@code true} if this is a raw scan, {@code false} otherwise
     * @see #setRaw(boolean)
     */
    public boolean isRaw() {
        return scan.isRaw();
    }

    /**
     * Sets whether this should be a raw scan that includes delete markers.
     * <p>
     * Raw scans include cells that have been marked for deletion but not yet compacted.
     * This is primarily useful for debugging, system administration, or implementing
     * custom compaction strategies. Normal applications should not need raw scans.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Raw scan to see deleted cells (for debugging)
     * AnyScan rawScan = AnyScan.create()
     *                          .addFamily("data")
     *                          .setRaw(true)            // returns this scan
     *                          .readAllVersions();
     * boolean raw = rawScan.isRaw();   // returns true
     *
     * ResultScanner scanner = table.getScanner(rawScan.val());
     * for (Result result : scanner) {
     *     // Will include cells marked with DELETE markers
     *     for (Cell cell : result.rawCells()) {
     *         // Process cells including tombstones
     *     }
     * }
     * }</pre>
     *
     * @param raw {@code true} for a raw scan, {@code false} for a normal scan
     * @return this AnyScan instance for method chaining
     * @see #isRaw()
     */
    public AnyScan setRaw(final boolean raw) {
        scan.setRaw(raw);

        return this;
    }

    //    /**
    //     * Get whether this scan is a small scan.
    //     *
    //     * @return {@code true} if small scan
    //     * @deprecated since 2.0.0. See the comment of {@code setSmall(boolean)}
    //     */
    //    @Deprecated
    //    public boolean isSmall() {
    //        return scan.isSmall();
    //    }
    //
    //    /**
    //     * Set whether this scan is a small scan
    //     * <p>
    //     * Small scan should use pread and big scan can use seek + read seek + read is fast but can cause
    //     * two problem (1) resource contention (2) cause too much network io [89-fb] Using pread for
    //     * non-compaction read request https://issues.apache.org/jira/browse/HBASE-7266 On the other hand,
    //     * if setting it true, we would do openScanner,next,closeScanner in one RPC call. It means the
    //     * better performance for small scan. [HBASE-9488]. Generally, if the scan range is within one
    //     * data block(64KB), it could be considered as a small scan.
    //     *
    //     * @param small
    //     * @return
    //     * @see Scan#setLimit(int)
    //     * @see Scan#setReadType(ReadType)
    //     * @deprecated since 2.0.0. Use {@code setLimit(int)} and {@code setReadType(ReadType)} instead.
    //     *             And for the one rpc optimization, now we will also fetch data when openScanner, and
    //     *             if the number of rows reaches the limit then we will close the scanner
    //     *             automatically which means we will fall back to one rpc.
    //     */
    //    @Deprecated
    //    public AnyScan setSmall(boolean small) {
    //        scan.setSmall(small);
    //
    //        return this;
    //    }

    /**
     * Returns whether scan metrics collection is enabled for this scan.
     * <p>
     * When enabled, HBase collects detailed performance metrics about the scan
     * operation, which can be useful for monitoring and performance tuning.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create();
     * boolean def = scan.isScanMetricsEnabled();   // returns false (disabled by default)
     *
     * scan.setScanMetricsEnabled(true);
     * boolean on = scan.isScanMetricsEnabled();     // returns true
     * }</pre>
     *
     * @return {@code true} if scan metrics are enabled, {@code false} otherwise
     * @see #setScanMetricsEnabled(boolean)
     */
    public boolean isScanMetricsEnabled() {
        return scan.isScanMetricsEnabled();
    }

    /**
     * Sets whether to collect scan metrics for this scan operation.
     * <p>
     * When enabled, HBase will collect detailed metrics about the scan's performance,
     * including the number of regions scanned, bytes transferred, RPC calls made, etc.
     * These metrics can be retrieved from the ResultScanner after the scan completes.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create().setScanMetricsEnabled(true);   // returns this scan
     * boolean on = scan.isScanMetricsEnabled();                      // returns true
     * // After scanning, retrieve metrics from ResultScanner
     * }</pre>
     *
     * @param enabled {@code true} to enable metrics collection, {@code false} to disable
     * @return this AnyScan instance for method chaining
     * @see #isScanMetricsEnabled()
     */
    public AnyScan setScanMetricsEnabled(final boolean enabled) {
        scan.setScanMetricsEnabled(enabled);

        return this;
    }

    //    /**
    //     * Gets the scan metrics.
    //     *
    //     * @return Metrics on this Scan, if metrics were enabled.
    //     * @see Scan#setScanMetricsEnabled(boolean)
    //     * @deprecated Use {@link ResultScanner#getScanMetrics()} instead. And notice that, please do not
    //     *             use this method and {@link ResultScanner#getScanMetrics()} together, the metrics
    //     *             will be messed up.
    //     */
    //    @Deprecated
    //    public ScanMetrics getScanMetrics() {
    //        return scan.getScanMetrics();
    //    }

    /**
     * Returns whether asynchronous prefetching is enabled for this scan.
     * <p>
     * Async prefetching allows the client to fetch the next batch of rows in the background
     * while processing the current batch, which can improve throughput for large scans.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create();
     * Boolean def = scan.isAsyncPrefetch();   // returns null (not explicitly set)
     *
     * scan.setAsyncPrefetch(true);
     * Boolean on = scan.isAsyncPrefetch();    // returns Boolean.TRUE
     * }</pre>
     *
     * @return {@code true} if async prefetch is enabled, {@code false} if disabled, or {@code null} if not set
     * @see #setAsyncPrefetch(boolean)
     */
    public Boolean isAsyncPrefetch() {
        return scan.isAsyncPrefetch();
    }

    /**
     * Sets whether to enable asynchronous prefetching for this scan.
     * <p>
     * When enabled, the client will asynchronously fetch the next batch of scan results
     * in the background while you're processing the current batch. This can significantly
     * improve throughput for large table scans by overlapping computation and I/O.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Enable async prefetch for large table scans
     * AnyScan scan = AnyScan.create()
     *                      .setCaching(1000)
     *                      .setAsyncPrefetch(true);   // returns this scan
     * Boolean on = scan.isAsyncPrefetch();            // returns Boolean.TRUE
     *
     * // Optimal for processing-heavy scans where I/O can overlap computation
     * AnyScan optimizedScan = AnyScan.create()
     *                                .addFamily("data")
     *                                .setCaching(500)
     *                                .setAsyncPrefetch(true);
     * }</pre>
     *
     * @param asyncPrefetch {@code true} to enable async prefetching, {@code false} to disable
     * @return this AnyScan instance for method chaining
     * @see #isAsyncPrefetch()
     */
    public AnyScan setAsyncPrefetch(final boolean asyncPrefetch) {
        scan.setAsyncPrefetch(asyncPrefetch);

        return this;
    }

    /**
     * Returns the read type for this scan operation.
     * <p>
     * The read type controls how HBase reads data from disk, affecting performance
     * characteristics. Options include STREAM (for sequential reads) and PREAD
     * (for random reads).
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create();
     * ReadType def = scan.getReadType();   // returns ReadType.DEFAULT
     *
     * scan.setReadType(ReadType.STREAM);
     * ReadType rt = scan.getReadType();    // returns ReadType.STREAM
     * }</pre>
     *
     * @return the ReadType for this scan
     * @see #setReadType(ReadType)
     */
    public ReadType getReadType() {
        return scan.getReadType();
    }

    /**
     * Sets the read type for this scan operation.
     * <p>
     * The read type controls how HBase reads data from the underlying storage:
     * <ul>
     *   <li>{@link ReadType#STREAM} - Optimized for sequential, large scans. Uses streaming reads.</li>
     *   <li>{@link ReadType#PREAD} - Optimized for random access patterns. Uses positioned reads.</li>
     *   <li>{@link ReadType#DEFAULT} - Let HBase choose the optimal read type based on scan characteristics.</li>
     * </ul>
     * <p>Note: HBase may override your choice in certain cases. For example, it will always use
     * pread for get scans regardless of this setting.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // For large sequential table scans
     * AnyScan fullScan = AnyScan.create().setReadType(ReadType.STREAM);   // returns this scan
     * ReadType rt = fullScan.getReadType();                               // returns ReadType.STREAM
     *
     * // For small, targeted scans
     * AnyScan smallScan = AnyScan.create()
     *                           .withStartRow("key")
     *                           .setOneRowLimit()
     *                           .setReadType(ReadType.PREAD);
     * }</pre>
     *
     * @param readType the type of read to perform (STREAM, PREAD, or DEFAULT)
     * @return this AnyScan instance for method chaining
     * @see #getReadType()
     * @see ReadType
     */
    public AnyScan setReadType(final ReadType readType) {
        scan.setReadType(readType);

        return this;
    }

    /**
     * Returns whether cursor results are needed for this scan.
     * <p>
     * Cursor results provide position information that can be used to resume a scan
     * from a specific point, which is useful for implementing pagination or handling
     * interrupted scans.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create();
     * boolean def = scan.isNeedCursorResult();   // returns false (disabled by default)
     *
     * scan.setNeedCursorResult(true);
     * boolean on = scan.isNeedCursorResult();    // returns true
     * }</pre>
     *
     * @return {@code true} if cursor results are needed, {@code false} otherwise
     * @see #setNeedCursorResult(boolean)
     * @see #createScanFromCursor(Cursor)
     */
    public boolean isNeedCursorResult() {
        return scan.isNeedCursorResult();
    }

    /**
     * Sets whether cursor results are needed for this scan.
     * <p>
     * When enabled, scan results will include cursor information that can be used
     * to resume the scan from that exact position later. This is particularly useful
     * for implementing pagination where you want to continue scanning from where you
     * left off without re-scanning previous rows.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create().setNeedCursorResult(true);   // returns this scan
     * boolean on = scan.isNeedCursorResult();                      // returns true
     * // Later, use the cursor to resume scanning
     * }</pre>
     *
     * @param needCursorResult {@code true} to enable cursor results, {@code false} to disable
     * @return this AnyScan instance for method chaining
     * @see #isNeedCursorResult()
     * @see #createScanFromCursor(Cursor)
     */
    public AnyScan setNeedCursorResult(final boolean needCursorResult) {
        scan.setNeedCursorResult(needCursorResult);

        return this;
    }

    /**
     * Returns a hash code value for this AnyScan.
     * <p>
     * The hash code is based on the underlying Scan object, ensuring consistency
     * with the equals method. Two AnyScan objects that are equal according to
     * {@link #equals(Object)} will have the same hash code.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create();
     * int hash = scan.hashCode();   // returns scan.val().hashCode()
     * }</pre>
     *
     * @return a hash code value for this object
     */
    @Override
    public int hashCode() {
        return scan.hashCode();
    }

    /**
     * Indicates whether some other object is "equal to" this AnyScan.
     * <p>
     * Equality is delegated to the underlying {@link Scan}, which does not override
     * {@link Object#equals(Object)} — so comparison is by reference identity, not by scan
     * configuration. Two distinct AnyScan instances are therefore equal only when they wrap the
     * very same {@link Scan} object; two separately built scans with identical settings are not
     * equal.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create();
     * boolean self = scan.equals(scan);            // returns true (same instance)
     * boolean nullCmp = scan.equals(null);         // returns false
     * boolean other = scan.equals("not a scan");   // returns false (not an AnyScan)
     *
     * // Edge: two independently built scans with identical settings are NOT equal.
     * boolean same = AnyScan.create().equals(AnyScan.create());   // returns false
     * }</pre>
     *
     * @param obj the reference object with which to compare
     * @return {@code true} if this object is the same as the obj argument;
     *         {@code false} otherwise
     */
    @SuppressFBWarnings
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof final AnyScan other) {
            return scan.equals(other.scan);
        }

        return false;
    }

    /**
     * Returns a string representation of this AnyScan.
     * <p>
     * The string representation includes detailed information about all scan
     * parameters such as row ranges, column families, qualifiers, filters,
     * time ranges, and other configuration settings. This is useful for
     * debugging and logging purposes.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan scan = AnyScan.create().addFamily("cf");
     * String text = scan.toString();   // returns a non-null description of the scan
     * }</pre>
     *
     * @return a string representation of this AnyScan; never null
     */
    @Override
    public String toString() {
        return scan.toString();
    }
}
