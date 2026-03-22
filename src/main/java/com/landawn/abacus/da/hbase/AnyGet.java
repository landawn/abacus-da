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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.io.TimeRange;

import com.landawn.abacus.annotation.SuppressFBWarnings;
import com.landawn.abacus.util.N;

/**
 * A fluent builder wrapper for HBase {@link Get} operations that simplifies row retrieval by handling
 * automatic type conversion between Java objects and HBase byte arrays. This class eliminates the need
 * for manual byte array conversions when working with HBase get operations.
 *
 * <p>AnyGet provides a chainable API for constructing HBase Get operations with support for:
 * <ul>
 * <li><strong>Row Key Operations</strong>: Automatic conversion of Java objects to row keys</li>
 * <li><strong>Column Family/Qualifier Selection</strong>: String-based column specification</li>
 * <li><strong>Versioning Control</strong>: Multi-version data retrieval with time ranges</li>
 * <li><strong>Performance Optimization</strong>: Cache control and result limiting</li>
 * <li><strong>Existence Checking</strong>: Efficient row existence verification</li>
 * </ul>
 *
 * <h2>Usage Examples</h2>
 *
 * <h3>Basic Usage</h3>
 * <pre>{@code
 * // Basic row retrieval
 * AnyGet get = AnyGet.of("user123")
 *                   .addColumn("info", "name")
 *                   .addColumn("info", "email");
 *
 * // Multi-family retrieval
 * AnyGet familyGet = AnyGet.of("user123")
 *                          .addFamily("info")
 *                          .addFamily("prefs");
 *
 * // Versioned data retrieval
 * AnyGet versionGet = AnyGet.of("user123")
 *                           .addColumn("info", "name")
 *                           .readVersions(5)
 *                           .setTimeRange(startTime, endTime);
 *
 * // Existence check optimization
 * AnyGet existsGet = AnyGet.of("user123")
 *                          .setCheckExistenceOnly(true);
 *
 * // Performance-optimized get
 * AnyGet optimizedGet = AnyGet.of("user123")
 *                             .addFamily("info")
 *                             .setCacheBlocks(false)
 *                             .setMaxResultsPerColumnFamily(100);
 * }</pre>
 *
 * <h3>Key Features:</h3>
 * <ul>
 * <li><strong>Type Safety</strong>: Automatic conversion of row keys from Java objects to byte arrays</li>
 * <li><strong>Fluent API</strong>: Chainable method calls for readable query construction</li>
 * <li><strong>Version Control</strong>: Support for multi-version reads with configurable limits</li>
 * <li><strong>Time Range Filtering</strong>: Retrieve data within specific time windows</li>
 * <li><strong>Column Filtering</strong>: Precise control over which columns to retrieve</li>
 * <li><strong>Performance Tuning</strong>: Control over caching, result limits, and offset handling</li>
 * </ul>
 *
 * <h3>Performance Considerations:</h3>
 * <ul>
 * <li><strong>Cache Control</strong>: Use {@code setCacheBlocks(false)} for large scans</li>
 * <li><strong>Column Selection</strong>: Specify only needed columns to reduce network transfer</li>
 * <li><strong>Version Limiting</strong>: Use {@code readVersions()} to control data volume</li>
 * <li><strong>Existence Checks</strong>: Use {@code setCheckExistenceOnly(true)} for efficient existence testing</li>
 * </ul>
 *
 * @see Get
 * @see AnyQuery
 * @see HBaseExecutor
 * @see <a href="http://hbase.apache.org/devapidocs/index.html">Apache HBase Java API Documentation</a>
 */
public final class AnyGet extends AnyQuery<AnyGet> implements Row {

    private final Get get;

    /**
     * Constructs a new AnyGet instance for the specified row key.
     * The row key is automatically converted to the appropriate byte array format.
     *
     * @param rowKey the row key object to retrieve, automatically converted to bytes
     */
    AnyGet(final Object rowKey) {
        super(new Get(toRowKeyBytes(rowKey)));
        get = (Get) query;
    }

    /**
     * Constructs a new AnyGet instance for a partial row key slice.
     * Enables retrieval operations on composite or structured row keys.
     *
     * @param rowKey the row key object whose byte representation will be sliced
     * @param rowOffset the starting position (0-based) within the row key bytes
     * @param rowLength the number of bytes to use from the row key, starting at offset
     */
    AnyGet(final Object rowKey, final int rowOffset, final int rowLength) {
        super(new Get(toRowKeyBytes(rowKey), rowOffset, rowLength));
        get = (Get) query;
    }

    /**
     * Constructs a new AnyGet instance from a ByteBuffer row key.
     * Useful for NIO operations or when the row key is already in ByteBuffer format.
     *
     * @param rowKey the row key as a ByteBuffer
     */
    AnyGet(final ByteBuffer rowKey) {
        super(new Get(rowKey));
        get = (Get) query;
    }

    /**
     * Constructs a new AnyGet instance by wrapping an existing Get object.
     * All configuration from the original Get is preserved.
     *
     * @param get the existing HBase Get object to wrap
     */
    AnyGet(final Get get) {
        super(get);
        this.get = (Get) query;
    }

    /**
     * Creates a new AnyGet instance for the specified row key.
     *
     * <p>This is the primary factory method for creating AnyGet instances. The row key
     * will be automatically converted to the appropriate byte array format for HBase operations.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyGet get = AnyGet.of("user123")
     *                    .addColumn("info", "name");
     * Result result = executor.get("users_table", get);
     * }</pre>
     *
     * @param rowKey the row key object to retrieve, automatically converted to bytes
     * @return a new AnyGet instance configured with the specified row key
     * @throws IllegalArgumentException if rowKey is null
     * @see #of(Object, int, int)
     * @see #of(ByteBuffer)
     * @see #of(Get)
     */
    public static AnyGet of(final Object rowKey) {
        return new AnyGet(rowKey);
    }

    /**
     * Creates a new AnyGet instance for a partial row key slice.
     *
     * <p>This method allows for more precise control over row key matching by specifying
     * an offset and length within the converted row key byte array. This is useful for
     * prefix-based row key schemes or when working with composite keys.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Use first 7 bytes of a composite key as row key
     * String compositeKey = "user123_session456";
     * AnyGet get = AnyGet.of(compositeKey, 0, 7) // "user123"
     *                    .addFamily("sessions");
     * }</pre>
     *
     * @param rowKey the row key object to retrieve, automatically converted to bytes
     * @param rowOffset the starting offset within the row key byte array
     * @param rowLength the number of bytes to use from the row key
     * @return a new AnyGet instance configured with the partial row key
     * @throws IllegalArgumentException if rowKey is null, rowOffset is negative, or rowLength is invalid
     * @see #of(Object)
     * @see #of(ByteBuffer)
     */
    public static AnyGet of(final Object rowKey, final int rowOffset, final int rowLength) {
        return new AnyGet(rowKey, rowOffset, rowLength);
    }

    /**
     * Creates a new AnyGet instance for the specified ByteBuffer row key.
     * 
     * <p>This factory method creates a get operation using a ByteBuffer as the row key.
     * This is useful when working with NIO operations or when the row key is already
     * in ByteBuffer format. The ByteBuffer's current position and limit determine
     * which bytes are used for the row key.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ByteBuffer keyBuffer = ByteBuffer.wrap("user123".getBytes());
     * AnyGet get = AnyGet.of(keyBuffer)
     *                    .addFamily("profile");
     * }</pre>
     * 
     * @param rowKey the row key as a ByteBuffer, must not be null and must have remaining bytes
     * @return a new AnyGet instance configured for the specified row
     * @throws IllegalArgumentException if rowKey is null or has no remaining bytes
     * @see #of(Object)
     * @see java.nio.ByteBuffer
     */
    public static AnyGet of(final ByteBuffer rowKey) {
        return new AnyGet(rowKey);
    }

    /**
     * Creates a new AnyGet instance by copying an existing HBase Get operation.
     * 
     * <p>This factory method creates a new AnyGet instance that wraps a copy of the
     * provided HBase Get object. All configuration, column specifications, and
     * attributes from the original get are preserved. This is useful for converting
     * existing HBase Get objects to the AnyGet wrapper for additional functionality.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Get existingGet = new Get(Bytes.toBytes("user123"));
     * existingGet.addFamily(Bytes.toBytes("profile"));
     * 
     * AnyGet get = AnyGet.of(existingGet)
     *                    .addColumn("stats", "login_count");
     * }</pre>
     * 
     * @param get the existing HBase Get object to copy
     * @return a new AnyGet instance that wraps a copy of the specified get
     * @throws IllegalArgumentException if get is null
     * @see org.apache.hadoop.hbase.client.Get
     * @see #val()
     */
    public static AnyGet of(final Get get) {
        return new AnyGet(get);
    }

    /**
     * Returns the underlying HBase Get object for direct access to native HBase operations.
     *
     * <p>This method provides access to the wrapped HBase Get instance, allowing for advanced
     * operations not directly exposed by the AnyGet fluent API. Use this method when you need
     * to access HBase-specific functionality or when integrating with existing HBase code.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyGet anyGet = AnyGet.of("user123").addFamily("info");
     * Get hbaseGet = anyGet.val();
     * Result result = table.get(hbaseGet);   // Use with native HBase API
     * }</pre>
     *
     * @return the underlying HBase Get object
     * @see Get
     * @see #of(Get)
     */
    public Get val() {
        return get;
    }

    /**
     * Adds a column family to retrieve all columns from that family.
     *
     * <p>When a family is added without specific qualifiers, all columns within that
     * family will be retrieved. This is useful for getting all data associated with
     * a particular column family in HBase.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyGet get = AnyGet.of("user123")
     *                    .addFamily("info")
     *                    .addFamily("preferences");
     * }</pre>
     *
     * @param family the column family name to retrieve
     * @return this AnyGet instance for method chaining
     * @throws IllegalArgumentException if family is null or empty
     * @see #addColumn(String, String)
     * @see #addFamily(byte[])
     */
    public AnyGet addFamily(final String family) {
        get.addFamily(toFamilyQualifierBytes(family));
        return this;
    }

    /**
     * Adds a column family to retrieve all columns from that family using byte array.
     * 
     * <p>This method is more efficient than the string version when you already have
     * the family name as a byte array, as it avoids the conversion overhead. When a
     * family is added without specific qualifiers, all columns within that family
     * will be retrieved. This is useful for getting all data associated with a
     * particular column family in HBase.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * byte[] familyBytes = Bytes.toBytes("profile");
     * AnyGet get = AnyGet.of("user123")
     *                    .addFamily(familyBytes);
     * }</pre>
     * 
     * @param family the column family name as a byte array, must not be null or empty
     * @return this AnyGet instance for method chaining
     * @throws IllegalArgumentException if family is null or empty
     * @see #addFamily(String)
     * @see #addColumn(byte[], byte[])
     */
    public AnyGet addFamily(final byte[] family) {
        get.addFamily(family);
        return this;
    }

    /**
     * Adds a specific column (family:qualifier combination) to retrieve.
     *
     * <p>This method allows for precise column selection, retrieving only the specified
     * column from the HBase table. This is more efficient than retrieving entire families
     * when you only need specific columns.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyGet get = AnyGet.of("user123")
     *                    .addColumn("info", "name")
     *                    .addColumn("info", "email")
     *                    .addColumn("stats", "login_count");
     * }</pre>
     *
     * @param family the column family name
     * @param qualifier the column qualifier within the family
     * @return this AnyGet instance for method chaining
     * @throws IllegalArgumentException if family or qualifier is null or empty
     * @see #addFamily(String)
     * @see #addColumn(byte[], byte[])
     */
    public AnyGet addColumn(final String family, final String qualifier) {
        get.addColumn(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier));
        return this;
    }

    /**
     * Adds a specific column to retrieve using byte array representation.
     *
     * <p>This method provides direct byte array access for column specification,
     * which is more efficient when working with pre-encoded column identifiers.
     * It allows for precise column selection without the overhead of string conversion.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * byte[] familyBytes = Bytes.toBytes("info");
     * byte[] qualifierBytes = Bytes.toBytes("email");
     * AnyGet get = AnyGet.of("user123")
     *                    .addColumn(familyBytes, qualifierBytes);
     * }</pre>
     *
     * @param family the column family name as a byte array
     * @param qualifier the column qualifier as a byte array
     * @return this AnyGet instance for method chaining
     * @throws IllegalArgumentException if family or qualifier is null
     * @see #addColumn(String, String)
     * @see #addFamily(byte[])
     */
    public AnyGet addColumn(final byte[] family, final byte[] qualifier) {
        get.addColumn(family, qualifier);
        return this;
    }

    /**
     * Returns the map of column families to their respective column qualifiers.
     *
     * <p>This method provides access to the internal family map that specifies which
     * columns to retrieve. The map contains column families as keys and navigable sets
     * of column qualifiers as values. An empty or null set for a family indicates that
     * all columns in that family should be retrieved.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyGet get = AnyGet.of("user123")
     *                    .addColumn("info", "name")
     *                    .addFamily("stats");
     * Map&lt;byte[], NavigableSet&lt;byte[]&gt;&gt; familyMap = get.getFamilyMap();
     * // familyMap contains mapping of families to their qualifiers
     * }</pre>
     *
     * @return the map of column families to column qualifiers, never null
     * @see #addFamily(String)
     * @see #addColumn(String, String)
     */
    public Map<byte[], NavigableSet<byte[]>> getFamilyMap() {
        return get.getFamilyMap();
    }

    /**
     * Checks if the operation is set to check existence only.
     *
     * <p>When a Get operation is configured for existence checking only, it doesn't
     * retrieve any actual data but only verifies whether the specified row exists
     * in the table. This is a highly optimized operation for existence verification.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyGet get = AnyGet.of("user123")
     *                    .setCheckExistenceOnly(true);
     * boolean existsOnly = get.isCheckExistenceOnly();   // returns true
     * }</pre>
     *
     * @return {@code true} if the operation is set to check existence only, {@code false} otherwise
     * @see #setCheckExistenceOnly(boolean)
     */
    public boolean isCheckExistenceOnly() {
        return get.isCheckExistenceOnly();
    }

    /**
     * Configures this Get operation to only check for row existence without retrieving data.
     *
     * <p>When set to true, this operation becomes highly efficient for existence checks as it
     * avoids transferring any actual data from the server to the client. The operation will
     * return quickly with just a boolean result indicating whether the specified row exists.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Efficient existence check
     * AnyGet existsCheck = AnyGet.of("user123")
     *                            .setCheckExistenceOnly(true);
     * boolean exists = executor.exists("users_table", existsCheck);
     * }</pre>
     *
     * @param checkExistenceOnly true to only check existence, {@code false} to retrieve actual data
     * @return this AnyGet instance for method chaining
     * @see HBaseExecutor#exists(String, AnyGet)
     * @see #isCheckExistenceOnly()
     */
    public AnyGet setCheckExistenceOnly(final boolean checkExistenceOnly) {
        get.setCheckExistenceOnly(checkExistenceOnly);
        return this;
    }

    /**
     * Returns the configured time range for this Get operation.
     *
     * <p>The time range specifies which versions of cells to retrieve based on their
     * timestamps. Only cells with timestamps within this range will be returned.
     * By default, the time range is [0, Long.MAX_VALUE), which includes all versions.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyGet get = AnyGet.of("user123")
     *                    .setTimeRange(startTime, endTime);
     * TimeRange range = get.getTimeRange();
     * long min = range.getMin();   // startTime
     * long max = range.getMax();   // endTime
     * }</pre>
     *
     * @return the TimeRange configured for this Get operation, never null
     * @see #setTimeRange(long, long)
     * @see #setTimestamp(long)
     * @see TimeRange
     */
    public TimeRange getTimeRange() {
        return get.getTimeRange();
    }

    /**
     * Sets the time range for retrieving versions of cells within a specific time window.
     *
     * <p>This method allows filtering of cell versions based on their timestamps. Only cells
     * with timestamps within the specified range (minStamp inclusive, maxStamp exclusive)
     * will be retrieved. This is useful for temporal queries or when working with time-series data.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Get data from the last 24 hours
     * long endTime = System.currentTimeMillis();
     * long startTime = endTime - (24 * 60 * 60 * 1000);
     * AnyGet get = AnyGet.of("user123")
     *                    .addFamily("activity")
     *                    .setTimeRange(startTime, endTime);
     * }</pre>
     *
     * @param minStamp the minimum timestamp (inclusive) for cell versions to retrieve
     * @param maxStamp the maximum timestamp (exclusive) for cell versions to retrieve
     * @return this AnyGet instance for method chaining
     * @throws IllegalArgumentException if minStamp is negative, maxStamp is negative, or minStamp >= maxStamp
     * @see #setTimestamp(long)
     * @see #getTimeRange()
     * @see TimeRange
     */
    public AnyGet setTimeRange(final long minStamp, final long maxStamp) {
        try {
            get.setTimeRange(minStamp, maxStamp);
        } catch (final IOException e) {
            throw new IllegalArgumentException(e);
        }
        return this;
    }

    /**
     * Sets a specific timestamp to retrieve cell versions from that exact point in time.
     *
     * <p>This method retrieves only the versions of cells that have the specified timestamp.
     * This is useful when you need to retrieve data as it existed at a specific point in time,
     * which is common in temporal database scenarios or audit trail implementations.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Get data as it existed at a specific timestamp
     * long snapshotTime = 1609459200000L;  // Jan 1, 2021
     * AnyGet get = AnyGet.of("user123")
     *                    .addFamily("profile")
     *                    .setTimestamp(snapshotTime);
     * }</pre>
     *
     * @param timestamp the exact timestamp for which to retrieve cell versions
     * @return this AnyGet instance for method chaining
     * @throws IllegalArgumentException if timestamp is negative
     * @see #setTimeRange(long, long)
     * @see #getTimeRange()
     */
    public AnyGet setTimestamp(final long timestamp) {
        get.setTimestamp(timestamp);
        return this;
    }

    /**
     * Returns the maximum number of versions configured to be retrieved for each column.
     *
     * <p>This value determines how many historical versions of each cell will be returned.
     * The default is 1, meaning only the latest version is retrieved. A value of Integer.MAX_VALUE
     * indicates that all available versions should be retrieved.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyGet get = AnyGet.of("user123")
     *                    .readVersions(5);
     * int maxVersions = get.getMaxVersions();   // returns 5
     * }</pre>
     *
     * @return the maximum number of versions to retrieve for each column
     * @see #readVersions(int)
     * @see #readAllVersions()
     */
    public int getMaxVersions() {
        return get.getMaxVersions();
    }

    /**
     * Configures the Get operation to retrieve up to the specified number of versions for each column.
     *
     * <p>HBase stores multiple versions of each cell, and this method controls how many versions
     * to retrieve. By default, only the latest version is returned. Setting this to a higher value
     * allows retrieval of historical data, which is useful for audit trails, time-series analysis,
     * or understanding data evolution over time.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Retrieve last 5 versions of each column
     * AnyGet get = AnyGet.of("user123")
     *                    .addColumn("info", "status")
     *                    .readVersions(5);
     * }</pre>
     *
     * @param versions the maximum number of versions to retrieve for each column (must be positive)
     * @return this AnyGet instance for method chaining
     * @throws IllegalArgumentException if versions is less than 1
     * @see #readAllVersions()
     * @see #getMaxVersions()
     */
    public AnyGet readVersions(final int versions) {
        try {
            get.readVersions(versions);
        } catch (final IOException e) {
            throw new IllegalArgumentException(e);
        }
        return this;
    }

    /**
     * Configures the Get operation to retrieve all available versions of each column.
     *
     * <p>This method removes any version limits, allowing retrieval of all stored versions
     * of each cell. Use with caution as this can result in large amounts of data transfer,
     * especially for columns with many historical versions. This is typically used for
     * comprehensive audit analysis or full data history retrieval.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Retrieve complete history for audit purposes
     * AnyGet get = AnyGet.of("user123")
     *                    .addColumn("audit", "changes")
     *                    .readAllVersions();
     * }</pre>
     *
     * @return this AnyGet instance for method chaining
     * @see #readVersions(int)
     * @see #getMaxVersions()
     */
    public AnyGet readAllVersions() {
        get.readAllVersions();
        return this;
    }

    /**
     * Returns the maximum number of results to return per column family.
     *
     * <p>This limit controls how many cells are returned for each column family.
     * It's useful for limiting the amount of data transferred when dealing with
     * families that contain many columns or versions.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyGet get = AnyGet.of("user123")
     *                    .setMaxResultsPerColumnFamily(100);
     * int limit = get.getMaxResultsPerColumnFamily();   // returns 100
     * }</pre>
     *
     * @return the maximum results per column family, or -1 if no limit is set
     * @see #setMaxResultsPerColumnFamily(int)
     * @see #getRowOffsetPerColumnFamily()
     */
    public int getMaxResultsPerColumnFamily() {
        return get.getMaxResultsPerColumnFamily();
    }

    /**
     * Sets the maximum number of results to return per column family.
     *
     * <p>This method limits the number of cells returned for each column family,
     * which is useful for controlling data transfer volume when dealing with
     * families containing many columns or when implementing pagination-like behavior.
     * The limit applies after any offset specified by {@link #setRowOffsetPerColumnFamily(int)}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Limit results to first 50 cells per family
     * AnyGet get = AnyGet.of("user123")
     *                    .addFamily("large_family")
     *                    .setMaxResultsPerColumnFamily(50);
     * }</pre>
     *
     * @param limit the maximum number of results per column family (must be positive)
     * @return this AnyGet instance for method chaining
     * @throws IllegalArgumentException if limit is less than 0
     * @see #getMaxResultsPerColumnFamily()
     * @see #setRowOffsetPerColumnFamily(int)
     */
    public AnyGet setMaxResultsPerColumnFamily(final int limit) {
        get.setMaxResultsPerColumnFamily(limit);
        return this;
    }

    /**
     * Returns the row offset configured for each column family.
     *
     * <p>The offset determines how many cells to skip before starting to return results
     * for each column family. This is useful for implementing pagination-like behavior
     * when combined with {@link #setMaxResultsPerColumnFamily(int)}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyGet get = AnyGet.of("user123")
     *                    .setRowOffsetPerColumnFamily(10);
     * int offset = get.getRowOffsetPerColumnFamily();   // returns 10
     * }</pre>
     *
     * @return the row offset per column family, or 0 if no offset is set
     * @see #setRowOffsetPerColumnFamily(int)
     * @see #getMaxResultsPerColumnFamily()
     */
    public int getRowOffsetPerColumnFamily() {
        return get.getRowOffsetPerColumnFamily();
    }

    /**
     * Sets the row offset for each column family.
     *
     * <p>This method configures how many cells to skip before returning results for
     * each column family. When combined with {@link #setMaxResultsPerColumnFamily(int)},
     * this enables pagination-like behavior for large column families. The offset is
     * applied before the limit.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Skip first 20 cells, then get next 10 cells per family
     * AnyGet get = AnyGet.of("user123")
     *                    .addFamily("large_family")
     *                    .setRowOffsetPerColumnFamily(20)
     *                    .setMaxResultsPerColumnFamily(10);
     * }</pre>
     *
     * @param offset the number of cells to skip per column family (must be non-negative)
     * @return this AnyGet instance for method chaining
     * @throws IllegalArgumentException if offset is negative
     * @see #getRowOffsetPerColumnFamily()
     * @see #setMaxResultsPerColumnFamily(int)
     */
    public AnyGet setRowOffsetPerColumnFamily(final int offset) {
        get.setRowOffsetPerColumnFamily(offset);
        return this;
    }

    /**
     * Returns whether cache blocks are enabled for this Get operation.
     *
     * <p>When cache blocks is true (the default), HBase will cache the data blocks
     * read during this Get operation. This improves performance for frequently
     * accessed data but uses more memory. When false, blocks are not cached,
     * which is better for large scans or one-time reads.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyGet get = AnyGet.of("user123")
     *                    .setCacheBlocks(false);
     * boolean useCache = get.getCacheBlocks();   // returns false
     * }</pre>
     *
     * @return {@code true} if cache blocks is enabled, {@code false} otherwise
     * @see #setCacheBlocks(boolean)
     */
    public boolean getCacheBlocks() { // NOSONAR
        return get.getCacheBlocks();
    }

    /**
     * Sets whether to cache data blocks for this Get operation.
     *
     * <p>This method controls HBase's block caching behavior for this operation.
     * When set to true (default), data blocks read during the Get are cached,
     * improving performance for frequently accessed data. When set to false,
     * blocks are not cached, which reduces memory usage and is better for
     * large scans or one-time reads that won't benefit from caching.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Disable caching for a one-time large read
     * AnyGet get = AnyGet.of("user123")
     *                    .addFamily("large_data")
     *                    .setCacheBlocks(false);
     * }</pre>
     *
     * @param cacheBlocks true to enable block caching, {@code false} to disable
     * @return this AnyGet instance for method chaining
     * @see #getCacheBlocks()
     */
    public AnyGet setCacheBlocks(final boolean cacheBlocks) {
        get.setCacheBlocks(cacheBlocks);
        return this;
    }

    /**
     * Returns the row key for this Get operation.
     *
     * <p>This method returns the byte array representation of the row key that
     * this Get operation will retrieve. The returned array should not be modified
     * as it may be used internally by the Get operation.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyGet get = AnyGet.of("user123");
     * byte[] rowKey = get.getRow();
     * String keyString = Bytes.toString(rowKey);   // "user123"
     * }</pre>
     *
     * @return the row key as a byte array; never null
     * @see Row#getRow()
     */
    @Override
    public byte[] getRow() {
        return get.getRow();
    }

    /**
     * Checks if any column families have been specified for this Get operation.
     *
     * <p>Returns true if at least one column family or specific column has been
     * added to this Get operation. If no families are specified, the Get will
     * retrieve all column families for the row.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyGet get1 = AnyGet.of("user123");
     * boolean has1 = get1.hasFamilies();   // returns false
     * 
     * AnyGet get2 = AnyGet.of("user123").addFamily("info");
     * boolean has2 = get2.hasFamilies();   // returns true
     * }</pre>
     *
     * @return {@code true} if column families have been specified, {@code false} otherwise
     * @see #numFamilies()
     * @see #familySet()
     */
    public boolean hasFamilies() {
        return get.hasFamilies();
    }

    /**
     * Returns the number of column families specified for this Get operation.
     *
     * <p>This method returns the count of distinct column families that have been
     * added to this Get operation. If no families are specified, returns 0, which
     * means all families will be retrieved.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyGet get = AnyGet.of("user123")
     *                    .addFamily("info")
     *                    .addFamily("stats")
     *                    .addColumn("prefs", "theme");
     * int count = get.numFamilies();   // returns 3
     * }</pre>
     *
     * @return the number of column families specified
     * @see #hasFamilies()
     * @see #familySet()
     */
    public int numFamilies() {
        return get.numFamilies();
    }

    /**
     * Returns the set of column families specified for this Get operation.
     *
     * <p>This method returns an unmodifiable set containing the byte array
     * representations of all column families that have been added to this
     * Get operation. The set will be empty if no families have been specified.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyGet get = AnyGet.of("user123")
     *                    .addFamily("info")
     *                    .addFamily("stats");
     * Set&lt;byte[]&gt; families = get.familySet();
     * // families contains byte arrays for "info" and "stats"
     * }</pre>
     *
     * @return the set of column families as byte arrays, never null
     * @see #hasFamilies()
     * @see #numFamilies()
     */
    public Set<byte[]> familySet() {
        return get.familySet();
    }

    /**
     * Compares this AnyGet with another Row operation for ordering.
     *
     * <p>The comparison is based on the row keys of the two operations.
     * This ordering is consistent with the natural ordering of byte arrays
     * in HBase, which uses lexicographic comparison.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyGet get1 = AnyGet.of("user123");
     * AnyGet get2 = AnyGet.of("user456");
     * int comparison = get1.compareTo(get2);   // negative value (user123 < user456)
     * }</pre>
     *
     * @param other the Row operation to compare with
     * @return a negative integer, zero, or positive integer as this object is less than,
     *         equal to, or greater than the specified object
     * @see Row#compareTo(Row)
     */
    @Override
    public int compareTo(final Row other) {
        return get.compareTo(other);
    }

    /**
     * Returns the hash code value for this AnyGet instance.
     *
     * <p>The hash code is based on the underlying HBase Get object and is consistent
     * with the {@link #equals(Object)} method. Two AnyGet instances with equivalent
     * Get operations will have the same hash code.</p>
     *
     * @return the hash code value for this AnyGet
     * @see #equals(Object)
     */
    @Override
    public int hashCode() {
        return get.hashCode();
    }

    /**
     * Compares this AnyGet instance with another object for equality.
     *
     * <p>Two AnyGet instances are considered equal if they wrap equivalent HBase Get
     * operations. This comparison is based on the underlying Get object's equality,
     * which considers row key, column specifications, time ranges, and other settings.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyGet get1 = AnyGet.of("user123").addFamily("info");
     * AnyGet get2 = AnyGet.of("user123").addFamily("info");
     * boolean equal = get1.equals(get2);   // returns true
     * }</pre>
     *
     * @param obj the object to compare with
     * @return {@code true} if the specified object represents an equivalent get operation, {@code false} otherwise
     * @see #hashCode()
     */
    @SuppressFBWarnings
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof final AnyGet other) {
            return get.equals(other.get);
        }

        return false;
    }

    /**
     * Returns a string representation of this AnyGet instance.
     *
     * <p>The string representation is delegated to the underlying HBase Get object
     * and includes information about the row key, column families, qualifiers,
     * time ranges, and other configuration settings.</p>
     *
     * @return a string representation of the get operation
     */
    @Override
    public String toString() {
        return get.toString();
    }

    /**
     * Converts a collection of AnyGet instances to native HBase Get objects.
     *
     * <p>This utility method extracts the underlying HBase Get objects from a collection
     * of AnyGet wrappers, creating a list suitable for batch get operations with
     * the native HBase client API. This is useful when you need to perform bulk reads
     * or when integrating with code that expects native HBase Get objects.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List&lt;AnyGet&gt; anyGets = Arrays.asList(
     *     AnyGet.of("user1").addFamily("info"),
     *     AnyGet.of("user2").addFamily("info"),
     *     AnyGet.of("user3").addFamily("info")
     * );
     * List&lt;Get&gt; gets = AnyGet.toGet(anyGets);
     * Result[] results = table.get(gets);   // Batch get with native HBase API
     * }</pre>
     *
     * @param anyGets the collection of AnyGet instances to convert; must not be null
     * @return a list of native HBase Get objects (null elements in the input collection are skipped)
     * @throws IllegalArgumentException if anyGets is null
     * @see Get
     * @see HBaseExecutor#get(String, Collection)
     */
    public static List<Get> toGet(final Collection<AnyGet> anyGets) {
        N.checkArgNotNull(anyGets, "anyGets");

        if (N.isEmpty(anyGets)) {
            return new ArrayList<>();
        }

        final List<Get> gets = new ArrayList<>(anyGets.size());

        for (final AnyGet anyGet : anyGets) {
            if (anyGet != null) {
                gets.add(anyGet.val());
            }
        }

        return gets;
    }
}
