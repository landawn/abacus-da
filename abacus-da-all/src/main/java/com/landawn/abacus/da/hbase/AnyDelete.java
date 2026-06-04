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
import static com.landawn.abacus.da.hbase.HBaseExecutor.toRowBytes;
import static com.landawn.abacus.da.hbase.HBaseExecutor.toRowKeyBytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;

import com.landawn.abacus.annotation.SuppressFBWarnings;
import com.landawn.abacus.util.N;

/**
 * A fluent builder wrapper for HBase {@link Delete} operations that simplifies data removal
 * by providing automatic type conversion, row key handling, and comprehensive deletion strategies.
 * This class eliminates the complexity of manual byte array conversions and provides both
 * fine-grained column deletion and bulk family deletion capabilities.
 *
 * <p>AnyDelete supports multiple deletion patterns:
 * <ul>
 * <li><strong>Row Deletion</strong>: Complete row removal with timestamp control</li>
 * <li><strong>Family Deletion</strong>: Remove entire column families or specific versions</li>
 * <li><strong>Column Deletion</strong>: Remove specific columns or all versions of columns</li>
 * <li><strong>Version Control</strong>: Target specific timestamps or version ranges</li>
 * <li><strong>Batch Operations</strong>: Efficient handling of multiple delete operations</li>
 * </ul>
 *
 * <h2>Usage Examples</h2>
 *
 * <h3>Basic Row and Column Deletion</h3>
 * <pre>{@code
 * // Delete entire row
 * AnyDelete deleteRow = AnyDelete.of("user123");
 * 
 * // Delete specific column
 * AnyDelete deleteColumn = AnyDelete.of("user123")
 *                                   .addColumn("info", "email");
 * 
 * // Delete all versions of a column
 * AnyDelete deleteAllVersions = AnyDelete.of("user123")
 *                                        .addColumns("info", "name");
 *
 * // Delete entire column family
 * AnyDelete deleteFamily = AnyDelete.of("user123")
 *                                   .addFamily("preferences");
 * }</pre>
 *
 * <h3>Timestamp and Version Control</h3>
 * <pre>{@code
 * // Delete row up to specific timestamp
 * AnyDelete timestampDelete = AnyDelete.of("user123", System.currentTimeMillis());
 *
 * // Delete column versions up to timestamp
 * AnyDelete columnVersionDelete = AnyDelete.of("user123")
 *                                          .addColumn("info", "name", timestamp);
 *
 * // Delete family versions up to timestamp
 * AnyDelete familyVersionDelete = AnyDelete.of("user123")
 *                                          .addFamily("activity", timestamp);
 *
 * // Delete specific version only
 * AnyDelete exactVersionDelete = AnyDelete.of("user123")
 *                                         .addFamilyVersion("info", exactTimestamp);
 * }</pre>
 *
 * <h3>Partial Row Key Operations</h3>
 * <pre>{@code
 * // Delete using row key slice
 * AnyDelete partialKeyDelete = AnyDelete.of("user123_profile", 0, 7) // "user123"
 *                                       .addFamily("info");
 *
 * // Delete with pre-built family map
 * NavigableMap&lt;byte[], List&lt;Cell&gt;&gt; familyMap = buildFamilyMap();
 * AnyDelete complexDelete = AnyDelete.of("user123", timestamp, familyMap);
 * }</pre>
 *
 * <h3>Key Features:</h3>
 * <ul>
 * <li><strong>Type Conversion</strong>: Automatic conversion of row keys from Java objects to byte arrays</li>
 * <li><strong>Fluent API</strong>: Chainable method calls for readable delete operation construction</li>
 * <li><strong>Version Control</strong>: Precise control over which versions to delete</li>
 * <li><strong>Granular Deletion</strong>: From single columns to entire rows</li>
 * <li><strong>Timestamp Management</strong>: Support for time-based deletion strategies</li>
 * <li><strong>Performance Optimization</strong>: Efficient batch deletion capabilities</li>
 * <li><strong>Per-row atomicity</strong>: Each delete is atomic at the single-row level</li>
 * </ul>
 *
 * <h3>Deletion Strategies:</h3>
 * <ul>
 * <li><strong>{@link #addColumn(String, String)}</strong>: deletes a single version of a column.
 *     When no explicit timestamp is supplied, the server uses {@code LATEST_TIMESTAMP}, which
 *     triggers a server-side get to resolve the latest version's stamp before writing the
 *     tombstone (expensive).</li>
 * <li><strong>{@link #addColumns(String, String)}</strong>: deletes all versions of a column up
 *     to {@code LATEST_TIMESTAMP} (or the supplied timestamp). No server-side get is required.</li>
 * <li><strong>{@link #addFamily(String)}</strong>: deletes all versions of all columns in the
 *     family up to {@code LATEST_TIMESTAMP} (or the supplied timestamp).</li>
 * <li><strong>{@link #addFamilyVersion(String, long)}</strong>: deletes all columns in the family
 *     stamped with exactly the supplied timestamp.</li>
 * </ul>
 *
 * <h3>Performance Considerations:</h3>
 * <ul>
 * <li><strong>Batch Operations</strong>: Use collection-based operations for multiple deletes</li>
 * <li><strong>Version Strategy</strong>: {@code addColumn()} is expensive due to server-side get operation</li>
 * <li><strong>Timestamp Precision</strong>: Use specific timestamps when possible to avoid unnecessary scanning</li>
 * <li><strong>Family vs Column</strong>: Family deletion is more efficient than individual column deletion</li>
 * <li><strong>Row Key Design</strong>: Optimize row keys for efficient deletion patterns</li>
 * </ul>
 *
 * <h3>HBase Delete Semantics:</h3>
 * <ul>
 * <li><strong>Tombstones</strong>: Deletes create tombstone markers, actual cleanup happens during compaction</li>
 * <li><strong>Version Ordering</strong>: Newer versions mask older versions during reads</li>
 * <li><strong>TTL Integration</strong>: Works with Time-To-Live settings for automatic cleanup</li>
 * <li><strong>Consistency</strong>: Deletes are immediately consistent within a single row</li>
 * </ul>
 *
 * @see Delete
 * @see AnyMutation
 * @see HBaseExecutor
 * @see <a href="http://hbase.apache.org/devapidocs/index.html">Apache HBase Java API Documentation</a>
 * @see org.apache.hadoop.hbase.client.Delete
 */
public final class AnyDelete extends AnyMutation<AnyDelete> {

    private final Delete delete;

    /**
     * Constructs a new AnyDelete instance for the specified row key.
     *
     * <p>The row key is automatically converted to its byte array representation via
     * {@link HBaseExecutor#toRowKeyBytes(Object)}. If no further {@code addFamily}/{@code addColumn}
     * calls are made, executing this delete will remove every version of every column in every
     * family of the row (using {@code HConstants.LATEST_TIMESTAMP} as the upper bound). Cells
     * whose timestamp is in the future relative to the server clock are not affected.</p>
     *
     * @param rowKey the row key object to delete, automatically converted to bytes
     */
    AnyDelete(final Object rowKey) {
        super(new Delete(toRowKeyBytes(rowKey)));
        delete = (Delete) mutation;
    }

    /**
     * Constructs a new AnyDelete instance with a default timestamp upper bound.
     *
     * <p>Without further {@code addFamily}/{@code addColumn} calls, executing this delete will
     * remove every version of every column in every family of the row whose stamp is less than
     * or equal to {@code timestamp}. The supplied timestamp also serves as the default upper
     * bound used by subsequent {@code addColumn(family, qualifier)} /
     * {@code addColumns(family, qualifier)} / {@code addFamily(family)} calls that do not pass
     * their own timestamp.</p>
     *
     * @param rowKey the row key object to delete, automatically converted to bytes
     * @param timestamp the maximum timestamp for versions to delete (inclusive)
     */
    AnyDelete(final Object rowKey, final long timestamp) {
        super(new Delete(toRowKeyBytes(rowKey), timestamp));
        delete = (Delete) mutation;
    }

    /**
     * Constructs a new AnyDelete instance using a subset of the row key.
     * Enables deletion operations on composite or structured row keys.
     *
     * @param rowKey the row key object whose byte representation will be sliced
     * @param rowOffset the starting position (0-based) within the row key bytes
     * @param rowLength the number of bytes to use from the row key, starting at offset
     */
    AnyDelete(final Object rowKey, final int rowOffset, final int rowLength) {
        super(new Delete(toRowKeyBytes(rowKey), rowOffset, rowLength));
        delete = (Delete) mutation;
    }

    /**
     * Constructs a new AnyDelete instance using a subset of the row key with timestamp control.
     * Combines partial row key extraction with timestamp-based version control.
     *
     * @param rowKey the row key object whose byte representation will be sliced
     * @param rowOffset the starting position (0-based) within the row key bytes
     * @param rowLength the number of bytes to use from the row key, starting at offset
     * @param timestamp the maximum timestamp for versions to delete (inclusive)
     */
    AnyDelete(final Object rowKey, final int rowOffset, final int rowLength, final long timestamp) {
        super(new Delete(toRowKeyBytes(rowKey), rowOffset, rowLength, timestamp));
        delete = (Delete) mutation;
    }

    /**
     * Constructs a new AnyDelete instance with a pre-populated family map.
     * Used for reconstructing delete operations from existing data structures.
     *
     * @param rowKey the row key object for the delete operation
     * @param timestamp the timestamp to apply to the delete operation
     * @param familyMap a pre-populated NavigableMap of column families to their respective Cell lists
     */
    AnyDelete(final Object rowKey, final long timestamp, final NavigableMap<byte[], List<Cell>> familyMap) {
        super(new Delete(toRowBytes(rowKey), timestamp, familyMap));
        delete = (Delete) mutation;
    }

    /**
     * Constructs a new AnyDelete instance by copying an existing Delete object.
     *
     * <p>Delegates to {@link Delete#Delete(Delete)}, which copies the row, timestamp, and the
     * family-to-cells map structure (the map and per-family {@code List<Cell>} are new collections,
     * but the {@link Cell} instances themselves are shared with the source). Subsequent
     * {@code addColumn}/{@code addFamily} calls on the wrapper do not affect the source delete.</p>
     *
     * @param deleteToCopy the HBase Delete object to copy
     */
    AnyDelete(final Delete deleteToCopy) {
        super(new Delete(deleteToCopy));
        delete = (Delete) mutation;
    }

    /**
     * Creates a new AnyDelete instance for the specified row key.
     *
     * <p>This is the primary factory method for creating delete operations. The entire row
     * and all its column families will be deleted unless specific columns are added using
     * the add methods. The row key is automatically converted to the appropriate byte array format.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyDelete delete = AnyDelete.of("user123");
     * executor.delete("users_table", delete);
     * }</pre>
     *
     * @param rowKey the row key object to delete, automatically converted to bytes
     * @return a new AnyDelete instance configured for the specified row
     * @throws IllegalArgumentException if {@code rowKey} resolves to a null/empty byte array
     * @see #of(Object, long)
     * @see #addColumn(String, String)
     * @see #addFamily(String)
     */
    public static AnyDelete of(final Object rowKey) {
        return new AnyDelete(rowKey);
    }

    /**
     * Creates a new AnyDelete instance for the specified row key with timestamp-based version control.
     *
     * <p>This factory method creates a delete operation that targets all data in the specified row
     * with timestamps less than or equal to the specified timestamp. This provides precise control
     * over which versions of data are deleted, enabling time-based data retention and cleanup strategies.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Delete all data older than 24 hours
     * long oneDayAgo = System.currentTimeMillis() - (24 * 60 * 60 * 1000);
     * AnyDelete oldDataDelete = AnyDelete.of("user123", oneDayAgo);
     * }</pre>
     *
     * @param rowKey the row key object to delete, automatically converted to bytes
     * @param timestamp the maximum timestamp for versions to delete (inclusive); also used as the
     *                  default timestamp for any subsequent {@code addColumn}/{@code addColumns}/
     *                  {@code addFamily} calls that omit a timestamp
     * @return a new AnyDelete instance configured for timestamp-based deletion
     * @throws IllegalArgumentException if {@code rowKey} resolves to a null/empty byte array
     * @see #of(Object)
     * @see #addFamily(String, long)
     * @see #addColumn(String, String, long)
     */
    public static AnyDelete of(final Object rowKey, final long timestamp) {
        return new AnyDelete(rowKey, timestamp);
    }

    /**
     * Creates a new AnyDelete instance using a subset of the row key object's byte representation.
     *
     * <p>This factory method enables deletion operations on composite or structured row keys where
     * only a portion of the serialized row key should be used as the actual HBase row key. This is
     * particularly useful for prefix-based row key schemes, fixed-width key formats, or when working
     * with complex key structures that embed multiple identifiers.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Use first 7 bytes of a composite key as row key
     * String compositeKey = "user123_session456_data789";
     * AnyDelete prefixDelete = AnyDelete.of(compositeKey, 0, 7) // "user123"
     *                                   .addFamily("sessions");
     * }</pre>
     *
     * @param rowKey the row key object whose byte representation will be sliced
     * @param rowOffset the starting position (0-based) within the row key bytes
     * @param rowLength the number of bytes to use from the row key, starting at offset
     * @return a new AnyDelete instance configured with the partial row key
     * @throws IllegalArgumentException if {@code rowKey} resolves to a null/empty byte array, or if
     *         {@code rowOffset}/{@code rowLength} do not describe a valid sub-range of the row key
     *         bytes
     * @see #of(Object)
     * @see #of(Object, int, int, long)
     */
    public static AnyDelete of(final Object rowKey, final int rowOffset, final int rowLength) {
        return new AnyDelete(rowKey, rowOffset, rowLength);
    }

    /**
     * Creates a new AnyDelete instance using a subset of the row key with timestamp-based version control.
     *
     * <p>This advanced factory method combines partial row key extraction with timestamp-based version
     * control, providing precise deletion capabilities for complex row key structures and time-partitioned
     * data. The operation will delete all versions of the specified row data with timestamps less than
     * or equal to the specified timestamp.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Delete old sessions using user prefix and timestamp cutoff
     * String sessionKey = "user123_session456_active";
     * long cutoffTime = System.currentTimeMillis() - (7 * 24 * 60 * 60 * 1000);
     * AnyDelete sessionCleanup = AnyDelete.of(sessionKey, 0, 7, cutoffTime)
     *                                     .addFamily("session_data");
     * }</pre>
     *
     * @param rowKey the row key object whose byte representation will be sliced
     * @param rowOffset the starting position (0-based) within the row key bytes
     * @param rowLength the number of bytes to use from the row key, starting at offset
     * @param timestamp the maximum timestamp for versions to delete (inclusive)
     * @return a new AnyDelete instance configured with partial row key and timestamp control
     * @throws IllegalArgumentException if {@code rowKey} resolves to a null/empty byte array, if
     *         {@code rowOffset}/{@code rowLength} do not describe a valid sub-range of the row key
     *         bytes, or if {@code timestamp} is negative (validated by the underlying
     *         {@link Delete} constructor)
     * @see #of(Object, int, int)
     * @see #of(Object, long)
     */
    public static AnyDelete of(final Object rowKey, final int rowOffset, final int rowLength, final long timestamp) {
        return new AnyDelete(rowKey, rowOffset, rowLength, timestamp);
    }

    /**
     * Creates a new AnyDelete instance with a pre-populated family map and timestamp control.
     *
     * <p>This advanced factory method is designed for scenarios where you need to reconstruct delete
     * operations from existing data structures or when implementing custom deletion logic that requires
     * precise control over the delete markers. The family map contains the specific cells to be deleted,
     * organized by column families, with the timestamp providing version control.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * NavigableMap&lt;byte[], List&lt;Cell&gt;&gt; familyMap = buildFamilyMapFromQuery();
     * long operationTimestamp = System.currentTimeMillis();
     * AnyDelete complexDelete = AnyDelete.of("complex_row", operationTimestamp, familyMap);
     * }</pre>
     *
     * @param rowKey the row key object for the delete operation, automatically converted to bytes
     * @param timestamp the timestamp to apply to the delete operation
     * @param familyMap a pre-populated NavigableMap of column families to their respective Cell lists
     * @return a new AnyDelete instance with the specified configuration
     * @throws IllegalArgumentException if {@code rowKey} resolves to an empty (zero-length) byte array
     * @throws NullPointerException if {@code rowKey} resolves to a null byte array or {@code familyMap} is {@code null}
     * @see #of(Object)
     * @see #of(Delete)
     * @see NavigableMap
     * @see Cell
     */
    public static AnyDelete of(final Object rowKey, final long timestamp, final NavigableMap<byte[], List<Cell>> familyMap) {
        return new AnyDelete(rowKey, timestamp, familyMap);
    }

    /**
     * Creates a new AnyDelete instance by copying an existing HBase Delete object.
     *
     * <p>This factory method delegates to {@link Delete#Delete(Delete)} to create a copy of the
     * provided HBase Delete operation. The copy holds the same row, timestamp, and the same set
     * of {@link Cell} instances in new collections, so further {@code addColumn}/{@code addFamily}
     * calls on the returned wrapper do not mutate the source. (Note: the {@code Cell} instances
     * themselves are shared by reference.) This is useful when you want to extend an existing
     * delete operation while preserving the original.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Delete existingDelete = buildStandardDelete();
     * AnyDelete extendedDelete = AnyDelete.of(existingDelete)
     *                                     .addFamily("additional_family")
     *                                     .addColumn("extra", "column");
     * }</pre>
     *
     * @param deleteToCopy the HBase Delete object to copy; must not be null
     * @return a new AnyDelete instance backed by a fresh Delete copied from {@code deleteToCopy}
     * @throws NullPointerException if {@code deleteToCopy} is null
     * @see Delete
     * @see #val()
     */
    public static AnyDelete of(final Delete deleteToCopy) {
        return new AnyDelete(deleteToCopy);
    }

    /**
     * Returns the underlying HBase Delete object for direct access to native HBase operations.
     *
     * <p>This method provides access to the wrapped HBase Delete instance, allowing for advanced
     * operations not directly exposed by the AnyDelete fluent API. Use this method when you need
     * to access HBase-specific functionality or when integrating with existing HBase code.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyDelete anyDelete = AnyDelete.of("user123").addFamily("info");
     * Delete hbaseDelete = anyDelete.val();
     * table.delete(hbaseDelete);   // Use with native HBase API
     * }</pre>
     *
     * @return the underlying HBase Delete object
     * @see Delete
     */
    public Delete val() {
        return delete;
    }

    /**
     * Adds an existing delete-marker Cell to this delete operation.
     *
     * <p>Advanced API that delegates to {@link Delete#add(Cell)}. To be meaningful as a delete
     * marker, the supplied cell should have one of the delete cell types
     * ({@code Delete}, {@code DeleteColumn}, {@code DeleteFamily}, or
     * {@code DeleteFamilyVersion}); the underlying call does <em>not</em> enforce this, so it is
     * the caller's responsibility. The cell's row must, however, equal this delete's row, and
     * its family must be non-empty — both are enforced by the underlying API.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cell deleteMarker = createDeleteMarker();
     * AnyDelete delete = AnyDelete.of("user123")
     *                            .add(deleteMarker);
     * }</pre>
     *
     * @param kv an existing Cell to attach to this delete; should be a delete-type cell
     *           whose row matches this delete's row
     * @return this AnyDelete instance for method chaining
     * @throws IOException if the cell's row does not match this delete's row
     *         (a {@code WrongRowIOException}, an {@code IOException} subtype)
     * @throws IllegalArgumentException if the cell's family is null or empty
     * @see Cell
     * @see Delete#add(Cell)
     */
    public AnyDelete add(final Cell kv) throws IOException {
        delete.add(kv);
        return this;
    }

    /**
     * Marks an entire column family for deletion.
     *
     * <p>Delegates to {@link Delete#addFamily(byte[])}. Writes a single family-level tombstone
     * that covers all columns and all versions of the family with timestamps up to the row's
     * default timestamp (i.e. the timestamp passed to the {@code AnyDelete} constructor, or
     * {@code HConstants.LATEST_TIMESTAMP} when none was supplied). Per HBase semantics, this
     * call overrides any previous {@code addColumn}/{@code addColumns} entries for the same
     * family on this delete.</p>
     *
     * <p>The actual storage cleanup happens lazily during major compaction; until then, the
     * tombstone masks matching cells from reads.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyDelete delete = AnyDelete.of("user123")
     *                            .addFamily("preferences")
     *                            .addFamily("activity_log");
     * }</pre>
     *
     * @param family the name of the column family to delete; encoded via
     *               {@link HBaseExecutor#toFamilyQualifierBytes(String)}
     * @return this AnyDelete instance for method chaining
     * @see #addFamily(String, long)
     * @see #addFamilyVersion(String, long)
     */
    public AnyDelete addFamily(final String family) {
        delete.addFamily(toFamilyQualifierBytes(family));
        return this;
    }

    /**
     * Marks a column family for deletion up to the specified timestamp.
     *
     * <p>Delegates to {@link Delete#addFamily(byte[], long)}. Writes a family-level tombstone
     * covering all columns and all versions whose timestamps are less than or equal to
     * {@code timestamp}. Useful for time-based retention strategies at the family level. Per
     * HBase semantics this call overrides any previous {@code addColumn}/{@code addColumns}
     * entries for the same family on this delete.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long retentionCutoff = System.currentTimeMillis() - (30L * 24 * 60 * 60 * 1000);
     * AnyDelete delete = AnyDelete.of("user123")
     *                            .addFamily("activity_log", retentionCutoff);
     * }</pre>
     *
     * @param family the name of the column family to delete; encoded via
     *               {@link HBaseExecutor#toFamilyQualifierBytes(String)}
     * @param timestamp the maximum timestamp for versions to delete (inclusive); must be non-negative
     * @return this AnyDelete instance for method chaining
     * @throws IllegalArgumentException if {@code timestamp} is negative
     * @see #addFamily(String)
     * @see #addFamilyVersion(String, long)
     */
    public AnyDelete addFamily(final String family, final long timestamp) {
        delete.addFamily(toFamilyQualifierBytes(family), timestamp);
        return this;
    }

    /**
     * Marks an entire column family for deletion using byte array representation.
     *
     * <p>This method provides direct byte array access for performance-critical operations
     * or when working with pre-encoded family names. It deletes all columns and all versions
     * within the specified column family.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * byte[] familyBytes = Bytes.toBytes("preferences");
     * AnyDelete delete = AnyDelete.of("user123")
     *                            .addFamily(familyBytes);
     * }</pre>
     *
     * @param family the column family name as a byte array
     * @return this AnyDelete instance for method chaining
     * @see #addFamily(String)
     * @see #addFamily(byte[], long)
     */
    public AnyDelete addFamily(final byte[] family) {
        delete.addFamily(family);
        return this;
    }

    /**
     * Marks a column family for deletion up to the specified timestamp using byte array representation.
     *
     * <p>This method provides direct byte array access with timestamp control, enabling efficient
     * time-based deletion of family data when working with pre-encoded family names.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * byte[] familyBytes = Bytes.toBytes("activity_log");
     * long cutoffTime = System.currentTimeMillis() - (7L * 24 * 60 * 60 * 1000);
     * AnyDelete delete = AnyDelete.of("user123")
     *                            .addFamily(familyBytes, cutoffTime);
     * }</pre>
     *
     * @param family the column family name as a byte array
     * @param timestamp the maximum timestamp for versions to delete (inclusive); must be non-negative
     * @return this AnyDelete instance for method chaining
     * @throws IllegalArgumentException if {@code timestamp} is negative
     * @see #addFamily(byte[])
     * @see #addFamilyVersion(byte[], long)
     */
    public AnyDelete addFamily(final byte[] family, final long timestamp) {
        delete.addFamily(family, timestamp);
        return this;
    }

    /**
     * Marks all cells in the family stamped with exactly the given timestamp for deletion.
     *
     * <p>Delegates to {@link Delete#addFamilyVersion(byte[], long)}. Writes a
     * {@code DeleteFamilyVersion} tombstone that matches cells across every column in
     * {@code family} whose stamp equals {@code timestamp} (an exact, point-in-time match — not
     * "less-than-or-equal" as with {@link #addFamily(String, long)}). Useful for surgical
     * rollback of a single mutation batch that touched the family.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long specificVersion = 1609459200000L;  // Specific timestamp
     * AnyDelete delete = AnyDelete.of("user123")
     *                            .addFamilyVersion("snapshots", specificVersion);
     * }</pre>
     *
     * @param family the name of the column family
     * @param timestamp the exact timestamp of the cells to delete; must be non-negative
     * @return this AnyDelete instance for method chaining
     * @throws IllegalArgumentException if {@code timestamp} is negative
     * @see #addFamily(String, long)
     * @see #addColumn(String, String, long)
     */
    public AnyDelete addFamilyVersion(final String family, final long timestamp) {
        delete.addFamilyVersion(toFamilyQualifierBytes(family), timestamp);
        return this;
    }

    /**
     * Marks a specific version of an entire column family for deletion using byte array representation.
     *
     * <p>This method provides direct byte array access for deleting a specific version of the
     * entire column family, useful when working with pre-encoded family names and requiring
     * precise version control.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * byte[] familyBytes = Bytes.toBytes("snapshots");
     * long specificVersion = 1609459200000L;
     * AnyDelete delete = AnyDelete.of("user123")
     *                            .addFamilyVersion(familyBytes, specificVersion);
     * }</pre>
     *
     * @param family the column family name as a byte array
     * @param timestamp the exact timestamp of the version to delete; must be non-negative
     * @return this AnyDelete instance for method chaining
     * @throws IllegalArgumentException if {@code timestamp} is negative
     * @see #addFamilyVersion(String, long)
     * @see #addFamily(byte[], long)
     */
    public AnyDelete addFamilyVersion(final byte[] family, final long timestamp) {
        delete.addFamilyVersion(family, timestamp);
        return this;
    }

    /**
     * Marks a single version of a specific column for deletion.
     *
     * <p>Delegates to {@link Delete#addColumn(byte[], byte[])} using this delete's default
     * timestamp ({@code HConstants.LATEST_TIMESTAMP} unless one was supplied to the constructor).
     * When the default {@code LATEST_TIMESTAMP} is in effect, the region server must first issue
     * a get to discover the latest version's stamp and then write a {@code Delete} tombstone for
     * exactly that stamp — an expensive round-trip. For better performance, prefer
     * {@link #addColumns(String, String)} (deletes all versions, no server-side get) or
     * {@link #addColumn(String, String, long)} with an explicit timestamp.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyDelete delete = AnyDelete.of("user123")
     *                            .addColumn("info", "email")
     *                            .addColumn("info", "phone");
     * }</pre>
     *
     * @param family the column family name; encoded via
     *               {@link HBaseExecutor#toFamilyQualifierBytes(String)}
     * @param qualifier the column qualifier name; encoded via
     *                  {@link HBaseExecutor#toFamilyQualifierBytes(String)}
     * @return this AnyDelete instance for method chaining
     * @see #addColumn(String, String, long)
     * @see #addColumns(String, String)
     */
    public AnyDelete addColumn(final String family, final String qualifier) {
        delete.addColumn(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier));
        return this;
    }

    /**
     * Marks a specific version of a column for deletion at the exact timestamp.
     *
     * <p>This method removes only the version of the specified column that exists at exactly
     * the given timestamp. This provides precise control over which version to delete and is
     * useful for version-specific cleanup or rollback operations.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long versionToDelete = 1609459200000L;
     * AnyDelete delete = AnyDelete.of("user123")
     *                            .addColumn("info", "email", versionToDelete);
     * }</pre>
     *
     * @param family the column family name; encoded via
     *               {@link HBaseExecutor#toFamilyQualifierBytes(String)}
     * @param qualifier the column qualifier name; encoded via
     *                  {@link HBaseExecutor#toFamilyQualifierBytes(String)}
     * @param timestamp the exact timestamp of the version to delete; must be non-negative
     * @return this AnyDelete instance for method chaining
     * @throws IllegalArgumentException if {@code timestamp} is negative
     * @see #addColumn(String, String)
     * @see #addColumns(String, String, long)
     */
    public AnyDelete addColumn(final String family, final String qualifier, final long timestamp) {
        delete.addColumn(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier), timestamp);
        return this;
    }

    /**
     * Marks a single version of a specific column for deletion, using byte array identifiers.
     *
     * <p>Delegates to {@link Delete#addColumn(byte[], byte[])} using this delete's default
     * timestamp ({@code HConstants.LATEST_TIMESTAMP} unless one was supplied to the constructor).
     * When the default {@code LATEST_TIMESTAMP} is in effect, the region server must first issue
     * a get to discover the latest version's stamp, so this call is more expensive than the
     * timestamped variant or {@link #addColumns(byte[], byte[])}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * byte[] familyBytes = Bytes.toBytes("info");
     * byte[] qualifierBytes = Bytes.toBytes("email");
     * AnyDelete delete = AnyDelete.of("user123")
     *                            .addColumn(familyBytes, qualifierBytes);
     * }</pre>
     *
     * @param family the column family name as a byte array
     * @param qualifier the column qualifier name as a byte array
     * @return this AnyDelete instance for method chaining
     * @see #addColumn(byte[], byte[], long)
     * @see #addColumns(byte[], byte[])
     */
    public AnyDelete addColumn(final byte[] family, final byte[] qualifier) {
        delete.addColumn(family, qualifier);
        return this;
    }

    /**
     * Marks a specific version of a column for deletion at the exact timestamp using byte array representation.
     *
     * <p>This method provides direct byte array access with precise timestamp control for
     * version-specific column deletion when working with pre-encoded column identifiers.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * byte[] familyBytes = Bytes.toBytes("info");
     * byte[] qualifierBytes = Bytes.toBytes("email");
     * long versionTimestamp = 1609459200000L;
     * AnyDelete delete = AnyDelete.of("user123")
     *                            .addColumn(familyBytes, qualifierBytes, versionTimestamp);
     * }</pre>
     *
     * @param family the column family name as a byte array
     * @param qualifier the column qualifier name as a byte array
     * @param timestamp the exact timestamp of the version to delete; must be non-negative
     * @return this AnyDelete instance for method chaining
     * @throws IllegalArgumentException if {@code timestamp} is negative
     * @see #addColumn(byte[], byte[])
     * @see #addColumns(byte[], byte[], long)
     */
    public AnyDelete addColumn(final byte[] family, final byte[] qualifier, final long timestamp) {
        delete.addColumn(family, qualifier, timestamp);
        return this;
    }

    /**
     * Marks all versions of a specific column for deletion.
     *
     * <p>Delegates to {@link Delete#addColumns(byte[], byte[])}. Writes a {@code DeleteColumn}
     * tombstone covering every version of the column whose timestamp is less than or equal to
     * this delete's default timestamp ({@code HConstants.LATEST_TIMESTAMP} unless one was supplied
     * to the constructor — interpreted server-side as "every version up to the time the delete
     * is applied"). Unlike {@link #addColumn(String, String)}, no server-side get is required,
     * making this the preferred call when you intend to remove the column entirely.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyDelete delete = AnyDelete.of("user123")
     *                            .addColumns("info", "email")
     *                            .addColumns("info", "phone");
     * }</pre>
     *
     * @param family the column family name; encoded via
     *               {@link HBaseExecutor#toFamilyQualifierBytes(String)}
     * @param qualifier the column qualifier name; encoded via
     *                  {@link HBaseExecutor#toFamilyQualifierBytes(String)}
     * @return this AnyDelete instance for method chaining
     * @see #addColumns(String, String, long)
     * @see #addColumn(String, String)
     */
    public AnyDelete addColumns(final String family, final String qualifier) {
        delete.addColumns(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier));
        return this;
    }

    /**
     * Marks all versions of a specific column for deletion up to the specified timestamp.
     *
     * <p>This method removes all versions of the specified column that have timestamps
     * less than or equal to the given timestamp. This is useful for time-based data
     * retention and cleanup strategies at the column level.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long retentionCutoff = System.currentTimeMillis() - (90L * 24 * 60 * 60 * 1000);
     * AnyDelete delete = AnyDelete.of("user123")
     *                            .addColumns("activity", "login_history", retentionCutoff);
     * }</pre>
     *
     * @param family the column family name; encoded via
     *               {@link HBaseExecutor#toFamilyQualifierBytes(String)}
     * @param qualifier the column qualifier name; encoded via
     *                  {@link HBaseExecutor#toFamilyQualifierBytes(String)}
     * @param timestamp the maximum timestamp for versions to delete (inclusive); must be non-negative
     * @return this AnyDelete instance for method chaining
     * @throws IllegalArgumentException if {@code timestamp} is negative
     * @see #addColumns(String, String)
     * @see #addColumn(String, String, long)
     */
    public AnyDelete addColumns(final String family, final String qualifier, final long timestamp) {
        delete.addColumns(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier), timestamp);
        return this;
    }

    /**
     * Marks all versions of a specific column for deletion, using byte array identifiers.
     *
     * <p>Delegates to {@link Delete#addColumns(byte[], byte[])}. Writes a {@code DeleteColumn}
     * tombstone that covers every version of the column with stamp less than or equal to this
     * delete's default timestamp. More efficient than {@link #addColumn(byte[], byte[])} because
     * it does not require a server-side get to resolve the latest version's stamp.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * byte[] familyBytes = Bytes.toBytes("info");
     * byte[] qualifierBytes = Bytes.toBytes("email");
     * AnyDelete delete = AnyDelete.of("user123")
     *                            .addColumns(familyBytes, qualifierBytes);
     * }</pre>
     *
     * @param family the column family name as a byte array
     * @param qualifier the column qualifier name as a byte array
     * @return this AnyDelete instance for method chaining
     * @see #addColumns(byte[], byte[], long)
     * @see #addColumn(byte[], byte[])
     */
    public AnyDelete addColumns(final byte[] family, final byte[] qualifier) {
        delete.addColumns(family, qualifier);
        return this;
    }

    /**
     * Marks all versions of a specific column for deletion up to the specified timestamp using byte array representation.
     *
     * <p>This method provides direct byte array access with timestamp control for efficient
     * bulk version deletion when working with pre-encoded column identifiers.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * byte[] familyBytes = Bytes.toBytes("activity");
     * byte[] qualifierBytes = Bytes.toBytes("login_history");
     * long cutoffTime = System.currentTimeMillis() - (30L * 24 * 60 * 60 * 1000);
     * AnyDelete delete = AnyDelete.of("user123")
     *                            .addColumns(familyBytes, qualifierBytes, cutoffTime);
     * }</pre>
     *
     * @param family the column family name as a byte array
     * @param qualifier the column qualifier name as a byte array
     * @param timestamp the maximum timestamp for versions to delete (inclusive); must be non-negative
     * @return this AnyDelete instance for method chaining
     * @throws IllegalArgumentException if {@code timestamp} is negative
     * @see #addColumns(byte[], byte[])
     * @see #addColumn(byte[], byte[], long)
     */
    public AnyDelete addColumns(final byte[] family, final byte[] qualifier, final long timestamp) {
        delete.addColumns(family, qualifier, timestamp);
        return this;
    }

    /**
     * Returns the hash code value for this AnyDelete instance.
     *
     * <p>The hash code is based on the underlying HBase Delete object and is consistent
     * with the {@link #equals(Object)} method. Two AnyDelete instances with equivalent
     * Delete operations will have the same hash code.</p>
     *
     * @return the hash code value for this AnyDelete
     * @see #equals(Object)
     */
    @Override
    public int hashCode() {
        return delete.hashCode();
    }

    /**
     * Compares this AnyDelete instance with another object for equality.
     *
     * <p>Two AnyDelete instances are considered equal if they wrap equivalent HBase Delete
     * operations. This comparison is based on the underlying Delete object's equality,
     * which considers row key, column specifications, and timestamps.</p>
     *
     * @param obj the object to compare with
     * @return {@code true} if the specified object represents an equivalent delete operation, {@code false} otherwise
     * @see #hashCode()
     */
    @SuppressFBWarnings
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof final AnyDelete other) {
            return delete.equals(other.delete);
        }

        return false;
    }

    /**
     * Returns a string representation of this AnyDelete instance.
     *
     * <p>The string representation is delegated to the underlying HBase Delete object
     * and includes information about the row key, column families, qualifiers, and
     * timestamps configured for deletion.</p>
     *
     * @return a string representation of the delete operation
     */
    @Override
    public String toString() {
        return delete.toString();
    }

    /**
     * Converts a collection of AnyDelete instances to native HBase Delete objects.
     *
     * <p>This utility method extracts the underlying HBase Delete objects from a collection
     * of AnyDelete wrappers, creating a list suitable for batch delete operations with
     * the native HBase client API. This is useful when you need to perform bulk deletions
     * or when integrating with code that expects native HBase Delete objects.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List&lt;AnyDelete&gt; anyDeletes = Arrays.asList(
     *     AnyDelete.of("user1").addFamily("info"),
     *     AnyDelete.of("user2").addColumn("prefs", "theme"),
     *     AnyDelete.of("user3").addColumns("activity", "logs")
     * );
     * List&lt;Delete&gt; deletes = AnyDelete.toDelete(anyDeletes);
     * table.delete(deletes);   // Batch delete with native HBase API
     * }</pre>
     *
     * @param anyDeletes the collection of AnyDelete instances to convert; must not be null and must not contain null elements
     * @return a list of native HBase Delete objects, in iteration order of {@code anyDeletes}
     * @throws IllegalArgumentException if {@code anyDeletes} is null, or any element of {@code anyDeletes} is null
     * @see Delete
     * @see HBaseExecutor#delete(String, Collection)
     */
    public static List<Delete> toDelete(final Collection<AnyDelete> anyDeletes) {
        N.checkArgNotNull(anyDeletes, "anyDeletes");

        final List<Delete> deletes = new ArrayList<>(anyDeletes.size());

        for (final AnyDelete anyDelete : anyDeletes) {
            N.checkArgNotNull(anyDelete, "anyDelete");
            deletes.add(anyDelete.val());
        }

        return deletes;
    }
}
