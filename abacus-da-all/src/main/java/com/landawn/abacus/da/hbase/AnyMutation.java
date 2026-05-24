/*
 * Copyright (C) 2019 HaiYang Li
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

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.UUID;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;

/**
 * Abstract base wrapper for HBase {@link Mutation} operations. Concrete subclasses include
 * {@link AnyPut}, {@link AnyDelete}, {@link AnyAppend}, and {@link AnyIncrement}.
 *
 * <p>This class implements {@link Row}, so any concrete mutation can be passed directly to HBase
 * batch APIs that accept {@code Row} instances. It exposes the common mutation-level controls —
 * durability, timestamp, cluster ids, cell visibility, ACLs, TTL, and family-map / cell
 * inspection — without forcing callers to deal with byte arrays.</p>
 *
 * <h3>Key features</h3>
 * <ul>
 * <li><strong>Automatic conversion</strong>: {@link String} family/qualifier names and arbitrary
 *     {@link Object} values are converted to byte arrays via {@link HBaseExecutor}.</li>
 * <li><strong>Fluent API</strong>: every setter returns {@code this} (typed as {@code AM}) for
 *     method chaining.</li>
 * <li><strong>Advanced controls</strong>: durability level, cell visibility expressions,
 *     per-user / per-map ACLs, and TTL.</li>
 * <li><strong>Cell inspection</strong>: {@link #has}, {@link #get}, {@link #getFamilyCellMap},
 *     {@link #cellScanner}, {@link #size}, {@link #numFamilies}, and {@link #isEmpty}.</li>
 * </ul>
 *
 * <h3>Common usage patterns</h3>
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Basic mutation with durability control
 * anyMutation.setTimestamp(System.currentTimeMillis())
 *           .setDurability(Durability.SYNC_WAL)
 *           .setTTL(86400000);   // 24 hours in milliseconds
 *
 * // Security and visibility controls
 * anyMutation.setCellVisibility(new CellVisibility("SECRET&DEPT_A"))
 *           .setACL("admin", new Permission(Permission.Action.READ));
 *
 * // Query existing mutation data
 * boolean hasColumn = anyMutation.has("family", "qualifier");
 * List<Cell> cells = anyMutation.get("family", "qualifier");
 * }</pre>
 *
 * <h3>Durability levels (see {@link Durability})</h3>
 * <ul>
 * <li><strong>SKIP_WAL</strong>: fastest, no durability guarantee</li>
 * <li><strong>ASYNC_WAL</strong>: fast, asynchronous write-ahead log</li>
 * <li><strong>SYNC_WAL</strong>: default, synchronous write-ahead log</li>
 * <li><strong>FSYNC_WAL</strong>: strongest, forces filesystem sync</li>
 * </ul>
 *
 * @param <AM> the concrete subtype of {@code AnyMutation}; declared so fluent setters can return
 *             {@code AM} and preserve the concrete type during chaining
 * @see AnyOperationWithAttributes
 * @see Mutation
 * @see AnyPut
 * @see AnyDelete
 * @see AnyAppend
 * @see AnyIncrement
 * @see <a href="http://hbase.apache.org/devapidocs/index.html">Apache HBase Java API Documentation</a>
 */
abstract class AnyMutation<AM extends AnyMutation<AM>> extends AnyOperationWithAttributes<AM> implements Row {

    protected final Mutation mutation;

    /**
     * Constructs a new {@code AnyMutation} that delegates to the supplied HBase {@link Mutation}.
     * The wrapped mutation is also passed to the {@link AnyOperationWithAttributes} constructor as
     * the underlying operation.
     *
     * @param mutation the HBase {@link Mutation} to wrap; must not be {@code null}
     * @throws IllegalArgumentException if {@code mutation} is {@code null}
     */
    protected AnyMutation(final Mutation mutation) {
        super(mutation);
        if (mutation == null) {
            throw new IllegalArgumentException("Mutation must not be null");
        }
        this.mutation = mutation;
    }

    /**
     * Returns a {@link CellScanner} over every cell this mutation will write. Lower-overhead than
     * materializing the family map; useful for inspection, debugging, or forwarding the cells to
     * other HBase APIs.
     *
     * @return a {@link CellScanner} over the cells contained in this mutation; never {@code null}
     * @see CellScanner
     * @see Cell
     * @see #getFamilyCellMap()
     */
    public CellScanner cellScanner() {
        return mutation.cellScanner();
    }

    /**
     * Returns the fingerprint for this mutation, overriding {@link AnyOperation#getFingerprint()}
     * with the {@link Mutation}-specific implementation. The fingerprint includes the set of
     * column families touched by this mutation but excludes per-cell data such as qualifiers,
     * values, and the row key.
     *
     * @return the fingerprint map produced by HBase; never {@code null} but may be empty
     * @see #toMap()
     */
    @Override
    public Map<String, Object> getFingerprint() {
        return mutation.getFingerprint();
    }

    /**
     * Returns the durability level currently set for this mutation. The level determines how
     * strongly the mutation is persisted to the write-ahead log; higher levels reduce the risk of
     * data loss at the cost of throughput.
     *
     * @return the current durability level (defaults to {@link Durability#USE_DEFAULT} when none
     *         has been set explicitly, meaning the table-level default is applied)
     * @see #setDurability(Durability)
     * @see Durability
     */
    public Durability getDurability() {
        return mutation.getDurability();
    }

    /**
     * Sets the durability level for this mutation, trading throughput against persistence
     * guarantees. The supported levels are:
     * <ul>
     * <li><strong>SKIP_WAL</strong>: fastest, bypasses the write-ahead log (data-loss risk)</li>
     * <li><strong>ASYNC_WAL</strong>: fast, asynchronous WAL append</li>
     * <li><strong>SYNC_WAL</strong>: synchronous WAL append</li>
     * <li><strong>FSYNC_WAL</strong>: strongest, forces filesystem sync</li>
     * <li><strong>USE_DEFAULT</strong>: use the table-level setting</li>
     * </ul>
     *
     * @param d the durability level to apply
     * @return this mutation instance, to allow fluent method chaining
     * @see #getDurability()
     * @see Durability
     */
    public AM setDurability(final Durability d) {
        mutation.setDurability(d);

        return (AM) this;
    }

    /**
     * Returns the underlying family-to-cell map. Keys are column-family names as byte arrays;
     * values are the lists of {@link Cell}s belonging to each family. The returned map is the
     * live map held by the wrapped {@link Mutation}, so mutating it changes the mutation;
     * prefer the family-specific {@link #get(String, String)} or the dedicated subclass APIs for
     * normal use.
     *
     * @return the family-to-cell {@link NavigableMap} held by the wrapped mutation; never
     *         {@code null}, but may be empty
     * @see Cell
     * @see #get(String, String)
     * @see #cellScanner()
     */
    public NavigableMap<byte[], List<Cell>> getFamilyCellMap() {
        return mutation.getFamilyCellMap();
    }

    //    /**
    //     * Method for setting the mutation's familyMap.
    //     *
    //     * @param map
    //     * @return
    //     * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
    //     *             Use {@link Mutation#Mutation(byte[], long, NavigableMap)} instead
    //     */
    //    @Deprecated
    //    public AM setFamilyCellMap(NavigableMap<byte[], List<Cell>> map) {
    //        mutation.setFamilyCellMap(map);
    //
    //        return (AM) this;
    //    }

    //    /**
    //     * Method for retrieving the timestamp.
    //     *
    //     * @return timestamp
    //     * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
    //     *             Use {@code getTimestamp()} instead
    //     */
    //    @Deprecated
    //    public long getTimeStamp() {
    //        return mutation.getTimeStamp();
    //    }

    /**
     * Returns the timestamp set on this mutation. The timestamp becomes the version of every
     * cell written by the mutation. When no timestamp has been set explicitly, HBase returns
     * {@code HConstants.LATEST_TIMESTAMP} and assigns the current server time when the mutation
     * is applied.
     *
     * @return the timestamp for this mutation, or {@code HConstants.LATEST_TIMESTAMP} when none
     *         has been set explicitly
     * @see #setTimestamp(long)
     */
    public long getTimestamp() {
        return mutation.getTimestamp();
    }

    /**
     * Sets the timestamp that will be applied to every cell already added to (and added
     * subsequently to) this mutation. The timestamp drives versioning, time-based queries, and
     * TTL / compaction decisions on the server.
     *
     * @param timestamp the timestamp to assign, in milliseconds since the epoch
     * @return this mutation instance, to allow fluent method chaining
     * @see #getTimestamp()
     * @see System#currentTimeMillis()
     */
    public AM setTimestamp(final long timestamp) {
        mutation.setTimestamp(timestamp);

        return (AM) this;
    }

    /**
     * Returns the list of cluster UUIDs that have already consumed this mutation. Used by HBase
     * replication to suppress loops when a mutation is replicated between bidirectionally linked
     * clusters.
     *
     * @return the list of cluster UUIDs; may be empty
     * @see #setClusterIds(List)
     * @see UUID
     */
    public List<UUID> getClusterIds() {
        return mutation.getClusterIds();
    }

    /**
     * Records the set of clusters that have already consumed this mutation. Used internally by
     * HBase replication to suppress loops; ordinary client code rarely needs to call this.
     *
     * @param clusterIds the cluster UUIDs to record
     * @return this mutation instance, to allow fluent method chaining
     * @see #getClusterIds()
     * @see UUID
     */
    public AM setClusterIds(final List<UUID> clusterIds) {
        mutation.setClusterIds(clusterIds);

        return (AM) this;
    }

    /**
     * Returns the cell visibility expression attached to this mutation. The expression is the
     * label-algebra string evaluated by HBase's visibility-label subsystem to gate cell-level read
     * access (e.g. {@code "SECRET&DEPT_A"}).
     *
     * @return the {@link CellVisibility} previously set, or {@code null} if none has been set
     * @throws DeserializationException if the stored visibility expression cannot be deserialized
     *         by HBase
     * @see #setCellVisibility(CellVisibility)
     * @see CellVisibility
     */
    public CellVisibility getCellVisibility() throws DeserializationException {
        return mutation.getCellVisibility();
    }

    /**
     * Attaches a cell-visibility expression that will be applied to every cell written by this
     * mutation. The expression uses HBase's label algebra; only users whose authorization tokens
     * satisfy the expression will be able to read the cells.
     *
     * <p><strong>Expression examples:</strong></p>
     * <ul>
     * <li><code>"PUBLIC"</code> - accessible to users with the {@code PUBLIC} label</li>
     * <li><code>"SECRET&amp;DEPT_A"</code> - requires both {@code SECRET} and {@code DEPT_A}</li>
     * <li><code>"(SECRET|CONFIDENTIAL)&amp;DEPT_A"</code> - combined boolean expression</li>
     * </ul>
     *
     * @param expression the {@link CellVisibility} expression to apply
     * @return this mutation instance, to allow fluent method chaining
     * @see #getCellVisibility()
     * @see CellVisibility
     */
    public AM setCellVisibility(final CellVisibility expression) {
        mutation.setCellVisibility(expression);

        return (AM) this;
    }

    /**
     * Returns the serialized Access Control List attached to this mutation. The bytes are the
     * protobuf-serialized form of the user-to-permission map written by
     * {@link #setACL(String, Permission)} or {@link #setACL(Map)}.
     *
     * @return the serialized ACL bytes, or {@code null} if no ACL has been set
     * @see #setACL(String, Permission)
     * @see #setACL(Map)
     * @see Permission
     */
    public byte[] getACL() {
        return mutation.getACL();
    }

    /**
     * Grants the specified {@link Permission} to a single user on the data written by this
     * mutation. Existing ACL settings on the mutation are replaced.
     *
     * @param user the username to grant permissions to
     * @param perms the {@link Permission} defining the allowed actions
     * @return this mutation instance, to allow fluent method chaining
     * @see #getACL()
     * @see #setACL(Map)
     * @see Permission
     */
    public AM setACL(final String user, final Permission perms) {
        mutation.setACL(user, perms);

        return (AM) this;
    }

    /**
     * Grants the specified {@link Permission}s to multiple users on the data written by this
     * mutation. Existing ACL settings on the mutation are replaced.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * import org.apache.hadoop.hbase.security.access.Permission;
     * import java.util.HashMap;
     * import java.util.Map;
     *
     * Map<String, Permission> acl = new HashMap<>();
     * acl.put("alice", new Permission(Permission.Action.READ, Permission.Action.WRITE));
     * acl.put("bob", new Permission(Permission.Action.READ));
     * mutation.setACL(acl);
     * }</pre>
     *
     * @param perms a map of username to {@link Permission}
     * @return this mutation instance, to allow fluent method chaining
     * @see #getACL()
     * @see #setACL(String, Permission)
     * @see Permission
     */
    public AM setACL(final Map<String, Permission> perms) {
        mutation.setACL(perms);

        return (AM) this;
    }

    /**
     * Returns the Time-To-Live (TTL), in milliseconds, that will be attached to cells written by
     * this mutation. When no TTL has been set, HBase returns {@link Long#MAX_VALUE}, meaning the
     * column-family-level TTL (or none) applies.
     *
     * @return the TTL in milliseconds, or {@link Long#MAX_VALUE} if not set
     * @see #setTTL(long)
     */
    public long getTTL() {
        return mutation.getTTL();
    }

    /**
     * Sets the Time-To-Live (TTL), in milliseconds, for cells written by this mutation. Cells
     * older than their TTL are eligible for removal during the next major compaction. The
     * effective TTL on a cell is the minimum of the per-mutation TTL set here and the column
     * family's TTL.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Set TTL to 24 hours
     * mutation.setTTL(86400000);   // 24 hours in milliseconds
     *
     * // Set TTL to 30 days
     * mutation.setTTL(2592000000L);   // 30 days in milliseconds
     * }</pre>
     *
     * @param ttl the TTL to apply, in milliseconds
     * @return this mutation instance, to allow fluent method chaining
     * @see #getTTL()
     */
    public AM setTTL(final long ttl) {
        mutation.setTTL(ttl);

        return (AM) this;
    }

    /**
     * Returns every {@link Cell} already queued by this mutation that matches the given column
     * family and qualifier. The family and qualifier names are converted to bytes via
     * {@link HBaseExecutor#toFamilyQualifierBytes(String)}. Use this to inspect what will be
     * written before the mutation is sent.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyPut put = AnyPut.of("user123");
     * put.addColumn("profile", "name", "Alice");
     * put.addColumn("profile", "name", System.currentTimeMillis() - 1000, "Alice_Old");
     *
     * // Get all cells for the "name" column (may include multiple versions)
     * List<Cell> cells = put.get("profile", "name");
     * System.out.println("Number of versions: " + cells.size());
     * }</pre>
     *
     * @param family the column-family name
     * @param qualifier the column-qualifier name
     * @return a (possibly empty) list of matching {@link Cell}s; never {@code null}
     * @see #get(byte[], byte[])
     * @see #has(String, String)
     * @see Cell
     */
    public List<Cell> get(final String family, final String qualifier) {
        return mutation.get(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier));
    }

    /**
     * Byte-array variant of {@link #get(String, String)}.
     *
     * @param family the column-family name as a byte array
     * @param qualifier the column-qualifier name as a byte array
     * @return a (possibly empty) list of matching {@link Cell}s; never {@code null}
     * @see #get(String, String)
     * @see #has(byte[], byte[])
     * @see Cell
     */
    public List<Cell> get(final byte[] family, final byte[] qualifier) {
        return mutation.get(family, qualifier);
    }

    /**
     * Returns {@code true} if at least one {@link Cell} already queued by this mutation matches
     * the given family and qualifier. The family and qualifier names are converted to bytes via
     * {@link HBaseExecutor#toFamilyQualifierBytes(String)}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyPut put = AnyPut.of("user123");
     * put.addColumn("profile", "name", "Alice");
     *
     * if (put.has("profile", "name")) {
     *     System.out.println("Mutation will update the name column");
     * }
     * }</pre>
     *
     * @param family the column-family name
     * @param qualifier the column-qualifier name
     * @return {@code true} if at least one matching cell is present; {@code false} otherwise
     * @see #has(String, String, long)
     * @see #has(String, String, Object)
     * @see #get(String, String)
     */
    public boolean has(final String family, final String qualifier) {
        return mutation.has(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier));
    }

    /**
     * Returns {@code true} if a queued {@link Cell} matches the given family, qualifier, and
     * timestamp. All three components must match exactly.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long timestamp = System.currentTimeMillis();
     * AnyPut put = AnyPut.of("user123");
     * put.addColumn("profile", "name", timestamp, "Alice");
     *
     * // Check for specific version
     * if (put.has("profile", "name", timestamp)) {
     *     System.out.println("Mutation contains this specific version");
     * }
     * }</pre>
     *
     * @param family the column-family name
     * @param qualifier the column-qualifier name
     * @param ts the cell timestamp, in milliseconds since the epoch
     * @return {@code true} if a matching cell is queued; {@code false} otherwise
     * @see #has(String, String)
     * @see #has(String, String, long, Object)
     */
    public boolean has(final String family, final String qualifier, final long ts) {
        return mutation.has(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier), ts);
    }

    /**
     * Returns {@code true} if a queued {@link Cell} matches the given family, qualifier, and
     * value. The value is converted to bytes via {@link HBaseExecutor#toValueBytes(Object)}
     * before comparison.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyPut put = AnyPut.of("user123");
     * put.addColumn("profile", "name", "Alice");
     *
     * // Check if mutation will write specific value
     * if (put.has("profile", "name", "Alice")) {
     *     System.out.println("Mutation will write 'Alice' to name column");
     * }
     *
     * // This would return false
     * boolean hasBob = put.has("profile", "name", "Bob");
     * }</pre>
     *
     * @param family the column-family name
     * @param qualifier the column-qualifier name
     * @param value the value to look for; encoded via {@link HBaseExecutor#toValueBytes(Object)}
     * @return {@code true} if a matching cell is queued; {@code false} otherwise
     * @see #has(String, String)
     * @see #has(String, String, long, Object)
     */
    public boolean has(final String family, final String qualifier, final Object value) {
        return mutation.has(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier), HBaseExecutor.toValueBytes(value));
    }

    /**
     * Returns {@code true} if a queued {@link Cell} matches all four of family, qualifier,
     * timestamp, and value. The value is converted via {@link HBaseExecutor#toValueBytes(Object)}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long timestamp = System.currentTimeMillis();
     * AnyPut put = AnyPut.of("user123");
     * put.addColumn("profile", "name", timestamp, "Alice");
     *
     * // Check for exact match of all parameters
     * if (put.has("profile", "name", timestamp, "Alice")) {
     *     System.out.println("Found exact cell match");
     * }
     *
     * // Different timestamp would return false
     * boolean hasDifferentTs = put.has("profile", "name", timestamp - 1000, "Alice");
     * }</pre>
     *
     * @param family the column-family name
     * @param qualifier the column-qualifier name
     * @param ts the cell timestamp, in milliseconds since the epoch
     * @param value the value to look for; encoded via {@link HBaseExecutor#toValueBytes(Object)}
     * @return {@code true} if a matching cell is queued; {@code false} otherwise
     * @see #has(String, String)
     * @see #has(String, String, long)
     * @see #has(String, String, Object)
     */
    public boolean has(final String family, final String qualifier, final long ts, final Object value) {
        return mutation.has(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier), ts, HBaseExecutor.toValueBytes(value));
    }

    /**
     * Byte-array variant of {@link #has(String, String)}.
     *
     * @param family the column-family name as a byte array
     * @param qualifier the column-qualifier name as a byte array
     * @return {@code true} if at least one matching cell is queued; {@code false} otherwise
     * @see #has(String, String)
     * @see #has(byte[], byte[], long)
     */
    public boolean has(final byte[] family, final byte[] qualifier) {
        return mutation.has(family, qualifier);
    }

    /**
     * Byte-array variant of {@link #has(String, String, long)}.
     *
     * @param family the column-family name as a byte array
     * @param qualifier the column-qualifier name as a byte array
     * @param ts the cell timestamp, in milliseconds since the epoch
     * @return {@code true} if a matching cell is queued; {@code false} otherwise
     * @see #has(String, String, long)
     * @see #has(byte[], byte[], long, byte[])
     */
    public boolean has(final byte[] family, final byte[] qualifier, final long ts) {
        return mutation.has(family, qualifier, ts);
    }

    /**
     * Byte-array variant of {@link #has(String, String, Object)}.
     *
     * @param family the column-family name as a byte array
     * @param qualifier the column-qualifier name as a byte array
     * @param value the value to look for as a byte array
     * @return {@code true} if a matching cell is queued; {@code false} otherwise
     * @see #has(String, String, Object)
     * @see #has(byte[], byte[], long, byte[])
     */
    public boolean has(final byte[] family, final byte[] qualifier, final byte[] value) {
        return mutation.has(family, qualifier, value);
    }

    /**
     * Byte-array variant of {@link #has(String, String, long, Object)}.
     *
     * @param family the column-family name as a byte array
     * @param qualifier the column-qualifier name as a byte array
     * @param ts the cell timestamp, in milliseconds since the epoch
     * @param value the value to look for as a byte array
     * @return {@code true} if a matching cell is queued; {@code false} otherwise
     * @see #has(String, String, long, Object)
     * @see #has(byte[], byte[])
     */
    public boolean has(final byte[] family, final byte[] qualifier, final long ts, final byte[] value) {
        return mutation.has(family, qualifier, ts, value);
    }

    /**
     * Returns the row key this mutation targets. Implementation of {@link Row#getRow()} —
     * every cell queued in this mutation belongs to this row.
     *
     * @return the row key as a byte array; never {@code null}
     */
    @Override
    public byte[] getRow() {
        return mutation.getRow();
    }

    /**
     * Returns {@code true} when this mutation has no queued cells (and therefore would be a no-op
     * if executed).
     *
     * @return {@code true} if this mutation contains no cells; {@code false} otherwise
     * @see #size()
     */
    public boolean isEmpty() {
        return mutation.isEmpty();
    }

    /**
     * Returns the total number of {@link Cell}s queued in this mutation, summed across all
     * families. For example, three columns in one family plus two columns in another family
     * yields {@code 5}.
     *
     * @return the total number of cells in this mutation
     * @see #isEmpty()
     * @see #numFamilies()
     */
    public int size() {
        return mutation.size();
    }

    /**
     * Returns the number of distinct column families that contain at least one queued cell. For
     * example, queueing cells in {@code "cf1"} and {@code "cf2"} yields {@code 2}, regardless of
     * how many cells are in each.
     *
     * @return the number of column families in this mutation
     * @see #size()
     */
    public int numFamilies() {
        return mutation.numFamilies();
    }

    /**
     * Returns the approximate heap footprint of this mutation in bytes, including the row key,
     * queued cells, and internal data structures. Useful for sizing batches.
     *
     * @return the approximate on-heap size, in bytes
     */
    public long heapSize() {
        return mutation.heapSize();
    }

    /**
     * Compares this mutation with another {@link Row} by row key. Implementation of
     * {@link Row#compareTo(Row)}, delegated to the wrapped {@link Mutation}.
     *
     * @param d the other {@link Row} to compare with
     * @return a negative integer, zero, or a positive integer as this row key is less than,
     *         equal to, or greater than the other row's key
     * @deprecated As of HBase 2.0.0; will be removed in HBase 3.0.0. Use {@link Row#COMPARATOR}
     *             instead.
     */
    @Override
    @Deprecated
    public int compareTo(final Row d) {
        return mutation.compareTo(d);
    }
}
