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
 * Abstract base wrapper for HBase {@code Mutation} operations that simplifies data modification
 * by providing automatic type conversion, fluent API design, and comprehensive mutation management.
 * This class serves as the foundation for all HBase mutation operations including Put, Delete, 
 * Append, and Increment operations.
 *
 * <p>This wrapper eliminates the complexity of manual byte array conversions while providing
 * access to advanced HBase mutation features such as durability control, timestamp management,
 * cell visibility, access control, and TTL (Time-To-Live) settings.</p>
 *
 * <h3>Key Features:</h3>
 * <ul>
 * <li><strong>Type Safety</strong>: Automatic conversion between Java objects and HBase byte arrays</li>
 * <li><strong>Fluent API</strong>: Method chaining for readable mutation construction</li>
 * <li><strong>Advanced Controls</strong>: Durability, visibility, ACL, and TTL management</li>
 * <li><strong>Cell Management</strong>: Direct access to mutation cells and family maps</li>
 * <li><strong>Metadata Support</strong>: Timestamp, cluster ID, and fingerprint access</li>
 * <li><strong>Performance Optimization</strong>: Efficient handling of large mutation operations</li>
 * </ul>
 *
 * <h3>Common Usage Patterns:</h3>
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Basic mutation with durability control
 * anyMutation.setTimestamp(System.currentTimeMillis())
 *           .setDurability(Durability.SYNC_WAL)
 *           .setTTL(86400000);   // 24 hours in milliseconds
 *
 * // Security and visibility controls
 * anyMutation.setCellVisibility(new CellVisibility("SECRET&DEPT_A"))
 *           .setACL("admin", Permission.Action.READ);
 *
 * // Query existing mutation data
 * boolean hasColumn = anyMutation.has("family", "qualifier");
 * List<Cell> cells = anyMutation.get("family", "qualifier");
 * }</pre>
 *
 * <h3>Durability Levels:</h3>
 * <ul>
 * <li><strong>SKIP_WAL</strong>: Fastest, no durability guarantee</li>
 * <li><strong>ASYNC_WAL</strong>: Fast, asynchronous write-ahead log</li>
 * <li><strong>SYNC_WAL</strong>: Default, synchronous write-ahead log</li>
 * <li><strong>FSYNC_WAL</strong>: Strongest durability, forces disk sync</li>
 * </ul>
 *
 * @param <AM> the concrete subtype of AnyMutation for method chaining
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
     * Constructs a new AnyMutation wrapper around the specified HBase Mutation.
     *
     * @param mutation the HBase Mutation to wrap; must not be null
     * @throws IllegalArgumentException if mutation is null
     */
    protected AnyMutation(final Mutation mutation) {
        super(mutation);
        if (mutation == null) {
            throw new IllegalArgumentException("Mutation must not be null");
        }
        this.mutation = mutation;
    }

    /**
     * Returns a CellScanner for iterating over all cells in this mutation.
     * <p>
     * The CellScanner provides efficient access to all cells contained in this mutation,
     * allowing for inspection or processing of the mutation's data without converting
     * to higher-level data structures. This is useful for debugging, logging, or
     * custom processing of mutation contents.
     * </p>
     *
     * @return a CellScanner for all cells in this mutation; never null
     * @see CellScanner
     * @see Cell
     */
    public CellScanner cellScanner() {
        return mutation.cellScanner();
    }

    /**
     * Compiles the column family (schema) information into a Map for debugging and analysis.
     * <p>
     * This method provides a structured view of the mutation's schema characteristics,
     * including column families, qualifiers, and other identifying attributes. The returned
     * map is particularly useful for debugging, logging, and administration tools that need
     * to analyze mutation structure without processing the actual cell data.
     * </p>
     *
     * @return a Map containing the mutation's fingerprint with column family information;
     *         never null but may be empty
     * @see #toMap()
     */
    @Override
    public Map<String, Object> getFingerprint() {
        return mutation.getFingerprint();
    }

    /**
     * Returns the durability level set for this mutation.
     * <p>
     * Durability determines how strongly this mutation is persisted to disk.
     * Higher durability levels provide stronger guarantees against data loss
     * but may impact performance.
     * </p>
     *
     * @return the current durability level for this mutation
     * @see #setDurability(Durability)
     * @see Durability
     */
    public Durability getDurability() {
        return mutation.getDurability();
    }

    /**
     * Sets the durability level for this mutation to control persistence guarantees.
     * <p>
     * Durability levels provide a trade-off between performance and data safety:
     * </p>
     * <ul>
     * <li><strong>SKIP_WAL</strong>: Fastest, bypasses write-ahead log (data loss risk)</li>
     * <li><strong>ASYNC_WAL</strong>: Fast, asynchronous write-ahead log</li>
     * <li><strong>SYNC_WAL</strong>: Default, synchronous write-ahead log</li>
     * <li><strong>FSYNC_WAL</strong>: Strongest, forces filesystem sync</li>
     * </ul>
     *
     * @param d the durability level to apply to this mutation
     * @return this mutation instance for method chaining
     * @see #getDurability()
     * @see Durability
     */
    public AM setDurability(final Durability d) {
        mutation.setDurability(d);

        return (AM) this;
    }

    /**
     * Returns the complete family-to-cell mapping for this mutation.
     * <p>
     * This method provides access to the internal structure of the mutation,
     * organized as a NavigableMap where keys are column family names (as byte arrays)
     * and values are lists of Cell objects. This is useful for advanced processing,
     * debugging, or when you need direct access to the mutation's cell structure.
     * </p>
     *
     * @return a NavigableMap of column families to their respective Cell lists; never null
     * @see Cell
     * @see #get(String, String)
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
     * Returns the timestamp set for this mutation.
     * <p>
     * The timestamp determines the version of the data being written. If not explicitly
     * set, HBase will use the current server time. Timestamps are used for versioning,
     * conflict resolution, and time-based queries.
     * </p>
     *
     * @return the timestamp for this mutation, or {@code HConstants.LATEST_TIMESTAMP} if not explicitly set (server will assign the current time)
     * @see #setTimestamp(long)
     */
    public long getTimestamp() {
        return mutation.getTimestamp();
    }

    /**
     * Sets the timestamp for this mutation to control data versioning.
     * <p>
     * The timestamp determines when this data version was created and is crucial for:
     * </p>
     * <ul>
     * <li><strong>Version Control</strong>: Multiple versions of the same cell</li>
     * <li><strong>Time-based Queries</strong>: Querying data as of a specific time</li>
     * <li><strong>Conflict Resolution</strong>: Newer timestamps typically win</li>
     * <li><strong>Data Lifecycle</strong>: TTL and compaction decisions</li>
     * </ul>
     *
     * @param timestamp the timestamp to assign to this mutation (milliseconds since epoch)
     * @return this mutation instance for method chaining
     * @see #getTimestamp()
     * @see System#currentTimeMillis()
     */
    public AM setTimestamp(final long timestamp) {
        mutation.setTimestamp(timestamp);

        return (AM) this;
    }

    /**
     * Returns the list of cluster IDs that have consumed this mutation.
     * <p>
     * Cluster IDs are used in multi-cluster replication scenarios to track which
     * clusters have already processed this mutation. This prevents infinite loops
     * in bidirectional replication and helps ensure data consistency across clusters.
     * </p>
     *
     * @return a list of cluster UUIDs that have consumed this mutation; may be null or empty
     * @see #setClusterIds(List)
     * @see UUID
     */
    public List<UUID> getClusterIds() {
        return mutation.getClusterIds();
    }

    /**
     * Marks that the clusters with the given cluster IDs have consumed this mutation.
     * <p>
     * This method is primarily used by HBase replication infrastructure to prevent
     * replication loops in multi-cluster deployments. When a mutation is replicated
     * to other clusters, those cluster IDs are added to prevent the mutation from
     * being replicated back to the originating cluster.
     * </p>
     *
     * @param clusterIds the list of cluster UUIDs that have consumed this mutation
     * @return this mutation instance for method chaining
     * @see #getClusterIds()
     * @see UUID
     */
    public AM setClusterIds(final List<UUID> clusterIds) {
        mutation.setClusterIds(clusterIds);

        return (AM) this;
    }

    /**
     * Returns the cell visibility expression associated with this mutation.
     * <p>
     * Cell visibility expressions define which users or groups can access the data
     * in this mutation. The expression is evaluated by HBase's visibility label system
     * to determine read access permissions.
     * </p>
     *
     * @return the cell visibility expression, or null if not set
     * @throws DeserializationException if the visibility expression cannot be deserialized
     * @see #setCellVisibility(CellVisibility)
     * @see CellVisibility
     */
    public CellVisibility getCellVisibility() throws DeserializationException {
        return mutation.getCellVisibility();
    }

    /**
     * Sets the visibility expression that controls access to cells in this mutation.
     * <p>
     * Cell visibility provides fine-grained access control at the cell level using
     * boolean expressions of visibility labels. Only users with the required labels
     * can read cells that match the visibility expression.
     * </p>
     * 
     * <p><strong>Expression Examples:</strong></p>
     * <ul>
     * <li><code>"PUBLIC"</code> - Accessible to users with PUBLIC label</li>
     * <li><code>"SECRET&amp;DEPT_A"</code> - Requires both SECRET and DEPT_A labels</li>
     * <li><code>"(SECRET|CONFIDENTIAL)&amp;DEPT_A"</code> - Complex boolean expression</li>
     * </ul>
     *
     * @param expression the visibility expression to apply to cells in this mutation
     * @return this mutation instance for method chaining
     * @see #getCellVisibility()
     * @see CellVisibility
     */
    public AM setCellVisibility(final CellVisibility expression) {
        mutation.setCellVisibility(expression);

        return (AM) this;
    }

    /**
     * Returns the serialized Access Control List (ACL) for this operation.
     * <p>
     * The ACL defines which users and groups have specific permissions on the data
     * affected by this mutation. The returned byte array contains the serialized
     * representation of the permission structure.
     * </p>
     *
     * @return the serialized ACL for this operation, or null if none has been set
     * @see #setACL(String, Permission)
     * @see #setACL(Map)
     * @see Permission
     */
    public byte[] getACL() {
        return mutation.getACL();
    }

    /**
     * Sets Access Control List permissions for a specific user on this mutation.
     * <p>
     * This method grants specific permissions to a single user for the data affected
     * by this mutation. The permissions control what actions the user can perform
     * on the data (read, write, execute, create, admin).
     * </p>
     *
     * @param user the username to grant permissions to; must not be null
     * @param perms the Permission object defining what actions are allowed
     * @return this mutation instance for method chaining
     * @throws IllegalArgumentException if user is null
     * @see #getACL()
     * @see #setACL(Map)
     * @see Permission
     */
    public AM setACL(final String user, final Permission perms) {
        mutation.setACL(user, perms);

        return (AM) this;
    }

    /**
     * Sets Access Control List permissions for multiple users on this mutation.
     * <p>
     * This method allows setting permissions for multiple users in a single call.
     * Each entry in the map specifies a username and the corresponding permissions
     * that user should have on the data affected by this mutation.
     * </p>
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
     * @param perms a map of usernames to their corresponding Permission objects
     * @return this mutation instance for method chaining
     * @throws IllegalArgumentException if perms is null
     * @see #getACL()
     * @see #setACL(String, Permission)
     * @see Permission
     */
    public AM setACL(final Map<String, Permission> perms) {
        mutation.setACL(perms);

        return (AM) this;
    }

    /**
     * Returns the Time-To-Live (TTL) value set for this mutation, in milliseconds.
     * <p>
     * TTL determines how long the data written by this mutation should be retained
     * in HBase before being automatically deleted. This is useful for implementing
     * data lifecycle policies and preventing storage from growing indefinitely.
     * </p>
     *
     * @return the TTL for this mutation in milliseconds, or Long.MAX_VALUE if not set
     * @see #setTTL(long)
     */
    public long getTTL() {
        return mutation.getTTL();
    }

    /**
     * Sets the Time-To-Live (TTL) for data written by this mutation, in milliseconds.
     * <p>
     * TTL provides automatic data expiration, which is useful for:
     * </p>
     * <ul>
     * <li><strong>Session Data</strong>: Automatically expire user sessions</li>
     * <li><strong>Cache Data</strong>: Implement time-based cache invalidation</li>
     * <li><strong>Temporary Data</strong>: Auto-cleanup of temporary or intermediate data</li>
     * <li><strong>Compliance</strong>: Meet data retention requirements</li>
     * </ul>
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
     * @param ttl the TTL for data written by this mutation, in milliseconds
     * @return this mutation instance for method chaining
     * @see #getTTL()
     */
    public AM setTTL(final long ttl) {
        mutation.setTTL(ttl);

        return (AM) this;
    }

    /**
     * Retrieves all Cell objects that match the specified column family and qualifier.
     * <p>
     * This method searches through the mutation's family map and returns all cells
     * (KeyValue objects) that match both the family and qualifier. This is useful for
     * inspecting what values will be written to a specific column before executing the mutation.
     * </p>
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
     * @param family the column family name; automatically converted to bytes
     * @param qualifier the column qualifier name; automatically converted to bytes
     * @return a list of Cell objects matching the family and qualifier; returns an empty
     *         list if no cells match or if the family doesn't exist
     * @see #get(byte[], byte[])
     * @see #has(String, String)
     * @see Cell
     */
    public List<Cell> get(final String family, final String qualifier) {
        return mutation.get(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier));
    }

    /**
     * Retrieves all Cell objects that match the specified column family and qualifier using byte arrays.
     * <p>
     * This is the byte array variant of {@link #get(String, String)}. Use this method when you
     * already have the family and qualifier as byte arrays to avoid additional conversion overhead.
     * </p>
     *
     * @param family the column family name as a byte array
     * @param qualifier the column qualifier name as a byte array
     * @return a list of Cell objects matching the family and qualifier; returns an empty
     *         list if no cells match or if the family doesn't exist
     * @see #get(String, String)
     * @see #has(byte[], byte[])
     * @see Cell
     */
    public List<Cell> get(final byte[] family, final byte[] qualifier) {
        return mutation.get(family, qualifier);
    }

    /**
     * Checks if this mutation contains any cells for the specified column family and qualifier.
     * <p>
     * This is a convenience method to quickly check whether this mutation will affect a
     * specific column without retrieving the actual cell data. Both the family and qualifier
     * must match for this method to return true.
     * </p>
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
     * @param family the column family name; automatically converted to bytes
     * @param qualifier the column qualifier name; automatically converted to bytes
     * @return {@code true} if this mutation contains at least one cell for the given
     *         family and qualifier; {@code false} otherwise
     * @see #has(String, String, long)
     * @see #has(String, String, Object)
     * @see #get(String, String)
     */
    public boolean has(final String family, final String qualifier) {
        return mutation.has(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier));
    }

    /**
     * Checks if this mutation contains a cell with the specified family, qualifier, and timestamp.
     * <p>
     * This method provides more precise checking than {@link #has(String, String)} by also
     * matching the timestamp. All three parameters (family, qualifier, and timestamp) must
     * match exactly for this method to return true. This is useful when working with multiple
     * versions of the same column.
     * </p>
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
     * @param family the column family name; automatically converted to bytes
     * @param qualifier the column qualifier name; automatically converted to bytes
     * @param ts the timestamp in milliseconds since epoch
     * @return {@code true} if this mutation contains a cell matching all three parameters;
     *         {@code false} otherwise
     * @see #has(String, String)
     * @see #has(String, String, long, Object)
     */
    public boolean has(final String family, final String qualifier, final long ts) {
        return mutation.has(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier), ts);
    }

    /**
     * Checks if this mutation contains a cell with the specified family, qualifier, and value.
     * <p>
     * This method checks whether this mutation will write a specific value to a specific column.
     * All three parameters (family, qualifier, and value) must match exactly for this method
     * to return true. The value is automatically converted to bytes for comparison.
     * </p>
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
     * @param family the column family name; automatically converted to bytes
     * @param qualifier the column qualifier name; automatically converted to bytes
     * @param value the value to check for; automatically converted to bytes for comparison
     * @return {@code true} if this mutation contains a cell matching the family, qualifier,
     *         and value; {@code false} otherwise
     * @see #has(String, String)
     * @see #has(String, String, long, Object)
     */
    public boolean has(final String family, final String qualifier, final Object value) {
        return mutation.has(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier), HBaseExecutor.toValueBytes(value));
    }

    /**
     * Checks if this mutation contains a cell with the specified family, qualifier, timestamp, and value.
     * <p>
     * This is the most specific version of the has() methods, requiring all four parameters
     * to match exactly. This is useful when you need to verify the presence of a specific
     * versioned value in the mutation.
     * </p>
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
     * @param family the column family name; automatically converted to bytes
     * @param qualifier the column qualifier name; automatically converted to bytes
     * @param ts the timestamp in milliseconds since epoch
     * @param value the value to check for; automatically converted to bytes for comparison
     * @return {@code true} if this mutation contains a cell matching all four parameters;
     *         {@code false} otherwise
     * @see #has(String, String)
     * @see #has(String, String, long)
     * @see #has(String, String, Object)
     */
    public boolean has(final String family, final String qualifier, final long ts, final Object value) {
        return mutation.has(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier), ts, HBaseExecutor.toValueBytes(value));
    }

    /**
     * Checks if this mutation contains any cells for the specified column family and qualifier using byte arrays.
     * <p>
     * This is the byte array variant of {@link #has(String, String)}. Use this method when you
     * already have the family and qualifier as byte arrays to avoid additional conversion overhead.
     * </p>
     *
     * @param family the column family name as a byte array
     * @param qualifier the column qualifier name as a byte array
     * @return {@code true} if this mutation contains at least one cell for the given
     *         family and qualifier; {@code false} otherwise
     * @see #has(String, String)
     * @see #has(byte[], byte[], long)
     */
    public boolean has(final byte[] family, final byte[] qualifier) {
        return mutation.has(family, qualifier);
    }

    /**
     * Checks if this mutation contains a cell with the specified family, qualifier, and timestamp using byte arrays.
     * <p>
     * This is the byte array variant of {@link #has(String, String, long)}. Use this method when you
     * already have the family and qualifier as byte arrays to avoid additional conversion overhead.
     * </p>
     *
     * @param family the column family name as a byte array
     * @param qualifier the column qualifier name as a byte array
     * @param ts the timestamp in milliseconds since epoch
     * @return {@code true} if this mutation contains a cell matching all three parameters;
     *         {@code false} otherwise
     * @see #has(String, String, long)
     * @see #has(byte[], byte[], long, byte[])
     */
    public boolean has(final byte[] family, final byte[] qualifier, final long ts) {
        return mutation.has(family, qualifier, ts);
    }

    /**
     * Checks if this mutation contains a cell with the specified family, qualifier, and value using byte arrays.
     * <p>
     * This is the byte array variant of {@link #has(String, String, Object)}. Use this method when you
     * already have the family, qualifier, and value as byte arrays to avoid additional conversion overhead.
     * </p>
     *
     * @param family the column family name as a byte array
     * @param qualifier the column qualifier name as a byte array
     * @param value the value to check for as a byte array
     * @return {@code true} if this mutation contains a cell matching the family, qualifier,
     *         and value; {@code false} otherwise
     * @see #has(String, String, Object)
     * @see #has(byte[], byte[], long, byte[])
     */
    public boolean has(final byte[] family, final byte[] qualifier, final byte[] value) {
        return mutation.has(family, qualifier, value);
    }

    /**
     * Checks if this mutation contains a cell with the specified family, qualifier, timestamp, and value using byte arrays.
     * <p>
     * This is the byte array variant of {@link #has(String, String, long, Object)}. Use this method when you
     * already have all parameters as byte arrays to avoid additional conversion overhead. All four parameters
     * must match exactly for this method to return true.
     * </p>
     *
     * @param family the column family name as a byte array
     * @param qualifier the column qualifier name as a byte array
     * @param ts the timestamp in milliseconds since epoch
     * @param value the value to check for as a byte array
     * @return {@code true} if this mutation contains a cell matching all four parameters;
     *         {@code false} otherwise
     * @see #has(String, String, long, Object)
     * @see #has(byte[], byte[])
     */
    public boolean has(final byte[] family, final byte[] qualifier, final long ts, final byte[] value) {
        return mutation.has(family, qualifier, ts, value);
    }

    /**
     * Returns the row key for this mutation as a byte array.
     * <p>
     * The row key uniquely identifies the row in HBase that this mutation will affect.
     * All cells in this mutation belong to this row.
     * </p>
     *
     * @return the row key as a byte array; never null
     */
    @Override
    public byte[] getRow() {
        return mutation.getRow();
    }

    /**
     * Checks if this mutation is empty (contains no cells).
     * <p>
     * An empty mutation has no cells to write and will have no effect when executed.
     * This can be useful for validation before attempting to execute the mutation.
     * </p>
     *
     * @return {@code true} if this mutation contains no cells; {@code false} otherwise
     * @see #size()
     */
    public boolean isEmpty() {
        return mutation.isEmpty();
    }

    /**
     * Returns the total number of Cell objects (KeyValue objects) in this mutation.
     * <p>
     * This count includes all cells across all column families. For example, if you
     * add 3 columns to one family and 2 columns to another family, size() will return 5.
     * </p>
     *
     * @return the total number of cells in this mutation
     * @see #isEmpty()
     * @see #numFamilies()
     */
    public int size() {
        return mutation.size();
    }

    /**
     * Returns the number of column families affected by this mutation.
     * <p>
     * This returns the count of distinct column families that have at least one cell
     * in this mutation. For example, if you add cells to "cf1" and "cf2", this method
     * returns 2, regardless of how many cells are in each family.
     * </p>
     *
     * @return the number of column families in this mutation
     * @see #size()
     */
    public int numFamilies() {
        return mutation.numFamilies();
    }

    /**
     * Returns the approximate heap size occupied by this mutation in bytes.
     * <p>
     * This method provides an estimate of the memory footprint of this mutation,
     * including the row key, all cells, and internal data structures. This can be
     * useful for memory management and batch size optimization.
     * </p>
     *
     * @return the approximate heap size in bytes
     */
    public long heapSize() {
        return mutation.heapSize();
    }

    /**
     * Compares this mutation with another Row operation for ordering.
     *
     * @param d the Row operation to compare with
     * @return a negative integer, zero, or positive integer as this object is less than, equal to, or greater than the specified object
     * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
     *             Use {@link Row#COMPARATOR} instead
     */
    @Override
    @Deprecated
    public int compareTo(final Row d) {
        return mutation.compareTo(d);
    }
}
