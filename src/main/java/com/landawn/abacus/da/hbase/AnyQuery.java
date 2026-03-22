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

import java.util.Map;

import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Query;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.visibility.Authorizations;

/**
 * Abstract base wrapper for HBase {@code Query} operations that simplifies data retrieval
 * by providing automatic type conversion, advanced filtering capabilities, and comprehensive
 * query configuration management. This class serves as the foundation for all HBase query
 * operations including Get and Scan operations.
 *
 * <p>This wrapper eliminates the complexity of manual byte array conversions while providing
 * access to advanced HBase query features such as filtering, consistency control, authorization,
 * replica selection, and column family loading strategies.</p>
 *
 * <h3>Key Features:</h3>
 * <ul>
 * <li><strong>Type Safety</strong>: Automatic conversion between Java objects and HBase byte arrays</li>
 * <li><strong>Advanced Filtering</strong>: Server-side filter application with full Filter API support</li>
 * <li><strong>Consistency Control</strong>: Strong vs. timeline consistency options for read operations</li>
 * <li><strong>Security Integration</strong>: Authorization and visibility label support</li>
 * <li><strong>Performance Tuning</strong>: Column family loading, replica selection, and isolation control</li>
 * <li><strong>Time-based Queries</strong>: Per-column-family time range support</li>
 * </ul>
 *
 * <h3>Common Usage Patterns:</h3>
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Basic query with filtering
 * anyQuery.setFilter(new SingleColumnValueFilter(family, qualifier, CompareOperator.EQUAL, value))
 *         .setConsistency(Consistency.STRONG)
 *         .setAuthorizations(new Authorizations("PUBLIC", "INTERNAL"));
 *
 * // Performance-optimized query
 * anyQuery.setLoadColumnFamiliesOnDemand(true)
 *         .setReplicaId(1)  // Read from specific replica
 *         .setIsolationLevel(IsolationLevel.READ_COMMITTED);
 *
 * // Time-range filtering per column family
 * long startTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);
 * anyQuery.setColumnFamilyTimeRange("metrics", startTime, System.currentTimeMillis());
 * }</pre>
 *
 * <h3>Consistency Levels:</h3>
 * <ul>
 * <li><strong>STRONG</strong>: Read from primary replica, strongest consistency</li>
 * <li><strong>TIMELINE</strong>: Read from any replica, eventual consistency</li>
 * </ul>
 *
 * <h3>Isolation Levels:</h3>
 * <ul>
 * <li><strong>READ_COMMITTED</strong>: Only see committed data (default)</li>
 * <li><strong>READ_UNCOMMITTED</strong>: See committed and uncommitted data</li>
 * </ul>
 *
 * @param <AQ> the concrete subtype of AnyQuery for method chaining
 * @see AnyOperationWithAttributes
 * @see Query
 * @see AnyGet
 * @see AnyScan
 * @see Filter
 * @see Consistency
 * @see <a href="http://hbase.apache.org/devapidocs/index.html">Apache HBase Java API Documentation</a>
 */
abstract class AnyQuery<AQ extends AnyQuery<AQ>> extends AnyOperationWithAttributes<AQ> {

    protected final Query query;

    /**
     * Constructs a new AnyQuery wrapper around the specified HBase Query.
     *
     * @param query the HBase Query to wrap; must not be null
     * @throws IllegalArgumentException if query is null
     */
    protected AnyQuery(final Query query) {
        super(query);
        this.query = query;
    }

    /**
     * Returns the server-side filter currently applied to this query.
     * <p>
     * Filters enable server-side data filtering, reducing network traffic and
     * improving query performance by processing filtering logic on the HBase
     * region servers before returning results to the client.
     * </p>
     *
     * @return the current Filter, or null if no filter is set
     * @see #setFilter(Filter)
     * @see Filter
     */
    public Filter getFilter() {
        return query.getFilter();
    }

    /**
     * Applies a server-side filter to this query to reduce network traffic and improve performance.
     * <p>
     * Filters are evaluated on the HBase region servers before results are sent to the client,
     * which significantly reduces network overhead and improves query performance. The filter
     * is applied AFTER all standard HBase checks (TTL, column matching, deletes, and version limits).
     * </p>
     *
     * <p><strong>Common Filter Types:</strong></p>
     * <ul>
     * <li><strong>SingleColumnValueFilter</strong>: Filter rows based on column values</li>
     * <li><strong>PrefixFilter</strong>: Match rows with specific row key prefixes</li>
     * <li><strong>ColumnPrefixFilter</strong>: Match columns with specific prefixes</li>
     * <li><strong>FilterList</strong>: Combine multiple filters with AND/OR logic</li>
     * <li><strong>PageFilter</strong>: Limit the number of rows returned</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Filter rows where age column equals 30
     * Filter filter = new SingleColumnValueFilter(
     *     family, qualifier,
     *     CompareOperator.EQUAL,
     *     Bytes.toBytes(30)
     * );
     * query.setFilter(filter);
     *
     * // Combine multiple filters
     * FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
     * filterList.addFilter(new PrefixFilter(Bytes.toBytes("user:")));
     * filterList.addFilter(new PageFilter(100));
     * query.setFilter(filterList);
     * }</pre>
     *
     * @param filter the Filter to apply on the server side; can be null to clear any existing filter
     * @return this query instance for method chaining
     * @see #getFilter()
     * @see Filter
     */
    public AQ setFilter(final Filter filter) {
        query.setFilter(filter);

        return (AQ) this;
    }

    /**
     * Returns the authorization labels for this query operation.
     * <p>
     * Authorizations specify which visibility labels the current user possesses,
     * determining which cells with visibility restrictions can be accessed by this query.
     * This is part of HBase's cell-level security model.
     * </p>
     *
     * @return the current Authorizations, or null if not set
     * @throws DeserializationException if authorization data cannot be deserialized
     * @see #setAuthorizations(Authorizations)
     * @see Authorizations
     */
    public Authorizations getAuthorizations() throws DeserializationException {
        return query.getAuthorizations();
    }

    /**
     * Sets the authorization labels that this query can access.
     * <p>
     * Authorizations define which visibility-labeled cells can be returned by this query.
     * Only cells with visibility labels that match the provided authorizations will be
     * included in the query results. This enables fine-grained, cell-level access control.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // User has PUBLIC and INTERNAL clearance
     * Authorizations auths = new Authorizations("PUBLIC", "INTERNAL");
     * query.setAuthorizations(auths);
     * }</pre>
     *
     * @param authorizations the authorization labels to apply to this query
     * @return this query instance for method chaining
     * @see #getAuthorizations()
     * @see Authorizations
     */
    public AQ setAuthorizations(final Authorizations authorizations) {
        query.setAuthorizations(authorizations);

        return (AQ) this;
    }

    /**
     * Returns the serialized Access Control List (ACL) for this query operation.
     * <p>
     * The ACL defines which users and groups have specific permissions to perform
     * this query operation. The returned byte array contains the serialized
     * representation of the permission structure.
     * </p>
     *
     * @return the serialized ACL for this query operation, or null if no ACL has been set
     * @see #setACL(String, Permission)
     * @see #setACL(Map)
     * @see Permission
     */
    public byte[] getACL() {
        return query.getACL();
    }

    /**
     * Sets Access Control List permissions for a specific user on this query operation.
     * <p>
     * This method grants specific permissions to a single user for executing this query.
     * The permissions control what actions the user can perform, such as reading data
     * from the table.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Grant read permission to a specific user
     * query.setACL("alice", new Permission(Permission.Action.READ));
     * }</pre>
     *
     * @param user the username to grant permissions to; must not be null
     * @param perms the Permission object defining what actions are allowed
     * @return this query instance for method chaining
     * @throws IllegalArgumentException if user is null
     * @see #getACL()
     * @see #setACL(Map)
     * @see Permission
     */
    public AQ setACL(final String user, final Permission perms) {
        query.setACL(user, perms);

        return (AQ) this;
    }

    /**
     * Sets Access Control List permissions for multiple users on this query operation.
     * <p>
     * This method allows setting permissions for multiple users in a single call.
     * Each entry in the map specifies a username and the corresponding permissions
     * that user should have for executing this query.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, Permission> acl = new HashMap<>();
     * acl.put("alice", new Permission(Permission.Action.READ));
     * acl.put("bob", new Permission(Permission.Action.READ));
     * query.setACL(acl);
     * }</pre>
     *
     * @param perms a map of usernames to their corresponding Permission objects; must not be null
     * @return this query instance for method chaining
     * @throws IllegalArgumentException if perms is null
     * @see #getACL()
     * @see #setACL(String, Permission)
     * @see Permission
     */
    public AQ setACL(final Map<String, Permission> perms) {
        query.setACL(perms);

        return (AQ) this;
    }

    /**
     * Returns the consistency level set for this query operation.
     * <p>
     * Consistency levels determine the trade-off between read performance and data freshness:
     * </p>
     * <ul>
     * <li><strong>STRONG</strong>: Always read from primary replica, strongest consistency</li>
     * <li><strong>TIMELINE</strong>: May read from secondary replicas, eventual consistency</li>
     * </ul>
     *
     * @return the current consistency level for this query
     * @see #setConsistency(Consistency)
     * @see Consistency
     */
    public Consistency getConsistency() {
        return query.getConsistency();
    }

    /**
     * Sets the consistency level for this query to control read behavior.
     * <p>
     * This setting determines whether the query should prioritize consistency or availability:
     * </p>
     * <ul>
     * <li><strong>STRONG</strong>: Reads from primary replica only, may be slower but always consistent</li>
     * <li><strong>TIMELINE</strong>: Reads from any available replica, faster but potentially stale data</li>
     * </ul>
     * 
     * <p>Use TIMELINE consistency with {@link #setReplicaId(int)} for reading from specific replicas.</p>
     *
     * @param consistency the consistency level to apply to this query
     * @return this query instance for method chaining
     * @see #getConsistency()
     * @see #setReplicaId(int)
     * @see Consistency
     */
    public AQ setConsistency(final Consistency consistency) {
        query.setConsistency(consistency);

        return (AQ) this;
    }

    /**
     * Returns the region replica ID from which this query will fetch data.
     * <p>
     * Replica IDs allow reading from specific replicas in a replicated HBase setup.
     * This can be useful for load balancing read operations or when you need to read
     * from a geographically closer replica.
     * </p>
     *
     * @return the region replica ID, or -1 if not set (reads from primary)
     * @see #setReplicaId(int)
     * @see #setConsistency(Consistency)
     */
    public int getReplicaId() {
        return query.getReplicaId();
    }

    /**
     * Specifies the region replica ID from which this query should fetch data.
     * <p>
     * <strong>Expert API:</strong> This is an advanced feature that should only be used
     * if you understand HBase replication architecture. Use this together with
     * {@code setConsistency(Consistency.TIMELINE)} to enable reads from secondary replicas.
     * </p>
     * 
     * <p><strong>Use Cases:</strong></p>
     * <ul>
     * <li><strong>Load Distribution</strong>: Spread read load across multiple replicas</li>
     * <li><strong>Geographic Optimization</strong>: Read from geographically closer replicas</li>
     * <li><strong>Failover Scenarios</strong>: Continue reading when primary replica is unavailable</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * query.setConsistency(Consistency.TIMELINE)
     *      .setReplicaId(1);   // Read from replica 1 instead of primary (0)
     * }</pre>
     *
     * @param id the replica ID to read from (0 = primary, 1+ = secondary replicas)
     * @return this query instance for method chaining
     * @see #getReplicaId()
     * @see #setConsistency(Consistency)
     * @see Consistency#TIMELINE
     */
    public AQ setReplicaId(final int id) {
        query.setReplicaId(id);

        return (AQ) this;
    }

    /**
     * Returns the isolation level set for this query operation.
     * <p>
     * Isolation levels control which data versions are visible during query execution:
     * </p>
     * <ul>
     * <li><strong>READ_COMMITTED</strong>: Only see committed data (default, safer)</li>
     * <li><strong>READ_UNCOMMITTED</strong>: See both committed and uncommitted data (faster, less safe)</li>
     * </ul>
     *
     * @return the isolation level for this query; defaults to READ_COMMITTED if not explicitly set
     * @see #setIsolationLevel(IsolationLevel)
     * @see IsolationLevel
     */
    public IsolationLevel getIsolationLevel() {
        return query.getIsolationLevel();
    }

    /**
     * Sets the isolation level for this query to control transaction visibility.
     * <p>
     * Isolation levels determine what data this query can see during concurrent operations:
     * </p>
     * <ul>
     * <li><strong>READ_COMMITTED</strong>: Only returns data from committed transactions,
     *     providing stronger consistency but potentially missing recent writes</li>
     * <li><strong>READ_UNCOMMITTED</strong>: Returns data from both committed and uncommitted transactions,
     *     providing faster reads but potentially seeing data that may be rolled back</li>
     * </ul>
     * 
     * <p><strong>Use READ_UNCOMMITTED carefully:</strong> While it can improve performance,
     * it may return data that hasn't been fully committed and could be subject to rollback.</p>
     *
     * @param level the isolation level to apply to this query
     * @return this query instance for method chaining
     * @see #getIsolationLevel()
     * @see IsolationLevel
     */
    public AQ setIsolationLevel(final IsolationLevel level) {
        query.setIsolationLevel(level);

        return (AQ) this;
    }

    /**
     * Returns the raw loadColumnFamiliesOnDemand setting for this query.
     * <p>
     * This method returns the exact value that was set, which can be null if the setting
     * was never configured. Use {@link #doLoadColumnFamiliesOnDemand()} to get the
     * effective boolean value that will be used.
     * </p>
     *
     * @return the loadColumnFamiliesOnDemand setting, or null if not explicitly set
     * @see #setLoadColumnFamiliesOnDemand(boolean)
     * @see #doLoadColumnFamiliesOnDemand()
     */
    public Boolean getLoadColumnFamiliesOnDemandValue() {
        return query.getLoadColumnFamiliesOnDemandValue();
    }

    /**
     * Sets whether column families should be loaded on-demand to optimize performance for filtered queries.
     * <p>
     * When enabled, HBase only loads column family data for rows that pass the filter criteria,
     * rather than loading all column families for all rows. This can provide significant performance
     * improvements when using column-specific filters (like SingleColumnValueFilter) on tables with
     * large column families.
     * </p>
     *
     * <p><strong>Performance Benefits:</strong></p>
     * <ul>
     * <li>Dramatically reduces I/O when filtering on specific columns</li>
     * <li>Reduces network traffic by not loading unnecessary column family data</li>
     * <li>Most beneficial with SingleColumnValueFilter and filterIfMissing=true</li>
     * </ul>
     *
     * <p><strong>Important Consistency Considerations:</strong></p>
     * <p>Enabling on-demand loading can lead to inconsistent results in concurrent environments:</p>
     * <ol>
     * <li><strong>Phantom Rows</strong>: Concurrent updates to multiple column families may result
     *     in rows with mixed old/new data that never actually existed as a consistent snapshot.
     *     Example: Filtering on "cat_videos == 1" during a concurrent update changing both
     *     cat_videos and video columns might return {cat_videos=1, video="new_value"}.</li>
     * <li><strong>Missing Column Families</strong>: Concurrent region splits on tables with 3+
     *     column families may result in some rows missing certain column families.</li>
     * </ol>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Enable on-demand loading for performance
     * query.setLoadColumnFamiliesOnDemand(true)
     *      .setFilter(new SingleColumnValueFilter(
     *          family, "status",
     *          CompareOperator.EQUAL,
     *          Bytes.toBytes("active")
     *      ));
     * }</pre>
     *
     * @param value {@code true} to enable on-demand column family loading;
     *              {@code false} to load all column families upfront (default)
     * @return this query instance for method chaining
     * @see #doLoadColumnFamiliesOnDemand()
     * @see #getLoadColumnFamiliesOnDemandValue()
     */
    public AQ setLoadColumnFamiliesOnDemand(final boolean value) {
        query.setLoadColumnFamiliesOnDemand(value);

        return (AQ) this;
    }

    /**
     * Returns whether on-demand column family loading is enabled for this query.
     * <p>
     * This method returns the effective boolean value that will be used, taking into
     * account both explicit settings and cluster defaults. On-demand loading can
     * significantly improve performance when you're filtering on specific columns.
     * </p>
     *
     * @return {@code true} if on-demand column family loading is enabled, {@code false} otherwise
     * @see #setLoadColumnFamiliesOnDemand(boolean)
     * @see #getLoadColumnFamiliesOnDemandValue()
     */
    public boolean doLoadColumnFamiliesOnDemand() {
        return query.doLoadColumnFamiliesOnDemand();
    }

    /**
     * Returns the per-column-family time ranges set for this query.
     * <p>
     * Column family time ranges allow specifying different time windows for different
     * column families within the same query. This enables fine-grained temporal filtering
     * when different column families have different update patterns or time requirements.
     * </p>
     *
     * @return a Map of column family names to their TimeRange objects; never null but may be empty
     * @see #setColumnFamilyTimeRange(String, long, long)
     * @see TimeRange
     */
    public Map<byte[], TimeRange> getColumnFamilyTimeRange() {
        return query.getColumnFamilyTimeRange();
    }

    /**
     * Sets a time range filter for a specific column family to retrieve only versions within the specified period.
     * <p>
     * This method restricts the query to only return cell versions with timestamps in the range
     * [minStamp, maxStamp) for the specified column family. This is useful when different column
     * families have different update patterns or when you need temporal filtering on specific
     * column families without affecting others.
     * </p>
     *
     * <p><strong>Important Notes:</strong></p>
     * <ul>
     * <li>The time range is half-open: includes minStamp (inclusive) but excludes maxStamp (exclusive)</li>
     * <li>Column family time ranges take precedence over any global time range</li>
     * <li>Default behavior returns only the latest version (maxVersions = 1)</li>
     * <li>To retrieve multiple versions within the time range, you must also increase maxVersions</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Get data from the last hour for "metrics" column family
     * long endTime = System.currentTimeMillis();
     * long startTime = endTime - TimeUnit.HOURS.toMillis(1);
     * query.setColumnFamilyTimeRange("metrics", startTime, endTime);
     *
     * // Get last 5 versions from the last 24 hours
     * AnyGet get = AnyGet.of(rowKey);
     * get.setMaxVersions(5)
     *    .setColumnFamilyTimeRange("data",
     *        System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1),
     *        System.currentTimeMillis());
     * }</pre>
     *
     * @param cf the column family name to apply the time range filter to; must not be null
     * @param minStamp minimum timestamp value in milliseconds, inclusive
     * @param maxStamp maximum timestamp value in milliseconds, exclusive
     * @return this query instance for method chaining
     * @throws IllegalArgumentException if cf is null or minStamp >= maxStamp
     * @see #setColumnFamilyTimeRange(byte[], long, long)
     * @see #getColumnFamilyTimeRange()
     * @see TimeRange
     */
    public AQ setColumnFamilyTimeRange(final String cf, final long minStamp, final long maxStamp) {
        query.setColumnFamilyTimeRange(toFamilyQualifierBytes(cf), minStamp, maxStamp);

        return (AQ) this;
    }

    /**
     * Sets a time range filter for a specific column family using a byte array column family name.
     * <p>
     * This is the byte array variant of {@link #setColumnFamilyTimeRange(String, long, long)}.
     * Use this method when you already have the column family name as a byte array to avoid
     * additional conversion overhead.
     * </p>
     *
     * <p><strong>Important Notes:</strong></p>
     * <ul>
     * <li>The time range is half-open: includes minStamp (inclusive) but excludes maxStamp (exclusive)</li>
     * <li>Column family time ranges take precedence over any global time range</li>
     * <li>Default behavior returns only the latest version (maxVersions = 1)</li>
     * <li>To retrieve multiple versions within the time range, you must also increase maxVersions</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * byte[] cfBytes = Bytes.toBytes("metrics");
     * long endTime = System.currentTimeMillis();
     * long startTime = endTime - TimeUnit.HOURS.toMillis(1);
     * query.setColumnFamilyTimeRange(cfBytes, startTime, endTime);
     * }</pre>
     *
     * @param cf the column family name as a byte array; must not be null
     * @param minStamp minimum timestamp value in milliseconds, inclusive
     * @param maxStamp maximum timestamp value in milliseconds, exclusive
     * @return this query instance for method chaining
     * @throws IllegalArgumentException if cf is null or minStamp >= maxStamp
     * @see #setColumnFamilyTimeRange(String, long, long)
     * @see #getColumnFamilyTimeRange()
     * @see TimeRange
     */
    public AQ setColumnFamilyTimeRange(final byte[] cf, final long minStamp, final long maxStamp) {
        query.setColumnFamilyTimeRange(cf, minStamp, maxStamp);

        return (AQ) this;
    }
}
