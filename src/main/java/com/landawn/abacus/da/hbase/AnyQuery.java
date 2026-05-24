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
 * Abstract base wrapper for HBase {@link Query} operations — the read-side counterpart of
 * {@link AnyMutation}. Concrete subclasses are {@link AnyGet} and {@link AnyScan}.
 *
 * <p>This class exposes the read-time controls that are common to both single-row Gets and
 * multi-row Scans: server-side {@link Filter}s, {@link Consistency} level, {@link Authorizations},
 * row-level ACLs, replica targeting, {@link IsolationLevel}, the load-column-families-on-demand
 * hint, and per-family time ranges. String family / qualifier names are converted to byte arrays
 * via {@link HBaseExecutor}.</p>
 *
 * <h3>Key features</h3>
 * <ul>
 * <li><strong>Server-side filtering</strong>: any {@link Filter} or {@code FilterList}.</li>
 * <li><strong>Consistency control</strong>: strong reads from the primary, or timeline-consistent
 *     reads from any replica.</li>
 * <li><strong>Security</strong>: visibility-label {@link Authorizations} and ACL bytes.</li>
 * <li><strong>Performance tuning</strong>: on-demand column-family loading, explicit replica id,
 *     {@link IsolationLevel}, and per-family time ranges.</li>
 * </ul>
 *
 * <h3>Common usage patterns</h3>
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Basic query with filtering
 * anyQuery.setFilter(new SingleColumnValueFilter(family, qualifier, CompareOperator.EQUAL, value))
 *         .setConsistency(Consistency.STRONG)
 *         .setAuthorizations(new Authorizations("PUBLIC", "INTERNAL"));
 *
 * // Performance-optimized query
 * anyQuery.setLoadColumnFamiliesOnDemand(true)
 *         .setReplicaId(1)  // Read from a specific replica
 *         .setIsolationLevel(IsolationLevel.READ_COMMITTED);
 *
 * // Time-range filtering per column family
 * long startTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);
 * anyQuery.setColumnFamilyTimeRange("metrics", startTime, System.currentTimeMillis());
 * }</pre>
 *
 * <h3>Consistency levels (see {@link Consistency})</h3>
 * <ul>
 * <li><strong>STRONG</strong>: read from the primary region replica only.</li>
 * <li><strong>TIMELINE</strong>: read from any replica; results may be slightly stale.</li>
 * </ul>
 *
 * <h3>Isolation levels (see {@link IsolationLevel})</h3>
 * <ul>
 * <li><strong>READ_COMMITTED</strong>: only committed data is visible (default).</li>
 * <li><strong>READ_UNCOMMITTED</strong>: in-flight writes are also visible.</li>
 * </ul>
 *
 * @param <AQ> the concrete subtype of {@code AnyQuery}; declared so fluent setters can return
 *             {@code AQ} and preserve the concrete type during chaining
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
     * Constructs a new {@code AnyQuery} that delegates to the supplied HBase {@link Query}. The
     * wrapped query is also passed to the {@link AnyOperationWithAttributes} constructor as the
     * underlying operation.
     *
     * @param query the HBase {@link Query} to wrap; must not be {@code null}
     * @throws IllegalArgumentException if {@code query} is {@code null}
     */
    protected AnyQuery(final Query query) {
        super(query);
        this.query = query;
    }

    /**
     * Returns the server-side {@link Filter} currently attached to this query, or {@code null} if
     * none has been set.
     *
     * @return the current {@link Filter}, or {@code null} if no filter is set
     * @see #setFilter(Filter)
     * @see Filter
     */
    public Filter getFilter() {
        return query.getFilter();
    }

    /**
     * Attaches a server-side {@link Filter} to this query, replacing any previous filter. The
     * filter is evaluated after HBase's standard TTL / column-matching / delete-marker / version
     * checks.
     *
     * <p><strong>Common filter types:</strong></p>
     * <ul>
     * <li><strong>SingleColumnValueFilter</strong>: filter rows based on a column value</li>
     * <li><strong>PrefixFilter</strong>: match rows by row-key prefix</li>
     * <li><strong>ColumnPrefixFilter</strong>: match columns by qualifier prefix</li>
     * <li><strong>FilterList</strong>: combine filters with AND/OR logic</li>
     * <li><strong>PageFilter</strong>: cap the number of rows returned</li>
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
     * @param filter the {@link Filter} to apply; pass {@code null} to clear any existing filter
     * @return this query instance, to allow fluent method chaining
     * @see #getFilter()
     * @see Filter
     */
    public AQ setFilter(final Filter filter) {
        query.setFilter(filter);

        return (AQ) this;
    }

    /**
     * Returns the visibility {@link Authorizations} attached to this query. Cells whose
     * visibility expression is not satisfied by this set are filtered out by the server.
     *
     * @return the {@link Authorizations} previously set, or {@code null} if none has been set
     * @throws DeserializationException if the stored authorizations cannot be deserialized by HBase
     * @see #setAuthorizations(Authorizations)
     * @see Authorizations
     */
    public Authorizations getAuthorizations() throws DeserializationException {
        return query.getAuthorizations();
    }

    /**
     * Sets the visibility labels this query is allowed to read. Cells whose visibility
     * expression is not satisfied by these labels are filtered out by the server.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // User has PUBLIC and INTERNAL clearance
     * Authorizations auths = new Authorizations("PUBLIC", "INTERNAL");
     * query.setAuthorizations(auths);
     * }</pre>
     *
     * @param authorizations the {@link Authorizations} to apply
     * @return this query instance, to allow fluent method chaining
     * @see #getAuthorizations()
     * @see Authorizations
     */
    public AQ setAuthorizations(final Authorizations authorizations) {
        query.setAuthorizations(authorizations);

        return (AQ) this;
    }

    /**
     * Returns the serialized Access Control List attached to this query. The bytes are the
     * protobuf-serialized form of the user-to-permission map written by
     * {@link #setACL(String, Permission)} or {@link #setACL(Map)}.
     *
     * @return the serialized ACL bytes, or {@code null} if no ACL has been set
     * @see #setACL(String, Permission)
     * @see #setACL(Map)
     * @see Permission
     */
    public byte[] getACL() {
        return query.getACL();
    }

    /**
     * Grants the specified {@link Permission} to a single user on this query. Existing ACL
     * settings on the query are replaced.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Grant read permission to a specific user
     * query.setACL("alice", new Permission(Permission.Action.READ));
     * }</pre>
     *
     * @param user the username to grant permissions to
     * @param perms the {@link Permission} defining the allowed actions
     * @return this query instance, to allow fluent method chaining
     * @see #getACL()
     * @see #setACL(Map)
     * @see Permission
     */
    public AQ setACL(final String user, final Permission perms) {
        query.setACL(user, perms);

        return (AQ) this;
    }

    /**
     * Grants the specified {@link Permission}s to multiple users on this query. Existing ACL
     * settings on the query are replaced.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, Permission> acl = new HashMap<>();
     * acl.put("alice", new Permission(Permission.Action.READ));
     * acl.put("bob", new Permission(Permission.Action.READ));
     * query.setACL(acl);
     * }</pre>
     *
     * @param perms a map of username to {@link Permission}
     * @return this query instance, to allow fluent method chaining
     * @see #getACL()
     * @see #setACL(String, Permission)
     * @see Permission
     */
    public AQ setACL(final Map<String, Permission> perms) {
        query.setACL(perms);

        return (AQ) this;
    }

    /**
     * Returns the {@link Consistency} level configured for this query.
     * <ul>
     * <li><strong>STRONG</strong>: always read from the primary region replica.</li>
     * <li><strong>TIMELINE</strong>: may read from any replica; the result can be stale.</li>
     * </ul>
     *
     * @return the current consistency level
     * @see #setConsistency(Consistency)
     * @see Consistency
     */
    public Consistency getConsistency() {
        return query.getConsistency();
    }

    /**
     * Sets the {@link Consistency} level for this query.
     * <ul>
     * <li><strong>STRONG</strong>: reads from the primary replica only — slower but always consistent.</li>
     * <li><strong>TIMELINE</strong>: reads from any available replica — faster but potentially stale.</li>
     * </ul>
     *
     * <p>Combine with {@link #setReplicaId(int)} to pin a TIMELINE read to a specific replica.</p>
     *
     * @param consistency the consistency level to apply
     * @return this query instance, to allow fluent method chaining
     * @see #getConsistency()
     * @see #setReplicaId(int)
     * @see Consistency
     */
    public AQ setConsistency(final Consistency consistency) {
        query.setConsistency(consistency);

        return (AQ) this;
    }

    /**
     * Returns the region replica id this query is pinned to, or {@code -1} when none has been
     * pinned (in which case the read is satisfied by the primary, or by any replica when
     * {@link Consistency#TIMELINE} is in effect).
     *
     * @return the pinned replica id, or {@code -1} if none
     * @see #setReplicaId(int)
     * @see #setConsistency(Consistency)
     */
    public int getReplicaId() {
        return query.getReplicaId();
    }

    /**
     * Pins this query to a specific region replica id. Intended for advanced use cases such as
     * spreading read load across replicas or reading from a geographically closer replica;
     * normally combined with {@link #setConsistency(Consistency)} of {@link Consistency#TIMELINE}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * query.setConsistency(Consistency.TIMELINE)
     *      .setReplicaId(1);   // Read from replica 1 instead of primary (0)
     * }</pre>
     *
     * @param id the replica id to read from ({@code 0} = primary; {@code 1+} = secondaries)
     * @return this query instance, to allow fluent method chaining
     * @see #getReplicaId()
     * @see #setConsistency(Consistency)
     * @see Consistency#TIMELINE
     */
    public AQ setReplicaId(final int id) {
        query.setReplicaId(id);

        return (AQ) this;
    }

    /**
     * Returns the {@link IsolationLevel} configured for this query.
     * <ul>
     * <li><strong>READ_COMMITTED</strong>: only committed data is visible (default).</li>
     * <li><strong>READ_UNCOMMITTED</strong>: also includes in-flight writes that have not yet
     *     been committed.</li>
     * </ul>
     *
     * @return the isolation level; defaults to {@link IsolationLevel#READ_COMMITTED} when none
     *         has been set explicitly
     * @see #setIsolationLevel(IsolationLevel)
     * @see IsolationLevel
     */
    public IsolationLevel getIsolationLevel() {
        return query.getIsolationLevel();
    }

    /**
     * Sets the {@link IsolationLevel} for this query, controlling whether in-flight (uncommitted)
     * writes are visible.
     * <ul>
     * <li><strong>READ_COMMITTED</strong>: only committed data is visible — stronger guarantee.</li>
     * <li><strong>READ_UNCOMMITTED</strong>: also includes in-flight writes — may see data that
     *     is subsequently rolled back.</li>
     * </ul>
     *
     * @param level the isolation level to apply
     * @return this query instance, to allow fluent method chaining
     * @see #getIsolationLevel()
     * @see IsolationLevel
     */
    public AQ setIsolationLevel(final IsolationLevel level) {
        query.setIsolationLevel(level);

        return (AQ) this;
    }

    /**
     * Returns the raw {@code loadColumnFamiliesOnDemand} setting as it was supplied to
     * {@link #setLoadColumnFamiliesOnDemand(boolean)} — or {@code null} when the setting has not
     * been explicitly configured. Use {@link #doLoadColumnFamiliesOnDemand()} to get the effective
     * {@code boolean} value after server-side defaults are applied.
     *
     * @return the raw setting, or {@code null} if it has not been set
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
     * @return this query instance, to allow fluent method chaining
     * @see #doLoadColumnFamiliesOnDemand()
     * @see #getLoadColumnFamiliesOnDemandValue()
     */
    public AQ setLoadColumnFamiliesOnDemand(final boolean value) {
        query.setLoadColumnFamiliesOnDemand(value);

        return (AQ) this;
    }

    /**
     * Returns the effective {@code boolean} value of the {@code loadColumnFamiliesOnDemand}
     * setting after any cluster defaults are applied. Unlike
     * {@link #getLoadColumnFamiliesOnDemandValue()}, this method never returns {@code null}.
     *
     * @return {@code true} if on-demand column family loading is enabled, {@code false} otherwise
     * @see #setLoadColumnFamiliesOnDemand(boolean)
     * @see #getLoadColumnFamiliesOnDemandValue()
     */
    public boolean doLoadColumnFamiliesOnDemand() {
        return query.doLoadColumnFamiliesOnDemand();
    }

    /**
     * Returns the per-column-family time ranges set on this query. Keys are family names as
     * byte arrays; values are the {@link TimeRange} restrictions applied to those families.
     *
     * @return the per-family time-range map; never {@code null} but may be empty
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
     * @param cf the column-family name; converted to bytes via
     *           {@link HBaseExecutor#toFamilyQualifierBytes(String)}
     * @param minStamp minimum timestamp, in milliseconds, inclusive
     * @param maxStamp maximum timestamp, in milliseconds, exclusive
     * @return this query instance, to allow fluent method chaining
     * @throws IllegalArgumentException if {@code minStamp} or {@code maxStamp} is negative or if
     *         {@code maxStamp < minStamp}
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
     * @param cf the column-family name as a byte array
     * @param minStamp minimum timestamp, in milliseconds, inclusive
     * @param maxStamp maximum timestamp, in milliseconds, exclusive
     * @return this query instance, to allow fluent method chaining
     * @throws IllegalArgumentException if {@code minStamp} or {@code maxStamp} is negative or if
     *         {@code maxStamp < minStamp}
     * @see #setColumnFamilyTimeRange(String, long, long)
     * @see #getColumnFamilyTimeRange()
     * @see TimeRange
     */
    public AQ setColumnFamilyTimeRange(final byte[] cf, final long minStamp, final long maxStamp) {
        query.setColumnFamilyTimeRange(cf, minStamp, maxStamp);

        return (AQ) this;
    }
}
