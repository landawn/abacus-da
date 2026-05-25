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
import static com.landawn.abacus.da.hbase.HBaseExecutor.toValueBytes;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;

import com.landawn.abacus.annotation.SuppressFBWarnings;
import com.landawn.abacus.da.hbase.annotation.ColumnFamily;
import com.landawn.abacus.parser.ParserUtil;
import com.landawn.abacus.parser.ParserUtil.BeanInfo;
import com.landawn.abacus.parser.ParserUtil.PropInfo;
import com.landawn.abacus.util.Beans;
import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.HBaseColumn;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.Tuple.Tuple3;

/**
 * A fluent builder wrapper around HBase {@link Put} that simplifies data insertion and updates by
 * providing automatic type conversion and object-relational mapping. This class hides the manual
 * byte-array conversions of the native {@link Put} API and offers both programmatic and
 * entity-based construction.
 *
 * <h2>Cell-write semantics (overwrite vs versioning)</h2>
 * <p>HBase stores each cell as a {@code (row, family, qualifier, timestamp)} tuple. The
 * {@code addColumn}/{@code add} methods on this class accumulate cells in the underlying Put,
 * which the server then applies as follows:</p>
 * <ul>
 *   <li>When two cells share the same {@code (row, family, qualifier, timestamp)}, the later one
 *       replaces the earlier one (overwrite).</li>
 *   <li>When two cells share the same key but differ in timestamp, both versions are stored
 *       (subject to the column family's {@code VERSIONS} setting, which controls how many
 *       historical versions HBase retains).</li>
 *   <li>{@code addColumn(family, qualifier, value)} variants without an explicit timestamp use the
 *       server's current time, so successive calls within a single millisecond may overwrite each
 *       other.</li>
 * </ul>
 * <p>In short, repeated {@code addColumn} calls do <i>not</i> concatenate or merge values; for
 * append-style semantics use {@link AnyAppend}, and for atomic counter increments use
 * {@link AnyIncrement}.</p>
 *
 * <p>AnyPut supports multiple construction patterns:
 * <ul>
 * <li><strong>Programmatic Construction</strong>: Manual column-by-column specification</li>
 * <li><strong>Entity Mapping</strong>: Automatic conversion from Java objects using annotations</li>
 * <li><strong>Batch Operations</strong>: Efficient handling of multiple entities</li>
 * <li><strong>Versioning</strong>: Support for timestamped data insertion</li>
 * <li><strong>Column Family Mapping</strong>: Flexible column family and qualifier assignment</li>
 * </ul>
 *
 * <h2>Usage Examples</h2>
 *
 * <h3>Programmatic Construction</h3>
 * <pre>{@code
 * // Basic column insertion
 * AnyPut put = AnyPut.of("user123")
 *                    .addColumn("info", "name", "John Doe")
 *                    .addColumn("info", "email", "john@example.com")
 *                    .addColumn("prefs", "theme", "dark");
 *
 * // Timestamped data insertion
 * long timestamp = System.currentTimeMillis();
 * AnyPut timestampedPut = AnyPut.of("user123", timestamp)
 *                               .addColumn("info", "name", timestamp, "John Doe")
 *                               .addColumn("activity", "lastLogin", timestamp, new Date());
 * }</pre>
 *
 * <h3>Entity Mapping</h3>
 * <pre>{@code
 * &#64;ColumnFamily("info")
 * public class User {
 *     &#64;Id
 *     private String userId;
 *     private String name;
 *     private String email;
 *     // getters and setters
 * }
 *
 * User user = new User("user123", "John Doe", "john@example.com");
 * AnyPut entityPut = AnyPut.create(user);
 *
 * // Batch entity insertion
 * List&lt;User&gt; users = Arrays.asList(user1, user2, user3);
 * List&lt;AnyPut&gt; batchPuts = AnyPut.create(users);
 * }</pre>
 *
 * <h3>Selective Property Mapping</h3>
 * <pre>{@code
 * // Only insert specific properties
 * Set&lt;String&gt; selectedProps = Set.of("name", "email");
 * AnyPut selectivePut = AnyPut.create(user, selectedProps);
 *
 * // Custom naming policy
 * AnyPut snakeCasePut = AnyPut.create(user, NamingPolicy.SNAKE_CASE);
 * }</pre>
 *
 * <h3>Key Features:</h3>
 * <ul>
 * <li><strong>Type Safety</strong>: Automatic conversion of Java objects to HBase byte arrays</li>
 * <li><strong>Entity Mapping</strong>: Object-relational mapping with {@code @ColumnFamily} and {@code @Id} annotations</li>
 * <li><strong>Flexible Construction</strong>: Multiple factory methods for different use cases</li>
 * <li><strong>Batch Support</strong>: Efficient processing of collections of entities</li>
 * <li><strong>Versioning Control</strong>: Support for timestamped data with version management</li>
 * <li><strong>Naming Policies</strong>: Configurable property-to-column name mapping strategies</li>
 * <li><strong>Performance Optimization</strong>: Reuses common family/qualifier byte arrays through {@link HBaseExecutor}'s internal pool</li>
 * </ul>
 *
 * <h3>Entity Mapping Rules:</h3>
 * <ul>
 * <li><strong>Row Key</strong>: The single property registered via {@link HBaseExecutor#registerRowKeyProperty(Class, String)}
 *     (or whose setter is otherwise discovered as the row-key setter) becomes the row key.</li>
 * <li><strong>Column Families</strong>: Resolved from {@link ColumnFamily} annotations on the class
 *     or individual properties; otherwise the property name is used.</li>
 * <li><strong>Column Qualifiers</strong>: For nested bean properties, the bean's own property
 *     names become qualifiers under the enclosing property's family.</li>
 * <li><strong>Versioning</strong>: {@link HBaseColumn} property values are stored at their
 *     embedded {@code version()} timestamp.</li>
 * <li><strong>Collections / Maps</strong>: {@code Collection<HBaseColumn>} and
 *     {@code Map<Long, HBaseColumn>} properties are expanded into one cell per element, each at
 *     its own version, enabling multi-version storage.</li>
 * <li><strong>Null values</strong>: Properties whose value is {@code null} are skipped.</li>
 * </ul>
 *
 * <h3>Performance Considerations:</h3>
 * <ul>
 * <li><strong>Batch Operations</strong>: Use {@code create(Collection)} for multiple entities</li>
 * <li><strong>Selective Mapping</strong>: Specify only needed properties to reduce data transfer</li>
 * <li><strong>Byte Array Pooling</strong>: Family/qualifier strings are converted via {@link HBaseExecutor}'s shared interning cache</li>
 * <li><strong>Entity Validation</strong>: Entity structure is validated when {@code create} is first called for a class</li>
 * </ul>
 *
 * @see Put
 * @see AnyMutation
 * @see AnyAppend
 * @see AnyIncrement
 * @see ColumnFamily
 * @see HBaseExecutor
 * @see <a href="http://hbase.apache.org/devapidocs/index.html">Apache HBase Java API Documentation</a>
 */
public final class AnyPut extends AnyMutation<AnyPut> {

    private final Put put;

    /**
     * Package-private constructor backing {@link #of(Object)}.
     *
     * @param rowKey the row key, converted to bytes via {@link HBaseExecutor#toRowBytes(Object)}
     */
    AnyPut(final Object rowKey) {
        super(new Put(toRowBytes(rowKey)));
        put = (Put) mutation;
    }

    /**
     * Package-private constructor backing {@link #of(Object, long)}.
     *
     * @param rowKey the row key, converted to bytes
     * @param timestamp the timestamp assigned to all cells added to this Put
     */
    AnyPut(final Object rowKey, final long timestamp) {
        super(new Put(toRowBytes(rowKey), timestamp));
        put = (Put) mutation;
    }

    /**
     * Package-private constructor backing {@link #of(Object, int, int)}.
     *
     * @param rowKey the row key whose byte representation is sliced
     * @param rowOffset the starting offset (0-based) within the row key bytes
     * @param rowLength the number of bytes to use from the row key starting at offset
     */
    AnyPut(final Object rowKey, final int rowOffset, final int rowLength) {
        super(new Put(toRowBytes(rowKey), rowOffset, rowLength));
        put = (Put) mutation;
    }

    /**
     * Package-private constructor backing {@link #of(Object, int, int, long)}.
     *
     * @param rowKey the row key whose byte representation is sliced
     * @param rowOffset the starting offset (0-based) within the row key bytes
     * @param rowLength the number of bytes to use from the row key starting at offset
     * @param timestamp the timestamp assigned to all cells added to this Put
     */
    AnyPut(final Object rowKey, final int rowOffset, final int rowLength, final long timestamp) {
        super(new Put(toRowBytes(rowKey), rowOffset, rowLength, timestamp));
        put = (Put) mutation;
    }

    /**
     * Package-private constructor backing {@link #of(Object, boolean)}.
     *
     * @param rowKey the row key, converted to bytes
     * @param rowIsImmutable when true, the row key byte array is treated as immutable and not defensively copied
     */
    AnyPut(final Object rowKey, final boolean rowIsImmutable) {
        super(new Put(toRowBytes(rowKey), rowIsImmutable));
        put = (Put) mutation;
    }

    /**
     * Package-private constructor backing {@link #of(Object, long, boolean)}.
     *
     * @param rowKey the row key, converted to bytes
     * @param timestamp the timestamp assigned to all cells added to this Put
     * @param rowIsImmutable when true, the row key byte array is treated as immutable and not defensively copied
     */
    AnyPut(final Object rowKey, final long timestamp, final boolean rowIsImmutable) {
        super(new Put(toRowBytes(rowKey), timestamp, rowIsImmutable));
        put = (Put) mutation;
    }

    /**
     * Package-private constructor backing {@link #of(ByteBuffer)}.
     *
     * @param rowKey the row key as a ByteBuffer; bytes from current position to limit are used
     */
    AnyPut(final ByteBuffer rowKey) {
        super(new Put(rowKey));
        put = (Put) mutation;
    }

    /**
     * Package-private constructor backing {@link #of(ByteBuffer, long)}.
     *
     * @param rowKey the row key as a ByteBuffer; bytes from current position to limit are used
     * @param timestamp the timestamp assigned to all cells added to this Put
     */
    AnyPut(final ByteBuffer rowKey, final long timestamp) {
        super(new Put(rowKey, timestamp));
        put = (Put) mutation;
    }

    /**
     * Package-private constructor backing {@link #of(Put)}. Creates a deep copy of the given Put.
     *
     * @param putToCopy the existing HBase Put to deep-copy
     */
    AnyPut(final Put putToCopy) {
        super(new Put(putToCopy));
        put = (Put) mutation;
    }

    /**
     * Creates a new AnyPut instance for the specified row key.
     *
     * <p>This is the primary factory method for creating put operations. The row key is
     * automatically converted to the appropriate byte array format using HBase's standard
     * conversion mechanisms. Use this variant for most common put operations.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyPut put = AnyPut.of("user123")
     *                    .addColumn("info", "name", "John Doe")
     *                    .addColumn("info", "email", "john@example.com");
     * }</pre>
     *
     * @param rowKey the row key for the put operation, automatically converted to bytes (String, Long, byte[], etc.); should not be {@code null}
     * @return a new AnyPut instance configured for the specified row
     * @see #of(Object, long)
     * @see #of(ByteBuffer)
     */
    public static AnyPut of(final Object rowKey) {
        return new AnyPut(rowKey);
    }

    /**
     * Creates a new AnyPut for the specified row key with a default timestamp applied to every
     * cell that does not carry its own.
     *
     * <p>The {@code timestamp} becomes the Put's default cell timestamp: any
     * {@code addColumn(family, qualifier, value)} call that does not specify a timestamp uses
     * {@code timestamp}; calls that pass an explicit {@code ts} override it on a per-cell basis.
     * Use this when you need to insert data with a specific point-in-time version, such as
     * backdating data or implementing temporal-database patterns.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long eventTime = 1609459200000L;  // Specific timestamp
     * AnyPut timestampedPut = AnyPut.of("user123", eventTime)
     *                               .addColumn("events", "action", "login")
     *                               .addColumn("events", "location", "NYC");
     * }</pre>
     *
     * @param rowKey the row key for the put operation, automatically converted to bytes
     * @param timestamp the default timestamp for cells added without an explicit timestamp
     *                  (milliseconds since epoch)
     * @return a new AnyPut configured with the specified default timestamp
     * @throws IllegalArgumentException if {@code timestamp} is negative (validated by the
     *         underlying {@link Put} constructor)
     * @see #of(Object)
     * @see #addColumn(String, String, long, Object)
     */
    public static AnyPut of(final Object rowKey, final long timestamp) {
        return new AnyPut(rowKey, timestamp);
    }

    /**
     * Creates a new AnyPut instance using a subset of the row key object's byte representation.
     *
     * <p>This factory method enables put operations on composite or structured row keys where only
     * a portion of the serialized row key should be used as the actual HBase row key. This is
     * particularly useful for prefix-based row key schemes, fixed-width key formats, or when working
     * with complex key structures that embed multiple identifiers.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Use first 10 bytes of a composite key
     * String compositeKey = "user123456_session789";
     * AnyPut partialKeyPut = AnyPut.of(compositeKey, 0, 10)
     *                              .addColumn("data", "value", "sample");
     * }</pre>
     *
     * @param rowKey the row key object whose byte representation will be sliced
     * @param rowOffset the starting position (0-based) within the row key bytes
     * @param rowLength the number of bytes to use from the row key, starting at offset
     * @return a new AnyPut instance configured with the partial row key
     * @throws IllegalArgumentException if {@code rowOffset}/{@code rowLength} do not describe a
     *         valid sub-range of the row key bytes (validated by the underlying {@link Put}
     *         constructor)
     * @see #of(Object)
     * @see #of(Object, int, int, long)
     */
    public static AnyPut of(final Object rowKey, final int rowOffset, final int rowLength) {
        return new AnyPut(rowKey, rowOffset, rowLength);
    }

    /**
     * Creates a new AnyPut using a subset of the row key's byte representation, with a default
     * timestamp applied to every cell that does not carry its own.
     *
     * <p>Combines partial row-key extraction (see {@link #of(Object, int, int)}) with the
     * default-timestamp behaviour described in {@link #of(Object, long)}, providing precise put
     * capabilities for complex row-key structures and time-partitioned data.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String compositeKey = "user123456_region_data";
     * long backfillTime = 1609459200000L;
     * AnyPut complexPut = AnyPut.of(compositeKey, 0, 10, backfillTime)
     *                           .addColumn("data", "imported", true);
     * }</pre>
     *
     * @param rowKey the row key object whose byte representation will be sliced
     * @param rowOffset the starting position (0-based) within the row key bytes
     * @param rowLength the number of bytes to use from the row key, starting at offset
     * @param timestamp the default timestamp for cells added without an explicit timestamp
     *                  (milliseconds since epoch)
     * @return a new AnyPut configured with the partial row key and default timestamp
     * @throws IllegalArgumentException if {@code rowOffset}/{@code rowLength} do not describe a
     *         valid sub-range of the row key bytes, or if {@code timestamp} is negative
     *         (validated by the underlying {@link Put} constructor)
     * @see #of(Object, int, int)
     * @see #of(Object, long)
     */
    public static AnyPut of(final Object rowKey, final int rowOffset, final int rowLength, final long timestamp) {
        return new AnyPut(rowKey, rowOffset, rowLength, timestamp);
    }

    /**
     * Creates a new AnyPut instance with row immutability flag control.
     *
     * <p>This factory method allows you to specify whether the row key byte array is immutable,
     * which can enable performance optimizations in HBase. When {@code rowIsImmutable} is true,
     * HBase can avoid defensive copying of the row key byte array. Use this variant only when
     * you can guarantee the row key byte array will not be modified after the put is created.</p>
     *
     * <p><strong>Performance Consideration:</strong> Only set {@code rowIsImmutable} to true if you
     * are certain the row key bytes will not be modified, otherwise data corruption may occur.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Safe to mark as immutable since we won't modify the byte array
     * AnyPut immutablePut = AnyPut.of("user123", true)
     *                             .addColumn("info", "status", "active");
     * }</pre>
     *
     * @param rowKey the row key for the put operation, automatically converted to bytes; should not be {@code null}
     * @param rowIsImmutable true if the row key byte array is guaranteed to be immutable
     * @return a new AnyPut instance with immutability control
     * @see #of(Object, long, boolean)
     * @see #of(Object)
     */
    public static AnyPut of(final Object rowKey, final boolean rowIsImmutable) {
        return new AnyPut(rowKey, rowIsImmutable);
    }

    /**
     * Creates a new AnyPut with a default cell timestamp and a row-immutability hint.
     *
     * <p>Combines the default-timestamp behaviour of {@link #of(Object, long)} with the
     * row-key immutability optimisation of {@link #of(Object, boolean)}. Use this when you need
     * a specific default timestamp <i>and</i> can guarantee the row-key bytes won't be modified
     * by the caller after construction.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long importTime = System.currentTimeMillis();
     * AnyPut optimizedPut = AnyPut.of("user123", importTime, true)
     *                             .addColumn("import", "source", "legacy_system");
     * }</pre>
     *
     * @param rowKey the row key for the put operation, automatically converted to bytes
     * @param timestamp the default timestamp for cells added without an explicit timestamp
     *                  (milliseconds since epoch)
     * @param rowIsImmutable {@code true} if the row-key byte array is guaranteed not to be
     *                       modified after this Put is created
     * @return a new AnyPut with timestamp and immutability control
     * @throws IllegalArgumentException if {@code timestamp} is negative (validated by the
     *         underlying {@link Put} constructor)
     * @see #of(Object, boolean)
     * @see #of(Object, long)
     */
    public static AnyPut of(final Object rowKey, final long timestamp, final boolean rowIsImmutable) {
        return new AnyPut(rowKey, timestamp, rowIsImmutable);
    }

    /**
     * Creates a new AnyPut instance for the specified ByteBuffer row key.
     *
     * <p>This factory method creates a put operation using a ByteBuffer as the row key, which is
     * useful for NIO-based operations or when the row key is already in ByteBuffer format. The
     * ByteBuffer's current position and limit determine which bytes are used for the row key.
     * Use this variant when working with direct buffers or off-heap memory.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ByteBuffer keyBuffer = ByteBuffer.wrap("user123".getBytes());
     * AnyPut bufferPut = AnyPut.of(keyBuffer)
     *                          .addColumn("data", "value", "sample");
     * }</pre>
     *
     * @param rowKey the row key as a ByteBuffer; must not be null
     * @return a new AnyPut instance configured for the ByteBuffer row key
     * @throws NullPointerException if {@code rowKey} is null (raised by the wrapped {@link Put#Put(ByteBuffer)} constructor)
     * @see #of(ByteBuffer, long)
     * @see #of(Object)
     */
    public static AnyPut of(final ByteBuffer rowKey) {
        return new AnyPut(rowKey);
    }

    /**
     * Creates a new AnyPut instance for the specified ByteBuffer row key with timestamp control.
     *
     * <p>This factory method creates a put operation using a ByteBuffer as the row key with a
     * specific timestamp for all cells. Combines the benefits of NIO buffer operations with
     * temporal version control. Useful for high-performance scenarios involving direct buffers
     * and time-series data.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ByteBuffer keyBuffer = ByteBuffer.allocateDirect(16);
     * keyBuffer.put("user123".getBytes());
     * keyBuffer.flip();
     * long timestamp = System.currentTimeMillis();
     * AnyPut timestampedBufferPut = AnyPut.of(keyBuffer, timestamp)
     *                                     .addColumn("events", "action", "login");
     * }</pre>
     *
     * @param rowKey the row key as a ByteBuffer; must not be null
     * @param timestamp the timestamp for all cells in this put operation (milliseconds since epoch)
     * @return a new AnyPut instance with ByteBuffer row key and timestamp control
     * @throws NullPointerException if {@code rowKey} is null (raised by the wrapped {@link Put#Put(ByteBuffer, long)} constructor)
     * @throws IllegalArgumentException if {@code timestamp} is negative
     * @see #of(ByteBuffer)
     * @see #of(Object, long)
     */
    public static AnyPut of(final ByteBuffer rowKey, final long timestamp) {
        return new AnyPut(rowKey, timestamp);
    }

    /**
     * Creates a new AnyPut instance by copying an existing HBase Put operation.
     *
     * <p>This factory method creates a deep copy of the provided HBase Put object, preserving all
     * configuration, column data, and attributes from the original. This is useful when you want
     * to modify a copy without affecting the original put, or when converting existing HBase Put
     * objects to the AnyPut wrapper for additional functionality.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Put existingPut = new Put(Bytes.toBytes("user123"));
     * existingPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("John"));
     *
     * AnyPut copiedPut = AnyPut.of(existingPut)
     *                          .addColumn("info", "email", "john@example.com");
     * // existingPut remains unchanged
     * }</pre>
     *
     * @param putToCopy the existing HBase Put object to copy; must not be null
     * @return a new AnyPut instance that is a deep copy of the specified put
     * @throws NullPointerException if {@code putToCopy} is null (raised by the wrapped
     *         {@link Put#Put(Put)} constructor)
     * @see Put
     * @see #val()
     */
    public static AnyPut of(final Put putToCopy) {
        return new AnyPut(putToCopy);
    }

    /**
     * Creates a new AnyPut from a Java entity object using the default camelCase naming policy.
     *
     * <p>Performs automatic object-to-HBase mapping by introspecting the entity's properties and
     * converting them to HBase column families and qualifiers. The entity class must have a row-key
     * property — either annotated with {@code @Id} or registered via
     * {@link HBaseExecutor#registerRowKeyProperty(Class, String)} — whose value becomes the HBase
     * row key. Properties whose value is {@code null} are skipped. See the class-level "Entity
     * Mapping Rules" section for full details, including how nested beans, {@link HBaseColumn}
     * values, and {@code Collection}/{@code Map} of {@link HBaseColumn} are mapped.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * &#64;ColumnFamily("info")
     * public class User {
     *     &#64;Id
     *     private String userId;
     *     private String name;
     *     private String email;
     *     // getters and setters
     * }
     *
     * User user = new User("user123", "John Doe", "john@example.com");
     * AnyPut put = AnyPut.create(user);
     * }</pre>
     *
     * @param entity the Java object to convert to a put operation; must not be null and must
     *               have a row-key property
     * @return a new AnyPut populated from the entity
     * @throws IllegalArgumentException if {@code entity} is null or its class lacks a row-key
     *         property
     * @see #create(Object, NamingPolicy)
     * @see #create(Object, Collection)
     * @see ColumnFamily
     */
    public static AnyPut create(final Object entity) {
        return create(entity, NamingPolicy.CAMEL_CASE);
    }

    /**
     * Creates a new AnyPut instance from a Java entity object using the specified naming policy.
     *
     * <p>This factory method performs automatic object-to-HBase mapping with custom property name
     * conversion. The naming policy controls how Java property names are converted to HBase column
     * qualifiers (e.g., SNAKE_CASE converts "userName" to "user_name"). This enables flexible
     * integration with different naming conventions.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User user = new User("user123", "John Doe");
     * AnyPut snakeCasePut = AnyPut.create(user, NamingPolicy.SNAKE_CASE);
     * // Property "userName" becomes column "user_name"
     * }</pre>
     *
     * @param entity the Java object to convert to a put operation; must not be null
     * @param namingPolicy the naming policy for property-to-column name conversion; must not be null
     * @return a new AnyPut instance with data from the entity
     * @throws IllegalArgumentException if entity or namingPolicy is null, or entity lacks a row key property
     * @see #create(Object)
     * @see NamingPolicy
     */
    public static AnyPut create(final Object entity, final NamingPolicy namingPolicy) {
        N.checkArgNotNull(entity, "entity");
        N.checkArgNotNull(namingPolicy, "namingPolicy");

        final BeanInfo entityInfo = ParserUtil.getBeanInfo(entity.getClass());

        return create(entity, namingPolicy, entityInfo, entityInfo.propInfoList);
    }

    /**
     * Creates a list of AnyPut instances from a collection of Java entity objects.
     *
     * <p>This batch factory method converts multiple entities to put operations in a single call,
     * using the default camelCase naming policy. This is more efficient than calling
     * {@code create(Object)} repeatedly. Any AnyPut instances in the collection are passed through
     * unchanged, while other objects are converted using entity mapping.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List&lt;User&gt; users = Arrays.asList(user1, user2, user3);
     * List&lt;AnyPut&gt; puts = AnyPut.create(users);
     * executor.put("users_table", puts);
     * }</pre>
     *
     * @param entities the collection of Java objects to convert; must not be null
     * @return a list of AnyPut instances, one for each entity
     * @throws IllegalArgumentException if entities is null or any entity lacks a row key property
     * @see #create(Collection, NamingPolicy)
     * @see #create(Object)
     */
    public static List<AnyPut> create(final Collection<?> entities) {
        N.checkArgNotNull(entities, "entities");

        final List<AnyPut> anyPuts = new ArrayList<>(entities.size());

        for (final Object entity : entities) {
            anyPuts.add(entity instanceof AnyPut anyPut ? anyPut : create(entity));
        }

        return anyPuts;
    }

    /**
     * Creates a list of AnyPut instances from a collection of entities using the specified naming policy.
     *
     * <p>This batch factory method converts multiple entities to put operations with custom naming
     * policy. Useful for bulk operations where property names need to follow a specific convention
     * like SNAKE_CASE or UPPER_CAMEL_CASE.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List&lt;User&gt; users = loadUsersFromLegacySystem();
     * List&lt;AnyPut&gt; puts = AnyPut.create(users, NamingPolicy.SNAKE_CASE);
     * }</pre>
     *
     * @param entities the collection of Java objects to convert; must not be null
     * @param namingPolicy the naming policy for property-to-column name conversion; must not be null
     * @return a list of AnyPut instances, one for each entity
     * @throws IllegalArgumentException if entities or namingPolicy is null
     * @see #create(Collection)
     * @see NamingPolicy
     */
    public static List<AnyPut> create(final Collection<?> entities, final NamingPolicy namingPolicy) {
        N.checkArgNotNull(entities, "entities");
        N.checkArgNotNull(namingPolicy, "namingPolicy");

        final List<AnyPut> anyPuts = new ArrayList<>(entities.size());

        for (final Object entity : entities) {
            anyPuts.add(entity instanceof AnyPut anyPut ? anyPut : create(entity, namingPolicy));
        }

        return anyPuts;
    }

    /**
     * Creates a new AnyPut instance from selected properties of a Java entity object.
     *
     * <p>This selective factory method creates a put operation containing only the specified properties
     * from the entity. This is useful when you want to update only certain columns without affecting
     * others, or when implementing partial updates. Uses the default camelCase naming policy.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User user = loadUser("user123");
     * user.setEmail("newemail@example.com");
     * user.setLastLogin(new Date());
     *
     * // Update only email and lastLogin, leave other properties unchanged
     * Set&lt;String&gt; propsToUpdate = Set.of("email", "lastLogin");
     * AnyPut partialPut = AnyPut.create(user, propsToUpdate);
     * }</pre>
     *
     * @param entity the Java object to convert; must not be null
     * @param selectPropNames the property names to include in the put; if null, all properties are included
     * @return a new AnyPut instance with only the selected properties
     * @throws IllegalArgumentException if entity is null, entity lacks a row key, or a property name is invalid
     * @see #create(Object, Collection, NamingPolicy)
     * @see #create(Object)
     */
    public static AnyPut create(final Object entity, final Collection<String> selectPropNames) {
        return create(entity, selectPropNames, NamingPolicy.CAMEL_CASE);
    }

    /**
     * Creates a new AnyPut instance from selected properties of an entity using the specified naming policy.
     *
     * <p>This selective factory method combines property selection with custom naming policy, providing
     * maximum flexibility for partial updates with specific naming conventions. Only the specified
     * properties will be included in the put operation.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User user = loadUser("user123");
     * user.setUserStatus("active");
     *
     * Set&lt;String&gt; updateProps = Set.of("userStatus");
     * AnyPut put = AnyPut.create(user, updateProps, NamingPolicy.SNAKE_CASE);
     * // Creates put with column "user_status"
     * }</pre>
     *
     * @param entity the Java object to convert; must not be null
     * @param selectPropNames the property names to include; if null, all properties are included
     * @param namingPolicy the naming policy for property-to-column name conversion; must not be null
     * @return a new AnyPut instance with only the selected properties
     * @throws IllegalArgumentException if entity or namingPolicy is null, or property names are invalid
     * @see #create(Object, Collection)
     * @see NamingPolicy
     */
    public static AnyPut create(final Object entity, final Collection<String> selectPropNames, final NamingPolicy namingPolicy) {
        N.checkArgNotNull(entity, "entity");
        N.checkArgNotNull(namingPolicy, "namingPolicy");

        if (selectPropNames == null) {
            return create(entity, namingPolicy);
        }

        final BeanInfo entityInfo = ParserUtil.getBeanInfo(entity.getClass());

        return create(entity, namingPolicy, entityInfo, N.map(selectPropNames, propName -> N.checkArgNotNull(entityInfo.getPropInfo(propName))));
    }

    /**
     * Creates a list of AnyPut instances from selected properties of multiple entities.
     *
     * <p>This batch selective factory method creates put operations for multiple entities, including
     * only the specified properties from each. This is efficient for partial bulk updates where you
     * want to update the same set of properties across many entities.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List&lt;User&gt; users = loadActiveUsers();
     * users.forEach(u -> u.setLastChecked(new Date()));
     *
     * Set&lt;String&gt; updateProps = Set.of("lastChecked");
     * List&lt;AnyPut&gt; puts = AnyPut.create(users, updateProps);
     * executor.put("users_table", puts);
     * }</pre>
     *
     * @param entities the collection of Java objects to convert; must not be null
     * @param selectPropNames the property names to include from each entity; if null, all properties are included
     * @return a list of AnyPut instances with only the selected properties from each entity
     * @throws IllegalArgumentException if entities is null or property names are invalid
     * @see #create(Collection, Collection, NamingPolicy)
     * @see #create(Object, Collection)
     */
    public static List<AnyPut> create(final Collection<?> entities, final Collection<String> selectPropNames) {
        N.checkArgNotNull(entities, "entities");

        final List<AnyPut> anyPuts = new ArrayList<>(entities.size());

        for (final Object entity : entities) {
            anyPuts.add(entity instanceof AnyPut anyPut ? anyPut : create(entity, selectPropNames));
        }

        return anyPuts;
    }

    /**
     * Creates a list of AnyPut instances from selected properties of multiple entities with custom naming policy.
     *
     * <p>This comprehensive batch factory method combines selective property inclusion, multiple entities,
     * and custom naming policy, providing maximum flexibility for bulk partial updates with specific
     * naming conventions.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List&lt;User&gt; users = importFromLegacySystem();
     * users.forEach(u -> u.setMigrationStatus("completed"));
     *
     * Set&lt;String&gt; props = Set.of("migrationStatus", "migrationDate");
     * List&lt;AnyPut&gt; puts = AnyPut.create(users, props, NamingPolicy.SNAKE_CASE);
     * }</pre>
     *
     * @param entities the collection of Java objects to convert; must not be null
     * @param selectPropNames the property names to include from each entity; if null, all properties included
     * @param namingPolicy the naming policy for property-to-column name conversion; must not be null
     * @return a list of AnyPut instances with selected properties from each entity
     * @throws IllegalArgumentException if entities or namingPolicy is null
     * @see #create(Collection, Collection)
     * @see NamingPolicy
     */
    public static List<AnyPut> create(final Collection<?> entities, final Collection<String> selectPropNames, final NamingPolicy namingPolicy) {
        N.checkArgNotNull(entities, "entities");
        N.checkArgNotNull(namingPolicy, "namingPolicy");

        final List<AnyPut> anyPuts = new ArrayList<>(entities.size());

        for (final Object entity : entities) {
            anyPuts.add(entity instanceof AnyPut anyPut ? anyPut : create(entity, selectPropNames, namingPolicy));
        }

        return anyPuts;
    }

    private static AnyPut create(final Object entity, final NamingPolicy namingPolicy, final BeanInfo entityInfo, final Collection<PropInfo> selectPropInfos) {
        final Class<?> cls = entity.getClass();

        HBaseExecutor.checkEntityClass(cls);

        final Map<String, Tuple3<String, String, Boolean>> classFamilyColumnNameMap = HBaseExecutor.getClassFamilyColumnNameMap(cls, namingPolicy);
        final Method rowKeySetMethod = HBaseExecutor.getRowKeySetMethod(cls);
        final Method rowKeyGetMethod = rowKeySetMethod == null ? null : Beans.getPropGetter(cls, Beans.getPropNameByMethod(rowKeySetMethod));

        if (rowKeySetMethod == null) {
            throw new IllegalArgumentException(
                    "Row key property is required to create AnyPut instance. But no row key property found in class: " + ClassUtil.getCanonicalClassName(cls));
        }

        final AnyPut anyPut = new AnyPut(Beans.<Object> getPropValue(entity, rowKeyGetMethod));
        final boolean annotatedByDefaultColumnFamily = entityInfo.isAnnotationPresent(ColumnFamily.class);

        PropInfo columnPropInfo = null;
        Collection<HBaseColumn<?>> columnColl = null;
        Map<Long, HBaseColumn<?>> columnMap = null;
        HBaseColumn<?> column = null;
        Object propValue = null;
        Tuple3<String, String, Boolean> tp = null;
        String columnName = null;

        for (final PropInfo propInfo : selectPropInfos) {
            if (propInfo.getMethod.equals(rowKeyGetMethod)) {
                continue;
            }

            propValue = propInfo.getPropValue(entity);

            if (propValue == null) {
                continue;
            }

            tp = classFamilyColumnNameMap.get(propInfo.name);
            columnName = tp._3 || annotatedByDefaultColumnFamily || propInfo.isAnnotationPresent(ColumnFamily.class) ? tp._2 : HBaseExecutor.EMPTY_QUALIFIER; //NOSONAR

            if (propInfo.jsonXmlType.isBean() && !tp._3) { //NOSONAR
                final Map<String, Tuple3<String, String, Boolean>> propEntityFamilyColumnNameMap = HBaseExecutor.getClassFamilyColumnNameMap(propInfo.clazz,
                        namingPolicy);
                final Class<?> propEntityClass = propInfo.jsonXmlType.javaType();
                final BeanInfo propBeanInfo = ParserUtil.getBeanInfo(propEntityClass);
                final Object propEntity = propValue;
                Tuple3<String, String, Boolean> propEntityTP = null;

                final Map<String, Method> columnGetMethodMap = Beans.getPropGetters(propEntityClass);

                for (final Map.Entry<String, Method> columnGetMethodEntry : columnGetMethodMap.entrySet()) {
                    columnPropInfo = propBeanInfo.getPropInfo(columnGetMethodEntry.getKey());

                    propValue = columnPropInfo.getPropValue(propEntity);

                    if (propValue == null) {
                        continue;
                    }

                    propEntityTP = propEntityFamilyColumnNameMap.get(columnPropInfo.name);

                    if (columnPropInfo.jsonXmlType.isMap() && columnPropInfo.jsonXmlType.parameterTypes().get(1).javaType().equals(HBaseColumn.class)) {
                        columnMap = (Map<Long, HBaseColumn<?>>) propValue;

                        for (final HBaseColumn<?> e : columnMap.values()) {
                            anyPut.addColumn(tp._1, propEntityTP._2, e.version(), e.value());

                        }
                    } else if (columnPropInfo.jsonXmlType.isCollection()
                            && columnPropInfo.jsonXmlType.parameterTypes().get(0).javaType().equals(HBaseColumn.class)) {
                        columnColl = (Collection<HBaseColumn<?>>) propValue;

                        for (final HBaseColumn<?> e : columnColl) {
                            anyPut.addColumn(tp._1, propEntityTP._2, e.version(), e.value());

                        }
                    } else if (columnPropInfo.jsonXmlType.javaType().equals(HBaseColumn.class)) {
                        column = (HBaseColumn<?>) propValue;
                        anyPut.addColumn(tp._1, propEntityTP._2, column.version(), column.value());
                    } else {
                        anyPut.addColumn(tp._1, propEntityTP._2, propValue);
                    }
                }
            } else if (propInfo.jsonXmlType.isMap() && propInfo.jsonXmlType.parameterTypes().get(1).javaType().equals(HBaseColumn.class)) {
                columnMap = (Map<Long, HBaseColumn<?>>) propValue;

                for (final HBaseColumn<?> e : columnMap.values()) {
                    anyPut.addColumn(tp._1, columnName, e.version(), e.value());

                }
            } else if (propInfo.jsonXmlType.isCollection() && propInfo.jsonXmlType.parameterTypes().get(0).javaType().equals(HBaseColumn.class)) {
                columnColl = (Collection<HBaseColumn<?>>) propValue;

                for (final HBaseColumn<?> e : columnColl) {
                    anyPut.addColumn(tp._1, columnName, e.version(), e.value());

                }
            } else if (propInfo.jsonXmlType.javaType().equals(HBaseColumn.class)) {
                column = (HBaseColumn<?>) propValue;
                anyPut.addColumn(tp._1, columnName, column.version(), column.value());
            } else {
                anyPut.addColumn(tp._1, columnName, propValue);
            }
        }

        return anyPut;
    }

    /**
     * Returns the underlying HBase Put object for direct access to native HBase operations.
     *
     * <p>This method provides access to the wrapped HBase Put instance, allowing for advanced
     * operations not directly exposed by the AnyPut fluent API. Use this method when you need
     * to access HBase-specific functionality or when integrating with existing HBase code that
     * expects the native Put type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyPut anyPut = AnyPut.of("user123").addColumn("info", "name", "John");
     * Put hbasePut = anyPut.val();
     * table.put(hbasePut);   // Use with native HBase API
     * }</pre>
     *
     * @return the underlying HBase Put object; never null
     * @see Put
     * @see #of(Put)
     */
    public Put val() {
        return put;
    }

    /**
     * Adds a single cell (column value) to this put using string identifiers.
     *
     * <p>The family, qualifier, and value are converted from their Java types to byte arrays using
     * {@link HBaseExecutor}'s standard conversion mechanisms. The value can be any supported Java
     * type — {@link String}, {@link Integer}, {@link Long}, {@link java.util.Date}, byte arrays, etc.</p>
     *
     * <p>This variant uses the timestamp the {@link Put} was constructed with (or
     * {@code HConstants.LATEST_TIMESTAMP} when none was supplied, in which case the region server
     * stamps the cell with its current wall-clock time when the mutation is applied). The same
     * cell key is therefore reused on repeated calls within the same Put, which means a subsequent
     * call with an identical {@code (family, qualifier)} pair <i>overwrites</i> the earlier cell
     * rather than appending to it or creating a new version. To create multiple versions of the
     * same column in one Put, use {@link #addColumn(String, String, long, Object)} with distinct
     * timestamps.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyPut put = AnyPut.of("user123")
     *     .addColumn("info", "name", "John Doe")      // String value
     *     .addColumn("info", "age", 30)               // Integer value
     *     .addColumn("stats", "balance", 1000.50)     // Double value
     *     .addColumn("meta", "created", new Date());  // Date value
     * }</pre>
     *
     * @param family the column family name; must not be null or empty
     * @param qualifier the column qualifier name; must not be null
     * @param value the value to store; automatically converted to bytes ({@code null} is permitted
     *              and stored as an empty value)
     * @return this {@code AnyPut} instance, to allow fluent method chaining
     * @throws IllegalArgumentException if {@code family} is null or empty (validated by the
     *         underlying {@link Put})
     * @see #addColumn(String, String, long, Object)
     * @see #addColumn(byte[], byte[], byte[])
     */
    public AnyPut addColumn(final String family, final String qualifier, final Object value) {
        put.addColumn(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier), toValueBytes(value));

        return this;
    }

    /**
     * Adds a single cell (column value) with an explicit timestamp to this put.
     *
     * <p>The explicit timestamp becomes part of the cell key, so this variant can be used to write
     * multiple versions of the same {@code (family, qualifier)} pair in a single Put by calling it
     * with distinct timestamps. If another call adds a cell with the same
     * {@code (family, qualifier, ts)} tuple, the later call overwrites the earlier one.</p>
     *
     * <p>Use this method to insert data with a specific point-in-time version — for example, when
     * backdating data or implementing event-sourcing patterns.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long eventTime = 1609459200000L;  // Jan 1, 2021 00:00:00
     * AnyPut put = AnyPut.of("user123")
     *     .addColumn("events", "login", eventTime, "NYC")
     *     .addColumn("events", "action", eventTime, "view_dashboard");
     * }</pre>
     *
     * @param family the column family name; must not be null or empty
     * @param qualifier the column qualifier name; must not be null
     * @param ts the timestamp for this cell (milliseconds since epoch); must be non-negative
     * @param value the value to store; automatically converted to bytes ({@code null} is permitted)
     * @return this {@code AnyPut} instance, to allow fluent method chaining
     * @throws IllegalArgumentException if {@code family} is null or empty, or {@code ts} is
     *         negative (validated by the underlying {@link Put})
     * @see #addColumn(String, String, Object)
     * @see #of(Object, long)
     */
    public AnyPut addColumn(final String family, final String qualifier, final long ts, final Object value) {
        put.addColumn(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier), ts, toValueBytes(value));

        return this;
    }

    /**
     * Adds a single cell (column value) to this put using raw byte-array identifiers.
     *
     * <p>This method provides direct byte-array access for maximum performance when callers
     * already have pre-converted byte arrays, avoiding the overhead of string-to-bytes conversion.
     * Useful in performance-critical paths or when working with pre-encoded column identifiers and
     * values.</p>
     *
     * <p>This variant uses the timestamp the Put was constructed with (or
     * {@code HConstants.LATEST_TIMESTAMP} if none was supplied), so successive calls with the same
     * {@code (family, qualifier)} pair overwrite the earlier cell rather than creating a new
     * version. Use {@link #addColumn(byte[], byte[], long, byte[])} with distinct timestamps to
     * write multiple versions in a single Put.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * byte[] family = Bytes.toBytes("info");
     * byte[] qualifier = Bytes.toBytes("name");
     * byte[] value = Bytes.toBytes("John Doe");
     * AnyPut put = AnyPut.of("user123").addColumn(family, qualifier, value);
     * }</pre>
     *
     * @param family the column family name as a byte array; must not be null or empty
     * @param qualifier the column qualifier as a byte array; may be {@code null} to denote an
     *                  empty qualifier
     * @param value the value as a byte array; may be {@code null}
     * @return this {@code AnyPut} instance, to allow fluent method chaining
     * @throws IllegalArgumentException if {@code family} is null or empty (validated by the
     *         underlying {@link Put})
     * @see #addColumn(String, String, Object)
     * @see #addColumn(byte[], byte[], long, byte[])
     */
    public AnyPut addColumn(final byte[] family, final byte[] qualifier, final byte[] value) {
        put.addColumn(family, qualifier, value);

        return this;
    }

    /**
     * Adds a single cell with explicit timestamp to this put using raw byte-array identifiers.
     *
     * <p>The explicit timestamp becomes part of the cell key, so this variant can write multiple
     * versions of the same {@code (family, qualifier)} pair in one Put by calling it with
     * distinct timestamps. If another call adds a cell with the same
     * {@code (family, qualifier, ts)} tuple, the later call overwrites the earlier one. Ideal for
     * high-throughput, time-versioned data ingestion with pre-encoded identifiers.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * byte[] family = Bytes.toBytes("events");
     * byte[] qualifier = Bytes.toBytes("action");
     * byte[] value = Bytes.toBytes("login");
     * long timestamp = System.currentTimeMillis();
     * AnyPut put = AnyPut.of("user123").addColumn(family, qualifier, timestamp, value);
     * }</pre>
     *
     * @param family the column family name as a byte array; must not be null or empty
     * @param qualifier the column qualifier as a byte array; may be {@code null}
     * @param ts the timestamp for this cell (milliseconds since epoch); must be non-negative
     * @param value the value as a byte array; may be {@code null}
     * @return this {@code AnyPut} instance, to allow fluent method chaining
     * @throws IllegalArgumentException if {@code family} is null or empty, or {@code ts} is
     *         negative (validated by the underlying {@link Put})
     * @see #addColumn(byte[], byte[], byte[])
     * @see #addColumn(String, String, long, Object)
     */
    public AnyPut addColumn(final byte[] family, final byte[] qualifier, final long ts, final byte[] value) {
        put.addColumn(family, qualifier, ts, value);

        return this;
    }

    /**
     * Adds a single cell with explicit timestamp to this put using {@link ByteBuffer} qualifier
     * and value.
     *
     * <p>Provides NIO {@link ByteBuffer} support for scenarios involving direct buffers or
     * off-heap memory, minimising memory copies and GC pressure. The bytes between each buffer's
     * current position and limit are used.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * byte[] familyBytes = Bytes.toBytes("data");
     * ByteBuffer qualifier = ByteBuffer.wrap(Bytes.toBytes("value"));
     * ByteBuffer value = ByteBuffer.allocateDirect(1024);
     * value.put(someData);
     * value.flip();
     * long timestamp = System.currentTimeMillis();
     *
     * AnyPut put = AnyPut.of("user123").addColumn(familyBytes, qualifier, timestamp, value);
     * }</pre>
     *
     * @param family the column family name as a byte array; must not be null or empty
     * @param qualifier the column qualifier as a ByteBuffer; must not be null
     * @param ts the timestamp for this cell (milliseconds since epoch); must be non-negative
     * @param value the value as a ByteBuffer; must not be null
     * @return this {@code AnyPut} instance, to allow fluent method chaining
     * @throws IllegalArgumentException if {@code family} is null/empty or {@code ts} is negative
     *         (validated by the underlying {@link Put})
     * @see #addColumn(byte[], byte[], long, byte[])
     * @see ByteBuffer
     */
    public AnyPut addColumn(final byte[] family, final ByteBuffer qualifier, final long ts, final ByteBuffer value) {
        put.addColumn(family, qualifier, ts, value);

        return this;
    }

    //    /**
    //     * See {@code addColumn(byte[], byte[], byte[])}. This version expects
    //     * that the underlying arrays won't change. It's intended
    //     * for usage internal HBase to and for advanced client applications.
    //     *
    //     * @param family
    //     * @param qualifier
    //     * @param value
    //     * @return
    //     * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
    //     *             Use {@code add(Cell)} and {@link org.apache.hadoop.hbase.CellBuilder} instead
    //     */
    //    @Deprecated
    //    public AnyPut addImmutable(String family, String qualifier, Object value) {
    //        put.addImmutable(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier), toValueBytes(value));
    //
    //        return this;
    //    }
    //
    //    /**
    //     * See {@code addColumn(byte[], byte[], long, byte[])}. This version expects
    //     * that the underlying arrays won't change. It's intended
    //     * for usage internal HBase to and for advanced client applications.
    //     *
    //     * @param family
    //     * @param qualifier
    //     * @param ts
    //     * @param value
    //     * @return
    //     * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
    //     *             Use {@code add(Cell)} and {@link org.apache.hadoop.hbase.CellBuilder} instead
    //     */
    //    @Deprecated
    //    public AnyPut addImmutable(String family, String qualifier, long ts, Object value) {
    //        put.addImmutable(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier), ts, toValueBytes(value));
    //
    //        return this;
    //    }
    //
    //    /**
    //     * See {@code addColumn(byte[], byte[], byte[])}. This version expects
    //     * that the underlying arrays won't change. It's intended
    //     * for usage internal HBase to and for advanced client applications.
    //     *
    //     * @param family
    //     * @param qualifier
    //     * @param value
    //     * @return
    //     * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
    //     *             Use {@code add(Cell)} and {@link org.apache.hadoop.hbase.CellBuilder} instead
    //     */
    //    @Deprecated
    //    public AnyPut addImmutable(byte[] family, byte[] qualifier, byte[] value) {
    //        put.addImmutable(family, qualifier, value);
    //
    //        return this;
    //    }
    //
    //    /**
    //     * See {@code addColumn(byte[], byte[], long, byte[])}. This version expects
    //     * that the underlying arrays won't change. It's intended
    //     * for usage internal HBase to and for advanced client applications.
    //     *
    //     * @param family
    //     * @param qualifier
    //     * @param ts
    //     * @param value
    //     * @return
    //     * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
    //     *             Use {@code add(Cell)} and {@link org.apache.hadoop.hbase.CellBuilder} instead
    //     */
    //    @Deprecated
    //    public AnyPut addImmutable(byte[] family, byte[] qualifier, long ts, byte[] value) {
    //        put.addImmutable(family, qualifier, ts, value);
    //
    //        return this;
    //    }
    //
    //    /**
    //     * See {@code addColumn(byte[], byte[], long, byte[])}. This version expects
    //     * that the underlying arrays won't change. It's intended
    //     * for usage internal HBase to and for advanced client applications.
    //     *
    //     * @param family
    //     * @param qualifier
    //     * @param ts
    //     * @param value
    //     * @return
    //     * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
    //     *             Use {@code add(Cell)} and {@link org.apache.hadoop.hbase.CellBuilder} instead
    //     */
    //    @Deprecated
    //    public AnyPut addImmutable(byte[] family, ByteBuffer qualifier, long ts, ByteBuffer value) {
    //        put.addImmutable(family, qualifier, ts, value);
    //
    //        return this;
    //    }

    /**
     * Adds a pre-constructed {@link Cell} to this put.
     *
     * <p>This advanced method allows adding a fully constructed HBase {@link Cell} directly to the
     * Put, providing precise control over all cell attributes including timestamp, type, tags, and
     * sequence id. Use this when you need exact control over cell construction or when handing
     * cells received from another operation (for example, a scanner's {@code CellScanner}) into
     * this Put.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cell cell = CellUtil.createCell(
     *     Bytes.toBytes("user123"),      // row
     *     Bytes.toBytes("info"),         // family
     *     Bytes.toBytes("name"),         // qualifier
     *     System.currentTimeMillis(),    // timestamp
     *     Cell.Type.Put,                 // type
     *     Bytes.toBytes("John Doe")      // value
     * );
     *
     * AnyPut put = AnyPut.of("user123").add(cell);
     * }</pre>
     *
     * @param kv the Cell to add; must not be null and must have the same row key as this put
     * @return this {@code AnyPut} instance, to allow fluent method chaining
     * @throws IOException if the cell's row key does not match this Put's row key (thrown by the
     *         underlying {@link Put#add(Cell)})
     * @see Cell
     * @see Put#add(Cell)
     * @see #addColumn(String, String, Object)
     */
    public AnyPut add(final Cell kv) throws IOException {
        put.add(kv);

        return this;
    }

    /**
     * Returns the hash code value for this AnyPut instance.
     *
     * <p>The hash code is based on the underlying HBase Put object and is consistent
     * with the {@link #equals(Object)} method. Two AnyPut instances with equivalent
     * Put operations will have the same hash code.</p>
     *
     * @return the hash code value for this AnyPut
     * @see #equals(Object)
     */
    @Override
    public int hashCode() {
        return put.hashCode();
    }

    /**
     * Compares this AnyPut instance with another object for equality.
     *
     * <p>Two AnyPut instances are considered equal if they wrap equivalent HBase Put
     * operations. This comparison is based on the underlying Put object's equality,
     * which considers row key, column specifications, timestamps, and all cell data.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyPut put1 = AnyPut.of("user123").addColumn("info", "name", "John");
     * AnyPut put2 = AnyPut.of("user123").addColumn("info", "name", "John");
     * boolean equal = put1.equals(put2);   // returns true
     * }</pre>
     *
     * @param obj the object to compare with
     * @return {@code true} if the specified object represents an equivalent put operation, {@code false} otherwise
     * @see #hashCode()
     */
    @SuppressFBWarnings
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof final AnyPut other) {
            return put.equals(other.put);
        }

        return false;
    }

    /**
     * Returns a string representation of this AnyPut instance.
     *
     * <p>The string representation is delegated to the underlying HBase Put object
     * and includes information about the row key, column families, qualifiers, timestamps,
     * and values contained in this put operation.</p>
     *
     * @return a string representation of the put operation
     */
    @Override
    public String toString() {
        return put.toString();
    }

    /**
     * Converts a collection of entities or AnyPut instances to native HBase Put objects.
     *
     * <p>This utility method converts a mixed collection of Java entity objects and AnyPut
     * instances to native HBase Put objects suitable for batch operations. Entity objects
     * are automatically converted using the default camelCase naming policy, while
     * AnyPut instances have their underlying Put objects extracted. This is useful for
     * batch put operations with the native HBase client API.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List&lt;Object&gt; mixed = new ArrayList&lt;&gt;();
     * mixed.add(new User("user1", "John"));                             // Entity
     * mixed.add(new User("user2", "Jane"));                             // Entity
     * mixed.add(AnyPut.of("user3").addColumn("info", "name", "Bob"));   // AnyPut
     *
     * List&lt;Put&gt; puts = AnyPut.toPut(mixed);
     * table.put(puts);   // Batch put with native HBase API
     * }</pre>
     *
     * @param entities the collection of entities and/or AnyPut instances to convert; must not be null and must not contain null elements
     * @return a list of native HBase Put objects, in iteration order of {@code entities}
     * @throws IllegalArgumentException if {@code entities} is null, an element is null, or an entity lacks the required row key property
     * @see #toPut(Collection, NamingPolicy)
     * @see #create(Collection)
     * @see Put
     */
    public static List<Put> toPut(final Collection<?> entities) {
        N.checkArgNotNull(entities, "entities");

        final List<Put> puts = new ArrayList<>(entities.size());

        for (final Object entity : entities) {
            puts.add(entity instanceof AnyPut anyPut ? anyPut.val() : AnyPut.create(entity).val());
        }

        return puts;
    }

    /**
     * Converts a collection of entities or AnyPut instances to native HBase Put objects with custom naming policy.
     *
     * <p>This utility method performs the same conversion as {@link #toPut(Collection)} but allows
     * specifying a custom naming policy for entity-to-column name conversion. This is useful when
     * working with entities that need specific naming conventions like SNAKE_CASE or UPPER_CAMEL_CASE.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List&lt;User&gt; users = loadUsersFromLegacySystem();
     *
     * // Convert entities using snake_case naming
     * List&lt;Put&gt; puts = AnyPut.toPut(users, NamingPolicy.SNAKE_CASE);
     * table.put(puts);   // All column names will be in snake_case
     * }</pre>
     *
     * @param entities the collection of entities and/or AnyPut instances to convert; must not be null and must not contain null elements
     * @param namingPolicy the naming policy for property-to-column name conversion; must not be null
     * @return a list of native HBase Put objects, in iteration order of {@code entities}
     * @throws IllegalArgumentException if {@code entities} or {@code namingPolicy} is null, an element is null, or an entity lacks the required row key property
     * @see #toPut(Collection)
     * @see #create(Collection, NamingPolicy)
     * @see NamingPolicy
     */
    public static List<Put> toPut(final Collection<?> entities, final NamingPolicy namingPolicy) {
        N.checkArgNotNull(entities, "entities");
        N.checkArgNotNull(namingPolicy, "namingPolicy");

        final List<Put> puts = new ArrayList<>(entities.size());

        for (final Object entity : entities) {
            puts.add(entity instanceof AnyPut anyPut ? anyPut.val() : AnyPut.create(entity, namingPolicy).val());
        }

        return puts;
    }
}
