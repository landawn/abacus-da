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

import static com.landawn.abacus.da.hbase.HBaseExecutor.toRowBytes;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;

import com.landawn.abacus.annotation.SuppressFBWarnings;

/**
 * Wrapper around HBase's {@link RowMutations} for grouping multiple {@link Put} and {@link Delete}
 * operations targeting the same single row.
 *
 * <h2>Atomicity Guarantees</h2>
 * <p>When the wrapped {@link RowMutations} is submitted via an HBase API such as
 * {@code Table.mutateRow(RowMutations)}, the server applies every contained {@link Put} and
 * {@link Delete} <strong>atomically as a single row-level operation</strong>: either all the
 * mutations take effect together, or none of them do, and no other client can observe a partial
 * state. The atomicity scope is the row identified by the shared row key, and is enforced by
 * HBase only because every mutation in this batch shares that row key — HBase rejects any
 * mutation whose row key differs from the one this {@code AnyRowMutations} was created with.</p>
 *
 * <p>Unlike the other {@code Any*} types in this package, {@code AnyRowMutations} is a thin
 * adapter and does not extend {@link AnyOperation} / {@link AnyMutation}; it implements
 * {@link Row} so it can be passed directly to {@code Table#mutateRow(RowMutations)}-style APIs
 * via {@link #val()}.</p>
 *
 * <p>Only {@link Put} and {@link Delete} mutations are supported by the underlying HBase
 * {@link RowMutations}; other {@link Mutation} subtypes (such as {@code Increment} or
 * {@code Append}) should not be added.</p>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * AnyRowMutations mutations = AnyRowMutations.of("user123");
 * mutations.add(new Put(Bytes.toBytes("user123")).addColumn(family, qualifier, value));
 * mutations.add(new Delete(Bytes.toBytes("user123")).addColumn(family, qualifier));
 * }</pre>
 *
 * @see <a href="http://hbase.apache.org/devapidocs/index.html">Apache HBase Java API Documentation</a>
 * @see org.apache.hadoop.hbase.client.RowMutations
 * @see org.apache.hadoop.hbase.client.Put
 * @see org.apache.hadoop.hbase.client.Delete
 */
public final class AnyRowMutations implements Row {

    private final RowMutations rowMutations;

    /**
     * Package-private constructor: prefer {@link #of(Object)}. Creates a new
     * {@link RowMutations} for the given row key, which is converted to bytes via
     * {@link HBaseExecutor#toRowBytes(Object)}.
     *
     * @param rowKey the row key shared by all mutations in this batch
     */
    AnyRowMutations(final Object rowKey) {
        rowMutations = new RowMutations(toRowBytes(rowKey));
    }

    /**
     * Package-private constructor: prefer {@link #of(Object, int)}. Same as
     * {@link #AnyRowMutations(Object)} but pre-sizes the internal mutation list.
     *
     * @param rowKey the row key shared by all mutations in this batch
     * @param initialCapacity the initial capacity for the internal mutation list
     */
    AnyRowMutations(final Object rowKey, final int initialCapacity) {
        rowMutations = new RowMutations(toRowBytes(rowKey), initialCapacity);
    }

    /**
     * Package-private constructor: prefer {@link #of(byte[])}. Creates a new
     * {@link RowMutations} for the given byte-array row key.
     *
     * @param rowKey the row key shared by all mutations in this batch, as a byte array
     */
    AnyRowMutations(final byte[] rowKey) {
        rowMutations = new RowMutations(rowKey);
    }

    /**
     * Package-private constructor: prefer {@link #of(byte[], int)}. Same as
     * {@link #AnyRowMutations(byte[])} but pre-sizes the internal mutation list.
     *
     * @param rowKey the row key shared by all mutations in this batch, as a byte array
     * @param initialCapacity the initial capacity for the internal mutation list
     */
    AnyRowMutations(final byte[] rowKey, final int initialCapacity) {
        rowMutations = new RowMutations(rowKey, initialCapacity);
    }

    /**
     * Factory: creates a new {@code AnyRowMutations} for the given row key. The row key is
     * converted to bytes via {@link HBaseExecutor#toRowBytes(Object)}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyRowMutations mutations = AnyRowMutations.of("user123");
     * mutations.getRow();                  // returns Bytes.toBytes("user123")
     * mutations.val().getMutations();      // returns an empty list (no mutations yet)
     *
     * // A non-String key is stringified, then converted to UTF-8 bytes
     * AnyRowMutations numeric = AnyRowMutations.of(42);
     * numeric.getRow();                    // returns Bytes.toBytes("42")
     *
     * // Edge: a null row key is rejected by the underlying RowMutations
     * AnyRowMutations.of((Object) null);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param rowKey the row key shared by all mutations in this batch
     * @return a new {@code AnyRowMutations} targeting {@code rowKey}
     */
    public static AnyRowMutations of(final Object rowKey) {
        return new AnyRowMutations(rowKey);
    }

    /**
     * Factory: creates a new {@code AnyRowMutations} for the given row key with the supplied
     * initial capacity for the internal mutation list. Useful when the number of mutations is
     * known in advance to avoid list resizing.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyRowMutations mutations = AnyRowMutations.of("user123", 10);
     * mutations.getRow();                  // returns Bytes.toBytes("user123")
     * mutations.val().getMutations();      // returns an empty list (capacity is only a hint)
     *
     * // Edge: capacity is purely a sizing hint, so even a negative value is accepted
     * AnyRowMutations neg = AnyRowMutations.of("user123", -1);
     * neg.val().getMutations();            // returns an empty list
     *
     * // Edge: a null row key is rejected by the underlying RowMutations
     * AnyRowMutations.of((Object) null, 10); // throws IllegalArgumentException
     * }</pre>
     *
     * @param rowKey the row key shared by all mutations in this batch
     * @param initialCapacity the initial capacity for the internal mutation list
     * @return a new {@code AnyRowMutations} targeting {@code rowKey}
     */
    public static AnyRowMutations of(final Object rowKey, final int initialCapacity) {
        return new AnyRowMutations(rowKey, initialCapacity);
    }

    /**
     * Factory: creates a new {@code AnyRowMutations} for the given byte-array row key. Use this
     * overload when the row key is already in byte form to avoid conversion overhead.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * byte[] rowKey = Bytes.toBytes("user123");
     * AnyRowMutations mutations = AnyRowMutations.of(rowKey);
     * mutations.getRow();                  // returns the same bytes as rowKey
     * mutations.val().getMutations();      // returns an empty list (no mutations yet)
     *
     * // Edge: a null byte[] row key is rejected by the underlying RowMutations
     * AnyRowMutations.of((byte[]) null);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param rowKey the row key shared by all mutations in this batch, as a byte array
     * @return a new {@code AnyRowMutations} targeting {@code rowKey}
     */
    public static AnyRowMutations of(final byte[] rowKey) {
        return new AnyRowMutations(rowKey);
    }

    /**
     * Factory: creates a new {@code AnyRowMutations} for the given byte-array row key with the
     * supplied initial capacity for the internal mutation list.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * byte[] rowKey = Bytes.toBytes("user123");
     * AnyRowMutations mutations = AnyRowMutations.of(rowKey, 5);
     * mutations.getRow();                  // returns the same bytes as rowKey
     * mutations.val().getMutations();      // returns an empty list (capacity is only a hint)
     *
     * // Edge: capacity is purely a sizing hint, so even a negative value is accepted
     * AnyRowMutations neg = AnyRowMutations.of(rowKey, -1);
     * neg.val().getMutations();            // returns an empty list
     *
     * // Edge: a null byte[] row key is rejected by the underlying RowMutations
     * AnyRowMutations.of((byte[]) null, 5); // throws IllegalArgumentException
     * }</pre>
     *
     * @param rowKey the row key shared by all mutations in this batch, as a byte array
     * @param initialCapacity the initial capacity for the internal mutation list
     * @return a new {@code AnyRowMutations} targeting {@code rowKey}
     */
    public static AnyRowMutations of(final byte[] rowKey, final int initialCapacity) {
        return new AnyRowMutations(rowKey, initialCapacity);
    }

    /**
     * Returns the underlying HBase {@link RowMutations}. Pass it directly to HBase APIs (such as
     * {@code Table.mutateRow(RowMutations)}) that expect the native type.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyRowMutations mutations = AnyRowMutations.of("user123");
     * RowMutations rm = mutations.val();
     * rm.getRow();                         // returns Bytes.toBytes("user123")
     *
     * // The same live instance is returned on every call (no defensive copy)
     * mutations.val() == mutations.val();  // returns true
     *
     * // Pass the native type to an HBase API
     * // table.mutateRow(mutations.val()); // submits the batch atomically
     * }</pre>
     *
     * @return the wrapped {@link RowMutations}; never {@code null}
     */
    public RowMutations val() {
        return rowMutations;
    }

    /**
     * Appends a {@link Put} or {@link Delete} mutation to the batch. All mutations in the batch
     * must share the row key this {@code AnyRowMutations} was created with; HBase rejects a
     * mismatched row key with an {@link IOException}. When this batch is later submitted to the
     * server, the appended mutation participates in the row-level atomic apply described in the
     * {@linkplain AnyRowMutations class-level javadoc}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * byte[] family = Bytes.toBytes("cf");
     * byte[] qualifier = Bytes.toBytes("q");
     * byte[] value = Bytes.toBytes("v");
     * AnyRowMutations mutations = AnyRowMutations.of("user123");
     *
     * // Add a Put mutation; returns this for chaining
     * Put put = new Put(Bytes.toBytes("user123")).addColumn(family, qualifier, value);
     * mutations.add(put);                       // returns the same mutations instance
     * mutations.val().getMutations().size();    // returns 1
     *
     * // Add a Delete mutation
     * Delete delete = new Delete(Bytes.toBytes("user123")).addColumn(family, qualifier);
     * mutations.add(delete);                    // returns the same mutations instance
     * mutations.val().getMutations().size();    // returns 2
     *
     * // Edge: a mutation with a different row key is rejected
     * Put other = new Put(Bytes.toBytes("user999")).addColumn(family, qualifier, value);
     * mutations.add(other);                     // throws IOException
     * }</pre>
     *
     * @param mutation the {@link Put} or {@link Delete} to add
     * @return this AnyRowMutations instance, to allow fluent method chaining
     * @throws IOException if {@code mutation}'s row key does not match this batch's row key
     * @see Put
     * @see Delete
     * @see #add(List)
     */
    public AnyRowMutations add(final Mutation mutation) throws IOException {
        rowMutations.add(mutation);

        return this;
    }

    /**
     * Appends multiple {@link Put} / {@link Delete} mutations to the batch in a single call.
     * All mutations must share the row key this {@code AnyRowMutations} was created with. When
     * this batch is later submitted to the server, every appended mutation participates in the
     * row-level atomic apply described in the {@linkplain AnyRowMutations class-level javadoc}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * byte[] family = Bytes.toBytes("cf");
     * AnyRowMutations rowMutations = AnyRowMutations.of("user123");
     *
     * List<Mutation> mutations = new ArrayList<>();
     * mutations.add(new Put(Bytes.toBytes("user123"))
     *     .addColumn(family, Bytes.toBytes("name"), Bytes.toBytes("John")));
     * mutations.add(new Put(Bytes.toBytes("user123"))
     *     .addColumn(family, Bytes.toBytes("age"), Bytes.toBytes(30)));
     * mutations.add(new Delete(Bytes.toBytes("user123"))
     *     .addColumn(family, Bytes.toBytes("oldColumn")));
     *
     * rowMutations.add(mutations);                 // returns the same rowMutations instance
     * rowMutations.val().getMutations().size();    // returns 3
     *
     * // Edge: an empty list is a no-op; the batch is unchanged
     * AnyRowMutations empty = AnyRowMutations.of("user123");
     * empty.add(new ArrayList<Mutation>());        // returns the same empty instance
     * empty.val().getMutations().size();           // returns 0
     *
     * // Edge: a list containing a mismatched row key is rejected
     * AnyRowMutations m = AnyRowMutations.of("user123");
     * List<Mutation> bad = new ArrayList<>();
     * bad.add(new Put(Bytes.toBytes("user999")).addColumn(family, Bytes.toBytes("q"), Bytes.toBytes("v")));
     * m.add(bad);                                  // throws IOException
     * }</pre>
     *
     * @param mutations the list of {@link Put} / {@link Delete} mutations to add
     * @return this AnyRowMutations instance, to allow fluent method chaining
     * @throws IOException if any mutation's row key does not match this batch's row key
     * @see Put
     * @see Delete
     * @see #add(Mutation)
     */
    public AnyRowMutations add(final List<? extends Mutation> mutations) throws IOException {
        rowMutations.add(mutations);

        return this;
    }

    /**
     * Returns the row key shared by every mutation in this batch. Implementation of
     * {@link Row#getRow()}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyRowMutations mutations = AnyRowMutations.of("the-row-key");
     * mutations.getRow();                  // returns Bytes.toBytes("the-row-key")
     *
     * // The key is set once at construction and is independent of any added mutations
     * AnyRowMutations fromBytes = AnyRowMutations.of(Bytes.toBytes("row-1"));
     * fromBytes.getRow();                  // returns Bytes.toBytes("row-1")
     * }</pre>
     *
     * @return the row key as a byte array; never {@code null}
     */
    @Override
    public byte[] getRow() {
        return rowMutations.getRow();
    }

    /**
     * Compares this batch with another {@link Row} by row key, using HBase's standard
     * lexicographic byte-array comparison. Implementation of {@link Row#compareTo(Row)}; the
     * underlying HBase method is itself deprecated in favour of {@link Row#COMPARATOR}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyRowMutations a = AnyRowMutations.of("a");
     * AnyRowMutations b = AnyRowMutations.of("b");
     *
     * a.compareTo(b);                      // returns a negative int ("a" < "b")
     * b.compareTo(a);                      // returns a positive int ("b" > "a")
     *
     * // Edge: equal row keys compare as zero (mutation contents are not considered)
     * AnyRowMutations a2 = AnyRowMutations.of("a");
     * a.compareTo(a2);                     // returns 0
     * }</pre>
     *
     * @param other the {@link Row} to compare with
     * @return a negative integer, zero, or a positive integer as this row key is less than,
     *         equal to, or greater than {@code other}'s key
     */
    @Override
    @SuppressWarnings("deprecation")
    public int compareTo(final Row other) {
        return rowMutations.compareTo(other);
    }

    /**
     * Returns a hash code value for this AnyRowMutations.
     * <p>
     * The hash code is based on the underlying RowMutations object,
     * ensuring consistency with the equals method.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyRowMutations mutations = AnyRowMutations.of("user123");
     * mutations.hashCode();                        // returns mutations.val().hashCode()
     *
     * // Delegates to the wrapped RowMutations, so the values match exactly
     * mutations.hashCode() == mutations.val().hashCode(); // returns true
     * }</pre>
     *
     * @return a hash code value for this object
     */
    @Override
    @SuppressWarnings("deprecation")
    public int hashCode() {
        return rowMutations.hashCode();
    }

    /**
     * Indicates whether some other object is "equal to" this AnyRowMutations.
     * <p>
     * Two AnyRowMutations objects are considered equal if their underlying
     * {@link RowMutations} objects are equal. HBase's {@link RowMutations#equals(Object)} compares
     * the row key only — the contained mutations are <em>not</em> considered — so two batches on
     * the same row are equal even when they hold different mutations.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyRowMutations a = AnyRowMutations.of("user123");
     * AnyRowMutations b = AnyRowMutations.of("user123");
     *
     * a.equals(a);                         // returns true (same instance)
     * a.equals(b);                         // returns a.val().equals(b.val())
     *
     * // Edge: non-AnyRowMutations objects (including null) are never equal
     * a.equals("user123");                 // returns false
     * a.equals(null);                      // returns false
     * }</pre>
     *
     * @param obj the reference object with which to compare
     * @return {@code true} if this object is the same as the obj argument;
     *         {@code false} otherwise
     */
    @SuppressFBWarnings
    @SuppressWarnings("deprecation")
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof final AnyRowMutations other) {
            return rowMutations.equals(other.rowMutations);
        }

        return false;
    }

    /**
     * Returns a string representation of this AnyRowMutations.
     * <p>
     * Delegates to the wrapped {@code RowMutations}, which does not override
     * {@code Object.toString()}, so the result is the default
     * {@code className@hashCode} form and does not include the row key or
     * the mutations in this batch.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyRowMutations mutations = AnyRowMutations.of("user123");
     * mutations.toString();                        // returns a non-null debug string
     *
     * // Delegates to the wrapped RowMutations, so the strings match exactly
     * mutations.toString().equals(mutations.val().toString()); // returns true
     * }</pre>
     *
     * @return a string representation of this AnyRowMutations; never null
     */
    @Override
    public String toString() {
        return rowMutations.toString();
    }
}
