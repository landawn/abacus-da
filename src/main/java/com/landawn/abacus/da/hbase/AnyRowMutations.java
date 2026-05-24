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
 * operations against a single row so the server applies them atomically.
 *
 * <p>Unlike the other {@code Any*} types in this package, {@code AnyRowMutations} is a thin
 * adapter and does not extend {@link AnyOperation} / {@link AnyMutation}; it implements
 * {@link Row} so it can be passed directly to {@code Table#mutateRow(RowMutations)}-style APIs
 * via {@link #val()}.</p>
 *
 * <p>Only {@link Put} and {@link Delete} are accepted; other {@link Mutation} subtypes (such as
 * {@code Increment} or {@code Append}) are rejected by the underlying HBase API.</p>
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
     * @return the wrapped {@link RowMutations}; never {@code null}
     */
    public RowMutations val() {
        return rowMutations;
    }

    /**
     * Appends a {@link Put} or {@link Delete} mutation to the batch. All mutations in the batch
     * must share the row key this {@code AnyRowMutations} was created with; HBase rejects
     * mismatches with an {@link IOException}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyRowMutations mutations = AnyRowMutations.of("user123");
     *
     * // Add a Put mutation
     * Put put = new Put(Bytes.toBytes("user123"))
     *     .addColumn(family, qualifier, value);
     * mutations.add(put);
     *
     * // Add a Delete mutation
     * Delete delete = new Delete(Bytes.toBytes("user123"))
     *     .addColumn(family, oldQualifier);
     * mutations.add(delete);
     * }</pre>
     *
     * @param mutation the {@link Put} or {@link Delete} to add
     * @return this AnyRowMutations instance, to allow fluent method chaining
     * @throws IOException if {@code mutation}'s row key does not match this batch's row key, or
     *         if {@code mutation} is not a {@link Put} or {@link Delete}
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
     * All mutations must share the row key this {@code AnyRowMutations} was created with.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyRowMutations rowMutations = AnyRowMutations.of("user123");
     *
     * List<Mutation> mutations = new ArrayList<>();
     * mutations.add(new Put(Bytes.toBytes("user123"))
     *     .addColumn(family, "name", Bytes.toBytes("John")));
     * mutations.add(new Put(Bytes.toBytes("user123"))
     *     .addColumn(family, "age", Bytes.toBytes(30)));
     * mutations.add(new Delete(Bytes.toBytes("user123"))
     *     .addColumn(family, "oldColumn"));
     *
     * rowMutations.add(mutations);
     * }</pre>
     *
     * @param mutations the list of {@link Put} / {@link Delete} mutations to add
     * @return this AnyRowMutations instance, to allow fluent method chaining
     * @throws IOException if any mutation's row key does not match this batch's row key, or if
     *         the list contains an unsupported mutation type
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
     * @param row the {@link Row} to compare with
     * @return a negative integer, zero, or a positive integer as this row key is less than,
     *         equal to, or greater than {@code row}'s key
     */
    @Override
    @SuppressWarnings("deprecation")
    public int compareTo(final Row row) {
        return rowMutations.compareTo(row);
    }

    /**
     * Returns a hash code value for this AnyRowMutations.
     * <p>
     * The hash code is based on the underlying RowMutations object,
     * ensuring consistency with the equals method.
     * </p>
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
     * RowMutations objects are equal, which means they have the same row key
     * and the same set of mutations.
     * </p>
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
     * The string representation includes information about the row key
     * and all mutations in this batch, which is useful for debugging
     * and logging purposes.
     * </p>
     *
     * @return a string representation of this AnyRowMutations; never null
     */
    @Override
    public String toString() {
        return rowMutations.toString();
    }
}
