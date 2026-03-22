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
 * A convenient wrapper around HBase's {@code RowMutations} class that simplifies batch mutation operations
 * by providing automatic conversion between different data types and bytes.
 * <p>
 * This class allows you to group multiple mutations (Put and Delete operations) for the same row key
 * into a single atomic operation, ensuring all mutations succeed or fail together. It reduces the
 * complexity of working with byte arrays by accepting various data types as row keys.
 * </p>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * AnyRowMutations mutations = AnyRowMutations.of("user123");
 * mutations.add(new Put(Bytes.toBytes("user123")).addColumn(family, qualifier, value));
 * mutations.add(new Delete(Bytes.toBytes("user123")).addColumn(family, qualifier));
 * }</pre>
 *
 * <p>
 * Currently supports {@link Put} and {@link Delete} mutations only.
 * </p>
 *
 * @see <a href="http://hbase.apache.org/devapidocs/index.html">Apache HBase Java API Documentation</a>
 * @see org.apache.hadoop.hbase.client.RowMutations
 * @see org.apache.hadoop.hbase.client.Put
 * @see org.apache.hadoop.hbase.client.Delete
 */
public final class AnyRowMutations implements Row {

    private final RowMutations rowMutations;

    /**
     * Constructs a new AnyRowMutations instance for the specified row key.
     *
     * @param rowKey the row key for all mutations in this batch; will be converted to bytes automatically
     * @throws IllegalArgumentException if rowKey is null
     */
    AnyRowMutations(final Object rowKey) {
        rowMutations = new RowMutations(toRowBytes(rowKey));
    }

    /**
     * Constructs a new AnyRowMutations instance for the specified row key with an initial capacity.
     *
     * @param rowKey the row key for all mutations in this batch; will be converted to bytes automatically
     * @param initialCapacity the initial capacity for the list of mutations to avoid resizing
     * @throws IllegalArgumentException if rowKey is null or initialCapacity is negative
     */
    AnyRowMutations(final Object rowKey, final int initialCapacity) {
        rowMutations = new RowMutations(toRowBytes(rowKey), initialCapacity);
    }

    /**
     * Constructs a new AnyRowMutations instance for the specified byte array row key.
     *
     * @param rowKey the row key for all mutations in this batch as a byte array
     * @throws IllegalArgumentException if rowKey is null
     */
    AnyRowMutations(final byte[] rowKey) {
        rowMutations = new RowMutations(rowKey);
    }

    /**
     * Constructs a new AnyRowMutations instance for the specified byte array row key with an initial capacity.
     *
     * @param rowKey the row key for all mutations in this batch as a byte array
     * @param initialCapacity the initial capacity for the list of mutations to avoid resizing
     * @throws IllegalArgumentException if rowKey is null or initialCapacity is negative
     */
    AnyRowMutations(final byte[] rowKey, final int initialCapacity) {
        rowMutations = new RowMutations(rowKey, initialCapacity);
    }

    /**
     * Creates a new AnyRowMutations instance for the specified row key.
     * <p>
     * This is a factory method that provides a convenient way to create instances
     * without using the constructor directly.
     * </p>
     *
     * @param rowKey the row key for all mutations in this batch; will be converted to bytes automatically
     * @return a new AnyRowMutations instance for the specified row key
     * @throws IllegalArgumentException if rowKey is null
     */
    public static AnyRowMutations of(final Object rowKey) {
        return new AnyRowMutations(rowKey);
    }

    /**
     * Creates a new AnyRowMutations instance for the specified row key with an initial capacity.
     * <p>
     * This factory method is useful when you know approximately how many mutations
     * you'll be adding, as it can improve performance by avoiding list resizing.
     * </p>
     *
     * @param rowKey the row key for all mutations in this batch; will be converted to bytes automatically
     * @param initialCapacity the initial capacity for the list of mutations to avoid resizing
     * @return a new AnyRowMutations instance for the specified row key
     * @throws IllegalArgumentException if rowKey is null or initialCapacity is negative
     */
    public static AnyRowMutations of(final Object rowKey, final int initialCapacity) {
        return new AnyRowMutations(rowKey, initialCapacity);
    }

    /**
     * Creates a new AnyRowMutations instance for the specified byte array row key.
     * <p>
     * Use this method when you already have the row key as a byte array and want
     * to avoid additional conversion overhead.
     * </p>
     *
     * @param rowKey the row key for all mutations in this batch as a byte array
     * @return a new AnyRowMutations instance for the specified row key
     * @throws IllegalArgumentException if rowKey is null
     */
    public static AnyRowMutations of(final byte[] rowKey) {
        return new AnyRowMutations(rowKey);
    }

    /**
     * Creates a new AnyRowMutations instance for the specified byte array row key with an initial capacity.
     * <p>
     * This method combines the performance benefits of using pre-converted byte arrays
     * with capacity optimization for better performance.
     * </p>
     *
     * @param rowKey the row key for all mutations in this batch as a byte array
     * @param initialCapacity the initial capacity for the list of mutations to avoid resizing
     * @return a new AnyRowMutations instance for the specified row key
     * @throws IllegalArgumentException if rowKey is null or initialCapacity is negative
     */
    public static AnyRowMutations of(final byte[] rowKey, final int initialCapacity) {
        return new AnyRowMutations(rowKey, initialCapacity);
    }

    /**
     * Returns the underlying HBase RowMutations object.
     * <p>
     * This method provides access to the native HBase RowMutations instance,
     * which can be useful when you need to interact directly with HBase APIs
     * that expect the native type.
     * </p>
     *
     * @return the underlying RowMutations object; never null
     */
    public RowMutations val() {
        return rowMutations;
    }

    /**
     * Adds a single mutation to this batch of row mutations.
     * <p>
     * This method allows you to add Put or Delete operations to the mutation batch.
     * All mutations must target the same row key that was specified when creating this
     * AnyRowMutations instance, ensuring atomic execution across all mutations.
     * </p>
     *
     * <p><strong>Supported Mutation Types:</strong></p>
     * <ul>
     * <li>{@link Put} - Add or update data in columns</li>
     * <li>{@link Delete} - Remove data from columns or entire rows</li>
     * </ul>
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
     * @param mutation the Put or Delete mutation to add to this batch; must not be null
     * @return this AnyRowMutations instance for method chaining
     * @throws IOException if the row key of the added mutation doesn't match the row key
     *                     specified when creating this AnyRowMutations instance
     * @throws IllegalArgumentException if mutation is null or is not a Put or Delete
     * @see Put
     * @see Delete
     * @see #add(List)
     */
    public AnyRowMutations add(final Mutation mutation) throws IOException {
        rowMutations.add(mutation);

        return this;
    }

    /**
     * Adds multiple mutations to this batch of row mutations in a single call.
     * <p>
     * This is a convenience method for adding multiple Put or Delete operations at once.
     * All mutations in the list must target the same row key that was specified when
     * creating this AnyRowMutations instance. This method is more efficient than calling
     * {@link #add(Mutation)} multiple times when you have multiple mutations to add.
     * </p>
     *
     * <p><strong>Supported Mutation Types:</strong></p>
     * <ul>
     * <li>{@link Put} - Add or update data in columns</li>
     * <li>{@link Delete} - Remove data from columns or entire rows</li>
     * </ul>
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
     * @param mutations the list of Put or Delete mutations to add to this batch; must not be null or empty
     * @return this AnyRowMutations instance for method chaining
     * @throws IOException if the row key of any mutation doesn't match the row key
     *                     specified when creating this AnyRowMutations instance
     * @throws IllegalArgumentException if mutations is null, empty, or contains unsupported mutation types
     * @see Put
     * @see Delete
     * @see #add(Mutation)
     */
    public AnyRowMutations add(final List<? extends Mutation> mutations) throws IOException {
        rowMutations.add(mutations);

        return this;
    }

    /**
     * Returns the row key for this batch of mutations as a byte array.
     * <p>
     * All mutations in this batch must operate on the same row, which is
     * represented by this row key.
     * </p>
     *
     * @return the row key as a byte array; never null
     */
    @Override
    public byte[] getRow() {
        return rowMutations.getRow();
    }

    /**
     * Compares this AnyRowMutations with another Row based on their row keys.
     * <p>
     * The comparison is performed using lexicographic byte array comparison,
     * which is the standard row key ordering in HBase.
     * </p>
     *
     * @param row the Row to compare with this AnyRowMutations
     * @return a negative integer, zero, or positive integer if this row key is
     *         less than, equal to, or greater than the specified row's key
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
