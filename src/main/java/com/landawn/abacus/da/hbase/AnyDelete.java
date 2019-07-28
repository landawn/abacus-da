/*
 * Copyright (C) 2015 HaiYang Li
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;

// TODO: Auto-generated Javadoc
/**
 * It's a wrapper of <code>Delete</code> in HBase to reduce the manual conversion between bytes and String/Object.
 *
 * @see <a href="http://hbase.apache.org/devapidocs/index.html">http://hbase.apache.org/devapidocs/index.html</a>
 * @see org.apache.hadoop.hbase.client.Delete
 * @since 0.8
 */
public final class AnyDelete extends AnyMutation<AnyDelete> {

    /** The delete. */
    private final Delete delete;

    /**
     * Instantiates a new any delete.
     *
     * @param rowKey the row key
     */
    public AnyDelete(final Object rowKey) {
        super(new Delete(toRowKeyBytes(rowKey)));
        this.delete = (Delete) mutation;
    }

    /**
     * Instantiates a new any delete.
     *
     * @param rowKey the row key
     * @param timestamp the timestamp
     */
    public AnyDelete(final Object rowKey, final long timestamp) {
        super(new Delete(toRowKeyBytes(rowKey), timestamp));
        this.delete = (Delete) mutation;
    }

    /**
     * Instantiates a new any delete.
     *
     * @param rowKey the row key
     * @param rowOffset the row offset
     * @param rowLength the row length
     */
    public AnyDelete(final Object rowKey, final int rowOffset, final int rowLength) {
        super(new Delete(toRowKeyBytes(rowKey), rowOffset, rowLength));
        this.delete = (Delete) mutation;
    }

    /**
     * Instantiates a new any delete.
     *
     * @param rowKey the row key
     * @param rowOffset the row offset
     * @param rowLength the row length
     * @param timestamp the timestamp
     */
    public AnyDelete(final Object rowKey, final int rowOffset, final int rowLength, final long timestamp) {
        super(new Delete(toRowKeyBytes(rowKey), rowOffset, rowLength, timestamp));
        this.delete = (Delete) mutation;
    }

    /**
     * Instantiates a new any delete.
     *
     * @param row the row
     * @param timestamp the timestamp
     * @param familyMap the family map
     */
    public AnyDelete(final Object row, final long timestamp, final NavigableMap<byte[], List<Cell>> familyMap) {
        super(new Delete(toRowBytes(row), timestamp, familyMap));
        this.delete = (Delete) mutation;
    }

    /**
     * Instantiates a new any delete.
     *
     * @param deleteToCopy delete to copy
     */
    public AnyDelete(final Delete deleteToCopy) {
        super(new Delete(deleteToCopy));
        this.delete = (Delete) mutation;
    }

    /**
     * Of.
     *
     * @param rowKey the row key
     * @return the any delete
     */
    public static AnyDelete of(final Object rowKey) {
        return new AnyDelete(rowKey);
    }

    /**
     * Of.
     *
     * @param rowKey the row key
     * @param timestamp the timestamp
     * @return the any delete
     */
    public static AnyDelete of(final Object rowKey, final long timestamp) {
        return new AnyDelete(rowKey, timestamp);
    }

    /**
     * Of.
     *
     * @param rowKey the row key
     * @param rowOffset the row offset
     * @param rowLength the row length
     * @return the any delete
     */
    public static AnyDelete of(final Object rowKey, final int rowOffset, final int rowLength) {
        return new AnyDelete(rowKey, rowOffset, rowLength);
    }

    /**
     * Of.
     *
     * @param rowKey the row key
     * @param rowOffset the row offset
     * @param rowLength the row length
     * @param timestamp the timestamp
     * @return the any delete
     */
    public static AnyDelete of(final Object rowKey, final int rowOffset, final int rowLength, final long timestamp) {
        return new AnyDelete(rowKey, timestamp);
    }

    /**
     * Of.
     *
     * @param row the row
     * @param timestamp the timestamp
     * @param familyMap the family map
     * @return the any delete
     */
    public static AnyDelete of(final Object row, final long timestamp, final NavigableMap<byte[], List<Cell>> familyMap) {
        return new AnyDelete(row, timestamp, familyMap);
    }

    /**
     * Of.
     *
     * @param deleteToCopy the delete to copy
     * @return the any delete
     */
    public static AnyDelete of(final Delete deleteToCopy) {
        return new AnyDelete(deleteToCopy);
    }

    /**
     * Val.
     *
     * @return the delete
     */
    public Delete val() {
        return delete;
    }

    /**
     * Advanced use only. Add an existing delete marker to this Delete object.
     *
     * @param kv An existing KeyValue of type "delete".
     * @return this for invocation chaining
     * @throws IOException Signals that an I/O exception has occurred.
     * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0. Use {@code add(Cell)}
     *             instead
     */
    @SuppressWarnings("unchecked")
    @Deprecated
    public AnyDelete addDeleteMarker(Cell kv) throws IOException {
        delete.addDeleteMarker(kv);

        return this;
    }

    /**
     * Adds the.
     *
     * @param kv the kv
     * @return the any delete
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public AnyDelete add(Cell kv) throws IOException {
        delete.add(kv);

        return this;
    }

    /**
     * Delete all versions of all columns of the specified family.
     * <p>
     * Overrides previous calls to deleteColumn and deleteColumns for the
     * specified family.
     *
     * @param family the family
     * @return the any delete
     */
    public AnyDelete addFamily(final String family) {
        delete.addFamily(toFamilyQualifierBytes(family));

        return this;
    }

    /**
     * Delete all columns of the specified family with a timestamp less than
     * or equal to the specified timestamp.
     * <p>
     * Overrides previous calls to deleteColumn and deleteColumns for the
     * specified family.
     *
     * @param family the family
     * @param timestamp the timestamp
     * @return the any delete
     */
    public AnyDelete addFamily(final String family, final long timestamp) {
        delete.addFamily(toFamilyQualifierBytes(family), timestamp);

        return this;
    }

    /**
     * Delete all versions of all columns of the specified family.
     * <p>
     * Overrides previous calls to deleteColumn and deleteColumns for the
     * specified family.
     *
     * @param family the family
     * @return the any delete
     */
    public AnyDelete addFamily(final byte[] family) {
        delete.addFamily(family);

        return this;
    }

    /**
     * Delete all columns of the specified family with a timestamp less than
     * or equal to the specified timestamp.
     * <p>
     * Overrides previous calls to deleteColumn and deleteColumns for the
     * specified family.
     *
     * @param family the family
     * @param timestamp the timestamp
     * @return the any delete
     */
    public AnyDelete addFamily(final byte[] family, final long timestamp) {
        delete.addFamily(family, timestamp);

        return this;
    }

    /**
     * Delete all columns of the specified family with a timestamp equal to the specified timestamp.
     *
     * @param family the family
     * @param timestamp the timestamp
     * @return the any delete
     */
    public AnyDelete addFamilyVersion(final String family, final long timestamp) {
        delete.addFamilyVersion(toFamilyQualifierBytes(family), timestamp);

        return this;
    }

    /**
     * Delete all columns of the specified family with a timestamp equal to the specified timestamp.
     *
     * @param family the family
     * @param timestamp the timestamp
     * @return the any delete
     */
    public AnyDelete addFamilyVersion(final byte[] family, final long timestamp) {
        delete.addFamilyVersion(family, timestamp);

        return this;
    }

    /**
     * Delete the latest version of the specified column.
     * This is an expensive call in that on the server-side, it first does a
     * get to find the latest versions timestamp.  Then it adds a delete using
     * the fetched cells timestamp.
     *
     * @param family the family
     * @param qualifier the qualifier
     * @return the any delete
     */
    public AnyDelete addColumn(final String family, final String qualifier) {
        delete.addColumn(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier));

        return this;
    }

    /**
     * Delete the specified version of the specified column.
     *
     * @param family the family
     * @param qualifier the qualifier
     * @param timestamp the timestamp
     * @return the any delete
     */
    public AnyDelete addColumn(final String family, final String qualifier, final long timestamp) {
        delete.addColumn(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier), timestamp);

        return this;
    }

    /**
     * Delete the latest version of the specified column.
     * This is an expensive call in that on the server-side, it first does a
     * get to find the latest versions timestamp.  Then it adds a delete using
     * the fetched cells timestamp.
     *
     * @param family the family
     * @param qualifier the qualifier
     * @return the any delete
     */
    public AnyDelete addColumn(final byte[] family, final byte[] qualifier) {
        delete.addColumn(family, qualifier);

        return this;
    }

    /**
     * Delete the specified version of the specified column.
     *
     * @param family the family
     * @param qualifier the qualifier
     * @param timestamp the timestamp
     * @return the any delete
     */
    public AnyDelete addColumn(final byte[] family, final byte[] qualifier, final long timestamp) {
        delete.addColumn(family, qualifier, timestamp);

        return this;
    }

    /**
     * Delete all versions of the specified column.
     *
     * @param family the family
     * @param qualifier the qualifier
     * @return the any delete
     */
    public AnyDelete addColumns(final String family, final String qualifier) {
        delete.addColumns(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier));

        return this;
    }

    /**
     * Delete all versions of the specified column with a timestamp less than or equal to the specified timestamp.
     *
     * @param family the family
     * @param qualifier the qualifier
     * @param timestamp the timestamp
     * @return the any delete
     */
    public AnyDelete addColumns(final String family, final String qualifier, final long timestamp) {
        delete.addColumns(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier), timestamp);

        return this;
    }

    /**
     * Delete all versions of the specified column.
     *
     * @param family the family
     * @param qualifier the qualifier
     * @return the any delete
     */
    public AnyDelete addColumns(final byte[] family, final byte[] qualifier) {
        delete.addColumns(family, qualifier);

        return this;
    }

    /**
     * Delete all versions of the specified column with a timestamp less than or equal to the specified timestamp.
     *
     * @param family the family
     * @param qualifier the qualifier
     * @param timestamp the timestamp
     * @return the any delete
     */
    public AnyDelete addColumns(final byte[] family, final byte[] qualifier, final long timestamp) {
        delete.addColumns(family, qualifier, timestamp);

        return this;
    }

    /**
     * Hash code.
     *
     * @return the int
     */
    @Override
    public int hashCode() {
        return delete.hashCode();
    }

    /**
     * Equals.
     *
     * @param obj the obj
     * @return true, if successful
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof AnyDelete) {
            AnyDelete other = (AnyDelete) obj;

            return this.delete.equals(other.delete);
        }

        return false;
    }

    /**
     * To string.
     *
     * @return the string
     */
    @Override
    public String toString() {
        return delete.toString();
    }
}
