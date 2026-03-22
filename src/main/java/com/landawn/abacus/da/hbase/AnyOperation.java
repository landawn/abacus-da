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

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.client.Operation;

import com.landawn.abacus.exception.UncheckedIOException;

/**
 * Abstract base class that provides a wrapper around HBase's {@code Operation} class to simplify
 * operations by reducing the need for manual conversion between bytes and String/Object types.
 * <p>
 * This class serves as the foundation for various HBase operation wrapper classes in the
 * abacus-da library, providing common functionality for operation fingerprinting, serialization,
 * and string representation.
 * </p>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * AnyGet anyGet = new AnyGet(get);
 * Map<String, Object> fingerprint = anyGet.getFingerprint();
 * }</pre>
 *
 * @param <AO> the concrete subtype of AnyOperation, enabling method chaining with proper return types
 * @see <a href="http://hbase.apache.org/devapidocs/index.html">Apache HBase Java API Documentation</a>
 * @see org.apache.hadoop.hbase.client.Operation
 */
abstract class AnyOperation<AO extends AnyOperation<AO>> {

    protected final Operation op;

    /**
     * Constructs a new AnyOperation wrapper around the specified HBase Operation.
     *
     * @param op the HBase Operation to wrap; must not be null
     */
    protected AnyOperation(final Operation op) {
        if (op == null) {
            throw new IllegalArgumentException("Operation must not be null");
        }
        this.op = op;
    }

    /**
     * Returns a map containing the fingerprint of this operation, which includes
     * key characteristics that uniquely identify the operation type and structure.
     * <p>
     * The fingerprint typically includes information such as operation type,
     * target table, row key, and other identifying attributes.
     * </p>
     *
     * @return a Map containing key-value pairs representing the operation's fingerprint;
     *         never null but may be empty
     */
    public Map<String, Object> getFingerprint() {
        return op.getFingerprint();
    }

    /**
     * Converts this operation to a Map representation containing all operation details.
     * <p>
     * This method provides a comprehensive view of the operation's structure and data,
     * including all columns, values, and metadata. The resulting map can be useful
     * for debugging, logging, or serialization purposes.
     * </p>
     *
     * @return a Map representation of this operation containing all operation details;
     *         never null but may be empty
     */
    public Map<String, Object> toMap() {
        return op.toMap();
    }

    /**
     * Converts this operation to a Map representation with a limit on the number of columns included.
     * <p>
     * This method is similar to {@link #toMap()} but allows limiting the number of columns
     * included in the output, which can be useful for large operations where you want to
     * avoid overwhelming output or improve performance.
     * </p>
     *
     * @param maxCols the maximum number of columns to include in the map representation;
     *                if negative, all columns will be included
     * @return a Map representation of this operation with at most maxCols columns;
     *         never null but may be empty
     */
    public Map<String, Object> toMap(final int maxCols) {
        return op.toMap(maxCols);
    }

    /**
     * Converts this operation to a JSON string representation.
     * <p>
     * This method serializes the entire operation structure to JSON format,
     * which is useful for logging, debugging, or network transmission. The JSON
     * includes all operation details such as row key, column families, qualifiers,
     * and values.
     * </p>
     *
     * @return a JSON string representation of this operation; never null
     * @throws UncheckedIOException if an I/O error occurs during JSON serialization
     */
    public String toJson() {
        try {
            return op.toJSON();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Converts this operation to a JSON string representation with a limit on the number of columns.
     * <p>
     * This method is similar to {@link #toJson()} but allows limiting the number of columns
     * included in the JSON output. This can be useful for large operations where you want
     * more manageable output or improved performance.
     * </p>
     *
     * @param maxCols the maximum number of columns to include in the JSON representation;
     *                if negative, all columns will be included
     * @return a JSON string representation of this operation with at most maxCols columns;
     *         never null
     * @throws UncheckedIOException if an I/O error occurs during JSON serialization
     */
    public String toJson(final int maxCols) {
        try {
            return op.toJSON(maxCols);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Converts this operation to a JSON string representation.
     *
     * @return a JSON string representation of this operation
     * @deprecated Use {@link #toJson()} instead.
     */
    @Deprecated
    public String toJSON() {
        return toJson();
    }

    /**
     * Converts this operation to a JSON string representation with a column limit.
     *
     * @param maxCols the maximum number of columns to include in the JSON output
     * @return a JSON string representation of this operation
     * @deprecated Use {@link #toJson(int)} instead.
     */
    @Deprecated
    public String toJSON(final int maxCols) {
        return toJson(maxCols);
    }

    /**
     * Returns a string representation of this operation.
     * <p>
     * This method provides a human-readable string representation of the operation,
     * including all relevant details. The format is determined by the underlying
     * HBase Operation's toString() implementation.
     * </p>
     *
     * @return a string representation of this operation; never null
     */
    @Override
    public String toString() {
        return op.toString();
    }

    /**
     * Returns a string representation of this operation with a limit on the number of columns.
     * <p>
     * This method is similar to {@link #toString()} but allows limiting the number of columns
     * included in the string representation. This can help manage output size for operations
     * with many columns.
     * </p>
     *
     * @param maxCols the maximum number of columns to include in the string representation;
     *                if negative, all columns will be included
     * @return a string representation of this operation with at most maxCols columns;
     *         never null
     */
    public String toString(final int maxCols) {
        return op.toString(maxCols);
    }
}
