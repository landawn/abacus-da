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
 * Abstract base class that wraps an HBase {@link Operation} and exposes its
 * fingerprint / map / JSON / string representations through a simpler API that does not require
 * callers to convert between byte arrays and Java {@link String}s or {@link Object}s.
 *
 * <p>This class is the root of the {@code AnyOperation} hierarchy in the abacus-da library and
 * supplies the common metadata-introspection methods (fingerprint, {@code toMap}, {@code toJson},
 * {@code toString}) that every concrete operation wrapper inherits. Mutation-specific or
 * query-specific behaviour is added by the subclasses {@link AnyOperationWithAttributes},
 * {@link AnyMutation}, and {@link AnyQuery}.</p>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * AnyGet anyGet = AnyGet.of("user123");
 * Map<String, Object> fingerprint = anyGet.getFingerprint();
 * String json = anyGet.toJson();
 * }</pre>
 *
 * @param <AO> the concrete subtype of {@code AnyOperation}; declared so subclasses (such as
 *             {@link AnyOperationWithAttributes}) can return {@code AO} from fluent setters and
 *             preserve the concrete type during method chaining
 * @see <a href="http://hbase.apache.org/devapidocs/index.html">Apache HBase Java API Documentation</a>
 * @see org.apache.hadoop.hbase.client.Operation
 */
abstract class AnyOperation<AO extends AnyOperation<AO>> {

    /**
     * The underlying HBase {@link Operation} that every method on this class delegates to.
     * Exposed to subclasses (such as {@link AnyOperationWithAttributes}, {@link AnyMutation},
     * and {@link AnyQuery}) so that they may downcast to their corresponding HBase operation
     * type and invoke type-specific behaviour. Set once in the constructor and never reassigned.
     */
    protected final Operation op;

    /**
     * Constructs a new {@code AnyOperation} that delegates to the supplied HBase {@link Operation}.
     * The wrapped operation is stored in the {@code op} field and used by every other method on
     * this class.
     *
     * @param op the HBase {@link Operation} to wrap; must not be {@code null}
     * @throws IllegalArgumentException if {@code op} is {@code null}
     */
    protected AnyOperation(final Operation op) {
        if (op == null) {
            throw new IllegalArgumentException("Operation must not be null");
        }
        this.op = op;
    }

    /**
     * Returns the fingerprint of this operation as supplied by the underlying HBase
     * {@link Operation#getFingerprint()}. The fingerprint captures the structural shape of the
     * operation (such as the affected column families) but does not include row keys, qualifier
     * values, or other potentially high-cardinality identifiers; it is meant to group operations
     * with the same schema for metrics and monitoring rather than to uniquely identify them.
     *
     * @return the fingerprint map produced by HBase; never {@code null} but may be empty
     * @see #toMap()
     */
    public Map<String, Object> getFingerprint() {
        return op.getFingerprint();
    }

    /**
     * Converts this operation to a {@link Map} representation containing all operation details,
     * up to HBase's default column limit (see {@link #toMap(int)} for an explicit limit). The
     * resulting map combines the {@linkplain #getFingerprint() fingerprint} with row-, column-,
     * and value-level details and is intended for debugging, logging, or serialization.
     *
     * @return a {@code Map} representation of this operation; never {@code null}
     * @see #toMap(int)
     * @see #getFingerprint()
     */
    public Map<String, Object> toMap() {
        return op.toMap();
    }

    /**
     * Converts this operation to a {@link Map} representation, capping the number of columns
     * included per family at {@code maxCols}. This is the explicit-limit counterpart to
     * {@link #toMap()} and is useful for very large operations where the full map would be
     * impractical to log or serialize.
     *
     * @param maxCols the maximum number of columns to include per family in the map representation
     * @return a {@code Map} representation of this operation with at most {@code maxCols} columns
     *         per family; never {@code null}
     * @see #toMap()
     */
    public Map<String, Object> toMap(final int maxCols) {
        return op.toMap(maxCols);
    }

    /**
     * Serializes this operation to a JSON string by delegating to
     * {@link Operation#toJSON()}. The JSON includes the operation's row key, column families,
     * qualifiers, values, and other details, and is intended for logging, debugging, or network
     * transmission. The underlying {@link IOException} thrown by HBase is wrapped as an
     * {@link UncheckedIOException}.
     *
     * @return a JSON string representation of this operation; never {@code null}
     * @throws UncheckedIOException if HBase's JSON serialization throws an {@link IOException}
     * @see #toJson(int)
     */
    public String toJson() {
        try {
            return op.toJSON();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Serializes this operation to a JSON string, capping the number of columns included per
     * family at {@code maxCols}. The explicit-limit counterpart to {@link #toJson()}; useful for
     * very large operations where the full JSON would be impractical to log.
     *
     * @param maxCols the maximum number of columns to include per family in the JSON representation
     * @return a JSON string representation of this operation with at most {@code maxCols} columns
     *         per family; never {@code null}
     * @throws UncheckedIOException if HBase's JSON serialization throws an {@link IOException}
     * @see #toJson()
     */
    public String toJson(final int maxCols) {
        try {
            return op.toJSON(maxCols);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    //    /**
    //     * Converts this operation to a JSON string representation.
    //     *
    //     * @return a JSON string representation of this operation
    //     * @deprecated Use {@link #toJson()} instead.
    //     */
    //    @Deprecated
    //    public String toJSON() {
    //        return toJson();
    //    }
    //
    //    /**
    //     * Converts this operation to a JSON string representation with a column limit.
    //     *
    //     * @param maxCols the maximum number of columns to include in the JSON output
    //     * @return a JSON string representation of this operation
    //     * @deprecated Use {@link #toJson(int)} instead.
    //     */
    //    @Deprecated
    //    public String toJSON(final int maxCols) {
    //        return toJson(maxCols);
    //    }

    /**
     * Returns a human-readable string representation of this operation. The exact format is
     * delegated to the underlying HBase {@link Operation#toString()} implementation.
     *
     * @return a string representation of this operation; never {@code null}
     * @see #toString(int)
     */
    @Override
    public String toString() {
        return op.toString();
    }

    /**
     * Returns a human-readable string representation of this operation, capping the number of
     * columns included per family at {@code maxCols}. Useful for limiting log output for very
     * large operations.
     *
     * @param maxCols the maximum number of columns to include per family in the string representation
     * @return a string representation of this operation with at most {@code maxCols} columns
     *         per family; never {@code null}
     * @see #toString()
     */
    public String toString(final int maxCols) {
        return op.toString(maxCols);
    }
}
