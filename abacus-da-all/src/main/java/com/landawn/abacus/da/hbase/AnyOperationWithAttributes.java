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

import java.util.Map;

import org.apache.hadoop.hbase.client.OperationWithAttributes;

/**
 * Abstract wrapper around HBase {@link OperationWithAttributes} that exposes attribute, operation
 * identifier, and priority management through a fluent API with automatic value conversion.
 *
 * <p>This class extends {@link AnyOperation} and is the common base for the two main families of
 * HBase operations that carry attributes: {@link AnyQuery} (and its concrete subclasses
 * {@link AnyGet}, {@link AnyScan}) and {@link AnyMutation} (and its concrete subclasses
 * {@link AnyPut}, {@link AnyDelete}, {@link AnyAppend}, {@link AnyIncrement}). Attribute
 * values supplied to {@link #setAttribute(String, Object)} are converted to byte arrays via
 * {@link HBaseExecutor#toValueBytes(Object)} before being stored on the underlying HBase
 * operation.</p>
 *
 * <p>Key features:</p>
 * <ul>
 * <li><strong>Attribute Management</strong>: set and retrieve named attributes that are forwarded
 *     to coprocessors and the server</li>
 * <li><strong>Operation Identification</strong>: assign a string id used in HBase's slow-query and
 *     tracing facilities</li>
 * <li><strong>Priority Control</strong>: hint the region server's request scheduler about the
 *     relative importance of this operation</li>
 * <li><strong>Automatic Conversion</strong>: Java {@link Object}s are serialized to byte arrays
 *     by {@link HBaseExecutor}</li>
 * </ul>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Set custom attributes
 * anyOperation.setAttribute("client.version", "1.0.0")
 *             .setAttribute("request.type", "batch")
 *             .setId("user-session-12345")
 *             .setPriority(100);
 *
 * // Retrieve attributes
 * byte[] version = anyOperation.getAttribute("client.version");
 * Map<String, byte[]> allAttrs = anyOperation.getAttributesMap();
 * }</pre>
 *
 * @param <AOWA> the concrete subtype of {@code AnyOperationWithAttributes}; declared so fluent
 *               setters can return {@code AOWA} and preserve the concrete type during chaining
 * @see AnyOperation
 * @see OperationWithAttributes
 * @see <a href="http://hbase.apache.org/devapidocs/index.html">Apache HBase Java API Documentation</a>
 */
abstract class AnyOperationWithAttributes<AOWA extends AnyOperationWithAttributes<AOWA>> extends AnyOperation<AOWA> {

    protected final OperationWithAttributes owa;

    /**
     * Constructs a new {@code AnyOperationWithAttributes} that delegates to the supplied HBase
     * {@link OperationWithAttributes}. The wrapped operation is also passed to the superclass
     * constructor as the underlying {@link AnyOperation#op}.
     *
     * @param owa the HBase {@link OperationWithAttributes} to wrap; must not be {@code null}
     * @throws IllegalArgumentException if {@code owa} is {@code null}
     */
    protected AnyOperationWithAttributes(final OperationWithAttributes owa) {
        super(owa);
        if (owa == null) {
            throw new IllegalArgumentException("OperationWithAttributes must not be null");
        }
        this.owa = owa;
    }

    /**
     * Returns the raw byte-array value of the attribute with the given name. Attributes are
     * named byte-array properties attached to HBase operations and forwarded to the server (and
     * to any coprocessors / filters). Callers that wrote the attribute via
     * {@link #setAttribute(String, Object)} will need to decode the bytes back to the original
     * type themselves.
     *
     * @param name the name of the attribute to retrieve
     * @return the attribute value as a byte array, or {@code null} if no attribute with the given
     *         name is set
     * @see #setAttribute(String, Object)
     * @see #getAttributesMap()
     */
    public byte[] getAttribute(final String name) {
        return owa.getAttribute(name);
    }

    /**
     * Returns the full attribute map for this operation. Keys are attribute names; values are the
     * raw byte-array payloads. The map is the one held by the underlying HBase operation —
     * whether changes to it propagate back depends on the HBase version and is best treated as
     * unspecified, so callers should mutate attributes through {@link #setAttribute(String, Object)}
     * rather than via the returned map.
     *
     * @return a {@link Map} of attribute name to byte-array value; never {@code null} but may be
     *         empty when no attributes have been set
     * @see #getAttribute(String)
     * @see #setAttribute(String, Object)
     */
    public Map<String, byte[]> getAttributesMap() {
        return owa.getAttributesMap();
    }

    /**
     * Sets or replaces an attribute on this operation. The {@code value} is converted to a byte
     * array via {@link HBaseExecutor#toValueBytes(Object)} before being stored, so any type
     * supported by that helper (including {@link String}, primitive wrappers, and arbitrary
     * {@link Object}s serialized by the configured codec) is accepted.
     *
     * <p><strong>Common use cases:</strong></p>
     * <ul>
     * <li>Client identification and version tracking</li>
     * <li>Request correlation and tracing</li>
     * <li>Custom coprocessor parameters</li>
     * <li>Operation metadata and annotations</li>
     * </ul>
     *
     * @param name the attribute name; must not be {@code null}
     * @param value the attribute value, converted to bytes via {@link HBaseExecutor#toValueBytes(Object)};
     *              the resulting byte-array semantics (including how {@code null} is handled) are
     *              defined by {@link HBaseExecutor#toValueBytes(Object)} and the underlying HBase
     *              {@link OperationWithAttributes#setAttribute(String, byte[])} contract
     * @return this instance, to allow fluent method chaining
     * @see #getAttribute(String)
     * @see HBaseExecutor#toValueBytes(Object)
     */
    public AOWA setAttribute(final String name, final Object value) {
        owa.setAttribute(name, HBaseExecutor.toValueBytes(value));

        return (AOWA) this;
    }

    /**
     * Returns the operation identifier set via {@link #setId(String)}. HBase echoes the id in
     * its slow-operation log, which makes it useful for correlating client-side traces with
     * server-side log entries.
     *
     * @return the operation identifier, or {@code null} if none has been set
     * @see #setId(String)
     */
    public String getId() {
        return owa.getId();
    }

    /**
     * Sets a string identifier on this operation. The id is recorded by HBase and surfaces in
     * its slow-operation log entries, which makes it useful for:
     * <ul>
     * <li><strong>Slow-query analysis</strong>: locating server-side log entries for an operation</li>
     * <li><strong>Request tracing</strong>: correlating client requests with server-side activity</li>
     * <li><strong>Debugging</strong>: distinguishing operations issued from different call sites</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Class.method format for debugging
     * operation.setId("UserService.getUserProfile");
     *
     * // UUID for unique tracking
     * operation.setId(UUID.randomUUID().toString());
     *
     * // Custom format with context
     * operation.setId("batch-user-updates-" + System.currentTimeMillis());
     * }</pre>
     *
     * @param id the identifier to assign; may be {@code null} to clear any previously set id
     * @return this instance, to allow fluent method chaining
     * @see #getId()
     */
    public AOWA setId(final String id) {
        owa.setId(id);

        return (AOWA) this;
    }

    /**
     * Returns the priority hint set on this operation. The region-server request scheduler may
     * use the value to order or throttle operations; the exact semantics depend on the server
     * configuration.
     *
     * @return the priority value previously set via {@link #setPriority(int)}, or the value
     *         returned by {@link OperationWithAttributes#getPriority()} (HBase's default) when
     *         none has been set on this operation
     * @see #setPriority(int)
     */
    public int getPriority() {
        return owa.getPriority();
    }

    /**
     * Sets a priority hint on this operation. Higher values indicate higher priority; the
     * region-server request scheduler may use the value to order or throttle operations when
     * resources are constrained. The actual interpretation depends on server configuration.
     *
     * <p><strong>Suggested priority bands:</strong></p>
     * <ul>
     * <li><strong>High Priority (100+)</strong>: Critical operations, user-facing requests</li>
     * <li><strong>Normal Priority (0-99)</strong>: Standard operations</li>
     * <li><strong>Low Priority (&lt;0)</strong>: Background tasks, bulk operations</li>
     * </ul>
     *
     * @param priority the priority value to assign; higher means higher priority
     * @return this instance, to allow fluent method chaining
     * @see #getPriority()
     */
    public AOWA setPriority(final int priority) {
        owa.setPriority(priority);

        return (AOWA) this;
    }
}
