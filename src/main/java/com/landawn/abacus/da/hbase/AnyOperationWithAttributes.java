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
 * Abstract wrapper for HBase {@code OperationWithAttributes} that simplifies attribute management
 * and operation identification by providing automatic type conversion and fluent API design.
 * <p>
 * This class extends {@link AnyOperation} and adds support for operation attributes, identification,
 * and priority management. It serves as the base class for HBase operations that support custom
 * attributes such as {@link AnyQuery} and {@link AnyMutation} operations.
 * </p>
 * 
 * <p>Key features include:</p>
 * <ul>
 * <li><strong>Attribute Management</strong>: Set and retrieve custom attributes with automatic type conversion</li>
 * <li><strong>Operation Identification</strong>: Assign unique identifiers for tracking and debugging</li>
 * <li><strong>Priority Control</strong>: Set operation priority for resource management</li>
 * <li><strong>Type Safety</strong>: Automatic conversion from Java objects to byte arrays for attributes</li>
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
 * @param <AOWA> the concrete subtype of AnyOperationWithAttributes for method chaining
 * @see AnyOperation
 * @see OperationWithAttributes
 * @see <a href="http://hbase.apache.org/devapidocs/index.html">Apache HBase Java API Documentation</a>
 */
abstract class AnyOperationWithAttributes<AOWA extends AnyOperationWithAttributes<AOWA>> extends AnyOperation<AOWA> {

    protected final OperationWithAttributes owa;

    /**
     * Constructs a new AnyOperationWithAttributes wrapper around the specified HBase OperationWithAttributes.
     *
     * @param owa the HBase OperationWithAttributes to wrap; must not be null
     * @throws IllegalArgumentException if owa is null
     */
    protected AnyOperationWithAttributes(final OperationWithAttributes owa) {
        super(owa);
        if (owa == null) {
            throw new IllegalArgumentException("OperationWithAttributes must not be null");
        }
        this.owa = owa;
    }

    /**
     * Retrieves the value of a custom attribute by name.
     * <p>
     * Attributes are custom key-value pairs that can be attached to HBase operations
     * to pass metadata, configuration, or tracking information. The value is returned
     * as a byte array, which can be converted back to the original type if needed.
     * </p>
     *
     * @param name the name of the attribute to retrieve; must not be null
     * @return the attribute value as a byte array, or null if the attribute doesn't exist
     * @throws IllegalArgumentException if name is null
     * @see #setAttribute(String, Object)
     * @see #getAttributesMap()
     */
    public byte[] getAttribute(final String name) {
        return owa.getAttribute(name);
    }

    /**
     * Returns a map of all custom attributes associated with this operation.
     * <p>
     * This method provides access to all custom attributes that have been set on this operation.
     * The returned map contains attribute names as keys and their corresponding byte array values.
     * Modifications to the returned map may or may not affect the underlying operation attributes.
     * </p>
     *
     * @return a Map containing all operation attributes; never null but may be empty
     * @see #getAttribute(String)
     * @see #setAttribute(String, Object)
     */
    public Map<String, byte[]> getAttributesMap() {
        return owa.getAttributesMap();
    }

    /**
     * Sets a custom attribute on this operation with automatic type conversion.
     * <p>
     * This method allows attaching custom metadata to HBase operations. The value object
     * is automatically converted to a byte array using HBase's standard serialization.
     * Attributes can be used by coprocessors, filters, or client code to pass additional
     * information or control operation behavior.
     * </p>
     * 
     * <p><strong>Common Use Cases:</strong></p>
     * <ul>
     * <li>Client identification and version tracking</li>
     * <li>Request correlation and tracing</li>
     * <li>Custom coprocessor parameters</li>
     * <li>Operation metadata and annotations</li>
     * </ul>
     *
     * @param name the attribute name; must not be null or empty
     * @param value the attribute value; will be converted to byte array automatically
     * @return this instance for method chaining
     * @throws IllegalArgumentException if name is null or empty
     * @see #getAttribute(String)
     * @see HBaseExecutor#toValueBytes(Object)
     */
    public AOWA setAttribute(final String name, final Object value) {
        owa.setAttribute(name, HBaseExecutor.toValueBytes(value));

        return (AOWA) this;
    }

    /**
     * Returns the unique identifier for this operation, if one has been set.
     * <p>
     * Operation identifiers are useful for tracking, debugging, and correlating operations
     * across different systems or logs. They can help identify slow operations, trace
     * request flows, or correlate client operations with server-side metrics.
     * </p>
     *
     * @return the operation identifier, or null if no identifier has been set
     * @see #setId(String)
     */
    public String getId() {
        return owa.getId();
    }

    /**
     * Sets a unique identifier for this operation to enable tracking and debugging.
     * <p>
     * Operation identifiers are extremely useful for:</p>
     * <ul>
     * <li><strong>Slow Query Analysis</strong>: Identify operations that exceed performance thresholds</li>
     * <li><strong>Request Tracing</strong>: Correlate operations across different system components</li>
     * <li><strong>Debugging</strong>: Track specific operations through logs and metrics</li>
     * <li><strong>Performance Monitoring</strong>: Associate operations with specific code paths</li>
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
     * @param id the unique identifier for this operation; can be null to clear
     * @return this instance for method chaining
     * @see #getId()
     */
    public AOWA setId(final String id) {
        owa.setId(id);

        return (AOWA) this;
    }

    /**
     * Returns the priority level set for this operation.
     * <p>
     * Operation priorities can be used by HBase region servers and custom schedulers
     * to manage resource allocation and operation ordering. Higher priority operations
     * may be processed before lower priority ones, depending on the server configuration.
     * </p>
     *
     * @return the current priority level for this operation
     * @see #setPriority(int)
     */
    public int getPriority() {
        return owa.getPriority();
    }

    /**
     * Sets the priority level for this operation to influence execution order.
     * <p>
     * Priority values allow clients to indicate the relative importance of operations
     * to the HBase server. While the exact behavior depends on server configuration,
     * higher priority operations are typically processed before lower priority ones
     * when resources are constrained.
     * </p>
     * 
     * <p><strong>Priority Guidelines:</strong></p>
     * <ul>
     * <li><strong>High Priority (100+)</strong>: Critical operations, user-facing requests</li>
     * <li><strong>Normal Priority (0-99)</strong>: Standard operations</li>
     * <li><strong>Low Priority (&lt;0)</strong>: Background tasks, bulk operations</li>
     * </ul>
     *
     * @param priority the priority level for this operation; higher values indicate higher priority
     * @return this instance for method chaining
     * @see #getPriority()
     */
    public AOWA setPriority(final int priority) {
        owa.setPriority(priority);

        return (AOWA) this;
    }
}
