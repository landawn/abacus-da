/*
 * Copyright (C) 2026 HaiYang Li
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

package com.landawn.abacus.da.aws;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Utility class providing property-map helper methods for the AWS data-access executors.
 *
 * <p>This class is not intended to be instantiated; all members are {@code static}.</p>
 */
public final class AnyUtil {
    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private AnyUtil() {
        // utility class
    }

    /**
     * Converts an array of alternating property names and values into an ordered
     * {@link Map} suitable for use as a property bag.
     *
     * <p>The input array is interpreted as alternating key-value pairs in the form
     * {@code [name1, value1, name2, value2, ...]}. Each name element must be a
     * {@link String}; a non-String (or {@code null}) name causes an
     * {@link IllegalArgumentException} to be thrown. The corresponding value is
     * stored as-is. The returned map is a {@link LinkedHashMap}, so iteration
     * order matches the order of the pairs in the input array. If a name occurs
     * more than once, the later value overwrites the earlier one (but the original
     * insertion position is retained).</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Typical: alternating name/value pairs, iteration order preserved.
     * Map<String, Object> p = AnyUtil.asProps(new Object[] { "name", "Alice", "age", 30 });
     * // returns {name=Alice, age=30} (a LinkedHashMap)
     *
     * // Negative: a non-String name is rejected.
     * AnyUtil.asProps(new Object[] { 100, "hundred" });
     * // throws IllegalArgumentException
     *
     * // Duplicate name: later value overwrites, original position retained.
     * Map<String, Object> r = AnyUtil.asProps(new Object[] { "z", 1, "a", 2, "z", 3 });
     * // returns {z=3, a=2}
     *
     * // Edge: null array yields an empty map (not null).
     * Map<String, Object> s = AnyUtil.asProps(null);
     * // returns {} (an empty LinkedHashMap)
     *
     * // Edge: empty array yields an empty map.
     * Map<String, Object> t = AnyUtil.asProps(new Object[0]);
     * // returns {}
     *
     * // Edge: a null value is allowed and preserved.
     * Map<String, Object> u = AnyUtil.asProps(new Object[] { "key", null });
     * // returns {key=null}
     *
     * // Negative: odd-length array is rejected.
     * AnyUtil.asProps(new Object[] { "name", "Alice", "age" });
     * // throws IllegalArgumentException
     * }</pre>
     *
     * @param propNameAndValues an even-length array of alternating property names
     *                          and values; may be {@code null}
     * @return a new {@link LinkedHashMap} containing the supplied pairs in order;
     *         an empty map if {@code propNameAndValues} is {@code null}
     * @throws IllegalArgumentException if {@code propNameAndValues} has an odd length,
     *         or if any property name is not a {@link String}
     */
    public static Map<String, Object> asProps(final Object[] propNameAndValues) {
        if (propNameAndValues == null) {
            return new LinkedHashMap<>(); // NOSONAR
        }

        if ((propNameAndValues.length % 2) != 0) {
            throw new IllegalArgumentException("The length of property name/value array must be even: " + propNameAndValues.length);
        }

        final Map<String, Object> props = new LinkedHashMap<>(propNameAndValues.length); // capacity = 2x pair count, so no rehash below the default load factor

        for (int i = 0, len = propNameAndValues.length; i < len; i += 2) {
            if (!(propNameAndValues[i] instanceof String)) {
                throw new IllegalArgumentException("Parameters must be property name-value pairs whose names are Strings, but found "
                        + (propNameAndValues[i] == null ? "null" : propNameAndValues[i].getClass().getName()) + " at index " + i);
            }

            props.put((String) propNameAndValues[i], propNameAndValues[i + 1]);
        }

        return props;
    }

}
