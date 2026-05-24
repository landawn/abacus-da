package com.landawn.abacus.da.util;

import java.util.LinkedHashMap;
import java.util.Map;

import com.landawn.abacus.util.N;

/**
 * Utility class providing general-purpose helper methods used across the data-access layer.
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
     * {@code [name1, value1, name2, value2, ...]}. Each name element is coerced to
     * a {@link String} via {@link N#stringOf(Object)}, and the corresponding value
     * is stored as-is. The returned map is a {@link LinkedHashMap}, so iteration
     * order matches the order of the pairs in the input array. If a name occurs
     * more than once, the later value overwrites the earlier one (but the original
     * insertion position is retained).</p>
     *
     * @param propNameAndValues an even-length array of alternating property names
     *                          and values; may be {@code null}
     * @return a new {@link LinkedHashMap} containing the supplied pairs in order;
     *         an empty map if {@code propNameAndValues} is {@code null}
     * @throws IllegalArgumentException if {@code propNameAndValues} has an odd length
     */
    public static Map<String, Object> array2Props(final Object[] propNameAndValues) {
        if (propNameAndValues == null) {
            return new LinkedHashMap<>(); // NOSONAR
        }

        if ((propNameAndValues.length % 2) != 0) {
            throw new IllegalArgumentException("The length of property name/value array must be even: " + propNameAndValues.length);
        }

        final Map<String, Object> props = new LinkedHashMap<>(propNameAndValues.length / 2);

        for (int i = 0, len = propNameAndValues.length; i < len; i += 2) {
            props.put(N.stringOf(propNameAndValues[i]), propNameAndValues[i + 1]);
        }

        return props;
    }

}
