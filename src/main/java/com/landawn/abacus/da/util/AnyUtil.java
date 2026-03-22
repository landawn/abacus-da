package com.landawn.abacus.da.util;

import java.util.LinkedHashMap;
import java.util.Map;

import com.landawn.abacus.util.N;

/**
 * Utility class providing general-purpose helper methods for the data access layer.
 */
public final class AnyUtil {
    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private AnyUtil() {
        // utility class
    }

    /**
     * Converts an object array of key-value pairs to a map.
     *
     * <p>The input array is interpreted as alternating key-value pairs:
     * {@code [key1, value1, key2, value2, ...]}.</p>
     *
     * @param propNameAndValues the alternating property name and value pairs
     * @return a map containing the key-value pairs, or an empty map if input is {@code null}
     * @throws IllegalArgumentException if the array length is odd
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
