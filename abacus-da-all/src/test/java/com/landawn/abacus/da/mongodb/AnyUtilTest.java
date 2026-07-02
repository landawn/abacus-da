package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.da.aws.AnyUtil;

/**
 * Tests for the AnyUtil utility class.
 *
 * <p>Covers {@link AnyUtil#asProps(Object[])} including normal pairs, null,
 * empty, odd-length error, non-string keys, and null values; plus a sanity test
 * that the class itself is non-instantiable.</p>
 */
public class AnyUtilTest extends TestBase {

    // -- asProps normal cases --

    @Test
    public void testasPropsWithKeyValuePairs() {
        Object[] input = new Object[] { "name", "John", "age", 30 };
        Map<String, Object> props = AnyUtil.asProps(input);

        assertNotNull(props);
        assertEquals(2, props.size());
        assertEquals("John", props.get("name"));
        assertEquals(30, props.get("age"));
    }

    @Test
    public void testasPropsPreservesInsertionOrder() {
        // Implementation uses LinkedHashMap so insertion order is preserved
        Object[] input = new Object[] { "z", 1, "a", 2, "m", 3 };
        Map<String, Object> props = AnyUtil.asProps(input);

        assertTrue(props instanceof LinkedHashMap);
        assertEquals(3, props.size());

        Object[] keys = props.keySet().toArray();
        assertEquals("z", keys[0]);
        assertEquals("a", keys[1]);
        assertEquals("m", keys[2]);
    }

    @Test
    public void testasPropsWithEmptyArray() {
        Map<String, Object> props = AnyUtil.asProps(new Object[0]);
        assertNotNull(props);
        assertEquals(0, props.size());
    }

    @Test
    public void testasPropsWithNullArray() {
        // null array returns an empty LinkedHashMap (not null)
        Map<String, Object> props = AnyUtil.asProps(null);
        assertNotNull(props);
        assertEquals(0, props.size());
        assertTrue(props instanceof LinkedHashMap);
    }

    // -- asProps error and edge cases --

    @Test
    public void testasPropsWithOddLengthThrows() {
        Object[] input = new Object[] { "name", "John", "age" };
        assertThrows(IllegalArgumentException.class, () -> AnyUtil.asProps(input));
    }

    @Test
    public void testasPropsWithNullValue() {
        // null value is allowed and preserved
        Object[] input = new Object[] { "key", null };
        Map<String, Object> props = AnyUtil.asProps(input);

        assertEquals(1, props.size());
        assertTrue(props.containsKey("key"));
        assertNull(props.get("key"));
    }

    @Test
    public void testasPropsWithNonStringKeyThrows() {
        // Property names must be Strings; a non-String name (here an Integer at index 0) is rejected.
        Object[] input = new Object[] { 100, "hundred", true, "yes" };
        assertThrows(IllegalArgumentException.class, () -> AnyUtil.asProps(input));
    }

    @Test
    public void testasPropsWithMixedValueTypes() {
        Object[] input = new Object[] { "str", "abc", "int", 42, "bool", true, "dbl", 3.14 };
        Map<String, Object> props = AnyUtil.asProps(input);

        assertEquals(4, props.size());
        assertEquals("abc", props.get("str"));
        assertEquals(42, props.get("int"));
        assertEquals(true, props.get("bool"));
        assertEquals(3.14, props.get("dbl"));
    }

    @Test
    public void testasPropsSingleEntry() {
        Object[] input = new Object[] { "only", "value" };
        Map<String, Object> props = AnyUtil.asProps(input);

        assertEquals(1, props.size());
        assertEquals("value", props.get("only"));
    }

    // -- class structural tests --

    @Test
    public void testClassIsFinal() {
        assertTrue(Modifier.isFinal(AnyUtil.class.getModifiers()));
    }

    @Test
    public void testPrivateConstructorIsInaccessibleByDefault() throws Exception {
        Constructor<AnyUtil> ctor = AnyUtil.class.getDeclaredConstructor();
        assertTrue(Modifier.isPrivate(ctor.getModifiers()));
        // Verify it is still callable via reflection so coverage of the empty constructor is captured
        ctor.setAccessible(true);
        AnyUtil instance = ctor.newInstance();
        assertNotNull(instance);
    }
}
