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


/**
 * Tests for the AnyUtil utility class.
 *
 * <p>Covers {@link AnyUtil#array2Props(Object[])} including normal pairs, null,
 * empty, odd-length error, non-string keys, and null values; plus a sanity test
 * that the class itself is non-instantiable.</p>
 */
public class AnyUtilTest extends TestBase {

    // -- array2Props normal cases --

    @Test
    public void testArray2PropsWithKeyValuePairs() {
        Object[] input = new Object[] { "name", "John", "age", 30 };
        Map<String, Object> props = AnyUtil.array2Props(input);

        assertNotNull(props);
        assertEquals(2, props.size());
        assertEquals("John", props.get("name"));
        assertEquals(30, props.get("age"));
    }

    @Test
    public void testArray2PropsPreservesInsertionOrder() {
        // Implementation uses LinkedHashMap so insertion order is preserved
        Object[] input = new Object[] { "z", 1, "a", 2, "m", 3 };
        Map<String, Object> props = AnyUtil.array2Props(input);

        assertTrue(props instanceof LinkedHashMap);
        assertEquals(3, props.size());

        Object[] keys = props.keySet().toArray();
        assertEquals("z", keys[0]);
        assertEquals("a", keys[1]);
        assertEquals("m", keys[2]);
    }

    @Test
    public void testArray2PropsWithEmptyArray() {
        Map<String, Object> props = AnyUtil.array2Props(new Object[0]);
        assertNotNull(props);
        assertEquals(0, props.size());
    }

    @Test
    public void testArray2PropsWithNullArray() {
        // null array returns an empty LinkedHashMap (not null)
        Map<String, Object> props = AnyUtil.array2Props(null);
        assertNotNull(props);
        assertEquals(0, props.size());
        assertTrue(props instanceof LinkedHashMap);
    }

    // -- array2Props error and edge cases --

    @Test
    public void testArray2PropsWithOddLengthThrows() {
        Object[] input = new Object[] { "name", "John", "age" };
        assertThrows(IllegalArgumentException.class, () -> AnyUtil.array2Props(input));
    }

    @Test
    public void testArray2PropsWithNullValue() {
        // null value is allowed and preserved
        Object[] input = new Object[] { "key", null };
        Map<String, Object> props = AnyUtil.array2Props(input);

        assertEquals(1, props.size());
        assertTrue(props.containsKey("key"));
        assertNull(props.get("key"));
    }

    @Test
    public void testArray2PropsWithNonStringKeyConvertedToString() {
        // Non-string keys are converted via N.stringOf
        Object[] input = new Object[] { 100, "hundred", true, "yes" };
        Map<String, Object> props = AnyUtil.array2Props(input);

        assertEquals(2, props.size());
        assertEquals("hundred", props.get("100"));
        assertEquals("yes", props.get("true"));
    }

    @Test
    public void testArray2PropsWithMixedValueTypes() {
        Object[] input = new Object[] { "str", "abc", "int", 42, "bool", true, "dbl", 3.14 };
        Map<String, Object> props = AnyUtil.array2Props(input);

        assertEquals(4, props.size());
        assertEquals("abc", props.get("str"));
        assertEquals(42, props.get("int"));
        assertEquals(true, props.get("bool"));
        assertEquals(3.14, props.get("dbl"));
    }

    @Test
    public void testArray2PropsSingleEntry() {
        Object[] input = new Object[] { "only", "value" };
        Map<String, Object> props = AnyUtil.array2Props(input);

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
