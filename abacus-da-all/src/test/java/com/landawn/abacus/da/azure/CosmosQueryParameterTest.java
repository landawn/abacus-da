package com.landawn.abacus.da.azure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class CosmosQueryParameterTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("literalCases")
    void rewrite_preservesLiteralsAndBindsPlaceholders(final String scenario, final String query, final int count, final String expected) throws Exception {
        assertEquals(expected, rewriteMethod().invoke(null, query, count));
    }

    private static Stream<Arguments> literalCases() {
        return Stream.of(Arguments.of("NoPlaceholders", "SELECT * FROM c WHERE c.id = 'static'", 0, "SELECT * FROM c WHERE c.id = 'static'"),
                Arguments.of("MultiPlaceholder", "SELECT * FROM c WHERE c.a = ? AND c.b = ? AND c.c = ?", 3,
                        "SELECT * FROM c WHERE c.a = @p0 AND c.b = @p1 AND c.c = @p2"),
                Arguments.of("QuestionMarkInsideSingleQuotes", "SELECT * FROM c WHERE c.name = 'a?b' AND c.id = ?", 1,
                        "SELECT * FROM c WHERE c.name = 'a?b' AND c.id = @p0"),
                Arguments.of("QuestionMarkInsideDoubleQuotes", "SELECT * FROM c WHERE c.name = \"a?b\" AND c.id = ?", 1,
                        "SELECT * FROM c WHERE c.name = \"a?b\" AND c.id = @p0"),
                Arguments.of("BackslashEscapedQuote", "SELECT * FROM c WHERE c.name = \"a\\\"?b\" AND c.id = ?", 1,
                        "SELECT * FROM c WHERE c.name = \"a\\\"?b\" AND c.id = @p0"),
                Arguments.of("EscapedSingleQuote", "SELECT * FROM c WHERE c.name = 'O''Brien' AND c.id = ?", 1,
                        "SELECT * FROM c WHERE c.name = 'O''Brien' AND c.id = @p0"));
    }

    @ParameterizedTest(name = "{0} with {1} parameters")
    @MethodSource("mismatchedCounts")
    void rewrite_rejectsParameterCountMismatch(final String query, final int count, final String message) throws Exception {
        final Method method = rewriteMethod();
        final InvocationTargetException wrapper = assertThrows(InvocationTargetException.class, () -> method.invoke(null, query, count));
        final IllegalArgumentException error = assertInstanceOf(IllegalArgumentException.class, wrapper.getCause());
        assertTrue(error.getMessage().contains(message), error.getMessage());
    }

    private static Stream<Arguments> mismatchedCounts() {
        return Stream.of(Arguments.of("SELECT * FROM c WHERE c.id = ? AND c.name = ?", 1, "expected 1 placeholders but found 2"),
                Arguments.of("SELECT * FROM c WHERE a=? AND b=? AND c=?", 2, "expected 2 placeholders but found 3"),
                Arguments.of("SELECT * FROM c", 1, "expected 1 placeholders but found 0"));
    }

    private static Method rewriteMethod() throws NoSuchMethodException {
        final Method method = CosmosContainerExecutor.class.getDeclaredMethod("rewritePositionalParameters", String.class, int.class);
        method.setAccessible(true);
        return method;
    }
}
