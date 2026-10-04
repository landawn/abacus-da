package com.landawn.abacus.da;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.function.Executable;

import com.landawn.abacus.query.AbstractQueryBuilder;

/**
 * Shared leak detector for executor-owned and Dsl-created query builders.
 *
 * <p>abacus-query counts every live builder that holds a pooled {@code StringBuilder} in
 * {@code AbstractQueryBuilder.activeStringBuilderCounter}; {@code build()} decrements it. A rejected call that abandons a
 * builder without building it leaves the counter permanently raised, which is what these assertions detect.</p>
 */
public final class QueryBuilderLeakAssertions {

    static final String COUNTER_FIELD_NAME = "activeStringBuilderCounter";

    private QueryBuilderLeakAssertions() {
    }

    /**
     * Returns abacus-query's live pooled-builder counter.
     *
     * @return the counter
     * @throws AssertionError if the field no longer exists or is inaccessible, so the test fails (rather than errors) with
     *         an actionable message
     */
    public static AtomicInteger activeStringBuilderCounter() {
        try {
            final Field field = AbstractQueryBuilder.class.getDeclaredField(COUNTER_FIELD_NAME);
            field.setAccessible(true);
            return (AtomicInteger) field.get(null);
        } catch (final ReflectiveOperationException e) {
            throw new AssertionError(COUNTER_FIELD_NAME + " not found on AbstractQueryBuilder — the abacus-query field was renamed or removed; update the leak detector",
                    e);
        }
    }

    /**
     * Asserts that {@code action} throws {@code expectedType} and leaves the active-builder count unchanged.
     *
     * @param <T> the expected exception type
     * @param expectedType the expected exception type
     * @param action the rejected call
     * @param message the failure message used when the count changed
     * @return the thrown exception
     */
    public static <T extends Throwable> T assertRejectedWithoutBuilderLeak(final Class<T> expectedType, final Executable action, final String message) {
        final AtomicInteger counter = activeStringBuilderCounter();
        final int before = counter.get();

        final T thrown = assertThrows(expectedType, action);
        assertEquals(before, counter.get(), message);

        return thrown;
    }
}
