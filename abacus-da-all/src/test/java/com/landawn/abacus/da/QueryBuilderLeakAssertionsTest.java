package com.landawn.abacus.da;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import com.landawn.abacus.query.Dsl;
import com.landawn.abacus.query.SqlBuilder;

public class QueryBuilderLeakAssertionsTest extends TestBase {

    @Test
    public void counterIsTheLiveAbacusQueryCounter() {
        final AtomicInteger counter = QueryBuilderLeakAssertions.activeStringBuilderCounter();
        assertNotNull(counter);
        assertSame(counter, QueryBuilderLeakAssertions.activeStringBuilderCounter());

        final int before = counter.get();
        final SqlBuilder builder = Dsl.PSC.select("a");
        assertEquals(before + 1, counter.get());
        builder.from("t").build();
        assertEquals(before, counter.get());
    }

    @Test
    public void rejectedCallWithoutLeakPassesAndReturnsTheFailure() {
        final IllegalArgumentException thrown = QueryBuilderLeakAssertions.assertRejectedWithoutBuilderLeak(IllegalArgumentException.class,
                () -> Dsl.PSC.select(""), "a factory rejection must not leak");
        assertNotNull(thrown);
    }

    @Test
    public void abandonedBuilderIsReportedAsLeak() {
        final SqlBuilder[] leaked = new SqlBuilder[1];

        try {
            final AssertionFailedError failure = assertThrows(AssertionFailedError.class,
                    () -> QueryBuilderLeakAssertions.assertRejectedWithoutBuilderLeak(IllegalStateException.class, () -> {
                        leaked[0] = Dsl.PSC.select("a");
                        throw new IllegalStateException("abandons its builder");
                    }, "leak detected"));
            assertTrue(failure.getMessage().startsWith("leak detected"), failure.getMessage());
        } finally {
            if (leaked[0] != null) {
                leaked[0].from("t").build();
            }
        }
    }

    @Test
    public void unexpectedOutcomeStillFails() {
        assertThrows(AssertionFailedError.class,
                () -> QueryBuilderLeakAssertions.assertRejectedWithoutBuilderLeak(IllegalArgumentException.class, () -> {
                }, "no exception"));
    }
}
