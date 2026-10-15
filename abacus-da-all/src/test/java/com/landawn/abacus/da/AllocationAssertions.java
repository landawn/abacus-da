package com.landawn.abacus.da;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.lang.management.ManagementFactory;
import java.util.function.Supplier;

/** Allocation checks for conversions whose output must grow linearly with the input. */
public final class AllocationAssertions {
    private static volatile Object sink;

    private AllocationAssertions() {
    }

    /** Check before grouped assertions, which would otherwise aggregate an assumption abort as a failure. */
    public static void assumeAllocationCountersAvailable() {
        allocationCounter();
    }

    /**
     * Compares allocations for prebuilt inputs whose sizes differ by a factor of four.
     * The generous bound tolerates fixed costs and JIT variation while detecting repeated copying of all prior entries.
     */
    public static void assertLinearAllocation(final String description, final Supplier<?> small, final Supplier<?> large) {
        final com.sun.management.ThreadMXBean counter = allocationCounter();

        try {
            // Do not include input construction, class loading, or mock invocation recording in the measurements.
            for (int i = 0; i < 12; i++) {
                sink = small.get();
                sink = large.get();
            }

            final long smallBytes = minimumAllocation(counter, small);
            final long largeBytes = minimumAllocation(counter, large);
            System.out.printf("%s: smaller input = %d bytes; 4x input = %d bytes%n", description, smallBytes, largeBytes);
            assertTrue(smallBytes > 0 && largeBytes > 0, "Allocation counters must return positive measurements");
            assertTrue(largeBytes <= smallBytes * 6, () -> description + " allocates " + largeBytes + " bytes for 4x the input, versus " + smallBytes
                    + " bytes for the smaller input; repeated copying should not cause quadratic allocation");
        } finally {
            sink = null;
        }
    }

    private static com.sun.management.ThreadMXBean allocationCounter() {
        final java.lang.management.ThreadMXBean platformBean = ManagementFactory.getThreadMXBean();
        assumeTrue(platformBean instanceof com.sun.management.ThreadMXBean, "The VM must expose per-thread allocation counters");
        final com.sun.management.ThreadMXBean counter = (com.sun.management.ThreadMXBean) platformBean;
        assumeTrue(counter.isThreadAllocatedMemorySupported() && counter.isThreadAllocatedMemoryEnabled(), "Per-thread allocation counters must be enabled");
        // The counter reports -1 for a virtual thread, which would otherwise fail the positive-measurement check.
        assumeTrue(counter.getThreadAllocatedBytes(Thread.currentThread().threadId()) >= 0, "The current thread must expose an allocation counter");
        return counter;
    }

    private static long minimumAllocation(final com.sun.management.ThreadMXBean counter, final Supplier<?> operation) {
        final long threadId = Thread.currentThread().threadId();
        long minimum = Long.MAX_VALUE;
        for (int i = 0; i < 5; i++) {
            final long before = counter.getThreadAllocatedBytes(threadId);
            sink = operation.get();
            minimum = Math.min(minimum, counter.getThreadAllocatedBytes(threadId) - before);
        }
        return minimum;
    }
}
