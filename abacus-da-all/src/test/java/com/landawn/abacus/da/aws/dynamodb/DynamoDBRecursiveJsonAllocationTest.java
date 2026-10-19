package com.landawn.abacus.da.aws.dynamodb;

import static com.landawn.abacus.da.AllocationAssertions.assertAllocationOverhead;
import static com.landawn.abacus.da.AllocationAssertions.assumeAllocationCountersAvailable;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.landawn.abacus.util.N;

public class DynamoDBRecursiveJsonAllocationTest {
    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void unformattedGrowingGenericCyclesDoNotReparseSubtrees(final boolean sdkV2) {
        assumeAllocationCountersAvailable();
        for (final int depth : new int[] { 12, 48 }) {
            final String json = recursiveJson(depth);
            final String rootJson = "{\"leaves\":" + json + "}";
            final Supplier<Root> baseline = () -> N.fromJson(rootJson, Root.class);
            final Supplier<Root> mapped = reader(json, sdkV2);
            assertDepth(baseline.get(), depth);
            assertDepth(mapped.get(), depth);

            // Class initialization order can make these growing generic types collide in the shared JSON
            // metadata cache. Type comparisons then allocate even without repeated subtree parsing, so an
            // absolute linear-growth assertion is unreliable in a full suite. Compare the same typed payload
            // with direct JSON parsing; allow only a fixed 1 KiB for the outer DynamoDB item mapping.
            assertAllocationOverhead("DynamoDB " + (sdkV2 ? "v2" : "v1") + " unformatted recursive JSON at depth " + depth, baseline, mapped, 1024);
        }
    }

    private static String recursiveJson(final int depth) {
        String node = "null";
        for (int i = depth - 1; i >= 0; i--) {
            node = "{\"index\":" + i + ",\"child\":" + node + "}";
        }
        return "[" + node + "]";
    }

    private static Supplier<Root> reader(final String json, final boolean sdkV2) {
        if (sdkV2) {
            final var item = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toItem(Map.of("leaves", json));
            return () -> com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(item, Root.class);
        }
        final var item = DynamoDBExecutor.toItem(Map.of("leaves", json));
        return () -> DynamoDBExecutor.toEntity(item, Root.class);
    }

    private static void assertDepth(final Root root, final int expected) {
        int count = 0;
        for (Growing<?> node = root.leaves.get(0); node != null; node = node.child) {
            assertEquals(count++, node.index);
        }
        assertEquals(expected, count);
    }

    public static class Root {
        public List<Growing<Long>> leaves;
    }

    public static class Growing<T> {
        public int index;
        public T value;
        public Growing<List<T>> child;
    }
}
