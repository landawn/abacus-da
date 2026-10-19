package com.landawn.abacus.da.aws.dynamodb;

import static com.landawn.abacus.da.AllocationAssertions.assertLinearAllocation;
import static com.landawn.abacus.da.AllocationAssertions.assumeAllocationCountersAvailable;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class DynamoDBRecursiveJsonAllocationTest {
    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void unformattedGrowingGenericCyclesAllocateLinearly(final boolean sdkV2) {
        assumeAllocationCountersAvailable();
        final Supplier<Root> small = reader(12, sdkV2);
        final Supplier<Root> large = reader(48, sdkV2);
        assertDepth(small.get(), 12);
        assertDepth(large.get(), 48);

        // Repeatedly parsing every remaining subtree makes allocations grow quadratically with depth.
        // Prebuild the payloads and warm the type caches so this measures the conversion itself.
        assertLinearAllocation("DynamoDB " + (sdkV2 ? "v2" : "v1") + " unformatted recursive JSON", small, large);
    }

    private static Supplier<Root> reader(final int depth, final boolean sdkV2) {
        String node = "null";
        for (int i = depth - 1; i >= 0; i--) {
            node = "{\"index\":" + i + ",\"child\":" + node + "}";
        }
        final String json = "[" + node + "]";
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
