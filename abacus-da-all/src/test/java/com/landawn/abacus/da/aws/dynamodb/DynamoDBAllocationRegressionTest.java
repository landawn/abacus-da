package com.landawn.abacus.da.aws.dynamodb;

import static com.landawn.abacus.da.AllocationAssertions.assertLinearAllocation;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

public class DynamoDBAllocationRegressionTest {
    @Test
    void legacyTopLevelCopyOnWriteListAvoidsQuadraticCopies() {
        final Supplier<Holder> small = legacyInput(512);
        final Supplier<Holder> large = legacyInput(2048);
        assertEquals(Collections.nCopies(512, "value"), small.get().getValues());
        assertEquals(Collections.nCopies(2048, "value"), large.get().getValues());
        assertLinearAllocation("DynamoDB v1 CopyOnWriteArrayList<Object>", small, large);
    }

    @Test
    void v2TopLevelCopyOnWriteListAvoidsQuadraticCopies() {
        final Supplier<Holder> small = v2Input(512);
        final Supplier<Holder> large = v2Input(2048);
        assertEquals(Collections.nCopies(512, "value"), small.get().getValues());
        assertEquals(Collections.nCopies(2048, "value"), large.get().getValues());
        assertLinearAllocation("DynamoDB v2 CopyOnWriteArrayList<Object>", small, large);
    }

    private static Supplier<Holder> legacyInput(final int size) {
        final Map<String, AttributeValue> item = Map.of("values", new AttributeValue().withL(Collections.nCopies(size, new AttributeValue().withS("value"))));
        return () -> DynamoDBExecutor.toEntity(item, Holder.class);
    }

    private static Supplier<Holder> v2Input(final int size) {
        final var item = Map.of("values", software.amazon.awssdk.services.dynamodb.model.AttributeValue
                .fromL(Collections.nCopies(size, software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS("value"))));
        return () -> com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(item, Holder.class);
    }

    public static class Holder {
        private CopyOnWriteArrayList<Object> values;

        public CopyOnWriteArrayList<Object> getValues() {
            return values;
        }

        public void setValues(final CopyOnWriteArrayList<Object> value) {
            values = value;
        }
    }
}
