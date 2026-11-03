package com.landawn.abacus.da.aws.dynamodb;

import static com.landawn.abacus.da.AllocationAssertions.assertLinearAllocation;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;

class DynamoDBCollectionRowTest extends DynamoDBExecutorTestSupport {
    @Test
    void boundedQueueRowsAreConstructedWithRoomForEveryAttribute() {
        final Map<String, AttributeValue> item = row(3);
        item.put("column1", new AttributeValue().withN("2"));
        final GetItemRequest request = new GetItemRequest().withTableName("test");
        when(mockDynamoDBClient.getItem(request)).thenReturn(new GetItemResult().withItem(item));

        final ArrayBlockingQueue<?> actual = executor.getItem(request, ArrayBlockingQueue.class);
        assertEquals(List.of("value", "2", "value"), new ArrayList<>(actual));
        assertEquals(0, actual.remainingCapacity());
        assertEquals(List.of("value", "2", "value"), new ArrayList<>(DynamoDBExecutor.toList(List.of(item), ArrayBlockingQueue.class).getFirst()));
    }

    @Test
    void emptyProjectedRowsStillProduceEmptyWritableQueues() {
        final ArrayBlockingQueue<?> actual = DynamoDBExecutor.toList(List.of(Map.<String, AttributeValue> of()), ArrayBlockingQueue.class).getFirst();
        assertTrue(actual.isEmpty());
        assertEquals(1, actual.remainingCapacity());
    }

    @Test
    void copyOnWriteRowsPreserveOrderDuplicatesAndNulls() {
        final Map<String, AttributeValue> item = row(2);
        item.put("missing", new AttributeValue().withNULL(true));
        final GetItemRequest request = new GetItemRequest().withTableName("test");
        when(mockDynamoDBClient.getItem(request)).thenReturn(new GetItemResult().withItem(item));

        final CopyOnWriteArrayList<?> actual = executor.getItem(request, CopyOnWriteArrayList.class);
        assertInstanceOf(CopyOnWriteArrayList.class, actual);
        assertEquals(Arrays.asList("value", "value", null), actual);
        assertNull(actual.get(2));
        assertEquals(actual, DynamoDBExecutor.toList(List.of(item), CopyOnWriteArrayList.class).getFirst());
        actual.clear();
        assertEquals(3, item.size());
    }

    @Test
    void collectionRowsKeepNativeBinaryValues() {
        final java.nio.ByteBuffer bytes = java.nio.ByteBuffer.wrap(new byte[] { 9, 1, 2, 3 });
        bytes.position(1);
        final Map<String, AttributeValue> item = Map.of("binary", new AttributeValue().withB(bytes));
        final Object value = DynamoDBExecutor.toList(List.of(item), CopyOnWriteArrayList.class).getFirst().getFirst();
        assertSame(bytes, value);
        assertSame(bytes, DynamoDBExecutor.toList(List.of(item), ArrayBlockingQueue.class).getFirst().element());
        assertEquals(1, bytes.position());
    }

    @Test
    void boundedQueuesContinueToRejectNullElements() {
        final Map<String, AttributeValue> item = Map.of("missing", new AttributeValue().withNULL(true));
        // The queue's NullPointerException is reported as an IllegalArgumentException naming the attribute (2026-11-02).
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toList(List.of(item), ArrayBlockingQueue.class));
        assertTrue(e.getMessage().startsWith("Attribute (missing, null)"), e.getMessage());
        assertInstanceOf(NullPointerException.class, e.getCause());
    }

    @Test
    void copyOnWriteRowAllocationGrowsLinearlyWithAttributeCount() {
        final List<Map<String, AttributeValue>> small = List.of(row(512));
        final List<Map<String, AttributeValue>> large = List.of(row(2048));
        assertEquals(Collections.nCopies(2048, "value"), DynamoDBExecutor.toList(large, CopyOnWriteArrayList.class).getFirst());
        assertLinearAllocation("DynamoDB v1 CopyOnWriteArrayList rows", () -> DynamoDBExecutor.toList(small, CopyOnWriteArrayList.class),
                () -> DynamoDBExecutor.toList(large, CopyOnWriteArrayList.class));
    }

    private static Map<String, AttributeValue> row(final int size) {
        final Map<String, AttributeValue> item = new LinkedHashMap<>();
        for (int i = 0; i < size; i++) {
            item.put("column" + i, new AttributeValue().withS("value"));
        }
        return item;
    }
}
