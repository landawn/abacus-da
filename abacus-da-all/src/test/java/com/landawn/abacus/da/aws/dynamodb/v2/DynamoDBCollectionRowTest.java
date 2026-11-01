package com.landawn.abacus.da.aws.dynamodb.v2;

import static com.landawn.abacus.da.AllocationAssertions.assertLinearAllocation;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
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

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;

class DynamoDBCollectionRowTest extends DynamoDBExecutorTestSupport {
    @Test
    void boundedQueueRowsAreConstructedWithRoomForEveryAttribute() {
        final Map<String, AttributeValue> item = row(3);
        item.put("column1", AttributeValue.fromN("2"));
        final GetItemRequest request = GetItemRequest.builder().tableName("test").build();
        when(mockDynamoDbClient.getItem(request)).thenReturn(GetItemResponse.builder().item(item).build());

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
        item.put("missing", AttributeValue.fromNul(true));
        final GetItemRequest request = GetItemRequest.builder().tableName("test").build();
        when(mockDynamoDbClient.getItem(request)).thenReturn(GetItemResponse.builder().item(item).build());

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
        final byte[] bytes = { 1, 2, 3 };
        final Map<String, AttributeValue> item = Map.of("binary", AttributeValue.fromB(software.amazon.awssdk.core.SdkBytes.fromByteArray(bytes)));
        final Object value = DynamoDBExecutor.toList(List.of(item), CopyOnWriteArrayList.class).getFirst().getFirst();
        assertArrayEquals(bytes, assertInstanceOf(byte[].class, value));
        final Object queued = DynamoDBExecutor.toList(List.of(item), ArrayBlockingQueue.class).getFirst().element();
        assertArrayEquals(bytes, assertInstanceOf(byte[].class, queued));
    }

    @Test
    void boundedQueuesContinueToRejectNullElements() {
        final Map<String, AttributeValue> item = Map.of("missing", AttributeValue.fromNul(true));
        assertThrows(NullPointerException.class, () -> DynamoDBExecutor.toList(List.of(item), ArrayBlockingQueue.class));
    }

    @Test
    void copyOnWriteRowAllocationGrowsLinearlyWithAttributeCount() {
        final List<Map<String, AttributeValue>> small = List.of(row(512));
        final List<Map<String, AttributeValue>> large = List.of(row(2048));
        assertEquals(Collections.nCopies(2048, "value"), DynamoDBExecutor.toList(large, CopyOnWriteArrayList.class).getFirst());
        assertLinearAllocation("DynamoDB v2 CopyOnWriteArrayList rows", () -> DynamoDBExecutor.toList(small, CopyOnWriteArrayList.class),
                () -> DynamoDBExecutor.toList(large, CopyOnWriteArrayList.class));
    }

    private static Map<String, AttributeValue> row(final int size) {
        final Map<String, AttributeValue> item = new LinkedHashMap<>();
        for (int i = 0; i < size; i++) {
            item.put("column" + i, AttributeValue.fromS("value"));
        }
        return item;
    }
}
