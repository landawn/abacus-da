package com.landawn.abacus.da.aws.dynamodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.LinkedHashMap;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;

/**
 * Pins the SDK v1 behaviors that the 2026-11-02 review documented on {@link DynamoDBExecutor} (verified against DynamoDB
 * Local as well): which values {@code toAttributeValue} writes as {@code S} text, and that {@code updateItem} passes a
 * {@code null} or empty update map to the client instead of rejecting it locally.
 */
public class DynamoDBExecutorReview20261102Test extends DynamoDBExecutorTestSupport {

    @Test
    public void testToAttributeValue_AtomicNumbersAreText() {
        final AttributeValue atomicInt = DynamoDBExecutor.toAttributeValue(new AtomicInteger(5));
        final AttributeValue atomicLong = DynamoDBExecutor.toAttributeValue(new AtomicLong(7));

        assertEquals("5", atomicInt.getS());
        assertNull(atomicInt.getN());
        assertEquals("7", atomicLong.getS());
        assertNull(atomicLong.getN());
    }

    @Test
    public void testToAttributeValue_WrappedNumberIsText() {
        final AttributeValue wrapped = DynamoDBExecutor.toAttributeValue(Optional.of(5));

        assertEquals("5", wrapped.getS());
        assertNull(wrapped.getN());
        // A bare number stays N.
        assertEquals("5", DynamoDBExecutor.toAttributeValue(5).getN());
    }

    @Test
    public void testUpdateItem_NullUpdatesPassedToClient() {
        final Map<String, AttributeValue> key = DynamoDBExecutor.asKey("id", "a");
        final UpdateItemResult result = new UpdateItemResult();
        when(mockDynamoDBClient.updateItem(eq("t"), eq(key), isNull())).thenReturn(result);

        assertSame(result, executor.updateItem("t", key, null));
        verify(mockDynamoDBClient).updateItem(eq("t"), eq(key), isNull());
    }

    @Test
    public void testUpdateItem_EmptyUpdatesWithReturnValuesPassedToClient() {
        final Map<String, AttributeValue> key = DynamoDBExecutor.asKey("id", "a");
        final Map<String, AttributeValueUpdate> updates = new HashMap<>();
        final UpdateItemResult result = new UpdateItemResult();
        when(mockDynamoDBClient.updateItem(eq("t"), eq(key), eq(updates), eq("ALL_NEW"))).thenReturn(result);

        assertSame(result, executor.updateItem("t", key, updates, "ALL_NEW"));
        verify(mockDynamoDBClient).updateItem(eq("t"), eq(key), any(), eq("ALL_NEW"));
    }

    /**
     * A NULL attribute read into a row map that rejects null values used to surface as a bare NullPointerException from
     * {@code Map.put}; it is now reported as an IllegalArgumentException naming the attribute, like nested containers.
     */
    @Test
    public void toMap_rowMapRejectingNullValue_isReportedAsIllegalArgument() {
        final Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("a", new AttributeValue().withS("x"));
        item.put("gone", new AttributeValue().withNULL(true));

        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toMap(item, ConcurrentHashMap::new));
        assertTrue(e.getMessage().contains("gone"), e.getMessage());
        assertTrue(e.getCause() instanceof NullPointerException);

        // A row map that accepts nulls is filled in item order, null included.
        final Map<String, Object> row = DynamoDBExecutor.toMap(item, LinkedHashMap::new);
        assertEquals(Arrays.asList("a", "gone"), new ArrayList<>(row.keySet()));
        assertNull(row.get("gone"));
    }

    /**
     * A NULL attribute read into a collection row that rejects null values (an ArrayBlockingQueue) used to surface as a bare
     * NullPointerException from {@code Collection.add}; it is now an IllegalArgumentException naming the attribute.
     */
    @Test
    public void getItem_queueRowRejectingNullValue_isReportedAsIllegalArgument() {
        final Map<String, AttributeValue> key = DynamoDBExecutor.asKey("id", "1");
        final Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("id", new AttributeValue().withS("1"));
        item.put("gone", new AttributeValue().withNULL(true));
        when(mockDynamoDBClient.getItem("t", key)).thenReturn(new GetItemResult().withItem(item));

        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> executor.getItem("t", key, ArrayBlockingQueue.class));
        assertEquals("Attribute (gone, null) is not accepted by the row collection " + ArrayBlockingQueue.class.getName(), e.getMessage());
        assertTrue(e.getCause() instanceof NullPointerException);

        assertEquals(Arrays.asList("1", null), executor.getItem("t", key, ArrayList.class));
    }
}
