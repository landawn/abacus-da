package com.landawn.abacus.da.aws.dynamodb.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.LinkedHashMap;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

/**
 * Pins the SDK v2 {@link DynamoDBExecutor} behaviors whose Javadoc was corrected or completed in the 2026-11-02 review.
 */
public class DynamoDBExecutorReview20261102Test {

    /**
     * {@code updateItem(String, Map, Map[, String])} documented a null {@code attributeUpdates} as failing service-side
     * validation, but DynamoDB (verified against DynamoDB Local) accepts an UpdateItem without updates and creates a key-only
     * item. The executor forwards such a request unchanged, without a client-side check.
     */
    @Test
    public void updateItem_nullOrEmptyAttributeUpdates_isForwardedWithoutUpdates() {
        final DynamoDbClient client = mock(DynamoDbClient.class);
        final UpdateItemResponse response = UpdateItemResponse.builder().build();
        when(client.updateItem(any(UpdateItemRequest.class))).thenReturn(response);
        final DynamoDBExecutor executor = new DynamoDBExecutor(client);
        final Map<String, AttributeValue> key = DynamoDBExecutor.asKey("pk", "a");

        assertSame(response, executor.updateItem("T", key, null));
        assertSame(response, executor.updateItem("T", key, new HashMap<String, AttributeValueUpdate>(), "ALL_NEW"));

        final ArgumentCaptor<UpdateItemRequest> captor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(client, times(2)).updateItem(captor.capture());

        final UpdateItemRequest first = captor.getAllValues().get(0);
        assertEquals("T", first.tableName());
        assertEquals(key, first.key());
        assertFalse(first.hasAttributeUpdates());
        assertNull(first.updateExpression());

        final UpdateItemRequest second = captor.getAllValues().get(1);
        assertEquals(0, second.attributeUpdates().size());
        assertEquals("ALL_NEW", second.returnValuesAsString());
    }

    /**
     * The toAttributeValue Javadoc now states that AtomicInteger/AtomicLong are written as S text (abacus-common does not type
     * them as numbers) and that SdkBytes and AttributeValue values are written as their toString() text.
     */
    @Test
    public void toAttributeValue_typesWrittenAsText() {
        assertEquals(AttributeValue.fromS("5"), DynamoDBExecutor.toAttributeValue(new AtomicInteger(5)));
        assertEquals(AttributeValue.fromS("7"), DynamoDBExecutor.toAttributeValue(new AtomicLong(7)));

        final SdkBytes bytes = SdkBytes.fromUtf8String("ab");
        assertEquals(AttributeValue.fromS(bytes.toString()), DynamoDBExecutor.toAttributeValue(bytes));
        assertEquals(AttributeValue.fromS("SdkBytes(bytes=0x6162)"), DynamoDBExecutor.toAttributeValue(bytes));
        // The documented alternative is binary.
        assertEquals(AttributeValue.fromB(bytes), DynamoDBExecutor.toAttributeValue(bytes.asByteArray()));

        final AttributeValue number = AttributeValue.fromN("1");
        assertEquals(AttributeValue.fromS(number.toString()), DynamoDBExecutor.toAttributeValue(number));
    }

    /**
     * A NULL attribute read into a row map that rejects null values used to surface as a bare NullPointerException from
     * {@code Map.put}; it is now reported as an IllegalArgumentException naming the attribute, like nested containers.
     */
    @Test
    public void toMap_rowMapRejectingNullValue_isReportedAsIllegalArgument() {
        final Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("a", AttributeValue.fromS("x"));
        item.put("gone", AttributeValue.builder().nul(true).build());

        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toMap(item, ConcurrentHashMap::new));
        assertTrue(e.getMessage().contains("gone"), e.getMessage());
        assertTrue(e.getCause() instanceof NullPointerException);

        // A row map that accepts nulls is filled in item order, null included.
        final Map<String, Object> row = DynamoDBExecutor.toMap(item, LinkedHashMap::new);
        assertEquals(Arrays.asList("a", "gone"), new ArrayList<>(row.keySet()));
        assertNull(row.get("gone"));
    }
}
