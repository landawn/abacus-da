package com.landawn.abacus.da.aws.dynamodb.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;

import com.landawn.abacus.util.NamingPolicy;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;

class DynamoDBBulkConversionTest {

    @ParameterizedTest
    @EnumSource(value = NamingPolicy.class, names = { "CAMEL_CASE", "SNAKE_CASE", "SCREAMING_SNAKE_CASE" })
    void bulkConversionPreservesInputOrderValuesAndUpdateActions(final NamingPolicy policy) {
        final List<Map<String, String>> input = List.of(Map.of("firstName", "Ada"), Map.of("firstName", "Grace"));
        final String key = switch (policy) {
            case CAMEL_CASE -> "firstName";
            case SNAKE_CASE -> "first_name";
            case SCREAMING_SNAKE_CASE -> "FIRST_NAME";
            default -> throw new AssertionError(policy);
        };
        final var items = DynamoDBExecutor.toItem(input, policy);
        final var updates = DynamoDBExecutor.toUpdateItem(input, policy);
        assertEquals(List.of(Map.of(key, AttributeValue.fromS("Ada")), Map.of(key, AttributeValue.fromS("Grace"))), items);
        assertEquals(2, updates.size());
        for (int i = 0; i < updates.size(); i++) {
            assertEquals(items.get(i).get(key), updates.get(i).get(key).value());
            assertEquals(AttributeAction.PUT, updates.get(i).get(key).action());
        }
        assertEquals(List.of(Map.of("firstName", "Ada"), Map.of("firstName", "Grace")), input);
    }

    @Test
    void defaultBulkPolicyAndEmptyCollectionsHaveDefinedResults() {
        final List<Map<String, String>> input = List.of(Map.of("firstName", "Ada"));
        assertEquals(AttributeValue.fromS("Ada"), DynamoDBExecutor.toItem(input).getFirst().get("firstName"));
        assertEquals(AttributeValue.fromS("Ada"), DynamoDBExecutor.toUpdateItem(input).getFirst().get("firstName").value());
        assertTrue(DynamoDBExecutor.toItem(List.of(), null).isEmpty());
        assertTrue(DynamoDBExecutor.toUpdateItem(List.of(), null).isEmpty());
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toItem((Collection<?>) null));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toUpdateItem((Collection<?>) null));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toItem(Arrays.asList(input.getFirst(), null)));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toUpdateItem(Arrays.asList(input.getFirst(), null)));
    }

    @Test
    void loosePropertyPairsPreserveOrderAndUseStringKeys() {
        final Map<String, Object> result = DynamoDBExecutor.asMap(7, "number", null, "null key", "name", null, 7, "replacement");
        assertEquals(List.of("7", "null", "name"), List.copyOf(result.keySet()));
        assertEquals("replacement", result.get("7"));
        assertEquals("null key", result.get("null"));
        assertTrue(result.containsKey("name"));
        assertNull(result.get("name"));
        assertNull(DynamoDBExecutor.asMap((Object[]) null));
        assertTrue(DynamoDBExecutor.asMap().isEmpty());
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asMap("name"));
    }

    @Test
    void keyVarargsChooseTheOneAndTwoAttributeOverloads() {
        assertEquals(Map.of("id", AttributeValue.fromS("a")), DynamoDBExecutor.asKey(new Object[] { "id", "a" }));
        assertEquals(Map.of("id", AttributeValue.fromS("a"), "sort", AttributeValue.fromN("1")), DynamoDBExecutor.asKey(new Object[] { "id", "a", "sort", 1 }));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey(new Object[0]));
        assertThrows(ClassCastException.class, () -> DynamoDBExecutor.asKey(new Object[] { 1, "value" }));
    }

    @Test
    void entityPutOverloadsForwardConvertedItemsAndReturnValues() {
        final DynamoDbClient client = mock(DynamoDbClient.class);
        final PutItemResponse response = PutItemResponse.builder().build();
        when(client.putItem(any(PutItemRequest.class))).thenReturn(response);
        final DynamoDBExecutor executor = new DynamoDBExecutor(client);
        final Object entity = Map.of("id", "a", "firstName", "Ada");
        assertSame(response, executor.putItem("people", entity));
        assertSame(response, executor.putItem("people", entity, "ALL_OLD"));
        final ArgumentCaptor<PutItemRequest> requests = ArgumentCaptor.forClass(PutItemRequest.class);
        verify(client, times(2)).putItem(requests.capture());
        for (final PutItemRequest request : requests.getAllValues()) {
            assertEquals("people", request.tableName());
            assertEquals(Map.of("id", AttributeValue.fromS("a"), "firstName", AttributeValue.fromS("Ada")), request.item());
        }
        assertEquals("ALL_OLD", requests.getAllValues().get(1).returnValuesAsString());
    }

    @Test
    void asyncBridgeUsesTheSuppliedAsyncClient() {
        final DynamoDbAsyncClient client = mock(DynamoDbAsyncClient.class);
        final AsyncDynamoDBExecutor async = new DynamoDBExecutor(mock(DynamoDbClient.class)).async(client);
        assertSame(client, async.dynamoDBAsyncClient());
    }

    @Test
    void asyncEntityPutsKeepTheClientFutureAndConvertedRequest() {
        final DynamoDbAsyncClient client = mock(DynamoDbAsyncClient.class);
        final CompletableFuture<PutItemResponse> future = new CompletableFuture<>();
        when(client.putItem(any(PutItemRequest.class))).thenReturn(future);
        final AsyncDynamoDBExecutor executor = new AsyncDynamoDBExecutor(client);
        final Object entity = Map.of("id", "a", "firstName", "Ada");
        assertSame(future, executor.putItem("people", entity));
        assertSame(future, executor.putItem("people", entity, "ALL_OLD"));
        final ArgumentCaptor<PutItemRequest> requests = ArgumentCaptor.forClass(PutItemRequest.class);
        verify(client, times(2)).putItem(requests.capture());
        for (final PutItemRequest request : requests.getAllValues()) {
            assertEquals("people", request.tableName());
            assertEquals(Map.of("id", AttributeValue.fromS("a"), "firstName", AttributeValue.fromS("Ada")), request.item());
        }
        assertEquals("ALL_OLD", requests.getAllValues().get(1).returnValuesAsString());
        final DynamoDbClient syncClient = mock(DynamoDbClient.class);
        assertSame(syncClient, executor.sync(syncClient).dynamoDBClient());
        assertThrows(IllegalArgumentException.class, () -> executor.sync(null));
    }
}
