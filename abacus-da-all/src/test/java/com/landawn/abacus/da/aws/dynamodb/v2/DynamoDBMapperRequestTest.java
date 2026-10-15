package com.landawn.abacus.da.aws.dynamodb.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;

import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.util.NamingPolicy;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

class DynamoDBMapperRequestTest {

    @ParameterizedTest
    @NullAndEmptySource
    void syncMapperFillsMissingTablesWhilePreservingAllRequestOptions(final String missingTable) {
        final DynamoDbClient client = mock(DynamoDbClient.class);
        final var mapper = new DynamoDBExecutor(client).mapper(Item.class, "people", NamingPolicy.CAMEL_CASE);
        final Requests input = requests(missingTable);
        final Requests expected = requests("people");
        final PutItemResponse put = PutItemResponse.builder().build();
        final UpdateItemResponse update = UpdateItemResponse.builder().build();
        final DeleteItemResponse delete = DeleteItemResponse.builder().build();
        when(client.getItem(expected.get())).thenReturn(GetItemResponse.builder().build());
        when(client.putItem(expected.put())).thenReturn(put);
        when(client.updateItem(expected.update())).thenReturn(update);
        when(client.deleteItem(expected.delete())).thenReturn(delete);
        when(client.query(expected.query())).thenReturn(QueryResponse.builder().build());
        when(client.scan(expected.scan())).thenReturn(ScanResponse.builder().build());

        assertNull(mapper.getItem(input.get()));
        assertSame(put, mapper.putItem(input.put()));
        assertSame(update, mapper.updateItem(input.update()));
        assertSame(delete, mapper.deleteItem(input.delete()));
        assertEquals(0, mapper.query(input.query()).size());
        try (var rows = mapper.scan(input.scan())) {
            assertEquals(0L, rows.count());
        }
        verify(client).getItem(expected.get());
        verify(client).putItem(expected.put());
        verify(client).updateItem(expected.update());
        verify(client).deleteItem(expected.delete());
        verify(client).query(expected.query());
        verify(client).scan(expected.scan());
        assertEquals(requests(missingTable), input);
    }

    @ParameterizedTest
    @NullAndEmptySource
    void asyncMapperFillsMissingTablesWhilePreservingAllRequestOptions(final String missingTable) {
        final DynamoDbAsyncClient client = mock(DynamoDbAsyncClient.class);
        final var mapper = new AsyncDynamoDBExecutor(client).mapper(Item.class, "people", NamingPolicy.CAMEL_CASE);
        final Requests input = requests(missingTable);
        final Requests expected = requests("people");
        final PutItemResponse put = PutItemResponse.builder().build();
        final UpdateItemResponse update = UpdateItemResponse.builder().build();
        final DeleteItemResponse delete = DeleteItemResponse.builder().build();
        when(client.getItem(expected.get())).thenReturn(CompletableFuture.completedFuture(GetItemResponse.builder().build()));
        when(client.putItem(expected.put())).thenReturn(CompletableFuture.completedFuture(put));
        when(client.updateItem(expected.update())).thenReturn(CompletableFuture.completedFuture(update));
        when(client.deleteItem(expected.delete())).thenReturn(CompletableFuture.completedFuture(delete));
        when(client.query(expected.query())).thenReturn(CompletableFuture.completedFuture(QueryResponse.builder().build()));
        when(client.scan(expected.scan())).thenReturn(CompletableFuture.completedFuture(ScanResponse.builder().build()));

        assertNull(mapper.getItem(input.get()).join());
        assertSame(put, mapper.putItem(input.put()).join());
        assertSame(update, mapper.updateItem(input.update()).join());
        assertSame(delete, mapper.deleteItem(input.delete()).join());
        assertEquals(0, mapper.query(input.query()).join().size());
        try (var rows = mapper.scan(input.scan()).join()) {
            assertEquals(0L, rows.count());
        }
        verify(client).getItem(expected.get());
        verify(client).putItem(expected.put());
        verify(client).updateItem(expected.update());
        verify(client).deleteItem(expected.delete());
        verify(client).query(expected.query());
        verify(client).scan(expected.scan());
        assertEquals(requests(missingTable), input);
    }

    private static Requests requests(final String tableName) {
        final Map<String, AttributeValue> key = Map.of("id", AttributeValue.fromS("a"));
        final Map<String, AttributeValue> values = Map.of(":id", AttributeValue.fromS("a"));
        return new Requests(GetItemRequest.builder().tableName(tableName).key(key).consistentRead(true).projectionExpression("id").build(),
                PutItemRequest.builder().tableName(tableName).item(key).conditionExpression("attribute_not_exists(id)").returnValues("ALL_OLD").build(),
                UpdateItemRequest.builder()
                        .tableName(tableName)
                        .key(key)
                        .updateExpression("SET name = :id")
                        .expressionAttributeValues(values)
                        .returnValues("ALL_NEW")
                        .build(),
                DeleteItemRequest.builder().tableName(tableName).key(key).conditionExpression("attribute_exists(id)").returnValues("ALL_OLD").build(),
                QueryRequest.builder()
                        .tableName(tableName)
                        .keyConditionExpression("id = :id")
                        .expressionAttributeValues(values)
                        .limit(5)
                        .consistentRead(true)
                        .build(),
                ScanRequest.builder()
                        .tableName(tableName)
                        .filterExpression("id = :id")
                        .expressionAttributeValues(values)
                        .limit(5)
                        .consistentRead(true)
                        .build());
    }

    private record Requests(GetItemRequest get, PutItemRequest put, UpdateItemRequest update, DeleteItemRequest delete, QueryRequest query, ScanRequest scan) {
    }

    public static class Item {
        @Id
        private String id;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }
    }
}
