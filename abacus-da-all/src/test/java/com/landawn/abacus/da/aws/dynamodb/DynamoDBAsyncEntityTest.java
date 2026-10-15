package com.landawn.abacus.da.aws.dynamodb;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.landawn.abacus.util.AsyncExecutor;

class DynamoDBAsyncEntityTest {

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void entityWritesConvertBeforeCallingTheClient(final boolean withReturnValues) throws Exception {
        final AmazonDynamoDBClient client = mock(AmazonDynamoDBClient.class);
        final PutItemResult response = new PutItemResult();
        final Map<String, AttributeValue> expected = Map.of("id", new AttributeValue("a"), "firstName", new AttributeValue("Ada"));
        when(client.putItem("people", expected)).thenReturn(response);
        when(client.putItem("people", expected, "ALL_OLD")).thenReturn(response);
        final AsyncDynamoDBExecutor executor = new AsyncDynamoDBExecutor(new DynamoDBExecutor(client), new AsyncExecutor(Runnable::run));
        final Object entity = Map.of("id", "a", "firstName", "Ada");
        final var future = withReturnValues ? executor.putItem("people", entity, "ALL_OLD") : executor.putItem("people", entity);
        assertSame(response, future.get());
        if (withReturnValues) {
            verify(client).putItem("people", expected, "ALL_OLD");
        } else {
            verify(client).putItem("people", expected);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void entityWriteFailuresAreReportedThroughTheFuture(final boolean withReturnValues) {
        final AmazonDynamoDBClient client = mock(AmazonDynamoDBClient.class);
        final Map<String, AttributeValue> expected = Map.of("id", new AttributeValue("a"));
        final IllegalStateException failure = new IllegalStateException("write failed");
        when(client.putItem("people", expected)).thenThrow(failure);
        when(client.putItem("people", expected, "ALL_OLD")).thenThrow(failure);
        final AsyncDynamoDBExecutor executor = new AsyncDynamoDBExecutor(new DynamoDBExecutor(client), new AsyncExecutor(Runnable::run));
        final Object entity = Map.of("id", "a");
        final var future = withReturnValues ? executor.putItem("people", entity, "ALL_OLD") : executor.putItem("people", entity);
        assertSame(failure, assertThrows(ExecutionException.class, future::get).getCause());
    }
}
