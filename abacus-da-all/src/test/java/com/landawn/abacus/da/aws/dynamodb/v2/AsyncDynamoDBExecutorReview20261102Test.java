package com.landawn.abacus.da.aws.dynamodb.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

/**
 * Pins the SDK v2 {@link AsyncDynamoDBExecutor} behaviors whose Javadoc was corrected or completed in the 2026-11-02 review.
 */
public class AsyncDynamoDBExecutorReview20261102Test {

    /**
     * {@code updateItem(String, Map, Map[, String])} documented a null {@code attributeUpdates} as failing service-side
     * validation; the executor forwards a null or empty map unchanged and DynamoDB accepts it (see the live test below).
     */
    @Test
    public void updateItem_nullOrEmptyAttributeUpdates_isForwardedWithoutUpdates() throws Exception {
        final DynamoDbAsyncClient client = mock(DynamoDbAsyncClient.class);
        final UpdateItemResponse response = UpdateItemResponse.builder().build();
        when(client.updateItem(any(UpdateItemRequest.class))).thenReturn(CompletableFuture.completedFuture(response));
        final AsyncDynamoDBExecutor executor = new AsyncDynamoDBExecutor(client);
        final Map<String, AttributeValue> key = DynamoDBExecutor.asKey("pk", "a");

        assertSame(response, executor.updateItem("T", key, null).get());
        assertSame(response, executor.updateItem("T", key, new HashMap<String, AttributeValueUpdate>(), "ALL_NEW").get());

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
     * Class Javadoc: a failed page request of the auto-paginating list fails the returned future on the thread that fails
     * the page future (the SDK's response thread), not on the default asynchronous executor, wrapped in a CompletionException.
     */
    @Test
    public void list_failedLaterPage_failsResultOnPageCompletingThread() throws Exception {
        final DynamoDbAsyncClient client = mock(DynamoDbAsyncClient.class);
        final QueryResponse page1 = QueryResponse.builder()
                .items(List.of(Map.of("pk", AttributeValue.fromS("a"))))
                .lastEvaluatedKey(Map.of("pk", AttributeValue.fromS("a")))
                .build();
        final CompletableFuture<QueryResponse> page2 = new CompletableFuture<>();
        when(client.query(any(QueryRequest.class))).thenReturn(CompletableFuture.completedFuture(page1), page2);

        final CompletableFuture<List<Map<String, Object>>> result = new AsyncDynamoDBExecutor(client).list(QueryRequest.builder().tableName("T").build());
        verify(client, timeout(5000).times(2)).query(any(QueryRequest.class));

        // query() has returned the page future, but the chain attaches to it just after; failing it before then would run the
        // dependent stage on the attaching pool thread instead.
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);

        while (page2.getNumberOfDependents() == 0 && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }

        assertTrue(page2.getNumberOfDependents() > 0);

        // The page-1 stage attaches page 2's stage inside its function and the result's relay to that stage right after the
        // function returns, on the same pool thread. Let that thread finish, or a page failure in between would complete the
        // result on the pool thread when the relay is attached (a load-dependent failure of this test).
        if (ForkJoinPool.getCommonPoolParallelism() > 1) {
            assertTrue(ForkJoinPool.commonPool().awaitQuiescence(5, TimeUnit.SECONDS));
        } else {
            Thread.sleep(200); // CompletableFuture runs each async stage on a new thread here; the window is microseconds.
        }

        final AtomicReference<String> completingThread = new AtomicReference<>();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final CountDownLatch done = new CountDownLatch(1);
        result.whenComplete((r, ex) -> {
            completingThread.set(Thread.currentThread().getName());
            failure.set(ex);
            done.countDown();
        });

        final ResourceNotFoundException cause = ResourceNotFoundException.builder().message("gone").build();
        final Thread sdkThread = new Thread(() -> page2.completeExceptionally(new CompletionException(cause)), "sdk-async-response-sim");
        sdkThread.start();
        sdkThread.join(5000);

        assertTrue(done.await(5, TimeUnit.SECONDS));
        assertEquals("sdk-async-response-sim", completingThread.get());
        assertInstanceOf(CompletionException.class, failure.get());
        assertSame(cause, failure.get().getCause());
    }

    /**
     * Live check against DynamoDB Local (skipped when it is not running on localhost:8000): an UpdateItem with null or empty
     * attribute updates succeeds and creates a key-only item; a failed condition reaches exceptionally(...) as a
     * CompletionException wrapping ConditionalCheckFailedException; and a call after close() fails through the returned
     * future with an SdkClientException wrapped in a CompletionException.
     */
    @Test
    public void live_updateItemWithoutUpdates_conditionFailureWrapping_andClosedClient() throws Exception {
        final DynamoDbAsyncClient client = newLocalClient();
        final String table = "AsyncReview20261102_" + UUID.randomUUID().toString().replace("-", "");

        try {
            try {
                client.listTables().get(3, TimeUnit.SECONDS);
            } catch (final Exception e) {
                Assumptions.abort("DynamoDB Local is not available on localhost:8000: " + e);
            }

            client.createTable(b -> b.tableName(table)
                    .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
                    .attributeDefinitions(AttributeDefinition.builder().attributeName("id").attributeType(ScalarAttributeType.S).build())
                    .billingMode(BillingMode.PAY_PER_REQUEST)).get(10, TimeUnit.SECONDS);

            try {
                final AsyncDynamoDBExecutor executor = new AsyncDynamoDBExecutor(client);
                final Map<String, AttributeValue> k1 = Map.of("id", AttributeValue.fromS("k1"));
                final Map<String, AttributeValue> k2 = Map.of("id", AttributeValue.fromS("k2"));

                executor.updateItem(table, k1, null).get(10, TimeUnit.SECONDS);
                executor.updateItem(table, k2, new HashMap<>(), "ALL_NEW").get(10, TimeUnit.SECONDS);

                assertEquals(Map.of("id", "k1"), executor.getItem(table, k1).get(10, TimeUnit.SECONDS));
                assertEquals(Map.of("id", "k2"), executor.getItem(table, k2).get(10, TimeUnit.SECONDS));

                final AtomicReference<Throwable> seen = new AtomicReference<>();
                executor.putItem(PutItemRequest.builder().tableName(table).item(k1).conditionExpression("attribute_not_exists(id)").build())
                        .exceptionally(ex -> {
                            seen.set(ex);
                            return null;
                        })
                        .get(10, TimeUnit.SECONDS);

                assertInstanceOf(CompletionException.class, seen.get());
                assertInstanceOf(ConditionalCheckFailedException.class, seen.get().getCause());
            } finally {
                client.deleteTable(b -> b.tableName(table)).get(10, TimeUnit.SECONDS);
            }
        } finally {
            client.close();
        }

        final AsyncDynamoDBExecutor closed = new AsyncDynamoDBExecutor(newLocalClient());
        closed.close();

        final AtomicReference<Throwable> afterClose = new AtomicReference<>();
        closed.putItem("T", Map.of("id", AttributeValue.fromS("x"))).handle((r, ex) -> {
            afterClose.set(ex);
            return null;
        }).get(10, TimeUnit.SECONDS);

        assertInstanceOf(CompletionException.class, afterClose.get());
        assertInstanceOf(SdkClientException.class, afterClose.get().getCause());
    }

    private static DynamoDbAsyncClient newLocalClient() {
        return DynamoDbAsyncClient.builder()
                .endpointOverride(URI.create("http://localhost:8000"))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")))
                .build();
    }
}
