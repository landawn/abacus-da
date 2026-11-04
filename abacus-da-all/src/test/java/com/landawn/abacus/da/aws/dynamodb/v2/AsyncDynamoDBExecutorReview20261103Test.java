package com.landawn.abacus.da.aws.dynamodb.v2;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.stream.Stream;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

/**
 * Pins the SDK v2 {@link AsyncDynamoDBExecutor} contracts whose Javadoc was corrected or completed in the 2026-11-03 review.
 */
public class AsyncDynamoDBExecutorReview20261103Test {

    private static final String TABLE = "T";

    /**
     * {@code list}/{@code query}/{@code batchGetItem} with a row class (and the Mapper's list/query/batchGetItem): an item that
     * cannot be converted is reported through the returned future, wrapped in a CompletionException, not thrown by the call.
     */
    @Test
    public void conversionFailures_failTheReturnedFutureInsteadOfTheCall() {
        final DynamoDbAsyncClient client = mock(DynamoDbAsyncClient.class);
        final Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("id", AttributeValue.fromS("a"));
        item.put("count", AttributeValue.fromS("not-a-number"));
        when(client.query(any(QueryRequest.class)))
                .thenAnswer(invocation -> CompletableFuture.completedFuture(QueryResponse.builder().items(List.of(item)).build()));
        when(client.batchGetItem(any(BatchGetItemRequest.class)))
                .thenAnswer(invocation -> CompletableFuture.completedFuture(BatchGetItemResponse.builder().responses(Map.of(TABLE, List.of(item))).build()));

        final AsyncDynamoDBExecutor executor = new AsyncDynamoDBExecutor(client);
        final AsyncDynamoDBExecutor.Mapper<CountEntity> mapper = executor.mapper(CountEntity.class, TABLE, NamingPolicy.CAMEL_CASE);
        final QueryRequest queryRequest = QueryRequest.builder().tableName(TABLE).build();
        final CountEntity key = new CountEntity();
        key.setId("a");

        // Two attributes for a single-value row type -> IllegalArgumentException; "not-a-number" into an int property -> a
        // RuntimeException from the value conversion.
        assertAll(() -> assertFutureFailsWith(IllegalArgumentException.class, executor.list(queryRequest, String.class)),
                () -> assertFutureFailsWith(RuntimeException.class, executor.list(queryRequest, CountEntity.class)),
                () -> assertFutureFailsWith(RuntimeException.class, executor.query(queryRequest, CountEntity.class)),
                () -> assertFutureFailsWith(IllegalArgumentException.class, executor.batchGetItem(Map.of(), String.class)),
                () -> assertFutureFailsWith(IllegalArgumentException.class, executor.batchGetItem(Map.of(), "TOTAL", String.class)),
                () -> assertFutureFailsWith(IllegalArgumentException.class, executor.batchGetItem(BatchGetItemRequest.builder().build(), String.class)),
                () -> assertFutureFailsWith(RuntimeException.class, mapper.list(queryRequest)),
                () -> assertFutureFailsWith(RuntimeException.class, mapper.query(queryRequest)),
                () -> assertFutureFailsWith(RuntimeException.class, mapper.batchGetItem(BatchGetItemRequest.builder().build())),
                () -> assertFutureFailsWith(RuntimeException.class, mapper.batchGetItem(List.of(key))));
    }

    /**
     * Class Javadoc: a failed page request of {@code list}/{@code query} fails the returned future on the thread that fails the
     * page future (the SDK's response thread), for the first page of every paginating entry point (the 2026-11-02 test covers a
     * later page of {@code list}).
     */
    @Test
    public void failedFirstPage_failsResultOnPageCompletingThread_forEveryPaginatingEntryPoint() {
        final QueryRequest queryRequest = QueryRequest.builder().tableName(TABLE).build();

        final List<Executable> checks = new ArrayList<>();

        for (final Map.Entry<String, Function<AsyncDynamoDBExecutor, CompletableFuture<?>>> entry : paginatingCalls(queryRequest).entrySet()) {
            checks.add(() -> {
                final DynamoDbAsyncClient client = mock(DynamoDbAsyncClient.class);
                final CompletableFuture<QueryResponse> page = new CompletableFuture<>();
                when(client.query(any(QueryRequest.class))).thenReturn(page);

                final CompletableFuture<?> result = entry.getValue().apply(new AsyncDynamoDBExecutor(client));

                // The whole stage chain is attached to the first page future before the call returns, so failing it now runs
                // every stage on the failing thread.
                final AtomicReference<String> completingThread = new AtomicReference<>();
                final AtomicReference<Throwable> failure = new AtomicReference<>();
                final CountDownLatch done = new CountDownLatch(1);
                result.whenComplete((r, ex) -> {
                    completingThread.set(Thread.currentThread().getName());
                    failure.set(ex);
                    done.countDown();
                });

                final ResourceNotFoundException cause = ResourceNotFoundException.builder().message("gone").build();
                final Thread sdkThread = new Thread(() -> page.completeExceptionally(new CompletionException(cause)), "sdk-async-response-sim");
                sdkThread.start();
                sdkThread.join(5000);

                assertTrue(done.await(5, TimeUnit.SECONDS), entry.getKey());
                assertEquals("sdk-async-response-sim", completingThread.get(), entry.getKey());
                assertInstanceOf(CompletionException.class, failure.get(), entry.getKey());
                assertSame(cause, failure.get().getCause(), entry.getKey());
            });
        }

        assertAll(checks);
    }

    /**
     * {@code close()} Javadoc: once the executor is closed (default Netty HTTP client), the returned futures fail with an
     * SdkClientException wrapped in a CompletionException, while the stream and scan futures complete normally and consuming
     * their stream throws the SdkClientException. Offline: the closed client fails before any connection is attempted (the
     * endpoint port is never contacted).
     */
    @Test
    public void closedExecutor_failsFutures_butStreamAndScanThrowWhenConsumed() throws Exception {
        final DynamoDbAsyncClient client = DynamoDbAsyncClient.builder()
                .endpointOverride(URI.create("http://localhost:1"))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")))
                .build();
        final AsyncDynamoDBExecutor executor = new AsyncDynamoDBExecutor(client);
        executor.close();

        final Map<String, AttributeValue> key = Map.of("id", AttributeValue.fromS("x"));
        final QueryRequest queryRequest = QueryRequest.builder().tableName(TABLE).build();

        final CompletableFuture<Stream<Map<String, Object>>> streamFuture = executor.stream(queryRequest);
        final CompletableFuture<Stream<Map<String, Object>>> scanFuture = executor.scan(ScanRequest.builder().tableName(TABLE).build());

        assertAll(() -> assertClosedClientFailure(executor.putItem(TABLE, key)), () -> assertClosedClientFailure(executor.getItem(TABLE, key)),
                () -> assertClosedClientFailure(executor.list(queryRequest)), () -> assertClosedClientFailure(executor.query(queryRequest)),
                () -> assertClosedClientFailure(executor.batchWriteItem(Map.of())), () -> {
                    final Stream<Map<String, Object>> stream = streamFuture.get(10, TimeUnit.SECONDS);
                    assertFalse(streamFuture.isCompletedExceptionally());
                    assertThrows(SdkClientException.class, stream::toList);
                }, () -> {
                    final Stream<Map<String, Object>> stream = scanFuture.get(10, TimeUnit.SECONDS);
                    assertFalse(scanFuture.isCompletedExceptionally());
                    assertThrows(SdkClientException.class, stream::toList);
                });
    }

    private static Map<String, Function<AsyncDynamoDBExecutor, CompletableFuture<?>>> paginatingCalls(final QueryRequest queryRequest) {
        final Map<String, Function<AsyncDynamoDBExecutor, CompletableFuture<?>>> calls = new LinkedHashMap<>();
        calls.put("list(QueryRequest)", executor -> executor.list(queryRequest));
        calls.put("list(QueryRequest, Class)", executor -> executor.list(queryRequest, CountEntity.class));
        calls.put("query(QueryRequest)", executor -> executor.query(queryRequest));
        calls.put("query(QueryRequest, Map)", executor -> executor.query(queryRequest, LinkedHashMap.class));
        calls.put("query(QueryRequest, Bean)", executor -> executor.query(queryRequest, CountEntity.class));
        calls.put("mapper.list", executor -> executor.mapper(CountEntity.class, TABLE, NamingPolicy.CAMEL_CASE).list(queryRequest));
        calls.put("mapper.query", executor -> executor.mapper(CountEntity.class, TABLE, NamingPolicy.CAMEL_CASE).query(queryRequest));
        return calls;
    }

    private static void assertFutureFailsWith(final Class<? extends Throwable> causeType, final CompletableFuture<?> future) throws Exception {
        final AtomicReference<Throwable> seen = new AtomicReference<>();
        future.handle((r, ex) -> {
            seen.set(ex);
            return null;
        }).get(5, TimeUnit.SECONDS);

        assertInstanceOf(CompletionException.class, seen.get());
        assertInstanceOf(causeType, seen.get().getCause());
        assertInstanceOf(causeType, assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS)).getCause());
    }

    private static void assertClosedClientFailure(final CompletableFuture<?> future) throws Exception {
        final AtomicReference<Throwable> seen = new AtomicReference<>();
        future.handle((r, ex) -> {
            seen.set(ex);
            return null;
        }).get(10, TimeUnit.SECONDS);

        assertInstanceOf(CompletionException.class, seen.get());
        assertInstanceOf(SdkClientException.class, seen.get().getCause());
    }

    public static class CountEntity {
        private String id;
        private int count;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public int getCount() {
            return count;
        }

        public void setCount(final int count) {
            this.count = count;
        }
    }
}
