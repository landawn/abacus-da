/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.aws.dynamodb.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import com.landawn.abacus.da.MockitoTestBase;
import com.landawn.abacus.da.aws.dynamodb.DynamoDBExecutorLineCoverageTest;
import com.landawn.abacus.util.InternalUtil;
import com.landawn.abacus.util.stream.Stream;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

class AsyncDynamoDBExecutorLineCoverageTest extends MockitoTestBase {

    @Mock
    private DynamoDbAsyncClient client;

    private AsyncDynamoDBExecutor executor;

    @BeforeEach
    void createExecutor() {
        executor = new AsyncDynamoDBExecutor(client);
    }

    @Test
    void constructorRejectsANullClient() {
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> new AsyncDynamoDBExecutor(null));

        assertEquals("dynamoDBClient cannot be null", ex.getMessage());
    }

    @Test
    void queryAndScanPageFailuresRethrowTheError() {
        final AssertionError failure = new AssertionError("page failed");
        when(client.query(any(QueryRequest.class))).thenReturn(CompletableFuture.failedFuture(failure));
        when(client.scan(any(ScanRequest.class))).thenReturn(CompletableFuture.failedFuture(failure));

        final Stream<?> query = executor.stream(QueryRequest.builder().tableName("Users").build(), Map.class).join();
        final Stream<?> scan = executor.scan(ScanRequest.builder().tableName("Users").build(), Map.class).join();
        final Iterator<?> queryPages = com.landawn.abacus.da.aws.dynamodb.DynamoDBExecutorLineCoverageTest.pageIterator(query, AsyncDynamoDBExecutor.class);
        final Iterator<?> scanPages = com.landawn.abacus.da.aws.dynamodb.DynamoDBExecutorLineCoverageTest.pageIterator(scan, AsyncDynamoDBExecutor.class);

        assertSame(failure, assertThrows(AssertionError.class, queryPages::hasNext));
        assertSame(failure, assertThrows(AssertionError.class, scanPages::hasNext));
    }

    @Test
    void exhaustedQueryAndScanIteratorsRejectNext() {
        when(client.query(any(QueryRequest.class))).thenReturn(CompletableFuture.completedFuture(QueryResponse.builder().build()));
        when(client.scan(any(ScanRequest.class))).thenReturn(CompletableFuture.completedFuture(ScanResponse.builder().build()));

        final Stream<?> query = executor.stream(QueryRequest.builder().tableName("Users").build(), Map.class).join();
        final Stream<?> scan = executor.scan(ScanRequest.builder().tableName("Users").build(), Map.class).join();
        final Iterator<?> queryPages = com.landawn.abacus.da.aws.dynamodb.DynamoDBExecutorLineCoverageTest.pageIterator(query, AsyncDynamoDBExecutor.class);
        final Iterator<?> scanPages = com.landawn.abacus.da.aws.dynamodb.DynamoDBExecutorLineCoverageTest.pageIterator(scan, AsyncDynamoDBExecutor.class);

        assertFalse(queryPages.hasNext());
        assertFalse(scanPages.hasNext());
        final NoSuchElementException queryEnd = assertThrows(NoSuchElementException.class, queryPages::next);
        final NoSuchElementException scanEnd = assertThrows(NoSuchElementException.class, scanPages::next);
        assertEquals(InternalUtil.ERROR_MSG_FOR_NO_SUCH_EX, queryEnd.getMessage());
        assertEquals(InternalUtil.ERROR_MSG_FOR_NO_SUCH_EX, scanEnd.getMessage());
    }

    @Test
    void mapperBatchGetOfAnEmptyResponseReturnsANewList() {
        when(client.batchGetItem(any(BatchGetItemRequest.class))).thenReturn(CompletableFuture.completedFuture(BatchGetItemResponse.builder().build()));
        final AsyncDynamoDBExecutor.Mapper<DynamoDBExecutorTestSupport.TestEntity> mapper = executor.mapper(DynamoDBExecutorTestSupport.TestEntity.class);
        final DynamoDBExecutorTestSupport.TestEntity key = new DynamoDBExecutorTestSupport.TestEntity();
        key.setId("1");
        final BatchGetItemRequest request = BatchGetItemRequest.builder()
                .requestItems(Map.of("TestTable", KeysAndAttributes.builder().keys(List.of(Map.of("id", AttributeValue.builder().s("1").build()))).build()))
                .build();

        final List<DynamoDBExecutorTestSupport.TestEntity> byEntities = mapper.batchGetItem(List.of(key)).join();
        final List<DynamoDBExecutorTestSupport.TestEntity> byCapacity = mapper.batchGetItem(List.of(key), "TOTAL").join();
        final List<DynamoDBExecutorTestSupport.TestEntity> byRequest = mapper.batchGetItem(request).join();

        assertEquals(ArrayList.class, byEntities.getClass());
        assertEquals(ArrayList.class, byCapacity.getClass());
        assertEquals(ArrayList.class, byRequest.getClass());
        assertTrue(byEntities.isEmpty());
        assertTrue(byCapacity.isEmpty());
        assertTrue(byRequest.isEmpty());
    }
}
