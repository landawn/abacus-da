/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.aws.dynamodb.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.logging.LoggerFactory;
import com.landawn.abacus.util.InternalUtil;
import com.landawn.abacus.util.stream.Stream;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
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

class DynamoDBExecutorLineCoverageTest extends DynamoDBExecutorTestSupport {

    @Test
    void asItemRejectsAnOddNumberOfArguments() {
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asItem("a", "b", "c"));

        assertEquals("Parameters must be name-value pairs (an even number of arguments)", ex.getMessage());
    }

    @Test
    void readRowOfANullRowUsesTheTargetDefault() {
        assertNull(DynamoDBExecutor.readRow((Map<String, AttributeValue>) null, null));
        assertEquals(0, DynamoDBExecutor.readRow((Map<String, AttributeValue>) null, int.class));
    }

    @Test
    void toValueRejectsAnAttributeWithNoMember() {
        final AttributeValue empty = AttributeValue.builder().build();
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toValue(empty));

        assertEquals("Unsupported Attribute type: " + empty, ex.getMessage());
    }

    @Test
    void itemMutationsLogWhenDebugIsEnabled() {
        final Level previous = com.landawn.abacus.da.aws.dynamodb.DynamoDBExecutorLineCoverageTest.enableDebug(DynamoDBExecutor.class);
        final GetItemResponse getResponse = GetItemResponse.builder().build();
        final PutItemResponse putResponse = PutItemResponse.builder().build();
        final UpdateItemResponse updateResponse = UpdateItemResponse.builder().build();
        final DeleteItemResponse deleteResponse = DeleteItemResponse.builder().build();
        final Map<String, AttributeValue> key = DynamoDBExecutor.asKey("id", "1");
        final Map<String, AttributeValue> item = DynamoDBExecutor.asItem("id", "1");
        final Map<String, AttributeValueUpdate> updates = Map.of("name",
                AttributeValueUpdate.builder().value(AttributeValue.builder().s("Ada").build()).build());

        when(mockDynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(getResponse);
        when(mockDynamoDbClient.putItem(any(PutItemRequest.class))).thenReturn(putResponse);
        when(mockDynamoDbClient.updateItem(any(UpdateItemRequest.class))).thenReturn(updateResponse);
        when(mockDynamoDbClient.deleteItem(any(DeleteItemRequest.class))).thenReturn(deleteResponse);

        try {
            assertTrue(LoggerFactory.getLogger(DynamoDBExecutor.class).isDebugEnabled());
            assertNull(executor.getItem("Users", key));
            assertSame(putResponse, executor.putItem("Users", item));
            assertSame(updateResponse, executor.updateItem("Users", key, updates));
            assertSame(deleteResponse, executor.deleteItem("Users", key));
        } finally {
            Logger.getLogger(DynamoDBExecutor.class.getName()).setLevel(previous);
        }

        verify(mockDynamoDbClient).getItem(any(GetItemRequest.class));
        verify(mockDynamoDbClient).putItem(any(PutItemRequest.class));
        verify(mockDynamoDbClient).updateItem(any(UpdateItemRequest.class));
        verify(mockDynamoDbClient).deleteItem(any(DeleteItemRequest.class));
    }

    @Test
    void exhaustedQueryAndScanIteratorsRejectNext() {
        when(mockDynamoDbClient.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().build());
        when(mockDynamoDbClient.scan(any(ScanRequest.class))).thenReturn(ScanResponse.builder().build());

        try (Stream<?> query = executor.stream(QueryRequest.builder().tableName("Users").build(), Map.class);
                Stream<?> scan = executor.scan(ScanRequest.builder().tableName("Users").build(), Map.class)) {
            final Iterator<?> queryPages = com.landawn.abacus.da.aws.dynamodb.DynamoDBExecutorLineCoverageTest.pageIterator(query, DynamoDBExecutor.class);
            final Iterator<?> scanPages = com.landawn.abacus.da.aws.dynamodb.DynamoDBExecutorLineCoverageTest.pageIterator(scan, DynamoDBExecutor.class);

            assertFalse(queryPages.hasNext());
            assertFalse(scanPages.hasNext());
            final NoSuchElementException queryEnd = assertThrows(NoSuchElementException.class, queryPages::next);
            final NoSuchElementException scanEnd = assertThrows(NoSuchElementException.class, scanPages::next);
            assertEquals(InternalUtil.ERROR_MSG_FOR_NO_SUCH_EX, queryEnd.getMessage());
            assertEquals(InternalUtil.ERROR_MSG_FOR_NO_SUCH_EX, scanEnd.getMessage());
        }
    }

    @Test
    void mapperBatchGetOfAnEmptyResponseReturnsANewList() {
        when(mockDynamoDbClient.batchGetItem(any(BatchGetItemRequest.class))).thenReturn(BatchGetItemResponse.builder().build());
        final DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        final TestEntity key = new TestEntity();
        key.setId("1");

        final List<TestEntity> byEntities = mapper.batchGetItem(List.of(key), "TOTAL");
        final List<TestEntity> byRequest = mapper.batchGetItem(BatchGetItemRequest.builder().build());

        assertEquals(ArrayList.class, byEntities.getClass());
        assertEquals(ArrayList.class, byRequest.getClass());
        assertTrue(byEntities.isEmpty());
        assertTrue(byRequest.isEmpty());
    }
}
