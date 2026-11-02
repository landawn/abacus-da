/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.aws.dynamodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.landawn.abacus.logging.LoggerFactory;
import com.landawn.abacus.util.InternalUtil;
import com.landawn.abacus.util.stream.Stream;

public class DynamoDBExecutorLineCoverageTest extends DynamoDBExecutorTestSupport {

    private static final String TABLE_MISMATCH = "Table name mismatch: request has 'Other' but mapper expects 'TestTable'";

    @Test
    void asKeyOfTwoArrayElementsBuildsOneAttribute() {
        final Map<String, AttributeValue> key = DynamoDBExecutor.asKey(new Object[] { "id", "user-1" });

        assertEquals(1, key.size());
        assertEquals("user-1", key.get("id").getS());
    }

    @Test
    void asItemOfFourAndSixArgumentsKeepsEachPair() {
        final Map<String, AttributeValue> two = DynamoDBExecutor.asItem("id", "a", "name", "b");
        final Map<String, AttributeValue> three = DynamoDBExecutor.asItem("id", "a", "name", "b", "age", 3);

        assertEquals("a", two.get("id").getS());
        assertEquals("b", two.get("name").getS());
        assertEquals(2, two.size());
        assertEquals("a", three.get("id").getS());
        assertEquals("b", three.get("name").getS());
        assertEquals("3", three.get("age").getN());
        assertEquals(3, three.size());
    }

    @Test
    void asItemRejectsAnOddNumberOfArguments() {
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asItem("a", "b", "c"));

        assertEquals("Parameters must be name-value pairs (an even number of arguments)", ex.getMessage());
    }

    @Test
    void toValueKeepsExplicitFalseNullAndRejectsAnEmptyAttribute() {
        assertEquals(Boolean.FALSE, DynamoDBExecutor.toValue(new AttributeValue().withNULL(Boolean.FALSE)));

        final AttributeValue empty = new AttributeValue();
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toValue(empty));

        assertEquals("Unsupported Attribute type: " + empty, ex.getMessage());
    }

    @Test
    void itemMutationsLogWhenDebugIsEnabled() {
        final Level previous = enableDebug(DynamoDBExecutor.class);
        final GetItemResult getResult = new GetItemResult();
        final PutItemResult putResult = new PutItemResult();
        final UpdateItemResult updateResult = new UpdateItemResult();
        final DeleteItemResult deleteResult = new DeleteItemResult();
        final Map<String, AttributeValue> key = DynamoDBExecutor.asKey("id", "1");
        final Map<String, AttributeValue> item = DynamoDBExecutor.asItem("id", "1");
        final Map<String, AttributeValueUpdate> updates = Map.of("name", new AttributeValueUpdate().withValue(new AttributeValue().withS("Ada")));

        when(mockDynamoDBClient.getItem(eq("Users"), ArgumentMatchers.<Map<String, AttributeValue>>any())).thenReturn(getResult);
        when(mockDynamoDBClient.putItem(eq("Users"), ArgumentMatchers.<Map<String, AttributeValue>>any())).thenReturn(putResult);
        when(mockDynamoDBClient.updateItem(eq("Users"), ArgumentMatchers.<Map<String, AttributeValue>>any(),
                ArgumentMatchers.<Map<String, AttributeValueUpdate>>any())).thenReturn(updateResult);
        when(mockDynamoDBClient.deleteItem(eq("Users"), ArgumentMatchers.<Map<String, AttributeValue>>any())).thenReturn(deleteResult);

        try {
            assertTrue(LoggerFactory.getLogger(DynamoDBExecutor.class).isDebugEnabled());
            assertNull(executor.getItem("Users", key));
            assertSame(putResult, executor.putItem("Users", item));
            assertSame(updateResult, executor.updateItem("Users", key, updates));
            assertSame(deleteResult, executor.deleteItem("Users", key));
        } finally {
            Logger.getLogger(DynamoDBExecutor.class.getName()).setLevel(previous);
        }

        verify(mockDynamoDBClient).getItem(eq("Users"), ArgumentMatchers.<Map<String, AttributeValue>>any());
        verify(mockDynamoDBClient).putItem(eq("Users"), ArgumentMatchers.<Map<String, AttributeValue>>any());
        verify(mockDynamoDBClient).updateItem(eq("Users"), ArgumentMatchers.<Map<String, AttributeValue>>any(),
                ArgumentMatchers.<Map<String, AttributeValueUpdate>>any());
        verify(mockDynamoDBClient).deleteItem(eq("Users"), ArgumentMatchers.<Map<String, AttributeValue>>any());
    }

    @Test
    void queryStreamSkipsAnEmptyPageThenRejectsNextAfterTheLastPage() {
        final Map<String, AttributeValue> resume = Map.of("id", new AttributeValue().withS("start"));
        final QueryResult emptyPage = new QueryResult().withItems(Collections.emptyList()).withLastEvaluatedKey(resume);
        final QueryResult rowPage = new QueryResult().withItems(List.of(item("row-1")));
        final List<Map<String, AttributeValue>> queryStartKeys = new ArrayList<>();
        when(mockDynamoDBClient.query(any(QueryRequest.class))).thenAnswer(invocation -> {
            final QueryRequest sent = invocation.getArgument(0);
            queryStartKeys.add(sent.getExclusiveStartKey() == null ? null : new LinkedHashMap<>(sent.getExclusiveStartKey()));
            return queryStartKeys.size() == 1 ? emptyPage : rowPage;
        });

        final QueryRequest request = new QueryRequest().withTableName("Users");
        try (Stream<TestEntity> rows = executor.stream(request, TestEntity.class)) {
            final Iterator<?> pages = pageIterator(rows, DynamoDBExecutor.class);

            assertTrue(pages.hasNext());
            @SuppressWarnings("unchecked")
            final List<Map<String, AttributeValue>> page = (List<Map<String, AttributeValue>>) pages.next();
            assertEquals("row-1", page.get(0).get("id").getS());
            assertEquals(1, page.size());
            assertFalse(pages.hasNext());

            final NoSuchElementException ex = assertThrows(NoSuchElementException.class, pages::next);
            assertEquals(InternalUtil.ERROR_MSG_FOR_NO_SUCH_EX, ex.getMessage());
        }

        verify(mockDynamoDBClient, times(2)).query(any(QueryRequest.class));
        assertNull(queryStartKeys.get(0));
        assertEquals(resume, queryStartKeys.get(1));
        assertEquals(2, queryStartKeys.size());
    }

    @Test
    void scanStreamCarriesTheLastEvaluatedKeyAndRejectsNextWhenExhausted() {
        final Map<String, AttributeValue> resume = Map.of("id", new AttributeValue().withS("start"));
        final ScanResult first = new ScanResult().withItems(List.of(item("a"))).withLastEvaluatedKey(resume);
        final ScanResult second = new ScanResult().withItems(List.of(item("b")));
        final List<Map<String, AttributeValue>> scanStartKeys = new ArrayList<>();
        when(mockDynamoDBClient.scan(any(ScanRequest.class))).thenAnswer(invocation -> {
            final ScanRequest sent = invocation.getArgument(0);
            scanStartKeys.add(sent.getExclusiveStartKey() == null ? null : new LinkedHashMap<>(sent.getExclusiveStartKey()));
            return scanStartKeys.size() == 1 ? first : second;
        });

        try (Stream<TestEntity> rows = executor.scan(new ScanRequest().withTableName("Users"), TestEntity.class)) {
            final Iterator<?> pages = pageIterator(rows, DynamoDBExecutor.class);

            @SuppressWarnings("unchecked")
            final List<Map<String, AttributeValue>> pageOne = (List<Map<String, AttributeValue>>) pages.next();
            @SuppressWarnings("unchecked")
            final List<Map<String, AttributeValue>> pageTwo = (List<Map<String, AttributeValue>>) pages.next();
            assertEquals("a", pageOne.get(0).get("id").getS());
            assertEquals("b", pageTwo.get(0).get("id").getS());
            assertFalse(pages.hasNext());

            final NoSuchElementException ex = assertThrows(NoSuchElementException.class, pages::next);
            assertEquals(InternalUtil.ERROR_MSG_FOR_NO_SUCH_EX, ex.getMessage());
        }

        verify(mockDynamoDBClient, times(2)).scan(any(ScanRequest.class));
        assertNull(scanStartKeys.get(0));
        assertEquals(resume, scanStartKeys.get(1));
        assertEquals(2, scanStartKeys.size());
    }

    @Test
    void mapperBatchGetOfAnEmptyResponseReturnsANewList() {
        when(mockDynamoDBClient.batchGetItem(ArgumentMatchers.<Map<String, KeysAndAttributes>>any(), eq("TOTAL"))).thenReturn(new BatchGetItemResult());
        when(mockDynamoDBClient.batchGetItem(any(BatchGetItemRequest.class))).thenReturn(new BatchGetItemResult());
        final DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        final TestEntity key = new TestEntity();
        key.setId("1");

        final List<TestEntity> byEntities = mapper.batchGetItem(List.of(key), "TOTAL");
        final List<TestEntity> byRequest = mapper.batchGetItem(new BatchGetItemRequest());

        assertEquals(ArrayList.class, byEntities.getClass());
        assertEquals(ArrayList.class, byRequest.getClass());
        assertTrue(byEntities.isEmpty());
        assertTrue(byRequest.isEmpty());
    }

    @Test
    void mapperRejectsARequestThatNamesAnotherTable() {
        final DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        final Map<String, AttributeValue> key = DynamoDBExecutor.asKey("id", "1");

        final IllegalArgumentException get = assertThrows(IllegalArgumentException.class,
                () -> mapper.getItem(new GetItemRequest().withTableName("Other").withKey(key)));
        final IllegalArgumentException put = assertThrows(IllegalArgumentException.class,
                () -> mapper.putItem(new PutItemRequest().withTableName("Other").withItem(key)));
        final IllegalArgumentException update = assertThrows(IllegalArgumentException.class,
                () -> mapper.updateItem(new UpdateItemRequest().withTableName("Other").withKey(key)));
        final IllegalArgumentException delete = assertThrows(IllegalArgumentException.class,
                () -> mapper.deleteItem(new DeleteItemRequest().withTableName("Other").withKey(key)));

        assertEquals(TABLE_MISMATCH, get.getMessage());
        assertEquals(TABLE_MISMATCH, put.getMessage());
        assertEquals(TABLE_MISMATCH, update.getMessage());
        assertEquals(TABLE_MISMATCH, delete.getMessage());
    }

    @Test
    void mapperScanFillsABlankTableName() {
        when(mockDynamoDBClient.scan(any(ScanRequest.class))).thenReturn(new ScanResult());
        final DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        final ScanRequest request = new ScanRequest();

        try (Stream<TestEntity> rows = mapper.scan(request)) {
            assertFalse(pageIterator(rows, DynamoDBExecutor.class).hasNext());
        }

        assertEquals("TestTable", request.getTableName());
        final ArgumentCaptor<ScanRequest> sent = ArgumentCaptor.forClass(ScanRequest.class);
        verify(mockDynamoDBClient).scan(sent.capture());
        assertEquals("TestTable", sent.getValue().getTableName());
    }

    private static Map<String, AttributeValue> item(final String id) {
        final Map<String, AttributeValue> row = new LinkedHashMap<>();
        row.put("id", new AttributeValue().withS(id));
        return row;
    }

    public static Iterator<?> pageIterator(final Object root, final Class<?> enclosing) {
        final ArrayDeque<Object> pending = new ArrayDeque<>();
        final Set<Object> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        pending.add(root);

        while (!pending.isEmpty()) {
            final Object current = pending.removeFirst();
            if (current == null || !seen.add(current)) {
                continue;
            }
            if (current instanceof Iterator<?> iterator && enclosing.equals(current.getClass().getEnclosingClass())) {
                return iterator;
            }

            Class<?> type = current.getClass();
            while (type != null && type != Object.class) {
                for (final Field field : type.getDeclaredFields()) {
                    if (field.getType().isPrimitive() || Modifier.isStatic(field.getModifiers())) {
                        continue;
                    }
                    field.setAccessible(true);
                    final Object value;
                    try {
                        value = field.get(current);
                    } catch (final IllegalAccessException e) {
                        throw new IllegalStateException(e);
                    }
                    if (value == null || value instanceof String || value instanceof Number || value instanceof Class<?> || value instanceof Enum<?>) {
                        continue;
                    }
                    if (value instanceof Object[] array) {
                        for (final Object element : array) {
                            if (element != null) {
                                pending.add(element);
                            }
                        }
                    } else {
                        pending.add(value);
                    }
                }
                type = type.getSuperclass();
            }
        }

        throw new AssertionError("No page iterator enclosed by " + enclosing.getName());
    }

    public static Level enableDebug(final Class<?> type) {
        final String name = type.getName();
        final Logger jul = Logger.getLogger(name);
        final Level previous = jul.getLevel();
        jul.setLevel(Level.ALL);
        try {
            final Class<?> levelClass = Class.forName("org.apache.logging.log4j.Level");
            final Object debug = levelClass.getField("DEBUG").get(null);
            final Class<?> configurator = Class.forName("org.apache.logging.log4j.core.config.Configurator");
            configurator.getMethod("setLevel", String.class, levelClass).invoke(null, name, debug);
        } catch (final Throwable ignored) {
            // Log4j 2 is not the active backend.
        }
        try {
            final Class<?> levelClass = Class.forName("org.apache.log4j.Level");
            final Object debug = levelClass.getField("DEBUG").get(null);
            final Class<?> loggerClass = Class.forName("org.apache.log4j.Logger");
            final Object logger = loggerClass.getMethod("getLogger", String.class).invoke(null, name);
            loggerClass.getMethod("setLevel", levelClass).invoke(logger, debug);
        } catch (final Throwable ignored) {
            // reload4j is not the active backend.
        }
        return previous;
    }
}
