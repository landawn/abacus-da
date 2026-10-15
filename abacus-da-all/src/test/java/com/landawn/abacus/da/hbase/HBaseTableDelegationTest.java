package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Service;
import com.google.protobuf.StringValue;
import com.landawn.abacus.util.NamingPolicy;

class HBaseTableDelegationTest {
    private static final byte[] START = Bytes.toBytes("start");
    private static final byte[] END = Bytes.toBytes("end");
    private static final byte[] REGION = Bytes.toBytes("region");

    @Test
    void mapperCoprocessorsForwardRangesAndReturnRegionResults() throws Throwable {
        final Table table = mock(Table.class);
        final HBaseExecutor executor = executor(table);
        try {
            final var mapper = executor.mapper(HBaseMapperTest.Profile.class, "people", NamingPolicy.CAMEL_CASE);
            final Batch.Call<Service, Long> callable = service -> 7L;
            final Map<byte[], Long> expected = Map.of(REGION, 7L);
            when(table.coprocessorService(eq(Service.class), aryEq(START), aryEq(END), eq(callable))).thenReturn(expected);
            assertSame(expected, mapper.coprocessorService(Service.class, "start", "end", callable));

            final Descriptors.MethodDescriptor method = mock(Descriptors.MethodDescriptor.class);
            final StringValue request = StringValue.of("request");
            final StringValue response = StringValue.of("response");
            final Map<byte[], StringValue> batchExpected = Map.of(REGION, response);
            when(table.batchCoprocessorService(eq(method), eq(request), aryEq(START), aryEq(END), eq(response))).thenReturn(batchExpected);
            assertSame(batchExpected, mapper.batchCoprocessorService(method, request, "start", "end", response));
            verify(table, times(2)).close();
        } finally {
            executor.close();
        }
    }

    @Test
    void mapperCoprocessorCallbacksReceiveTheDriverRegionAndValues() throws Throwable {
        final Table table = mock(Table.class);
        final HBaseExecutor executor = executor(table);
        try {
            final var mapper = executor.mapper(HBaseMapperTest.Profile.class, "people", NamingPolicy.CAMEL_CASE);
            final Batch.Call<Service, Long> callable = service -> 7L;
            final List<Object> delivered = new ArrayList<>();
            final Batch.Callback<Long> callback = (region, row, value) -> {
                assertSame(REGION, region);
                assertSame(START, row);
                delivered.add(value);
            };
            doAnswer(invocation -> {
                callback.update(REGION, START, 7L);
                return null;
            }).when(table).coprocessorService(eq(Service.class), aryEq(START), aryEq(END), eq(callable), eq(callback));
            mapper.coprocessorService(Service.class, "start", "end", callable, callback);

            final Descriptors.MethodDescriptor method = mock(Descriptors.MethodDescriptor.class);
            final StringValue request = StringValue.of("request");
            final StringValue response = StringValue.of("response");
            final Batch.Callback<StringValue> batchCallback = (region, row, value) -> {
                assertSame(REGION, region);
                assertSame(START, row);
                delivered.add(value);
            };
            doAnswer(invocation -> {
                batchCallback.update(REGION, START, response);
                return null;
            }).when(table).batchCoprocessorService(eq(method), eq(request), aryEq(START), aryEq(END), eq(response), eq(batchCallback));
            mapper.batchCoprocessorService(method, request, "start", "end", response, batchCallback);
            assertEquals(List.of(7L, response), delivered);
            verify(table, times(2)).close();
        } finally {
            executor.close();
        }
    }

    @Test
    void objectRowKeysAreEncodedForExistsGetAndDelete() throws Exception {
        final Table table = mock(Table.class);
        final HBaseExecutor executor = executor(table);
        final byte[] row = Bytes.toBytes("row");
        final Result result = Result.create(List.of(new KeyValue(row, Bytes.toBytes("info"), Bytes.toBytes("value"), Bytes.toBytes("Ada"))));
        when(table.exists(any(Get.class))).thenReturn(true);
        when(table.get(any(Get.class))).thenReturn(result);
        try {
            final Object key = "row";
            assertTrue(executor.exists("people", key));
            assertSame(result, executor.get("people", key));
            assertEquals("Ada", executor.get("people", key, String.class));
            executor.delete("people", key);
            final ArgumentCaptor<Get> reads = ArgumentCaptor.forClass(Get.class);
            verify(table, times(2)).get(reads.capture());
            for (final Get read : reads.getAllValues()) {
                assertArrayEquals(row, read.getRow());
            }
            final ArgumentCaptor<Delete> deletion = ArgumentCaptor.forClass(Delete.class);
            verify(table).delete(deletion.capture());
            assertArrayEquals(row, deletion.getValue().getRow());
            verify(table, times(4)).close();
        } finally {
            executor.close();
        }
    }

    private static HBaseExecutor executor(final Table table) throws Exception {
        final Connection connection = mock(Connection.class);
        when(connection.getAdmin()).thenReturn(mock(Admin.class));
        when(connection.getTable(TableName.valueOf("people"))).thenReturn(table);
        return new HBaseExecutor(connection);
    }
}
