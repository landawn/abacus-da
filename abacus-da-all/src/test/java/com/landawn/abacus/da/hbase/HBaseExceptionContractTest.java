/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */
package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.IncompatibleFilterException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.exception.UncheckedIOException;
import com.landawn.abacus.util.AsyncExecutor;

class HBaseExceptionContractTest {

    @Test
    void typedReadsValidateBeforeAcquiringATable() throws IOException {
        final Connection connection = mock(Connection.class);
        when(connection.getAdmin()).thenReturn(mock(Admin.class));
        final HBaseExecutor executor = new HBaseExecutor(connection);
        clearInvocations(connection);

        final Get get = new Get(Bytes.toBytes("row"));
        final AnyGet anyGet = AnyGet.of("row");
        final Collection<AnyGet> anyGets = List.of(anyGet);

        assertThrows(IllegalArgumentException.class, () -> executor.get("table", get, null));
        assertThrows(IllegalArgumentException.class, () -> executor.get("table", List.of(get), null));
        assertThrows(IllegalArgumentException.class, () -> executor.get("table", anyGet, null));
        assertThrows(IllegalArgumentException.class, () -> executor.get("table", anyGets, null));
        assertThrows(IllegalArgumentException.class, () -> executor.get(null, get, null));
        verify(connection, never()).getTable(any(TableName.class));
    }

    @Test
    void submissionFailuresAreSynchronousButTaskFailuresBelongToTheFuture() throws Exception {
        final HBaseExecutor sync = mock(HBaseExecutor.class);
        final Get get = new Get(Bytes.toBytes("row"));
        final AsyncExecutor stopped = new AsyncExecutor(Runnable::run);
        stopped.shutdown();

        assertThrows(IllegalStateException.class, () -> new AsyncHBaseExecutor(sync, stopped).get("table", get));

        final AsyncExecutor rejecting = new AsyncExecutor(task -> {
            throw new RejectedExecutionException("full");
        });
        try {
            assertThrows(RejectedExecutionException.class, () -> new AsyncHBaseExecutor(sync, rejecting).get("table", get));
            verifyNoInteractions(sync);
        } finally {
            rejecting.shutdown();
        }

        final AsyncExecutor inline = new AsyncExecutor(Runnable::run);
        final UncheckedIOException failure = new UncheckedIOException(new IOException("read failed"));
        when(sync.get("table", get)).thenThrow(failure);
        try {
            final var result = assertDoesNotThrow(() -> new AsyncHBaseExecutor(sync, inline).get("table", get));
            assertSame(failure, assertThrows(ExecutionException.class, result::get).getCause());
        } finally {
            inline.shutdown();
        }
    }

    @Test
    void constructorsExposeTheDelegatesDistinctValidationFailures() {
        assertThrows(NegativeArraySizeException.class, () -> AnyGet.of("row", 0, -1));
        assertThrows(NegativeArraySizeException.class, () -> AnyPut.of("row", 0, -1));
        assertThrows(NegativeArraySizeException.class, () -> AnyAppend.of("row", 0, -1));
        assertThrows(NegativeArraySizeException.class, () -> AnyIncrement.of("row", 0, -1));
        assertThrows(NegativeArraySizeException.class, () -> AnyDelete.of("row", 0, -1));
        assertThrows(IllegalArgumentException.class, () -> AnyPut.of(new byte[32768]));
        assertThrows(IllegalArgumentException.class, () -> AnyScan.create().withStartRow(new byte[32768]));
        assertThrows(IllegalArgumentException.class, () -> AnyGet.of("row").setTimestamp(Long.MAX_VALUE));
        assertThrows(IllegalArgumentException.class, () -> AnyScan.create().setTimestamp(Long.MAX_VALUE));
    }

    @Test
    void cellRowMismatchPrecedesFamilyValidation() {
        final KeyValue wrongRow = new KeyValue(Bytes.toBytes("other"), new byte[0], new byte[0], new byte[0]);
        assertThrows(IOException.class, () -> AnyPut.of("row").add(wrongRow));
        assertThrows(IOException.class, () -> AnyDelete.of("row").add(wrongRow));
        assertThrows(IOException.class, () -> AnyIncrement.of("row").add(wrongRow));

        final KeyValue emptyFamily = new KeyValue(Bytes.toBytes("row"), new byte[0], new byte[0], new byte[0]);
        assertThrows(IllegalArgumentException.class, () -> AnyPut.of("row").add(emptyFamily));
    }

    @Test
    void delegatedNullFamilyAndBufferValuesRemainAccepted() {
        assertDoesNotThrow(() -> AnyGet.of("row").addFamily((byte[]) null));
        assertDoesNotThrow(() -> AnyScan.create().addColumn((byte[]) null, null));
        assertDoesNotThrow(() -> AnyPut.of("row").addColumn(null, (ByteBuffer) null, 0, null));
        assertDoesNotThrow(() -> HBaseExecutor.toEntity(Result.EMPTY_RESULT, String.class));
    }

    @Test
    void batchScanFailureDependsOnTheInstalledFilter() {
        final AnyScan scan = AnyScan.create().setFilter(new FilterBase() {
            @Override
            public boolean hasFilterRow() {
                return true;
            }
        });
        assertThrows(IncompatibleFilterException.class, () -> scan.setBatch(1));
    }
}
