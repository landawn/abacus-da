/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */
package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
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
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.IncompatibleFilterException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import com.google.protobuf.Service;
import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.exception.UncheckedIOException;
import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.HBaseColumn;

class HBaseExceptionContractTest {

    @Test
    void mapperValidatesEntityMetadataBeforeLaterParameters() {
        final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> new HBaseExecutor.HBaseMapper<>(EntityWithoutId.class, null, null, null));

        assertTrue(failure.getMessage().contains("No or multiple ids"));
    }

    @Test
    void typedBatchReadAcquiresTableBeforeDelegatingNullElements() throws IOException {
        final Connection connection = mock(Connection.class);
        when(connection.getAdmin()).thenReturn(mock(Admin.class));
        final HBaseExecutor executor = new HBaseExecutor(connection);
        final IOException failure = new IOException("table unavailable");
        when(connection.getTable(any(TableName.class))).thenThrow(failure);

        final UncheckedIOException actual = assertThrows(UncheckedIOException.class,
                () -> executor.get("table", Arrays.asList((Get) null), String.class));

        assertSame(failure, actual.getCause());
    }

    @Test
    void mapperWritesPropagateByteConversionFailuresBeforeAcquiringTable() throws IOException {
        final Connection connection = mock(Connection.class);
        when(connection.getAdmin()).thenReturn(mock(Admin.class));
        final HBaseExecutor executor = new HBaseExecutor(connection);
        final HBaseExecutor.HBaseMapper<OpaqueKeyEntity, Object> mapper = executor.mapper(OpaqueKeyEntity.class, "table", null);
        final IllegalStateException failure = new IllegalStateException("cannot encode key");
        final Object key = new Object() {
            @Override
            public String toString() {
                throw failure;
            }
        };
        final OpaqueKeyEntity entity = new OpaqueKeyEntity();
        entity.setId(key);

        assertSame(failure, assertThrows(IllegalStateException.class, () -> mapper.put(entity)));
        assertSame(failure, assertThrows(IllegalStateException.class, () -> mapper.put((Collection<OpaqueKeyEntity>) List.of(entity))));
        assertSame(failure, assertThrows(IllegalStateException.class, () -> mapper.delete(entity)));
        assertSame(failure, assertThrows(IllegalStateException.class, () -> mapper.delete((Collection<OpaqueKeyEntity>) List.of(entity))));
        verify(connection, never()).getTable(any(TableName.class));
    }

    @Test
    void mapperReadsPropagateUnsupportedRowKeyMetadata() throws IOException {
        final Connection connection = mock(Connection.class);
        final Table table = mock(Table.class);
        when(connection.getAdmin()).thenReturn(mock(Admin.class));
        when(connection.getTable(any(TableName.class))).thenReturn(table);
        final Result row = Result.create(List.of(new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("family"), new byte[0], Bytes.toBytes("value"))));
        when(table.get(any(Get.class))).thenReturn(row);
        when(table.get(anyList())).thenReturn(new Result[] { row });
        final HBaseExecutor.HBaseMapper<VersionedKeyEntity, String> mapper = new HBaseExecutor(connection).mapper(VersionedKeyEntity.class, "table", null);

        assertThrows(IllegalArgumentException.class, () -> mapper.get("row"));
        assertThrows(IllegalArgumentException.class, () -> mapper.get((Collection<String>) List.of("row")));
        assertThrows(IllegalArgumentException.class, () -> mapper.get(AnyGet.of("row")));
        assertThrows(IllegalArgumentException.class, () -> mapper.get(List.of(AnyGet.of("row"))));
    }

    @Test
    void coprocessorBoundaryConversionFailuresPreserveCauseAndCloseTable() throws IOException {
        final Connection connection = mock(Connection.class);
        final Table table = mock(Table.class);
        when(connection.getAdmin()).thenReturn(mock(Admin.class));
        when(connection.getTable(any(TableName.class))).thenReturn(table);
        final HBaseExecutor executor = new HBaseExecutor(connection);

        for (final Throwable failure : List.of(new IllegalStateException("cannot encode boundary"), new AssertionError("boundary conversion error"))) {
            final Object boundary = new Object() {
                @Override
                public String toString() {
                    if (failure instanceof Error error) {
                        throw error;
                    }
                    throw (RuntimeException) failure;
                }
            };

            assertSame(failure, assertThrows(failure.getClass(), () -> executor.coprocessorService("table", Service.class, boundary, null, service -> null)));
            assertSame(failure, assertThrows(failure.getClass(),
                    () -> executor.coprocessorService("table", Service.class, boundary, null, service -> null, (region, row, value) -> { })));
            assertSame(failure, assertThrows(failure.getClass(), () -> executor.batchCoprocessorService("table", null, null, boundary, null, null)));
            assertSame(failure, assertThrows(failure.getClass(),
                    () -> executor.batchCoprocessorService("table", null, null, boundary, null, null, (region, row, value) -> { })));
        }

        verify(table, times(8)).close();
    }

    @Test
    void explicitColumnTimestampOverridesByteBufferPutDefault() {
        final AnyPut put = AnyPut.of(ByteBuffer.wrap(Bytes.toBytes("row")), 10L)
                .addColumn("family", "default", "value")
                .addColumn("family", "explicit", 20L, "value");

        assertEquals(10L, put.getTimestamp());
        assertEquals(10L, put.get("family", "default").get(0).getTimestamp());
        assertEquals(20L, put.get("family", "explicit").get(0).getTimestamp());
    }

    public static final class OpaqueKeyEntity {
        @Id
        private Object id;

        public Object getId() {
            return id;
        }

        public void setId(final Object id) {
            this.id = id;
        }
    }

    public static final class VersionedKeyEntity {
        @Id
        private HBaseColumn<String> id;

        public HBaseColumn<String> getId() {
            return id;
        }

        public void setId(final HBaseColumn<String> id) {
            this.id = id;
        }
    }

    public static final class EntityWithoutId {
        private String value;

        public String getValue() {
            return value;
        }

        public void setValue(final String value) {
            this.value = value;
        }
    }

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
