package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import com.google.protobuf.Service;
import com.landawn.abacus.exception.UncheckedIOException;

class HBaseDriverFailureTest {
    private static final byte[] ROW = { 1 };
    private static final byte[] FAMILY = { 2 };
    private static final byte[] QUALIFIER = { 3 };

    enum Operation {
        EXISTS_ROW,
        EXISTS_GET,
        EXISTS_BATCH,
        GET_ROW,
        GET_GET,
        GET_BATCH,
        GET_TYPED_ROW,
        PUT,
        PUT_BATCH,
        DELETE_ROW,
        DELETE,
        DELETE_BATCH,
        MUTATE,
        APPEND,
        INCREMENT,
        INCREMENT_COLUMN,
        INCREMENT_DURABLE,
        COPROCESSOR,
        COPROCESSOR_CALLBACK,
        BATCH_COPROCESSOR,
        BATCH_COPROCESSOR_CALLBACK,
        MAPPER_COPROCESSOR,
        MAPPER_COPROCESSOR_CALLBACK,
        MAPPER_BATCH_COPROCESSOR,
        MAPPER_BATCH_COPROCESSOR_CALLBACK
    }

    @ParameterizedTest
    @EnumSource(Operation.class)
    void driverIoFailurePreservesCauseAndClosesTheAcquiredTable(final Operation operation) throws Exception {
        final IOException failure = new IOException("driver operation failed");
        final Table table = mock(Table.class, invocation -> {
            if (invocation.getMethod().getName().equals("close")) {
                return null;
            }
            throw failure;
        });
        final Connection connection = mock(Connection.class);
        when(connection.getAdmin()).thenReturn(mock(Admin.class));
        when(connection.getTable(any(TableName.class))).thenReturn(table);
        final HBaseExecutor executor = new HBaseExecutor(connection);
        try {
            final UncheckedIOException actual = assertThrows(UncheckedIOException.class, () -> {
                switch (operation) {
                    case EXISTS_ROW -> executor.exists("items", (Object) ROW);
                    case EXISTS_GET -> executor.exists("items", new Get(ROW));
                    case EXISTS_BATCH -> executor.exists("items", List.of(new Get(ROW)));
                    case GET_ROW -> executor.get("items", (Object) ROW);
                    case GET_GET -> executor.get("items", new Get(ROW));
                    case GET_BATCH -> executor.get("items", List.of(new Get(ROW)));
                    case GET_TYPED_ROW -> executor.get("items", (Object) ROW, String.class);
                    case PUT -> executor.put("items", new Put(ROW));
                    case PUT_BATCH -> executor.put("items", List.of(new Put(ROW)));
                    case DELETE_ROW -> executor.delete("items", (Object) ROW);
                    case DELETE -> executor.delete("items", new Delete(ROW));
                    case DELETE_BATCH -> executor.delete("items", List.of(new Delete(ROW)));
                    case MUTATE -> executor.mutateRow("items", new RowMutations(ROW));
                    case APPEND -> executor.append("items", new Append(ROW));
                    case INCREMENT -> executor.increment("items", new Increment(ROW));
                    case INCREMENT_COLUMN -> executor.incrementColumnValue("items", ROW, FAMILY, QUALIFIER, 1L);
                    case INCREMENT_DURABLE -> executor.incrementColumnValue("items", ROW, FAMILY, QUALIFIER, 1L, Durability.SYNC_WAL);
                    case COPROCESSOR -> executor.coprocessorService("items", Service.class, ROW, ROW, service -> null);
                    case COPROCESSOR_CALLBACK -> executor.coprocessorService("items", Service.class, ROW, ROW, service -> null, (region, row, value) -> {
                    });
                    case BATCH_COPROCESSOR -> executor.batchCoprocessorService("items", null, null, ROW, ROW, null);
                    case BATCH_COPROCESSOR_CALLBACK -> executor.batchCoprocessorService("items", null, null, ROW, ROW, null, (region, row, value) -> {
                    });
                    case MAPPER_COPROCESSOR -> executor.mapper(HBaseMapperTest.Profile.class, "items", com.landawn.abacus.util.NamingPolicy.CAMEL_CASE)
                            .coprocessorService(Service.class, ROW, ROW, service -> null);
                    case MAPPER_COPROCESSOR_CALLBACK -> executor.mapper(HBaseMapperTest.Profile.class, "items", com.landawn.abacus.util.NamingPolicy.CAMEL_CASE)
                            .coprocessorService(Service.class, ROW, ROW, service -> null, (region, row, value) -> {
                            });
                    case MAPPER_BATCH_COPROCESSOR -> executor.mapper(HBaseMapperTest.Profile.class, "items", com.landawn.abacus.util.NamingPolicy.CAMEL_CASE)
                            .batchCoprocessorService(null, null, ROW, ROW, null);
                    case MAPPER_BATCH_COPROCESSOR_CALLBACK -> executor
                            .mapper(HBaseMapperTest.Profile.class, "items", com.landawn.abacus.util.NamingPolicy.CAMEL_CASE)
                            .batchCoprocessorService(null, null, ROW, ROW, null, (region, row, value) -> {
                            });
                }
            });
            assertSame(failure, actual.getCause());
            verify(table).close();
        } finally {
            executor.close();
        }
    }

    @Test
    void failureToAcquireAdminRetainsTheCauseAndLeavesTheConnectionWithItsCaller() throws Exception {
        final Connection connection = mock(Connection.class);
        final IOException failure = new IOException("admin unavailable");
        when(connection.getAdmin()).thenThrow(failure);
        assertSame(failure, assertThrows(UncheckedIOException.class, () -> new HBaseExecutor(connection)).getCause());
        verify(connection, never()).close();
    }

    @ParameterizedTest
    @MethodSource("closeFailures")
    void connectionCloseFailureIsNotLostWhenAdminClosedSuccessfully(final Throwable failure) throws Exception {
        final Connection connection = mock(Connection.class);
        final Admin admin = mock(Admin.class);
        when(connection.getAdmin()).thenReturn(admin);
        doAnswer(invocation -> {
            throw failure;
        }).when(connection).close();
        final HBaseExecutor executor = new HBaseExecutor(connection);
        final Throwable actual = assertThrows(failure.getClass(), executor::close);
        assertSame(failure, actual);
        verify(admin).close();
        verify(connection).close();
    }

    static Stream<Throwable> closeFailures() {
        return Stream.of(new IOException("I/O"), new IllegalStateException("runtime"), new AssertionError("error"));
    }

    @Test
    void theSameFailureFromBothResourcesIsNotSuppressedOntoItself() throws Exception {
        final Connection connection = mock(Connection.class);
        final Admin admin = mock(Admin.class);
        final IOException failure = new IOException("shared failure");
        when(connection.getAdmin()).thenReturn(admin);
        doAnswer(invocation -> {
            throw failure;
        }).when(admin).close();
        doAnswer(invocation -> {
            throw failure;
        }).when(connection).close();
        final HBaseExecutor executor = new HBaseExecutor(connection);
        assertSame(failure, assertThrows(IOException.class, executor::close));
        assertEquals(0, failure.getSuppressed().length);
        verify(connection).close();
    }

    @Test
    void absentAdminAndAlreadyClosedConnectionNeedNoFurtherClose() throws Exception {
        final Connection connection = mock(Connection.class);
        when(connection.isClosed()).thenReturn(true);
        new HBaseExecutor(connection).close();
        verify(connection, never()).close();
    }
}
