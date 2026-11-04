/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.exceptions.ConnectionClosedException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.exception.UncheckedIOException;
import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.stream.Stream;

class AsyncHBaseExecutorReview20261103Test extends TestBase {

    // Pins the class Javadoc "Lifecycle" paragraph: HBaseExecutor.close() closes the shared connection but leaves the
    // AsyncExecutor running, so a later async call is still accepted and its failure arrives through the future (and,
    // for a scan, through the stream when it fetches rows).
    @Test
    void closeKeepsTheAsyncExecutorAndLaterOperationsFailThroughTheirFutureOrStream() throws Exception {
        final Connection conn = mock(Connection.class);
        when(conn.getAdmin()).thenReturn(mock(Admin.class));

        final AsyncExecutor pool = new AsyncExecutor(1, 1, 1L, TimeUnit.SECONDS);

        try {
            final HBaseExecutor executor = new HBaseExecutor(conn, pool);
            executor.close();
            verify(conn).close();

            // What a closed connection reports once an operation reaches it.
            final ConnectionClosedException closed = new ConnectionClosedException("connection closed");
            when(conn.getTable(any(TableName.class))).thenThrow(closed);

            final ContinuableFuture<Boolean> exists = executor.async().exists("t", new Get(Bytes.toBytes("r")));
            final ContinuableFuture<Stream<Result>> scan = executor.async().scan("t", AnyScan.create());

            assertAll(() -> {
                final ExecutionException e = assertThrows(ExecutionException.class, exists::get);
                assertInstanceOf(UncheckedIOException.class, e.getCause());
                assertEquals(closed, e.getCause().getCause());
            }, () -> {
                try (Stream<Result> stream = scan.get()) { // the future itself succeeds: the stream is lazy
                    final UncheckedIOException e = assertThrows(UncheckedIOException.class, stream::count);
                    assertEquals(closed, e.getCause());
                }
            }, () -> assertEquals(Integer.valueOf(1), pool.execute(() -> 1).get())); // not shut down by close()
        } finally {
            pool.shutdown();
        }
    }
}
