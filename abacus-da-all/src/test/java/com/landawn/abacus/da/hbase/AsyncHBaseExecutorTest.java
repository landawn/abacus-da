/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.ContinuableFuture;

/**
 * Pure unit tests for {@link AsyncHBaseExecutor}: verifies that each async method
 * dispatches to its synchronous counterpart on the wrapped {@link HBaseExecutor}
 * and returns a {@link ContinuableFuture} carrying the same value.
 *
 * <p>The synchronous {@code HBaseExecutor} is mocked via Mockito (no live HBase
 * cluster). A small {@link AsyncExecutor} is used to actually run the dispatched
 * tasks so we can call {@code .get()} on the returned future and confirm the
 * value flows through correctly.</p>
 */
public class AsyncHBaseExecutorTest extends TestBase {

    private AsyncHBaseExecutor newAsync(final HBaseExecutor sync) {
        return new AsyncHBaseExecutor(sync, new AsyncExecutor(2, 4, 30L, java.util.concurrent.TimeUnit.SECONDS));
    }

    @Test
    public void testSync_returnsWrappedExecutor() {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        AsyncHBaseExecutor async = newAsync(sync);
        assertSame(sync, async.sync());
    }

    // ---------------------------------------------------------------------
    // exists(...) variants
    // ---------------------------------------------------------------------

    @Test
    public void testExists_singleGet_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        Get get = new Get("k".getBytes());
        when(sync.exists("tbl", get)).thenReturn(true);

        AsyncHBaseExecutor async = newAsync(sync);
        ContinuableFuture<Boolean> fut = async.exists("tbl", get);
        assertTrue(fut.get());
        verify(sync, times(1)).exists("tbl", get);
    }

    @Test
    public void testExists_listOfGets_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        List<Get> gets = Arrays.asList(new Get("a".getBytes()));
        when(sync.exists(eq("tbl"), eq(gets))).thenReturn(Arrays.asList(Boolean.TRUE));

        AsyncHBaseExecutor async = newAsync(sync);
        List<Boolean> result = async.exists("tbl", gets).get();
        assertEquals(1, result.size());
        assertTrue(result.get(0));
    }

    @Test
    public void testExists_anyGet_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        AnyGet anyGet = AnyGet.of("k");
        when(sync.exists("tbl", anyGet)).thenReturn(true);

        AsyncHBaseExecutor async = newAsync(sync);
        assertTrue(async.exists("tbl", anyGet).get());
    }

    @Test
    public void testExists_collectionOfAnyGet_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        Collection<AnyGet> anyGets = Arrays.asList(AnyGet.of("k"));
        when(sync.exists("tbl", anyGets)).thenReturn(Arrays.asList(Boolean.FALSE));

        AsyncHBaseExecutor async = newAsync(sync);
        assertEquals(Arrays.asList(Boolean.FALSE), async.exists("tbl", anyGets).get());
    }

    // ---------------------------------------------------------------------
    // get(...) variants
    // ---------------------------------------------------------------------

    @Test
    public void testGet_singleGet_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        Get get = new Get("k".getBytes());
        Result expected = Result.create(Collections.emptyList());
        when(sync.get("tbl", get)).thenReturn(expected);

        AsyncHBaseExecutor async = newAsync(sync);
        assertSame(expected, async.get("tbl", get).get());
    }

    @Test
    public void testGet_listOfGets_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        List<Get> gets = Arrays.asList(new Get("k".getBytes()));
        List<Result> expected = Arrays.asList(Result.create(Collections.emptyList()));
        when(sync.get("tbl", gets)).thenReturn(expected);

        AsyncHBaseExecutor async = newAsync(sync);
        assertSame(expected, async.get("tbl", gets).get());
    }

    @Test
    public void testGet_anyGet_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        AnyGet anyGet = AnyGet.of("k");
        Result expected = Result.create(Collections.emptyList());
        when(sync.get("tbl", anyGet)).thenReturn(expected);

        AsyncHBaseExecutor async = newAsync(sync);
        assertSame(expected, async.get("tbl", anyGet).get());
    }

    @Test
    public void testGet_collectionOfAnyGet_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        Collection<AnyGet> anyGets = Arrays.asList(AnyGet.of("k"));
        List<Result> expected = Arrays.asList(Result.create(Collections.emptyList()));
        when(sync.get("tbl", anyGets)).thenReturn(expected);

        AsyncHBaseExecutor async = newAsync(sync);
        assertSame(expected, async.get("tbl", anyGets).get());
    }

    @Test
    public void testGet_singleGetWithClass_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        Get get = new Get("k".getBytes());
        when(sync.get("tbl", get, String.class)).thenReturn("hello");

        AsyncHBaseExecutor async = newAsync(sync);
        assertEquals("hello", async.get("tbl", get, String.class).get());
    }

    @Test
    public void testGet_listGetsWithClass_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        List<Get> gets = Arrays.asList(new Get("k".getBytes()));
        when(sync.get("tbl", gets, String.class)).thenReturn(Arrays.asList("v"));

        AsyncHBaseExecutor async = newAsync(sync);
        assertEquals(Arrays.asList("v"), async.get("tbl", gets, String.class).get());
    }

    @Test
    public void testGet_anyGetWithClass_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        AnyGet anyGet = AnyGet.of("k");
        when(sync.get("tbl", anyGet, String.class)).thenReturn("v");

        AsyncHBaseExecutor async = newAsync(sync);
        assertEquals("v", async.get("tbl", anyGet, String.class).get());
    }

    @Test
    public void testGet_collectionAnyGetWithClass_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        Collection<AnyGet> anyGets = Arrays.asList(AnyGet.of("k"));
        when(sync.get("tbl", anyGets, String.class)).thenReturn(Arrays.asList("v"));

        AsyncHBaseExecutor async = newAsync(sync);
        assertEquals(Arrays.asList("v"), async.get("tbl", anyGets, String.class).get());
    }

    // ---------------------------------------------------------------------
    // put(...) variants - they all return Void
    // ---------------------------------------------------------------------

    @Test
    public void testPut_singlePut_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        Put put = new Put("k".getBytes());

        AsyncHBaseExecutor async = newAsync(sync);
        async.put("tbl", put).get();
        verify(sync, times(1)).put("tbl", put);
    }

    @Test
    public void testPut_listOfPuts_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        List<Put> puts = Arrays.asList(new Put("k".getBytes()));

        AsyncHBaseExecutor async = newAsync(sync);
        async.put("tbl", puts).get();
        verify(sync, times(1)).put("tbl", puts);
    }

    @Test
    public void testPut_anyPut_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        AnyPut anyPut = AnyPut.of("k");

        AsyncHBaseExecutor async = newAsync(sync);
        async.put("tbl", anyPut).get();
        verify(sync, times(1)).put("tbl", anyPut);
    }

    @Test
    public void testPut_collectionOfAnyPut_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        Collection<AnyPut> anyPuts = Arrays.asList(AnyPut.of("k"));

        AsyncHBaseExecutor async = newAsync(sync);
        async.put("tbl", anyPuts).get();
        verify(sync, times(1)).put("tbl", anyPuts);
    }

    // ---------------------------------------------------------------------
    // delete(...) variants
    // ---------------------------------------------------------------------

    @Test
    public void testDelete_singleDelete_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        Delete del = new Delete("k".getBytes());

        AsyncHBaseExecutor async = newAsync(sync);
        async.delete("tbl", del).get();
        verify(sync, times(1)).delete("tbl", del);
    }

    @Test
    public void testDelete_listOfDeletes_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        List<Delete> deletes = Arrays.asList(new Delete("k".getBytes()));

        AsyncHBaseExecutor async = newAsync(sync);
        async.delete("tbl", deletes).get();
        verify(sync, times(1)).delete("tbl", deletes);
    }

    @Test
    public void testDelete_anyDelete_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        AnyDelete anyDel = AnyDelete.of("k");

        AsyncHBaseExecutor async = newAsync(sync);
        async.delete("tbl", anyDel).get();
        verify(sync, times(1)).delete("tbl", anyDel);
    }

    @Test
    public void testDelete_collectionOfAnyDelete_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        Collection<AnyDelete> anyDeletes = Arrays.asList(AnyDelete.of("k"));

        AsyncHBaseExecutor async = newAsync(sync);
        async.delete("tbl", anyDeletes).get();
        verify(sync, times(1)).delete("tbl", anyDeletes);
    }

    // ---------------------------------------------------------------------
    // scan(...) variants
    // ---------------------------------------------------------------------

    @Test
    public void testScan_byFamily_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        com.landawn.abacus.util.stream.Stream<Result> stream = com.landawn.abacus.util.stream.Stream.empty();
        when(sync.scan("tbl", "cf")).thenReturn(stream);

        AsyncHBaseExecutor async = newAsync(sync);
        assertSame(stream, async.scan("tbl", "cf").get());
    }

    @Test
    public void testScan_byFamilyAndQualifier_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        com.landawn.abacus.util.stream.Stream<Result> stream = com.landawn.abacus.util.stream.Stream.empty();
        when(sync.scan("tbl", "cf", "q")).thenReturn(stream);

        AsyncHBaseExecutor async = newAsync(sync);
        assertSame(stream, async.scan("tbl", "cf", "q").get());
    }

    @Test
    public void testScan_byFamilyBytes_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        com.landawn.abacus.util.stream.Stream<Result> stream = com.landawn.abacus.util.stream.Stream.empty();
        byte[] fam = "cf".getBytes();
        when(sync.scan("tbl", fam)).thenReturn(stream);

        AsyncHBaseExecutor async = newAsync(sync);
        assertSame(stream, async.scan("tbl", fam).get());
    }

    @Test
    public void testScan_byFamilyAndQualifierBytes_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        com.landawn.abacus.util.stream.Stream<Result> stream = com.landawn.abacus.util.stream.Stream.empty();
        byte[] fam = "cf".getBytes();
        byte[] q = "q".getBytes();
        when(sync.scan("tbl", fam, q)).thenReturn(stream);

        AsyncHBaseExecutor async = newAsync(sync);
        assertSame(stream, async.scan("tbl", fam, q).get());
    }

    @Test
    public void testScan_anyScan_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        com.landawn.abacus.util.stream.Stream<Result> stream = com.landawn.abacus.util.stream.Stream.empty();
        AnyScan anyScan = AnyScan.create();
        when(sync.scan("tbl", anyScan)).thenReturn(stream);

        AsyncHBaseExecutor async = newAsync(sync);
        assertSame(stream, async.scan("tbl", anyScan).get());
    }

    @Test
    public void testScan_rawScan_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        com.landawn.abacus.util.stream.Stream<Result> stream = com.landawn.abacus.util.stream.Stream.empty();
        org.apache.hadoop.hbase.client.Scan scan = new org.apache.hadoop.hbase.client.Scan();
        when(sync.scan("tbl", scan)).thenReturn(stream);

        AsyncHBaseExecutor async = newAsync(sync);
        assertSame(stream, async.scan("tbl", scan).get());
    }

    // ---------------------------------------------------------------------
    // mutateRow / append / increment
    // ---------------------------------------------------------------------

    @Test
    public void testMutateRow_anyRowMutations_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        AnyRowMutations rm = AnyRowMutations.of("k");

        AsyncHBaseExecutor async = newAsync(sync);
        async.mutateRow("tbl", rm).get();
        verify(sync, times(1)).mutateRow("tbl", rm);
    }

    @Test
    public void testMutateRow_rawRowMutations_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        org.apache.hadoop.hbase.client.RowMutations rm = new org.apache.hadoop.hbase.client.RowMutations("k".getBytes());

        AsyncHBaseExecutor async = newAsync(sync);
        async.mutateRow("tbl", rm).get();
        verify(sync, times(1)).mutateRow("tbl", rm);
    }

    @Test
    public void testAppend_anyAppend_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        AnyAppend ap = AnyAppend.of("k");
        Result expected = Result.create(Collections.emptyList());
        when(sync.append("tbl", ap)).thenReturn(expected);

        AsyncHBaseExecutor async = newAsync(sync);
        assertSame(expected, async.append("tbl", ap).get());
    }

    @Test
    public void testAppend_rawAppend_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        org.apache.hadoop.hbase.client.Append ap = new org.apache.hadoop.hbase.client.Append("k".getBytes());
        Result expected = Result.create(Collections.emptyList());
        when(sync.append("tbl", ap)).thenReturn(expected);

        AsyncHBaseExecutor async = newAsync(sync);
        assertSame(expected, async.append("tbl", ap).get());
    }

    @Test
    public void testIncrement_anyIncrement_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        AnyIncrement inc = AnyIncrement.of("k");
        Result expected = Result.create(Collections.emptyList());
        when(sync.increment("tbl", inc)).thenReturn(expected);

        AsyncHBaseExecutor async = newAsync(sync);
        assertSame(expected, async.increment("tbl", inc).get());
    }

    @Test
    public void testIncrement_rawIncrement_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        org.apache.hadoop.hbase.client.Increment inc = new org.apache.hadoop.hbase.client.Increment("k".getBytes());
        Result expected = Result.create(Collections.emptyList());
        when(sync.increment("tbl", inc)).thenReturn(expected);

        AsyncHBaseExecutor async = newAsync(sync);
        assertSame(expected, async.increment("tbl", inc).get());
    }

    @Test
    public void testIncrementColumnValue_stringFamilyQualifier_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        when(sync.incrementColumnValue(anyString(), any(), anyString(), anyString(), anyLong())).thenReturn(42L);

        AsyncHBaseExecutor async = newAsync(sync);
        assertEquals(42L, async.incrementColumnValue("tbl", "k", "cf", "q", 1L).get().longValue());
    }

    @Test
    public void testIncrementColumnValue_stringFamilyQualifierWithDurability_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        when(sync.incrementColumnValue(anyString(), any(), anyString(), anyString(), anyLong(), any(Durability.class))).thenReturn(100L);

        AsyncHBaseExecutor async = newAsync(sync);
        assertEquals(100L, async.incrementColumnValue("tbl", "k", "cf", "q", 5L, Durability.SYNC_WAL).get().longValue());
    }

    @Test
    public void testIncrementColumnValue_byteArrayFamilyQualifier_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        when(sync.incrementColumnValue(anyString(), any(), any(byte[].class), any(byte[].class), anyLong())).thenReturn(7L);

        AsyncHBaseExecutor async = newAsync(sync);
        assertEquals(7L, async.incrementColumnValue("tbl", "k", "cf".getBytes(), "q".getBytes(), 3L).get().longValue());
    }

    @Test
    public void testIncrementColumnValue_byteArrayFamilyQualifierWithDurability_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        when(sync.incrementColumnValue(anyString(), any(), any(byte[].class), any(byte[].class), anyLong(), any(Durability.class))).thenReturn(8L);

        AsyncHBaseExecutor async = newAsync(sync);
        assertEquals(8L, async.incrementColumnValue("tbl", "k", "cf".getBytes(), "q".getBytes(), 4L, Durability.ASYNC_WAL).get().longValue());
    }

    // ---------------------------------------------------------------------
    // coprocessorService
    // ---------------------------------------------------------------------

    @Test
    public void testCoprocessorService_singleRow_dispatchesToSync() throws Exception {
        HBaseExecutor sync = mock(HBaseExecutor.class);
        org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel ch = mock(org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel.class);
        when(sync.coprocessorService("tbl", "k")).thenReturn(ch);

        AsyncHBaseExecutor async = newAsync(sync);
        assertSame(ch, async.coprocessorService("tbl", "k").get());
    }

    // ---------------------------------------------------------------------
    // End-to-end integration with a real HBaseExecutor (mocked Connection).
    // ---------------------------------------------------------------------

    @Test
    public void testAsyncAccessor_fromHBaseExecutor_returnsLinkedAsyncExecutor() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        when(conn.getAdmin()).thenReturn(admin);
        HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            AsyncHBaseExecutor async = executor.async();
            assertNotNull(async);
            assertSame(executor, async.sync());
        } finally {
            executor.close();
        }
    }
}
