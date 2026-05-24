/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.annotation.Column;
import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.annotation.Table;
import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.da.hbase.HBaseExecutor.HBaseMapper;
import com.landawn.abacus.da.hbase.annotation.ColumnFamily;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Pure unit tests for {@link HBaseMapper} (no live HBase). All Table/Connection
 * interactions are mocked via Mockito.
 */
public class HBaseMapperTest extends TestBase {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Table("users")
    @ColumnFamily("info")
    public static class User {
        @Id
        private String userId;
        @Column("user_name")
        private String name;
    }

    // Helper: create executor with mocked Connection/Admin/Table.
    private static class Mocks {
        final Connection conn;
        final Admin admin;
        final org.apache.hadoop.hbase.client.Table table;
        final HBaseExecutor executor;
        final HBaseMapper<User, String> mapper;

        Mocks() throws Exception {
            conn = mock(Connection.class);
            admin = mock(Admin.class);
            table = mock(org.apache.hadoop.hbase.client.Table.class);
            when(conn.getAdmin()).thenReturn(admin);
            when(conn.getTable(any(TableName.class))).thenReturn(table);
            executor = new HBaseExecutor(conn);
            mapper = executor.mapper(User.class);
        }
    }

    // ---------------------------------------------------------------------
    // Constructor / accessor sanity
    // ---------------------------------------------------------------------

    @Test
    public void testMapper_returnsNonNull() throws Exception {
        Mocks m = new Mocks();
        assertNotNull(m.mapper);
    }

    // ---------------------------------------------------------------------
    // exists() — by row key, by collection of row keys, by AnyGet, by list of AnyGet
    // ---------------------------------------------------------------------

    @Test
    public void testExists_byRowKey_delegates() throws Exception {
        Mocks m = new Mocks();
        when(m.table.exists(any(org.apache.hadoop.hbase.client.Get.class))).thenReturn(true);

        assertTrue(m.mapper.exists("user-1"));
        verify(m.table, atLeastOnce()).exists(any(org.apache.hadoop.hbase.client.Get.class));
    }

    @Test
    public void testExists_byAnyGet_delegates() throws Exception {
        Mocks m = new Mocks();
        when(m.table.exists(any(org.apache.hadoop.hbase.client.Get.class))).thenReturn(true);

        assertTrue(m.mapper.exists(AnyGet.of("user-x")));
        verify(m.table, atLeastOnce()).exists(any(org.apache.hadoop.hbase.client.Get.class));
    }

    @Test
    public void testExists_collectionOfRowKeys_delegates() throws Exception {
        Mocks m = new Mocks();
        when(m.table.exists(any(List.class))).thenReturn(new boolean[] { true, false });

        List<Boolean> result = m.mapper.exists(Arrays.asList("r1", "r2"));
        assertEquals(2, result.size());
        assertEquals(Boolean.TRUE, result.get(0));
        assertEquals(Boolean.FALSE, result.get(1));
    }

    @Test
    public void testExists_listOfAnyGet_delegates() throws Exception {
        Mocks m = new Mocks();
        when(m.table.exists(any(List.class))).thenReturn(new boolean[] { true });

        List<Boolean> result = m.mapper.exists(Arrays.asList(AnyGet.of("rk")));
        assertEquals(1, result.size());
        assertEquals(Boolean.TRUE, result.get(0));
    }

    // ---------------------------------------------------------------------
    // get() — by row key, AnyGet, list/collection
    // ---------------------------------------------------------------------

    @Test
    public void testGet_byRowKey_delegates() throws Exception {
        Mocks m = new Mocks();
        Cell cell = new KeyValue(Bytes.toBytes("u1"), Bytes.toBytes("info"), Bytes.toBytes("user_name"), Bytes.toBytes("Alice"));
        Result result = Result.create(Arrays.<Cell> asList(cell));
        when(m.table.get(any(org.apache.hadoop.hbase.client.Get.class))).thenReturn(result);

        User user = m.mapper.get("u1");
        assertNotNull(user);
        assertEquals("u1", user.getUserId());
        assertEquals("Alice", user.getName());
    }

    @Test
    public void testGet_byAnyGet_delegates() throws Exception {
        Mocks m = new Mocks();
        Cell cell = new KeyValue(Bytes.toBytes("u2"), Bytes.toBytes("info"), Bytes.toBytes("user_name"), Bytes.toBytes("Bob"));
        Result result = Result.create(Arrays.<Cell> asList(cell));
        when(m.table.get(any(org.apache.hadoop.hbase.client.Get.class))).thenReturn(result);

        User user = m.mapper.get(AnyGet.of("u2"));
        assertNotNull(user);
        assertEquals("Bob", user.getName());
    }

    @Test
    public void testGet_collectionOfRowKeys_delegates() throws Exception {
        Mocks m = new Mocks();
        Cell c1 = new KeyValue(Bytes.toBytes("r1"), Bytes.toBytes("info"), Bytes.toBytes("user_name"), Bytes.toBytes("A"));
        Result[] results = new Result[] { Result.create(Arrays.<Cell> asList(c1)) };
        when(m.table.get(any(List.class))).thenReturn(results);

        List<User> users = m.mapper.get(Arrays.asList("r1"));
        assertEquals(1, users.size());
        assertEquals("A", users.get(0).getName());
    }

    @Test
    public void testGet_listOfAnyGet_delegates() throws Exception {
        Mocks m = new Mocks();
        Cell c1 = new KeyValue(Bytes.toBytes("r1"), Bytes.toBytes("info"), Bytes.toBytes("user_name"), Bytes.toBytes("A"));
        Result[] results = new Result[] { Result.create(Arrays.<Cell> asList(c1)) };
        when(m.table.get(any(List.class))).thenReturn(results);

        List<User> users = m.mapper.get(Arrays.asList(AnyGet.of("r1")));
        assertEquals(1, users.size());
    }

    // ---------------------------------------------------------------------
    // put() — entity, collection of entities, AnyPut, list of AnyPut
    // ---------------------------------------------------------------------

    @Test
    public void testPut_entity_delegatesToTable() throws Exception {
        Mocks m = new Mocks();
        User u = new User("u1", "Alice");
        m.mapper.put(u);
        verify(m.table, atLeastOnce()).put(any(org.apache.hadoop.hbase.client.Put.class));
    }

    @Test
    public void testPut_collectionOfEntities_delegatesToTable() throws Exception {
        Mocks m = new Mocks();
        m.mapper.put(Arrays.asList(new User("u1", "Alice"), new User("u2", "Bob")));
        verify(m.table, atLeastOnce()).put(any(List.class));
    }

    @Test
    public void testPut_anyPut_delegatesToTable() throws Exception {
        Mocks m = new Mocks();
        AnyPut put = AnyPut.of("rk").addColumn("info", "user_name", "Alice");
        m.mapper.put(put);
        verify(m.table).put(put.val());
    }

    @Test
    public void testPut_listOfAnyPut_delegatesToTable() throws Exception {
        Mocks m = new Mocks();
        AnyPut p1 = AnyPut.of("rk1").addColumn("info", "user_name", "Alice");
        AnyPut p2 = AnyPut.of("rk2").addColumn("info", "user_name", "Bob");
        m.mapper.put(Arrays.asList(p1, p2));
        verify(m.table).put(any(List.class));
    }

    // ---------------------------------------------------------------------
    // delete() — entity, collection, by row key, AnyDelete, list of AnyDelete
    // ---------------------------------------------------------------------

    @Test
    public void testDeleteByRowKey_single_delegatesToTable() throws Exception {
        Mocks m = new Mocks();
        m.mapper.deleteByRowKey("rk-del");
        verify(m.table).delete(any(org.apache.hadoop.hbase.client.Delete.class));
    }

    @Test
    public void testDeleteByRowKey_collection_delegatesToTable() throws Exception {
        Mocks m = new Mocks();
        m.mapper.deleteByRowKey(Arrays.asList("rk1", "rk2"));
        verify(m.table).delete(any(List.class));
    }

    @Test
    public void testDelete_entity_delegatesToTable() throws Exception {
        Mocks m = new Mocks();
        User u = new User("u-del", "Alice");
        m.mapper.delete(u);
        verify(m.table).delete(any(org.apache.hadoop.hbase.client.Delete.class));
    }

    @Test
    public void testDelete_collectionOfEntities_delegatesToTable() throws Exception {
        Mocks m = new Mocks();
        m.mapper.delete(Arrays.asList(new User("u1", "A"), new User("u2", "B")));
        verify(m.table).delete(any(List.class));
    }

    @Test
    public void testDelete_anyDelete_delegatesToTable() throws Exception {
        Mocks m = new Mocks();
        AnyDelete del = AnyDelete.of("rk");
        m.mapper.delete(del);
        verify(m.table).delete(del.val());
    }

    @Test
    public void testDelete_listOfAnyDelete_delegatesToTable() throws Exception {
        Mocks m = new Mocks();
        m.mapper.delete(Arrays.asList(AnyDelete.of("r1"), AnyDelete.of("r2")));
        verify(m.table).delete(any(List.class));
    }

    // ---------------------------------------------------------------------
    // mutateRow / append / increment
    // ---------------------------------------------------------------------

    @Test
    public void testMutateRow_delegatesToTable() throws Exception {
        Mocks m = new Mocks();
        org.apache.hadoop.hbase.client.Put put = new org.apache.hadoop.hbase.client.Put(Bytes.toBytes("rk"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("user_name"), Bytes.toBytes("Alice"));
        AnyRowMutations rm = AnyRowMutations.of("rk").add(put);
        m.mapper.mutateRow(rm);
        verify(m.table).mutateRow(rm.val());
    }

    @Test
    public void testAppend_delegatesToTable() throws Exception {
        Mocks m = new Mocks();
        AnyAppend append = AnyAppend.of("rk").addColumn("info", "user_name", "X");
        Cell cell = new KeyValue(Bytes.toBytes("rk"), Bytes.toBytes("info"), Bytes.toBytes("user_name"), Bytes.toBytes("ABCX"));
        Result expected = Result.create(Arrays.<Cell> asList(cell));
        when(m.table.append(any(org.apache.hadoop.hbase.client.Append.class))).thenReturn(expected);

        Result actual = m.mapper.append(append);
        assertSame(expected, actual);
    }

    @Test
    public void testIncrement_delegatesToTable() throws Exception {
        Mocks m = new Mocks();
        AnyIncrement inc = AnyIncrement.of("rk").addColumn("info", "counter", 1L);
        Cell cell = new KeyValue(Bytes.toBytes("rk"), Bytes.toBytes("info"), Bytes.toBytes("counter"), Bytes.toBytes(42L));
        Result expected = Result.create(Arrays.<Cell> asList(cell));
        when(m.table.increment(any(org.apache.hadoop.hbase.client.Increment.class))).thenReturn(expected);

        Result actual = m.mapper.increment(inc);
        assertSame(expected, actual);
    }

    // ---------------------------------------------------------------------
    // incrementColumnValue overloads
    // ---------------------------------------------------------------------

    @Test
    public void testIncrementColumnValue_stringFamily_delegates() throws Exception {
        Mocks m = new Mocks();
        when(m.table.incrementColumnValue(any(byte[].class), any(byte[].class), any(byte[].class), org.mockito.ArgumentMatchers.anyLong())).thenReturn(10L);

        long result = m.mapper.incrementColumnValue("rk", "info", "counter", 5L);
        assertEquals(10L, result);
    }

    @Test
    public void testIncrementColumnValue_stringFamily_withDurability_delegates() throws Exception {
        Mocks m = new Mocks();
        when(m.table.incrementColumnValue(any(byte[].class), any(byte[].class), any(byte[].class), org.mockito.ArgumentMatchers.anyLong(),
                any(Durability.class))).thenReturn(11L);

        long result = m.mapper.incrementColumnValue("rk", "info", "counter", 5L, Durability.SYNC_WAL);
        assertEquals(11L, result);
    }

    @Test
    public void testIncrementColumnValue_byteArray_delegates() throws Exception {
        Mocks m = new Mocks();
        when(m.table.incrementColumnValue(any(byte[].class), any(byte[].class), any(byte[].class), org.mockito.ArgumentMatchers.anyLong())).thenReturn(12L);

        long result = m.mapper.incrementColumnValue("rk", Bytes.toBytes("info"), Bytes.toBytes("counter"), 5L);
        assertEquals(12L, result);
    }

    @Test
    public void testIncrementColumnValue_byteArray_withDurability_delegates() throws Exception {
        Mocks m = new Mocks();
        when(m.table.incrementColumnValue(any(byte[].class), any(byte[].class), any(byte[].class), org.mockito.ArgumentMatchers.anyLong(),
                any(Durability.class))).thenReturn(13L);

        long result = m.mapper.incrementColumnValue("rk", Bytes.toBytes("info"), Bytes.toBytes("counter"), 5L, Durability.SKIP_WAL);
        assertEquals(13L, result);
    }

    // ---------------------------------------------------------------------
    // scan family / qualifier / AnyScan
    // ---------------------------------------------------------------------

    @Test
    public void testScan_byFamilyString_returnsStream() throws Exception {
        Mocks m = new Mocks();
        // Scan returns a deferred stream; we just verify a non-null Stream is returned and can be closed.
        try (var stream = m.mapper.scan("info")) {
            assertNotNull(stream);
        }
        // Table is opened lazily — not requiring open here.
    }

    @Test
    public void testScan_byFamilyAndQualifierString_returnsStream() throws Exception {
        Mocks m = new Mocks();
        try (var stream = m.mapper.scan("info", "user_name")) {
            assertNotNull(stream);
        }
    }

    @Test
    public void testScan_byFamilyBytes_returnsStream() throws Exception {
        Mocks m = new Mocks();
        try (var stream = m.mapper.scan(Bytes.toBytes("info"))) {
            assertNotNull(stream);
        }
    }

    @Test
    public void testScan_byFamilyAndQualifierBytes_returnsStream() throws Exception {
        Mocks m = new Mocks();
        try (var stream = m.mapper.scan(Bytes.toBytes("info"), Bytes.toBytes("user_name"))) {
            assertNotNull(stream);
        }
    }

    @Test
    public void testScan_byAnyScan_returnsStream() throws Exception {
        Mocks m = new Mocks();
        try (var stream = m.mapper.scan(AnyScan.create().addFamily("info"))) {
            assertNotNull(stream);
        }
    }

    // ---------------------------------------------------------------------
    // coprocessorService (returns a channel — we just verify no NPE; the table.close()
    // is invoked once for the lookup)
    // ---------------------------------------------------------------------

    @Test
    public void testCoprocessorService_byRowKey_delegates() throws Exception {
        Mocks m = new Mocks();
        org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel channel = mock(org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel.class);
        when(m.table.coprocessorService(any(byte[].class))).thenReturn(channel);

        assertSame(channel, m.mapper.coprocessorService("rk"));
        verify(m.table, times(1)).close();
    }
}
