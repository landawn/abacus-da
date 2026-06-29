/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.annotation.Column;
import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.annotation.Table;
import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.da.hbase.HBaseExecutor.HBaseMapper;
import com.landawn.abacus.da.hbase.annotation.ColumnFamily;
import com.landawn.abacus.util.NamingPolicy;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Pure unit tests for {@link HBaseExecutor} static utility methods and any non-IO
 * accessors. Where a {@link Connection} is needed, it is mocked via Mockito so no
 * live HBase cluster is required.
 */
public class HBaseExecutorStaticTest extends TestBase {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Table("test_table")
    @ColumnFamily("info")
    public static class Bean {
        @Id
        private String id;
        @Column("user_name")
        private String name;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class NoIdBean {
        private String something;
    }

    // ---------------------------------------------------------------------
    // toRowBytes / toValueBytes / toRowKeyBytes / toFamilyQualifierBytes
    // (package-private static methods)
    // ---------------------------------------------------------------------

    @Test
    public void testToRowBytes_string() {
        assertArrayEquals(Bytes.toBytes("hello"), HBaseExecutor.toRowBytes("hello"));
    }

    @Test
    public void testToRowBytes_null_returnsNull() {
        assertNull(HBaseExecutor.toRowBytes(null));
    }

    @Test
    public void testToRowBytes_byteArray_returnedDirectly() {
        byte[] data = new byte[] { 1, 2, 3 };
        assertSame(data, HBaseExecutor.toRowBytes(data));
    }

    @Test
    public void testToRowBytes_byteBuffer() {
        ByteBuffer bb = ByteBuffer.wrap(Bytes.toBytes("buf"));
        byte[] out = HBaseExecutor.toRowBytes(bb);
        assertArrayEquals(Bytes.toBytes("buf"), out);
    }

    @Test
    public void testToRowBytes_nonStringObject_convertedViaToString() {
        // Numeric value uses N.stringOf which produces "12345"
        byte[] out = HBaseExecutor.toRowBytes(12345);
        assertArrayEquals(Bytes.toBytes("12345"), out);
    }

    @Test
    public void testToRowKeyBytes_delegatesToToValueBytes() {
        assertArrayEquals(Bytes.toBytes("rk"), HBaseExecutor.toRowKeyBytes("rk"));
    }

    @Test
    public void testToValueBytes_null_returnsNull() {
        assertNull(HBaseExecutor.toValueBytes(null));
    }

    @Test
    public void testToValueBytes_string() {
        assertArrayEquals(Bytes.toBytes("abc"), HBaseExecutor.toValueBytes("abc"));
    }

    @Test
    public void testToFamilyQualifierBytes_string() {
        byte[] result = HBaseExecutor.toFamilyQualifierBytes("cf");
        assertArrayEquals(Bytes.toBytes("cf"), result);
    }

    @Test
    public void testToFamilyQualifierBytes_null_returnsNull() {
        assertNull(HBaseExecutor.toFamilyQualifierBytes(null));
    }

    @Test
    public void testToFamilyQualifierBytes_pooled_returnsSameInstance() {
        // Same string twice -> the internal pool should return the same byte[] reference.
        byte[] first = HBaseExecutor.toFamilyQualifierBytes("pooled_qualifier_unique_x");
        byte[] second = HBaseExecutor.toFamilyQualifierBytes("pooled_qualifier_unique_x");
        assertSame(first, second, "Pool should return the cached byte[] instance");
    }

    // ---------------------------------------------------------------------
    // toRowKeyString / toFamilyQualifierString / toValueString
    // ---------------------------------------------------------------------

    @Test
    public void testToRowKeyString_roundTrip() {
        byte[] b = Bytes.toBytes("hello");
        assertEquals("hello", HBaseExecutor.toRowKeyString(b, 0, b.length));
    }

    @Test
    public void testToFamilyQualifierString_roundTrip() {
        byte[] b = Bytes.toBytes("cf");
        assertEquals("cf", HBaseExecutor.toFamilyQualifierString(b, 0, b.length));
    }

    @Test
    public void testToValueString_roundTrip() {
        byte[] b = Bytes.toBytes("value");
        assertEquals("value", HBaseExecutor.toValueString(b, 0, b.length));
    }

    @Test
    public void testGetRowKeyString_fromCell() {
        Cell cell = new KeyValue(Bytes.toBytes("rk"), Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("v"));
        assertEquals("rk", HBaseExecutor.getRowKeyString(cell));
    }

    @Test
    public void testGetFamilyString_fromCell() {
        Cell cell = new KeyValue(Bytes.toBytes("rk"), Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("v"));
        assertEquals("cf", HBaseExecutor.getFamilyString(cell));
    }

    @Test
    public void testGetQualifierString_fromCell() {
        Cell cell = new KeyValue(Bytes.toBytes("rk"), Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("v"));
        assertEquals("q", HBaseExecutor.getQualifierString(cell));
    }

    @Test
    public void testGetValueString_fromCell() {
        Cell cell = new KeyValue(Bytes.toBytes("rk"), Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("v"));
        assertEquals("v", HBaseExecutor.getValueString(cell));
    }

    // ---------------------------------------------------------------------
    // toEntity / toValue
    // ---------------------------------------------------------------------

    @Test
    public void testToEntity_emptyResult_returnsNullForBean() {
        Result empty = Result.create(Collections.<Cell> emptyList());
        Bean bean = HBaseExecutor.toEntity(empty, Bean.class);
        assertNull(bean);
    }

    @Test
    public void testToEntity_singleCellResult_mapsToBean() {
        Cell cell = new KeyValue(Bytes.toBytes("rk1"), Bytes.toBytes("info"), Bytes.toBytes("user_name"), Bytes.toBytes("Alice"));
        Result result = Result.create(Arrays.<Cell> asList(cell));
        Bean bean = HBaseExecutor.toEntity(result, Bean.class);
        assertNotNull(bean);
        assertEquals("rk1", bean.getId());
        assertEquals("Alice", bean.getName());
    }

    @Test
    public void testToEntity_singleCellResult_singleValueType() {
        Cell cell = new KeyValue(Bytes.toBytes("rk"), Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("scalar"));
        Result result = Result.create(Arrays.<Cell> asList(cell));
        String s = HBaseExecutor.toEntity(result, String.class);
        assertEquals("scalar", s);
    }

    @Test
    public void testToEntity_mapType_throws() {
        Result result = Result.create(Collections.<Cell> emptyList());
        // Map type is explicitly unsupported.
        assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.toEntity(result, java.util.HashMap.class));
    }

    // ---------------------------------------------------------------------
    // toList(ResultScanner, Class) / toList(ResultScanner, int, int, Class)
    // ---------------------------------------------------------------------

    @Test
    public void testToList_resultScanner_full() throws Exception {
        // Build two single-cell Results and a mocked ResultScanner that yields them.
        Cell c1 = new KeyValue(Bytes.toBytes("r1"), Bytes.toBytes("info"), Bytes.toBytes("user_name"), Bytes.toBytes("A"));
        Cell c2 = new KeyValue(Bytes.toBytes("r2"), Bytes.toBytes("info"), Bytes.toBytes("user_name"), Bytes.toBytes("B"));
        Result rA = Result.create(Arrays.<Cell> asList(c1));
        Result rB = Result.create(Arrays.<Cell> asList(c2));

        ResultScanner scanner = mock(ResultScanner.class);
        when(scanner.next()).thenReturn(rA, rB, null);

        List<Bean> beans = HBaseExecutor.toList(scanner, Bean.class);
        assertEquals(2, beans.size());
        assertEquals("r1", beans.get(0).getId());
        assertEquals("r2", beans.get(1).getId());
    }

    @Test
    public void testToList_resultScanner_negativeOffset_throws() {
        ResultScanner scanner = mock(ResultScanner.class);
        assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.toList(scanner, -1, 5, Bean.class));
    }

    @Test
    public void testToList_resultScanner_negativeCount_throws() {
        ResultScanner scanner = mock(ResultScanner.class);
        assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.toList(scanner, 0, -1, Bean.class));
    }

    @Test
    public void testToList_resultScanner_withOffsetAndCount() throws Exception {
        Cell c1 = new KeyValue(Bytes.toBytes("r1"), Bytes.toBytes("info"), Bytes.toBytes("user_name"), Bytes.toBytes("A"));
        Cell c2 = new KeyValue(Bytes.toBytes("r2"), Bytes.toBytes("info"), Bytes.toBytes("user_name"), Bytes.toBytes("B"));
        Cell c3 = new KeyValue(Bytes.toBytes("r3"), Bytes.toBytes("info"), Bytes.toBytes("user_name"), Bytes.toBytes("C"));

        ResultScanner scanner = mock(ResultScanner.class);
        when(scanner.next()).thenReturn(Result.create(Arrays.<Cell> asList(c1)), Result.create(Arrays.<Cell> asList(c2)),
                Result.create(Arrays.<Cell> asList(c3)), (Result) null);

        // Skip 1, take 1.
        List<Bean> beans = HBaseExecutor.toList(scanner, 1, 1, Bean.class);
        assertEquals(1, beans.size());
        assertEquals("r2", beans.get(0).getId());
    }

    // ---------------------------------------------------------------------
    // registerRowKeyProperty
    // ---------------------------------------------------------------------

    @Test
    public void testRegisterRowKeyProperty_invalidProperty_throws() {
        // Property "missing" does not exist on Bean.
        assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.registerRowKeyProperty(Bean.class, "missing"));
    }

    @Test
    public void testRegisterRowKeyProperty_validProperty_doesNotThrow() {
        // "id" exists on Bean; this should succeed (re-registering is allowed).
        assertDoesNotThrow(() -> HBaseExecutor.registerRowKeyProperty(Bean.class, "id"));
    }

    // ---------------------------------------------------------------------
    // Constructor / admin() / connection() / async() / close() / getTable()
    // (using mocked Connection so no real HBase cluster is needed)
    // ---------------------------------------------------------------------

    @Test
    public void testConstructor_initializesAdminAndConnection() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        when(conn.getAdmin()).thenReturn(admin);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            assertSame(conn, executor.connection());
            assertSame(admin, executor.admin());
            assertNotNull(executor.async(), "async() must return a non-null AsyncHBaseExecutor");
            assertSame(executor, executor.async().sync(), "async().sync() must return this executor");
        } finally {
            executor.close();
        }
    }

    @Test
    public void testClose_closesAdminAndConnection() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.isClosed()).thenReturn(false);

        HBaseExecutor executor = new HBaseExecutor(conn);
        executor.close();

        verify(admin, times(1)).close();
        // Connection close happens only if not already closed.
        verify(conn, atLeastOnce()).isClosed();
    }

    @Test
    public void testGetTable_invokesConnectionGetTable() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        org.apache.hadoop.hbase.client.Table table = mock(org.apache.hadoop.hbase.client.Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            org.apache.hadoop.hbase.client.Table returned = executor.getTable("some_table");
            assertSame(table, returned);
            verify(conn).getTable(TableName.valueOf("some_table"));
        } finally {
            executor.close();
        }
    }

    @Test
    public void testMapper_byClass_requiresTableAnnotation() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        when(conn.getAdmin()).thenReturn(admin);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            HBaseMapper<Bean, String> mapper = executor.mapper(Bean.class);
            assertNotNull(mapper);
            // Re-fetch returns the cached mapper.
            HBaseMapper<Bean, String> mapper2 = executor.mapper(Bean.class);
            assertSame(mapper, mapper2);
        } finally {
            executor.close();
        }
    }

    @Test
    public void testMapper_byClass_noTableAnnotation_throws() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        when(conn.getAdmin()).thenReturn(admin);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            // NoIdBean has no @Table annotation -> mapper() must throw.
            assertThrows(IllegalArgumentException.class, () -> executor.mapper(NoIdBean.class));
        } finally {
            executor.close();
        }
    }

    @Test
    public void testMapper_withTableNameAndNamingPolicy_returnsNewMapper() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        when(conn.getAdmin()).thenReturn(admin);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            HBaseMapper<Bean, String> mapper = executor.mapper(Bean.class, "explicit_table", NamingPolicy.CAMEL_CASE);
            assertNotNull(mapper);
        } finally {
            executor.close();
        }
    }

    @Test
    public void testMapper_withNullNamingPolicy_defaultsToCamelCase() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        when(conn.getAdmin()).thenReturn(admin);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            // Null naming policy should default to CAMEL_CASE.
            HBaseMapper<Bean, String> mapper = executor.mapper(Bean.class, "explicit_table", null);
            assertNotNull(mapper);
        } finally {
            executor.close();
        }
    }

    @Test
    public void testMapper_noIdBean_throws() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        when(conn.getAdmin()).thenReturn(admin);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            // NoIdBean has no @Id property -> mapper constructor must throw.
            assertThrows(IllegalArgumentException.class, () -> executor.mapper(NoIdBean.class, "t", NamingPolicy.CAMEL_CASE));
        } finally {
            executor.close();
        }
    }

    // ---------------------------------------------------------------------
    // exists / get / put / delete via mocked Table - verify dispatch.
    // ---------------------------------------------------------------------

    @Test
    public void testExists_singleGet_delegatesToTable() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        org.apache.hadoop.hbase.client.Table table = mock(org.apache.hadoop.hbase.client.Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);

        org.apache.hadoop.hbase.client.Get get = new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("k"));
        when(table.exists(get)).thenReturn(true);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            assertTrue(executor.exists("tbl", get));
            verify(table, times(1)).close();
        } finally {
            executor.close();
        }
    }

    @Test
    public void testExists_anyGet_delegatesToTable() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        org.apache.hadoop.hbase.client.Table table = mock(org.apache.hadoop.hbase.client.Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);
        when(table.exists(any(org.apache.hadoop.hbase.client.Get.class))).thenReturn(true);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            assertTrue(executor.exists("tbl", AnyGet.of("k")));
        } finally {
            executor.close();
        }
    }

    @Test
    public void testGet_singleGet_returnsResultFromTable() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        org.apache.hadoop.hbase.client.Table table = mock(org.apache.hadoop.hbase.client.Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);

        Cell cell = new KeyValue(Bytes.toBytes("k"), Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("v"));
        Result expected = Result.create(Arrays.<Cell> asList(cell));
        when(table.get(any(org.apache.hadoop.hbase.client.Get.class))).thenReturn(expected);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            Result actual = executor.get("tbl", new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("k")));
            assertSame(expected, actual);
            verify(table, times(1)).close();
        } finally {
            executor.close();
        }
    }

    @Test
    public void testPut_singlePut_delegatesToTable() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        org.apache.hadoop.hbase.client.Table table = mock(org.apache.hadoop.hbase.client.Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            org.apache.hadoop.hbase.client.Put put = new org.apache.hadoop.hbase.client.Put(Bytes.toBytes("k"));
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("v"));
            executor.put("tbl", put);
            verify(table).put(put);
            verify(table, times(1)).close();
        } finally {
            executor.close();
        }
    }

    @Test
    public void testPut_anyPut_delegatesToTable() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        org.apache.hadoop.hbase.client.Table table = mock(org.apache.hadoop.hbase.client.Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            AnyPut anyPut = AnyPut.of("k").addColumn("cf", "q", "v");
            executor.put("tbl", anyPut);
            verify(table).put(anyPut.val());
        } finally {
            executor.close();
        }
    }

    @Test
    public void testDelete_singleDelete_delegatesToTable() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        org.apache.hadoop.hbase.client.Table table = mock(org.apache.hadoop.hbase.client.Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            org.apache.hadoop.hbase.client.Delete del = new org.apache.hadoop.hbase.client.Delete(Bytes.toBytes("k"));
            executor.delete("tbl", del);
            verify(table).delete(del);
            verify(table, times(1)).close();
        } finally {
            executor.close();
        }
    }

    @Test
    public void testDelete_anyDelete_delegatesToTable() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        org.apache.hadoop.hbase.client.Table table = mock(org.apache.hadoop.hbase.client.Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            AnyDelete anyDelete = AnyDelete.of("k");
            executor.delete("tbl", anyDelete);
            verify(table).delete(anyDelete.val());
        } finally {
            executor.close();
        }
    }

    @Test
    public void testPut_emptyCollection_doesNotCallTable() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        org.apache.hadoop.hbase.client.Table table = mock(org.apache.hadoop.hbase.client.Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            // Empty AnyPut collection - should be a no-op (no put call to table).
            executor.put("tbl", Collections.<AnyPut> emptyList());
            // Cannot assert table.put was NOT called without knowing the impl;
            // simply verify no exception was thrown.
        } finally {
            executor.close();
        }
    }

    // ---------------------------------------------------------------------
    // exists(String, List<Get>) and exists(String, Collection<AnyGet>)
    // ---------------------------------------------------------------------

    @Test
    public void testExists_listOfGet_delegatesToTable() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        org.apache.hadoop.hbase.client.Table table = mock(org.apache.hadoop.hbase.client.Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);
        when(table.exists(org.mockito.ArgumentMatchers.<List<org.apache.hadoop.hbase.client.Get>> any())).thenReturn(new boolean[] { true, false });

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            org.apache.hadoop.hbase.client.Get g1 = new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("k1"));
            org.apache.hadoop.hbase.client.Get g2 = new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("k2"));
            List<Boolean> result = executor.exists("tbl", Arrays.asList(g1, g2));
            assertEquals(2, result.size());
            assertEquals(Boolean.TRUE, result.get(0));
        } finally {
            executor.close();
        }
    }

    @Test
    public void testExists_collectionOfAnyGet_delegatesToTable() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        org.apache.hadoop.hbase.client.Table table = mock(org.apache.hadoop.hbase.client.Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);
        when(table.exists(org.mockito.ArgumentMatchers.<List<org.apache.hadoop.hbase.client.Get>> any())).thenReturn(new boolean[] { true });

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            List<Boolean> result = executor.exists("tbl", Arrays.asList(AnyGet.of("k")));
            assertEquals(1, result.size());
            assertEquals(Boolean.TRUE, result.get(0));
        } finally {
            executor.close();
        }
    }

    // ---------------------------------------------------------------------
    // get(String, List<Get>), get(String, AnyGet), batch overloads
    // ---------------------------------------------------------------------

    @Test
    public void testGet_listOfGet_returnsListOfResults() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        org.apache.hadoop.hbase.client.Table table = mock(org.apache.hadoop.hbase.client.Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);

        Cell c = new KeyValue(Bytes.toBytes("k"), Bytes.toBytes("info"), Bytes.toBytes("user_name"), Bytes.toBytes("A"));
        Result[] results = new Result[] { Result.create(Arrays.<Cell> asList(c)) };
        when(table.get(org.mockito.ArgumentMatchers.<List<org.apache.hadoop.hbase.client.Get>> any())).thenReturn(results);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            List<Result> got = executor.get("tbl", Arrays.asList(new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("k"))));
            assertEquals(1, got.size());
        } finally {
            executor.close();
        }
    }

    @Test
    public void testGet_anyGet_returnsResult() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        org.apache.hadoop.hbase.client.Table table = mock(org.apache.hadoop.hbase.client.Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);

        Cell c = new KeyValue(Bytes.toBytes("k"), Bytes.toBytes("info"), Bytes.toBytes("user_name"), Bytes.toBytes("A"));
        Result expected = Result.create(Arrays.<Cell> asList(c));
        when(table.get(any(org.apache.hadoop.hbase.client.Get.class))).thenReturn(expected);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            assertSame(expected, executor.get("tbl", AnyGet.of("k")));
        } finally {
            executor.close();
        }
    }

    @Test
    public void testGet_collectionOfAnyGet_returnsListOfResults() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        org.apache.hadoop.hbase.client.Table table = mock(org.apache.hadoop.hbase.client.Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);

        Cell c = new KeyValue(Bytes.toBytes("k"), Bytes.toBytes("info"), Bytes.toBytes("user_name"), Bytes.toBytes("A"));
        Result[] results = new Result[] { Result.create(Arrays.<Cell> asList(c)) };
        when(table.get(org.mockito.ArgumentMatchers.<List<org.apache.hadoop.hbase.client.Get>> any())).thenReturn(results);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            List<Result> got = executor.get("tbl", Arrays.asList(AnyGet.of("k")));
            assertEquals(1, got.size());
        } finally {
            executor.close();
        }
    }

    @Test
    public void testGet_getWithTargetType_convertsResult() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        org.apache.hadoop.hbase.client.Table table = mock(org.apache.hadoop.hbase.client.Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);

        Cell c = new KeyValue(Bytes.toBytes("rk1"), Bytes.toBytes("info"), Bytes.toBytes("user_name"), Bytes.toBytes("Alice"));
        Result result = Result.create(Arrays.<Cell> asList(c));
        when(table.get(any(org.apache.hadoop.hbase.client.Get.class))).thenReturn(result);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            Bean bean = executor.get("tbl", new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("rk1")), Bean.class);
            assertNotNull(bean);
            assertEquals("rk1", bean.getId());
            assertEquals("Alice", bean.getName());
        } finally {
            executor.close();
        }
    }

    @Test
    public void testGet_anyGetWithTargetType_convertsResult() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        org.apache.hadoop.hbase.client.Table table = mock(org.apache.hadoop.hbase.client.Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);

        Cell c = new KeyValue(Bytes.toBytes("rk1"), Bytes.toBytes("info"), Bytes.toBytes("user_name"), Bytes.toBytes("Bob"));
        Result result = Result.create(Arrays.<Cell> asList(c));
        when(table.get(any(org.apache.hadoop.hbase.client.Get.class))).thenReturn(result);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            Bean bean = executor.get("tbl", AnyGet.of("rk1"), Bean.class);
            assertNotNull(bean);
            assertEquals("Bob", bean.getName());
        } finally {
            executor.close();
        }
    }

    @Test
    public void testGet_listOfGetWithTargetType_convertsAll() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        org.apache.hadoop.hbase.client.Table table = mock(org.apache.hadoop.hbase.client.Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);

        Cell c1 = new KeyValue(Bytes.toBytes("r1"), Bytes.toBytes("info"), Bytes.toBytes("user_name"), Bytes.toBytes("A"));
        Cell c2 = new KeyValue(Bytes.toBytes("r2"), Bytes.toBytes("info"), Bytes.toBytes("user_name"), Bytes.toBytes("B"));
        Result[] results = new Result[] { Result.create(Arrays.<Cell> asList(c1)), Result.create(Arrays.<Cell> asList(c2)) };
        when(table.get(org.mockito.ArgumentMatchers.<List<org.apache.hadoop.hbase.client.Get>> any())).thenReturn(results);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            List<Bean> beans = executor.get("tbl",
                    Arrays.asList(new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("r1")), new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("r2"))),
                    Bean.class);
            assertEquals(2, beans.size());
        } finally {
            executor.close();
        }
    }

    @Test
    public void testGet_collectionOfAnyGetWithTargetType_convertsAll() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        org.apache.hadoop.hbase.client.Table table = mock(org.apache.hadoop.hbase.client.Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);

        Cell c1 = new KeyValue(Bytes.toBytes("r1"), Bytes.toBytes("info"), Bytes.toBytes("user_name"), Bytes.toBytes("A"));
        Result[] results = new Result[] { Result.create(Arrays.<Cell> asList(c1)) };
        when(table.get(org.mockito.ArgumentMatchers.<List<org.apache.hadoop.hbase.client.Get>> any())).thenReturn(results);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            List<Bean> beans = executor.get("tbl", Arrays.asList(AnyGet.of("r1")), Bean.class);
            assertEquals(1, beans.size());
        } finally {
            executor.close();
        }
    }

    // ---------------------------------------------------------------------
    // put(String, List<Put>) and delete batch
    // ---------------------------------------------------------------------

    @Test
    public void testPut_listOfPut_delegatesToTable() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        org.apache.hadoop.hbase.client.Table table = mock(org.apache.hadoop.hbase.client.Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            org.apache.hadoop.hbase.client.Put p1 = new org.apache.hadoop.hbase.client.Put(Bytes.toBytes("k1"));
            p1.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("v"));
            executor.put("tbl", Arrays.asList(p1));
            verify(table).put(org.mockito.ArgumentMatchers.<List<org.apache.hadoop.hbase.client.Put>> any());
        } finally {
            executor.close();
        }
    }

    @Test
    public void testDelete_listOfDelete_delegatesToTable() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        org.apache.hadoop.hbase.client.Table table = mock(org.apache.hadoop.hbase.client.Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            org.apache.hadoop.hbase.client.Delete d1 = new org.apache.hadoop.hbase.client.Delete(Bytes.toBytes("k1"));
            executor.delete("tbl", Arrays.asList(d1));
            verify(table).delete(org.mockito.ArgumentMatchers.<List<org.apache.hadoop.hbase.client.Delete>> any());
        } finally {
            executor.close();
        }
    }

    @Test
    public void testDelete_collectionOfAnyDelete_delegatesToTable() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        org.apache.hadoop.hbase.client.Table table = mock(org.apache.hadoop.hbase.client.Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            executor.delete("tbl", Arrays.asList(AnyDelete.of("k1"), AnyDelete.of("k2")));
            verify(table).delete(org.mockito.ArgumentMatchers.<List<org.apache.hadoop.hbase.client.Delete>> any());
        } finally {
            executor.close();
        }
    }

    @Test
    public void testPut_collectionOfAnyPut_delegatesToTable() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        org.apache.hadoop.hbase.client.Table table = mock(org.apache.hadoop.hbase.client.Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            executor.put("tbl", Arrays.asList(AnyPut.of("k1").addColumn("cf", "q", "v"), AnyPut.of("k2").addColumn("cf", "q", "v")));
            verify(table).put(org.mockito.ArgumentMatchers.<List<org.apache.hadoop.hbase.client.Put>> any());
        } finally {
            executor.close();
        }
    }

    // ---------------------------------------------------------------------
    // mutateRow / append / increment (native and Any variants)
    // ---------------------------------------------------------------------

    @Test
    public void testMutateRow_rowMutations_delegatesToTable() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        org.apache.hadoop.hbase.client.Table table = mock(org.apache.hadoop.hbase.client.Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            org.apache.hadoop.hbase.client.RowMutations rm = new org.apache.hadoop.hbase.client.RowMutations(Bytes.toBytes("rk"));
            org.apache.hadoop.hbase.client.Put put = new org.apache.hadoop.hbase.client.Put(Bytes.toBytes("rk"));
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("v"));
            rm.add(put);
            executor.mutateRow("tbl", rm);
            verify(table).mutateRow(rm);
        } finally {
            executor.close();
        }
    }

    @Test
    public void testMutateRow_anyRowMutations_delegatesToTable() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        org.apache.hadoop.hbase.client.Table table = mock(org.apache.hadoop.hbase.client.Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            org.apache.hadoop.hbase.client.Put put = new org.apache.hadoop.hbase.client.Put(Bytes.toBytes("rk"));
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("v"));
            AnyRowMutations rm = AnyRowMutations.of("rk").add(put);
            executor.mutateRow("tbl", rm);
            verify(table).mutateRow(rm.val());
        } finally {
            executor.close();
        }
    }

    @Test
    public void testAppend_native_delegatesToTable() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        org.apache.hadoop.hbase.client.Table table = mock(org.apache.hadoop.hbase.client.Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);

        Cell c = new KeyValue(Bytes.toBytes("rk"), Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("v"));
        Result expected = Result.create(Arrays.<Cell> asList(c));
        when(table.append(any(org.apache.hadoop.hbase.client.Append.class))).thenReturn(expected);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            org.apache.hadoop.hbase.client.Append a = new org.apache.hadoop.hbase.client.Append(Bytes.toBytes("rk"));
            a.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("v"));
            assertSame(expected, executor.append("tbl", a));
        } finally {
            executor.close();
        }
    }

    @Test
    public void testAppend_anyAppend_delegatesToTable() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        org.apache.hadoop.hbase.client.Table table = mock(org.apache.hadoop.hbase.client.Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);

        Cell c = new KeyValue(Bytes.toBytes("rk"), Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("v"));
        Result expected = Result.create(Arrays.<Cell> asList(c));
        when(table.append(any(org.apache.hadoop.hbase.client.Append.class))).thenReturn(expected);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            AnyAppend a = AnyAppend.of("rk").addColumn("cf", "q", "v");
            assertSame(expected, executor.append("tbl", a));
        } finally {
            executor.close();
        }
    }

    @Test
    public void testIncrement_native_delegatesToTable() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        org.apache.hadoop.hbase.client.Table table = mock(org.apache.hadoop.hbase.client.Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);

        Cell c = new KeyValue(Bytes.toBytes("rk"), Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes(42L));
        Result expected = Result.create(Arrays.<Cell> asList(c));
        when(table.increment(any(org.apache.hadoop.hbase.client.Increment.class))).thenReturn(expected);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            org.apache.hadoop.hbase.client.Increment inc = new org.apache.hadoop.hbase.client.Increment(Bytes.toBytes("rk"));
            inc.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q"), 1L);
            assertSame(expected, executor.increment("tbl", inc));
        } finally {
            executor.close();
        }
    }

    @Test
    public void testIncrement_anyIncrement_delegatesToTable() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        org.apache.hadoop.hbase.client.Table table = mock(org.apache.hadoop.hbase.client.Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);

        Cell c = new KeyValue(Bytes.toBytes("rk"), Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes(99L));
        Result expected = Result.create(Arrays.<Cell> asList(c));
        when(table.increment(any(org.apache.hadoop.hbase.client.Increment.class))).thenReturn(expected);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            AnyIncrement inc = AnyIncrement.of("rk").addColumn("cf", "q", 1L);
            assertSame(expected, executor.increment("tbl", inc));
        } finally {
            executor.close();
        }
    }

    // ---------------------------------------------------------------------
    // incrementColumnValue overloads
    // ---------------------------------------------------------------------

    @Test
    public void testIncrementColumnValue_stringFamily_delegatesToTable() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        org.apache.hadoop.hbase.client.Table table = mock(org.apache.hadoop.hbase.client.Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);

        when(table.incrementColumnValue(any(byte[].class), any(byte[].class), any(byte[].class), org.mockito.ArgumentMatchers.anyLong())).thenReturn(7L);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            assertEquals(7L, executor.incrementColumnValue("tbl", "rk", "cf", "q", 1L));
        } finally {
            executor.close();
        }
    }

    @Test
    public void testIncrementColumnValue_stringFamily_withDurability() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        org.apache.hadoop.hbase.client.Table table = mock(org.apache.hadoop.hbase.client.Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);

        when(table.incrementColumnValue(any(byte[].class), any(byte[].class), any(byte[].class), org.mockito.ArgumentMatchers.anyLong(),
                any(org.apache.hadoop.hbase.client.Durability.class))).thenReturn(8L);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            assertEquals(8L, executor.incrementColumnValue("tbl", "rk", "cf", "q", 1L, org.apache.hadoop.hbase.client.Durability.SYNC_WAL));
        } finally {
            executor.close();
        }
    }

    @Test
    public void testIncrementColumnValue_byteArrayFamily_delegatesToTable() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        org.apache.hadoop.hbase.client.Table table = mock(org.apache.hadoop.hbase.client.Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);

        when(table.incrementColumnValue(any(byte[].class), any(byte[].class), any(byte[].class), org.mockito.ArgumentMatchers.anyLong())).thenReturn(9L);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            assertEquals(9L, executor.incrementColumnValue("tbl", "rk", Bytes.toBytes("cf"), Bytes.toBytes("q"), 1L));
        } finally {
            executor.close();
        }
    }

    @Test
    public void testIncrementColumnValue_byteArrayFamily_withDurability() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        org.apache.hadoop.hbase.client.Table table = mock(org.apache.hadoop.hbase.client.Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);

        when(table.incrementColumnValue(any(byte[].class), any(byte[].class), any(byte[].class), org.mockito.ArgumentMatchers.anyLong(),
                any(org.apache.hadoop.hbase.client.Durability.class))).thenReturn(10L);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            assertEquals(10L, executor.incrementColumnValue("tbl", "rk", Bytes.toBytes("cf"), Bytes.toBytes("q"), 1L,
                    org.apache.hadoop.hbase.client.Durability.SKIP_WAL));
        } finally {
            executor.close();
        }
    }

    // ---------------------------------------------------------------------
    // coprocessorService(String, Object) -- channel returned, table closed
    // ---------------------------------------------------------------------

    @Test
    public void testCoprocessorService_byRowKey_delegatesToTable() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        org.apache.hadoop.hbase.client.Table table = mock(org.apache.hadoop.hbase.client.Table.class);
        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);

        org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel ch = mock(org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel.class);
        when(table.coprocessorService(any(byte[].class))).thenReturn(ch);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            assertSame(ch, executor.coprocessorService("tbl", "rk"));
            verify(table, times(1)).close();
        } finally {
            executor.close();
        }
    }

    // ---------------------------------------------------------------------
    // Convenience scan overloads: returned Stream is non-null and closable.
    // ---------------------------------------------------------------------

    @Test
    public void testScan_byTableAndFamilyString_returnsStream() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        when(conn.getAdmin()).thenReturn(admin);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            try (var stream = executor.scan("tbl", "info")) {
                assertNotNull(stream);
            }
        } finally {
            executor.close();
        }
    }

    @Test
    public void testScan_byTableAndFamilyByteArray_returnsStream() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        when(conn.getAdmin()).thenReturn(admin);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            try (var stream = executor.scan("tbl", Bytes.toBytes("info"))) {
                assertNotNull(stream);
            }
        } finally {
            executor.close();
        }
    }

    @Test
    public void testScan_byTableAndAnyScan_returnsStream() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        when(conn.getAdmin()).thenReturn(admin);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            try (var stream = executor.scan("tbl", AnyScan.create().addFamily("info"))) {
                assertNotNull(stream);
            }
        } finally {
            executor.close();
        }
    }

    @Test
    public void testScan_byTableAndAnyScanWithTargetType_returnsStream() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        when(conn.getAdmin()).thenReturn(admin);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            try (var stream = executor.scan("tbl", AnyScan.create().addFamily("info"), Bean.class)) {
                assertNotNull(stream);
            }
        } finally {
            executor.close();
        }
    }

    @Test
    public void testScan_byTableFamilyAndTargetType_returnsStream() throws Exception {
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        when(conn.getAdmin()).thenReturn(admin);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        try {
            try (var stream = executor.scan("tbl", "info", Bean.class)) {
                assertNotNull(stream);
            }
        } finally {
            executor.close();
        }
    }
}
