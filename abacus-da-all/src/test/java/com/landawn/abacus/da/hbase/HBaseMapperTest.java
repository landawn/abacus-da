/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import com.landawn.abacus.util.NamingPolicy;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Pure unit tests for {@link HBaseMapper} (no live HBase). All Table/Connection
 * interactions are mocked via Mockito.
 */
public class HBaseMapperTest extends TestBase {

    // Construction and configuration

    @Test
    public void testMapper_namingPolicy_appliesToWritesAndReadsAcceptAnyPolicy() throws Exception {
        Mocks m = new Mocks();
        HBaseMapper<Profile, String> snake = m.executor.mapper(Profile.class, "profiles", NamingPolicy.SNAKE_CASE);

        snake.put(new Profile("p1", "Ann"));

        org.mockito.ArgumentCaptor<org.apache.hadoop.hbase.client.Put> captor = org.mockito.ArgumentCaptor.forClass(org.apache.hadoop.hbase.client.Put.class);
        verify(m.table).put(captor.capture());
        org.apache.hadoop.hbase.client.Put written = captor.getValue();
        assertTrue(written.has(Bytes.toBytes("p"), Bytes.toBytes("display_name")), "the write side must apply SNAKE_CASE to the qualifier");
        assertTrue(!written.has(Bytes.toBytes("p"), Bytes.toBytes("displayName")));

        Cell camel = new KeyValue(Bytes.toBytes("p1"), Bytes.toBytes("p"), Bytes.toBytes("displayName"), Bytes.toBytes("Ann"));
        when(m.table.get(any(org.apache.hadoop.hbase.client.Get.class))).thenReturn(Result.create(Arrays.<Cell> asList(camel)));

        Profile readBySnake = snake.get("p1");
        assertNotNull(readBySnake);
        assertEquals("p1", readBySnake.getProfileId());
        assertEquals("Ann", readBySnake.getDisplayName(), "a CAMEL_CASE-named cell must be readable through a SNAKE_CASE mapper");

        Cell snakeCell = new KeyValue(Bytes.toBytes("p1"), Bytes.toBytes("p"), Bytes.toBytes("display_name"), Bytes.toBytes("Bob"));
        when(m.table.get(any(org.apache.hadoop.hbase.client.Get.class))).thenReturn(Result.create(Arrays.<Cell> asList(snakeCell)));

        Profile readByCamel = m.executor.mapper(Profile.class).get("p1");
        assertEquals("Bob", readByCamel.getDisplayName(), "a SNAKE_CASE-named cell must be readable through the default CAMEL_CASE mapper");
    }

    // Factories and insert operations

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

    // Updates and deletes

    @Test
    public void testDelete_anyDelete_delegatesToTable() throws Exception {
        Mocks m = new Mocks();
        AnyDelete del = AnyDelete.of("rk");
        m.mapper.delete(del);
        verify(m.table).delete(del.val());
    }

    @Test
    public void testDelete_collectionOfEntities_delegatesToTable() throws Exception {
        Mocks m = new Mocks();
        m.mapper.delete(Arrays.asList(new User("u1", "A"), new User("u2", "B")));
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
    public void testDelete_listOfAnyDelete_delegatesToTable() throws Exception {
        Mocks m = new Mocks();
        m.mapper.delete(Arrays.asList(AnyDelete.of("r1"), AnyDelete.of("r2")));
        verify(m.table).delete(any(List.class));
    }

    @Test
    public void testDeleteByRowKey_collection_delegatesToTable() throws Exception {
        Mocks m = new Mocks();
        m.mapper.deleteByRowKey(Arrays.asList("rk1", "rk2"));
        verify(m.table).delete(any(List.class));
    }

    // delete() — entity, collection, by row key, AnyDelete, list of AnyDelete

    @Test
    public void testDeleteByRowKey_single_delegatesToTable() throws Exception {
        Mocks m = new Mocks();
        m.mapper.deleteByRowKey("rk-del");
        verify(m.table).delete(any(org.apache.hadoop.hbase.client.Delete.class));
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

    // incrementColumnValue overloads

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

    // Queries and results

    @Test
    public void testExists_byAnyGet_delegates() throws Exception {
        Mocks m = new Mocks();
        when(m.table.exists(any(org.apache.hadoop.hbase.client.Get.class))).thenReturn(true);

        assertTrue(m.mapper.exists(AnyGet.of("user-x")));
        verify(m.table, atLeastOnce()).exists(any(org.apache.hadoop.hbase.client.Get.class));
    }

    // exists() — by row key, by collection of row keys, by AnyGet, by list of AnyGet

    @Test
    public void testExists_byRowKey_delegates() throws Exception {
        Mocks m = new Mocks();
        when(m.table.exists(any(org.apache.hadoop.hbase.client.Get.class))).thenReturn(true);

        assertTrue(m.mapper.exists("user-1"));
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

    // get() — by row key, AnyGet, list/collection

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

    @Test
    public void testPut_listOfAnyPut_delegatesToTable() throws Exception {
        Mocks m = new Mocks();
        AnyPut p1 = AnyPut.of("rk1").addColumn("info", "user_name", "Alice");
        AnyPut p2 = AnyPut.of("rk2").addColumn("info", "user_name", "Bob");
        m.mapper.put(Arrays.asList(p1, p2));
        verify(m.table).put(any(List.class));
    }

    @Test
    public void testScan_byAnyScan_returnsStream() throws Exception {
        Mocks m = new Mocks();
        try (var stream = m.mapper.scan(AnyScan.create().addFamily("info"))) {
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

    // scan family / qualifier / AnyScan

    @Test
    public void testScan_byFamilyString_returnsStream() throws Exception {
        Mocks m = new Mocks();
        // Scan returns a deferred stream; we just verify a non-null Stream is returned and can be closed.
        try (var stream = m.mapper.scan("info")) {
            assertNotNull(stream);
        }
        // Table is opened lazily — not requiring open here.
    }

    // Resource lifecycle

    // Null-argument validation: rejected as IllegalArgumentException, not NPE

    @Test
    public void testNullAnyOperationArgs_throwIllegalArgumentException() throws Exception {
        Mocks m = new Mocks();

        assertThrows(IllegalArgumentException.class, () -> m.mapper.exists((AnyGet) null));
        assertThrows(IllegalArgumentException.class, () -> m.mapper.get((AnyGet) null));
        assertThrows(IllegalArgumentException.class, () -> m.mapper.exists((List<AnyGet>) null));
        assertThrows(IllegalArgumentException.class, () -> m.mapper.put((AnyPut) null));
        assertThrows(IllegalArgumentException.class, () -> m.mapper.delete((AnyDelete) null));
        assertThrows(IllegalArgumentException.class, () -> m.mapper.mutateRow(null));
        assertThrows(IllegalArgumentException.class, () -> m.mapper.append(null));
        assertThrows(IllegalArgumentException.class, () -> m.mapper.increment(null));
        assertThrows(IllegalArgumentException.class, () -> m.mapper.scan((AnyScan) null));
        assertThrows(IllegalArgumentException.class, () -> m.mapper.put((User) null));
        assertThrows(IllegalArgumentException.class, () -> m.mapper.delete((User) null));
    }

    // Additional contracts

    @Test
    public void testNullRowKeyCollections_areTreatedAsEmpty() throws Exception {
        Mocks m = new Mocks();

        assertTrue(m.mapper.exists((List<String>) null).isEmpty());
        assertTrue(m.mapper.get((List<String>) null).isEmpty());

        // no-op rather than an exception
        m.mapper.deleteByRowKey((List<String>) null);
        m.mapper.delete((List<User>) null);
    }

    // coprocessorService (returns a channel — we just verify no NPE; the table.close()
    // is invoked once for the lookup)

    @Test
    public void testCoprocessorService_byRowKey_delegates() throws Exception {
        Mocks m = new Mocks();
        org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel channel = mock(org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel.class);
        when(m.table.coprocessorService(any(byte[].class))).thenReturn(channel);

        assertSame(channel, m.mapper.coprocessorService("rk"));
        verify(m.table, times(1)).close();
    }

    @Test
    public void testMapper_conventionalIdPropertyWithoutIdAnnotationIsTheRowKey() throws Exception {
        Mocks m = new Mocks();

        final HBaseMapper<ConventionalIdUser, String> mapper = m.executor.mapper(ConventionalIdUser.class, "t", null);
        mapper.delete(new ConventionalIdUser("u1", "Ann"));

        final org.mockito.ArgumentCaptor<org.apache.hadoop.hbase.client.Delete> captor = org.mockito.ArgumentCaptor
                .forClass(org.apache.hadoop.hbase.client.Delete.class);
        verify(m.table).delete(captor.capture());
        assertEquals("u1", Bytes.toString(captor.getValue().getRow()));
    }

    @Test
    public void testMapper_missingTableAnnotation_messageNamesClassOnce() throws Exception {
        Mocks m = new Mocks();

        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> m.executor.mapper(UserWithoutTable.class));

        assertTrue(ex.getMessage().startsWith("Entity class com.landawn.abacus.da.hbase.HBaseMapperTest.UserWithoutTable must be annotated with @Table"),
                ex.getMessage());
    }

    // Regression (2026-09-27 deep review): the non-bean message printed Class.toString(),
    // i.e. "class java.lang.String is not an entity class ...".
    @Test
    public void testMapper_nonBeanClass_messageNamesClassWithoutClassPrefix() throws Exception {
        Mocks m = new Mocks();

        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> m.executor.mapper(String.class, "t", null));

        assertTrue(ex.getMessage().startsWith("java.lang.String is not an entity class"), ex.getMessage());
    }

    // Constructor / accessor sanity

    @Test
    public void testMapper_returnsNonNull() throws Exception {
        Mocks m = new Mocks();
        assertNotNull(m.mapper);
    }

    // mutateRow / append / increment

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
    public void testPut_anyPut_delegatesToTable() throws Exception {
        Mocks m = new Mocks();
        AnyPut put = AnyPut.of("rk").addColumn("info", "user_name", "Alice");
        m.mapper.put(put);
        verify(m.table).put(put.val());
    }

    @Test
    public void testPut_collectionOfEntities_delegatesToTable() throws Exception {
        Mocks m = new Mocks();
        m.mapper.put(Arrays.asList(new User("u1", "Alice"), new User("u2", "Bob")));
        verify(m.table, atLeastOnce()).put(any(List.class));
    }

    // put() — entity, collection of entities, AnyPut, list of AnyPut

    @Test
    public void testPut_entity_delegatesToTable() throws Exception {
        Mocks m = new Mocks();
        User u = new User("u1", "Alice");
        m.mapper.put(u);
        verify(m.table, atLeastOnce()).put(any(org.apache.hadoop.hbase.client.Put.class));
    }

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

    // Regression (2026-09-22 deep review): the missing-@Table message printed Class.toString(),
    // i.e. "Entity class class com.x.Foo must be annotated ...".
    @Data
    @NoArgsConstructor
    public static class UserWithoutTable {
        @Id
        private String userId;
    }

    // ---------------------------------------------------------------------
    // Naming policy: applied on the write side only; reads recognize cells named under any
    // supported policy, so a SNAKE_CASE mapper still reads rows written with CAMEL_CASE names.
    // ---------------------------------------------------------------------

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Table("profiles")
    @ColumnFamily("p")
    public static class Profile {
        @Id
        private String profileId;
        private String displayName;
    }

    // A class with no @Id annotation still gets a row key from a conventionally named and typed "id" property.
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ConventionalIdUser {
        private String id;
        private String name;
    }

    // ---- 2026-10-24 sliceM ----

    // put(entity) writes the row keyed by the property AnyPut.create resolves, which is the one registered through the
    // (deprecated) registerRowKeyProperty when there is one; delete(entity) used the "id" property instead and so removed
    // a different row (here "7" instead of "rk-1").
    @Test
    @SuppressWarnings({ "deprecation", "rawtypes", "unchecked" })
    public void testDelete_entityUsesTheRowKeyPropertyThatPutWrites() throws Exception {
        HBaseExecutor.registerRowKeyProperty(RegisteredRowKeyEntity.class, "rowKey");

        final Mocks m = new Mocks();
        final HBaseMapper<RegisteredRowKeyEntity, String> mapper = m.executor.mapper(RegisteredRowKeyEntity.class, "t", null);

        mapper.put(new RegisteredRowKeyEntity(7L, "rk-1"));
        mapper.delete(new RegisteredRowKeyEntity(7L, "rk-1"));
        mapper.delete(Arrays.asList(new RegisteredRowKeyEntity(8L, "rk-2")));

        final org.mockito.ArgumentCaptor<org.apache.hadoop.hbase.client.Put> putCaptor = org.mockito.ArgumentCaptor
                .forClass(org.apache.hadoop.hbase.client.Put.class);
        final org.mockito.ArgumentCaptor<org.apache.hadoop.hbase.client.Delete> deleteCaptor = org.mockito.ArgumentCaptor
                .forClass(org.apache.hadoop.hbase.client.Delete.class);
        final org.mockito.ArgumentCaptor<List> deletesCaptor = org.mockito.ArgumentCaptor.forClass(List.class);
        verify(m.table).put(putCaptor.capture());
        verify(m.table).delete(deleteCaptor.capture());
        verify(m.table).delete(deletesCaptor.capture());

        final List<org.apache.hadoop.hbase.client.Delete> deletes = deletesCaptor.getValue();

        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals("rk-1", Bytes.toString(putCaptor.getValue().getRow())),
                () -> assertEquals("rk-1", Bytes.toString(deleteCaptor.getValue().getRow())), () -> assertEquals(1, deletes.size()),
                () -> assertEquals("rk-2", Bytes.toString(deletes.get(0).getRow())));
    }

    @Test
    public void testDelete_nullEntity_throwsIllegalArgumentException() throws Exception {
        final Mocks m = new Mocks();

        org.junit.jupiter.api.Assertions.assertAll(() -> assertThrows(IllegalArgumentException.class, () -> m.mapper.delete((User) null)),
                () -> assertThrows(IllegalArgumentException.class, () -> m.mapper.delete(Arrays.asList(new User("u1", "A"), null))));
    }

    // Only used by testDelete_entityUsesTheRowKeyPropertyThatPutWrites: registerRowKeyProperty is process-wide.
    public static class RegisteredRowKeyEntity {
        private Long id;
        private String rowKey;

        public RegisteredRowKeyEntity() {
        }

        RegisteredRowKeyEntity(final Long id, final String rowKey) {
            this.id = id;
            this.rowKey = rowKey;
        }

        public Long getId() {
            return id;
        }

        public void setId(final Long id) {
            this.id = id;
        }

        public String getRowKey() {
            return rowKey;
        }

        public void setRowKey(final String rowKey) {
            this.rowKey = rowKey;
        }
    }

    // put(entity) skips a property holding an empty value wrapper like a null one. It used to fail with
    // NullPointerException (N.stringOf(empty) is null); an empty cell instead would read back as a PRESENT empty string
    // (Optional.of(""), Nullable.of(""), Holder.of("")) rather than the empty value that was written.
    @Test
    public void testPut_entityWithEmptyValueWrappers_writesNoCellsForThemAndReadsBackEmptyValues() throws Exception {
        final Mocks m = new Mocks();
        final HBaseMapper<ValueWrapperEntity, String> mapper = m.executor.mapper(ValueWrapperEntity.class, "t", null);
        final ValueWrapperEntity entity = new ValueWrapperEntity();
        entity.setId("w1");
        entity.setName("n");

        mapper.put(entity);

        final org.mockito.ArgumentCaptor<org.apache.hadoop.hbase.client.Put> putCaptor = org.mockito.ArgumentCaptor
                .forClass(org.apache.hadoop.hbase.client.Put.class);
        verify(m.table).put(putCaptor.capture());

        final org.apache.hadoop.hbase.client.Put written = putCaptor.getValue();
        final List<Cell> cells = new java.util.ArrayList<>();
        written.getFamilyCellMap().values().forEach(cells::addAll);

        final ValueWrapperEntity back = HBaseExecutor.toEntity(Result.create(cells), ValueWrapperEntity.class);

        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals(1, cells.size(), "only the name cell is written"),
                () -> assertTrue(written.has(Bytes.toBytes("cf"), Bytes.toBytes("name"))), () -> assertEquals("w1", back.getId()),
                () -> assertEquals("n", back.getName()), () -> assertEquals(java.util.Optional.empty(), back.getJdkOptional()),
                () -> assertEquals(java.util.OptionalInt.empty(), back.getJdkOptionalInt()),
                () -> assertEquals(com.landawn.abacus.util.u.Optional.empty(), back.getOptional()),
                () -> assertEquals(com.landawn.abacus.util.u.Nullable.empty(), back.getNullable()), () -> assertTrue(back.getHolder().isNull()));
    }

    // Every wrapper starts empty, so a property without a cell reads back as the empty value.
    @ColumnFamily("cf")
    public static class ValueWrapperEntity {
        @Id
        private String id;
        private String name;
        private java.util.Optional<String> jdkOptional = java.util.Optional.empty();
        private java.util.OptionalInt jdkOptionalInt = java.util.OptionalInt.empty();
        private com.landawn.abacus.util.u.Optional<String> optional = com.landawn.abacus.util.u.Optional.empty();
        private com.landawn.abacus.util.u.Nullable<String> nullable = com.landawn.abacus.util.u.Nullable.empty();
        private com.landawn.abacus.util.Holder<String> holder = com.landawn.abacus.util.Holder.of(null);

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public java.util.Optional<String> getJdkOptional() {
            return jdkOptional;
        }

        public void setJdkOptional(final java.util.Optional<String> jdkOptional) {
            this.jdkOptional = jdkOptional;
        }

        public java.util.OptionalInt getJdkOptionalInt() {
            return jdkOptionalInt;
        }

        public void setJdkOptionalInt(final java.util.OptionalInt jdkOptionalInt) {
            this.jdkOptionalInt = jdkOptionalInt;
        }

        public com.landawn.abacus.util.u.Optional<String> getOptional() {
            return optional;
        }

        public void setOptional(final com.landawn.abacus.util.u.Optional<String> optional) {
            this.optional = optional;
        }

        public com.landawn.abacus.util.u.Nullable<String> getNullable() {
            return nullable;
        }

        public void setNullable(final com.landawn.abacus.util.u.Nullable<String> nullable) {
            this.nullable = nullable;
        }

        public com.landawn.abacus.util.Holder<String> getHolder() {
            return holder;
        }

        public void setHolder(final com.landawn.abacus.util.Holder<String> holder) {
            this.holder = holder;
        }
    }

    // ---- 2026-10-24 apply-hbase ----

    /**
     * Item 10a: the executor's and the mapper's collection put paths convert their elements through AnyPut.toPut /
     * AnyPut.create(Collection), so a native Put element (reachable through an unchecked collection) is now sent as it is, in
     * order, while any other operation element is rejected. It used to be bean-mapped (IllegalArgumentException "row key ... null").
     */
    @Test
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testPut_collectionPathsPassNativePutThrough_applyHbase() throws Exception {
        final org.apache.hadoop.hbase.client.Put nativePut = new org.apache.hadoop.hbase.client.Put(Bytes.toBytes("rk")).addColumn(Bytes.toBytes("info"),
                Bytes.toBytes("user_name"), Bytes.toBytes("Alice"));
        final Mocks m = new Mocks();

        m.executor.put("users", (java.util.Collection) Arrays.asList(nativePut, AnyPut.of("rk2").addColumn("info", "user_name", "Bob")));
        m.mapper.put((java.util.Collection) Arrays.asList(nativePut, new User("u1", "Carol")));

        final org.mockito.ArgumentCaptor<List> captor = org.mockito.ArgumentCaptor.forClass(List.class);
        verify(m.table, times(2)).put(captor.capture());

        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals(2, captor.getAllValues().get(0).size()),
                () -> assertSame(nativePut, captor.getAllValues().get(0).get(0)), () -> assertEquals(2, captor.getAllValues().get(1).size()),
                () -> assertSame(nativePut, captor.getAllValues().get(1).get(0)),
                () -> assertThrows(IllegalArgumentException.class, () -> m.executor.put("users", (java.util.Collection) Arrays.asList(AnyDelete.of("rk")))),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> m.mapper.put((java.util.Collection) Arrays.asList(new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("rk"))))));
    }
}
