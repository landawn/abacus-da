/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.annotation.Column;
import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.annotation.Table;
import com.landawn.abacus.da.hbase.HBaseExecutor.HBaseMapper;
import com.landawn.abacus.da.hbase.annotation.ColumnFamily;
import com.landawn.abacus.exception.UncheckedIOException;
import com.landawn.abacus.util.DateUtil;
import com.landawn.abacus.util.HBaseColumn;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.stream.Stream;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public class HBaseExecutorTest {
    // Install HBase and create tables:
    // create 'account', 'id', 'gui', 'name', 'emailAddress', 'lastUpdateTime', 'createTime', 'contact'
    // create 'contact', 'id', 'accountId', 'telephone', 'city', 'state', 'zipCode', 'status', 'lastUpdateTime', 'createTime'

    static final HBaseExecutor hbaseExecutor;

    static final HBaseMapper<Account, String> accountMapper;

    static {
        Configuration config = HBaseConfiguration.create();
        config.setInt("timeout", 120000);
        config.set("hbase.master", "localhost:9000");
        config.set("hbase.zookeeper.quorum", "localhost");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            hbaseExecutor = new HBaseExecutor(ConnectionFactory.createConnection(config));
            final TableName tableName = TableName.valueOf("account");

            final List<ColumnFamilyDescriptor> families = Stream.of("id", "gui", "fullName", "emailAddress", //
                    "time", "createTime", "contact", "columnFamily2B").map(it -> ColumnFamilyDescriptorBuilder.newBuilder(it.getBytes()).build()).toList();

            hbaseExecutor.admin().disableTable(tableName);
            hbaseExecutor.admin().deleteTable(tableName);

            final TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName).setColumnFamilies(families).build();
            hbaseExecutor.admin().createTable(desc);

            accountMapper = hbaseExecutor.mapper(Account.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        // HBaseExecutor.registerRowKeyProperty(Account.class, "id");
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Table("account")
    @ColumnFamily("columnFamily2B")
    public static class Account {
        @Id
        private String id;
        @Column("guid")
        private String gui;
        @ColumnFamily("fullName")
        private Name name;
        private String emailAddress;
        @ColumnFamily("time")
        @Column("updateTime")
        private Timestamp lastUpdateTime;
        @ColumnFamily("time")
        @Column("creationTime")
        private Timestamp createTime;
        @ColumnFamily("contact")
        private Contact contact;

        private HBaseColumn<List<String>> hc1;
        private List<HBaseColumn<Double>> hc2;
        private Map<Long, HBaseColumn<Map<Integer, Timestamp>>> hc3;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Name {
        @Column("givenName")
        private String firstName;
        private String middleName;
        @Column("Surname")
        private String lastName;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Contact {
        private long id;
        private long accountId;
        private String telephone;
        private String city;
        private String state;
        private String country;
        private String zipCode;
        private int status;
        private Timestamp lastUpdateTime;
        private Timestamp createTime;
    }

    @Test
    public void test_toAnyPut() {
        Account account = Account.builder()
                .id("2021")
                .gui(N.uuid())
                .emailAddress("abc@email.com")
                .name(Name.builder().firstName("fn").middleName("mn").lastName("ln").build())
                .contact(Contact.builder().city("San Jose").state("CA").build())
                .build();
        N.println(account);

        final String tableName = "account";

        hbaseExecutor.delete(tableName, AnyDelete.of(account.getId()));

        AnyPut put = AnyPut.from(account);
        N.println(put.toString(100));

        hbaseExecutor.put(tableName, put);

        Account dbAccount = hbaseExecutor.get(Account.class, tableName, AnyGet.of(account.getId()));
        N.println(dbAccount);

        assertEquals(account, dbAccount);
    }

    @Test
    public void test_mapper() {
        Account account = Account.builder()
                .id("2020")
                .gui(N.uuid())
                .emailAddress("abc@email.com")
                .name(Name.builder().firstName("fn").lastName("ln").build())
                .contact(Contact.builder().city("San Jose").state("CA").build())
                .build();
        N.println(account);

        accountMapper.delete(account);

        assertEquals(0, accountMapper.scan(AnyScan.create()).count());

        accountMapper.put(account);

        Account dbAccount = accountMapper.get(account.getId());
        N.println(dbAccount);

        assertEquals(account, dbAccount);
        assertEquals(account, accountMapper.scan(AnyScan.create()).onlyOne().orNull());

        accountMapper.deleteByRowKey(account.getId());

        assertNull(accountMapper.get(account.getId()));

        Account account2 = Account.builder()
                .id("2021")
                .gui(N.uuid())
                .emailAddress("abc@email.com")
                .name(Name.builder().firstName("fn").lastName("ln").build())
                .contact(Contact.builder().city("San Jose").state("CA").build())
                .build();

        List<Account> accounts = N.asList(account, account2);
        accountMapper.put(accounts);

        List<Account> dbAccounts = accountMapper.get(N.asList(account.getId(), account2.getId()));
        assertEquals(accounts, dbAccounts);
        assertEquals(accounts, accountMapper.scan(AnyScan.create()).toList());

        accountMapper.delete(dbAccounts);

        assertEquals(0, accountMapper.get(N.asList(account.getId(), account2.getId())).size());
        assertEquals(0, accountMapper.scan(AnyScan.create()).toList().size());

        assertNull(hbaseExecutor.mapper(Account.class).get(account.getId()));
        hbaseExecutor.mapper(Account.class).deleteByRowKey(account.getId());
    }

    @Test
    public void test_scan() {

        List<Account> accounts = Stream.range(1000, 1099)
                .map(it -> Account.builder()
                        .id(String.valueOf(it))
                        .gui(N.uuid())
                        .emailAddress(it + "abc@email.com")
                        .name(Name.builder().firstName(it + "fn").middleName(it + "mn").lastName(it + "ln").build())
                        .contact(Contact.builder().city(it + "San Jose").state("CA").build())
                        .build())
                .toList();

        accountMapper.delete(accounts);

        assertEquals(0, accountMapper.scan(AnyScan.create()).count());

        accountMapper.put(accounts);

        List<Account> dbAccounts = accountMapper.get(N.map(accounts, it -> it.getId()));
        N.println(dbAccounts);

        assertEquals(accounts, dbAccounts);
        assertEquals(accounts, accountMapper.scan(AnyScan.create()).toList());

        accountMapper.delete(accounts);

        assertTrue(accountMapper.exists(N.map(accounts, it -> it.getId())).stream().allMatch(it -> it.booleanValue() == false));
    }

    @Test
    public void test_HBaseColumn() throws IOException {
        final long ts = System.currentTimeMillis() + 10000;
        Account account = Account.builder()
                .id("2020")
                .gui(N.uuid())
                .emailAddress("abc@email.com")
                .name(Name.builder().firstName("fn").lastName("ln").build())
                .contact(Contact.builder().city("San Jose").state("CA").build())
                .hc1(HBaseColumn.valueOf(N.asList("hc1-abc", "hc1-ef"), ts))
                .hc2(N.asList(HBaseColumn.valueOf(1.22, ts)))
                .hc3(N.asMap(ts, HBaseColumn.valueOf(N.asMap(1, DateUtil.currentTimestamp()), ts)))
                .build();
        N.println(account);

        accountMapper.delete(account);

        assertEquals(0, accountMapper.scan(AnyScan.create()).count());

        accountMapper.put(account);

        Account dbAccount = accountMapper.get(AnyGet.of(account.getId()).readVersions(1000));
        N.println(dbAccount);

        assertEquals(account, dbAccount);
        assertEquals(account, accountMapper.scan(AnyScan.create()).onlyOne().orNull());

        accountMapper.deleteByRowKey(account.getId());
    }

    //    @Test
    //    public void test_01() throws ZooKeeperConnectionException, ServiceException, IOException {
    //        Result result = hbaseExecutor.get("account", AnyGet.of("row1"));
    //
    //        String row = Bytes.toString(result.getRow());
    //
    //        N.println(row);
    //
    //        if (result.advance()) {
    //            Cell cell = result.current();
    //            N.println(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
    //            N.println(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
    //
    //            N.println(cell.getTimestamp());
    //        }
    //    }
    //
    //    @Test
    //    public void test_crud() {
    //        Account account = createAccount2();
    //
    //        // Insert is supported by Model/entity
    //        hbaseExecutor.put("account", toAnyPut(account));
    //
    //        // Get is supported by Model/entity
    //        Account dbAccount = hbaseExecutor.get(Account.class, "account", AnyGet.of(account.getId()));
    //        N.println(dbAccount);
    //
    //        N.println(hbaseExecutor.get(Account.class, "account", N.asList(AnyGet.of(account.getId()))));
    //        N.println(hbaseExecutor.get(Account.class, "account", N.asList(AnyGet.of(account.getId()).value())));
    //
    //        assertEquals(hbaseExecutor.get(Account.class, "account", N.asList(AnyGet.of(account.getId()))),
    //                hbaseExecutor.get(Account.class, "account", N.asList(AnyGet.of(account.getId()).value())));
    //
    //        Timestamp ceateTime = hbaseExecutor.get(Timestamp.class, "account", AnyGet.of(account.getId()).addColumn("createTime", ""));
    //        N.println(ceateTime);
    //
    //        N.println(hbaseExecutor.get(Timestamp.class, "account", N.asList(AnyGet.of(account.getId()).addColumn("createTime", ""))));
    //        N.println(hbaseExecutor.get(Timestamp.class, "account", N.asList(AnyGet.of(account.getId()).addColumn("createTime", "").value())));
    //
    //        assertEquals(hbaseExecutor.get(Timestamp.class, "account", N.asList(AnyGet.of(account.getId()).addColumn("createTime", ""))),
    //                hbaseExecutor.get(Timestamp.class, "account", N.asList(AnyGet.of(account.getId()).addColumn("createTime", "").value())));
    //
    //        // Delete the inserted account
    //        hbaseExecutor.delete("account", AnyDelete.of(account.getId()));
    //        dbAccount = hbaseExecutor.get(Account.class, "account", AnyGet.of(account.getId()));
    //    }
    //
    //    @Test
    //    public void test_crud_2() throws IOException {
    //        Account account = createAccount2();
    //
    //        // Insert an account into HBase store
    //        Put put = new Put(Bytes.toBytes(account.getId()));
    //        put.addColumn(Bytes.toBytes("name"), Bytes.toBytes("firstName"), Bytes.toBytes(account.getName().firstName().value()));
    //        put.addColumn(Bytes.toBytes("name"), Bytes.toBytes("lastName"), Bytes.toBytes(account.getName().lastName().value()));
    //        put.addColumn(Bytes.toBytes("contact"), Bytes.toBytes("city"), Bytes.toBytes(account.getContact().city().value()));
    //        put.addColumn(Bytes.toBytes("contact"), Bytes.toBytes("state"), Bytes.toBytes(account.getContact().state().value()));
    //        put.addColumn(Bytes.toBytes("createTime"), Bytes.toBytes(""), Bytes.toBytes(N.stringOf(account.createTime().value())));
    //
    //        hbaseExecutor.put("account", put);
    //
    //        // Get the inserted account from HBase store
    //        Result result = hbaseExecutor.get("account", new Get(Bytes.toBytes(account.getId())));
    //        CellScanner cellScanner = result.cellScanner();
    //        while (cellScanner.advance()) {
    //            final Cell cell = cellScanner.current();
    //            N.println(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
    //            N.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
    //            N.println(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
    //            // ... a lot of work to do
    //        }
    //
    //        // Delete the inserted account from HBase store.
    //        hbaseExecutor.delete("account", new Delete(Bytes.toBytes(account.getId())));
    //    }
    //
    //    private Account createAccount2() {
    //        Account account = new Account();
    //        account.setId(123);
    //        account.setName(new Name().setFirstName("firstName123").setLastName("lastName123"));
    //        account.addCreateTime(N.currentTimestamp());
    //        account.setStrSet(N.asSet("a", "b", "c"));
    //        Map<String, Long> strMap = N.asMap("b", 2l);
    //        account.setStrMap(strMap);
    //        account.setContact(new AccountContact().setCity("Sunnyvale").setState("CA"));
    //        return account;
    //    }
    //
    //    @Test
    //    public void test_scan() throws IOException {
    //        for (int i = 0; i < 100; i++) {
    //            Account account = new Account();
    //            account.setId(i + 10000);
    //            account.setName(new Name().setFirstName("firstName123").setLastName("lastName123"));
    //            account.addCreateTime(N.currentTimestamp());
    //            account.setContact(new AccountContact().setCity("Sunnyvale").setState("CA"));
    //
    //            hbaseExecutor.put("account", HBaseExecutor.toAnyPut(account));
    //        }
    //
    //        N.sleep(1000);
    //        List<Result> results = hbaseExecutor.scan("account", "name");
    //        N.println(HBaseExecutor.toList(Account.class, results));
    //
    //        List<Account> accounts = hbaseExecutor.scan(Account.class, "account", "name");
    //        N.println(accounts);
    //
    //        List<Timestamp> createTimes = hbaseExecutor.scan(Timestamp.class, "account", "createTime");
    //        N.println(createTimes);
    //    }
    //
    //    @Test
    //    public void test_scan_2() throws IOException {
    //        for (int i = 0; i < 100; i++) {
    //            Account account = new Account();
    //            account.setId(i + 10000);
    //            account.setName(new Name().setFirstName("firstName123").setLastName("lastName123"));
    //            account.addCreateTime(N.currentTimestamp());
    //            account.setContact(new AccountContact().setCity("Sunnyvale").setState("CA"));
    //
    //            hbaseExecutor.put("account", HBaseExecutor.toAnyPut(account));
    //        }
    //
    //        N.sleep(1000);
    //        ResultScanner resultScanner = hbaseExecutor.getScanner("account", "name");
    //        N.println(HBaseExecutor.toList(Account.class, resultScanner));
    //
    //        resultScanner = hbaseExecutor.getScanner("account", "name");
    //        assertEquals(99, HBaseExecutor.toList(Account.class, resultScanner, 1, 99).size());
    //
    //        resultScanner = hbaseExecutor.getScanner("account", "name");
    //        assertEquals(99, HBaseExecutor.toList(Account.class, resultScanner, 1, 100).size());
    //
    //        resultScanner = hbaseExecutor.getScanner("account", "name");
    //        assertEquals(1, HBaseExecutor.toList(Account.class, resultScanner, 99, 9).size());
    //
    //        resultScanner = hbaseExecutor.getScanner("account", "name");
    //        assertEquals(0, HBaseExecutor.toList(Account.class, resultScanner, 100, 9).size());
    //
    //        resultScanner = hbaseExecutor.getScanner("account", "name");
    //        assertEquals(0, HBaseExecutor.toList(Account.class, resultScanner, 101, 9).size());
    //
    //        resultScanner = hbaseExecutor.getScanner("account", "name");
    //        assertEquals(0, HBaseExecutor.toList(Account.class, resultScanner, 0, 0).size());
    //        resultScanner = hbaseExecutor.getScanner("account", "name");
    //        assertEquals(0, HBaseExecutor.toList(Account.class, resultScanner, 99, 0).size());
    //        resultScanner = hbaseExecutor.getScanner("account", "name");
    //        assertEquals(0, HBaseExecutor.toList(Account.class, resultScanner, 100, 0).size());
    //
    //        resultScanner = hbaseExecutor.getScanner("account", "name");
    //        try {
    //            assertEquals(0, HBaseExecutor.toList(Account.class, resultScanner, -1, 0).size());
    //            fail("Should throw IllegalArgumentException");
    //        } catch (IllegalArgumentException e) {
    //
    //        }
    //
    //        try {
    //            assertEquals(0, HBaseExecutor.toList(Account.class, resultScanner, 0, -1).size());
    //            fail("Should throw IllegalArgumentException");
    //        } catch (IllegalArgumentException e) {
    //
    //        }
    //
    //        Result result = null;
    //        while ((result = resultScanner.next()) != null) {
    //            Account dbAccount = HBaseExecutor.toEntity(Account.class, result);
    //            N.println(dbAccount);
    //            N.println(hbaseExecutor.get(Account.class, "account", AnyGet.of(dbAccount.getId())));
    //            hbaseExecutor.delete("account", AnyDelete.of(dbAccount.getId()));
    //        }
    //    }
    //
    //    @Test
    //    public void test_serialize_json() {
    //        Account account = createAccount2();
    //
    //        String json = jsonParser.serialize(account, JSC.of(DateTimeFormat.LONG));
    //
    //        N.println(json);
    //
    //        Account account2 = jsonParser.deserialize(Account.class, json);
    //        N.println(account);
    //        N.println(account2);
    //
    //        Timestamp sysTime = N.currentTimestamp();
    //        N.println(sysTime.getTime());
    //
    //        Timestamp sysTime2 = N.asTimestamp(sysTime.getTime());
    //
    //        assertEquals(sysTime, sysTime2);
    //
    //        N.println(N.stringOf(sysTime));
    //
    //        sysTime2 = N.valueOf(Timestamp.class, N.stringOf(sysTime));
    //
    //        assertEquals(sysTime, sysTime2);
    //
    //        assertEquals(account, account2);
    //    }
    //
    //    @Test
    //    public void test_serialize_xml() {
    //        Account account = createAccount2();
    //        account.setCreateTime((Map) null);
    //
    //        String xml = xmlParser.serialize(account);
    //
    //        N.println(xml);
    //
    //        Account account2 = xmlParser.deserialize(Account.class, xml);
    //        N.println(account);
    //        N.println(account2);
    //
    //        assertEquals(account, account2);
    //    }
}
