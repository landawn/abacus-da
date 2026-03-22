/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Date;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.AbstractNoSQLTest;
import com.landawn.abacus.da.cassandra.CqlBuilder.ACCB;
import com.landawn.abacus.da.cassandra.CqlBuilder.LCCB;
import com.landawn.abacus.da.cassandra.CqlBuilder.NAC;
import com.landawn.abacus.da.cassandra.CqlBuilder.NLC;
import com.landawn.abacus.da.cassandra.CqlBuilder.NSC;
import com.landawn.abacus.da.cassandra.CqlBuilder.PAC;
import com.landawn.abacus.da.cassandra.CqlBuilder.PLC;
import com.landawn.abacus.da.cassandra.CqlBuilder.PSC;
import com.landawn.abacus.da.cassandra.CqlBuilder.SCCB;
import com.landawn.abacus.da.entity.Account;
import com.landawn.abacus.da.entity.Users;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.condition.In;
import com.landawn.abacus.util.Beans;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Profiler;
import com.landawn.abacus.util.Strings;

public class CqlBuilderTest extends AbstractNoSQLTest {

    @Test
    public void testUsingTimestampConvertsMillisToMicros() {
        final String cql = PSC.insert("id").into("users").usingTimestamp(123L).build().query();

        assertTrue(cql.contains("USING TIMESTAMP 123000"));
    }

    @Test
    public void testUsingTimestampConvertsDateMillisToMicros() {
        final String cql = PSC.insert("id").into("users").usingTimestamp(new Date(5L)).build().query();

        assertTrue(cql.contains("USING TIMESTAMP 5000"));
    }

    @Test
    public void test_000() {
        In cond = Filters.in("id", N.asList("a", "b"));
        N.println(NSC.deleteFrom(Users.class).append(cond).build());

        N.println(NSC.delete(Users.class).from(Users.class).append(cond).build().query());
    }

    public void test_00() {
        N.println(SCCB.select("firstName", "lastName").from("account").where(Filters.eq("id", 1)).build().query());
        N.println(ACCB.select("firstName", "lastName").from("account").where(Filters.eq("id", 1)).build().query());
        N.println(LCCB.select("firstName", "lastName").from("account").where(Filters.eq("id", 1)).build().query());

        N.println(PSC.select("firstName", "lastName").from("account").where(Filters.eq("id", 1)).build().query());
        N.println(PAC.select("firstName", "lastName").from("account").where(Filters.eq("id", 1)).build().query());
        N.println(PLC.select("firstName", "lastName").from("account").where(Filters.eq("id", 1)).build().query());

        N.println(NSC.select("firstName", "lastName").from("account").where(Filters.eq("id", 1)).build().query());
        N.println(NAC.select("firstName", "lastName").from("account").where(Filters.eq("id", 1)).build().query());
        N.println(NLC.select("firstName", "lastName").from("account").where(Filters.eq("id", 1)).build().query());

        N.println(LCCB.select("firstName", "last_name").from("account").where(Filters.eq("id", 1).and(Filters.ne("first_name", "fn"))).build().query());
        N.println(PLC.select("firstName", "last_name").from("account").where(Filters.eq("id", 1).and(Filters.ne("first_name", "fn"))).build().query());
        N.println(NLC.select("firstName", "last_name").from("account").where(Filters.eq("id", 1).and(Filters.ne("first_name", "fn"))).build().query());
    }

    @Test
    public void test_performance() {
        for (int i = 0; i < 10; i++) {
            String cql = SCCB.insert("gui", "firstName", "lastName", "lastUpdateTime", "createTime").into("account").build().query();
            assertEquals(102, cql.length());

            cql = NSC.select("gui", "firstName", "lastName", "lastUpdateTime", "createTime").from("account").where(Filters.eq("id", 1)).build().query();
            assertEquals(157, cql.length());
        }

        Profiler.run(16, 10000, 3, () -> {
            String cql = SCCB.insert("gui", "firstName", "lastName", "lastUpdateTime", "createTime").into("account").build().query();
            assertEquals(102, cql.length());

            cql = NSC.select("gui", "firstName", "lastName", "lastUpdateTime", "createTime").from("account").where(Filters.eq("id", 1)).build().query();
            assertEquals(157, cql.length());
        }).writeHtmlResult(System.out);
    }

    public void test_11() {

        N.println(NSC.update(Account.class).set("firstName", "lastName").iF(Filters.eq("firstName", "123")).build().query());

        String cql = SCCB.insert("gui", "firstName", "lastName").into("account").build().query();
        N.println(cql);

        cql = PSC.insert("gui", "firstName", "lastName").into("account").build().query();
        N.println(cql);

        Map<String, Object> props = N.asMap("gui", Strings.uuid(), "firstName", "fn", "lastName", "ln");

        cql = SCCB.insert(props).into("account").build().query();
        N.println(cql);
        N.println(SCCB.insert(props).into("account").build().parameters());

        cql = PSC.insert(props).into("account").build().query();
        N.println(cql);
        N.println(PSC.insert(props).into("account").build().parameters());

        cql = SCCB.select(N.asList("firstName", "lastName")).distinct().from("account2", "account2").where("id > ?").build().query();
        N.println(cql);

        Map<String, String> m = N.asMap("firstName", "lastName");
        cql = SCCB.select(m).distinct().from("account2", "account2").where("id > ?").build().query();
        N.println(cql);
    }

    public void test_set() {
        String cql = "UPDATE account SET id = ?, first_name=? WHERE id > 0";
        assertEquals(cql, SCCB.update("account").set("id = ?, first_name=?").where("id > 0").build().query());

        cql = "UPDATE account SET first_name = ? WHERE id > 0";
        assertEquals(cql, SCCB.update("account").set("first_name").where("id > 0").build().query());

        cql = "UPDATE account SET id = ?, first_name = ? WHERE id > 0";
        assertEquals(cql, SCCB.update("account").set("id", "first_name").where("id > 0").build().query());

        cql = "UPDATE account SET id = ?, first_name = ? WHERE id > 0";
        assertEquals(cql, SCCB.update("account").set(N.asList("id", "first_name")).where("id > 0").build().query());

        cql = "UPDATE account SET id = 1, first_name = 'updatedFM' WHERE id > 0";
        assertEquals(cql, SCCB.update("account").set(N.asMap("id", 1, "first_name", "updatedFM")).where("id > 0").build().query());

        cql = "UPDATE account SET id = ?, first_name = ? WHERE id > 0";
        assertEquals(cql, SCCB.update("account").set(N.asMap("id", Filters.QME, "first_name", Filters.QME)).where("id > 0").build().query());
    }

    public void testCqlBuilder_2() {
        Account account = Beans.newRandom(Account.class);
        String cql = SCCB.insert(account).into("account").build().query();

        N.println(cql);

        N.println(SCCB.update("account").set("first_name = ?").where("id = ?").build().query());
    }

    public void testSQL_1() {
        String cql = ACCB.insert("id", "first_name", "last_name").into("account").build().query();
        N.println(cql);

        cql = ACCB.insert(N.asList("id", "first_name", "last_name")).into("account").build().query();
        N.println(cql);

        cql = SCCB.insert(N.asMap("id", 1, "first_name", "firstNamae", "last_name", "last_name")).into("account").build().query();
        N.println(cql);

        cql = SCCB.select("id, first_name").from("account").where("id > 0").limit(10).build().query();
        N.println(cql);

        cql = SCCB.select("id", "first_name").from("account").where("id > 0").limit(10).build().query();
        N.println(cql);

        cql = SCCB.select(N.asList("id", "first_name")).from("account").where("id > 0").limit(10).build().query();
        N.println(cql);

        cql = SCCB.select("id, first_name").from("account").where("id > 0").limit(10).build().query();
        N.println(cql);

        Account account = new Account();
        account.setId(123);
        account.setFirstName("first_name");

        cql = SCCB.update("account").set("first_name=?").where("id = 1").build().query();
        N.println(cql);
        assertEquals("UPDATE account SET first_name=? WHERE id = 1", cql);

        cql = SCCB.update("account").set("first_name = ?").where("id = 1").build().query();
        N.println(cql);
        assertEquals("UPDATE account SET first_name = ? WHERE id = 1", cql);

        cql = SCCB.update("account").set("first_name= ?").where("id = 1").build().query();
        N.println(cql);
        assertEquals("UPDATE account SET first_name= ? WHERE id = 1", cql);

        cql = SCCB.update("account").set("first_name =?").where("id = 1").build().query();
        N.println(cql);
        assertEquals("UPDATE account SET first_name =? WHERE id = 1", cql);

    }

    //
    //    public void test_perf() {
    //        Profiler.run(new Throwables.Runnable<RuntimeException>() {
    //            @Override
    //            public void run() {
    //                E.batchInsert(createAccountPropsList(99)).into("account").build().query().length();
    //            }
    //        }, 32, 10000, 3).printResult();
    //
    //        Profiler.run(new Throwables.Runnable<RuntimeException>() {
    //            @Override
    //            public void run() {
    //                E.batchInsert(createAccountPropsList(99)).into("account").build().query().length();
    //            }
    //        }, 32, 10000, 3).printResult();
    //    }

    public void test_QME() {
        String cql = SCCB.select("first_name", "last_name").from("account").where(Filters.eq("first_name", Filters.QME)).build().query();
        N.println(cql);

        assertEquals("SELECT first_name AS \"first_name\", last_name AS \"last_name\" FROM account WHERE first_name = ?", cql);

        Map<String, Object> props = N.asMap("first_name", "?", "last_name", "?");
        cql = SCCB.insert(props).into("account").build().query();
        N.println(cql);
        assertEquals("INSERT INTO account (first_name, last_name) VALUES ('?', '?')", cql);

        props = N.asMap("first_name", Filters.QME, "last_name", Filters.QME);
        cql = SCCB.insert(props).into("account").build().query();
        N.println(cql);
        assertEquals("INSERT INTO account (first_name, last_name) VALUES (?, ?)", cql);
    }

    public void test_NPE() {
        String cql = SCCB.select("firstName", "last_name").from("account").where(Filters.eq("firstName", Filters.QME)).build().query();
        N.println(cql);

        // assertEquals("SELECT first_name, last_name FROM account WHERE first_name = #{firstName}", cql);

        cql = SCCB.insert(N.asMap("firstName", Filters.QME, "lastName", Filters.QME)).into("account").build().query();
        N.println(cql);
        assertEquals("INSERT INTO account (first_name, last_name) VALUES (?, ?)", cql);

        cql = SCCB.insert(N.asMap("first_name", Filters.QME, "last_name", Filters.QME)).into("account").build().query();
        N.println(cql);
        assertEquals("INSERT INTO account (first_name, last_name) VALUES (?, ?)", cql);

        cql = SCCB.update("account").set(N.asMap("first_name", Filters.QME, "last_name", Filters.QME)).build().query();
        N.println(cql);
        assertEquals("UPDATE account SET first_name = ?, last_name = ?", cql);

        cql = SCCB.update("account").set(N.asMap("firstNmae", Filters.QME, "lastName", Filters.QME)).build().query();
        N.println(cql);
        assertEquals("UPDATE account SET first_nmae = ?, last_name = ?", cql);
    }

    public void test_excludedPropNames() throws Exception {
        String cql = SCCB.select(Account.class, N.asSet("firstName")).from("account").build().query();
        N.println(cql);
        assertEquals(
                "SELECT id AS \"id\", gui AS \"gui\", email_address AS \"emailAddress\", middle_name AS \"middleName\", last_name AS \"lastName\", birth_date AS \"birthDate\", status AS \"status\", last_update_time AS \"lastUpdateTime\", create_time AS \"createTime\", contact AS \"contact\", devices AS \"devices\" FROM account",
                cql);
    }

    public void test_expr_cond() throws Exception {
        String cql = NSC.select("id", "firstName").from("account").where("firstName=?").build().query();
        N.println(cql);
        assertEquals("SELECT id AS \"id\", first_name AS \"firstName\" FROM account WHERE first_name=?", cql);

        cql = NSC.select("id", "firstName").from("account").where("firstName=:firstName").build().query();
        N.println(cql);
        assertEquals("SELECT id AS \"id\", first_name AS \"firstName\" FROM account WHERE first_name=:firstName", cql);

        cql = NSC.select("id", "firstName").from("account").where("firstName = :firstName").build().query();
        N.println(cql);
        assertEquals("SELECT id AS \"id\", first_name AS \"firstName\" FROM account WHERE first_name = :firstName", cql);

        cql = NSC.select("id", "firstName").from("account").where("firstName=$firstName").build().query();
        N.println(cql);
        assertEquals("SELECT id AS \"id\", first_name AS \"firstName\" FROM account WHERE first_name=$firstName", cql);

        cql = NSC.select("id", "firstName").from("account").where("firstName = $firstName").build().query();
        N.println(cql);
        assertEquals("SELECT id AS \"id\", first_name AS \"firstName\" FROM account WHERE first_name = $firstName", cql);

        cql = NSC.select("id", "firstName").from("account").where("firstName=#{firstName}").build().query();
        N.println(cql);
        assertEquals("SELECT id AS \"id\", first_name AS \"firstName\" FROM account WHERE first_name=#{firstName}", cql);

        cql = NSC.select("id", "firstName").from("account").where("firstName = #{firstName}").build().query();
        N.println(cql);
        assertEquals("SELECT id AS \"id\", first_name AS \"firstName\" FROM account WHERE first_name = #{firstName}", cql);

        cql = NSC.select("id", "firstName").from("account").where("firstName=\"firstName\"").build().query();
        N.println(cql);
        assertEquals("SELECT id AS \"id\", first_name AS \"firstName\" FROM account WHERE first_name=\"firstName\"", cql);

        cql = NSC.select("id", "firstName").from("account").where("firstName = \"firstName\"").build().query();
        N.println(cql);
        assertEquals("SELECT id AS \"id\", first_name AS \"firstName\" FROM account WHERE first_name = \"firstName\"", cql);

        cql = NSC.select("id", "firstName").from("account").where("firstName=\"firstName\"").build().query();
        N.println(cql);
        assertEquals("SELECT id AS \"id\", first_name AS \"firstName\" FROM account WHERE first_name=\"firstName\"", cql);

        cql = NSC.select("id", "firstName").from("account").where("firstName = \"firstName\"").build().query();
        N.println(cql);
        assertEquals("SELECT id AS \"id\", first_name AS \"firstName\" FROM account WHERE first_name = \"firstName\"", cql);

        cql = NSC.select("id", "firstName").from("account").where("firstName(abc, 123) = \"firstName\"").build().query();
        N.println(cql);
        assertEquals("SELECT id AS \"id\", first_name AS \"firstName\" FROM account WHERE firstName(abc, 123) = \"firstName\"", cql);

        cql = NSC.select("id", "firstName").from("account").where("firstName (abc, 123) = \"firstName\"").build().query();
        N.println(cql);
        assertEquals("SELECT id AS \"id\", first_name AS \"firstName\" FROM account WHERE first_name (abc, 123) = \"firstName\"", cql);
    }

    @Test
    public void test_limit_offset() {
        String cql = NSC.select("firstName", "lastName").from("account").where(Filters.eq("id", Filters.QME)).limit(9).build().query();
        N.println(cql);
        assertEquals("SELECT first_name AS \"firstName\", last_name AS \"lastName\" FROM account WHERE id = :id LIMIT 9", cql);
    }
}
