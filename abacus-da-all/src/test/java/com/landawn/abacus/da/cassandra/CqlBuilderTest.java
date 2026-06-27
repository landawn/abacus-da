/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.cassandra;

import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.ACCB;
import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.LCCB;
import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.NAC;
import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.NLC;
import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.NSB;
import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.NSC;
import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.PAC;
import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.PLC;
import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.PSB;
import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.PSC;
import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.SCCB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Date;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.da.entity.Account;
import com.landawn.abacus.da.entity.Users;
import com.landawn.abacus.query.AbstractQueryBuilder.SP;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.query.condition.In;
import com.landawn.abacus.util.Beans;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Profiler;
import com.landawn.abacus.util.Strings;

public class CqlBuilderTest extends TestBase {

    @Test
    public void testUsingTimestampConvertsMillisToMicros() {
        final String cql = PSC.insert("id").into("users").usingTimestamp(123L).build().query();

        assertTrue(cql.contains("USING TIMESTAMP 123000"));
    }

    @Test
    public void zzz_explore() {
        N.println("BETWEEN PSC: " + PSC.select("id").from("account").where(Filters.between("age", 18, 65)).build().query());
        N.println("BETWEEN NSC: " + NSC.select("id").from("account").where(Filters.between("age", 18, 65)).build().query());
        N.println("NOTBETWEEN PSC: " + PSC.select("id").from("account").where(Filters.notBetween("age", 18, 65)).build().query());
        N.println("IN PSC: " + PSC.select("id").from("account").where(Filters.in("id", N.asList(1, 2, 3))).build().query());
        N.println("IN NSC: " + NSC.select("id").from("account").where(Filters.in("id", N.asList(1, 2, 3))).build().query());
        N.println("NOTIN NSC: " + NSC.select("id").from("account").where(Filters.notIn("id", N.asList(1, 2, 3))).build().query());
        N.println("iF cond PSC: " + PSC.update("account").set("firstName").where(Filters.eq("id", 1)).iF(Filters.eq("status", "x")).build().query());
        N.println("DELETE cols: " + PSC.delete("firstName", "lastName").from("account").where(Filters.eq("id", 1)).build().query());
        N.println("AND junction: " + PSC.select("id").from("account").where(Filters.and(Filters.eq("a", 1), Filters.eq("b", 2))).build().query());
        assertThrows(IllegalArgumentException.class, () -> PSC.select("id").from("account").where(Filters.or(Filters.eq("a", 1), Filters.eq("b", 2))).build());
        N.println("TTL+IF order: " + PSC.update("account").set("firstName").where(Filters.eq("id", 1)).usingTTL(60).iF(Filters.eq("s", "x")).build().query());
        N.println("INSERT TTL: " + PSC.insert("id", "name").into("account").usingTTL(60).build().query());
        N.println("batchInsert: " + SCCB.batchInsert(N.asList(N.asMap("firstName", "a"), N.asMap("firstName", "b"))).into("account").build().query());
        N.println("NotEqual null: " + SCCB.select("id").from("account").where(Filters.ne("firstName", "x")).build().query());
        N.println("InSubQuery: " + PSC.select("id").from("account").where(Filters.in("id", SubQueryGen())).build().query());
        N.println("NotInSubQuery: " + NSC.select("id").from("account").where(Filters.notIn("id", SubQueryGen())).build().query());
        N.println("parse cond NSC: " + NSC.renderCondition(Filters.and(Filters.eq("status", "A"), Filters.gt("balance", 1000)), Account.class).build().query());
        N.println(
                "parse cond SCCB: " + SCCB.renderCondition(Filters.and(Filters.eq("status", "A"), Filters.gt("balance", 1000)), Account.class).build().query());
        assertThrows(IllegalArgumentException.class,
                () -> PSC.select("id").from("account").where(Filters.and(Filters.eq("a", 1), Filters.or(Filters.eq("b", 2), Filters.eq("c", 3)))).build());
        N.println("update entity: " + SCCB.update(Account.class).set("firstName", "lastName").where(Filters.eq("id", 1)).build().query());
        N.println("multi-col InSubQuery: " + PSC.select("id")
                .from("account")
                .where(new com.landawn.abacus.query.condition.InSubQuery(N.asList("a", "b"),
                        new com.landawn.abacus.query.condition.SubQuery("t", N.asList("x", "y"), Filters.eq("z", 1))))
                .build()
                .query());
        N.println("eq null SCCB: " + SCCB.select("id").from("account").where(Filters.eq("firstName", null)).build().query());
        N.println("count SCCB: " + SCCB.count("account").where(Filters.eq("id", 1)).build().query());
    }

    private static com.landawn.abacus.query.condition.SubQuery SubQueryGen() {
        return new com.landawn.abacus.query.condition.SubQuery("account", N.asList("id"), Filters.eq("status", "A"));
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
        Account account = Beans.newRandomBean(Account.class);
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

    /**
     * Regression test for the count(*) LIMIT 1 bug.
     *
     * <p>{@code CassandraExecutorBase.count(Class, Condition)} and {@code asyncCount(Class, Condition)}
     * previously built the count query via {@code prepareQuery(targetClass, [count(*)], where, 1)},
     * which appended {@code LIMIT 1} to a {@code SELECT count(*)} statement. In Cassandra, a LIMIT on
     * a {@code count(*)} aggregate caps the returned count itself, so a table with 10 matching rows
     * would report a count of 1.</p>
     *
     * <p>This test reproduces the exact CQL the (fixed) count path builds and asserts it no longer
     * carries a bogus {@code LIMIT}.</p>
     */
    @Test
    public void test_count_query_has_no_bogus_limit() {
        // Mirrors CassandraExecutorBase.prepareQuery(..., count = 0) for the SNAKE_CASE naming policy.
        final String countCql = NSC.select(N.asList(CqlBuilder.COUNT_ALL)).from("account").appendIf(true, Filters.eq("id", Filters.QME)).build().query();
        N.println(countCql);

        assertTrue(countCql.startsWith("SELECT count(*)"), countCql);
        assertTrue(countCql.contains("FROM account"), countCql);
        assertTrue(countCql.contains("WHERE id = :id"), countCql);
        // The core of the fix: a COUNT query must NOT be capped by LIMIT.
        assertTrue(!countCql.toUpperCase().contains("LIMIT"), "Count CQL must not contain a LIMIT clause: " + countCql);

        // Sanity check: the buggy behavior (count = 1 -> limit(1)) would have produced "LIMIT 1",
        // which truncates the aggregated count in Cassandra.
        final String buggyCql = NSC.select(N.asList(CqlBuilder.COUNT_ALL))
                .from("account")
                .appendIf(true, Filters.eq("id", Filters.QME))
                .limit(1)
                .build()
                .query();
        N.println(buggyCql);
        assertTrue(buggyCql.endsWith("LIMIT 1"), buggyCql);
    }

    /**
     * Verifies the (NamingPolicy, SqlPolicy) pairing and createInstance() return type implied by each
     * dialect class name, exercised through the generated CQL string.
     *
     * <ul>
     * <li>S*  -> no params (RAW_SQL, values inlined)</li>
     * <li>P*  -> parameterized (?)</li>
     * <li>N*  -> named (:name)</li>
     * <li>*CB / *SC -> snake_case, *AC -> SCREAMING_SNAKE_CASE, *LC -> camelCase (NO_CHANGE for *SB)</li>
     * </ul>
     */
    @Test
    public void test_dialect_policy_pairings() {
        // The naming policy applies to ALL identifiers (select columns AND condition columns),
        // so we assert the select-column casing per suffix and the parameter style per prefix,
        // without over-asserting the WHERE-column casing.

        // S* : RAW_SQL -> literal value embedded (no '?' / no named ':' placeholder).
        final String sccb = SCCB.select("firstName").from("account").where(Filters.eq("id", 123)).build().query();
        N.println(sccb);
        assertTrue(sccb.endsWith("= 123"), sccb);
        assertTrue(!sccb.contains("?") && !sccb.contains(" = :"), sccb);
        assertTrue(sccb.contains("first_name"), sccb);

        final String accb = ACCB.select("firstName").from("account").where(Filters.eq("id", 123)).build().query();
        N.println(accb);
        assertTrue(accb.endsWith("= 123"), accb);
        assertTrue(!accb.contains("?") && !accb.contains(" = :"), accb);
        assertTrue(accb.contains("FIRST_NAME"), accb);

        final String lccb = LCCB.select("firstName").from("account").where(Filters.eq("id", 123)).build().query();
        N.println(lccb);
        assertTrue(lccb.endsWith("= 123"), lccb);
        assertTrue(!lccb.contains("?") && !lccb.contains(" = :"), lccb);
        assertTrue(lccb.contains("firstName"), lccb);

        // P* : PARAMETERIZED_SQL -> '?' placeholder.
        final String psc = PSC.select("firstName").from("account").where(Filters.eq("id", 123)).build().query();
        N.println(psc);
        assertTrue(psc.endsWith("= ?"), psc);
        assertTrue(psc.contains("first_name"), psc);

        final String pac = PAC.select("firstName").from("account").where(Filters.eq("id", 123)).build().query();
        N.println(pac);
        assertTrue(pac.endsWith("= ?"), pac);
        assertTrue(pac.contains("FIRST_NAME"), pac);

        final String plc = PLC.select("firstName").from("account").where(Filters.eq("id", 123)).build().query();
        N.println(plc);
        assertTrue(plc.endsWith("= ?"), plc);
        assertTrue(plc.contains("firstName"), plc);

        // N* : NAMED_SQL -> ':name' placeholder.
        final String nsc = NSC.select("firstName").from("account").where(Filters.eq("id", 123)).build().query();
        N.println(nsc);
        assertTrue(nsc.contains(" = :"), nsc);
        assertTrue(nsc.contains("first_name"), nsc);

        final String nac = NAC.select("firstName").from("account").where(Filters.eq("id", 123)).build().query();
        N.println(nac);
        assertTrue(nac.contains(" = :"), nac);
        assertTrue(nac.contains("FIRST_NAME"), nac);

        final String nlc = NLC.select("firstName").from("account").where(Filters.eq("id", 123)).build().query();
        N.println(nlc);
        assertTrue(nlc.contains(" = :"), nlc);
        assertTrue(nlc.contains("firstName"), nlc);
    }

    /**
     * Verifies the Cassandra-specific keyword constants render with correct spacing and that
     * USING TIMESTAMP applies the ms -> microsecond (x1000) conversion while USING TTL does not.
     */
    @Test
    public void test_cassandra_keyword_generation() {
        // USING TTL: seconds, NOT multiplied (must be exactly the supplied value).
        final String ttlCql = PSC.update("account").set("firstName").where(Filters.eq("id", 1)).usingTTL(3600).build().query();
        N.println(ttlCql);
        assertTrue(ttlCql.contains("UPDATE account USING TTL 3600 SET"), ttlCql);
        assertTrue(ttlCql.contains(" WHERE id = ?"), ttlCql);

        // USING TIMESTAMP(long): milliseconds -> microseconds (x1000).
        final String tsLongCql = PSC.update("account").set("firstName").where(Filters.eq("id", 1)).usingTimestamp(1234567890123L).build().query();
        N.println(tsLongCql);
        assertTrue(tsLongCql.contains("UPDATE account USING TIMESTAMP 1234567890123000 SET"), tsLongCql);
        assertTrue(tsLongCql.contains(" WHERE id = ?"), tsLongCql);

        // USING TIMESTAMP(Date): Date.getTime() (ms) -> microseconds (x1000), same as the long overload.
        final String tsDateCql = PSC.update("account").set("firstName").where(Filters.eq("id", 1)).usingTimestamp(new Date(1234567890123L)).build().query();
        N.println(tsDateCql);
        assertTrue(tsDateCql.contains("UPDATE account USING TIMESTAMP 1234567890123000 SET"), tsDateCql);
        assertTrue(tsDateCql.contains(" WHERE id = ?"), tsDateCql);

        // IF EXISTS vs IF NOT EXISTS (not swapped) + leading space (must not yield "?IF EXISTS").
        final String ifExistsCql = PSC.update("account").set("firstName").where(Filters.eq("id", 1)).ifExists().build().query();
        N.println(ifExistsCql);
        assertTrue(ifExistsCql.endsWith(" IF EXISTS"), ifExistsCql);
        assertTrue(!ifExistsCql.contains("IF NOT EXISTS"), ifExistsCql);

        final String ifNotExistsCql = PSC.update("account").set("firstName").where(Filters.eq("id", 1)).ifNotExists().build().query();
        N.println(ifNotExistsCql);
        assertTrue(ifNotExistsCql.endsWith(" IF NOT EXISTS"), ifNotExistsCql);

        // ALLOW FILTERING leading space (must not produce "id = ?ALLOW FILTERING").
        final String afCql = PSC.select("firstName").from("account").where(Filters.eq("id", 1)).allowFiltering().build().query();
        N.println(afCql);
        assertTrue(afCql.endsWith(" ALLOW FILTERING"), afCql);
        assertTrue(afCql.contains("? ALLOW FILTERING"), afCql);

        // Lightweight transaction IF clause spacing (leading space, single space around expr).
        final String iFCql = PSC.update("account").set("firstName").where(Filters.eq("id", 1)).iF("status = 'inactive'").build().query();
        N.println(iFCql);
        assertTrue(iFCql.endsWith(" IF status = 'inactive'"), iFCql);

        final String onlyIfExprCql = PSC.update("account").set("firstName").where(Filters.eq("id", 1)).onlyIf("status = 'inactive'").build().query();
        N.println(onlyIfExprCql);
        assertEquals(iFCql, onlyIfExprCql);

        final String onlyIfConditionCql = PSC.update("account")
                .set("firstName")
                .where(Filters.eq("id", 1))
                .onlyIf(Filters.eq("status", "inactive"))
                .build()
                .query();
        N.println(onlyIfConditionCql);
        assertTrue(onlyIfConditionCql.endsWith(" IF status = ?"), onlyIfConditionCql);
    }

    /**
     * Regression guard for {@code appendCondition}: BETWEEN / NOT BETWEEN must emit the correct
     * keyword (not swapped) with the {@code AND} separator, IN / NOT IN must emit the correct
     * keyword with a parenthesized, comma-separated placeholder list, and the column must be
     * snake_case formalized. These are the parallel condition-rendering paths most exposed to
     * copy-paste/inverted-keyword defects.
     */
    @Test
    public void test_between_in_keyword_and_operator_rendering() {
        final String between = PSC.select("id").from("account").where(Filters.between("age", 18, 65)).build().query();
        N.println(between);
        assertEquals("SELECT id FROM account WHERE age BETWEEN ? AND ?", between);

        final String notBetween = PSC.select("id").from("account").where(Filters.notBetween("age", 18, 65)).build().query();
        N.println(notBetween);
        assertEquals("SELECT id FROM account WHERE age NOT BETWEEN ? AND ?", notBetween);

        final String in = PSC.select("id").from("account").where(Filters.in("id", N.asList(1, 2, 3))).build().query();
        N.println(in);
        assertEquals("SELECT id FROM account WHERE id IN (?, ?, ?)", in);

        final String notIn = PSC.select("id").from("account").where(Filters.notIn("id", N.asList(1, 2, 3))).build().query();
        N.println(notIn);
        assertEquals("SELECT id FROM account WHERE id NOT IN (?, ?, ?)", notIn);

        // NAMED_SQL min/max + indexed-IN parameter naming must not collide between the two bounds/elements.
        final String namedBetween = NSC.select("id").from("account").where(Filters.between("age", 18, 65)).build().query();
        N.println(namedBetween);
        assertEquals("SELECT id FROM account WHERE age BETWEEN :minAge AND :maxAge", namedBetween);

        final String namedIn = NSC.select("id").from("account").where(Filters.in("id", N.asList(1, 2, 3))).build().query();
        N.println(namedIn);
        assertEquals("SELECT id FROM account WHERE id IN (:id1, :id2, :id3)", namedIn);
    }

    /**
     * Regression guard for supported AND junction rendering and rejected OR junctions in Cassandra
     * WHERE clauses.
     */
    @Test
    public void test_and_or_junction_keyword_rendering() {
        final String and = PSC.select("id").from("account").where(Filters.and(Filters.eq("a", 1), Filters.eq("b", 2))).build().query();
        N.println(and);
        assertEquals("SELECT id FROM account WHERE (a = ?) AND (b = ?)", and);

        assertThrows(IllegalArgumentException.class, () -> PSC.select("id").from("account").where(Filters.or(Filters.eq("a", 1), Filters.eq("b", 2))).build());
    }

    /**
     * Regression guard that the SELECT vs DELETE keyword (and column list) is emitted by the correct
     * branch of {@code appendOperationBeforeFrom} for column-level DELETE and that snake_case
     * formalization is applied to the deleted columns.
     */
    @Test
    public void test_select_vs_delete_keyword() {
        final String select = PSC.select("firstName", "lastName").from("account").where(Filters.eq("id", 1)).build().query();
        N.println(select);
        assertTrue(select.startsWith("SELECT "), select);
        assertTrue(select.contains("first_name") && select.contains("last_name"), select);

        final String delete = PSC.delete("firstName", "lastName").from("account").where(Filters.eq("id", 1)).build().query();
        N.println(delete);
        assertEquals("DELETE first_name, last_name FROM account WHERE id = ?", delete);

        final String deleteFrom = PSC.deleteFrom("account").where(Filters.eq("id", 1)).build().query();
        N.println(deleteFrom);
        assertEquals("DELETE FROM account WHERE id = ?", deleteFrom);
    }

    /**
     * Regression guard that USING TTL is NOT unit-converted (seconds, verbatim) while USING TIMESTAMP
     * IS converted ms -&gt; microseconds (x1000) for every overload, and that the two clauses are not
     * swapped.
     */
    @Test
    public void test_ttl_vs_timestamp_unit_conversion() {
        final String ttl = PSC.update("account").set("firstName").where(Filters.eq("id", 1)).usingTTL(3600).build().query();
        N.println(ttl);
        assertTrue(ttl.contains("UPDATE account USING TTL 3600 SET"), ttl);
        assertTrue(ttl.contains(" WHERE id = ?"), ttl);
        assertTrue(!ttl.contains("TIMESTAMP"), ttl);

        final String tsLong = PSC.update("account").set("firstName").where(Filters.eq("id", 1)).usingTimestamp(1000L).build().query();
        N.println(tsLong);
        assertTrue(tsLong.contains("UPDATE account USING TIMESTAMP 1000000 SET"), tsLong);
        assertTrue(tsLong.contains(" WHERE id = ?"), tsLong);

        final String tsDate = PSC.update("account").set("firstName").where(Filters.eq("id", 1)).usingTimestamp(new Date(2L)).build().query();
        N.println(tsDate);
        assertTrue(tsDate.contains("UPDATE account USING TIMESTAMP 2000 SET"), tsDate);
        assertTrue(tsDate.contains(" WHERE id = ?"), tsDate);

        final String tsStr = PSC.update("account").set("firstName").where(Filters.eq("id", 1)).usingTimestamp("42").build().query();
        N.println(tsStr);
        // String overload is verbatim (no x1000) per its contract.
        assertTrue(tsStr.contains("UPDATE account USING TIMESTAMP 42 SET"), tsStr);
        assertTrue(tsStr.contains(" WHERE id = ?"), tsStr);
    }

    @Test
    public void test_updateUsingClausesAreInsertedBeforeSetAndCombined() {
        final String cql = PSC.update("account").set("firstName").where(Filters.eq("id", 1)).usingTTL(60).usingTimestamp("42").build().query();

        assertTrue(cql.contains("UPDATE account USING TTL 60 AND TIMESTAMP 42 SET"), cql);
        assertTrue(cql.contains(" WHERE id = ?"), cql);
    }

    /**
     * Regression guard for the missing {@code ComposableCell} branch in
     * {@code CqlBuilder.appendCondition}. {@code Filters.not(...)} returns a {@code Not} which
     * extends {@code ComposableCell} (NOT {@code Cell}); before the fix, {@code Not} (and the
     * other {@code ComposableCell} subclasses: {@code Exists}, {@code NotExists}, {@code Any},
     * {@code All}, {@code Some}) fell through to the final {@code else} branch of
     * {@code appendCondition} and raised {@code IllegalArgumentException("Unsupported condition: ...")}.
     *
     * <p>Asserted via {@code contains(...)} on a whitespace-normalized form: the underlying
     * {@code Cell}/{@code ComposableCell} rendering pattern in both {@code CqlBuilder} and the
     * sister {@code SqlBuilder} prepends a defensive leading space (needed when invoked inside a
     * {@code (...)} junction element); when invoked directly via {@code where(Not)} this yields a
     * cosmetic double-space before {@code NOT}, which CQL/SQL parsers tolerate. The asserted
     * substring captures the load-bearing token ordering and parenthesization, not the cosmetic
     * spacing.</p>
     */
    @Test
    public void test_composable_cell_not_is_supported() {
        // Filters.not(Binary) — must render "NOT (binary)" rather than throwing.
        final String pscNot = PSC.select("id").from("account").where(Filters.not(Filters.eq("status", "X"))).build().query();
        N.println(pscNot);
        assertTrue(pscNot.startsWith("SELECT id FROM account WHERE"), pscNot);
        assertTrue(normalizeWs(pscNot).contains("WHERE NOT (status = ?)"), pscNot);

        // Snake-case formalization of the inner column must still apply inside the NOT wrapper.
        final String pscNotSnake = PSC.select("id").from("account").where(Filters.not(Filters.eq("firstName", "X"))).build().query();
        N.println(pscNotSnake);
        assertTrue(normalizeWs(pscNotSnake).contains("WHERE NOT (first_name = ?)"), pscNotSnake);

        // Inline-value (RAW_SQL) variant must also work and inline the value, not throw.
        final String sccbNot = SCCB.select("id").from("account").where(Filters.not(Filters.eq("status", "X"))).build().query();
        N.println(sccbNot);
        assertTrue(normalizeWs(sccbNot).contains("WHERE NOT (status = 'X')"), sccbNot);

        // Named-parameter (NSC) variant must also work.
        final String nscNot = NSC.select("id").from("account").where(Filters.not(Filters.eq("status", "X"))).build().query();
        N.println(nscNot);
        assertTrue(normalizeWs(nscNot).contains("WHERE NOT (status = :status)"), nscNot);
    }

    private static String normalizeWs(final String s) {
        return s.replaceAll("\\s+", " ").trim();
    }

    // ---------------------------------------------------------------------------------------------
    // NSB (NO_CHANGE naming + NAMED_SQL): exercise all factory methods (was 0% coverage).
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testNSB_insertString() {
        final String cql = NSB.insert("firstName").into("account").build().query();
        assertTrue(cql.startsWith("INSERT INTO account"), cql);
        // NO_CHANGE policy: column name is kept as-is, named param uses the same identifier.
        assertTrue(cql.contains("(firstName)"), cql);
        assertTrue(cql.contains(":firstName"), cql);
    }

    @Test
    public void testNSB_insertStringArray() {
        final String cql = NSB.insert("firstName", "lastName").into("account").build().query();
        assertEquals("INSERT INTO account (firstName, lastName) VALUES (:firstName, :lastName)", cql);
    }

    @Test
    public void testNSB_insertCollection() {
        final String cql = NSB.insert(N.asList("firstName", "lastName")).into("account").build().query();
        assertEquals("INSERT INTO account (firstName, lastName) VALUES (:firstName, :lastName)", cql);
    }

    @Test
    public void testNSB_insertMap() {
        final Map<String, Object> props = N.asMap("firstName", Filters.QME, "lastName", Filters.QME);
        final String cql = NSB.insert(props).into("account").build().query();
        assertTrue(cql.startsWith("INSERT INTO account"), cql);
        assertTrue(cql.contains("firstName"), cql);
        assertTrue(cql.contains("lastName"), cql);
    }

    @Test
    public void testNSB_insertEntity() {
        final Account account = new Account();
        account.setFirstName("John");
        account.setLastName("Doe");
        final String cql = NSB.insert(account).into("account").build().query();
        assertTrue(cql.startsWith("INSERT INTO account"), cql);
        assertTrue(cql.contains(":firstName") && cql.contains(":lastName"), cql);
    }

    @Test
    public void testNSB_insertEntityWithExcluded() {
        final Account account = new Account();
        account.setFirstName("John");
        account.setLastName("Doe");
        final String cql = NSB.insert(account, N.asSet("lastName")).into("account").build().query();
        assertTrue(cql.contains(":firstName"), cql);
        assertTrue(!cql.contains(":lastName"), cql);
    }

    @Test
    public void testNSB_insertClass() {
        final String cql = NSB.insert(Account.class).into("account").build().query();
        assertTrue(cql.startsWith("INSERT INTO account"), cql);
        assertTrue(cql.contains(":firstName"), cql);
    }

    @Test
    public void testNSB_insertClassWithExcluded() {
        final String cql = NSB.insert(Account.class, N.asSet("firstName")).into("account").build().query();
        assertTrue(!cql.contains(":firstName"), cql);
        assertTrue(cql.contains(":lastName"), cql);
    }

    @Test
    public void testNSB_insertIntoClass() {
        final String cql = NSB.insertInto(Users.class).build().query();
        assertTrue(cql.startsWith("INSERT INTO simplex.users"), cql);
    }

    @Test
    public void testNSB_insertIntoClassWithExcluded() {
        final String cql = NSB.insertInto(Users.class, N.asSet("createdTime")).build().query();
        assertTrue(cql.startsWith("INSERT INTO simplex.users"), cql);
        assertTrue(!cql.contains(":createdTime"), cql);
    }

    @Test
    public void testNSB_batchInsert() {
        final String cql = NSB.batchInsert(N.asList(N.asMap("firstName", "a"), N.asMap("firstName", "b"))).into("account").build().query();
        assertTrue(cql.startsWith("INSERT INTO account"), cql);
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testNSB_updateString() {
        final String cql = NSB.update("account").set("firstName").where(Filters.eq("id", 1)).build().query();
        assertEquals("UPDATE account SET firstName = :firstName WHERE id = :id", cql);
    }

    @Test
    public void testNSB_updateStringClass() {
        final String cql = NSB.update("account", Account.class).set("firstName").where(Filters.eq("id", 1)).build().query();
        assertTrue(cql.startsWith("UPDATE account SET firstName"), cql);
    }

    @Test
    public void testNSB_updateClass() {
        final String cql = NSB.update(Account.class).set("firstName").where(Filters.eq("id", 1)).build().query();
        assertTrue(cql.contains("UPDATE"), cql);
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testNSB_updateClassWithExcluded() {
        final String cql = NSB.update(Account.class, N.asSet("id", "createTime")).where(Filters.eq("id", 1)).build().query();
        assertTrue(cql.contains("UPDATE"), cql);
        assertTrue(!cql.contains("SET id"), cql);
    }

    @Test
    public void testNSB_deleteString() {
        final String cql = NSB.delete("firstName").from("account").where(Filters.eq("id", 1)).build().query();
        assertEquals("DELETE firstName FROM account WHERE id = :id", cql);
    }

    @Test
    public void testNSB_deleteStringArray() {
        final String cql = NSB.delete("firstName", "lastName").from("account").where(Filters.eq("id", 1)).build().query();
        assertEquals("DELETE firstName, lastName FROM account WHERE id = :id", cql);
    }

    @Test
    public void testNSB_deleteCollection() {
        final String cql = NSB.delete(N.asList("firstName", "lastName")).from("account").where(Filters.eq("id", 1)).build().query();
        assertEquals("DELETE firstName, lastName FROM account WHERE id = :id", cql);
    }

    @Test
    public void testNSB_deleteClass() {
        final String cql = NSB.delete(Account.class).from("account").where(Filters.eq("id", 1)).build().query();
        assertTrue(cql.startsWith("DELETE "), cql);
        assertTrue(cql.contains("FROM account"), cql);
    }

    @Test
    public void testNSB_deleteClassWithExcluded() {
        final String cql = NSB.delete(Account.class, N.asSet("firstName")).from("account").where(Filters.eq("id", 1)).build().query();
        assertTrue(cql.startsWith("DELETE "), cql);
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testNSB_deleteFromString() {
        final String cql = NSB.deleteFrom("account").where(Filters.eq("id", 1)).build().query();
        assertEquals("DELETE FROM account WHERE id = :id", cql);
    }

    @Test
    public void testNSB_deleteFromStringClass() {
        final String cql = NSB.deleteFrom("account", Account.class).where(Filters.eq("id", 1)).build().query();
        assertTrue(cql.startsWith("DELETE FROM account"), cql);
    }

    @Test
    public void testNSB_deleteFromClass() {
        final String cql = NSB.deleteFrom(Users.class).where(Filters.eq("id", Filters.QME)).build().query();
        assertTrue(cql.startsWith("DELETE FROM simplex.users"), cql);
    }

    @Test
    public void testNSB_selectString() {
        final String cql = NSB.select("firstName").from("account").build().query();
        assertEquals("SELECT firstName FROM account", cql);
    }

    @Test
    public void testNSB_selectStringArray() {
        final String cql = NSB.select("firstName", "lastName").from("account").build().query();
        assertEquals("SELECT firstName, lastName FROM account", cql);
    }

    @Test
    public void testNSB_selectCollection() {
        final String cql = NSB.select(N.asList("firstName", "lastName")).from("account").build().query();
        assertEquals("SELECT firstName, lastName FROM account", cql);
    }

    @Test
    public void testNSB_selectMap() {
        final String cql = NSB.select(N.asMap("firstName", "fn")).from("account").build().query();
        assertTrue(cql.startsWith("SELECT firstName"), cql);
        assertTrue(cql.contains("\"fn\""), cql);
    }

    @Test
    public void testNSB_selectClass() {
        final String cql = NSB.select(Account.class).from("account").build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testNSB_selectClassWithSubEntities() {
        final String cql = NSB.select(Account.class, false).from("account").build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testNSB_selectClassWithExcluded() {
        final String cql = NSB.select(Account.class, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testNSB_selectClassFull() {
        final String cql = NSB.select(Account.class, false, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testNSB_selectFromClass() {
        final String cql = NSB.selectFrom(Users.class).build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testNSB_selectFromClassWithAlias() {
        final String cql = NSB.selectFrom(Users.class, "u").build().query();
        assertTrue(cql.contains("FROM simplex.users u"), cql);
    }

    @Test
    public void testNSB_selectFromClassWithSubEntities() {
        final String cql = NSB.selectFrom(Account.class, false).build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testNSB_selectFromClassWithExcluded() {
        final String cql = NSB.selectFrom(Account.class, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testNSB_selectFromClassAliasSubEntities() {
        final String cql = NSB.selectFrom(Users.class, "u", false).build().query();
        assertTrue(cql.contains("FROM simplex.users u"), cql);
    }

    @Test
    public void testNSB_selectFromClassAliasExcluded() {
        final String cql = NSB.selectFrom(Users.class, "u", N.asSet("createdTime")).build().query();
        assertTrue(cql.contains("FROM simplex.users u"), cql);
        assertTrue(!cql.contains("createdTime"), cql);
    }

    @Test
    public void testNSB_selectFromClassSubEntitiesExcluded() {
        final String cql = NSB.selectFrom(Account.class, false, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testNSB_selectFromClassFull() {
        final String cql = NSB.selectFrom(Users.class, "u", false, N.asSet("createdTime")).build().query();
        assertTrue(cql.contains("FROM simplex.users u"), cql);
        assertTrue(!cql.contains("createdTime"), cql);
    }

    @Test
    public void testNSB_countString() {
        final String cql = NSB.count("account").where(Filters.eq("id", 1)).build().query();
        assertEquals("SELECT count(*) FROM account WHERE id = :id", cql);
    }

    @Test
    public void testNSB_countClass() {
        final String cql = NSB.count(Users.class).where(Filters.eq("id", Filters.QME)).build().query();
        assertTrue(cql.startsWith("SELECT count(*) FROM simplex.users"), cql);
    }

    @Test
    public void testNSB_parse() {
        final String cql = NSB.renderCondition(Filters.eq("firstName", "John"), Account.class).build().query();
        assertEquals("firstName = :firstName", cql.trim());
    }

    @Test
    public void testNSB_isNamedSql() {
        // Indirect: a NAMED_SQL builder must emit ':' placeholders, not '?'.
        final String cql = NSB.select("a").from("t").where(Filters.eq("a", 1)).build().query();
        assertTrue(cql.contains(":a"), cql);
        assertTrue(!cql.contains("?"), cql);
    }

    @Test
    public void testNSB_insert_EmptyString() {
        assertThrows(IllegalArgumentException.class, () -> NSB.insert(""));
    }

    // ---------------------------------------------------------------------------------------------
    // PSB (NO_CHANGE naming + PARAMETERIZED_SQL): mirrors NSB API but emits '?' placeholders.
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testPSB_insertString() {
        final String cql = PSB.insert("firstName").into("account").build().query();
        assertEquals("INSERT INTO account (firstName) VALUES (?)", cql);
    }

    @Test
    public void testPSB_insertStringArray() {
        final String cql = PSB.insert("firstName", "lastName").into("account").build().query();
        assertEquals("INSERT INTO account (firstName, lastName) VALUES (?, ?)", cql);
    }

    @Test
    public void testPSB_insertCollection() {
        final String cql = PSB.insert(N.asList("firstName", "lastName")).into("account").build().query();
        assertEquals("INSERT INTO account (firstName, lastName) VALUES (?, ?)", cql);
    }

    @Test
    public void testPSB_insertMap() {
        final Map<String, Object> props = N.asMap("firstName", Filters.QME);
        final String cql = PSB.insert(props).into("account").build().query();
        assertEquals("INSERT INTO account (firstName) VALUES (?)", cql);
    }

    @Test
    public void testPSB_insertEntity() {
        final Account account = new Account();
        account.setFirstName("John");
        final String cql = PSB.insert(account).into("account").build().query();
        assertTrue(cql.startsWith("INSERT INTO account"), cql);
        assertTrue(cql.contains("?"), cql);
    }

    @Test
    public void testPSB_insertEntityWithExcluded() {
        final Account account = new Account();
        account.setFirstName("John");
        account.setLastName("Doe");
        final String cql = PSB.insert(account, N.asSet("lastName")).into("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
        assertTrue(!cql.contains("lastName"), cql);
    }

    @Test
    public void testPSB_insertClass() {
        final String cql = PSB.insert(Account.class).into("account").build().query();
        assertTrue(cql.startsWith("INSERT INTO account"), cql);
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testPSB_insertClassWithExcluded() {
        final String cql = PSB.insert(Account.class, N.asSet("firstName")).into("account").build().query();
        assertTrue(!cql.contains("firstName"), cql);
    }

    @Test
    public void testPSB_insertIntoClass() {
        final String cql = PSB.insertInto(Users.class).build().query();
        assertTrue(cql.startsWith("INSERT INTO simplex.users"), cql);
    }

    @Test
    public void testPSB_insertIntoClassWithExcluded() {
        final String cql = PSB.insertInto(Users.class, N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("createdTime"), cql);
    }

    @Test
    public void testPSB_batchInsert() {
        final String cql = PSB.batchInsert(N.asList(N.asMap("firstName", "a"), N.asMap("firstName", "b"))).into("account").build().query();
        assertTrue(cql.startsWith("INSERT INTO account"), cql);
    }

    @Test
    public void testPSB_updateString() {
        final String cql = PSB.update("account").set("firstName").where(Filters.eq("id", 1)).build().query();
        assertEquals("UPDATE account SET firstName = ? WHERE id = ?", cql);
    }

    @Test
    public void testPSB_updateStringClass() {
        final String cql = PSB.update("account", Account.class).set("firstName").where(Filters.eq("id", 1)).build().query();
        assertTrue(cql.startsWith("UPDATE account"), cql);
    }

    @Test
    public void testPSB_updateClass() {
        final String cql = PSB.update(Account.class).set("firstName").where(Filters.eq("id", 1)).build().query();
        assertTrue(cql.contains("UPDATE"), cql);
    }

    @Test
    public void testPSB_updateClassWithExcluded() {
        final String cql = PSB.update(Account.class, N.asSet("id", "createTime")).where(Filters.eq("id", 1)).build().query();
        assertTrue(cql.contains("UPDATE"), cql);
    }

    @Test
    public void testPSB_deleteString() {
        final String cql = PSB.delete("firstName").from("account").where(Filters.eq("id", 1)).build().query();
        assertEquals("DELETE firstName FROM account WHERE id = ?", cql);
    }

    @Test
    public void testPSB_deleteStringArray() {
        final String cql = PSB.delete("firstName", "lastName").from("account").build().query();
        assertEquals("DELETE firstName, lastName FROM account", cql);
    }

    @Test
    public void testPSB_deleteCollection() {
        final String cql = PSB.delete(N.asList("firstName", "lastName")).from("account").build().query();
        assertEquals("DELETE firstName, lastName FROM account", cql);
    }

    @Test
    public void testPSB_deleteClass() {
        final String cql = PSB.delete(Account.class).from("account").where(Filters.eq("id", 1)).build().query();
        assertTrue(cql.startsWith("DELETE "), cql);
    }

    @Test
    public void testPSB_deleteClassWithExcluded() {
        final String cql = PSB.delete(Account.class, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testPSB_deleteFromString() {
        final String cql = PSB.deleteFrom("account").where(Filters.eq("id", 1)).build().query();
        assertEquals("DELETE FROM account WHERE id = ?", cql);
    }

    @Test
    public void testPSB_deleteFromStringClass() {
        final String cql = PSB.deleteFrom("account", Account.class).where(Filters.eq("id", 1)).build().query();
        assertTrue(cql.startsWith("DELETE FROM account"), cql);
    }

    @Test
    public void testPSB_deleteFromClass() {
        final String cql = PSB.deleteFrom(Users.class).build().query();
        assertEquals("DELETE FROM simplex.users", cql);
    }

    @Test
    public void testPSB_selectString() {
        final String cql = PSB.select("firstName").from("account").build().query();
        assertEquals("SELECT firstName FROM account", cql);
    }

    @Test
    public void testPSB_selectStringArray() {
        final String cql = PSB.select("firstName", "lastName").from("account").build().query();
        assertEquals("SELECT firstName, lastName FROM account", cql);
    }

    @Test
    public void testPSB_selectCollection() {
        final String cql = PSB.select(N.asList("firstName", "lastName")).from("account").build().query();
        assertEquals("SELECT firstName, lastName FROM account", cql);
    }

    @Test
    public void testPSB_selectMap() {
        final String cql = PSB.select(N.asMap("firstName", "fn")).from("account").build().query();
        assertTrue(cql.contains("\"fn\""), cql);
    }

    @Test
    public void testPSB_selectClass() {
        final String cql = PSB.select(Account.class).from("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testPSB_selectClassWithSubEntities() {
        final String cql = PSB.select(Account.class, false).from("account").build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testPSB_selectClassWithExcluded() {
        final String cql = PSB.select(Account.class, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testPSB_selectClassFull() {
        final String cql = PSB.select(Account.class, false, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testPSB_selectFromClass() {
        final String cql = PSB.selectFrom(Users.class).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testPSB_selectFromClassWithAlias() {
        final String cql = PSB.selectFrom(Users.class, "u").build().query();
        assertTrue(cql.contains("FROM simplex.users u"), cql);
    }

    @Test
    public void testPSB_selectFromClassWithSubEntities() {
        final String cql = PSB.selectFrom(Account.class, false).build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testPSB_selectFromClassWithExcluded() {
        final String cql = PSB.selectFrom(Account.class, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testPSB_selectFromClassAliasSubEntities() {
        final String cql = PSB.selectFrom(Users.class, "u", false).build().query();
        assertTrue(cql.contains("FROM simplex.users u"), cql);
    }

    @Test
    public void testPSB_selectFromClassAliasExcluded() {
        final String cql = PSB.selectFrom(Users.class, "u", N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("createdTime"), cql);
    }

    @Test
    public void testPSB_selectFromClassSubEntitiesExcluded() {
        final String cql = PSB.selectFrom(Account.class, false, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testPSB_selectFromClassFull() {
        final String cql = PSB.selectFrom(Users.class, "u", false, N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("createdTime"), cql);
    }

    @Test
    public void testPSB_countString() {
        final String cql = PSB.count("account").where(Filters.eq("id", 1)).build().query();
        assertEquals("SELECT count(*) FROM account WHERE id = ?", cql);
    }

    @Test
    public void testPSB_countClass() {
        final String cql = PSB.count(Users.class).build().query();
        assertEquals("SELECT count(*) FROM simplex.users", cql);
    }

    @Test
    public void testPSB_parse() {
        final String cql = PSB.renderCondition(Filters.eq("firstName", "John"), Account.class).build().query();
        assertTrue(cql.contains("firstName"), cql);
        assertTrue(cql.contains("?"), cql);
    }

    // ---------------------------------------------------------------------------------------------
    // NAC (SCREAMING_SNAKE_CASE + NAMED_SQL): identifiers become UPPER_SNAKE_CASE.
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testNAC_insertStringArray() {
        final String cql = NAC.insert("firstName", "lastName").into("account").build().query();
        assertEquals("INSERT INTO account (FIRST_NAME, LAST_NAME) VALUES (:firstName, :lastName)", cql);
    }

    @Test
    public void testNAC_insertCollection() {
        final String cql = NAC.insert(N.asList("firstName")).into("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
        assertTrue(cql.contains(":firstName"), cql);
    }

    @Test
    public void testNAC_insertMap() {
        final String cql = NAC.insert(N.asMap("firstName", Filters.QME)).into("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testNAC_insertString() {
        final String cql = NAC.insert("firstName").into("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testNAC_insertEntity() {
        final Account account = new Account();
        account.setFirstName("John");
        final String cql = NAC.insert(account).into("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testNAC_insertEntityWithExcluded() {
        final Account account = new Account();
        account.setFirstName("John");
        account.setLastName("Doe");
        final String cql = NAC.insert(account, N.asSet("lastName")).into("account").build().query();
        assertTrue(!cql.contains("LAST_NAME"), cql);
    }

    @Test
    public void testNAC_insertClass() {
        final String cql = NAC.insert(Account.class).into("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testNAC_insertClassWithExcluded() {
        final String cql = NAC.insert(Account.class, N.asSet("firstName")).into("account").build().query();
        assertTrue(!cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testNAC_insertIntoClass() {
        final String cql = NAC.insertInto(Users.class).build().query();
        assertTrue(cql.startsWith("INSERT INTO simplex.users"), cql);
    }

    @Test
    public void testNAC_insertIntoClassWithExcluded() {
        final String cql = NAC.insertInto(Users.class, N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("CREATED_TIME"), cql);
    }

    @Test
    public void testNAC_batchInsert() {
        final String cql = NAC.batchInsert(N.asList(N.asMap("firstName", "a"))).into("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testNAC_updateString() {
        final String cql = NAC.update("account").set("firstName").where(Filters.eq("id", 1)).build().query();
        assertEquals("UPDATE account SET FIRST_NAME = :firstName WHERE ID = :id", cql);
    }

    @Test
    public void testNAC_updateStringClass() {
        final String cql = NAC.update("account", Account.class).set("firstName").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testNAC_updateClass() {
        final String cql = NAC.update(Account.class).set("firstName").where(Filters.eq("id", 1)).build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testNAC_updateClassWithExcluded() {
        final String cql = NAC.update(Account.class, N.asSet("id")).where(Filters.eq("id", 1)).build().query();
        assertTrue(cql.contains("UPDATE"), cql);
    }

    @Test
    public void testNAC_deleteString() {
        final String cql = NAC.delete("firstName").from("account").build().query();
        assertEquals("DELETE FIRST_NAME FROM account", cql);
    }

    @Test
    public void testNAC_deleteStringArray() {
        final String cql = NAC.delete("firstName", "lastName").from("account").build().query();
        assertEquals("DELETE FIRST_NAME, LAST_NAME FROM account", cql);
    }

    @Test
    public void testNAC_deleteCollection() {
        final String cql = NAC.delete(N.asList("firstName")).from("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testNAC_deleteClass() {
        final String cql = NAC.delete(Account.class).from("account").build().query();
        assertTrue(cql.startsWith("DELETE "), cql);
    }

    @Test
    public void testNAC_deleteClassWithExcluded() {
        final String cql = NAC.delete(Account.class, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testNAC_deleteFromString() {
        final String cql = NAC.deleteFrom("account").where(Filters.eq("id", 1)).build().query();
        assertEquals("DELETE FROM account WHERE ID = :id", cql);
    }

    @Test
    public void testNAC_deleteFromStringClass() {
        final String cql = NAC.deleteFrom("account", Account.class).where(Filters.eq("id", 1)).build().query();
        assertTrue(cql.startsWith("DELETE FROM account"), cql);
    }

    @Test
    public void testNAC_deleteFromClass() {
        final String cql = NAC.deleteFrom(Users.class).build().query();
        assertEquals("DELETE FROM simplex.users", cql);
    }

    @Test
    public void testNAC_selectString() {
        final String cql = NAC.select("firstName").from("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testNAC_selectStringArray() {
        final String cql = NAC.select("firstName", "lastName").from("account").build().query();
        assertTrue(cql.contains("FIRST_NAME") && cql.contains("LAST_NAME"), cql);
    }

    @Test
    public void testNAC_selectCollection() {
        final String cql = NAC.select(N.asList("firstName")).from("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testNAC_selectMap() {
        final String cql = NAC.select(N.asMap("firstName", "fn")).from("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
        assertTrue(cql.contains("\"fn\""), cql);
    }

    @Test
    public void testNAC_selectClass() {
        final String cql = NAC.select(Account.class).from("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testNAC_selectClassWithSubEntities() {
        final String cql = NAC.select(Account.class, false).from("account").build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testNAC_selectClassWithExcluded() {
        final String cql = NAC.select(Account.class, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testNAC_selectClassFull() {
        final String cql = NAC.select(Account.class, false, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testNAC_selectFromClass() {
        final String cql = NAC.selectFrom(Users.class).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testNAC_selectFromClassWithAlias() {
        final String cql = NAC.selectFrom(Users.class, "u").build().query();
        assertTrue(cql.contains("FROM simplex.users u"), cql);
    }

    @Test
    public void testNAC_selectFromClassWithSubEntities() {
        final String cql = NAC.selectFrom(Account.class, false).build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testNAC_selectFromClassWithExcluded() {
        final String cql = NAC.selectFrom(Account.class, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testNAC_selectFromClassAliasSubEntities() {
        final String cql = NAC.selectFrom(Users.class, "u", false).build().query();
        assertTrue(cql.contains("FROM simplex.users u"), cql);
    }

    @Test
    public void testNAC_selectFromClassAliasExcluded() {
        final String cql = NAC.selectFrom(Users.class, "u", N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("CREATED_TIME"), cql);
    }

    @Test
    public void testNAC_selectFromClassSubEntitiesExcluded() {
        final String cql = NAC.selectFrom(Account.class, false, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testNAC_selectFromClassFull() {
        final String cql = NAC.selectFrom(Users.class, "u", false, N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("CREATED_TIME"), cql);
    }

    @Test
    public void testNAC_countString() {
        final String cql = NAC.count("account").build().query();
        assertEquals("SELECT count(*) FROM account", cql);
    }

    @Test
    public void testNAC_countClass() {
        final String cql = NAC.count(Users.class).build().query();
        assertEquals("SELECT count(*) FROM simplex.users", cql);
    }

    @Test
    public void testNAC_parse() {
        final String cql = NAC.renderCondition(Filters.eq("firstName", "John"), Account.class).build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
        assertTrue(cql.contains(":firstName"), cql);
    }

    // ---------------------------------------------------------------------------------------------
    // PAC (SCREAMING_SNAKE_CASE + PARAMETERIZED_SQL): UPPER_SNAKE columns, '?' placeholders.
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testPAC_insertString() {
        final String cql = PAC.insert("firstName").into("account").build().query();
        assertEquals("INSERT INTO account (FIRST_NAME) VALUES (?)", cql);
    }

    @Test
    public void testPAC_insertStringArray() {
        final String cql = PAC.insert("firstName", "lastName").into("account").build().query();
        assertEquals("INSERT INTO account (FIRST_NAME, LAST_NAME) VALUES (?, ?)", cql);
    }

    @Test
    public void testPAC_insertCollection() {
        final String cql = PAC.insert(N.asList("firstName")).into("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testPAC_insertMap() {
        final String cql = PAC.insert(N.asMap("firstName", Filters.QME)).into("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testPAC_insertEntity() {
        final Account account = new Account();
        account.setFirstName("John");
        final String cql = PAC.insert(account).into("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testPAC_insertEntityWithExcluded() {
        final Account account = new Account();
        account.setFirstName("J");
        account.setLastName("D");
        final String cql = PAC.insert(account, N.asSet("lastName")).into("account").build().query();
        assertTrue(!cql.contains("LAST_NAME"), cql);
    }

    @Test
    public void testPAC_insertClass() {
        final String cql = PAC.insert(Account.class).into("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testPAC_insertClassWithExcluded() {
        final String cql = PAC.insert(Account.class, N.asSet("firstName")).into("account").build().query();
        assertTrue(!cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testPAC_insertIntoClass() {
        final String cql = PAC.insertInto(Users.class).build().query();
        assertTrue(cql.startsWith("INSERT INTO simplex.users"), cql);
    }

    @Test
    public void testPAC_insertIntoClassWithExcluded() {
        final String cql = PAC.insertInto(Users.class, N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("CREATED_TIME"), cql);
    }

    @Test
    public void testPAC_batchInsert() {
        final String cql = PAC.batchInsert(N.asList(N.asMap("firstName", "a"))).into("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testPAC_updateString() {
        final String cql = PAC.update("account").set("firstName").where(Filters.eq("id", 1)).build().query();
        assertEquals("UPDATE account SET FIRST_NAME = ? WHERE ID = ?", cql);
    }

    @Test
    public void testPAC_updateStringClass() {
        final String cql = PAC.update("account", Account.class).set("firstName").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testPAC_updateClass() {
        final String cql = PAC.update(Account.class).set("firstName").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testPAC_updateClassWithExcluded() {
        final String cql = PAC.update(Account.class, N.asSet("id")).build().query();
        assertTrue(cql.contains("UPDATE"), cql);
    }

    @Test
    public void testPAC_deleteString() {
        final String cql = PAC.delete("firstName").from("account").build().query();
        assertEquals("DELETE FIRST_NAME FROM account", cql);
    }

    @Test
    public void testPAC_deleteStringArray() {
        final String cql = PAC.delete("firstName", "lastName").from("account").build().query();
        assertEquals("DELETE FIRST_NAME, LAST_NAME FROM account", cql);
    }

    @Test
    public void testPAC_deleteCollection() {
        final String cql = PAC.delete(N.asList("firstName")).from("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testPAC_deleteClass() {
        final String cql = PAC.delete(Account.class).from("account").build().query();
        assertTrue(cql.startsWith("DELETE "), cql);
    }

    @Test
    public void testPAC_deleteClassWithExcluded() {
        final String cql = PAC.delete(Account.class, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testPAC_deleteFromString() {
        final String cql = PAC.deleteFrom("account").where(Filters.eq("id", 1)).build().query();
        assertEquals("DELETE FROM account WHERE ID = ?", cql);
    }

    @Test
    public void testPAC_deleteFromStringClass() {
        final String cql = PAC.deleteFrom("account", Account.class).build().query();
        assertEquals("DELETE FROM account", cql);
    }

    @Test
    public void testPAC_deleteFromClass() {
        final String cql = PAC.deleteFrom(Users.class).build().query();
        assertEquals("DELETE FROM simplex.users", cql);
    }

    @Test
    public void testPAC_selectString() {
        final String cql = PAC.select("firstName").from("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testPAC_selectStringArray() {
        final String cql = PAC.select("firstName", "lastName").from("account").build().query();
        assertTrue(cql.contains("FIRST_NAME") && cql.contains("LAST_NAME"), cql);
    }

    @Test
    public void testPAC_selectCollection() {
        final String cql = PAC.select(N.asList("firstName")).from("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testPAC_selectMap() {
        final String cql = PAC.select(N.asMap("firstName", "fn")).from("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testPAC_selectClass() {
        final String cql = PAC.select(Account.class).from("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testPAC_selectClassWithSubEntities() {
        final String cql = PAC.select(Account.class, false).from("account").build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testPAC_selectClassWithExcluded() {
        final String cql = PAC.select(Account.class, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testPAC_selectClassFull() {
        final String cql = PAC.select(Account.class, false, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testPAC_selectFromClass() {
        final String cql = PAC.selectFrom(Users.class).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testPAC_selectFromClassWithAlias() {
        final String cql = PAC.selectFrom(Users.class, "u").build().query();
        assertTrue(cql.contains("FROM simplex.users u"), cql);
    }

    @Test
    public void testPAC_selectFromClassWithSubEntities() {
        final String cql = PAC.selectFrom(Account.class, false).build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testPAC_selectFromClassWithExcluded() {
        final String cql = PAC.selectFrom(Account.class, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testPAC_selectFromClassAliasSubEntities() {
        final String cql = PAC.selectFrom(Users.class, "u", false).build().query();
        assertTrue(cql.contains("FROM simplex.users u"), cql);
    }

    @Test
    public void testPAC_selectFromClassAliasExcluded() {
        final String cql = PAC.selectFrom(Users.class, "u", N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("CREATED_TIME"), cql);
    }

    @Test
    public void testPAC_selectFromClassSubEntitiesExcluded() {
        final String cql = PAC.selectFrom(Account.class, false, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testPAC_selectFromClassFull() {
        final String cql = PAC.selectFrom(Users.class, "u", false, N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("CREATED_TIME"), cql);
    }

    @Test
    public void testPAC_countString() {
        final String cql = PAC.count("account").build().query();
        assertEquals("SELECT count(*) FROM account", cql);
    }

    @Test
    public void testPAC_countClass() {
        final String cql = PAC.count(Users.class).build().query();
        assertEquals("SELECT count(*) FROM simplex.users", cql);
    }

    @Test
    public void testPAC_parse() {
        final String cql = PAC.renderCondition(Filters.eq("firstName", "John"), Account.class).build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
        assertTrue(cql.contains("?"), cql);
    }

    // ---------------------------------------------------------------------------------------------
    // PLC (CAMEL_CASE + PARAMETERIZED_SQL): camelCase columns + '?' placeholders.
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testPLC_insertString() {
        final String cql = PLC.insert("firstName").into("account").build().query();
        assertEquals("INSERT INTO account (firstName) VALUES (?)", cql);
    }

    @Test
    public void testPLC_insertStringArray() {
        final String cql = PLC.insert("firstName", "lastName").into("account").build().query();
        assertEquals("INSERT INTO account (firstName, lastName) VALUES (?, ?)", cql);
    }

    @Test
    public void testPLC_insertCollection() {
        final String cql = PLC.insert(N.asList("firstName")).into("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testPLC_insertMap() {
        final String cql = PLC.insert(N.asMap("firstName", Filters.QME)).into("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testPLC_insertEntity() {
        final Account a = new Account();
        a.setFirstName("J");
        final String cql = PLC.insert(a).into("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testPLC_insertEntityWithExcluded() {
        final Account a = new Account();
        a.setFirstName("J");
        a.setLastName("D");
        final String cql = PLC.insert(a, N.asSet("lastName")).into("account").build().query();
        assertTrue(!cql.contains("lastName"), cql);
    }

    @Test
    public void testPLC_insertClass() {
        final String cql = PLC.insert(Account.class).into("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testPLC_insertClassWithExcluded() {
        final String cql = PLC.insert(Account.class, N.asSet("firstName")).into("account").build().query();
        assertTrue(!cql.contains("firstName"), cql);
    }

    @Test
    public void testPLC_insertIntoClass() {
        final String cql = PLC.insertInto(Users.class).build().query();
        assertTrue(cql.startsWith("INSERT INTO simplex.users"), cql);
    }

    @Test
    public void testPLC_insertIntoClassWithExcluded() {
        final String cql = PLC.insertInto(Users.class, N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("createdTime"), cql);
    }

    @Test
    public void testPLC_batchInsert() {
        final String cql = PLC.batchInsert(N.asList(N.asMap("firstName", "a"))).into("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testPLC_updateString() {
        final String cql = PLC.update("account").set("firstName").where(Filters.eq("id", 1)).build().query();
        assertEquals("UPDATE account SET firstName = ? WHERE id = ?", cql);
    }

    @Test
    public void testPLC_updateStringClass() {
        final String cql = PLC.update("account", Account.class).set("firstName").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testPLC_updateClass() {
        final String cql = PLC.update(Account.class).set("firstName").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testPLC_updateClassWithExcluded() {
        final String cql = PLC.update(Account.class, N.asSet("id")).build().query();
        assertTrue(cql.contains("UPDATE"), cql);
    }

    @Test
    public void testPLC_deleteString() {
        final String cql = PLC.delete("firstName").from("account").build().query();
        assertEquals("DELETE firstName FROM account", cql);
    }

    @Test
    public void testPLC_deleteStringArray() {
        final String cql = PLC.delete("firstName", "lastName").from("account").build().query();
        assertEquals("DELETE firstName, lastName FROM account", cql);
    }

    @Test
    public void testPLC_deleteCollection() {
        final String cql = PLC.delete(N.asList("firstName")).from("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testPLC_deleteClass() {
        final String cql = PLC.delete(Account.class).from("account").build().query();
        assertTrue(cql.startsWith("DELETE "), cql);
    }

    @Test
    public void testPLC_deleteClassWithExcluded() {
        final String cql = PLC.delete(Account.class, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testPLC_deleteFromString() {
        final String cql = PLC.deleteFrom("account").where(Filters.eq("id", 1)).build().query();
        assertEquals("DELETE FROM account WHERE id = ?", cql);
    }

    @Test
    public void testPLC_deleteFromStringClass() {
        final String cql = PLC.deleteFrom("account", Account.class).build().query();
        assertEquals("DELETE FROM account", cql);
    }

    @Test
    public void testPLC_deleteFromClass() {
        final String cql = PLC.deleteFrom(Users.class).build().query();
        assertEquals("DELETE FROM simplex.users", cql);
    }

    @Test
    public void testPLC_selectString() {
        final String cql = PLC.select("firstName").from("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testPLC_selectStringArray() {
        final String cql = PLC.select("firstName", "lastName").from("account").build().query();
        assertTrue(cql.contains("firstName") && cql.contains("lastName"), cql);
    }

    @Test
    public void testPLC_selectCollection() {
        final String cql = PLC.select(N.asList("firstName")).from("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testPLC_selectMap() {
        final String cql = PLC.select(N.asMap("firstName", "fn")).from("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testPLC_selectClass() {
        final String cql = PLC.select(Account.class).from("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testPLC_selectClassWithSubEntities() {
        final String cql = PLC.select(Account.class, false).from("account").build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testPLC_selectClassWithExcluded() {
        final String cql = PLC.select(Account.class, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testPLC_selectClassFull() {
        final String cql = PLC.select(Account.class, false, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testPLC_selectFromClass() {
        final String cql = PLC.selectFrom(Users.class).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testPLC_selectFromClassWithAlias() {
        final String cql = PLC.selectFrom(Users.class, "u").build().query();
        assertTrue(cql.contains("FROM simplex.users u"), cql);
    }

    @Test
    public void testPLC_selectFromClassWithSubEntities() {
        final String cql = PLC.selectFrom(Account.class, false).build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testPLC_selectFromClassWithExcluded() {
        final String cql = PLC.selectFrom(Account.class, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testPLC_selectFromClassAliasSubEntities() {
        final String cql = PLC.selectFrom(Users.class, "u", false).build().query();
        assertTrue(cql.contains("FROM simplex.users u"), cql);
    }

    @Test
    public void testPLC_selectFromClassAliasExcluded() {
        final String cql = PLC.selectFrom(Users.class, "u", N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("createdTime"), cql);
    }

    @Test
    public void testPLC_selectFromClassSubEntitiesExcluded() {
        final String cql = PLC.selectFrom(Account.class, false, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testPLC_selectFromClassFull() {
        final String cql = PLC.selectFrom(Users.class, "u", false, N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("createdTime"), cql);
    }

    @Test
    public void testPLC_countString() {
        final String cql = PLC.count("account").build().query();
        assertEquals("SELECT count(*) FROM account", cql);
    }

    @Test
    public void testPLC_countClass() {
        final String cql = PLC.count(Users.class).build().query();
        assertEquals("SELECT count(*) FROM simplex.users", cql);
    }

    @Test
    public void testPLC_parse() {
        final String cql = PLC.renderCondition(Filters.eq("firstName", "John"), Account.class).build().query();
        assertTrue(cql.contains("firstName"), cql);
        assertTrue(cql.contains("?"), cql);
    }

    // ---------------------------------------------------------------------------------------------
    // ACCB (SCREAMING_SNAKE_CASE + RAW_SQL): UPPER_SNAKE columns, inlined literal values.
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testACCB_insertString() {
        final String cql = ACCB.insert("firstName").into("account").build().query();
        assertTrue(cql.startsWith("INSERT INTO account"), cql);
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testACCB_insertStringArray() {
        final String cql = ACCB.insert("firstName", "lastName").into("account").build().query();
        assertTrue(cql.contains("FIRST_NAME") && cql.contains("LAST_NAME"), cql);
    }

    @Test
    public void testACCB_insertCollection() {
        final String cql = ACCB.insert(N.asList("firstName")).into("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testACCB_insertMap() {
        final String cql = ACCB.insert(N.asMap("firstName", "John")).into("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
        assertTrue(cql.contains("'John'"), cql);
    }

    @Test
    public void testACCB_insertEntity() {
        final Account a = new Account();
        a.setFirstName("John");
        final String cql = ACCB.insert(a).into("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
        assertTrue(cql.contains("'John'"), cql);
    }

    @Test
    public void testACCB_insertEntityWithExcluded() {
        final Account a = new Account();
        a.setFirstName("J");
        a.setLastName("D");
        final String cql = ACCB.insert(a, N.asSet("lastName")).into("account").build().query();
        assertTrue(!cql.contains("LAST_NAME"), cql);
    }

    @Test
    public void testACCB_insertClass() {
        final String cql = ACCB.insert(Account.class).into("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testACCB_insertClassWithExcluded() {
        final String cql = ACCB.insert(Account.class, N.asSet("firstName")).into("account").build().query();
        assertTrue(!cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testACCB_insertIntoClass() {
        final String cql = ACCB.insertInto(Users.class).build().query();
        assertTrue(cql.startsWith("INSERT INTO simplex.users"), cql);
    }

    @Test
    public void testACCB_insertIntoClassWithExcluded() {
        final String cql = ACCB.insertInto(Users.class, N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("CREATED_TIME"), cql);
    }

    @Test
    public void testACCB_batchInsert() {
        final String cql = ACCB.batchInsert(N.asList(N.asMap("firstName", "a"))).into("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
        assertTrue(cql.contains("'a'"), cql);
    }

    @Test
    public void testACCB_updateString() {
        // RAW_SQL inlines values; identifiers in set() expressions are snake-cased per naming policy.
        final String cql = ACCB.update("account").set("firstName = 'X'").where(Filters.eq("id", 1)).build().query();
        assertEquals("UPDATE account SET FIRST_NAME = 'X' WHERE ID = 1", cql);
    }

    @Test
    public void testACCB_updateStringClass() {
        final String cql = ACCB.update("account", Account.class).set("firstName = 'X'").build().query();
        assertEquals("UPDATE account SET FIRST_NAME = 'X'", cql);
    }

    @Test
    public void testACCB_updateClass() {
        final String cql = ACCB.update(Account.class).set(N.asMap("firstName", "X")).where(Filters.eq("id", 1)).build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testACCB_updateClassWithExcluded() {
        final String cql = ACCB.update(Account.class, N.asSet("id")).set(N.asMap("firstName", "X")).build().query();
        assertTrue(cql.contains("UPDATE"), cql);
    }

    @Test
    public void testACCB_deleteString() {
        final String cql = ACCB.delete("firstName").from("account").build().query();
        assertEquals("DELETE FIRST_NAME FROM account", cql);
    }

    @Test
    public void testACCB_deleteStringArray() {
        final String cql = ACCB.delete("firstName", "lastName").from("account").build().query();
        assertEquals("DELETE FIRST_NAME, LAST_NAME FROM account", cql);
    }

    @Test
    public void testACCB_deleteCollection() {
        final String cql = ACCB.delete(N.asList("firstName")).from("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testACCB_deleteClass() {
        final String cql = ACCB.delete(Account.class).from("account").build().query();
        assertTrue(cql.startsWith("DELETE "), cql);
    }

    @Test
    public void testACCB_deleteClassWithExcluded() {
        final String cql = ACCB.delete(Account.class, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testACCB_deleteFromString() {
        final String cql = ACCB.deleteFrom("account").where(Filters.eq("id", 1)).build().query();
        assertEquals("DELETE FROM account WHERE ID = 1", cql);
    }

    @Test
    public void testACCB_deleteFromStringClass() {
        final String cql = ACCB.deleteFrom("account", Account.class).build().query();
        assertEquals("DELETE FROM account", cql);
    }

    @Test
    public void testACCB_deleteFromClass() {
        final String cql = ACCB.deleteFrom(Users.class).build().query();
        assertEquals("DELETE FROM simplex.users", cql);
    }

    @Test
    public void testACCB_selectString() {
        final String cql = ACCB.select("firstName").from("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testACCB_selectStringArray() {
        final String cql = ACCB.select("firstName", "lastName").from("account").build().query();
        assertTrue(cql.contains("FIRST_NAME") && cql.contains("LAST_NAME"), cql);
    }

    @Test
    public void testACCB_selectCollection() {
        final String cql = ACCB.select(N.asList("firstName")).from("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testACCB_selectMap() {
        final String cql = ACCB.select(N.asMap("firstName", "fn")).from("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
        assertTrue(cql.contains("\"fn\""), cql);
    }

    @Test
    public void testACCB_selectClass() {
        final String cql = ACCB.select(Account.class).from("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testACCB_selectClassWithSubEntities() {
        final String cql = ACCB.select(Account.class, false).from("account").build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testACCB_selectClassWithExcluded() {
        final String cql = ACCB.select(Account.class, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testACCB_selectClassFull() {
        final String cql = ACCB.select(Account.class, false, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testACCB_selectFromClass() {
        final String cql = ACCB.selectFrom(Users.class).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testACCB_selectFromClassWithAlias() {
        final String cql = ACCB.selectFrom(Users.class, "u").build().query();
        assertTrue(cql.contains("FROM simplex.users u"), cql);
    }

    @Test
    public void testACCB_selectFromClassWithSubEntities() {
        final String cql = ACCB.selectFrom(Account.class, false).build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testACCB_selectFromClassWithExcluded() {
        final String cql = ACCB.selectFrom(Account.class, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testACCB_selectFromClassAliasSubEntities() {
        final String cql = ACCB.selectFrom(Users.class, "u", false).build().query();
        assertTrue(cql.contains("FROM simplex.users u"), cql);
    }

    @Test
    public void testACCB_selectFromClassAliasExcluded() {
        final String cql = ACCB.selectFrom(Users.class, "u", N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("CREATED_TIME"), cql);
    }

    @Test
    public void testACCB_selectFromClassSubEntitiesExcluded() {
        final String cql = ACCB.selectFrom(Account.class, false, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testACCB_selectFromClassFull() {
        final String cql = ACCB.selectFrom(Users.class, "u", false, N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("CREATED_TIME"), cql);
    }

    @Test
    public void testACCB_countString() {
        final String cql = ACCB.count("account").build().query();
        assertEquals("SELECT count(*) FROM account", cql);
    }

    @Test
    public void testACCB_countClass() {
        final String cql = ACCB.count(Users.class).build().query();
        assertEquals("SELECT count(*) FROM simplex.users", cql);
    }

    @Test
    public void testACCB_parse() {
        final String cql = ACCB.renderCondition(Filters.eq("firstName", "John"), Account.class).build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
        // RAW_SQL inlines the value.
        assertTrue(cql.contains("'John'"), cql);
    }

    // ---------------------------------------------------------------------------------------------
    // NLC (CAMEL_CASE + NAMED_SQL): camelCase columns + ':name' placeholders.
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testNLC_insertString() {
        final String cql = NLC.insert("firstName").into("account").build().query();
        assertEquals("INSERT INTO account (firstName) VALUES (:firstName)", cql);
    }

    @Test
    public void testNLC_insertStringArray() {
        final String cql = NLC.insert("firstName", "lastName").into("account").build().query();
        assertEquals("INSERT INTO account (firstName, lastName) VALUES (:firstName, :lastName)", cql);
    }

    @Test
    public void testNLC_insertCollection() {
        final String cql = NLC.insert(N.asList("firstName")).into("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
        assertTrue(cql.contains(":firstName"), cql);
    }

    @Test
    public void testNLC_insertMap() {
        final String cql = NLC.insert(N.asMap("firstName", Filters.QME)).into("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testNLC_insertEntity() {
        final Account a = new Account();
        a.setFirstName("J");
        final String cql = NLC.insert(a).into("account").build().query();
        assertTrue(cql.contains(":firstName"), cql);
    }

    @Test
    public void testNLC_insertEntityWithExcluded() {
        final Account a = new Account();
        a.setFirstName("J");
        a.setLastName("D");
        final String cql = NLC.insert(a, N.asSet("lastName")).into("account").build().query();
        assertTrue(!cql.contains(":lastName"), cql);
    }

    @Test
    public void testNLC_insertClass() {
        final String cql = NLC.insert(Account.class).into("account").build().query();
        assertTrue(cql.contains(":firstName"), cql);
    }

    @Test
    public void testNLC_insertClassWithExcluded() {
        final String cql = NLC.insert(Account.class, N.asSet("firstName")).into("account").build().query();
        assertTrue(!cql.contains(":firstName"), cql);
    }

    @Test
    public void testNLC_insertIntoClass() {
        final String cql = NLC.insertInto(Users.class).build().query();
        assertTrue(cql.startsWith("INSERT INTO simplex.users"), cql);
    }

    @Test
    public void testNLC_insertIntoClassWithExcluded() {
        final String cql = NLC.insertInto(Users.class, N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains(":createdTime"), cql);
    }

    @Test
    public void testNLC_batchInsert() {
        final String cql = NLC.batchInsert(N.asList(N.asMap("firstName", "a"))).into("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testNLC_updateString() {
        final String cql = NLC.update("account").set("firstName").where(Filters.eq("id", 1)).build().query();
        assertEquals("UPDATE account SET firstName = :firstName WHERE id = :id", cql);
    }

    @Test
    public void testNLC_updateStringClass() {
        final String cql = NLC.update("account", Account.class).set("firstName").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testNLC_updateClass() {
        final String cql = NLC.update(Account.class).set("firstName").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testNLC_updateClassWithExcluded() {
        final String cql = NLC.update(Account.class, N.asSet("id")).build().query();
        assertTrue(cql.contains("UPDATE"), cql);
    }

    @Test
    public void testNLC_deleteString() {
        final String cql = NLC.delete("firstName").from("account").build().query();
        assertEquals("DELETE firstName FROM account", cql);
    }

    @Test
    public void testNLC_deleteStringArray() {
        final String cql = NLC.delete("firstName", "lastName").from("account").build().query();
        assertEquals("DELETE firstName, lastName FROM account", cql);
    }

    @Test
    public void testNLC_deleteCollection() {
        final String cql = NLC.delete(N.asList("firstName")).from("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testNLC_deleteClass() {
        final String cql = NLC.delete(Account.class).from("account").build().query();
        assertTrue(cql.startsWith("DELETE "), cql);
    }

    @Test
    public void testNLC_deleteClassWithExcluded() {
        final String cql = NLC.delete(Account.class, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testNLC_deleteFromString() {
        final String cql = NLC.deleteFrom("account").where(Filters.eq("id", 1)).build().query();
        assertEquals("DELETE FROM account WHERE id = :id", cql);
    }

    @Test
    public void testNLC_deleteFromStringClass() {
        final String cql = NLC.deleteFrom("account", Account.class).build().query();
        assertEquals("DELETE FROM account", cql);
    }

    @Test
    public void testNLC_deleteFromClass() {
        final String cql = NLC.deleteFrom(Users.class).build().query();
        assertEquals("DELETE FROM simplex.users", cql);
    }

    @Test
    public void testNLC_selectString() {
        final String cql = NLC.select("firstName").from("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testNLC_selectStringArray() {
        final String cql = NLC.select("firstName", "lastName").from("account").build().query();
        assertTrue(cql.contains("firstName") && cql.contains("lastName"), cql);
    }

    @Test
    public void testNLC_selectCollection() {
        final String cql = NLC.select(N.asList("firstName")).from("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testNLC_selectMap() {
        final String cql = NLC.select(N.asMap("firstName", "fn")).from("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testNLC_selectClass() {
        final String cql = NLC.select(Account.class).from("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testNLC_selectClassWithSubEntities() {
        final String cql = NLC.select(Account.class, false).from("account").build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testNLC_selectClassWithExcluded() {
        final String cql = NLC.select(Account.class, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testNLC_selectClassFull() {
        final String cql = NLC.select(Account.class, false, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testNLC_selectFromClass() {
        final String cql = NLC.selectFrom(Users.class).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testNLC_selectFromClassWithAlias() {
        final String cql = NLC.selectFrom(Users.class, "u").build().query();
        assertTrue(cql.contains("FROM simplex.users u"), cql);
    }

    @Test
    public void testNLC_selectFromClassWithSubEntities() {
        final String cql = NLC.selectFrom(Account.class, false).build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testNLC_selectFromClassWithExcluded() {
        final String cql = NLC.selectFrom(Account.class, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testNLC_selectFromClassAliasSubEntities() {
        final String cql = NLC.selectFrom(Users.class, "u", false).build().query();
        assertTrue(cql.contains("FROM simplex.users u"), cql);
    }

    @Test
    public void testNLC_selectFromClassAliasExcluded() {
        final String cql = NLC.selectFrom(Users.class, "u", N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("createdTime"), cql);
    }

    @Test
    public void testNLC_selectFromClassSubEntitiesExcluded() {
        final String cql = NLC.selectFrom(Account.class, false, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testNLC_selectFromClassFull() {
        final String cql = NLC.selectFrom(Users.class, "u", false, N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("createdTime"), cql);
    }

    @Test
    public void testNLC_countString() {
        final String cql = NLC.count("account").build().query();
        assertEquals("SELECT count(*) FROM account", cql);
    }

    @Test
    public void testNLC_countClass() {
        final String cql = NLC.count(Users.class).build().query();
        assertEquals("SELECT count(*) FROM simplex.users", cql);
    }

    @Test
    public void testNLC_parse() {
        final String cql = NLC.renderCondition(Filters.eq("firstName", "John"), Account.class).build().query();
        assertTrue(cql.contains("firstName"), cql);
        assertTrue(cql.contains(":firstName"), cql);
    }

    // ---------------------------------------------------------------------------------------------
    // Argument-validation smoke tests for each variant (exercise the precondition branches).
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testFactoryValidation_NullArgsThrow() {
        // Ensure each variant rejects null/empty inputs symmetrically.
        assertThrows(IllegalArgumentException.class, () -> NSB.insert((String) null));
        assertThrows(IllegalArgumentException.class, () -> PSB.insert((String) null));
        assertThrows(IllegalArgumentException.class, () -> NAC.insert((String) null));
        assertThrows(IllegalArgumentException.class, () -> PAC.insert((String) null));
        assertThrows(IllegalArgumentException.class, () -> PLC.insert((String) null));
        assertThrows(IllegalArgumentException.class, () -> NLC.insert((String) null));
        assertThrows(IllegalArgumentException.class, () -> ACCB.insert((String) null));

        assertThrows(IllegalArgumentException.class, () -> NSB.update((String) null));
        assertThrows(IllegalArgumentException.class, () -> PSB.update((String) null));

        assertThrows(IllegalArgumentException.class, () -> NSB.deleteFrom((String) null));
        assertThrows(IllegalArgumentException.class, () -> PSB.deleteFrom((String) null));

        assertThrows(IllegalArgumentException.class, () -> NSB.select((String) null));
        assertThrows(IllegalArgumentException.class, () -> PSB.select((String) null));

        assertThrows(IllegalArgumentException.class, () -> NSB.renderCondition(null, Account.class));
        assertThrows(IllegalArgumentException.class, () -> PSB.renderCondition(null, Account.class));

        assertThrows(IllegalArgumentException.class, () -> NSB.count((String) null));
        assertThrows(IllegalArgumentException.class, () -> NSB.count((Class<?>) null));
    }

    @Test
    public void testReturnNotNull() {
        // createInstance() is exercised via every public factory; verify the chain returns a usable
        // builder (the build() result is non-null and yields a non-null query string).
        assertNotNull(NSB.select("a").from("t").build());
        assertNotNull(NSB.select("a").from("t").build().query());
        assertNotNull(PSB.select("a").from("t").build().query());
        assertNotNull(NAC.select("a").from("t").build().query());
        assertNotNull(PAC.select("a").from("t").build().query());
        assertNotNull(PLC.select("a").from("t").build().query());
        assertNotNull(NLC.select("a").from("t").build().query());
        assertNotNull(ACCB.select("a").from("t").build().query());
    }

    // ---------------------------------------------------------------------------------------------
    // NSC (SNAKE_CASE naming + NAMED_SQL): exercise all factory methods.
    // Column names are converted to snake_case, named placeholders use original property names.
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testNSC_insertString() {
        final String cql = NSC.insert("firstName").into("account").build().query();
        assertTrue(cql.startsWith("INSERT INTO account"), cql);
        // SNAKE_CASE policy: column name is converted, named param keeps the original identifier.
        assertTrue(cql.contains("(first_name)"), cql);
        assertTrue(cql.contains(":firstName"), cql);
    }

    @Test
    public void testNSC_insertStringArray() {
        final String cql = NSC.insert("firstName", "lastName").into("account").build().query();
        assertEquals("INSERT INTO account (first_name, last_name) VALUES (:firstName, :lastName)", cql);
    }

    @Test
    public void testNSC_insertCollection() {
        final String cql = NSC.insert(N.asList("firstName", "lastName")).into("account").build().query();
        assertEquals("INSERT INTO account (first_name, last_name) VALUES (:firstName, :lastName)", cql);
    }

    @Test
    public void testNSC_insertMap() {
        final Map<String, Object> props = N.asMap("firstName", Filters.QME, "lastName", Filters.QME);
        final String cql = NSC.insert(props).into("account").build().query();
        assertTrue(cql.startsWith("INSERT INTO account"), cql);
        assertTrue(cql.contains("first_name"), cql);
        assertTrue(cql.contains("last_name"), cql);
    }

    @Test
    public void testNSC_insertEntity() {
        final Account account = new Account();
        account.setFirstName("John");
        account.setLastName("Doe");
        final String cql = NSC.insert(account).into("account").build().query();
        assertTrue(cql.startsWith("INSERT INTO account"), cql);
        assertTrue(cql.contains(":firstName") && cql.contains(":lastName"), cql);
        assertTrue(cql.contains("first_name") && cql.contains("last_name"), cql);
    }

    @Test
    public void testNSC_insertEntityWithExcluded() {
        final Account account = new Account();
        account.setFirstName("John");
        account.setLastName("Doe");
        final String cql = NSC.insert(account, N.asSet("lastName")).into("account").build().query();
        assertTrue(cql.contains(":firstName"), cql);
        assertTrue(!cql.contains(":lastName"), cql);
    }

    @Test
    public void testNSC_insertClass() {
        final String cql = NSC.insert(Account.class).into("account").build().query();
        assertTrue(cql.startsWith("INSERT INTO account"), cql);
        assertTrue(cql.contains(":firstName"), cql);
    }

    @Test
    public void testNSC_insertClassWithExcluded() {
        final String cql = NSC.insert(Account.class, N.asSet("firstName")).into("account").build().query();
        assertTrue(!cql.contains(":firstName"), cql);
        assertTrue(cql.contains(":lastName"), cql);
    }

    @Test
    public void testNSC_insertIntoClass() {
        final String cql = NSC.insertInto(Users.class).build().query();
        assertTrue(cql.startsWith("INSERT INTO simplex.users"), cql);
    }

    @Test
    public void testNSC_insertIntoClassWithExcluded() {
        final String cql = NSC.insertInto(Users.class, N.asSet("createdTime")).build().query();
        assertTrue(cql.startsWith("INSERT INTO simplex.users"), cql);
        assertTrue(!cql.contains(":createdTime"), cql);
    }

    @Test
    public void testNSC_batchInsert() {
        final String cql = NSC.batchInsert(N.asList(N.asMap("firstName", "a"), N.asMap("firstName", "b"))).into("account").build().query();
        assertTrue(cql.startsWith("INSERT INTO account"), cql);
        assertTrue(cql.contains("first_name"), cql);
    }

    @Test
    public void testNSC_updateString() {
        final String cql = NSC.update("account").set("firstName").where(Filters.eq("id", 1)).build().query();
        assertEquals("UPDATE account SET first_name = :firstName WHERE id = :id", cql);
    }

    @Test
    public void testNSC_updateStringClass() {
        final String cql = NSC.update("account", Account.class).set("firstName").where(Filters.eq("id", 1)).build().query();
        assertTrue(cql.startsWith("UPDATE account SET first_name"), cql);
    }

    @Test
    public void testNSC_updateClass() {
        final String cql = NSC.update(Account.class).set("firstName").where(Filters.eq("id", 1)).build().query();
        assertTrue(cql.contains("UPDATE"), cql);
        assertTrue(cql.contains("first_name"), cql);
    }

    @Test
    public void testNSC_updateClassWithExcluded() {
        final String cql = NSC.update(Account.class, N.asSet("id", "createTime")).where(Filters.eq("id", 1)).build().query();
        assertTrue(cql.contains("UPDATE"), cql);
        assertTrue(!cql.contains("SET id"), cql);
    }

    @Test
    public void testNSC_deleteString() {
        final String cql = NSC.delete("firstName").from("account").where(Filters.eq("id", 1)).build().query();
        assertEquals("DELETE first_name FROM account WHERE id = :id", cql);
    }

    @Test
    public void testNSC_deleteStringArray() {
        final String cql = NSC.delete("firstName", "lastName").from("account").where(Filters.eq("id", 1)).build().query();
        assertEquals("DELETE first_name, last_name FROM account WHERE id = :id", cql);
    }

    @Test
    public void testNSC_deleteCollection() {
        final String cql = NSC.delete(N.asList("firstName", "lastName")).from("account").where(Filters.eq("id", 1)).build().query();
        assertEquals("DELETE first_name, last_name FROM account WHERE id = :id", cql);
    }

    @Test
    public void testNSC_deleteClass() {
        final String cql = NSC.delete(Account.class).from("account").where(Filters.eq("id", 1)).build().query();
        assertTrue(cql.startsWith("DELETE "), cql);
        assertTrue(cql.contains("FROM account"), cql);
    }

    @Test
    public void testNSC_deleteClassWithExcluded() {
        final String cql = NSC.delete(Account.class, N.asSet("firstName")).from("account").where(Filters.eq("id", 1)).build().query();
        assertTrue(cql.startsWith("DELETE "), cql);
        assertTrue(!cql.contains(" first_name"), cql);
    }

    @Test
    public void testNSC_deleteFromString() {
        final String cql = NSC.deleteFrom("account").where(Filters.eq("id", 1)).build().query();
        assertEquals("DELETE FROM account WHERE id = :id", cql);
    }

    @Test
    public void testNSC_deleteFromStringClass() {
        final String cql = NSC.deleteFrom("account", Account.class).where(Filters.eq("id", 1)).build().query();
        assertTrue(cql.startsWith("DELETE FROM account"), cql);
    }

    @Test
    public void testNSC_deleteFromClass() {
        final String cql = NSC.deleteFrom(Users.class).where(Filters.eq("id", Filters.QME)).build().query();
        assertTrue(cql.startsWith("DELETE FROM simplex.users"), cql);
    }

    @Test
    public void testNSC_selectString() {
        // SNAKE_CASE select produces "snake AS \"camel\"" so result objects can be mapped back.
        final String cql = NSC.select("firstName").from("account").build().query();
        assertEquals("SELECT first_name AS \"firstName\" FROM account", cql);
    }

    @Test
    public void testNSC_selectStringArray() {
        final String cql = NSC.select("firstName", "lastName").from("account").build().query();
        assertEquals("SELECT first_name AS \"firstName\", last_name AS \"lastName\" FROM account", cql);
    }

    @Test
    public void testNSC_selectCollection() {
        final String cql = NSC.select(N.asList("firstName", "lastName")).from("account").build().query();
        assertEquals("SELECT first_name AS \"firstName\", last_name AS \"lastName\" FROM account", cql);
    }

    @Test
    public void testNSC_selectMap() {
        final String cql = NSC.select(N.asMap("firstName", "fn")).from("account").build().query();
        assertTrue(cql.startsWith("SELECT first_name"), cql);
        assertTrue(cql.contains("\"fn\""), cql);
    }

    @Test
    public void testNSC_selectClass() {
        final String cql = NSC.select(Account.class).from("account").build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
        assertTrue(cql.contains("first_name"), cql);
    }

    @Test
    public void testNSC_selectClassWithSubEntities() {
        final String cql = NSC.select(Account.class, false).from("account").build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testNSC_selectClassWithExcluded() {
        final String cql = NSC.select(Account.class, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains(" first_name"), cql);
    }

    @Test
    public void testNSC_selectClassFull() {
        final String cql = NSC.select(Account.class, false, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains(" first_name"), cql);
    }

    @Test
    public void testNSC_selectFromClass() {
        final String cql = NSC.selectFrom(Users.class).build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testNSC_selectFromClassWithAlias() {
        final String cql = NSC.selectFrom(Users.class, "u").build().query();
        assertTrue(cql.contains("FROM simplex.users u"), cql);
    }

    @Test
    public void testNSC_selectFromClassWithSubEntities() {
        final String cql = NSC.selectFrom(Account.class, false).build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testNSC_selectFromClassWithExcluded() {
        final String cql = NSC.selectFrom(Account.class, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains(" first_name"), cql);
    }

    @Test
    public void testNSC_selectFromClassAliasSubEntities() {
        final String cql = NSC.selectFrom(Users.class, "u", false).build().query();
        assertTrue(cql.contains("FROM simplex.users u"), cql);
    }

    @Test
    public void testNSC_selectFromClassAliasExcluded() {
        final String cql = NSC.selectFrom(Users.class, "u", N.asSet("createdTime")).build().query();
        assertTrue(cql.contains("FROM simplex.users u"), cql);
        assertTrue(!cql.contains("created_time"), cql);
    }

    @Test
    public void testNSC_selectFromClassSubEntitiesExcluded() {
        final String cql = NSC.selectFrom(Account.class, false, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains(" first_name"), cql);
    }

    @Test
    public void testNSC_selectFromClassFull() {
        final String cql = NSC.selectFrom(Users.class, "u", false, N.asSet("createdTime")).build().query();
        assertTrue(cql.contains("FROM simplex.users u"), cql);
        assertTrue(!cql.contains("created_time"), cql);
    }

    @Test
    public void testNSC_countString() {
        final String cql = NSC.count("account").where(Filters.eq("id", 1)).build().query();
        assertEquals("SELECT count(*) FROM account WHERE id = :id", cql);
    }

    @Test
    public void testNSC_countClass() {
        final String cql = NSC.count(Users.class).where(Filters.eq("id", Filters.QME)).build().query();
        assertTrue(cql.startsWith("SELECT count(*) FROM simplex.users"), cql);
    }

    @Test
    public void testNSC_parse() {
        final String cql = NSC.renderCondition(Filters.eq("firstName", "John"), Account.class).build().query();
        assertEquals("first_name = :firstName", cql.trim());
    }

    @Test
    public void testNSC_isNamedSql() {
        // Indirect: a NAMED_SQL builder must emit ':' placeholders, not '?'.
        final String cql = NSC.select("a").from("t").where(Filters.eq("a", 1)).build().query();
        assertTrue(cql.contains(":a"), cql);
        assertTrue(!cql.contains("?"), cql);
    }

    @Test
    public void testNSC_insert_EmptyString() {
        assertThrows(IllegalArgumentException.class, () -> NSC.insert(""));
    }

    @Test
    public void testNSC_insert_NullString() {
        assertThrows(IllegalArgumentException.class, () -> NSC.insert((String) null));
    }

    @Test
    public void testNSC_update_NullString() {
        assertThrows(IllegalArgumentException.class, () -> NSC.update((String) null));
    }

    @Test
    public void testNSC_deleteFrom_NullString() {
        assertThrows(IllegalArgumentException.class, () -> NSC.deleteFrom((String) null));
    }

    @Test
    public void testNSC_select_NullString() {
        assertThrows(IllegalArgumentException.class, () -> NSC.select((String) null));
    }

    @Test
    public void testNSC_parse_NullCondition() {
        assertThrows(IllegalArgumentException.class, () -> NSC.renderCondition(null, Account.class));
    }

    @Test
    public void testNSC_count_NullClass() {
        assertThrows(IllegalArgumentException.class, () -> NSC.count((Class<?>) null));
    }

    @Test
    public void testNSC_chainBuildable() {
        // Smoke test: createInstance() reached via every public factory, build returns usable query.
        assertNotNull(NSC.select("a").from("t").build());
        assertNotNull(NSC.select("a").from("t").build().query());
        assertNotNull(NSC.insert("a").into("t").build().query());
        assertNotNull(NSC.update("t").set("a").where(Filters.eq("id", 1)).build().query());
        assertNotNull(NSC.deleteFrom("t").where(Filters.eq("id", 1)).build().query());
        assertNotNull(NSC.count("t").build().query());
    }

    // ---------------------------------------------------------------------------------------------
    // PSC (SNAKE_CASE + PARAMETERIZED_SQL): snake_case columns + '?' placeholders.
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testPSC_insertString() {
        final String cql = PSC.insert("firstName").into("account").build().query();
        assertEquals("INSERT INTO account (first_name) VALUES (?)", cql);
    }

    @Test
    public void testPSC_insertStringArray() {
        final String cql = PSC.insert("firstName", "lastName").into("account").build().query();
        assertEquals("INSERT INTO account (first_name, last_name) VALUES (?, ?)", cql);
    }

    @Test
    public void testPSC_insertCollection() {
        final String cql = PSC.insert(N.asList("firstName", "lastName")).into("account").build().query();
        assertEquals("INSERT INTO account (first_name, last_name) VALUES (?, ?)", cql);
    }

    @Test
    public void testPSC_insertMap() {
        final Map<String, Object> props = N.asMap("firstName", Filters.QME);
        final String cql = PSC.insert(props).into("account").build().query();
        assertEquals("INSERT INTO account (first_name) VALUES (?)", cql);
    }

    @Test
    public void testPSC_insertEntity() {
        final Account a = new Account();
        a.setFirstName("John");
        final String cql = PSC.insert(a).into("account").build().query();
        assertTrue(cql.startsWith("INSERT INTO account"), cql);
        assertTrue(cql.contains("first_name"), cql);
        assertTrue(cql.contains("?"), cql);
    }

    @Test
    public void testPSC_insertEntityWithExcluded() {
        final Account a = new Account();
        a.setFirstName("John");
        a.setLastName("Doe");
        final String cql = PSC.insert(a, N.asSet("lastName")).into("account").build().query();
        assertTrue(cql.contains("first_name"), cql);
        assertTrue(!cql.contains("last_name"), cql);
    }

    @Test
    public void testPSC_insertClass() {
        final String cql = PSC.insert(Account.class).into("account").build().query();
        assertTrue(cql.startsWith("INSERT INTO account"), cql);
        assertTrue(cql.contains("first_name"), cql);
    }

    @Test
    public void testPSC_insertClassWithExcluded() {
        final String cql = PSC.insert(Account.class, N.asSet("firstName")).into("account").build().query();
        assertTrue(!cql.contains("first_name"), cql);
    }

    @Test
    public void testPSC_insertIntoClass() {
        final String cql = PSC.insertInto(Users.class).build().query();
        assertTrue(cql.startsWith("INSERT INTO simplex.users"), cql);
    }

    @Test
    public void testPSC_insertIntoClassWithExcluded() {
        final String cql = PSC.insertInto(Users.class, N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("created_time"), cql);
    }

    @Test
    public void testPSC_batchInsert() {
        final String cql = PSC.batchInsert(N.asList(N.asMap("firstName", "a"), N.asMap("firstName", "b"))).into("account").build().query();
        assertTrue(cql.startsWith("INSERT INTO account"), cql);
        assertTrue(cql.contains("first_name"), cql);
    }

    @Test
    public void testPSC_updateString() {
        final String cql = PSC.update("account").set("firstName").where(Filters.eq("id", 1)).build().query();
        assertEquals("UPDATE account SET first_name = ? WHERE id = ?", cql);
    }

    @Test
    public void testPSC_updateStringClass() {
        final String cql = PSC.update("account", Account.class).set("firstName").where(Filters.eq("id", 1)).build().query();
        assertTrue(cql.startsWith("UPDATE account SET first_name"), cql);
    }

    @Test
    public void testPSC_updateClass() {
        final String cql = PSC.update(Account.class).set("firstName").where(Filters.eq("id", 1)).build().query();
        assertTrue(cql.contains("UPDATE"), cql);
        assertTrue(cql.contains("first_name"), cql);
    }

    @Test
    public void testPSC_updateClassWithExcluded() {
        final String cql = PSC.update(Account.class, N.asSet("id", "createTime")).where(Filters.eq("id", 1)).build().query();
        assertTrue(cql.contains("UPDATE"), cql);
        assertTrue(!cql.contains("SET id"), cql);
    }

    @Test
    public void testPSC_deleteString() {
        final String cql = PSC.delete("firstName").from("account").where(Filters.eq("id", 1)).build().query();
        assertEquals("DELETE first_name FROM account WHERE id = ?", cql);
    }

    @Test
    public void testPSC_deleteStringArray() {
        final String cql = PSC.delete("firstName", "lastName").from("account").where(Filters.eq("id", 1)).build().query();
        assertEquals("DELETE first_name, last_name FROM account WHERE id = ?", cql);
    }

    @Test
    public void testPSC_deleteCollection() {
        final String cql = PSC.delete(N.asList("firstName", "lastName")).from("account").where(Filters.eq("id", 1)).build().query();
        assertEquals("DELETE first_name, last_name FROM account WHERE id = ?", cql);
    }

    @Test
    public void testPSC_deleteClass() {
        final String cql = PSC.delete(Account.class).from("account").where(Filters.eq("id", 1)).build().query();
        assertTrue(cql.startsWith("DELETE "), cql);
        assertTrue(cql.contains("FROM account"), cql);
    }

    @Test
    public void testPSC_deleteClassWithExcluded() {
        final String cql = PSC.delete(Account.class, N.asSet("firstName")).from("account").where(Filters.eq("id", 1)).build().query();
        assertTrue(cql.startsWith("DELETE "), cql);
        assertTrue(!cql.contains(" first_name"), cql);
    }

    @Test
    public void testPSC_deleteFromString() {
        final String cql = PSC.deleteFrom("account").where(Filters.eq("id", 1)).build().query();
        assertEquals("DELETE FROM account WHERE id = ?", cql);
    }

    @Test
    public void testPSC_deleteFromStringClass() {
        final String cql = PSC.deleteFrom("account", Account.class).where(Filters.eq("id", 1)).build().query();
        assertTrue(cql.startsWith("DELETE FROM account"), cql);
    }

    @Test
    public void testPSC_deleteFromClass() {
        final String cql = PSC.deleteFrom(Users.class).build().query();
        assertEquals("DELETE FROM simplex.users", cql);
    }

    @Test
    public void testPSC_selectString() {
        // SNAKE_CASE select produces "snake AS \"camel\"" aliasing.
        final String cql = PSC.select("firstName").from("account").build().query();
        assertEquals("SELECT first_name AS \"firstName\" FROM account", cql);
    }

    @Test
    public void testPSC_selectStringArray() {
        final String cql = PSC.select("firstName", "lastName").from("account").build().query();
        assertEquals("SELECT first_name AS \"firstName\", last_name AS \"lastName\" FROM account", cql);
    }

    @Test
    public void testPSC_selectCollection() {
        final String cql = PSC.select(N.asList("firstName", "lastName")).from("account").build().query();
        assertEquals("SELECT first_name AS \"firstName\", last_name AS \"lastName\" FROM account", cql);
    }

    @Test
    public void testPSC_selectMap() {
        final String cql = PSC.select(N.asMap("firstName", "fn")).from("account").build().query();
        assertTrue(cql.startsWith("SELECT first_name"), cql);
        assertTrue(cql.contains("\"fn\""), cql);
    }

    @Test
    public void testPSC_selectClass() {
        final String cql = PSC.select(Account.class).from("account").build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
        assertTrue(cql.contains("first_name"), cql);
    }

    @Test
    public void testPSC_selectClassWithSubEntities() {
        final String cql = PSC.select(Account.class, false).from("account").build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testPSC_selectClassWithExcluded() {
        final String cql = PSC.select(Account.class, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains(" first_name"), cql);
    }

    @Test
    public void testPSC_selectClassFull() {
        final String cql = PSC.select(Account.class, false, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains(" first_name"), cql);
    }

    @Test
    public void testPSC_selectFromClass() {
        final String cql = PSC.selectFrom(Users.class).build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testPSC_selectFromClassWithAlias() {
        final String cql = PSC.selectFrom(Users.class, "u").build().query();
        assertTrue(cql.contains("FROM simplex.users u"), cql);
    }

    @Test
    public void testPSC_selectFromClassWithSubEntities() {
        final String cql = PSC.selectFrom(Account.class, false).build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testPSC_selectFromClassWithExcluded() {
        final String cql = PSC.selectFrom(Account.class, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains(" first_name"), cql);
    }

    @Test
    public void testPSC_selectFromClassAliasSubEntities() {
        final String cql = PSC.selectFrom(Users.class, "u", false).build().query();
        assertTrue(cql.contains("FROM simplex.users u"), cql);
    }

    @Test
    public void testPSC_selectFromClassAliasExcluded() {
        final String cql = PSC.selectFrom(Users.class, "u", N.asSet("createdTime")).build().query();
        assertTrue(cql.contains("FROM simplex.users u"), cql);
        assertTrue(!cql.contains("created_time"), cql);
    }

    @Test
    public void testPSC_selectFromClassSubEntitiesExcluded() {
        final String cql = PSC.selectFrom(Account.class, false, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains(" first_name"), cql);
    }

    @Test
    public void testPSC_selectFromClassFull() {
        final String cql = PSC.selectFrom(Users.class, "u", false, N.asSet("createdTime")).build().query();
        assertTrue(cql.contains("FROM simplex.users u"), cql);
        assertTrue(!cql.contains("created_time"), cql);
    }

    @Test
    public void testPSC_countString() {
        final String cql = PSC.count("account").where(Filters.eq("id", 1)).build().query();
        assertEquals("SELECT count(*) FROM account WHERE id = ?", cql);
    }

    @Test
    public void testPSC_countClass() {
        final String cql = PSC.count(Users.class).build().query();
        assertEquals("SELECT count(*) FROM simplex.users", cql);
    }

    @Test
    public void testPSC_parse() {
        final String cql = PSC.renderCondition(Filters.eq("firstName", "John"), Account.class).build().query();
        assertTrue(cql.contains("first_name"), cql);
        assertTrue(cql.contains("?"), cql);
    }

    @Test
    public void testPSC_insert_EmptyString() {
        assertThrows(IllegalArgumentException.class, () -> PSC.insert(""));
    }

    @Test
    public void testPSC_chainBuildable() {
        // Smoke test: every public factory must reach createInstance() and build() must return a usable query.
        assertNotNull(PSC.select("a").from("t").build().query());
        assertNotNull(PSC.insert("a").into("t").build().query());
        assertNotNull(PSC.update("t").set("a").where(Filters.eq("id", 1)).build().query());
        assertNotNull(PSC.deleteFrom("t").where(Filters.eq("id", 1)).build().query());
        assertNotNull(PSC.count("t").build().query());
    }

    // ---------------------------------------------------------------------------------------------
    // SCCB (SNAKE_CASE + RAW_SQL): snake_case columns, inlined literal values.
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testSCCB_insertString() {
        final String cql = SCCB.insert("firstName").into("account").build().query();
        assertTrue(cql.startsWith("INSERT INTO account"), cql);
        assertTrue(cql.contains("first_name"), cql);
    }

    @Test
    public void testSCCB_insertStringArray() {
        final String cql = SCCB.insert("firstName", "lastName").into("account").build().query();
        assertTrue(cql.contains("first_name") && cql.contains("last_name"), cql);
    }

    @Test
    public void testSCCB_insertCollection() {
        final String cql = SCCB.insert(N.asList("firstName")).into("account").build().query();
        assertTrue(cql.contains("first_name"), cql);
    }

    @Test
    public void testSCCB_insertMap() {
        final String cql = SCCB.insert(N.asMap("firstName", "John")).into("account").build().query();
        assertTrue(cql.contains("first_name"), cql);
        assertTrue(cql.contains("'John'"), cql);
    }

    @Test
    public void testSCCB_insertEntity() {
        final Account a = new Account();
        a.setFirstName("John");
        final String cql = SCCB.insert(a).into("account").build().query();
        assertTrue(cql.contains("first_name"), cql);
        assertTrue(cql.contains("'John'"), cql);
    }

    @Test
    public void testSCCB_insertEntityWithExcluded() {
        final Account a = new Account();
        a.setFirstName("J");
        a.setLastName("D");
        final String cql = SCCB.insert(a, N.asSet("lastName")).into("account").build().query();
        assertTrue(!cql.contains("last_name"), cql);
    }

    @Test
    public void testSCCB_insertClass() {
        final String cql = SCCB.insert(Account.class).into("account").build().query();
        assertTrue(cql.contains("first_name"), cql);
    }

    @Test
    public void testSCCB_insertClassWithExcluded() {
        final String cql = SCCB.insert(Account.class, N.asSet("firstName")).into("account").build().query();
        assertTrue(!cql.contains("first_name"), cql);
    }

    @Test
    public void testSCCB_insertIntoClass() {
        final String cql = SCCB.insertInto(Users.class).build().query();
        assertTrue(cql.startsWith("INSERT INTO simplex.users"), cql);
    }

    @Test
    public void testSCCB_insertIntoClassWithExcluded() {
        final String cql = SCCB.insertInto(Users.class, N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("created_time"), cql);
    }

    @Test
    public void testSCCB_batchInsert() {
        final String cql = SCCB.batchInsert(N.asList(N.asMap("firstName", "a"))).into("account").build().query();
        assertTrue(cql.contains("first_name"), cql);
        assertTrue(cql.contains("'a'"), cql);
    }

    @Test
    public void testSCCB_updateString() {
        // RAW_SQL inlines values; identifiers in set() are snake-cased per naming policy.
        final String cql = SCCB.update("account").set("firstName = 'X'").where(Filters.eq("id", 1)).build().query();
        assertEquals("UPDATE account SET first_name = 'X' WHERE id = 1", cql);
    }

    @Test
    public void testSCCB_updateStringClass() {
        final String cql = SCCB.update("account", Account.class).set("firstName = 'X'").build().query();
        assertEquals("UPDATE account SET first_name = 'X'", cql);
    }

    @Test
    public void testSCCB_updateClass() {
        final String cql = SCCB.update(Account.class).set(N.asMap("firstName", "X")).where(Filters.eq("id", 1)).build().query();
        assertTrue(cql.contains("first_name"), cql);
    }

    @Test
    public void testSCCB_updateClassWithExcluded() {
        final String cql = SCCB.update(Account.class, N.asSet("id")).set(N.asMap("firstName", "X")).build().query();
        assertTrue(cql.contains("UPDATE"), cql);
    }

    @Test
    public void testSCCB_deleteString() {
        final String cql = SCCB.delete("firstName").from("account").build().query();
        assertEquals("DELETE first_name FROM account", cql);
    }

    @Test
    public void testSCCB_deleteStringArray() {
        final String cql = SCCB.delete("firstName", "lastName").from("account").build().query();
        assertEquals("DELETE first_name, last_name FROM account", cql);
    }

    @Test
    public void testSCCB_deleteCollection() {
        final String cql = SCCB.delete(N.asList("firstName")).from("account").build().query();
        assertTrue(cql.contains("first_name"), cql);
    }

    @Test
    public void testSCCB_deleteClass() {
        final String cql = SCCB.delete(Account.class).from("account").build().query();
        assertTrue(cql.startsWith("DELETE "), cql);
    }

    @Test
    public void testSCCB_deleteClassWithExcluded() {
        final String cql = SCCB.delete(Account.class, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains("first_name"), cql);
    }

    @Test
    public void testSCCB_deleteFromString() {
        final String cql = SCCB.deleteFrom("account").where(Filters.eq("id", 1)).build().query();
        assertEquals("DELETE FROM account WHERE id = 1", cql);
    }

    @Test
    public void testSCCB_deleteFromStringClass() {
        final String cql = SCCB.deleteFrom("account", Account.class).build().query();
        assertEquals("DELETE FROM account", cql);
    }

    @Test
    public void testSCCB_deleteFromClass() {
        final String cql = SCCB.deleteFrom(Users.class).build().query();
        assertEquals("DELETE FROM simplex.users", cql);
    }

    @Test
    public void testSCCB_selectString() {
        final String cql = SCCB.select("firstName").from("account").build().query();
        assertTrue(cql.contains("first_name"), cql);
    }

    @Test
    public void testSCCB_selectStringArray() {
        final String cql = SCCB.select("firstName", "lastName").from("account").build().query();
        assertTrue(cql.contains("first_name") && cql.contains("last_name"), cql);
    }

    @Test
    public void testSCCB_selectCollection() {
        final String cql = SCCB.select(N.asList("firstName")).from("account").build().query();
        assertTrue(cql.contains("first_name"), cql);
    }

    @Test
    public void testSCCB_selectMap() {
        final String cql = SCCB.select(N.asMap("firstName", "fn")).from("account").build().query();
        assertTrue(cql.contains("first_name"), cql);
        assertTrue(cql.contains("\"fn\""), cql);
    }

    @Test
    public void testSCCB_selectClass() {
        final String cql = SCCB.select(Account.class).from("account").build().query();
        assertTrue(cql.contains("first_name"), cql);
    }

    @Test
    public void testSCCB_selectClassWithSubEntities() {
        final String cql = SCCB.select(Account.class, false).from("account").build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testSCCB_selectClassWithExcluded() {
        final String cql = SCCB.select(Account.class, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains("first_name"), cql);
    }

    @Test
    public void testSCCB_selectClassFull() {
        final String cql = SCCB.select(Account.class, false, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains("first_name"), cql);
    }

    @Test
    public void testSCCB_selectFromClass() {
        final String cql = SCCB.selectFrom(Users.class).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testSCCB_selectFromClassWithAlias() {
        final String cql = SCCB.selectFrom(Users.class, "u").build().query();
        assertTrue(cql.contains("FROM simplex.users u"), cql);
    }

    @Test
    public void testSCCB_selectFromClassWithSubEntities() {
        final String cql = SCCB.selectFrom(Account.class, false).build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testSCCB_selectFromClassWithExcluded() {
        final String cql = SCCB.selectFrom(Account.class, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains("first_name"), cql);
    }

    @Test
    public void testSCCB_selectFromClassAliasSubEntities() {
        final String cql = SCCB.selectFrom(Users.class, "u", false).build().query();
        assertTrue(cql.contains("FROM simplex.users u"), cql);
    }

    @Test
    public void testSCCB_selectFromClassAliasExcluded() {
        final String cql = SCCB.selectFrom(Users.class, "u", N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("created_time"), cql);
    }

    @Test
    public void testSCCB_selectFromClassSubEntitiesExcluded() {
        final String cql = SCCB.selectFrom(Account.class, false, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains("first_name"), cql);
    }

    @Test
    public void testSCCB_selectFromClassFull() {
        final String cql = SCCB.selectFrom(Users.class, "u", false, N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("created_time"), cql);
    }

    @Test
    public void testSCCB_countString() {
        final String cql = SCCB.count("account").build().query();
        assertEquals("SELECT count(*) FROM account", cql);
    }

    @Test
    public void testSCCB_countClass() {
        final String cql = SCCB.count(Users.class).build().query();
        assertEquals("SELECT count(*) FROM simplex.users", cql);
    }

    @Test
    public void testSCCB_parse() {
        final String cql = SCCB.renderCondition(Filters.eq("firstName", "John"), Account.class).build().query();
        assertTrue(cql.contains("first_name"), cql);
        // RAW_SQL inlines the value.
        assertTrue(cql.contains("'John'"), cql);
    }

    @Test
    public void testSCCB_insert_EmptyString() {
        assertThrows(IllegalArgumentException.class, () -> SCCB.insert(""));
    }

    @Test
    public void testSCCB_chainBuildable() {
        // Smoke test for SCCB createInstance() reach via every public factory.
        assertNotNull(SCCB.select("a").from("t").build().query());
        assertNotNull(SCCB.insert("a").into("t").build().query());
        assertNotNull(SCCB.update("t").set("a = 'x'").build().query());
        assertNotNull(SCCB.deleteFrom("t").build().query());
        assertNotNull(SCCB.count("t").build().query());
    }

    // ---------------------------------------------------------------------------------------------
    // LCCB (CAMEL_CASE + RAW_SQL): camelCase columns, inlined literal values.
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testLCCB_insertString() {
        final String cql = LCCB.insert("firstName").into("account").build().query();
        assertTrue(cql.startsWith("INSERT INTO account"), cql);
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testLCCB_insertStringArray() {
        final String cql = LCCB.insert("firstName", "lastName").into("account").build().query();
        assertTrue(cql.contains("firstName") && cql.contains("lastName"), cql);
    }

    @Test
    public void testLCCB_insertCollection() {
        final String cql = LCCB.insert(N.asList("firstName")).into("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testLCCB_insertMap() {
        final String cql = LCCB.insert(N.asMap("firstName", "John")).into("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
        assertTrue(cql.contains("'John'"), cql);
    }

    @Test
    public void testLCCB_insertEntity() {
        final Account a = new Account();
        a.setFirstName("John");
        final String cql = LCCB.insert(a).into("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
        assertTrue(cql.contains("'John'"), cql);
    }

    @Test
    public void testLCCB_insertEntityWithExcluded() {
        final Account a = new Account();
        a.setFirstName("J");
        a.setLastName("D");
        final String cql = LCCB.insert(a, N.asSet("lastName")).into("account").build().query();
        assertTrue(!cql.contains("lastName"), cql);
    }

    @Test
    public void testLCCB_insertClass() {
        final String cql = LCCB.insert(Account.class).into("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testLCCB_insertClassWithExcluded() {
        final String cql = LCCB.insert(Account.class, N.asSet("firstName")).into("account").build().query();
        assertTrue(!cql.contains("firstName"), cql);
    }

    @Test
    public void testLCCB_insertIntoClass() {
        final String cql = LCCB.insertInto(Users.class).build().query();
        assertTrue(cql.startsWith("INSERT INTO simplex.users"), cql);
    }

    @Test
    public void testLCCB_insertIntoClassWithExcluded() {
        final String cql = LCCB.insertInto(Users.class, N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("createdTime"), cql);
    }

    @Test
    public void testLCCB_batchInsert() {
        final String cql = LCCB.batchInsert(N.asList(N.asMap("firstName", "a"))).into("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
        assertTrue(cql.contains("'a'"), cql);
    }

    @Test
    public void testLCCB_updateString() {
        // RAW_SQL inlines values; CAMEL_CASE keeps identifier casing.
        final String cql = LCCB.update("account").set("firstName = 'X'").where(Filters.eq("id", 1)).build().query();
        assertEquals("UPDATE account SET firstName = 'X' WHERE id = 1", cql);
    }

    @Test
    public void testLCCB_updateStringClass() {
        final String cql = LCCB.update("account", Account.class).set("firstName = 'X'").build().query();
        assertEquals("UPDATE account SET firstName = 'X'", cql);
    }

    @Test
    public void testLCCB_updateClass() {
        final String cql = LCCB.update(Account.class).set(N.asMap("firstName", "X")).where(Filters.eq("id", 1)).build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testLCCB_updateClassWithExcluded() {
        final String cql = LCCB.update(Account.class, N.asSet("id")).set(N.asMap("firstName", "X")).build().query();
        assertTrue(cql.contains("UPDATE"), cql);
    }

    @Test
    public void testLCCB_deleteString() {
        final String cql = LCCB.delete("firstName").from("account").build().query();
        assertEquals("DELETE firstName FROM account", cql);
    }

    @Test
    public void testLCCB_deleteStringArray() {
        final String cql = LCCB.delete("firstName", "lastName").from("account").build().query();
        assertEquals("DELETE firstName, lastName FROM account", cql);
    }

    @Test
    public void testLCCB_deleteCollection() {
        final String cql = LCCB.delete(N.asList("firstName")).from("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testLCCB_deleteClass() {
        final String cql = LCCB.delete(Account.class).from("account").build().query();
        assertTrue(cql.startsWith("DELETE "), cql);
    }

    @Test
    public void testLCCB_deleteClassWithExcluded() {
        final String cql = LCCB.delete(Account.class, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testLCCB_deleteFromString() {
        final String cql = LCCB.deleteFrom("account").where(Filters.eq("id", 1)).build().query();
        assertEquals("DELETE FROM account WHERE id = 1", cql);
    }

    @Test
    public void testLCCB_deleteFromStringClass() {
        final String cql = LCCB.deleteFrom("account", Account.class).build().query();
        assertEquals("DELETE FROM account", cql);
    }

    @Test
    public void testLCCB_deleteFromClass() {
        final String cql = LCCB.deleteFrom(Users.class).build().query();
        assertEquals("DELETE FROM simplex.users", cql);
    }

    @Test
    public void testLCCB_selectString() {
        final String cql = LCCB.select("firstName").from("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testLCCB_selectStringArray() {
        final String cql = LCCB.select("firstName", "lastName").from("account").build().query();
        assertTrue(cql.contains("firstName") && cql.contains("lastName"), cql);
    }

    @Test
    public void testLCCB_selectCollection() {
        final String cql = LCCB.select(N.asList("firstName")).from("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testLCCB_selectMap() {
        final String cql = LCCB.select(N.asMap("firstName", "fn")).from("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
        assertTrue(cql.contains("\"fn\""), cql);
    }

    @Test
    public void testLCCB_selectClass() {
        final String cql = LCCB.select(Account.class).from("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testLCCB_selectClassWithSubEntities() {
        final String cql = LCCB.select(Account.class, false).from("account").build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testLCCB_selectClassWithExcluded() {
        final String cql = LCCB.select(Account.class, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testLCCB_selectClassFull() {
        final String cql = LCCB.select(Account.class, false, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testLCCB_selectFromClass() {
        final String cql = LCCB.selectFrom(Users.class).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testLCCB_selectFromClassWithAlias() {
        final String cql = LCCB.selectFrom(Users.class, "u").build().query();
        assertTrue(cql.contains("FROM simplex.users u"), cql);
    }

    @Test
    public void testLCCB_selectFromClassWithSubEntities() {
        final String cql = LCCB.selectFrom(Account.class, false).build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testLCCB_selectFromClassWithExcluded() {
        final String cql = LCCB.selectFrom(Account.class, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testLCCB_selectFromClassAliasSubEntities() {
        final String cql = LCCB.selectFrom(Users.class, "u", false).build().query();
        assertTrue(cql.contains("FROM simplex.users u"), cql);
    }

    @Test
    public void testLCCB_selectFromClassAliasExcluded() {
        final String cql = LCCB.selectFrom(Users.class, "u", N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("createdTime"), cql);
    }

    @Test
    public void testLCCB_selectFromClassSubEntitiesExcluded() {
        final String cql = LCCB.selectFrom(Account.class, false, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testLCCB_selectFromClassFull() {
        final String cql = LCCB.selectFrom(Users.class, "u", false, N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("createdTime"), cql);
    }

    @Test
    public void testLCCB_countString() {
        final String cql = LCCB.count("account").build().query();
        assertEquals("SELECT count(*) FROM account", cql);
    }

    @Test
    public void testLCCB_countClass() {
        final String cql = LCCB.count(Users.class).build().query();
        assertEquals("SELECT count(*) FROM simplex.users", cql);
    }

    @Test
    public void testLCCB_parse() {
        final String cql = LCCB.renderCondition(Filters.eq("firstName", "John"), Account.class).build().query();
        assertTrue(cql.contains("firstName"), cql);
        // RAW_SQL inlines the value.
        assertTrue(cql.contains("'John'"), cql);
    }

    @Test
    public void testLCCB_insert_EmptyString() {
        assertThrows(IllegalArgumentException.class, () -> LCCB.insert(""));
    }

    @Test
    public void testLCCB_chainBuildable() {
        // Smoke test for LCCB createInstance() reach via every public factory.
        assertNotNull(LCCB.select("a").from("t").build().query());
        assertNotNull(LCCB.insert("a").into("t").build().query());
        assertNotNull(LCCB.update("t").set("a = 'x'").build().query());
        assertNotNull(LCCB.deleteFrom("t").build().query());
        assertNotNull(LCCB.count("t").build().query());
    }

    // ---------------------------------------------------------------------------------------------
    // CqlBuilder (top-level): repeatPlaceholders, repeatQM, deprecated/edge-case behavior.
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testCqlBuilder_repeatPlaceholders_zero() {
        // Zero placeholders produces an empty string.
        final String s = CqlBuilder.repeatPlaceholders(0);
        assertEquals("", s);
    }

    @Test
    public void testCqlBuilder_repeatPlaceholders_one() {
        final String s = CqlBuilder.repeatPlaceholders(1);
        assertEquals("?", s);
    }

    @Test
    public void testCqlBuilder_repeatPlaceholders_many() {
        final String s = CqlBuilder.repeatPlaceholders(5);
        assertEquals("?, ?, ?, ?, ?", s);
    }

    @Test
    public void testCqlBuilder_repeatPlaceholders_Negative() {
        assertThrows(IllegalArgumentException.class, () -> CqlBuilder.repeatPlaceholders(-1));
    }

    @Test
    public void testCqlBuilder_repeatQM_DeprecatedAliasDelegates() {
        // Deprecated alias must produce the same output as repeatPlaceholders.
        assertEquals(CqlBuilder.repeatPlaceholders(3), CqlBuilder.repeatQM(3));
    }

    // ---------------------------------------------------------------------------------------------
    // Regression tests for clause-ordering / parameter-preservation / escaping bug fixes.
    // ---------------------------------------------------------------------------------------------

    /**
     * Regression guard for {@code appendUsingOption}: calling {@code usingTTL} BEFORE
     * {@code where()}/{@code build()} on {@code update(entityClass)} used to consume the one-shot
     * {@code init} with {@code init(false)}, silently dropping the implicit SET assignments expanded
     * from the entity class (the query ended up with an empty SET clause).
     */
    @Test
    public void test_update_entityClass_usingTTL_beforeWhere_preservesSetClause() {
        final SP sp = PSC.update(Account.class).usingTTL(3600).where(Filters.eq("id", 1)).build();
        final String cql = sp.query();
        N.println(cql);

        assertTrue(cql.contains("UPDATE account USING TTL 3600 SET "), cql);

        // The SET clause must contain the entity's set assignments, not be empty.
        final int setIdx = cql.indexOf(" SET ");
        final int whereIdx = cql.indexOf(" WHERE ");
        assertTrue(setIdx > 0 && whereIdx > setIdx, cql);
        final String setClause = cql.substring(setIdx + " SET ".length(), whereIdx);
        assertFalse(setClause.trim().isEmpty(), cql);
        assertTrue(setClause.contains("first_name = ?"), cql);
        assertTrue(cql.endsWith(" WHERE id = ?"), cql);

        // Placeholder count = one per SET assignment + 1 for the WHERE.
        final int setAssignmentCount = setClause.split(", ").length;
        assertEquals(setAssignmentCount + 1, Strings.countMatches(cql, '?'), cql);

        // set(propNames) renders bare '?' placeholders without binding values, so only the WHERE
        // value is collected — the regression was parameters (and SET) being dropped entirely.
        assertEquals(N.asList(1), sp.parameters());
    }

    /**
     * Same regression as {@link #test_update_entityClass_usingTTL_beforeWhere_preservesSetClause()}
     * via the {@code usingTimestamp(String)} overload (which shared the broken
     * {@code appendUsingOption} path).
     */
    @Test
    public void test_update_entityClass_usingTimestamp_beforeWhere_preservesSetClause() {
        final SP sp = PSC.update(Account.class).usingTimestamp("123").where(Filters.eq("id", 1)).build();
        final String cql = sp.query();
        N.println(cql);

        assertTrue(cql.contains("UPDATE account USING TIMESTAMP 123 SET "), cql);

        final int setIdx = cql.indexOf(" SET ");
        final int whereIdx = cql.indexOf(" WHERE ");
        assertTrue(setIdx > 0 && whereIdx > setIdx, cql);
        final String setClause = cql.substring(setIdx + " SET ".length(), whereIdx);
        assertFalse(setClause.trim().isEmpty(), cql);
        assertTrue(setClause.contains("first_name = ?"), cql);

        final int setAssignmentCount = setClause.split(", ").length;
        assertEquals(setAssignmentCount + 1, Strings.countMatches(cql, '?'), cql);
        assertEquals(N.asList(1), sp.parameters());
    }

    /**
     * Regression guard for INSERT clause ordering: the CQL grammar is
     * {@code INSERT ... VALUES (...) [IF NOT EXISTS] [USING ...]}, so IF NOT EXISTS must precede
     * USING TTL regardless of the order in which the builder methods are called. Previously
     * {@code ifNotExists()} after {@code usingTTL(...)} appended IF NOT EXISTS after the USING
     * clause, and {@code usingTTL(...)} after {@code ifNotExists()} inserted USING before IF.
     */
    @Test
    public void test_insert_ifNotExists_usingTTL_orderedPerCqlGrammar_bothCallOrders() {
        final String expected = "INSERT INTO account (id, first_name) VALUES (?, ?) IF NOT EXISTS USING TTL 3600";

        final String ifFirst = PSC.insert("id", "firstName").into("account").ifNotExists().usingTTL(3600).build().query();
        N.println(ifFirst);
        assertEquals(expected, ifFirst);

        final String usingFirst = PSC.insert("id", "firstName").into("account").usingTTL(3600).ifNotExists().build().query();
        N.println(usingFirst);
        assertEquals(expected, usingFirst);
    }

    /**
     * Regression guard for {@code checkInsertClauseOrder}: appending USING/IF/ALLOW FILTERING
     * clauses to an INSERT before {@code into()} used to insert the clause into the empty buffer
     * (silently prepending it before the INSERT keyword); it must throw IllegalStateException.
     */
    @Test
    public void test_insert_usingOrIf_beforeInto_throwsIllegalStateException() {
        assertThrows(IllegalStateException.class, () -> PSC.insert("id").usingTTL(60));
        assertThrows(IllegalStateException.class, () -> PSC.insert("id").ifNotExists());
        assertThrows(IllegalStateException.class, () -> PSC.insert("id").onlyIf(Filters.isNull("name")));
    }

    /**
     * Regression guard for {@code iF(Condition)}/{@code onlyIf(Condition)} with an AND junction:
     * Cassandra's LWT grammar is {@code IF columnCondition (AND columnCondition)*} and rejects
     * parenthesized members, so junction members must render WITHOUT per-member parentheses in the
     * IF clause (WHERE junction rendering keeps its parentheses and is unaffected).
     */
    @Test
    public void test_onlyIf_andJunction_rendersWithoutParentheses() {
        final String cql = PSC.update("account")
                .set("firstName")
                .where(Filters.eq("id", 1))
                .onlyIf(Filters.and(Filters.eq("lastName", "x"), Filters.lt("id", 3)))
                .build()
                .query();
        N.println(cql);

        assertEquals("UPDATE account SET first_name = ? WHERE id = ? IF last_name = ? AND id < ?", cql);

        // iF(Condition) is the same path.
        final String iFCql = PSC.update("account")
                .set("firstName")
                .where(Filters.eq("id", 1))
                .iF(Filters.and(Filters.eq("lastName", "x"), Filters.lt("id", 3)))
                .build()
                .query();
        assertEquals(cql, iFCql);

        // The WHERE-clause junction rendering (with parentheses) is unaffected by the IF fix.
        final String whereCql = PSC.select("id").from("account").where(Filters.and(Filters.eq("a", 1), Filters.eq("b", 2))).build().query();
        assertEquals("SELECT id FROM account WHERE (a = ?) AND (b = ?)", whereCql);
    }

    /**
     * Regression guard for the In/NotIn branches of {@code appendCondition}: they must iterate
     * {@code getValues()} (one placeholder per IN-list element), not {@code getParameters()} (which
     * splices Condition-typed elements like {@code Filters.QME} out of the list and silently changed
     * the placeholder count from 3 to 2).
     */
    @Test
    public void test_in_withConditionTypedElement_rendersOnePlaceholderPerElement() {
        final SP sp = PSC.select("firstName").from("account").where(Filters.in("id", java.util.Arrays.asList(1, Filters.QME, 3))).build();
        N.println(sp.query());

        assertTrue(sp.query().endsWith("WHERE id IN (?, ?, ?)"), sp.query());
        // The QME element renders as a bare '?' without binding a value.
        assertEquals(N.asList(1, 3), sp.parameters());
    }

    /**
     * Regression guard for the SubQuery branch of {@code appendCondition}: the subquery used to be
     * rendered via {@code ...build().query()} which dropped the subquery's bind parameters, leaving
     * '?' placeholders with no bound values. The parameters must now be propagated to the outer
     * builder, and a SubQuery with a NULL condition (allowed by the SubQuery contract) must render
     * without a WHERE clause instead of throwing.
     */
    @Test
    public void test_subQuery_parametersPreserved() {
        final Condition cond = Filters.in("id", Filters.subQuery("account", N.asList("id"), Filters.eq("lastName", "Smith")));
        final SP sp = PSC.select("firstName").from("account").where(cond).build();
        N.println(sp.query());

        assertTrue(sp.query().contains("IN (SELECT id FROM account WHERE last_name = ?)"), sp.query());
        assertEquals(N.asList("Smith"), sp.parameters());

        // Named builder: the subquery's named placeholder and parameter must be preserved too.
        final Condition namedCond = Filters.in("id", Filters.subQuery("account", N.asList("id"), Filters.eq("lastName", "Smith")));
        final SP namedSp = NSC.select("firstName").from("account").where(namedCond).build();
        N.println(namedSp.query());
        assertTrue(namedSp.query().contains("IN (SELECT id FROM account WHERE last_name = :lastName)"), namedSp.query());
        assertEquals(N.asList("Smith"), namedSp.parameters());
    }

    @Test
    public void test_subQuery_withNullCondition_rendersWithoutWhere() {
        final Condition cond = Filters.in("id", Filters.subQuery("account", N.asList("id"), (Condition) null));
        final SP sp = PSC.select("firstName").from("account").where(cond).build();
        N.println(sp.query());

        assertTrue(sp.query().contains("IN (SELECT id FROM account)"), sp.query());
        assertTrue(sp.parameters().isEmpty(), sp.parameters().toString());
    }

    /**
     * Regression guard for {@code setParameterForRawSQL}: CQL escapes a single quote inside a string
     * literal by doubling it ({@code 'O''Brien'}); the SQL-style backslash escaping previously
     * inherited from the parent builder is a CQL syntax error.
     */
    @Test
    public void test_rawBuilder_escapesSingleQuoteCqlStyle() {
        final String cql = SCCB.select("firstName").from("account").where(Filters.eq("lastName", "O'Brien")).build().query();
        N.println(cql);

        assertTrue(cql.contains("last_name = 'O''Brien'"), cql);
        assertFalse(cql.contains("\\"), cql);
    }

    /**
     * Regression guard for {@code getDeletePropNamesByClass}: {@code delete(entityClass)} must use
     * the writable prop names (id / read-only / transient props removed) — Cassandra rejects DELETE
     * on primary-key columns. Previously the full select prop-name list (including the id) was used.
     */
    @Test
    public void test_delete_entityClass_excludesIdProps() {
        // Account: the conventional "id" prop must not appear in the DELETE column list
        // (it still appears in the WHERE clause, which is fine).
        final String cql = PSC.delete(Account.class).from("account").where(Filters.eq("id", 1)).build().query();
        N.println(cql);

        assertTrue(cql.startsWith("DELETE "), cql);
        final String columnList = cql.substring("DELETE ".length(), cql.indexOf(" FROM "));
        assertFalse(columnList.trim().isEmpty(), cql);
        assertTrue(N.asList(columnList.split(", ")).contains("first_name"), cql);
        assertFalse(N.asList(columnList.split(", ")).contains("id"), cql);

        // Users: the @Id-annotated "id" prop must be excluded as well.
        final String usersCql = NSC.delete(Users.class).from(Users.class).build().query();
        N.println(usersCql);

        final String usersColumnList = usersCql.substring("DELETE ".length(), usersCql.indexOf(" FROM "));
        assertTrue(N.asList(usersColumnList.split(", ")).contains("name"), usersCql);
        assertFalse(N.asList(usersColumnList.split(", ")).contains("id"), usersCql);
    }

    /**
     * Regression guard for {@code assertNotClosed}: after {@code build()} releases the internal
     * buffer, calling clause methods must throw IllegalStateException (not NullPointerException).
     */
    @Test
    public void test_builder_usedAfterBuild_throwsIllegalStateException() {
        final CqlBuilder ttlBuilder = PSC.select("firstName").from("account");
        ttlBuilder.build();
        assertThrows(IllegalStateException.class, () -> ttlBuilder.usingTTL(1));

        final CqlBuilder ifExistsBuilder = PSC.select("firstName").from("account");
        ifExistsBuilder.build();
        assertThrows(IllegalStateException.class, ifExistsBuilder::ifExists);

        final CqlBuilder allowFilteringBuilder = PSC.select("firstName").from("account");
        allowFilteringBuilder.build();
        assertThrows(IllegalStateException.class, allowFilteringBuilder::allowFiltering);
    }

    /**
     * Regression guard for BETWEEN named-parameter naming: an alias-qualified prop name like
     * {@code "o.orderDate"} must produce {@code :minOrderDate}/{@code :maxOrderDate} (alias prefix
     * stripped via {@code sanitizeNamedParameterName}), not punctuation-bearing placeholders like
     * {@code :minO.orderDate} that named-parameter parsers reject.
     */
    @Test
    public void test_between_namedParams_withAliasQualifiedProp() {
        final SP sp = NSC.select("firstName").from("account").where(Filters.between("o.orderDate", "a", "b")).build();
        N.println(sp.query());

        assertTrue(sp.query().contains("BETWEEN :minOrderDate AND :maxOrderDate"), sp.query());
        assertEquals(N.asList("a", "b"), sp.parameters());

        // Non-regression: the simple unaliased case keeps :minPropName/:maxPropName.
        final String simple = NSC.select("id").from("account").where(Filters.between("age", 18, 65)).build().query();
        assertEquals("SELECT id FROM account WHERE age BETWEEN :minAge AND :maxAge", simple);
    }
}
