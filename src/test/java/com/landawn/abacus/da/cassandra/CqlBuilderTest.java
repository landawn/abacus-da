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
        N.println("OR junction: " + PSC.select("id").from("account").where(Filters.or(Filters.eq("a", 1), Filters.eq("b", 2))).build().query());
        N.println("TTL+IF order: " + PSC.update("account").set("firstName").where(Filters.eq("id", 1)).usingTTL(60).iF(Filters.eq("s", "x")).build().query());
        N.println("INSERT TTL: " + PSC.insert("id", "name").into("account").usingTTL(60).build().query());
        N.println("batchInsert: " + SCCB.batchInsert(N.asList(N.asMap("firstName", "a"), N.asMap("firstName", "b"))).into("account").build().query());
        N.println("NotEqual null: " + SCCB.select("id").from("account").where(Filters.ne("firstName", "x")).build().query());
        N.println("InSubQuery: " + PSC.select("id").from("account")
                .where(Filters.in("id", SubQueryGen())).build().query());
        N.println("NotInSubQuery: " + NSC.select("id").from("account")
                .where(Filters.notIn("id", SubQueryGen())).build().query());
        N.println("parse cond NSC: " + NSC.parse(Filters.and(Filters.eq("status", "A"), Filters.gt("balance", 1000)), Account.class).build().query());
        N.println("parse cond SCCB: " + SCCB.parse(Filters.and(Filters.eq("status", "A"), Filters.gt("balance", 1000)), Account.class).build().query());
        N.println("nested AND/OR: " + PSC.select("id").from("account")
                .where(Filters.and(Filters.eq("a", 1), Filters.or(Filters.eq("b", 2), Filters.eq("c", 3)))).build().query());
        N.println("update entity: " + SCCB.update(Account.class).set("firstName", "lastName").where(Filters.eq("id", 1)).build().query());
        N.println("multi-col InSubQuery: " + PSC.select("id").from("account")
                .where(new com.landawn.abacus.query.condition.InSubQuery(N.asList("a", "b"),
                        new com.landawn.abacus.query.condition.SubQuery("t", N.asList("x", "y"), Filters.eq("z", 1)))).build().query());
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
        final String countCql = NSC.select(N.asList(CqlBuilder.COUNT_ALL))
                .from("account")
                .appendIf(true, Filters.eq("id", Filters.QME))
                .build()
                .query();
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
     * Verifies the (NamingPolicy, SQLPolicy) pairing and createInstance() return type implied by each
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
        assertTrue(ttlCql.endsWith(" USING TTL 3600"), ttlCql);

        // USING TIMESTAMP(long): milliseconds -> microseconds (x1000).
        final String tsLongCql = PSC.update("account").set("firstName").where(Filters.eq("id", 1)).usingTimestamp(1234567890123L).build().query();
        N.println(tsLongCql);
        assertTrue(tsLongCql.endsWith(" USING TIMESTAMP 1234567890123000"), tsLongCql);

        // USING TIMESTAMP(Date): Date.getTime() (ms) -> microseconds (x1000), same as the long overload.
        final String tsDateCql = PSC.update("account").set("firstName").where(Filters.eq("id", 1)).usingTimestamp(new Date(1234567890123L)).build()
                .query();
        N.println(tsDateCql);
        assertTrue(tsDateCql.endsWith(" USING TIMESTAMP 1234567890123000"), tsDateCql);

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
     * Regression guard for the AND vs OR junction keyword in {@code appendCondition} (Junction branch)
     * and the documented Cassandra parenthesization (per-element parens, no outer wrapping parens).
     */
    @Test
    public void test_and_or_junction_keyword_rendering() {
        final String and = PSC.select("id").from("account").where(Filters.and(Filters.eq("a", 1), Filters.eq("b", 2))).build().query();
        N.println(and);
        assertEquals("SELECT id FROM account WHERE (a = ?) AND (b = ?)", and);

        final String or = PSC.select("id").from("account").where(Filters.or(Filters.eq("a", 1), Filters.eq("b", 2))).build().query();
        N.println(or);
        assertEquals("SELECT id FROM account WHERE (a = ?) OR (b = ?)", or);
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
        assertTrue(ttl.endsWith(" USING TTL 3600"), ttl);
        assertTrue(!ttl.contains("TIMESTAMP"), ttl);

        final String tsLong = PSC.update("account").set("firstName").where(Filters.eq("id", 1)).usingTimestamp(1000L).build().query();
        N.println(tsLong);
        assertTrue(tsLong.endsWith(" USING TIMESTAMP 1000000"), tsLong);

        final String tsDate = PSC.update("account").set("firstName").where(Filters.eq("id", 1)).usingTimestamp(new Date(2L)).build().query();
        N.println(tsDate);
        assertTrue(tsDate.endsWith(" USING TIMESTAMP 2000"), tsDate);

        final String tsStr = PSC.update("account").set("firstName").where(Filters.eq("id", 1)).usingTimestamp("42").build().query();
        N.println(tsStr);
        // String overload is verbatim (no x1000) per its contract.
        assertTrue(tsStr.endsWith(" USING TIMESTAMP 42"), tsStr);
    }
}
