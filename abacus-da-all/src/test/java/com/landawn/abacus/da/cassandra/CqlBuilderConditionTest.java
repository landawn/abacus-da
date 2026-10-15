package com.landawn.abacus.da.cassandra;

import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.NSC;
import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.PSC;
import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.SCCB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Date;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.da.entity.Account;
import com.landawn.abacus.query.AbstractQueryBuilder.SP;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.util.N;

public class CqlBuilderConditionTest extends TestBase {

    @Test
    public void zzz_explore() {
        assertThrows(IllegalArgumentException.class, () -> PSC.select("id").from("account").where(Filters.between("age", 18, 65)).build());
        assertThrows(IllegalArgumentException.class, () -> NSC.select("id").from("account").where(Filters.between("age", 18, 65)).build());
        assertThrows(IllegalArgumentException.class, () -> PSC.select("id").from("account").where(Filters.notBetween("age", 18, 65)).build());
        N.println("IN PSC: " + PSC.select("id").from("account").where(Filters.in("id", N.asList(1, 2, 3))).build().query());
        N.println("IN NSC: " + NSC.select("id").from("account").where(Filters.in("id", N.asList(1, 2, 3))).build().query());
        assertThrows(IllegalArgumentException.class, () -> NSC.select("id").from("account").where(Filters.notIn("id", N.asList(1, 2, 3))).build());
        N.println("onlyIf cond PSC: " + PSC.update("account").set("firstName").where(Filters.eq("id", 1)).onlyIf(Filters.eq("status", "x")).build().query());
        N.println("DELETE cols: " + PSC.delete("firstName", "lastName").from("account").where(Filters.eq("id", 1)).build().query());
        N.println("AND junction: " + PSC.select("id").from("account").where(Filters.and(Filters.eq("a", 1), Filters.eq("b", 2))).build().query());
        assertThrows(IllegalArgumentException.class, () -> PSC.select("id").from("account").where(Filters.or(Filters.eq("a", 1), Filters.eq("b", 2))).build());
        N.println(
                "TTL+IF order: " + PSC.update("account").set("firstName").where(Filters.eq("id", 1)).usingTTL(60).onlyIf(Filters.eq("s", "x")).build().query());
        N.println("INSERT TTL: " + PSC.insert("id", "name").into("account").usingTTL(60).build().query());
        N.println("batchInsert: " + SCCB.batchInsert(N.asList(N.asMap("firstName", "a"), N.asMap("firstName", "b"))).into("account").build().query());
        N.println("NotEqual null: " + SCCB.select("id").from("account").where(Filters.ne("firstName", "x")).build().query());
        assertThrows(IllegalArgumentException.class, () -> PSC.select("id").from("account").where(Filters.in("id", SubQueryGen())).build());
        assertThrows(IllegalArgumentException.class, () -> NSC.select("id").from("account").where(Filters.notIn("id", SubQueryGen())).build());
        N.println("parse cond NSC: " + NSC.renderCondition(Filters.and(Filters.eq("status", "A"), Filters.gt("balance", 1000)), Account.class).build().query());
        N.println(
                "parse cond SCCB: " + SCCB.renderCondition(Filters.and(Filters.eq("status", "A"), Filters.gt("balance", 1000)), Account.class).build().query());
        assertThrows(IllegalArgumentException.class,
                () -> PSC.select("id").from("account").where(Filters.and(Filters.eq("a", 1), Filters.or(Filters.eq("b", 2), Filters.eq("c", 3)))).build());
        N.println("update entity: " + SCCB.update(Account.class).set("firstName", "lastName").where(Filters.eq("id", 1)).build().query());
        assertThrows(IllegalArgumentException.class,
                () -> PSC.select("id")
                        .from("account")
                        .where(new com.landawn.abacus.query.condition.InSubQuery(N.asList("a", "b"),
                                new com.landawn.abacus.query.condition.SubQuery("t", N.asList("x", "y"), Filters.eq("z", 1))))
                        .build());
        assertThrows(IllegalArgumentException.class, () -> SCCB.select("id").from("account").where(Filters.eq("firstName", null)).build());
        N.println("count SCCB: " + SCCB.count("account").where(Filters.eq("id", 1)).build().query());
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

        // IF EXISTS vs IF NOT EXISTS on their valid statement types (not swapped) + leading spacing.
        final String ifExistsCql = PSC.update("account").set("firstName").where(Filters.eq("id", 1)).ifExists().build().query();
        N.println(ifExistsCql);
        assertTrue(ifExistsCql.endsWith(" IF EXISTS"), ifExistsCql);
        assertTrue(!ifExistsCql.contains("IF NOT EXISTS"), ifExistsCql);

        final String ifNotExistsCql = PSC.insert("id", "firstName").into("account").ifNotExists().build().query();
        N.println(ifNotExistsCql);
        assertTrue(ifNotExistsCql.endsWith(" IF NOT EXISTS"), ifNotExistsCql);

        // ALLOW FILTERING leading space (must not produce "id = ?ALLOW FILTERING").
        final String afCql = PSC.select("firstName").from("account").where(Filters.eq("id", 1)).allowFiltering().build().query();
        N.println(afCql);
        assertTrue(afCql.endsWith(" ALLOW FILTERING"), afCql);
        assertTrue(afCql.contains("? ALLOW FILTERING"), afCql);

        // Lightweight transaction IF clause spacing (leading space, single space around expr).
        final String iFCql = PSC.update("account").set("firstName").where(Filters.eq("id", 1)).onlyIf("status = 'inactive'").build().query();
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

        final String onlyIfNullCql = SCCB.update("account")
                .set("first_name = 'updated'")
                .where(Filters.eq("id", 1))
                .onlyIf(Filters.eq("deleted_at", null))
                .build()
                .query();
        assertEquals("UPDATE account SET first_name = 'updated' WHERE id = 1 IF deleted_at = null", onlyIfNullCql);

        final SP onlyIfNullSp = PSC.update("account").set("firstName").where(Filters.eq("id", 1)).onlyIf(Filters.eq("deletedAt", null)).build();
        assertEquals("UPDATE account SET first_name = ? WHERE id = ? IF deleted_at = ?", onlyIfNullSp.query());
        assertNull(onlyIfNullSp.parameters().get(onlyIfNullSp.parameters().size() - 1));

        assertThrows(IllegalArgumentException.class,
                () -> PSC.update("account").set("firstName").where(Filters.eq("id", 1)).onlyIf(Filters.gt("deletedAt", null)).build());
        assertThrows(IllegalArgumentException.class,
                () -> PSC.update("account").set("firstName").where(Filters.eq("id", 1)).onlyIf(Filters.like("status", "active%")).build());
    }

    /** Verifies the supported IN renderer and fail-fast handling for SQL-only predicates. */
    @Test
    public void test_in_and_unsupported_sql_predicates() {
        final String in = PSC.select("id").from("account").where(Filters.in("id", N.asList(1, 2, 3))).build().query();
        N.println(in);
        assertEquals("SELECT id FROM account WHERE id IN (?, ?, ?)", in);

        final String namedIn = NSC.select("id").from("account").where(Filters.in("id", N.asList(1, 2, 3))).build().query();
        N.println(namedIn);
        assertEquals("SELECT id FROM account WHERE id IN (:id1, :id2, :id3)", namedIn);

        assertThrows(IllegalArgumentException.class, () -> PSC.select("id").from("account").where(Filters.between("age", 18, 65)).build());
        assertThrows(IllegalArgumentException.class, () -> PSC.select("id").from("account").where(Filters.notBetween("age", 18, 65)).build());
        assertThrows(IllegalArgumentException.class, () -> PSC.select("id").from("account").where(Filters.notIn("id", N.asList(1, 2, 3))).build());
        assertThrows(IllegalArgumentException.class, () -> PSC.select("id").from("account").where(Filters.isNull("name")).build());
        assertThrows(IllegalArgumentException.class, () -> PSC.select("id").from("account").where(Filters.isNotNull("name")).build());
        assertEquals("SELECT id FROM account WHERE name LIKE ?", PSC.select("id").from("account").where(Filters.like("name", "A%")).build().query());
        assertThrows(IllegalArgumentException.class, () -> PSC.select("id").from("account").where(Filters.in("id", SubQueryGen())).build());
    }

    /**
     * Regression guard for supported AND junction rendering and rejected OR junctions in Cassandra
     * WHERE clauses.
     */
    @Test
    public void test_and_or_junction_keyword_rendering() {
        final String and = PSC.select("id").from("account").where(Filters.and(Filters.eq("a", 1), Filters.eq("b", 2))).build().query();
        N.println(and);
        assertEquals("SELECT id FROM account WHERE a = ? AND b = ?", and);

        assertThrows(IllegalArgumentException.class, () -> PSC.select("id").from("account").where(Filters.or(Filters.eq("a", 1), Filters.eq("b", 2))).build());
    }

    /**
     * Regression guard for the In/NotIn branches of {@code appendCondition}: they must iterate
     * {@code values()} (one placeholder per IN-list element), not {@code parameters()} (which
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
     * Registered keys are excluded at every site that drops key columns -- update(Class[, excluded]) (implicit SET list,
     * also when USING keeps it pending), set(Class[, excluded]) / set(bean[, excluded]) (withIdPropNames) and
     * delete(Class[, excluded]) (column DELETE) -- in a positional, a named and a raw-literal dialect. assertAll reports
     * every case, so the HEAD run shows each site rendering the key columns, not only the first one.
     */
    @Test
    public void test_coverageCQ_registeredKeys_excludedAtEverySiteAndOverload() {
        CassandraExecutorBase.registerKeys(CoverageCqRegKeyItem.class, N.asList("userId", "tenantId"));

        try {
            final Condition key = Filters.and(Filters.eq("userId", "u"), Filters.eq("tenantId", "t"));
            final CoverageCqRegKeyItem item = new CoverageCqRegKeyItem();
            item.setUserId("u");
            item.setTenantId("t");
            item.setName("n");
            item.setScore(3);

            org.junit.jupiter.api.Assertions.assertAll(
                    () -> assertEquals("UPDATE coverage_cq_reg_key_item SET name = ?, score = ? WHERE user_id = ? AND tenant_id = ?",
                            PSC.update(CoverageCqRegKeyItem.class).where(key).build().query()),
                    () -> assertEquals("UPDATE coverage_cq_reg_key_item SET name = ? WHERE user_id = ? AND tenant_id = ?",
                            PSC.update(CoverageCqRegKeyItem.class, N.asSet("score")).where(key).build().query()),
                    () -> assertEquals("UPDATE coverage_cq_reg_key_item SET name = :name, score = :score WHERE user_id = :userId AND tenant_id = :tenantId",
                            NSC.update(CoverageCqRegKeyItem.class).where(key).build().query()),
                    () -> assertEquals("UPDATE coverage_cq_reg_key_item SET name = ?, score = ? WHERE user_id = 'u' AND tenant_id = 't'",
                            SCCB.update(CoverageCqRegKeyItem.class).where(key).build().query()),
                    () -> assertEquals(
                            "UPDATE coverage_cq_reg_key_item USING TTL 5 SET name = :name, score = :score WHERE user_id = :userId AND tenant_id = :tenantId",
                            NSC.update(CoverageCqRegKeyItem.class).usingTTL(5).where(key).build().query()),
                    () -> assertEquals("UPDATE coverage_cq_reg_key_item SET name = ?, score = ? WHERE user_id = ? AND tenant_id = ?",
                            PSC.update("coverage_cq_reg_key_item").set(CoverageCqRegKeyItem.class).where(key).build().query()),
                    () -> assertEquals("UPDATE coverage_cq_reg_key_item SET score = ? WHERE user_id = ? AND tenant_id = ?",
                            PSC.update("coverage_cq_reg_key_item").set(CoverageCqRegKeyItem.class, N.asSet("name")).where(key).build().query()),
                    () -> {
                        final SP sp = PSC.update("coverage_cq_reg_key_item").set(item).where(key).build();
                        assertEquals("UPDATE coverage_cq_reg_key_item SET name = ?, score = ? WHERE user_id = ? AND tenant_id = ?", sp.query());
                        assertEquals(N.asList("n", 3, "u", "t"), sp.parameters());
                    }, () -> {
                        final SP sp = PSC.update("coverage_cq_reg_key_item").set(item, N.asSet("score")).where(key).build();
                        assertEquals("UPDATE coverage_cq_reg_key_item SET name = ? WHERE user_id = ? AND tenant_id = ?", sp.query());
                        assertEquals(N.asList("n", "u", "t"), sp.parameters());
                    },
                    () -> assertEquals("DELETE name, score FROM coverage_cq_reg_key_item WHERE user_id = ? AND tenant_id = ?",
                            PSC.delete(CoverageCqRegKeyItem.class).from(CoverageCqRegKeyItem.class).where(key).build().query()),
                    () -> assertEquals("DELETE name FROM coverage_cq_reg_key_item WHERE user_id = :userId AND tenant_id = :tenantId",
                            NSC.delete(CoverageCqRegKeyItem.class, N.asSet("score")).from(CoverageCqRegKeyItem.class).where(key).build().query()),
                    // Excluding every non-key property leaves nothing to SET (on HEAD the key columns were still set).
                    () -> assertThrows(IllegalArgumentException.class,
                            () -> PSC.update("coverage_cq_reg_key_item").set(CoverageCqRegKeyItem.class, N.asSet("name", "score"))));

            // Previous behavior kept: INSERT and SELECT still list the key columns.
            assertEquals("INSERT INTO coverage_cq_reg_key_item (user_id, tenant_id, name, score) VALUES (?, ?, ?, ?)",
                    PSC.insertInto(CoverageCqRegKeyItem.class).build().query());
            assertEquals("SELECT user_id AS \"userId\", tenant_id AS \"tenantId\", name AS \"name\", score AS \"score\" FROM coverage_cq_reg_key_item",
                    PSC.selectFrom(CoverageCqRegKeyItem.class).build().query());
        } finally {
            CassandraExecutorBase.entityKeyNamesMap.remove(CoverageCqRegKeyItem.class);
        }
    }

    private static com.landawn.abacus.query.condition.SubQuery SubQueryGen() {
        return new com.landawn.abacus.query.condition.SubQuery("account", N.asList("id"), Filters.eq("status", "A"));
    }

    // ---- 2026-10-04 coverageCQ ----

    /** Bean whose composite primary key (userId, tenantId) is declared only via registerKeys (no @Id, no "id" property). */
    public static class CoverageCqRegKeyItem {
        private String userId;
        private String tenantId;
        private String name;
        private int score;

        public String getUserId() {
            return userId;
        }

        public void setUserId(final String userId) {
            this.userId = userId;
        }

        public String getTenantId() {
            return tenantId;
        }

        public void setTenantId(final String tenantId) {
            this.tenantId = tenantId;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public int getScore() {
            return score;
        }

        public void setScore(final int score) {
            this.score = score;
        }
    }
}
