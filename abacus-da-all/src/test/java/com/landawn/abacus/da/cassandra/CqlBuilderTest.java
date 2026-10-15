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

import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.da.entity.Account;
import com.landawn.abacus.query.AbstractQueryBuilder.SP;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.util.N;

public class CqlBuilderTest extends TestBase {

    // Metadata and validation

    /**
     * The documented key exclusion ("via @Id / registered keys") ignored keys registered with
     * {@code CassandraExecutorBase.registerKeys}: update(Class), set(bean), set(Class) and delete(Class) rendered the key
     * columns in SET / the DELETE column list, which Cassandra rejects for PRIMARY KEY parts.
     */
    @Test
    public void test_sliceQ_registeredKeys_areExcludedLikeIdProperties() {
        CassandraExecutorBase.registerKeys(RegKeyUser.class, N.asList("userId", "tenantId"));

        assertEquals("UPDATE reg_key_user SET name = ? WHERE user_id = ?", PSC.update(RegKeyUser.class).where(Filters.eq("userId", "u")).build().query());
        assertEquals("UPDATE reg_key_user SET name = ? WHERE user_id = ?",
                PSC.update("reg_key_user").set(RegKeyUser.class).where(Filters.eq("userId", "u")).build().query());
        assertEquals("DELETE name FROM reg_key_user WHERE user_id = ?",
                PSC.delete(RegKeyUser.class).from(RegKeyUser.class).where(Filters.eq("userId", "u")).build().query());

        final RegKeyUser user = new RegKeyUser();
        user.setUserId("u");
        user.setTenantId("t");
        user.setName("n");

        final SP sp = PSC.update("reg_key_user").set(user).where(Filters.eq("userId", "u")).build();
        assertEquals("UPDATE reg_key_user SET name = ? WHERE user_id = ?", sp.query());
        assertEquals(N.asList("n", "u"), sp.parameters());
    }

    /** Keys registered after the builder first resolved the class's (then empty) key names take effect on the next builder. */
    @Test
    public void test_verifyQB_registerKeys_afterFirstUse_takesEffect() {
        try {
            assertEquals("UPDATE verify_qb_late_key_user SET user_id = ?, name = ? WHERE user_id = ?",
                    PSC.update(VerifyQbLateKeyUser.class).where(Filters.eq("userId", "u")).build().query());

            CassandraExecutorBase.registerKeys(VerifyQbLateKeyUser.class, N.asList("userId"));

            assertEquals("UPDATE verify_qb_late_key_user SET name = ? WHERE user_id = ?",
                    PSC.update(VerifyQbLateKeyUser.class).where(Filters.eq("userId", "u")).build().query());
            assertEquals("UPDATE verify_qb_late_key_user USING TTL 3 SET name = :name WHERE user_id = :userId",
                    NSC.update(VerifyQbLateKeyUser.class).usingTTL(3).where(Filters.eq("userId", "u")).build().query());
            assertEquals("DELETE name FROM verify_qb_late_key_user WHERE user_id = ?",
                    PSC.delete(VerifyQbLateKeyUser.class).from(VerifyQbLateKeyUser.class).where(Filters.eq("userId", "u")).build().query());
        } finally {
            CassandraExecutorBase.entityKeyNamesMap.remove(VerifyQbLateKeyUser.class);
        }
    }

    // Row and entity mapping

    @Test
    public void testACCB_parse() {
        final String cql = ACCB.renderCondition(Filters.eq("firstName", "John"), Account.class).build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
        // RAW_SQL inlines the value.
        assertTrue(cql.contains("'John'"), cql);
    }

    @Test
    public void testLCCB_parse() {
        final String cql = LCCB.renderCondition(Filters.eq("firstName", "John"), Account.class).build().query();
        assertTrue(cql.contains("firstName"), cql);
        // RAW_SQL inlines the value.
        assertTrue(cql.contains("'John'"), cql);
    }

    @Test
    public void testNAC_parse() {
        final String cql = NAC.renderCondition(Filters.eq("firstName", "John"), Account.class).build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
        assertTrue(cql.contains(":firstName"), cql);
    }

    @Test
    public void testNLC_parse() {
        final String cql = NLC.renderCondition(Filters.eq("firstName", "John"), Account.class).build().query();
        assertTrue(cql.contains("firstName"), cql);
        assertTrue(cql.contains(":firstName"), cql);
    }

    @Test
    public void testNSB_parse() {
        final String cql = NSB.renderCondition(Filters.eq("firstName", "John"), Account.class).build().query();
        assertEquals("firstName = :firstName", cql.trim());
    }

    @Test
    public void testNSC_parse() {
        final String cql = NSC.renderCondition(Filters.eq("firstName", "John"), Account.class).build().query();
        assertEquals("first_name = :firstName", cql.trim());
    }

    @Test
    public void testPAC_parse() {
        final String cql = PAC.renderCondition(Filters.eq("firstName", "John"), Account.class).build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
        assertTrue(cql.contains("?"), cql);
    }

    @Test
    public void testPLC_parse() {
        final String cql = PLC.renderCondition(Filters.eq("firstName", "John"), Account.class).build().query();
        assertTrue(cql.contains("firstName"), cql);
        assertTrue(cql.contains("?"), cql);
    }

    @Test
    public void testPSB_parse() {
        final String cql = PSB.renderCondition(Filters.eq("firstName", "John"), Account.class).build().query();
        assertTrue(cql.contains("firstName"), cql);
        assertTrue(cql.contains("?"), cql);
    }

    @Test
    public void testPSC_parse() {
        final String cql = PSC.renderCondition(Filters.eq("firstName", "John"), Account.class).build().query();
        assertTrue(cql.contains("first_name"), cql);
        assertTrue(cql.contains("?"), cql);
    }

    @Test
    public void testSCCB_parse() {
        final String cql = SCCB.renderCondition(Filters.eq("firstName", "John"), Account.class).build().query();
        assertTrue(cql.contains("first_name"), cql);
        // RAW_SQL inlines the value.
        assertTrue(cql.contains("'John'"), cql);
    }

    // Additional contracts

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
    public void testCqlBuilder_repeatPlaceholders_one() {
        final String s = CqlBuilder.repeatPlaceholders(1);
        assertEquals("?", s);
    }

    // CqlBuilder (top-level): repeatPlaceholders, repeatQM, deprecated/edge-case behavior.

    @Test
    public void testCqlBuilder_repeatPlaceholders_zero() {
        // Zero placeholders produces an empty string.
        final String s = CqlBuilder.repeatPlaceholders(0);
        assertEquals("", s);
    }

    @Test
    public void testCqlBuilder_repeatQM_DeprecatedAliasDelegates() {
        // Deprecated alias must produce the same output as repeatPlaceholders.
        assertEquals(CqlBuilder.repeatPlaceholders(3), CqlBuilder.repeatQM(3));
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

    @Test
    public void testLCCB_chainBuildable() {
        // Smoke test for LCCB createInstance() reach via every public factory.
        assertNotNull(LCCB.select("a").from("t").build().query());
        assertNotNull(LCCB.insert("a").into("t").build().query());
        assertNotNull(LCCB.update("t").set("a = 'x'").build().query());
        assertNotNull(LCCB.deleteFrom("t").build().query());
        assertNotNull(LCCB.count("t").build().query());
    }

    @Test
    public void testNSB_isNamedSql() {
        // Indirect: a NAMED_SQL builder must emit ':' placeholders, not '?'.
        final String cql = NSB.select("a").from("t").where(Filters.eq("a", 1)).build().query();
        assertTrue(cql.contains(":a"), cql);
        assertTrue(!cql.contains("?"), cql);
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

    @Test
    public void testNSC_isNamedSql() {
        // Indirect: a NAMED_SQL builder must emit ':' placeholders, not '?'.
        final String cql = NSC.select("a").from("t").where(Filters.eq("a", 1)).build().query();
        assertTrue(cql.contains(":a"), cql);
        assertTrue(!cql.contains("?"), cql);
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
     * Raw (deprecated) builders inline values as CQL literals. A CQL {@code uuid}/{@code timeuuid} literal is bare hex;
     * the parent builder's fallback rendered a {@link java.util.UUID} as a quoted string, which Cassandra rejects for
     * uuid columns ("Invalid STRING constant ... for type uuid").
     */
    @Test
    public void test_rawBuilder_rendersUuidAsBareLiteral() {
        final java.util.UUID uuid = java.util.UUID.fromString("123e4567-e89b-12d3-a456-426614174000");

        final String select = SCCB.select("firstName").from("account").where(Filters.eq("id", uuid)).build().query();
        N.println(select);
        assertEquals("SELECT first_name AS \"firstName\" FROM account WHERE id = 123e4567-e89b-12d3-a456-426614174000", select);

        final String insert = SCCB.insert(N.asMap("id", uuid)).into("account").build().query();
        assertEquals("INSERT INTO account (id) VALUES (123e4567-e89b-12d3-a456-426614174000)", insert);

        final String update = LCCB.update("account").set(N.asMap("ownerId", uuid)).where(Filters.eq("id", 1)).build().query();
        assertEquals("UPDATE account SET ownerId = 123e4567-e89b-12d3-a456-426614174000 WHERE id = 1", update);

        // Strings are still quoted (CQL doubled-quote escaping), so a uuid-shaped String is not affected.
        assertEquals("SELECT first_name AS \"firstName\" FROM account WHERE id = '123e4567-e89b-12d3-a456-426614174000'",
                SCCB.select("firstName").from("account").where(Filters.eq("id", uuid.toString())).build().query());
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

    // ---- 2026-10-02 sliceQ ----

    /** Bean whose primary key is declared only via {@link CassandraExecutorBase#registerKeys(Class, java.util.Collection)}. */
    public static class RegKeyUser {
        private String userId;
        private String tenantId;
        private String name;

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
    }

    /** Bean whose keys are registered only after the builder has already resolved (and cached) its key names. */
    public static class VerifyQbLateKeyUser {
        private String userId;
        private String name;

        public String getUserId() {
            return userId;
        }

        public void setUserId(final String userId) {
            this.userId = userId;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }
}
