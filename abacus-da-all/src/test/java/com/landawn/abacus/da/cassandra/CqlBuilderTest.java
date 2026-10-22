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

    // ---- 2026-10-19 sliceI ----

    /**
     * abacus-query 4.9.5 inlines a Date/Calendar as zone-less JVM-local text, which Cassandra reads in the coordinator's
     * time zone (a live insert from a UTC-8 JVM stored the instant 8 hours early). Raw CQL must carry the UTC instant.
     */
    @Test
    public void test_sliceI_rawTimestampLiteralsAreUtcInstants() {
        final java.util.TimeZone defaultZone = java.util.TimeZone.getDefault();
        java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("America/Los_Angeles"));

        try {
            final long millis = 1700000000123L; // 2023-11-14T22:13:20.123Z
            final String utc = "'2023-11-14T22:13:20.123Z'";
            final java.sql.Timestamp timestamp = new java.sql.Timestamp(millis);
            timestamp.setNanos(123456789);
            final java.util.Calendar tokyo = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("Asia/Tokyo"));
            tokyo.setTimeInMillis(millis);
            final java.time.ZonedDateTime zoned = java.time.Instant.ofEpochMilli(millis).atZone(java.time.ZoneId.of("Asia/Tokyo"));
            final java.util.Map<String, Object> props = new java.util.LinkedHashMap<>();
            props.put("id", 1);
            props.put("ts", new java.util.Date(millis));

            org.junit.jupiter.api.Assertions.assertAll(
                    () -> assertEquals("INSERT INTO t (id, ts) VALUES (1, " + utc + ")", SCCB.insert(props).into("t").build().query()),
                    () -> assertEquals("SELECT id FROM t WHERE ts = " + utc, SCCB.select("id").from("t").where(Filters.eq("ts", timestamp)).build().query()),
                    () -> assertEquals("UPDATE t SET ts = " + utc + " WHERE id = 1",
                            SCCB.update("t").set(N.asMap("ts", tokyo)).where(Filters.eq("id", 1)).build().query()),
                    () -> assertEquals("DELETE FROM t WHERE id = 1 IF ts = " + utc,
                            SCCB.deleteFrom("t").where(Filters.eq("id", 1)).onlyIf(Filters.eq("ts", zoned)).build().query()),
                    () -> assertEquals("SELECT id FROM t WHERE ts IN ('2023-11-14T22:13:20Z', " + utc + ")",
                            SCCB.select("id").from("t").where(Filters.in("ts", N.asList(new java.util.Date(1700000000000L), tokyo))).build().query()),
                    () -> assertEquals("BEGIN BATCH INSERT INTO t (id, ts) VALUES (1, " + utc + "); APPLY BATCH",
                            SCCB.batchInsert(N.asList(props)).into("t").build().query()),
                    // java.sql.Date/Time keep the parent's local date / time-of-day text, which CQL date/time columns accept.
                    () -> assertEquals("SELECT id FROM t WHERE d = '2023-11-14' AND tm = '14:13:20'",
                            SCCB.select("id")
                                    .from("t")
                                    .where(Filters.and(Filters.eq("d", new java.sql.Date(millis)), Filters.eq("tm", new java.sql.Time(millis))))
                                    .build()
                                    .query()),
                    // Parameterized builders bind the value itself, unchanged.
                    () -> assertEquals(N.asList(timestamp), PSC.select("id").from("t").where(Filters.eq("ts", timestamp)).build().parameters()));
        } finally {
            java.util.TimeZone.setDefault(defaultZone);
        }
    }

    // ---- 2026-10-19 verify3 ----

    /** A raw ZonedDateTime literal has the documented millisecond form; it carried the nanosecond fraction before. */
    @Test
    public void test_verify3_rawZonedDateTimeLiteralIsTruncatedToMillis() {
        final java.time.ZonedDateTime zoned = java.time.Instant.ofEpochSecond(1700000000L, 123_999_999).atZone(java.time.ZoneId.of("Asia/Tokyo"));
        final java.time.ZonedDateTime beforeEpoch = java.time.Instant.ofEpochSecond(-1L, 999_500_000).atZone(java.time.ZoneOffset.UTC);

        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertEquals("SELECT id FROM t WHERE ts = '2023-11-14T22:13:20.123Z'",
                        SCCB.select("id").from("t").where(Filters.eq("ts", zoned)).build().query()),
                () -> assertEquals("SELECT id FROM t WHERE ts = '1969-12-31T23:59:59.999Z'",
                        SCCB.select("id").from("t").where(Filters.eq("ts", beforeEpoch)).build().query()));
    }

    // ---- 2026-10-20 cassfix ----

    /**
     * A raw Instant/OffsetDateTime fell through to the parent, which inlines its toString() with a nanosecond fraction
     * ('2023-11-14T22:13:20.123456789Z'; Cassandra 5.0 accepts and truncates it, so this is consistency with the documented
     * millisecond form); a raw LocalDateTime rendered as zone-less text ('2023-11-14T14:13:20.123456789'), read in the
     * coordinator's zone, which shifted the stored instant. They now render the millisecond UTC instant; the
     * LocalDateTime denotes the instant a bound LocalDateTime stores (its wall-clock time in the JVM default zone).
     */
    @Test
    public void test_cassfix_rawJavaTimeTimestampLiterals() {
        final java.util.TimeZone defaultZone = java.util.TimeZone.getDefault();
        java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("America/Los_Angeles"));

        try {
            final java.time.Instant instant = java.time.Instant.ofEpochSecond(1700000000L, 123_456_789); // 2023-11-14T22:13:20.123456789Z
            final java.time.OffsetDateTime offset = instant.atOffset(java.time.ZoneOffset.ofHours(5)); // 2023-11-15T03:13:20.123456789+05:00
            final java.time.LocalDateTime local = java.time.LocalDateTime.of(2023, 11, 14, 14, 13, 20, 123_456_789); // 14:13 PST = 22:13Z
            final java.time.ZonedDateTime zoned = instant.atZone(java.time.ZoneId.of("Asia/Tokyo"));
            final String utc = "'2023-11-14T22:13:20.123Z'";
            final java.util.Map<String, Object> props = new java.util.LinkedHashMap<>();
            props.put("id", 1);
            props.put("ts", instant);
            final java.util.Map<String, Object> setProps = new java.util.LinkedHashMap<>();
            setProps.put("ts", offset);
            final java.util.Map<String, Object> localProps = new java.util.LinkedHashMap<>();
            localProps.put("id", 1);
            localProps.put("ts", local);

            org.junit.jupiter.api.Assertions.assertAll(
                    () -> assertEquals("SELECT id FROM t WHERE ts = " + utc, SCCB.select("id").from("t").where(Filters.eq("ts", instant)).build().query()),
                    () -> assertEquals("SELECT id FROM t WHERE ts = " + utc, SCCB.select("id").from("t").where(Filters.eq("ts", offset)).build().query()),
                    () -> assertEquals("SELECT id FROM t WHERE ts = " + utc, SCCB.select("id").from("t").where(Filters.eq("ts", local)).build().query()),
                    () -> assertEquals("INSERT INTO t (id, ts) VALUES (1, " + utc + ")", SCCB.insert(props).into("t").build().query()),
                    () -> assertEquals("INSERT INTO t (id, ts) VALUES (1, " + utc + ")", SCCB.insert(localProps).into("t").build().query()),
                    () -> assertEquals("UPDATE t SET ts = " + utc + " WHERE id = 1", SCCB.update("t").set(setProps).where(Filters.eq("id", 1)).build().query()),
                    () -> assertEquals("UPDATE t SET ts = " + utc + " WHERE id = 1",
                            SCCB.update("t").set(N.asMap("ts", local)).where(Filters.eq("id", 1)).build().query()),
                    () -> assertEquals("SELECT id FROM t WHERE ts IN (" + utc + ", " + utc + ", '2023-11-14T22:13:20Z', " + utc + ")",
                            SCCB.select("id")
                                    .from("t")
                                    .where(Filters.in("ts", N.asList(zoned, instant, java.time.Instant.ofEpochSecond(1700000000L), local)))
                                    .build()
                                    .query()),
                    () -> assertEquals("DELETE FROM t WHERE id = 1 IF ts = " + utc,
                            SCCB.deleteFrom("t").where(Filters.eq("id", 1)).onlyIf(Filters.eq("ts", offset)).build().query()),
                    // Parameterized builders bind the values themselves, unchanged.
                    () -> assertEquals(N.asList(instant, offset, local),
                            PSC.select("id")
                                    .from("t")
                                    .where(Filters.and(Filters.eq("a", instant), Filters.eq("b", offset), Filters.eq("c", local)))
                                    .build()
                                    .parameters()));
        } finally {
            java.util.TimeZone.setDefault(defaultZone);
        }
    }

    // ---- 2026-10-21 fixQ ----

    /**
     * Instant.toString() renders a UTC year outside 0000-9999 with a sign ('+10000-01-01T00:00:00Z', '-0001-01-01T00:00:00Z');
     * Cassandra 5.0 rejects that quoted literal, and the unsigned year 0000 too ("Unable to parse a date/time from ..."), while
     * it accepts the bare epoch-millisecond integer for the whole long range. Every raw instant-valued branch
     * (Date, Calendar, Instant, OffsetDateTime, ZonedDateTime and LocalDateTime) now renders the integer outside UTC years
     * 0001-9999 and keeps the quoted ISO form inside it; the range is judged on the UTC year, not the local one.
     */
    @Test
    public void test_fixQ_rawTimestampLiteralOutsideFourDigitYearsRendersEpochMillis() {
        final java.util.TimeZone defaultZone = java.util.TimeZone.getDefault();
        java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("America/Los_Angeles"));

        try {
            final java.time.Instant lastIso = java.time.Instant.parse("9999-12-31T23:59:59.999Z");
            final java.time.Instant firstIso = java.time.Instant.parse("0001-01-01T00:00:00Z");
            final java.time.Instant year10000 = java.time.Instant.parse("+10000-01-01T00:00:00Z"); // 253402300800000
            final java.time.Instant yearMinus1 = java.time.Instant.parse("-0001-01-01T00:00:00Z"); // -62198755200000
            final java.time.Instant year0 = java.time.Instant.parse("0000-12-31T23:59:59.999Z"); // -62135596800001
            final java.util.Date date = new java.util.Date(253402300800000L);
            final java.util.Calendar calendar = java.util.Calendar.getInstance();
            calendar.setTimeInMillis(-62198755200000L);
            final java.time.ZonedDateTime zoned = year10000.atZone(java.time.ZoneId.of("Asia/Tokyo")); // +10000-01-01T09:00+09:00
            final java.time.OffsetDateTime offset = yearMinus1.atOffset(java.time.ZoneOffset.ofHours(5));
            final java.time.LocalDateTime local = java.time.LocalDateTime.of(9999, 12, 31, 16, 0); // 16:00 PST on 9999-12-31 = +10000-01-01T00:00Z
            final java.util.Map<String, Object> props = new java.util.LinkedHashMap<>();
            props.put("id", 1);
            props.put("ts", year10000);
            final java.util.Map<String, Object> dateProps = new java.util.LinkedHashMap<>();
            dateProps.put("id", 1);
            dateProps.put("ts", date);
            final java.util.Map<String, Object> localProps = new java.util.LinkedHashMap<>();
            localProps.put("id", 1);
            localProps.put("ts", local);

            org.junit.jupiter.api.Assertions.assertAll(
                    // Inside 0001-9999 the quoted ISO form is unchanged, at both ends of the range.
                    () -> assertEquals("SELECT id FROM t WHERE ts = '9999-12-31T23:59:59.999Z'",
                            SCCB.select("id").from("t").where(Filters.eq("ts", lastIso)).build().query()),
                    () -> assertEquals("SELECT id FROM t WHERE ts = '0001-01-01T00:00:00Z'",
                            SCCB.select("id").from("t").where(Filters.eq("ts", firstIso)).build().query()),
                    () -> assertEquals("SELECT id FROM t WHERE ts = '9999-12-31T23:59:59.999Z'",
                            SCCB.select("id").from("t").where(Filters.eq("ts", new java.util.Date(253402300799999L))).build().query()),
                    // Outside it, the bare epoch-millisecond integer, for every instant-valued input type.
                    () -> assertEquals("SELECT id FROM t WHERE ts = 253402300800000",
                            SCCB.select("id").from("t").where(Filters.eq("ts", year10000)).build().query()),
                    () -> assertEquals("SELECT id FROM t WHERE ts = -62198755200000",
                            SCCB.select("id").from("t").where(Filters.eq("ts", yearMinus1)).build().query()),
                    () -> assertEquals("SELECT id FROM t WHERE ts = -62135596800001",
                            SCCB.select("id").from("t").where(Filters.eq("ts", year0)).build().query()),
                    () -> assertEquals("SELECT id FROM t WHERE ts = 253402300800000",
                            SCCB.select("id").from("t").where(Filters.eq("ts", date)).build().query()),
                    () -> assertEquals("SELECT id FROM t WHERE ts = -62198755200000",
                            SCCB.select("id").from("t").where(Filters.eq("ts", calendar)).build().query()),
                    () -> assertEquals("SELECT id FROM t WHERE ts = 253402300800000",
                            SCCB.select("id").from("t").where(Filters.eq("ts", zoned)).build().query()),
                    () -> assertEquals("SELECT id FROM t WHERE ts = -62198755200000",
                            SCCB.select("id").from("t").where(Filters.eq("ts", offset)).build().query()),
                    () -> assertEquals("SELECT id FROM t WHERE ts = 253402300800000",
                            SCCB.select("id").from("t").where(Filters.eq("ts", local)).build().query()),
                    () -> assertEquals("SELECT id FROM t WHERE ts IN (253402300800000, '9999-12-31T23:59:59.999Z', -62198755200000)",
                            SCCB.select("id").from("t").where(Filters.in("ts", N.asList(zoned, lastIso, calendar))).build().query()),
                    () -> assertEquals("INSERT INTO t (id, ts) VALUES (1, 253402300800000)", SCCB.insert(props).into("t").build().query()),
                    () -> assertEquals("INSERT INTO t (id, ts) VALUES (1, 253402300800000)", SCCB.insert(dateProps).into("t").build().query()),
                    () -> assertEquals("INSERT INTO t (id, ts) VALUES (1, 253402300800000)", SCCB.insert(localProps).into("t").build().query()),
                    () -> assertEquals("UPDATE t SET ts = -62198755200000 WHERE id = 1",
                            SCCB.update("t").set(N.asMap("ts", yearMinus1)).where(Filters.eq("id", 1)).build().query()),
                    () -> assertEquals("DELETE FROM t WHERE id = 1 IF ts = 253402300800000",
                            SCCB.deleteFrom("t").where(Filters.eq("id", 1)).onlyIf(Filters.eq("ts", date)).build().query()),
                    // Beyond the 64-bit millisecond range no CQL timestamp exists: rejected as an invalid argument, not an ArithmeticException.
                    () -> assertThrows(IllegalArgumentException.class,
                            () -> SCCB.select("id").from("t").where(Filters.eq("ts", java.time.Instant.MAX)).build()),
                    () -> assertThrows(IllegalArgumentException.class,
                            () -> SCCB.select("id").from("t").where(Filters.eq("ts", java.time.Instant.MIN)).build()),
                    // Within LocalDate's range (so the value itself is constructible) but past the long millisecond range.
                    () -> assertThrows(IllegalArgumentException.class,
                            () -> SCCB.select("id")
                                    .from("t")
                                    .where(Filters.eq("ts", java.time.ZonedDateTime.of(999_999_999, 12, 31, 0, 0, 0, 0, java.time.ZoneOffset.UTC)))
                                    .build()),
                    // Parameterized builders bind the values themselves, unchanged.
                    () -> assertEquals(N.asList(year10000, date, local),
                            PSC.select("id")
                                    .from("t")
                                    .where(Filters.and(Filters.eq("a", year10000), Filters.eq("b", date), Filters.eq("c", local)))
                                    .build()
                                    .parameters()));
        } finally {
            java.util.TimeZone.setDefault(defaultZone);
        }
    }

    /**
     * A heap buffer sliced out of the middle of an array has a non-zero arrayOffset(); the hex must start at
     * arrayOffset + position and stop at arrayOffset + limit, and the buffer's position/limit must be left unchanged.
     * A read-only buffer has no accessible array and takes the absolute-get path.
     */
    @Test
    public void test_cassfix_rawBlobLiteralHonoursArrayOffsetAndReadOnlyBuffers() {
        final byte[] backing = { 1, 2, 3, 4, 5, 6 };
        final java.nio.ByteBuffer slice = java.nio.ByteBuffer.wrap(backing, 2, 3).slice(); // array offset 2 -> bytes 3, 4, 5
        slice.position(1); // remaining: 4, 5
        final java.nio.ByteBuffer readOnly = java.nio.ByteBuffer.wrap(backing, 1, 4).slice().asReadOnlyBuffer(); // bytes 2, 3, 4, 5
        readOnly.position(1).limit(3); // remaining: 3, 4

        org.junit.jupiter.api.Assertions.assertAll(() -> assertTrue(slice.hasArray()), () -> assertEquals(2, slice.arrayOffset()),
                () -> assertEquals("SELECT id FROM t WHERE b = 0x0405", SCCB.select("id").from("t").where(Filters.eq("b", slice)).build().query()),
                () -> assertEquals(1, slice.position()), () -> assertEquals(3, slice.limit()), () -> assertFalse(readOnly.hasArray()),
                () -> assertEquals("SELECT id FROM t WHERE b = 0x0304", SCCB.select("id").from("t").where(Filters.eq("b", readOnly)).build().query()),
                () -> assertEquals(1, readOnly.position()), () -> assertEquals(3, readOnly.limit()), () -> assertEquals("UPDATE t SET b = 0x0405 WHERE id = 1",
                        SCCB.update("t").set(N.asMap("b", slice)).where(Filters.eq("id", 1)).build().query()));
    }

    // ---- 2026-10-19 cqlfix ----

    /**
     * Raw CQL rendered a byte[] as '[1, 2, -1]', a ByteBuffer as '' or a Base64 fragment, and an InetAddress as
     * '/127.0.0.1'; Cassandra rejects all of them for blob/inet columns ("Invalid STRING constant ... of type blob").
     * A blob is the constant 0x&lt;hex&gt; and an inet is the quoted host address (both verified on live Cassandra).
     */
    @Test
    public void test_cqlfix_rawBlobAndInetLiterals() throws Exception {
        final byte[] bytes = { 1, 2, (byte) 0xff };
        final java.nio.ByteBuffer heap = java.nio.ByteBuffer.wrap(new byte[] { 9, 8, 7, 6 });
        heap.position(1);
        final java.nio.ByteBuffer direct = java.nio.ByteBuffer.allocateDirect(2).put(new byte[] { 0x10, (byte) 0xab }).flip();
        final java.nio.ByteBuffer readOnlySlice = java.nio.ByteBuffer.wrap(new byte[] { 5, 6, 7 }, 1, 2).slice().asReadOnlyBuffer();
        final java.net.InetAddress v4 = java.net.InetAddress.getByName("127.0.0.1");
        final java.net.InetAddress v6 = java.net.InetAddress.getByName("::1");
        final java.util.Map<String, Object> props = new java.util.LinkedHashMap<>();
        props.put("id", 1);
        props.put("b", bytes);
        props.put("ip", v4);
        final java.util.Map<String, Object> setProps = new java.util.LinkedHashMap<>();
        setProps.put("b", new byte[0]);
        setProps.put("ip", v6);

        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertEquals("INSERT INTO t (id, b, ip) VALUES (1, 0x0102ff, '127.0.0.1')", SCCB.insert(props).into("t").build().query()),
                () -> assertEquals("BEGIN BATCH INSERT INTO t (id, b, ip) VALUES (1, 0x0102ff, '127.0.0.1'); APPLY BATCH",
                        SCCB.batchInsert(N.asList(props)).into("t").build().query()),
                () -> assertEquals("UPDATE t SET b = 0x, ip = '0:0:0:0:0:0:0:1' WHERE id = 1",
                        SCCB.update("t").set(setProps).where(Filters.eq("id", 1)).build().query()),
                () -> assertEquals("SELECT id FROM t WHERE b = 0x080706", SCCB.select("id").from("t").where(Filters.eq("b", heap)).build().query()),
                () -> assertEquals(1, heap.position()),
                () -> assertEquals("SELECT id FROM t WHERE b IN (0x10ab, 0x0607)",
                        SCCB.select("id").from("t").where(Filters.in("b", N.asList(direct, readOnlySlice))).build().query()),
                () -> assertEquals(0, direct.position()), () -> assertEquals(0, readOnlySlice.position()),
                () -> assertEquals("DELETE FROM t WHERE id = 1 IF ip = '0:0:0:0:0:0:0:1'",
                        SCCB.deleteFrom("t").where(Filters.eq("id", 1)).onlyIf(Filters.eq("ip", v6)).build().query()),
                () -> {
                    final String cql = ACCB.select("id").from("t").where(Filters.and(Filters.eq("b", bytes), Filters.eq("ip", v4))).build().query();
                    assertTrue(cql.endsWith(" = 0x0102ff AND IP = '127.0.0.1'"), cql);
                }, () -> {
                    final String cql = LCCB.select("id").from("t").where(Filters.eq("b", java.nio.ByteBuffer.allocate(0))).build().query();
                    assertTrue(cql.endsWith(" = 0x"), cql);
                },
                // Parameterized builders bind the values themselves (the condition keeps its own copy of a byte[]).
                () -> {
                    final SP sp = PSC.select("id").from("t").where(Filters.and(Filters.eq("b", bytes), Filters.eq("c", heap), Filters.eq("ip", v4))).build();
                    assertEquals("SELECT id FROM t WHERE b = ? AND c = ? AND ip = ?", sp.query());
                    org.junit.jupiter.api.Assertions.assertArrayEquals(bytes, (byte[]) sp.parameters().get(0));
                    org.junit.jupiter.api.Assertions.assertSame(heap, sp.parameters().get(1));
                    org.junit.jupiter.api.Assertions.assertSame(v4, sp.parameters().get(2));
                });
    }

    // ---- 2026-10-20 verify8 ----

    /**
     * A scoped IPv6 address's host address carries a "%zone" suffix. Live Cassandra rejects an interface-name zone
     * ('fe80:0:0:0:0:0:0:1%ethernet_32769', even '%lo'/'%eth0': "Unable to make inet address from ...") and drops a
     * numeric one; a bound InetAddress sends only the 16 address bytes. The literal must be the unscoped address.
     */
    @Test
    public void test_verify8_rawScopedIpv6LiteralDropsZone() throws Exception {
        final byte[] linkLocal = java.net.InetAddress.getByName("fe80::1").getAddress();
        final java.net.InetAddress numericScope = java.net.Inet6Address.getByAddress(null, linkLocal, 1);
        assertEquals("fe80:0:0:0:0:0:0:1%1", numericScope.getHostAddress());

        assertEquals("SELECT id FROM t WHERE ip = 'fe80:0:0:0:0:0:0:1'", SCCB.select("id").from("t").where(Filters.eq("ip", numericScope)).build().query());

        // An interface-name zone (as on addresses enumerated from NetworkInterface), when this machine has one.
        for (final java.net.NetworkInterface ni : java.util.Collections.list(java.net.NetworkInterface.getNetworkInterfaces())) {
            final java.net.InetAddress named;

            try {
                named = java.net.Inet6Address.getByAddress(null, linkLocal, ni);
            } catch (final java.net.UnknownHostException e) {
                continue; // no link-local IPv6 address on this interface
            }

            assertTrue(named.getHostAddress().contains("%"), named.getHostAddress());
            assertEquals("SELECT id FROM t WHERE ip = 'fe80:0:0:0:0:0:0:1'", SCCB.select("id").from("t").where(Filters.eq("ip", named)).build().query());
            break;
        }
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

    // ---- 2026-10-21 cassfix2 ----

    /**
     * Pins the Dsl.forDialect Javadoc: a KEBAB_CASE dialect is accepted, but every statement method of the returned Dsl
     * throws IllegalArgumentException, because hyphenated names are not valid unquoted CQL identifiers.
     */
    @Test
    public void cassfix2_kebabCaseDsl_isAcceptedButEveryStatementMethodThrows() {
        final com.landawn.abacus.query.SqlDialect kebab = com.landawn.abacus.query.SqlDialect.builder()
                .namingPolicy(com.landawn.abacus.util.NamingPolicy.KEBAB_CASE)
                .sqlPolicy(com.landawn.abacus.query.SqlDialect.SqlPolicy.PARAMETERIZED_SQL)
                .build();
        final CqlBuilder.Dsl dsl = CqlBuilder.Dsl.forDialect(kebab);

        assertNotNull(dsl);
        org.junit.jupiter.api.Assertions.assertAll(() -> assertThrows(IllegalArgumentException.class, () -> dsl.select("firstName").from("t").build().query()),
                () -> assertThrows(IllegalArgumentException.class, () -> dsl.select("firstName").from(Account.class).build().query()),
                () -> assertThrows(IllegalArgumentException.class, () -> dsl.renderCondition(Filters.eq("firstName", 1), null).build().query()),
                () -> assertThrows(IllegalArgumentException.class, () -> dsl.renderCondition(Filters.eq("firstName", 1), Account.class).build().query()),
                () -> assertThrows(IllegalArgumentException.class, () -> dsl.insert("firstName").into("t").build().query()),
                () -> assertThrows(IllegalArgumentException.class, () -> dsl.update("t").set("firstName").where(Filters.eq("id", 1)).build().query()),
                () -> assertThrows(IllegalArgumentException.class, () -> dsl.deleteFrom("t").where(Filters.eq("firstName", 1)).build().query()),
                // Predefined dialects are unaffected.
                () -> assertEquals("SELECT first_name AS \"firstName\" FROM t", PSC.select("firstName").from("t").build().query()));
    }
}
