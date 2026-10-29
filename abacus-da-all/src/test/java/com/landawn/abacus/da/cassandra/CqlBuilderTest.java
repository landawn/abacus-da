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

    // ---- 2026-10-24 sliceI ----

    /**
     * Raw CQL quoted a driver duration ('1h30m'), and Cassandra accepts a quoted constant only for text, ascii, inet, date,
     * time and timestamp columns: "Invalid STRING constant (1h30m) for "d" of type duration" (Cassandra 5.0
     * Constants.Literal). The duration constant is the bare text of the driver value; a zero duration, whose toString() is
     * "", renders 0s. Both driver generations' duration classes are recognized; parameterized builders bind the value.
     */
    @Test
    public void test_sliceI_rawDriverDurationLiteralsAreBare() {
        final com.datastax.oss.driver.api.core.data.CqlDuration v4 = com.datastax.oss.driver.api.core.data.CqlDuration.from("1h30m");
        final com.datastax.oss.driver.api.core.data.CqlDuration negative = com.datastax.oss.driver.api.core.data.CqlDuration.newInstance(-14, -3,
                -5_000_000_123L);
        final com.datastax.oss.driver.api.core.data.CqlDuration zero = com.datastax.oss.driver.api.core.data.CqlDuration.newInstance(0, 0, 0);
        final com.datastax.driver.core.Duration v3 = com.datastax.driver.core.Duration.from("2d4h");
        final com.datastax.driver.core.Duration v3Zero = com.datastax.driver.core.Duration.newInstance(0, 0, 0);
        final java.util.Map<String, Object> props = new java.util.LinkedHashMap<>();
        props.put("id", 1);
        props.put("d", v4);

        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertEquals("UPDATE t SET d = 1h30m WHERE id = 1", SCCB.update("t").set("d", v4).where(Filters.eq("id", 1)).build().query()),
                () -> assertEquals("UPDATE t SET d = -1y2mo3d5s123ns WHERE id = 1",
                        SCCB.update("t").set("d", negative).where(Filters.eq("id", 1)).build().query()),
                () -> assertEquals("UPDATE t SET d = 0s WHERE id = 1", SCCB.update("t").set("d", zero).where(Filters.eq("id", 1)).build().query()),
                () -> assertEquals("UPDATE t SET d = 2d4h WHERE id = 1", SCCB.update("t").set("d", v3).where(Filters.eq("id", 1)).build().query()),
                () -> assertEquals("UPDATE t SET d = 0s WHERE id = 1", SCCB.update("t").set("d", v3Zero).where(Filters.eq("id", 1)).build().query()),
                () -> assertEquals("INSERT INTO t (id, d) VALUES (1, 1h30m)", SCCB.insert(props).into("t").build().query()),
                () -> assertEquals("BEGIN BATCH INSERT INTO t (id, d) VALUES (1, 1h30m); APPLY BATCH",
                        SCCB.batchInsert(N.asList(props)).into("t").build().query()),
                () -> assertEquals("DELETE FROM t WHERE id = 1 IF d = 1h30m",
                        SCCB.deleteFrom("t").where(Filters.eq("id", 1)).onlyIf(Filters.eq("d", v4)).build().query()),
                () -> {
                    final String cql = ACCB.update("t").set("d", v3).where(Filters.eq("id", 1)).build().query();
                    assertTrue(cql.contains(" = 2d4h "), cql);
                }, () -> {
                    final SP sp = PSC.update("t").set("d", v4).where(Filters.eq("id", 1)).build();
                    assertEquals("UPDATE t SET d = ? WHERE id = ?", sp.query());
                    org.junit.jupiter.api.Assertions.assertSame(v4, sp.parameters().get(0));
                });
    }

    /** Bean with a primitive {@code @Id} key; 0 is a real Cassandra key value. */
    public static class ZeroKeyUser {
        @com.landawn.abacus.annotation.Id
        private long id;
        private String name;
        private String email;

        public long getId() {
            return id;
        }

        public void setId(final long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public String getEmail() {
            return email;
        }

        public void setEmail(final String email) {
            this.email = email;
        }
    }

    /** Bean whose key is the conventional property named {@code id} (no annotation). */
    public static class ConventionalKeyUser {
        private long id;
        private String name;

        public long getId() {
            return id;
        }

        public void setId(final long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    /** Bean with a composite {@code @Id} key. */
    public static class CompositeKeyUser {
        @com.landawn.abacus.annotation.Id
        private int a;
        @com.landawn.abacus.annotation.Id
        private Integer b;
        private String name;

        public int getA() {
            return a;
        }

        public void setA(final int a) {
            this.a = a;
        }

        public Integer getB() {
            return b;
        }

        public void setB(final Integer b) {
            this.b = b;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    /**
     * The parent builder applies the SQL generated-key convention to entity INSERTs: when every id property (@Id, or a
     * property named id) is null or numerically zero it is left out, so insert(entity) and batchInsert(entities) rendered
     * "INSERT INTO t (name) VALUES (?)" for a key of 0, which Cassandra rejects ("Some partition key parts are missing").
     * Cassandra never generates keys: a zero key is kept now, a null one is still left out, and nothing else changes.
     */
    @Test
    public void test_sliceI_zeroKeyIsKeptInEntityInserts() {
        final ZeroKeyUser zero = new ZeroKeyUser();
        zero.setName("n");
        final ZeroKeyUser five = new ZeroKeyUser();
        five.setId(5);
        five.setName("m");
        final ConventionalKeyUser conventional = new ConventionalKeyUser();
        conventional.setName("n");
        final CompositeKeyUser compositeZero = new CompositeKeyUser();
        compositeZero.setB(0);
        compositeZero.setName("n");
        final CompositeKeyUser compositeNull = new CompositeKeyUser();
        compositeNull.setName("n");

        org.junit.jupiter.api.Assertions.assertAll(() -> {
            final SP sp = PSC.insert(zero).into("t").build();
            assertEquals("INSERT INTO t (id, name) VALUES (?, ?)", sp.query());
            assertEquals(N.asList(0L, "n"), sp.parameters());
        }, () -> assertEquals("INSERT INTO t (id, name) VALUES (:id, :name)", NSC.insert(zero).into("t").build().query()),
                () -> assertEquals("INSERT INTO t (ID, NAME) VALUES (?, ?)", PAC.insert(zero).into("t").build().query()),
                () -> assertEquals("INSERT INTO t (id, name) VALUES (0, 'n')", SCCB.insert(zero).into("t").build().query()),
                () -> assertEquals("INSERT INTO t (id, name) VALUES (?, ?)", PSC.insert(zero, N.asSet("email")).into("t").build().query()),
                // An explicitly excluded key and a null property are still left out.
                () -> assertEquals("INSERT INTO t (name) VALUES (?)", PSC.insert(zero, N.asSet("id")).into("t").build().query()),
                () -> assertEquals("INSERT INTO t (id, name) VALUES (?, ?)", PSC.insert(conventional).into("t").build().query()),
                () -> assertEquals("INSERT INTO t (a, b, name) VALUES (?, ?, ?)", PSC.insert(compositeZero).into("t").build().query()),
                () -> assertEquals("INSERT INTO t (a, name) VALUES (?, ?)", PSC.insert(compositeNull).into("t").build().query()),
                () -> assertEquals("INSERT INTO zero_key_user (id, name) VALUES (?, ?)", PSC.insert(zero).into(ZeroKeyUser.class).build().query()), () -> {
                    final SP sp = PSC.batchInsert(N.asList(zero, zero)).into("t").build();
                    assertEquals("BEGIN BATCH INSERT INTO t (id, name) VALUES (?, ?); INSERT INTO t (id, name) VALUES (?, ?); APPLY BATCH", sp.query());
                    assertEquals(N.asList(0L, "n", 0L, "n"), sp.parameters());
                },
                () -> assertEquals(
                        "BEGIN BATCH INSERT INTO t (id, name) VALUES (:id_0, :name_0); INSERT INTO t (id, name) VALUES (:id_1, :name_1); APPLY BATCH",
                        NSC.batchInsert(N.asList(zero, five)).into("t").build().query()),
                () -> assertEquals("BEGIN BATCH INSERT INTO t (a, b, name) VALUES (?, ?, ?); APPLY BATCH",
                        PSC.batchInsert(N.asList(compositeZero)).into("t").build().query()),
                // A column null in every row (email, b) is still dropped.
                () -> assertEquals("BEGIN BATCH INSERT INTO t (a, name) VALUES (?, ?); INSERT INTO t (a, name) VALUES (?, ?); APPLY BATCH",
                        PSC.batchInsert(N.asList(compositeNull, null, compositeNull)).into("t").build().query()),
                () -> assertEquals("BEGIN BATCH INSERT INTO t (id, name) VALUES (?, ?); APPLY BATCH",
                        PSC.batchInsert(N.asList(five)).into("t").build().query()),
                () -> assertThrows(IllegalArgumentException.class, () -> PSC.batchInsert(N.asList(zero, conventional))),
                () -> assertThrows(IllegalArgumentException.class, () -> PSC.batchInsert(N.asList((Object) null, null))));
    }

    /** Entity mapped to reserved CQL keywords through quoted {@code @Table}/{@code @Column} names (the documented workaround). */
    @com.landawn.abacus.annotation.Table("\"order\"")
    public static class QuotedKeywordOrder {
        @com.landawn.abacus.annotation.Id
        private long id;
        @com.landawn.abacus.annotation.Column("\"desc\"")
        private String desc;
        private String group;

        public long getId() {
            return id;
        }

        public void setId(final long id) {
            this.id = id;
        }

        public String getDesc() {
            return desc;
        }

        public void setDesc(final String desc) {
            this.desc = desc;
        }

        public String getGroup() {
            return group;
        }

        public void setGroup(final String group) {
            this.group = group;
        }
    }

    /**
     * Pins the class Javadoc on reserved keywords: a quoted @Table/@Column or explicit name is rendered verbatim by every
     * naming policy, and a non-reserved keyword such as group stays unquoted (each output below parses with Cassandra 5.0's
     * CQL grammar). (2026-10-24 apply-cql: an unquoted reserved name is now quoted by the builder, see the apply-cql block.)
     */
    @Test
    public void test_sliceI_quotedNamesWorkAroundReservedKeywords() {
        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertEquals("SELECT id AS \"id\", \"desc\" AS \"desc\", group AS \"group\" FROM \"order\" WHERE \"desc\" = ?",
                        PSC.selectFrom(QuotedKeywordOrder.class).where(Filters.eq("desc", "x")).build().query()),
                () -> assertEquals("SELECT ID AS \"id\", \"desc\" AS \"desc\", GROUP AS \"group\" FROM \"order\" WHERE \"desc\" = ?",
                        PAC.selectFrom(QuotedKeywordOrder.class).where(Filters.eq("desc", "x")).build().query()),
                () -> assertEquals("INSERT INTO \"order\" (id, \"desc\", group) VALUES (?, ?, ?)", PSC.insertInto(QuotedKeywordOrder.class).build().query()),
                () -> assertEquals("UPDATE \"order\" SET \"desc\" = ?, group = ? WHERE id = ?",
                        PSC.update(QuotedKeywordOrder.class).where(Filters.eq("id", 1)).build().query()),
                () -> assertEquals("UPDATE \"order\" SET \"desc\" = ? WHERE id = ? IF \"desc\" = ?",
                        PSC.update("\"order\"").set("\"desc\"").where(Filters.eq("id", 1)).onlyIf(Filters.eq("\"desc\"", "y")).build().query()),
                // 2026-10-24 apply-cql: an unquoted reserved name is quoted by the builder (it used to stay bare: "SELECT desc FROM t").
                () -> assertEquals("SELECT \"desc\" FROM t", PSC.select("desc").from("t").build().query()));
    }

    // ---- 2026-10-24 apply-cql ----

    /** Entity whose class-derived table name ({@code order}) and two columns ({@code desc}, {@code @Column("token")}) are reserved CQL keywords. */
    public static class Order {
        @com.landawn.abacus.annotation.Id
        private long id;
        private String desc;
        private String group;
        @com.landawn.abacus.annotation.Column("token")
        private String tok;

        public long getId() {
            return id;
        }

        public void setId(final long id) {
            this.id = id;
        }

        public String getDesc() {
            return desc;
        }

        public void setDesc(final String desc) {
            this.desc = desc;
        }

        public String getGroup() {
            return group;
        }

        public void setGroup(final String group) {
            this.group = group;
        }

        public String getTok() {
            return tok;
        }

        public void setTok(final String tok) {
            this.tok = tok;
        }
    }

    /**
     * Regression: a reserved CQL keyword used as a column or table name (desc, order, select, from, limit, token, ...) was
     * rendered unquoted, which Cassandra rejects ("no viable alternative at input 'desc'"). The builder now renders it as the
     * quoted lower-case name, under every naming policy, for entity-derived and caller-supplied names, in every clause
     * (each quoted output below parses with Cassandra 5.0's CQL grammar; with named markers, after ParsedCql's rewrite).
     */
    @Test
    @SuppressWarnings("deprecation")
    public void test_applyCql_reservedKeywordNamesAreQuoted() {
        final Order order = new Order();
        order.setId(1);
        order.setDesc("d");
        order.setGroup("g");
        order.setTok("t");

        org.junit.jupiter.api.Assertions.assertAll(
                // Entity-derived names (property, @Column and class-derived table name). The parent keeps an entity's full
                // select list in a static cache (fullSelectPartsPool) shared with abacus-query's own SQL builders; for an entity
                // with a reserved-keyword column this builder neither reads nor fills it. So the SQL builder still renders (and
                // caches) the unquoted text, and a selectFrom after that cache was filled is still quoted (assertAll runs in order).
                () -> assertEquals("SELECT id AS \"id\", \"desc\" AS \"desc\", group AS \"group\", \"token\" AS \"tok\" FROM \"order\"",
                        PSC.selectFrom(Order.class).build().query()),
                () -> assertEquals("SELECT id AS \"id\", desc AS \"desc\", group AS \"group\", token AS \"tok\" FROM order",
                        com.landawn.abacus.query.Dsl.PSC.selectFrom(Order.class).build().query()),
                () -> assertEquals("SELECT id AS \"id\", \"desc\" AS \"desc\", group AS \"group\", \"token\" AS \"tok\" FROM \"order\"",
                        PSC.selectFrom(Order.class).build().query()),
                () -> assertEquals("SELECT ID AS \"id\", \"desc\" AS \"desc\", GROUP AS \"group\", \"token\" AS \"tok\" FROM \"order\" WHERE \"desc\" = ?",
                        PAC.selectFrom(Order.class).where(Filters.eq("desc", 1)).build().query()),
                () -> assertEquals("SELECT id, \"desc\", group, \"token\" FROM \"order\" WHERE \"token\" = ? ORDER BY \"desc\" DESC",
                        PSB.selectFrom(Order.class).where(Filters.eq("tok", 1)).orderBy("desc", com.landawn.abacus.query.SortDirection.DESC).build().query()),
                () -> assertEquals("INSERT INTO \"order\" (id, \"desc\", group, \"token\") VALUES (?, ?, ?, ?)", PSC.insertInto(Order.class).build().query()),
                () -> assertEquals("INSERT INTO \"order\" (id, \"desc\", group, \"token\") VALUES (?, ?, ?, ?)",
                        ParsedCql.parse(NSC.insert(order).into(Order.class).build().query()).parameterizedCql()),
                () -> assertEquals("UPDATE \"order\" SET \"desc\" = ?, group = ?, \"token\" = ? WHERE id = ? IF \"desc\" = ?",
                        PSC.update(Order.class).where(Filters.eq("id", 1)).onlyIf(Filters.eq("desc", "x")).build().query()),
                () -> assertEquals("DELETE FROM \"order\" WHERE \"desc\" = ?", PSC.deleteFrom(Order.class).where(Filters.eq("desc", 1)).build().query()),
                () -> assertEquals("DELETE \"desc\" FROM \"order\" WHERE id = ?",
                        PSC.delete("desc").from(Order.class).where(Filters.eq("id", 1)).build().query()),
                () -> assertEquals("BEGIN BATCH INSERT INTO \"order\" (id, \"desc\", group, \"token\") VALUES (1, 'd', 'g', 't'); APPLY BATCH",
                        SCCB.batchInsert(N.asList(order)).into(Order.class).build().query()),
                () -> assertEquals("SELECT count(*) FROM \"order\" WHERE \"desc\" IN (?, ?)",
                        PSC.count(Order.class).where(Filters.in("desc", N.asList(1, 2))).build().query()),
                // Caller-supplied names, in any case, and each reserved part of a dotted name; function calls and aliases unchanged.
                () -> assertEquals(
                        "SELECT \"desc\", token(id), a.\"desc\" AS \"a.desc\" FROM \"select\" WHERE \"limit\" IN (?, ?) AND token(id) > ? ORDER BY \"desc\" DESC",
                        PSC.select("desc", "token(id)", "a.desc")
                                .from("select")
                                .where(Filters.and(Filters.in("limit", N.asList(1, 2)), Filters.gt("token(id)", 3)))
                                .orderBy("desc", com.landawn.abacus.query.SortDirection.DESC)
                                .build()
                                .query()),
                () -> assertEquals("UPDATE \"order\" SET \"desc\" = ? WHERE ID = ? IF \"token\" = ?",
                        PAC.update("ORDER").set("DESC").where(Filters.eq("id", 1)).onlyIf(Filters.eq("Token", 2)).build().query()),
                () -> assertEquals("SELECT \"desc\" FROM ks.\"order\"", PSB.select("Desc").from("ks.order").build().query()),
                () -> assertEquals("INSERT INTO ks.\"order\" (\"desc\", group) VALUES (?, ?)",
                        ParsedCql.parse(NSC.insert("desc", "group").into("ks.\"order\"").build().query()).parameterizedCql()),
                () -> assertEquals("DELETE FROM \"from\" WHERE \"where\" = ?", PSC.deleteFrom("from").where(Filters.eq("where", 1)).build().query()),
                () -> assertEquals("SELECT id FROM t GROUP BY \"desc\" ORDER BY \"limit\"",
                        PLC.select("id").from("t").groupBy("desc").orderBy("limit").build().query()),
                () -> assertEquals("SELECT \"desc\" AS d FROM t", PSC.select("desc AS d").from("t").build().query()),
                () -> assertEquals("\"order\" = ? AND value = ?",
                        PSC.renderCondition(Filters.and(Filters.eq("order", 1), Filters.eq("value", 2)), null).build().query()),
                // Pins (unchanged): raw expression text, an alias, a function argument, non-reserved keywords, quoted names,
                // the literal keywords NULL/NaN, expressions, a Criteria's sort list (raw text) and names merely containing a keyword.
                () -> assertEquals("SELECT id FROM t WHERE desc = 1", PSC.select("id").from("t").where(Filters.expr("desc = 1")).build().query()),
                () -> assertEquals("SELECT writetime(desc), id AS desc FROM t", PSC.select("writetime(desc)", "id AS desc").from("t").build().query()),
                () -> assertEquals("SELECT group, value, key, ttl, \"desc\", \"Order\", NULL, nan FROM \"select\"",
                        PSC.select("group", "value", "key", "ttl", "\"desc\"", "\"Order\"", "NULL", "nan").from("\"select\"").build().query()),
                () -> assertEquals("SELECT desc + 1, count(*), * FROM t", PSC.select("desc + 1", "count(*)", "*").from("t").build().query()),
                () -> assertEquals(
                        "SELECT id AS \"id\", \"desc\" AS \"desc\", group AS \"group\", \"token\" AS \"tok\" FROM \"order\" WHERE \"desc\" = ? ORDER BY desc",
                        PSC.selectFrom(Order.class)
                                .append(com.landawn.abacus.query.condition.Criteria.builder().where(Filters.eq("desc", 1)).orderBy("desc").build())
                                .build()
                                .query()),
                () -> assertEquals("SELECT id FROM t WHERE descx = ? ORDER BY desc_x",
                        PSC.select("id").from("t").where(Filters.eq("descx", 1)).orderBy("desc_x").build().query()),
                () -> assertEquals("SELECT id, first_name AS \"firstName\", last_name AS \"lastName\" FROM account",
                        PSC.select("id", "firstName", "lastName").from("account").build().query()));
    }

    /**
     * Regression: NaN and infinite floating-point values inlined by a raw-CQL builder threw the parent's
     * "NaN/Infinity has no portable SQL literal" IAE, although CQL has the constants NaN, Infinity and -Infinity, which
     * Cassandra 5.0 accepts for double and float columns. Every floating-point holder whose text is one of them is rendered
     * the same way; finite and BigDecimal values, and bound (parameterized) values, are unchanged.
     */
    @Test
    @SuppressWarnings("deprecation")
    public void test_applyCql_nonFiniteFloatingPointRawLiterals() {
        final java.util.concurrent.atomic.DoubleAdder adder = new java.util.concurrent.atomic.DoubleAdder();
        adder.add(Double.POSITIVE_INFINITY);
        final java.util.concurrent.atomic.DoubleAccumulator accumulator = new java.util.concurrent.atomic.DoubleAccumulator(Double::sum, Double.NaN);

        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertEquals("INSERT INTO t (x) VALUES (NaN)", SCCB.insert(N.asMap("x", Double.NaN)).into("t").build().query()),
                () -> assertEquals("INSERT INTO t (x) VALUES (Infinity)", SCCB.insert(N.asMap("x", Double.POSITIVE_INFINITY)).into("t").build().query()),
                () -> assertEquals("INSERT INTO t (x) VALUES (-Infinity)", SCCB.insert(N.asMap("x", Double.NEGATIVE_INFINITY)).into("t").build().query()),
                () -> assertEquals("UPDATE t SET X = NaN, Y = -Infinity WHERE K = 1",
                        ACCB.update("t").set(N.asMap("x", Float.NaN, "y", Float.NEGATIVE_INFINITY)).where(Filters.eq("k", 1)).build().query()),
                () -> assertEquals("SELECT k FROM t WHERE x = Infinity",
                        LCCB.select("k").from("t").where(Filters.eq("x", Float.POSITIVE_INFINITY)).build().query()),
                () -> assertEquals("SELECT k FROM t WHERE x IN (NaN, -Infinity, 1)",
                        SCCB.select("k").from("t").where(Filters.in("x", N.asList(Double.NaN, Double.NEGATIVE_INFINITY, 1))).build().query()),
                () -> assertEquals("UPDATE t SET y = 1 WHERE k = 1 IF x = NaN",
                        SCCB.update("t").set(N.asMap("y", 1)).where(Filters.eq("k", 1)).onlyIf(Filters.eq("x", Double.NaN)).build().query()),
                () -> assertEquals("BEGIN BATCH INSERT INTO t (x) VALUES (NaN); INSERT INTO t (x) VALUES (2.5); APPLY BATCH",
                        SCCB.batchInsert(N.asList(N.asMap("x", Double.NaN), N.asMap("x", 2.5))).into("t").build().query()),
                // Floating-point holders, judged by their text (and doubleValue()).
                () -> assertEquals("INSERT INTO t (x) VALUES (NaN)",
                        SCCB.insert(N.asMap("x", com.landawn.abacus.util.MutableDouble.of(Double.NaN))).into("t").build().query()),
                () -> assertEquals("INSERT INTO t (x) VALUES (-Infinity)",
                        SCCB.insert(N.asMap("x", com.landawn.abacus.util.MutableFloat.of(Float.NEGATIVE_INFINITY))).into("t").build().query()),
                () -> assertEquals("INSERT INTO t (x) VALUES (Infinity)", SCCB.insert(N.asMap("x", adder)).into("t").build().query()),
                () -> assertEquals("INSERT INTO t (x) VALUES (NaN)", SCCB.insert(N.asMap("x", accumulator)).into("t").build().query()),
                () -> assertEquals("INSERT INTO t (x) VALUES (Infinity)",
                        SCCB.insert(N.asMap("x", new com.google.common.util.concurrent.AtomicDouble(Double.POSITIVE_INFINITY))).into("t").build().query()),
                // Pins: finite values, a BigDecimal beyond the double range, and bound values are unchanged.
                () -> assertEquals("INSERT INTO t (a, b, c) VALUES (1.5, -0.0, 1E+400)",
                        SCCB.insert(N.asMap("a", 1.5, "b", -0.0, "c", new java.math.BigDecimal("1e400"))).into("t").build().query()),
                () -> {
                    final SP sp = PSC.insert(N.asMap("x", Double.NaN)).into("t").build();
                    assertEquals("INSERT INTO t (x) VALUES (?)", sp.query());
                    assertTrue(Double.isNaN((Double) sp.parameters().get(0)));
                });
    }

    /** Entity whose only reserved-keyword column is the unquoted field of a quoted UDT column ({@code "Addr".desc}). */
    public static class QuotedUdtField {
        @com.landawn.abacus.annotation.Id
        private long id;
        @com.landawn.abacus.annotation.Column("\"Addr\".desc")
        private String d;

        public long getId() {
            return id;
        }

        public void setId(final long id) {
            this.id = id;
        }

        public String getD() {
            return d;
        }

        public void setD(final String d) {
            this.d = d;
        }
    }

    /**
     * Regression (verify of apply-cql): a column reference whose dotted chain has a quoted part kept a reserved unquoted part
     * bare ({@code order."x"}, {@code "Order".desc}), although a table reference of the same shape was quoted
     * ({@code "order"."x"}) and the class Javadoc quotes "each part of a dotted name". Quoted parts stay as written.
     */
    @Test
    public void test_vapplyCql_reservedPartNextToQuotedPartIsQuoted() {
        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertEquals("SELECT id FROM t WHERE \"order\".\"x\" = ? AND \"Order\".\"desc\" = ?",
                        PSB.select("id").from("t").where(Filters.and(Filters.eq("order.\"x\"", 1), Filters.eq("\"Order\".desc", 2))).build().query()),
                () -> assertEquals("UPDATE t SET \"order\".\"x\" = ? WHERE \"a\"\"b\".\"limit\" = ?",
                        PSC.update("t").set("order.\"x\"").where(Filters.eq("\"a\"\"b\".limit", 1)).build().query()),
                // With the implicit alias (the item text, its quotes doubled) of a non-NO_CHANGE naming policy.
                () -> assertEquals("SELECT \"Order\".\"desc\" AS \"\"\"Order\"\".desc\" FROM t", PSC.select("\"Order\".desc").from("t").build().query()),
                // An entity column: selectFrom's full select list must not fall back to the parent's unquoted shared-cache text.
                () -> assertEquals("SELECT id AS \"id\", \"Addr\".\"desc\" AS \"d\" FROM quoted_udt_field WHERE \"Addr\".\"desc\" = ?",
                        PSC.selectFrom(QuotedUdtField.class).where(Filters.eq("d", 1)).build().query()),
                // Pins (unchanged): quoted parts, also a quoted reserved name, and non-reserved unquoted parts.
                () -> assertEquals("SELECT id FROM t WHERE \"desc\".x = ? AND \"Order\".value = ? AND \"order\" = ?",
                        PSB.select("id")
                                .from("t")
                                .where(Filters.and(Filters.eq("\"desc\".x", 1), Filters.eq("\"Order\".value", 2), Filters.eq("\"order\"", 3)))
                                .build()
                                .query()));
    }

    // ---- 2026-10-24 r2 sliceI ----

    /**
     * Regression: a raw (SCCB/ACCB/LCCB) builder inlined a LocalTime as LocalTime.toString(), which leaves out zero seconds
     * ('09:00', midnight '00:00'); Cassandra 5.0's time literal requires hh:mm:ss[.fffffffff] and rejected it ("Unable to
     * coerce '09:00' to a formatted time (long)"). Every inlining path (SET, WHERE, IN, INSERT, batch INSERT) now keeps the
     * seconds; times that already had seconds or a fraction, java.sql.Time and bound values are unchanged.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void test_r2sliceI_rawLocalTimeLiteralAlwaysCarriesSeconds() {
        org.junit.jupiter.api.Assertions
                .assertAll(
                        () -> assertEquals("UPDATE t SET opens_at = '09:00:00' WHERE id = 1",
                                SCCB.update("t").set("opensAt", java.time.LocalTime.of(9, 0)).where(Filters.eq("id", 1)).build().query()),
                        () -> assertEquals("SELECT id FROM t WHERE at IN ('00:00:00', '23:59:00')",
                                SCCB.select("id")
                                        .from("t")
                                        .where(Filters.in("at", N.asList(java.time.LocalTime.MIDNIGHT, java.time.LocalTime.of(23, 59))))
                                        .build()
                                        .query()),
                        () -> assertEquals("INSERT INTO t (id, at) VALUES (1, '12:00:00')",
                                SCCB.insert(N.asMap("id", 1, "at", java.time.LocalTime.NOON)).into("t").build().query()),
                        () -> assertEquals("BEGIN BATCH INSERT INTO t (at) VALUES ('08:00:00'); APPLY BATCH",
                                SCCB.batchInsert(N.asList(N.asMap("at", java.time.LocalTime.of(8, 0)))).into("t").build().query()),
                        () -> assertEquals("UPDATE t SET AT = '07:15:00' WHERE ID = 1",
                                ACCB.update("t").set("at", java.time.LocalTime.of(7, 15)).where(Filters.eq("id", 1)).build().query()),
                        // Pins (unchanged): seconds or a fraction already present, java.sql.Time, and a bound LocalTime.
                        () -> assertEquals("INSERT INTO t (a, b, c, d) VALUES ('09:30:05', '09:30:00.000001', '09:00:00.500', '12:00:00')",
                                SCCB.insert(N.asMap("a", java.time.LocalTime.of(9, 30, 5), "b", java.time.LocalTime.of(9, 30, 0, 1000), "c",
                                        java.time.LocalTime.of(9, 0, 0, 500_000_000), "d", java.sql.Time.valueOf("12:00:00"))).into("t").build().query()),
                        () -> {
                            final SP sp = PSC.update("t").set("at", java.time.LocalTime.of(9, 0)).where(Filters.eq("id", 1)).build();
                            assertEquals("UPDATE t SET at = ? WHERE id = ?", sp.query());
                            assertEquals(N.asList(java.time.LocalTime.of(9, 0), 1), sp.parameters());
                        });
    }

    /** An {@code @Entity}-annotated Map subclass, which {@code Beans.isBeanClass} accepts like an entity bean. */
    @com.landawn.abacus.annotation.Entity
    @com.landawn.abacus.annotation.Table("map_rows")
    public static class R2sliceIMapEntity extends java.util.LinkedHashMap<String, Object> {
        private static final long serialVersionUID = 1L;

        static R2sliceIMapEntity of(final Object id, final Object name) {
            final R2sliceIMapEntity row = new R2sliceIMapEntity();
            row.put("id", id);
            row.put("name", name);
            return row;
        }
    }

    /**
     * Pin (cross-cutting alert, unchanged): every entity-taking entry point (insert, insert with exclusions, batchInsert,
     * set(entity), set(entity, excluded)) reads an {@code @Entity}-annotated Map subclass as a map of its entries, keeping a
     * zero key, instead of as a bean (whose only "property" would be the Map's {@code isEmpty} getter).
     */
    @Test
    public void test_r2sliceI_entityAnnotatedMapSubclassIsReadAsItsEntries() {
        org.junit.jupiter.api.Assertions.assertAll(() -> {
            final SP sp = PSC.insert(R2sliceIMapEntity.of(0, "a")).into("t").build();
            assertEquals("INSERT INTO t (id, name) VALUES (?, ?)", sp.query());
            assertEquals(N.asList(0, "a"), sp.parameters());
        }, () -> {
            final SP sp = PSC.insert(R2sliceIMapEntity.of(0, "a"), N.asSet("name")).into(R2sliceIMapEntity.class).build();
            assertEquals("INSERT INTO map_rows (id) VALUES (?)", sp.query());
            assertEquals(N.asList(0), sp.parameters());
        }, () -> {
            final SP sp = PSC.batchInsert(N.asList(R2sliceIMapEntity.of(0, "a"), R2sliceIMapEntity.of(1, "b"))).into("t").build();
            assertEquals("BEGIN BATCH INSERT INTO t (id, name) VALUES (?, ?); INSERT INTO t (id, name) VALUES (?, ?); APPLY BATCH", sp.query());
            assertEquals(N.asList(0, "a", 1, "b"), sp.parameters());
        }, () -> {
            final SP sp = PSC.update("t").set(R2sliceIMapEntity.of(0, "a")).where(Filters.eq("id", 0)).build();
            assertEquals("UPDATE t SET id = ?, name = ? WHERE id = ?", sp.query());
            assertEquals(N.asList(0, "a", 0), sp.parameters());
        }, () -> {
            final SP sp = PSC.update("t").set(R2sliceIMapEntity.of(0, "a"), N.asSet("id")).where(Filters.eq("id", 0)).build();
            assertEquals("UPDATE t SET name = ? WHERE id = ?", sp.query());
            assertEquals(N.asList("a", 0), sp.parameters());
        });
    }

    /**
     * Pin for the class Javadoc: the inherited groupByAsc(...)/groupByDesc(...) shortcuts delegate to the GROUP BY overloads
     * that take a SortDirection, so they fail with the same UnsupportedOperationException (CQL GROUP BY has no ASC/DESC),
     * and a closed builder still reports IllegalStateException first.
     */
    @Test
    public void test_r2sliceI_groupByAscAndDescShortcutsAreUnsupported() {
        final com.landawn.abacus.da.cassandra.CqlBuilder closed = PSC.select("a").from("t");
        closed.build();

        org.junit.jupiter.api.Assertions.assertAll(() -> assertThrows(UnsupportedOperationException.class, () -> PSC.select("a").from("t").groupByAsc("a")),
                () -> assertThrows(UnsupportedOperationException.class, () -> PSC.select("a").from("t").groupByAsc("a", "b")),
                () -> assertThrows(UnsupportedOperationException.class, () -> PSC.select("a").from("t").groupByAsc(N.asList("a"))),
                () -> assertThrows(UnsupportedOperationException.class, () -> PSC.select("a").from("t").groupByDesc("a")),
                () -> assertThrows(UnsupportedOperationException.class, () -> PSC.select("a").from("t").groupByDesc("a", "b")),
                () -> assertThrows(UnsupportedOperationException.class, () -> PSC.select("a").from("t").groupByDesc(N.asList("a"))),
                () -> assertThrows(IllegalStateException.class, () -> closed.groupByAsc("a")),
                () -> assertThrows(IllegalStateException.class, () -> closed.groupByDesc("a", "b")));
    }

    /**
     * Pin for the class Javadoc: a named-parameter DSL names the marker of a reserved-keyword column after it ({@code :desc}),
     * which Cassandra's parser rejects; ParsedCql, through which the executors prepare CQL text, turns it into {@code ?}.
     */
    @Test
    public void test_r2sliceI_namedMarkerOfReservedKeywordColumnIsRewrittenByParsedCql() {
        final String cql = NSC.update("t").set("desc").where(Filters.eq("order", 1)).build().query();

        assertEquals("UPDATE t SET \"desc\" = :desc WHERE \"order\" = :order", cql);
        assertEquals("UPDATE t SET \"desc\" = ? WHERE \"order\" = ?", ParsedCql.parse(cql).parameterizedCql());
        assertEquals("UPDATE t SET \"desc\" = ? WHERE \"order\" = ?", PSC.update("t").set("desc").where(Filters.eq("order", 1)).build().query());
    }

    // ---- 2026-10-27 sliceI ----

    /** An entity with an {@code @Id} key, so set(...) adds the key to the caller's exclusions. */
    public static class SliceI2KeyedUser {
        @com.landawn.abacus.annotation.Id
        private long id;
        private String firstName;
        private String lastName;

        public long getId() {
            return id;
        }

        public void setId(final long id) {
            this.id = id;
        }

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(final String firstName) {
            this.firstName = firstName;
        }

        public String getLastName() {
            return lastName;
        }

        public void setLastName(final String lastName) {
            this.lastName = lastName;
        }
    }

    /**
     * An excluded-name set with its own comparator (a case-insensitive TreeSet holding "FIRSTNAME") excludes firstName on
     * every path, as abacus-query asks the set itself (and its 4.9.6 Dsl keeps such a set's comparator). set(Class, excluded)
     * and set(bean, excluded) of a class with a key copied the set into a HashSet (case-sensitive), and insert(map, excluded)
     * and set(map, excluded) looked each excluded name up in the map; all of them left firstName in.
     */
    @Test
    public void test_sliceI2_sortedExclusionSetKeepsItsComparator() {
        final java.util.Set<String> caseInsensitive = new java.util.TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        caseInsensitive.add("FIRSTNAME");

        final SliceI2KeyedUser user = new SliceI2KeyedUser();
        user.setId(1);
        user.setFirstName("f");
        user.setLastName("l");

        final java.util.Map<String, Object> row = new java.util.LinkedHashMap<>();
        row.put("id", 1);
        row.put("firstName", "f");
        row.put("lastName", "l");

        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals("UPDATE t SET last_name = ? WHERE id = ?",
                PSC.update("t").set(SliceI2KeyedUser.class, caseInsensitive).where(Filters.eq("id", 1)).build().query()), () -> {
                    final SP sp = PSC.update("t").set(user, caseInsensitive).where(Filters.eq("id", 1)).build();
                    assertEquals("UPDATE t SET last_name = ? WHERE id = ?", sp.query());
                    assertEquals(N.asList("l", 1), sp.parameters());
                }, () -> {
                    final SP sp = PSC.insert(row, caseInsensitive).into("t").build();
                    assertEquals("INSERT INTO t (id, last_name) VALUES (?, ?)", sp.query());
                    assertEquals(N.asList(1, "l"), sp.parameters());
                },
                // Nothing left after the exclusions: rejected like any other all-excluded map.
                () -> assertThrows(IllegalArgumentException.class, () -> PSC.insert(N.asMap("firstName", "f"), caseInsensitive)),
                // Pins (unchanged): the bean and class paths already asked the set; the caller's set and map are not modified;
                // a non-String map key is still rejected before the set is asked about it.
                () -> assertEquals("INSERT INTO t (id, last_name) VALUES (?, ?)", PSC.insert(user, caseInsensitive).into("t").build().query()),
                () -> assertEquals("UPDATE slice_i2_keyed_user SET last_name = ? WHERE id = ?",
                        PSC.update(SliceI2KeyedUser.class, caseInsensitive).where(Filters.eq("id", 1)).build().query()),
                () -> assertEquals(N.asList("FIRSTNAME"), new java.util.ArrayList<>(caseInsensitive)),
                () -> assertEquals(N.asList("id", "firstName", "lastName"), new java.util.ArrayList<>(row.keySet())), () -> {
                    @SuppressWarnings({ "rawtypes", "unchecked" })
                    final java.util.Map<String, Object> badKey = new java.util.LinkedHashMap();
                    ((java.util.Map<Object, Object>) (java.util.Map<?, ?>) badKey).put(7, "x");
                    badKey.put("firstName", "f");
                    assertThrows(IllegalArgumentException.class, () -> PSC.insert(badKey, caseInsensitive));
                    // set(map, excluded) likewise rejects the bad key itself, not with the comparator's ClassCastException.
                    assertThrows(IllegalArgumentException.class, () -> PSC.update("t").set(badKey, caseInsensitive));
                }, () -> {
                    final SP sp = PSC.update("t").set(row, caseInsensitive).where(Filters.eq("id", 1)).build();
                    assertEquals("UPDATE t SET id = ?, last_name = ? WHERE id = ?", sp.query());
                    assertEquals(N.asList(1, "l", 1), sp.parameters());
                }, () -> {
                    final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                            () -> PSC.update("t").set(N.asMap("firstName", "f"), caseInsensitive));
                    assertEquals("No properties remain after exclusions are applied", e.getMessage());
                }, () -> {
                    // Pins (unchanged): the parent's state checks still come first, and a null key next to a Set.of(...)
                    // exclusion is the parent's IllegalArgumentException, not a NullPointerException from Set.of's contains.
                    final com.landawn.abacus.da.cassandra.CqlBuilder closed = PSC.update("t").set("a").where(Filters.eq("id", 1));
                    closed.build();
                    assertThrows(IllegalStateException.class, () -> closed.set(N.asMap("firstName", "f"), caseInsensitive));

                    final java.util.Map<String, Object> nullKey = new java.util.LinkedHashMap<>();
                    nullKey.put(null, 1);
                    nullKey.put("a", 2);
                    assertThrows(IllegalArgumentException.class, () -> PSC.update("t").set(nullKey, java.util.Set.of("a")));
                });
    }

    /**
     * onlyIf(Filters.where(...)) rendered "... IF  WHERE b = ?", which Cassandra rejects; an IF clause takes relations only. The
     * rejection leaves the builder unchanged, so a retry with the relation itself renders normally.
     */
    @Test
    public void test_sliceI2_whereClauseAsIfConditionIsRejected() {
        org.junit.jupiter.api.Assertions.assertAll(() -> {
            final com.landawn.abacus.da.cassandra.CqlBuilder builder = PSC.update("t").set("a").where(Filters.eq("k", 1));
            assertThrows(IllegalArgumentException.class, () -> builder.onlyIf(Filters.where(Filters.eq("b", 2))));

            final SP sp = builder.onlyIf(Filters.eq("b", 2)).build();
            assertEquals("UPDATE t SET a = ? WHERE k = ? IF b = ?", sp.query());
            assertEquals(N.asList(1, 2), sp.parameters());
        }, () -> {
            final com.landawn.abacus.da.cassandra.CqlBuilder builder = SCCB.deleteFrom("t").where(Filters.eq("k", 1));
            assertThrows(IllegalArgumentException.class, () -> builder.onlyIf(Filters.where("b = 1")));
            assertEquals("DELETE FROM t WHERE k = 1 IF EXISTS", builder.ifExists().build().query());
        },
                // Pin (unchanged): an appended Where clause still renders the statement's WHERE clause.
                () -> assertEquals("SELECT a FROM t WHERE c = ?", PSC.select("a").from("t").append(Filters.where(Filters.eq("c", 2))).build().query()));
    }

    /** A bean used as a sub-entity property; its class name, not the property name, prefixes the columns. */
    public static class SliceI2Addr {
        private String street;
        private String desc;

        public String getStreet() {
            return street;
        }

        public void setStreet(final String street) {
            this.street = street;
        }

        public String getDesc() {
            return desc;
        }

        public void setDesc(final String desc) {
            this.desc = desc;
        }
    }

    /** {@code home} is a sub-entity property; {@code work} is the same bean type mapped as a plain column. */
    public static class SliceI2Person {
        @com.landawn.abacus.annotation.Id
        private long id;
        private SliceI2Addr home;
        @com.landawn.abacus.annotation.Column("work")
        private SliceI2Addr work;

        public long getId() {
            return id;
        }

        public void setId(final long id) {
            this.id = id;
        }

        public SliceI2Addr getHome() {
            return home;
        }

        public void setHome(final SliceI2Addr home) {
            this.home = home;
        }

        public SliceI2Addr getWork() {
            return work;
        }

        public void setWork(final SliceI2Addr work) {
            this.work = work;
        }
    }

    /**
     * Pin for the class Javadoc (doc corrected this round): a sub-entity property's columns are prefixed with the sub-entity
     * CLASS's table name and only the SELECT alias carries the property name, in every clause; the bare-name expansion is not
     * keyword-quoted while select(entityClass, true) is; an {@code @Column} bean property is a plain column (UDT field form).
     */
    @Test
    public void test_sliceI2_subEntityColumnsArePrefixedWithTheClassTableName() {
        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertEquals("SELECT slice_i2_addr.street AS \"home.street\", slice_i2_addr.desc AS \"home.desc\" FROM t",
                        PSC.select("home").from("t", SliceI2Person.class).build().query()),
                () -> assertEquals(
                        "SELECT id AS \"id\", work AS \"work\", slice_i2_addr.street AS \"home.street\", slice_i2_addr.\"desc\" AS \"home.desc\" FROM t",
                        PSC.select(SliceI2Person.class, true).from("t").build().query()),
                () -> assertEquals("SELECT id AS \"id\" FROM t WHERE slice_i2_addr.street = ?",
                        PSC.select("id").from("t", SliceI2Person.class).where(Filters.eq("home.street", 1)).build().query()),
                () -> assertEquals("UPDATE t SET slice_i2_addr.street = ? WHERE id = ?",
                        PSC.update("t", SliceI2Person.class).set("home.street").where(Filters.eq("id", 1)).build().query()),
                () -> assertEquals("INSERT INTO t (slice_i2_addr.street) VALUES (?)", PSC.insert("home.street").into("t", SliceI2Person.class).build().query()),
                () -> assertEquals("DELETE slice_i2_addr.street FROM t WHERE id = ?",
                        PSC.delete("home.street").from("t", SliceI2Person.class).where(Filters.eq("id", 1)).build().query()),
                () -> assertEquals("UPDATE t SET work.street = ? WHERE id = ?",
                        PSC.update("t", SliceI2Person.class).set("work.street").where(Filters.eq("id", 1)).build().query()),
                () -> assertEquals("SELECT home.street AS \"home.street\" FROM t", PSC.select("home.street").from("t").build().query()));
    }

    /**
     * Second-pass review (r2C): set(Class, excluded) and set(bean, excluded) of a class with key properties copied the
     * exclusion set (into a TreeSet only when it was itself a SortedSet), so a case-insensitive set that is not a SortedSet (an
     * unmodifiable, synchronized or abacus ImmutableSet view of a case-insensitive TreeSet, or a set backed by a
     * case-insensitive TreeMap) left firstName in, while a class without keys, insert(map/bean) and set(map) excluded it.
     */
    @Test
    public void test_r2C_exclusionSetWithItsOwnMembershipRuleIsAskedForAKeyedClass() {
        final java.util.function.Supplier<java.util.TreeSet<String>> ci = () -> {
            final java.util.TreeSet<String> set = new java.util.TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            set.add("FIRSTNAME");
            return set;
        };
        final java.util.Set<String> backedByMap = java.util.Collections.newSetFromMap(new java.util.TreeMap<>(String.CASE_INSENSITIVE_ORDER));
        backedByMap.add("FIRSTNAME");
        final java.util.List<java.util.Set<String>> views = N.asList(java.util.Collections.unmodifiableSet(ci.get()),
                java.util.Collections.synchronizedSet(ci.get()), com.landawn.abacus.util.ImmutableSet.wrap(ci.get()), backedByMap);

        final SliceI2KeyedUser user = new SliceI2KeyedUser();
        user.setId(1);
        user.setFirstName("f");
        user.setLastName("l");

        final java.util.List<org.junit.jupiter.api.function.Executable> checks = new java.util.ArrayList<>();

        for (final java.util.Set<String> view : views) {
            checks.add(() -> assertEquals("UPDATE t SET last_name = ? WHERE id = ?",
                    PSC.update("t").set(SliceI2KeyedUser.class, view).where(Filters.eq("id", 1)).build().query(), view.getClass().getName()));
            checks.add(() -> {
                final SP sp = PSC.update("t").set(user, view).where(Filters.eq("id", 1)).build();
                assertEquals("UPDATE t SET last_name = ? WHERE id = ?", sp.query(), view.getClass().getName());
                assertEquals(N.asList("l", 1), sp.parameters());
            });
            checks.add(() -> assertEquals(N.asList("FIRSTNAME"), new java.util.ArrayList<>(view), view.getClass().getName()));
        }

        // Unchanged: a case-sensitive set still matches exactly; keys are always left out; no name left -> the parent's IAE;
        // a null element is not asked about; the parent's state checks still come first.
        checks.add(() -> assertEquals("UPDATE t SET first_name = ?, last_name = ? WHERE id = ?",
                PSC.update("t").set(SliceI2KeyedUser.class, new java.util.HashSet<>(N.asList("FIRSTNAME"))).where(Filters.eq("id", 1)).build().query()));
        checks.add(() -> assertEquals("UPDATE t SET first_name = ?, last_name = ? WHERE id = ?",
                PSC.update("t").set(SliceI2KeyedUser.class, (java.util.Set<String>) null).where(Filters.eq("id", 1)).build().query()));
        checks.add(() -> assertEquals("UPDATE t SET last_name = ? WHERE id = ?",
                PSC.update("t").set(user, java.util.Set.of("firstName", "nope")).where(Filters.eq("id", 1)).build().query()));
        checks.add(() -> {
            final java.util.Set<String> withNull = new java.util.HashSet<>();
            withNull.add(null);
            withNull.add("lastName");
            assertEquals("UPDATE t SET first_name = ? WHERE id = ?",
                    PSC.update("t").set(SliceI2KeyedUser.class, withNull).where(Filters.eq("id", 1)).build().query());
        });
        checks.add(() -> assertThrows(IllegalArgumentException.class,
                () -> PSC.update("t").set(SliceI2KeyedUser.class, java.util.Set.of("firstName", "lastName"))));
        checks.add(() -> {
            final java.util.Set<String> all = java.util.Collections.unmodifiableSet(ci.get());
            final java.util.TreeSet<String> both = ci.get();
            both.add("LASTNAME");
            assertThrows(IllegalArgumentException.class, () -> PSC.update("t").set(user, java.util.Collections.unmodifiableSet(both)));
            final com.landawn.abacus.da.cassandra.CqlBuilder afterWhere = PSC.update("t").set("a").where(Filters.eq("id", 1));
            assertThrows(IllegalStateException.class, () -> afterWhere.set(SliceI2KeyedUser.class, all));
            assertThrows(IllegalStateException.class, () -> afterWhere.set(user, all));
        });

        org.junit.jupiter.api.Assertions.assertAll(checks);
    }

    /**
     * Pin (r2C, unchanged from HEAD): set(map, excluded) still hands the parent the excluded names themselves, next to the keys
     * the set matches, so a {@code null} or non-String key equal to an excluded element is still dropped before the parent
     * validates the keys (insert(map, excluded) validates the keys first, as the parent always did there).
     */
    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void test_r2C_setMapStillDropsKeysEqualToAnExcludedElement() {
        final java.util.Map<String, Object> nullKey = new java.util.LinkedHashMap<>();
        nullKey.put(null, 1);
        nullKey.put("a", 2);
        nullKey.put("b", 3);
        final java.util.Set<String> nullAndB = new java.util.HashSet<>();
        nullAndB.add(null);
        nullAndB.add("b");
        final java.util.Set<String> onlyNull = new java.util.HashSet<>();
        onlyNull.add(null);

        final java.util.Map<String, Object> intKey = new java.util.LinkedHashMap();
        ((java.util.Map) intKey).put(7, "x");
        intKey.put("a", 2);
        final java.util.Set<String> seven = new java.util.HashSet(N.asList(7));

        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertEquals("UPDATE t SET a = ? WHERE id = ?", PSC.update("t").set(nullKey, nullAndB).where(Filters.eq("id", 1)).build().query()),
                () -> assertEquals("UPDATE t SET a = ?, b = ? WHERE id = ?", PSC.update("t").set(nullKey, onlyNull).where(Filters.eq("id", 1)).build().query()),
                () -> assertEquals("UPDATE t SET a = ? WHERE id = ?", PSC.update("t").set(intKey, seven).where(Filters.eq("id", 1)).build().query()),
                () -> assertThrows(IllegalArgumentException.class, () -> PSC.insert(nullKey, nullAndB)), () -> assertEquals(3, nullKey.size()),
                () -> assertEquals(2, nullAndB.size()));
    }

    /**
     * fixC: set(Class|bean, excluded) of a keyed class, set(map, excluded) and insert(map, excluded) now ask the caller's set
     * about each name (see above). A set whose contains(String) throws ClassCastException or NullPointerException (a
     * TreeSet&lt;Integer&gt; passed as a raw Set, a comparator that cannot compare a String) failed those calls, although HEAD
     * and the parent's own paths (N.excludeAll's safeContains) treat such a probe as "not excluded". It excludes nothing again.
     * The column-delete factory delete(Class, excluded) failed the same way (on HEAD too) and now tolerates it as well.
     */
    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void test_fixC_exclusionSetThatRejectsAStringProbeExcludesNothing() {
        final java.util.Set<String> intTree = (java.util.Set) new java.util.TreeSet<>(N.asList(7));
        final java.util.Set<Object> npeTree = new java.util.TreeSet<>(
                java.util.Comparator.comparing((final Object o) -> o instanceof Integer ? (Integer) o : null));
        npeTree.add(7);
        final java.util.Map<String, java.util.Set<String>> rejectingSets = new java.util.LinkedHashMap<>();
        rejectingSets.put("TreeSet<Integer>", intTree);
        rejectingSets.put("NPE comparator", (java.util.Set) npeTree);

        final SliceI2KeyedUser user = new SliceI2KeyedUser();
        user.setId(1);
        user.setFirstName("f");
        user.setLastName("l");

        final java.util.Map<String, Object> row = new java.util.LinkedHashMap<>();
        row.put("id", 1);
        row.put("firstName", "f");
        row.put("lastName", "l");

        final java.util.Map<String, Object> intKey = new java.util.LinkedHashMap();
        ((java.util.Map) intKey).put(7, "x");
        intKey.put("a", 2);

        final java.util.List<org.junit.jupiter.api.function.Executable> checks = new java.util.ArrayList<>();

        for (final java.util.Map.Entry<String, java.util.Set<String>> e : rejectingSets.entrySet()) {
            final String name = e.getKey();
            final java.util.Set<String> rejecting = e.getValue();

            checks.add(() -> assertEquals("UPDATE t SET first_name = ?, last_name = ? WHERE id = ?",
                    PSC.update("t").set(SliceI2KeyedUser.class, rejecting).where(Filters.eq("id", 1)).build().query(), name));
            checks.add(() -> {
                final SP sp = PSC.update("t").set(user, rejecting).where(Filters.eq("id", 1)).build();
                assertEquals("UPDATE t SET first_name = ?, last_name = ? WHERE id = ?", sp.query(), name);
                assertEquals(N.asList("f", "l", 1), sp.parameters(), name);
            });
            checks.add(() -> {
                final SP sp = PSC.update("t").set(row, rejecting).where(Filters.eq("id", 1)).build();
                assertEquals("UPDATE t SET id = ?, first_name = ?, last_name = ? WHERE id = ?", sp.query(), name);
                assertEquals(N.asList(1, "f", "l", 1), sp.parameters(), name);
            });
            checks.add(() -> {
                final SP sp = PSC.insert(row, rejecting).into("t").build();
                assertEquals("INSERT INTO t (id, first_name, last_name) VALUES (?, ?, ?)", sp.query(), name);
                assertEquals(N.asList(1, "f", "l"), sp.parameters(), name);
            });
            checks.add(() -> assertEquals("DELETE first_name, last_name FROM t WHERE id = ?",
                    PSC.delete(SliceI2KeyedUser.class, rejecting).from("t").where(Filters.eq("id", 1)).build().query(), name));
            // As on HEAD, set(map) still drops a non-String key merely equal to an element (the parent's map lookup), after
            // the String key "a" is asked about.
            checks.add(() -> assertEquals("UPDATE t SET a = ? WHERE id = ?", PSC.update("t").set(intKey, rejecting).where(Filters.eq("id", 1)).build().query(),
                    name));
            // Pins (unchanged): the parent's paths already ignored such a probe; insert(map) still rejects a non-String key
            // before any probe; neither the set nor the map is modified.
            checks.add(() -> assertEquals("UPDATE t SET street = ?, \"desc\" = ? WHERE id = ?",
                    PSC.update("t").set(SliceI2Addr.class, rejecting).where(Filters.eq("id", 1)).build().query(), name));
            checks.add(() -> assertEquals("UPDATE slice_i2_keyed_user SET first_name = ?, last_name = ? WHERE id = ?",
                    PSC.update(SliceI2KeyedUser.class, rejecting).where(Filters.eq("id", 1)).build().query(), name));
            checks.add(() -> assertEquals("INSERT INTO t (id, first_name, last_name) VALUES (?, ?, ?)", PSC.insert(user, rejecting).into("t").build().query(),
                    name));
            checks.add(() -> assertThrows(IllegalArgumentException.class, () -> PSC.insert(intKey, rejecting), name));
            checks.add(() -> assertEquals(N.asList(7), new java.util.ArrayList<>(rejecting), name));
            checks.add(() -> assertEquals(N.asList("id", "firstName", "lastName"), new java.util.ArrayList<>(row.keySet()), name));
        }

        // Pins (unchanged) for delete(Class, excluded) with well-typed sets: the set itself is still asked about each column (a
        // case-insensitive TreeSet, also one larger than the column list), keys are never deleted, nothing left -> IAE.
        final java.util.Set<String> caseInsensitive = new java.util.TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        caseInsensitive.add("FIRSTNAME");
        final java.util.Set<String> largerCaseInsensitive = new java.util.TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        largerCaseInsensitive.addAll(N.asList("LASTNAME", "a", "b", "c", "d", "e", "ID"));
        checks.add(() -> assertEquals("DELETE last_name FROM t WHERE id = ?",
                PSC.delete(SliceI2KeyedUser.class, caseInsensitive).from("t").where(Filters.eq("id", 1)).build().query()));
        checks.add(() -> assertEquals("DELETE first_name FROM t WHERE id = ?",
                PSC.delete(SliceI2KeyedUser.class, largerCaseInsensitive).from("t").where(Filters.eq("id", 1)).build().query()));
        checks.add(() -> assertEquals("DELETE first_name FROM t WHERE id = ?",
                PSC.delete(SliceI2KeyedUser.class, new java.util.HashSet<>(N.asList("lastName", "x1", "x2", "x3", "x4", "FIRSTNAME")))
                        .from("t")
                        .where(Filters.eq("id", 1))
                        .build()
                        .query()));
        checks.add(() -> assertThrows(IllegalArgumentException.class, () -> PSC.delete(SliceI2KeyedUser.class, java.util.Set.of("firstName", "lastName"))));

        org.junit.jupiter.api.Assertions.assertAll(checks);
    }
}
