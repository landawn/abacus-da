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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.da.entity.Account;
import com.landawn.abacus.da.entity.Users;
import com.landawn.abacus.query.AbstractQueryBuilder.SP;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Strings;

public class CqlBuilderInsertTest extends TestBase {

    // ---- 2026-10-02 verifyQB ----

    private static final java.util.Set<String> VERIFY_QB_ALL_BUT_NAMES = N.asSet("gui", "emailAddress", "middleName", "birthDate", "status", "lastUpdateTime",
            "createTime", "contact", "devices");

    // Row and entity mapping

    /**
     * abacus-query 4.9.3 renders {@code into(Class)} / {@code into(String, Class)} directly instead of delegating
     * to {@code into(String)}, which bypassed the CQL batch rendering and emitted SQL's multi-row
     * {@code VALUES (?, ?), (?, ?)} form that Cassandra rejects.
     */
    @Test
    public void test_intoEntityClass_batchInsert_rendersCqlBatch() {
        final String expected = "BEGIN BATCH INSERT INTO account (first_name, last_name) VALUES (?, ?); "
                + "INSERT INTO account (first_name, last_name) VALUES (?, ?); APPLY BATCH";

        final SP byClass = PSC.batchInsert(N.asList(N.asMap("firstName", "a", "lastName", "b"), N.asMap("firstName", "c", "lastName", "d")))
                .into(Account.class)
                .build();
        assertEquals(expected, byClass.query());
        assertEquals(N.asList("a", "b", "c", "d"), byClass.parameters());

        final SP byNameAndClass = PSC.batchInsert(N.asList(N.asMap("firstName", "a", "lastName", "b"), N.asMap("firstName", "c", "lastName", "d")))
                .into("account", Account.class)
                .build();
        assertEquals(expected, byNameAndClass.query());
        assertEquals(N.asList("a", "b", "c", "d"), byNameAndClass.parameters());

        // Single-row forms are unchanged.
        assertEquals("INSERT INTO account (first_name) VALUES (?)", PSC.insert("firstName").into(Account.class).build().query());
        assertEquals("INSERT INTO account_archive (first_name) VALUES (?)", PSC.insert("firstName").into("account_archive", Account.class).build().query());
    }

    @Test
    public void testUsingTimestampConvertsDateMillisToMicros() {
        final String cql = PSC.insert("id").into("users").usingTimestamp(new Date(5L)).build().query();

        assertTrue(cql.contains("USING TIMESTAMP 5000"));
    }

    @Test
    public void testUsingTimestampConvertsMillisToMicros() {
        final String cql = PSC.insert("id").into("users").usingTimestamp(123L).build().query();

        assertTrue(cql.contains("USING TIMESTAMP 123000"));
    }

    @Test
    public void testUsingTimestampMicrosDoesNotConvertUnit() {
        final String cql = PSC.insert("id").into("users").usingTimestampMicros(123456L).build().query();

        assertTrue(cql.contains("USING TIMESTAMP 123456"));
    }

    // Factories and insert operations

    @Test
    public void testACCB_batchInsert() {
        final String cql = ACCB.batchInsert(N.asList(N.asMap("firstName", "a"))).into("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
        assertTrue(cql.contains("'a'"), cql);
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
    public void testACCB_insertCollection() {
        final String cql = ACCB.insert(N.asList("firstName")).into("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
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
    public void testACCB_insertMap() {
        final String cql = ACCB.insert(N.asMap("firstName", "John")).into("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
        assertTrue(cql.contains("'John'"), cql);
    }

    // ACCB (SCREAMING_SNAKE_CASE + RAW_SQL): UPPER_SNAKE columns, inlined literal values.

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
    public void testBatchInsertAppliesInsertClausesToEveryStatement() {
        final String cql = PSC.batchInsert(N.asList(N.asMap("id", 1), N.asMap("id", 2))).into("account").usingTTL(60).ifNotExists().build().query();

        assertEquals("BEGIN BATCH INSERT INTO account (id) VALUES (?) IF NOT EXISTS USING TTL 60; "
                + "INSERT INTO account (id) VALUES (?) IF NOT EXISTS USING TTL 60; APPLY BATCH", cql);
    }

    @Test
    public void test_batchInsertClauses_ignoreSemicolonsInsideDollarQuotedValues() {
        final String cql = SCCB.batchInsert(List.of(Map.of("path", com.landawn.abacus.query.condition.SqlExpression.of("$$a;b$$"))))
                .into("files")
                .usingTTL(60)
                .ifNotExists()
                .build()
                .query();

        assertEquals("BEGIN BATCH INSERT INTO files (path) VALUES ($$a;b$$) IF NOT EXISTS USING TTL 60; APPLY BATCH", cql);
    }

    @Test
    public void testBatchInsertUsesCqlBatchSyntax() {
        final SP cqlPair = PSC.batchInsert(N.asList(N.asMap("firstName", "John", "lastName", "Doe"), N.asMap("firstName", "Jane", "lastName", "Smith")))
                .into("account")
                .build();

        assertEquals("BEGIN BATCH INSERT INTO account (first_name, last_name) VALUES (?, ?); "
                + "INSERT INTO account (first_name, last_name) VALUES (?, ?); APPLY BATCH", cqlPair.query());
        assertEquals(N.asList("John", "Doe", "Jane", "Smith"), cqlPair.parameters());
    }

    /**
     * {@code from()} on an INSERT builder is invalid; the error must use the CQL keyword INSERT
     * (not the internal OperationType name ADD), on both the String- and Class-typed overloads.
     */
    @Test
    public void test_insert_from_throwsIllegalStateException_withCqlKeyword() {
        final IllegalStateException e1 = assertThrows(IllegalStateException.class, () -> PSC.insert("firstName").from("account"));
        assertTrue(e1.getMessage().contains("INSERT"), e1.getMessage());
        assertFalse(e1.getMessage().contains("ADD"), e1.getMessage());

        final IllegalStateException e2 = assertThrows(IllegalStateException.class, () -> PSC.insert("firstName").from(Account.class));
        assertTrue(e2.getMessage().contains("INSERT"), e2.getMessage());
        assertFalse(e2.getMessage().contains("ADD"), e2.getMessage());
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

    @Test
    public void test_insert_rawLiteralContainingClauseKeywords_doesNotCorruptUsingOrIfPlacement() {
        final String cql = SCCB.insert(N.asMap("note", "text USING marker WHERE marker")).into("events").usingTTL(60).ifNotExists().build().query();

        assertEquals("INSERT INTO events (note) VALUES ('text USING marker WHERE marker') IF NOT EXISTS USING TTL 60", cql);
    }

    @Test
    public void insertAndSelect_repeatedBuilds_preserveQueryAndParameters() {
        for (int i = 0; i < 3; i++) {
            final SP insert = SCCB.insert("gui", "firstName", "lastName", "lastUpdateTime", "createTime").into("account").build();
            assertEquals("INSERT INTO account (gui, first_name, last_name, last_update_time, create_time) VALUES (?, ?, ?, ?, ?)", insert.query());
            assertTrue(insert.parameters().isEmpty());

            final SP select = NSC.select("gui", "firstName", "lastName", "lastUpdateTime", "createTime").from("account").where(Filters.eq("id", 1)).build();
            assertEquals("SELECT gui, first_name AS \"firstName\", last_name AS \"lastName\", "
                    + "last_update_time AS \"lastUpdateTime\", create_time AS \"createTime\" FROM account WHERE id = :id", select.query());
            assertEquals(List.of(1), select.parameters());
        }
    }

    @Test
    public void test_insertClauses_afterLiteralEndingInBackslash() {
        final String cql = SCCB.insert(Map.of("path", "C:\\")).into("files").usingTTL(60).ifNotExists().build().query();
        assertEquals("INSERT INTO files (path) VALUES ('C:\\') IF NOT EXISTS USING TTL 60", cql);

        final String batch = SCCB.batchInsert(List.of(Map.of("path", "C:\\"), Map.of("path", "D:\\"))).into("files").usingTTL(60).ifNotExists().build().query();
        assertEquals("BEGIN BATCH INSERT INTO files (path) VALUES ('C:\\') IF NOT EXISTS USING TTL 60;"
                + " INSERT INTO files (path) VALUES ('D:\\') IF NOT EXISTS USING TTL 60; APPLY BATCH", batch);
    }

    @Test
    public void testLCCB_batchInsert() {
        final String cql = LCCB.batchInsert(N.asList(N.asMap("firstName", "a"))).into("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
        assertTrue(cql.contains("'a'"), cql);
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
    public void testLCCB_insertCollection() {
        final String cql = LCCB.insert(N.asList("firstName")).into("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
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
    public void testLCCB_insertMap() {
        final String cql = LCCB.insert(N.asMap("firstName", "John")).into("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
        assertTrue(cql.contains("'John'"), cql);
    }

    // LCCB (CAMEL_CASE + RAW_SQL): camelCase columns, inlined literal values.

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
    public void testNAC_batchInsert() {
        final String cql = NAC.batchInsert(N.asList(N.asMap("firstName", "a"))).into("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
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
    public void testNAC_insertCollection() {
        final String cql = NAC.insert(N.asList("firstName")).into("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
        assertTrue(cql.contains(":firstName"), cql);
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
    public void testNAC_insertMap() {
        final String cql = NAC.insert(N.asMap("firstName", Filters.QME)).into("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testNAC_insertString() {
        final String cql = NAC.insert("firstName").into("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    // NAC (SCREAMING_SNAKE_CASE + NAMED_SQL): identifiers become UPPER_SNAKE_CASE.

    @Test
    public void testNAC_insertStringArray() {
        final String cql = NAC.insert("firstName", "lastName").into("account").build().query();
        assertEquals("INSERT INTO account (FIRST_NAME, LAST_NAME) VALUES (:firstName, :lastName)", cql);
    }

    @Test
    public void testNLC_batchInsert() {
        final String cql = NLC.batchInsert(N.asList(N.asMap("firstName", "a"))).into("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
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
    public void testNLC_insertCollection() {
        final String cql = NLC.insert(N.asList("firstName")).into("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
        assertTrue(cql.contains(":firstName"), cql);
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
    public void testNLC_insertMap() {
        final String cql = NLC.insert(N.asMap("firstName", Filters.QME)).into("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    // NLC (CAMEL_CASE + NAMED_SQL): camelCase columns + ':name' placeholders.

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
    public void testNSB_batchInsert() {
        final String cql = NSB.batchInsert(N.asList(N.asMap("firstName", "a"), N.asMap("firstName", "b"))).into("account").build().query();
        assertTrue(cql.startsWith("BEGIN BATCH INSERT INTO account"), cql);
        assertTrue(cql.contains("firstName"), cql);
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
    public void testNSB_insertCollection() {
        final String cql = NSB.insert(N.asList("firstName", "lastName")).into("account").build().query();
        assertEquals("INSERT INTO account (firstName, lastName) VALUES (:firstName, :lastName)", cql);
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
    public void testNSB_insertMap() {
        final Map<String, Object> props = N.asMap("firstName", Filters.QME, "lastName", Filters.QME);
        final String cql = NSB.insert(props).into("account").build().query();
        assertTrue(cql.startsWith("INSERT INTO account"), cql);
        assertTrue(cql.contains("firstName"), cql);
        assertTrue(cql.contains("lastName"), cql);
    }

    // NSB (NO_CHANGE naming + NAMED_SQL): exercise all factory methods (was 0% coverage).

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
    public void testNSC_batchInsert() {
        final String cql = NSC.batchInsert(N.asList(N.asMap("firstName", "a"), N.asMap("firstName", "b"))).into("account").build().query();
        assertTrue(cql.startsWith("BEGIN BATCH INSERT INTO account"), cql);
        assertTrue(cql.contains("first_name"), cql);
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
    public void testNSC_insertCollection() {
        final String cql = NSC.insert(N.asList("firstName", "lastName")).into("account").build().query();
        assertEquals("INSERT INTO account (first_name, last_name) VALUES (:firstName, :lastName)", cql);
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
    public void testNSC_insertMap() {
        final Map<String, Object> props = N.asMap("firstName", Filters.QME, "lastName", Filters.QME);
        final String cql = NSC.insert(props).into("account").build().query();
        assertTrue(cql.startsWith("INSERT INTO account"), cql);
        assertTrue(cql.contains("first_name"), cql);
        assertTrue(cql.contains("last_name"), cql);
    }

    // NSC (SNAKE_CASE naming + NAMED_SQL): exercise all factory methods.
    // Column names are converted to snake_case, named placeholders use original property names.

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
    public void testPAC_batchInsert() {
        final String cql = PAC.batchInsert(N.asList(N.asMap("firstName", "a"))).into("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
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
    public void testPAC_insertCollection() {
        final String cql = PAC.insert(N.asList("firstName")).into("account").build().query();
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
    public void testPAC_insertMap() {
        final String cql = PAC.insert(N.asMap("firstName", Filters.QME)).into("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    // PAC (SCREAMING_SNAKE_CASE + PARAMETERIZED_SQL): UPPER_SNAKE columns, '?' placeholders.

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
    public void testPLC_batchInsert() {
        final String cql = PLC.batchInsert(N.asList(N.asMap("firstName", "a"))).into("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
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
    public void testPLC_insertCollection() {
        final String cql = PLC.insert(N.asList("firstName")).into("account").build().query();
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
    public void testPLC_insertMap() {
        final String cql = PLC.insert(N.asMap("firstName", Filters.QME)).into("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    // PLC (CAMEL_CASE + PARAMETERIZED_SQL): camelCase columns + '?' placeholders.

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
    public void testPSB_batchInsert() {
        final String cql = PSB.batchInsert(N.asList(N.asMap("firstName", "a"), N.asMap("firstName", "b"))).into("account").build().query();
        assertTrue(cql.startsWith("BEGIN BATCH INSERT INTO account"), cql);
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
    public void testPSB_insertCollection() {
        final String cql = PSB.insert(N.asList("firstName", "lastName")).into("account").build().query();
        assertEquals("INSERT INTO account (firstName, lastName) VALUES (?, ?)", cql);
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
    public void testPSB_insertMap() {
        final Map<String, Object> props = N.asMap("firstName", Filters.QME);
        final String cql = PSB.insert(props).into("account").build().query();
        assertEquals("INSERT INTO account (firstName) VALUES (?)", cql);
    }

    // PSB (NO_CHANGE naming + PARAMETERIZED_SQL): mirrors NSB API but emits '?' placeholders.

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
    public void testPSC_batchInsert() {
        final String cql = PSC.batchInsert(N.asList(N.asMap("firstName", "a"), N.asMap("firstName", "b"))).into("account").build().query();
        assertTrue(cql.startsWith("BEGIN BATCH INSERT INTO account"), cql);
        assertTrue(cql.contains("first_name"), cql);
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
    public void testPSC_insertCollection() {
        final String cql = PSC.insert(N.asList("firstName", "lastName")).into("account").build().query();
        assertEquals("INSERT INTO account (first_name, last_name) VALUES (?, ?)", cql);
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
    public void testPSC_insertMap() {
        final Map<String, Object> props = N.asMap("firstName", Filters.QME);
        final String cql = PSC.insert(props).into("account").build().query();
        assertEquals("INSERT INTO account (first_name) VALUES (?)", cql);
    }

    // PSC (SNAKE_CASE + PARAMETERIZED_SQL): snake_case columns + '?' placeholders.

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
    public void testSCCB_batchInsert() {
        final String cql = SCCB.batchInsert(N.asList(N.asMap("firstName", "a"))).into("account").build().query();
        assertTrue(cql.contains("first_name"), cql);
        assertTrue(cql.contains("'a'"), cql);
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
    public void testSCCB_insertCollection() {
        final String cql = SCCB.insert(N.asList("firstName")).into("account").build().query();
        assertTrue(cql.contains("first_name"), cql);
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
    public void testSCCB_insertMap() {
        final String cql = SCCB.insert(N.asMap("firstName", "John")).into("account").build().query();
        assertTrue(cql.contains("first_name"), cql);
        assertTrue(cql.contains("'John'"), cql);
    }

    // SCCB (SNAKE_CASE + RAW_SQL): snake_case columns, inlined literal values.

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
    public void test_updateUsingClausesAreInsertedBeforeSetAndCombined() {
        final String cql = PSC.update("account").set("firstName").where(Filters.eq("id", 1)).usingTTL(60).usingTimestamp("42").build().query();

        assertTrue(cql.contains("UPDATE account USING TTL 60 AND TIMESTAMP 42 SET"), cql);
        assertTrue(cql.contains(" WHERE id = ?"), cql);
    }

    // Updates and deletes

    /**
     * The overloads not covered by the sliceQ/verifyQB tests -- usingTTL(String), usingTimestamp(Date),
     * usingTimestampMicros(long) -- also keep the implicit SET list of update(Class) pending, so a later set(...) replaces
     * it (HEAD appended it: duplicate columns, unbound placeholders). The other statement shapes keep their USING placement.
     */
    @Test
    public void test_coverageCQ_usingBeforeSet_remainingUsingOverloads() {
        final Date date = new Date(7L);
        final java.util.Set<String> allButFirstName = N.asSet("gui", "emailAddress", "middleName", "lastName", "birthDate", "status", "lastUpdateTime",
                "createTime", "contact", "devices");
        final Account account = new Account();
        account.setFirstName("f");

        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertEquals("UPDATE account USING TTL 5 SET first_name = ? WHERE id = ?",
                        PSC.update(Account.class).usingTTL("5").set("firstName").where(Filters.eq("id", 1)).build().query()),
                () -> assertEquals("UPDATE account USING TIMESTAMP 7000 SET first_name = ? WHERE id = ?",
                        PSC.update(Account.class).usingTimestamp(date).set("firstName").where(Filters.eq("id", 1)).build().query()),
                () -> assertEquals("UPDATE account USING TIMESTAMP 7 SET first_name = ? WHERE id = ?",
                        PSC.update(Account.class).usingTimestampMicros(7L).set("firstName").where(Filters.eq("id", 1)).build().query()),
                () -> assertEquals("UPDATE account USING TIMESTAMP 7 AND TTL 5 SET first_name = ? WHERE id = ?",
                        PSC.update(Account.class).usingTimestampMicros(7L).usingTTL("5").set("firstName").where(Filters.eq("id", 1)).build().query()),
                () -> {
                    final SP sp = NSC.update(Account.class, allButFirstName).usingTTL("5").set(account, allButFirstName).where(Filters.eq("id", 1)).build();
                    assertEquals("UPDATE account USING TTL 5 SET first_name = :firstName WHERE id = :id", sp.query());
                    assertEquals(N.asList("f", 1), sp.parameters());
                });

        // Previous behavior kept: without set(...) the implicit list is rendered at where(); update(table[, Class]),
        // INSERT and DELETE place USING as before.
        assertEquals("UPDATE account USING TIMESTAMP 7000 SET first_name = ? WHERE id = ?",
                PSC.update(Account.class, allButFirstName).usingTimestamp(date).where(Filters.eq("id", 1)).build().query());
        assertEquals("UPDATE account USING TTL 5 SET first_name = ? WHERE id = ?",
                PSC.update("account", Account.class).set("firstName").usingTTL(5).where(Filters.eq("id", 1)).build().query());
        assertEquals("UPDATE account USING TTL 5 SET first_name = ? WHERE id = ?",
                PSC.update("account").set("firstName").usingTTL(5).where(Filters.eq("id", 1)).build().query());
        assertEquals("INSERT INTO account (first_name) VALUES (?) USING TTL 5", PSC.insert("firstName").into("account").usingTTL(5).build().query());
        assertEquals("DELETE FROM account USING TIMESTAMP 7000 WHERE id = ?",
                PSC.deleteFrom("account").usingTimestamp(7L).where(Filters.eq("id", 1)).build().query());
        assertEquals("DELETE first_name FROM account USING TIMESTAMP 7000 WHERE id = ?",
                PSC.delete("firstName").from("account").usingTimestamp(7L).where(Filters.eq("id", 1)).build().query());
    }

    /**
     * USING appended to update(Class) before set(...) expanded the implicit SET list immediately, so a following
     * explicit set(...) was appended to it (duplicate columns, unbound placeholders) instead of replacing it as
     * documented for update(Class, Set).
     */
    @Test
    public void test_sliceQ_updateEntityClass_usingBeforeSet_explicitSetReplacesImplicitList() {
        assertEquals("UPDATE account USING TTL 5 SET first_name = ? WHERE id = ?",
                PSC.update(Account.class).usingTTL(5).set("firstName").where(Filters.eq("id", 1)).build().query());

        final SP sp = PSC.update(Account.class).usingTTL(5).usingTimestamp(7L).set(N.asMap("firstName", (Object) "f")).where(Filters.eq("id", 1)).build();
        assertEquals("UPDATE account USING TTL 5 AND TIMESTAMP 7000 SET first_name = ? WHERE id = ?", sp.query());
        assertEquals(N.asList("f", 1), sp.parameters());

        final Account account = new Account();
        account.setFirstName("f");
        final String beanCql = PSC.update(Account.class).usingTTL(5).set(account).where(Filters.eq("id", 1)).build().query();
        assertTrue(beanCql.startsWith("UPDATE account USING TTL 5 SET gui = ?, "), beanCql);
        assertEquals(1, Strings.countMatches(beanCql, "first_name = ?"), beanCql);

        assertEquals("UPDATE account USING TIMESTAMP 7 SET first_name = :firstName WHERE id = :id",
                NSC.update(Account.class).usingTimestamp("7").set("firstName").where(Filters.eq("id", 1)).build().query());

        // Without an explicit set(...) the implicit list is still rendered, whether where() or build() finalizes it.
        final String implicitCql = PSC.update(Account.class).usingTTL(5).build().query();
        assertTrue(implicitCql.startsWith("UPDATE account USING TTL 5 SET gui = ?, "), implicitCql);
        assertTrue(implicitCql.endsWith("devices = ?"), implicitCql);

        // An UPDATE with no SET columns at all is still rejected when USING is appended.
        assertThrows(IllegalStateException.class, () -> PSC.update("account").usingTTL(5));
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

    // Regression tests for clause-ordering / parameter-preservation / escaping bug fixes.

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

    /** Every SET overload called after USING replaces the pending implicit SET list of update(Class) (was appended to it). */
    @Test
    public void test_verifyQB_usingBeforeSet_everySetOverloadReplacesImplicitList() {
        SP sp = PSC.update(Account.class).usingTTL(5).set(N.asList("firstName", "lastName")).where(Filters.eq("id", 1)).build();
        assertEquals("UPDATE account USING TTL 5 SET first_name = ?, last_name = ? WHERE id = ?", sp.query());
        assertEquals(N.asList(1), sp.parameters());

        sp = PSC.update(Account.class).usingTTL(5).set("firstName", "v").where(Filters.eq("id", 1)).build();
        assertEquals("UPDATE account USING TTL 5 SET first_name = ? WHERE id = ?", sp.query());
        assertEquals(N.asList("v", 1), sp.parameters());

        assertEquals("UPDATE account USING TTL 5 SET first_name = ?, last_name = ? WHERE id = ?",
                PSC.update(Account.class).usingTTL(5).set(Account.class, VERIFY_QB_ALL_BUT_NAMES).where(Filters.eq("id", 1)).build().query());
        assertEquals("UPDATE account USING TIMESTAMP 7000 SET first_name = ?, last_name = ? WHERE id = ?",
                PSC.update(Account.class).usingTimestamp(7L).set("firstName").set("lastName").where(Filters.eq("id", 1)).build().query());
        assertEquals("UPDATE account USING TTL 5 SET first_name = 'v' WHERE id = 1",
                SCCB.update(Account.class).usingTTL(5).set("firstName", "v").where(Filters.eq("id", 1)).build().query());

        // Named parameters are numbered as for a fresh builder: the replaced implicit list consumes no names.
        sp = NSC.update(Account.class).usingTTL(5).set(N.asList("firstName")).where(Filters.and(Filters.eq("firstName", "a"), Filters.eq("id", 1))).build();
        assertEquals("UPDATE account USING TTL 5 SET first_name = :firstName WHERE first_name = :firstName_2 AND id = :id", sp.query());
        assertEquals(N.asList("a", 1), sp.parameters());
        assertEquals("UPDATE ACCOUNT USING TTL 5 SET FIRST_NAME = :firstName WHERE FIRST_NAME = :firstName_2 AND ID = :id",
                NAC.update(Account.class)
                        .usingTTL(5)
                        .set(N.asList("firstName"))
                        .where(Filters.and(Filters.eq("firstName", "a"), Filters.eq("id", 1)))
                        .build()
                        .query());

        // Previous behavior kept: set(...) before USING, and a rejected explicit set(...) leaves the implicit list pending.
        assertEquals("UPDATE account USING TTL 5 AND TIMESTAMP 8000 SET first_name = ? WHERE id = ?",
                PSC.update(Account.class).set("firstName").usingTTL(5).usingTimestamp(8L).where(Filters.eq("id", 1)).build().query());

        final CqlBuilder builder = PSC.update(Account.class, VERIFY_QB_ALL_BUT_NAMES).usingTTL(5);
        assertThrows(IllegalArgumentException.class, () -> builder.set(N.asList("firstName", "bad -- x")));
        assertEquals("UPDATE account USING TTL 5 SET first_name = ?, last_name = ? WHERE id = ?", builder.where(Filters.eq("id", 1)).build().query());
    }

    // Queries and results

    /** The implicit SET list kept pending by USING is rendered (with its exclusions) by every clause that finalizes the statement. */
    @Test
    public void test_verifyQB_usingBeforeWhere_pendingImplicitListFinalizedByEveryClause() {
        assertEquals("UPDATE account USING TTL 5 SET first_name = ?, last_name = ? WHERE id = ?",
                PSC.update(Account.class, VERIFY_QB_ALL_BUT_NAMES).usingTTL(5).where(Filters.eq("id", 1)).build().query());

        SP sp = PSC.update(Account.class, VERIFY_QB_ALL_BUT_NAMES).usingTTL(5).where(Filters.eq("id", 1)).onlyIf(Filters.eq("firstName", "x")).build();
        assertEquals("UPDATE account USING TTL 5 SET first_name = ?, last_name = ? WHERE id = ? IF first_name = ?", sp.query());
        assertEquals(N.asList(1, "x"), sp.parameters());

        assertEquals("UPDATE account USING TTL 5 SET first_name = ?, last_name = ? WHERE id = ? IF EXISTS",
                PSC.update(Account.class, VERIFY_QB_ALL_BUT_NAMES).usingTTL(5).where(Filters.eq("id", 1)).ifExists().build().query());
        assertEquals("UPDATE account USING TTL 5 SET first_name = ?, last_name = ? WHERE id = 1",
                PSC.update(Account.class, VERIFY_QB_ALL_BUT_NAMES).usingTTL(5).append(" WHERE id = 1").build().query());
        assertEquals("UPDATE account USING TTL 5 AND TIMESTAMP 7000 SET first_name = ?, last_name = ?",
                PSC.update(Account.class, VERIFY_QB_ALL_BUT_NAMES).usingTTL(5).usingTimestamp(7L).build().query());
        assertEquals("UPDATE account USING TIMESTAMP 7000 SET first_name = ?, last_name = ? WHERE id = 1",
                SCCB.update(Account.class, VERIFY_QB_ALL_BUT_NAMES).usingTimestamp(7L).where(Filters.eq("id", 1)).build().query());

        sp = NSC.update(Account.class, VERIFY_QB_ALL_BUT_NAMES).usingTTL(5).where(Filters.and(Filters.eq("firstName", "a"), Filters.eq("id", 1))).build();
        assertEquals("UPDATE account USING TTL 5 SET first_name = :firstName, last_name = :lastName WHERE first_name = :firstName_2 AND id = :id", sp.query());
        assertEquals(N.asList("a", 1), sp.parameters());

        // An UPDATE with no SET column at all is still rejected when USING is appended.
        assertThrows(IllegalStateException.class, () -> PSC.update("account", Account.class).usingTTL(5));
        assertThrows(IllegalStateException.class, () -> PSC.update(Account.class, N.asSet("gui", "emailAddress", "firstName", "middleName", "lastName",
                "birthDate", "status", "lastUpdateTime", "createTime", "contact", "devices")).usingTimestamp(7L));
    }

    // Resource lifecycle

    /**
     * A batch INSERT whose rendering fails after {@code BEGIN BATCH} was emitted (an unsafe column name, or a NaN
     * inlined by a raw-CQL builder) must not leave the truncated batch behind: previously the second
     * {@code into(...)} failed with "can only be called once" and {@code build()} returned the partial text.
     */
    @Test
    public void test_batchInto_renderFailure_leavesBuilderUnchanged() {
        final CqlBuilder unsafeColumn = PSC.batchInsert(N.asList(N.asMap("a", 1, "b--c", 2)));
        assertThrows(IllegalArgumentException.class, () -> unsafeColumn.into("t"));
        assertThrows(IllegalArgumentException.class, () -> unsafeColumn.into("t"));
        assertThrows(IllegalStateException.class, unsafeColumn::build);

        @SuppressWarnings("deprecation")
        final CqlBuilder nanLiteral = SCCB.batchInsert(N.asList(N.asMap("a", 1, "b", Double.NaN)));
        assertThrows(IllegalArgumentException.class, () -> nanLiteral.into("t"));
        assertThrows(IllegalArgumentException.class, () -> nanLiteral.into("t"));
        assertThrows(IllegalStateException.class, nanLiteral::build);
    }

    // Additional contracts

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

    /**
     * Cassandra rejects client-supplied timestamps on lightweight transactions. Guard both method orders so the
     * builder fails locally instead of generating CQL that the server will reject.
     */
    @Test
    public void test_usingTimestamp_cannotBeCombinedWithIfClause() {
        assertThrows(IllegalStateException.class, () -> PSC.insert("id", "name").into("account").usingTimestampMicros(1).ifNotExists());
        assertThrows(IllegalStateException.class, () -> PSC.insert("id", "name").into("account").ifNotExists().usingTimestampMicros(1));

        assertThrows(IllegalStateException.class,
                () -> PSC.update("account").set("name").where(Filters.eq("id", 1)).usingTimestampMicros(1).onlyIf(Filters.eq("name", "old")));
        assertThrows(IllegalStateException.class, () -> PSC.update("account").set("name").where(Filters.eq("id", 1)).ifExists().usingTimestampMicros(1));

        assertThrows(IllegalStateException.class, () -> PSC.deleteFrom("account").where(Filters.eq("id", 1)).usingTimestampMicros(1).ifExists());
        assertThrows(IllegalStateException.class, () -> PSC.deleteFrom("account").where(Filters.eq("id", 1)).onlyIf("name = 'old'").usingTimestampMicros(1));

        // TTL remains valid on a conditional INSERT.
        assertEquals("INSERT INTO account (id, name) VALUES (?, ?) IF NOT EXISTS USING TTL 60",
                PSC.insert("id", "name").into("account").ifNotExists().usingTTL(60).build().query());
    }
}
