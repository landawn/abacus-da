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

import java.util.List;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.da.entity.Account;
import com.landawn.abacus.da.entity.Users;
import com.landawn.abacus.query.AbstractQueryBuilder.SP;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.condition.In;
import com.landawn.abacus.util.N;

public class CqlBuilderDeleteTest extends TestBase {

    // Updates and deletes

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
    public void testACCB_deleteCollection() {
        final String cql = ACCB.delete(N.asList("firstName")).from("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testACCB_deleteFromClass() {
        final String cql = ACCB.deleteFrom(Users.class).build().query();
        assertEquals("DELETE FROM simplex.users", cql);
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
    public void testACCB_deleteString() {
        final String cql = ACCB.delete("firstName").from("account").build().query();
        assertEquals("DELETE FIRST_NAME FROM account", cql);
    }

    @Test
    public void testACCB_deleteStringArray() {
        final String cql = ACCB.delete("firstName", "lastName").from("account").build().query();
        assertEquals("DELETE FIRST_NAME, LAST_NAME FROM account", cql);
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
     * Regression guard for the {@code from(...)} overrides added for abacus-query 4.8.9, whose parent
     * builder made {@code from()} SELECT-only: CQL's DELETE-with-columns chain
     * {@code delete(...).from(...)} must keep working through the Class-typed overloads too.
     */
    @Test
    public void test_delete_from_classTypedOverloads_supportDelete() {
        // from(Class)
        final String byClass = PSC.delete("firstName").from(Account.class).where(Filters.eq("id", 1)).build().query();
        assertEquals("DELETE first_name FROM account WHERE id = ?", byClass);

        // from(String, Class): the entity class association still maps property names in WHERE
        final String byExprAndClass = PSC.delete("firstName").from("account", Account.class).where(Filters.eq("id", 1)).build().query();
        assertEquals("DELETE first_name FROM account WHERE id = ?", byExprAndClass);
    }

    @Test
    public void delete_namedInCondition_distinguishesRowsFromColumns() {
        final In condition = Filters.in("id", N.asList("a", "b"));
        final SP direct = NSC.deleteFrom(Users.class).append(condition).build();
        final SP explicit = NSC.delete(Users.class).from(Users.class).append(condition).build();

        assertEquals("DELETE FROM simplex.users WHERE id IN (:id1, :id2)", direct.query());
        assertEquals(List.of("a", "b"), direct.parameters());
        assertEquals("DELETE name, addresses, someDate, last_update_time, created_time, bytes, shorts FROM simplex.users WHERE id IN (:id1, :id2)",
                explicit.query());
        assertEquals(direct.parameters(), explicit.parameters());
    }

    /**
     * abacus-query 4.9.4 made every parent {@code from(...)} overload reject a non-SELECT operation, so each
     * {@code delete(columns).from(...)} threw "Invalid operation for from(): DELETE. Expected QUERY". Every FROM
     * overload must render the CQL column DELETE, with naming-policy conversion and no SELECT aliases.
     */
    @Test
    @SuppressWarnings("deprecation")
    public void test_deleteColumns_everyFromOverload_rendersCqlColumnDelete() {
        final String expected = "DELETE first_name, last_name FROM account WHERE id = ?";

        assertEquals(expected, PSC.delete("firstName", "lastName").from("account").where(Filters.eq("id", 1)).build().query());
        assertEquals(expected, PSC.delete("firstName", "lastName").from("  account ").where(Filters.eq("id", 1)).build().query());
        assertEquals(expected, PSC.delete("firstName", "lastName").from(new String[] { "account" }).where(Filters.eq("id", 1)).build().query());
        assertEquals(expected, PSC.delete("firstName", "lastName").from(N.asList("account")).where(Filters.eq("id", 1)).build().query());
        assertEquals(expected, PSC.delete("firstName", "lastName").from(Account.class).where(Filters.eq("id", 1)).build().query());
        assertEquals(expected, PSC.delete("firstName", "lastName").from(Account.class, (String) null).where(Filters.eq("id", 1)).build().query());
        assertEquals(expected, PSC.delete("firstName", "lastName").from("account", Account.class).where(Filters.eq("id", 1)).build().query());
        assertEquals(expected, PSC.delete("firstName", "lastName").from("account", "account").where(Filters.eq("id", 1)).build().query());
        assertEquals(expected, PSC.delete("firstName", "lastName").from(Account.class, N.asList("account")).where(Filters.eq("id", 1)).build().query());
        assertThrows(IllegalArgumentException.class, () -> PSC.delete("firstName").from(Account.class, N.asList("account", "other")));
        assertEquals("DELETE first_name, last_name FROM ks.account WHERE id = ?",
                PSC.delete(N.asList("firstName", "lastName")).from("ks.account").where(Filters.eq("id", 1)).build().query());

        assertEquals("DELETE FIRST_NAME, LAST_NAME FROM ACCOUNT WHERE ID = ?",
                PAC.delete("firstName", "lastName").from(Account.class).where(Filters.eq("id", 1)).build().query());
        assertEquals("DELETE firstName FROM account WHERE id = :id", NLC.delete("firstName").from(Account.class).where(Filters.eq("id", 1)).build().query());
        assertEquals("DELETE first_name FROM account WHERE id = 1", SCCB.delete("firstName").from("account").where(Filters.eq("id", 1)).build().query());

        // Entity-derived column list: key columns excluded, and no SELECT-style "AS" aliases.
        final String byClass = NSC.delete(Account.class).from(Account.class).where(Filters.eq("id", 1)).build().query();
        assertTrue(byClass.startsWith("DELETE gui, email_address, first_name, "), byClass);
        assertTrue(byClass.endsWith(" FROM account WHERE id = :id"), byClass);
        assertFalse(byClass.contains(" AS "), byClass);

        // Trailing clauses keep their CQL positions.
        assertEquals("DELETE first_name FROM account USING TIMESTAMP 5000 WHERE id = ?",
                PSC.delete("firstName").from("account").where(Filters.eq("id", 1)).usingTimestamp(5L).build().query());
        assertEquals("DELETE first_name FROM account WHERE id = ? IF status = ?",
                PSC.delete("firstName").from("account").where(Filters.eq("id", 1)).onlyIf(Filters.eq("status", 1)).build().query());

        // State validation is unchanged.
        assertThrows(IllegalStateException.class, () -> PSC.delete("firstName").from("account").from("account"));
        assertThrows(IllegalArgumentException.class, () -> PSC.delete("firstName").from("account a"));
        assertThrows(IllegalArgumentException.class, () -> PSC.delete("firstName").from("account", "other"));
        assertThrows(IllegalArgumentException.class, () -> PSC.delete("firstName").from(Account.class, "a"));
        assertThrows(IllegalStateException.class, () -> PSC.update("account").from("account"));

        // SELECT rendering through the same overloads is unchanged.
        assertEquals("SELECT first_name AS \"firstName\" FROM account WHERE id = ?",
                PSC.select("firstName").from("account", "account").where(Filters.eq("id", 1)).build().query());
        assertEquals("SELECT first_name AS \"firstName\" FROM account WHERE id = ?",
                PSC.select("firstName").from(Account.class).where(Filters.eq("id", 1)).build().query());
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
    public void testLCCB_deleteCollection() {
        final String cql = LCCB.delete(N.asList("firstName")).from("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testLCCB_deleteFromClass() {
        final String cql = LCCB.deleteFrom(Users.class).build().query();
        assertEquals("DELETE FROM simplex.users", cql);
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
    public void testNAC_deleteCollection() {
        final String cql = NAC.delete(N.asList("firstName")).from("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testNAC_deleteFromClass() {
        final String cql = NAC.deleteFrom(Users.class).build().query();
        assertEquals("DELETE FROM simplex.users", cql);
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
    public void testNLC_deleteCollection() {
        final String cql = NLC.delete(N.asList("firstName")).from("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testNLC_deleteFromClass() {
        final String cql = NLC.deleteFrom(Users.class).build().query();
        assertEquals("DELETE FROM simplex.users", cql);
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
    public void testNSB_deleteCollection() {
        final String cql = NSB.delete(N.asList("firstName", "lastName")).from("account").where(Filters.eq("id", 1)).build().query();
        assertEquals("DELETE firstName, lastName FROM account WHERE id = :id", cql);
    }

    @Test
    public void testNSB_deleteFromClass() {
        final String cql = NSB.deleteFrom(Users.class).where(Filters.eq("id", Filters.QME)).build().query();
        assertTrue(cql.startsWith("DELETE FROM simplex.users"), cql);
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
    public void testNSC_deleteCollection() {
        final String cql = NSC.delete(N.asList("firstName", "lastName")).from("account").where(Filters.eq("id", 1)).build().query();
        assertEquals("DELETE first_name, last_name FROM account WHERE id = :id", cql);
    }

    @Test
    public void testNSC_deleteFromClass() {
        final String cql = NSC.deleteFrom(Users.class).where(Filters.eq("id", Filters.QME)).build().query();
        assertTrue(cql.startsWith("DELETE FROM simplex.users"), cql);
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
    public void testPAC_deleteCollection() {
        final String cql = PAC.delete(N.asList("firstName")).from("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testPAC_deleteFromClass() {
        final String cql = PAC.deleteFrom(Users.class).build().query();
        assertEquals("DELETE FROM simplex.users", cql);
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
    public void testPLC_deleteCollection() {
        final String cql = PLC.delete(N.asList("firstName")).from("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testPLC_deleteFromClass() {
        final String cql = PLC.deleteFrom(Users.class).build().query();
        assertEquals("DELETE FROM simplex.users", cql);
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
    public void testPSB_deleteCollection() {
        final String cql = PSB.delete(N.asList("firstName", "lastName")).from("account").build().query();
        assertEquals("DELETE firstName, lastName FROM account", cql);
    }

    @Test
    public void testPSB_deleteFromClass() {
        final String cql = PSB.deleteFrom(Users.class).build().query();
        assertEquals("DELETE FROM simplex.users", cql);
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
    public void testPSC_deleteCollection() {
        final String cql = PSC.delete(N.asList("firstName", "lastName")).from("account").where(Filters.eq("id", 1)).build().query();
        assertEquals("DELETE first_name, last_name FROM account WHERE id = ?", cql);
    }

    @Test
    public void testPSC_deleteFromClass() {
        final String cql = PSC.deleteFrom(Users.class).build().query();
        assertEquals("DELETE FROM simplex.users", cql);
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
    public void testSCCB_deleteCollection() {
        final String cql = SCCB.delete(N.asList("firstName")).from("account").build().query();
        assertTrue(cql.contains("first_name"), cql);
    }

    @Test
    public void testSCCB_deleteFromClass() {
        final String cql = SCCB.deleteFrom(Users.class).build().query();
        assertEquals("DELETE FROM simplex.users", cql);
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
    public void testSCCB_deleteString() {
        final String cql = SCCB.delete("firstName").from("account").build().query();
        assertEquals("DELETE first_name FROM account", cql);
    }

    @Test
    public void testSCCB_deleteStringArray() {
        final String cql = SCCB.delete("firstName", "lastName").from("account").build().query();
        assertEquals("DELETE first_name, last_name FROM account", cql);
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
}
