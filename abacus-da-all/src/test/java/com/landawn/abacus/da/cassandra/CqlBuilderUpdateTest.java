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

import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.da.entity.Account;
import com.landawn.abacus.da.entity.Users;
import com.landawn.abacus.query.AbstractQueryBuilder.SP;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.util.N;

public class CqlBuilderUpdateTest extends TestBase {

    // Updates and deletes

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
    public void test_limit_offset() {
        String cql = NSC.select("firstName", "lastName").from("account").where(Filters.eq("id", Filters.QME)).limit(9).build().query();
        N.println(cql);
        assertEquals("SELECT first_name AS \"firstName\", last_name AS \"lastName\" FROM account WHERE id = :id LIMIT 9", cql);
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

    /**
     * Same rule for the entity-based {@code set(...)} overloads: {@code set(entity)} / {@code set(Class)} previously
     * put the id property into the SET list, which Cassandra rejects ("PRIMARY KEY part id found in SET part").
     */
    @Test
    public void test_set_entityAndEntityClass_excludeIdPropsFromSetClause() {
        final Account account = new Account();
        account.setId(7);
        account.setFirstName("f");

        final SP entitySp = NSC.update("account").set(account).where(Filters.eq("id", account.getId())).build();
        N.println(entitySp.query());
        assertTrue(entitySp.query().startsWith("UPDATE account SET gui = :gui, "), entitySp.query());
        assertFalse(entitySp.query().contains("SET id = :id"), entitySp.query());
        assertTrue(entitySp.query().contains("first_name = :firstName"), entitySp.query());
        assertTrue(entitySp.query().endsWith(" WHERE id = :id"), entitySp.query());
        assertFalse(entitySp.parameters().contains(7L) && entitySp.parameters().indexOf(7L) < entitySp.parameters().size() - 1,
                entitySp.parameters().toString());
        assertEquals(7L, entitySp.parameters().get(entitySp.parameters().size() - 1));

        final String excluded = PSC.update("account").set(account, N.asSet("gui", "createTime")).where(Filters.eq("id", 7)).build().query();
        assertFalse(excluded.contains("gui = ?"), excluded);
        assertFalse(excluded.contains("create_time = ?"), excluded);
        assertFalse(excluded.contains("SET id = ?"), excluded);
        assertTrue(excluded.contains("first_name = ?"), excluded);

        final String byClass = PSC.update("account").set(Account.class).where(Filters.eq("id", 7)).build().query();
        assertTrue(byClass.startsWith("UPDATE account SET gui = ?, "), byClass);
        assertFalse(byClass.contains("SET id = ?"), byClass);

        final String byClassExcluded = PSC.update("account").set(Account.class, N.asSet("gui")).where(Filters.eq("id", 7)).build().query();
        assertTrue(byClassExcluded.startsWith("UPDATE account SET email_address = ?, "), byClassExcluded);

        final String users = PSC.update("simplex.users").set(Users.class).where(Filters.eq("id", 1)).build().query();
        assertTrue(users.startsWith("UPDATE simplex.users SET name = ?, "), users);
        assertFalse(users.contains("SET id = ?"), users);

        // String / Map entities are handed to the parent unchanged; the parent's argument contract is preserved.
        assertEquals("UPDATE account SET first_name = ? WHERE id = ?",
                PSC.update("account").set((Object) "firstName").where(Filters.eq("id", 7)).build().query());
        assertEquals("UPDATE account SET id = ? WHERE id = ?", PSC.update("account").set(N.asMap("id", 1)).where(Filters.eq("id", 7)).build().query());
        assertThrows(IllegalArgumentException.class, () -> PSC.update("account").set((Object) null));
        assertThrows(IllegalArgumentException.class, () -> PSC.update("account").set((Class<?>) null));
        assertThrows(IllegalArgumentException.class, () -> PSC.update("account").set((Object) N.asList("firstName")));
        assertThrows(IllegalStateException.class, () -> PSC.select("id").from("account").set(account));
        assertThrows(IllegalStateException.class, () -> PSC.select("id").from("account").set(Account.class));
    }

    /**
     * The key-excluding {@code set(Class, Set)} / {@code set(Object, Set)} overrides validated the argument before the
     * inherited state checks, so a closed or non-UPDATE builder reported IllegalArgumentException for a null class or a
     * non-bean argument instead of the documented IllegalStateException (the parent checks state first).
     */
    @Test
    public void test_set_stateCheckedBeforeEntityArgument() {
        final CqlBuilder closed = PSC.update("account").set("firstName");
        closed.build();

        assertThrows(IllegalStateException.class, () -> closed.set((Class<?>) null));
        assertThrows(IllegalStateException.class, () -> closed.set(Integer.class));
        assertThrows(IllegalStateException.class, () -> closed.set((Object) 3));
        assertThrows(IllegalStateException.class, () -> PSC.select("id").from("account").set(Integer.class));
        assertThrows(IllegalStateException.class, () -> PSC.select("id").from("account").set((Object) 3));

        // Argument errors on a valid UPDATE builder are unchanged.
        assertThrows(IllegalArgumentException.class, () -> PSC.update("account").set((Class<?>) null));
        assertThrows(IllegalArgumentException.class, () -> PSC.update("account").set(Integer.class));
        assertThrows(IllegalArgumentException.class, () -> PSC.update("account").set((Object) 3));
    }

    /**
     * {@code update(Class)} / {@code update(Class, Set)} expand an implicit SET list. Cassandra rejects SET on
     * primary-key columns ("PRIMARY KEY part id found in SET part"), so — like {@code delete(Class)} — the id props
     * must be left out of the SET list (they belong in the WHERE clause). Previously the list contained {@code id}.
     */
    @Test
    public void test_update_entityClass_excludesIdPropsFromSetClause() {
        final SP sp = PSC.update(Account.class).where(Filters.eq("id", 1)).build();
        final String cql = sp.query();
        N.println(cql);

        assertTrue(cql.startsWith("UPDATE account SET "), cql);
        final String setClause = cql.substring(cql.indexOf(" SET ") + " SET ".length(), cql.indexOf(" WHERE "));
        assertFalse(setClause.contains("id = ?"), cql);
        assertTrue(setClause.startsWith("gui = ?, "), cql);
        assertTrue(setClause.contains("first_name = ?"), cql);
        assertTrue(cql.endsWith(" WHERE id = ?"), cql);
        assertEquals(N.asList(1), sp.parameters());

        // Same list when USING is appended before where() (implicit SET expansion path) and for named parameters.
        final String ttlCql = NSC.update(Account.class).usingTTL(60).where(Filters.eq("id", 1)).build().query();
        assertTrue(ttlCql.startsWith("UPDATE account USING TTL 60 SET gui = :gui, "), ttlCql);
        assertFalse(ttlCql.contains("id = :id,"), ttlCql);
        assertTrue(ttlCql.endsWith(" WHERE id = :id"), ttlCql);

        // Explicit exclusions are applied on top of the id exclusion.
        final String excluded = PSC.update(Account.class, N.asSet("gui", "createTime")).where(Filters.eq("id", 1)).build().query();
        assertFalse(excluded.contains("gui = ?"), excluded);
        assertFalse(excluded.contains("create_time = ?"), excluded);
        assertFalse(excluded.contains("SET id = ?"), excluded);
        assertTrue(excluded.contains("first_name = ?"), excluded);

        // An @Id-annotated key is excluded as well.
        final String users = PSC.update(Users.class).where(Filters.eq("id", 1)).build().query();
        assertTrue(users.startsWith("UPDATE simplex.users SET name = ?, "), users);
        assertFalse(users.contains("SET id = ?"), users);

        // An explicit set(...) after update(Class) is unaffected.
        assertEquals("UPDATE account SET first_name = ? WHERE id = ?", PSC.update(Account.class).set("firstName").where(Filters.eq("id", 1)).build().query());
    }

    @Test
    public void update_expressionValues_areRenderedVerbatim() {

        String cql = PSC.update(com.landawn.abacus.da.Account.class)
                .set(N.asMap("firstName", Filters.expr("first_name + 'abc'")))
                .where(Filters.eq("firstName", Filters.expr("first_name + 'abc'")))
                .build()
                .query();

        assertEquals("UPDATE account SET first_name = first_name + 'abc' WHERE first_name = first_name + 'abc'", cql);
    }

    // Additional contracts

    /**
     * Regression guard for {@code onlyIf(Condition)} with an AND junction:
     * Cassandra's LWT grammar is {@code IF columnCondition (AND columnCondition)*} and rejects
     * parenthesized members, so junction members must render WITHOUT per-member parentheses in the
     * IF clause. WHERE uses the same unparenthesized relation-junction grammar.
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

        // WHERE junctions use the same relation AND relation shape.
        final String whereCql = PSC.select("id").from("account").where(Filters.and(Filters.eq("a", 1), Filters.eq("b", 2))).build().query();
        assertEquals("SELECT id FROM account WHERE a = ? AND b = ?", whereCql);
    }
}
