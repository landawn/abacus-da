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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.da.entity.Account;
import com.landawn.abacus.da.entity.Users;
import com.landawn.abacus.query.AbstractQueryBuilder.SP;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.SK;
import com.landawn.abacus.util.Strings;

public class CqlBuilderSelectTest extends TestBase {

    // Queries and results

    @Test
    public void testACCB_countClass() {
        final String cql = ACCB.count(Users.class).build().query();
        assertEquals("SELECT count(*) FROM simplex.users", cql);
    }

    @Test
    public void testACCB_countString() {
        final String cql = ACCB.count("account").build().query();
        assertEquals("SELECT count(*) FROM account", cql);
    }

    @Test
    public void testACCB_selectClass() {
        final String cql = ACCB.select(Account.class).from("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testACCB_selectClassFull() {
        final String cql = ACCB.select(Account.class, false, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testACCB_selectClassWithExcluded() {
        final String cql = ACCB.select(Account.class, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testACCB_selectClassWithSubEntities() {
        final String cql = ACCB.select(Account.class, false).from("account").build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testACCB_selectCollection() {
        final String cql = ACCB.select(N.asList("firstName")).from("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testACCB_selectFromClass() {
        final String cql = ACCB.selectFrom(Users.class).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testACCB_selectFromClassAliasExcluded() {
        final String cql = ACCB.selectFrom(Users.class, null, N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("CREATED_TIME"), cql);
    }

    @Test
    public void testACCB_selectFromClassAliasSubEntities() {
        final String cql = ACCB.selectFrom(Users.class, null, false).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testACCB_selectFromClassFull() {
        final String cql = ACCB.selectFrom(Users.class, null, false, N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("CREATED_TIME"), cql);
    }

    @Test
    public void testACCB_selectFromClassSubEntitiesExcluded() {
        final String cql = ACCB.selectFrom(Account.class, false, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testACCB_selectFromClassWithAlias() {
        final String cql = ACCB.selectFrom(Users.class, (String) null).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testACCB_selectFromClassWithExcluded() {
        final String cql = ACCB.selectFrom(Account.class, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testACCB_selectFromClassWithSubEntities() {
        final String cql = ACCB.selectFrom(Account.class, false).build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testACCB_selectMap() {
        final String cql = ACCB.select(N.asMap("firstName", "fn")).from("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
        assertTrue(cql.contains("\"fn\""), cql);
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

    /**
     * CQL grammar: {@code SELECT ... [WHERE] [GROUP BY] [ORDER BY] [LIMIT] [ALLOW FILTERING]} — ALLOW FILTERING must
     * be the final clause. Previously {@code allowFiltering()} appended the clause immediately, so a later
     * {@code limit(...)}, {@code orderBy(...)}, {@code where(...)} or {@code append(...)} landed after it and produced
     * CQL that Cassandra rejects (e.g. {@code ... ALLOW FILTERING LIMIT 5}).
     */
    @Test
    public void test_allowFiltering_isRenderedLast_regardlessOfCallOrder() {
        final String expected = "SELECT id FROM account WHERE first_name = ? ORDER BY id ASC LIMIT 5 ALLOW FILTERING";

        final SP afFirst = PSC.select("id").from("account").allowFiltering().where(Filters.eq("firstName", "x")).orderByAsc("id").limit(5).build();
        N.println(afFirst.query());
        assertEquals(expected, afFirst.query());
        assertEquals(N.asList("x"), afFirst.parameters());

        final String afMiddle = PSC.select("id").from("account").where(Filters.eq("firstName", "x")).allowFiltering().orderByAsc("id").limit(5).build().query();
        assertEquals(expected, afMiddle);

        final String afLast = PSC.select("id").from("account").where(Filters.eq("firstName", "x")).orderByAsc("id").limit(5).allowFiltering().build().query();
        assertEquals(expected, afLast);

        // append(Condition)/appendIf(...) after allowFiltering() are ordered the same way.
        assertEquals("SELECT id FROM account WHERE id = ? ALLOW FILTERING",
                PSC.select("id").from("account").allowFiltering().appendIf(true, Filters.eq("id", 1)).build().query());
        assertEquals("SELECT id FROM account WHERE id = :id ALLOW FILTERING",
                NSC.select("id").from("account").allowFiltering().append(Filters.eq("id", 1)).build().query());

        // The clause is still emitted exactly once, and without any preceding clause it directly follows FROM.
        final String bare = PSC.select("id").from("account").allowFiltering().build().query();
        assertEquals("SELECT id FROM account ALLOW FILTERING", bare);
        assertEquals(1, Strings.countMatches(afFirst.query(), "ALLOW FILTERING"));

        // apply()/accept() go through build() as well.
        assertEquals(bare, PSC.select("id").from("account").allowFiltering().apply(sp -> sp.query()));
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
        final String countCql = NSC.select(N.asList(SK.COUNT_ALL)).from("account").appendIf(true, Filters.eq("id", Filters.QME)).build().query();
        N.println(countCql);

        assertTrue(countCql.startsWith("SELECT count(*)"), countCql);
        assertTrue(countCql.contains("FROM account"), countCql);
        assertTrue(countCql.contains("WHERE id = :id"), countCql);
        // The core of the fix: a COUNT query must NOT be capped by LIMIT.
        assertTrue(!countCql.toUpperCase().contains("LIMIT"), "Count CQL must not contain a LIMIT clause: " + countCql);

        // Sanity check: the buggy behavior (count = 1 -> limit(1)) would have produced "LIMIT 1",
        // which truncates the aggregated count in Cassandra.
        final String buggyCql = NSC.select(N.asList(SK.COUNT_ALL)).from("account").appendIf(true, Filters.eq("id", Filters.QME)).limit(1).build().query();
        N.println(buggyCql);
        assertTrue(buggyCql.endsWith("LIMIT 1"), buggyCql);
    }

    /**
     * Regression guard for {@code appendOperationBeforeFrom}: {@code distinct()}/{@code selectModifier(...)}
     * called AFTER {@code from(...)} must be inserted right after the emitted SELECT keyword. The
     * override didn't record {@code _selectKeywordEndIdx} (the parent does it right after appending
     * SELECT), so a post-from modifier was silently dropped from the generated CQL.
     */
    @Test
    public void test_distinct_afterFrom_isNotDropped() {
        assertEquals("SELECT DISTINCT name FROM account", PSC.select("name").from("account").distinct().build().query());

        // Same order via the general modifier entry point (SELECT JSON is a real CQL feature; the
        // naming policy may normalize the modifier's case, which CQL treats as equivalent).
        final String json = PSC.select("name").from("account").selectModifier("JSON").build().query();
        assertTrue(json.equalsIgnoreCase("SELECT JSON name FROM account"), json);

        // Non-regression: modifier before from() keeps working.
        assertEquals("SELECT DISTINCT name FROM account", PSC.select("name").distinct().from("account").build().query());
    }

    @Test
    public void testLCCB_countClass() {
        final String cql = LCCB.count(Users.class).build().query();
        assertEquals("SELECT count(*) FROM simplex.users", cql);
    }

    @Test
    public void testLCCB_countString() {
        final String cql = LCCB.count("account").build().query();
        assertEquals("SELECT count(*) FROM account", cql);
    }

    @Test
    public void testLCCB_selectClass() {
        final String cql = LCCB.select(Account.class).from("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testLCCB_selectClassFull() {
        final String cql = LCCB.select(Account.class, false, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testLCCB_selectClassWithExcluded() {
        final String cql = LCCB.select(Account.class, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testLCCB_selectClassWithSubEntities() {
        final String cql = LCCB.select(Account.class, false).from("account").build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testLCCB_selectCollection() {
        final String cql = LCCB.select(N.asList("firstName")).from("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testLCCB_selectFromClass() {
        final String cql = LCCB.selectFrom(Users.class).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testLCCB_selectFromClassAliasExcluded() {
        final String cql = LCCB.selectFrom(Users.class, null, N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("createdTime"), cql);
    }

    @Test
    public void testLCCB_selectFromClassAliasSubEntities() {
        final String cql = LCCB.selectFrom(Users.class, null, false).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testLCCB_selectFromClassFull() {
        final String cql = LCCB.selectFrom(Users.class, null, false, N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("createdTime"), cql);
    }

    @Test
    public void testLCCB_selectFromClassSubEntitiesExcluded() {
        final String cql = LCCB.selectFrom(Account.class, false, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testLCCB_selectFromClassWithAlias() {
        final String cql = LCCB.selectFrom(Users.class, (String) null).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testLCCB_selectFromClassWithExcluded() {
        final String cql = LCCB.selectFrom(Account.class, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testLCCB_selectFromClassWithSubEntities() {
        final String cql = LCCB.selectFrom(Account.class, false).build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testLCCB_selectMap() {
        final String cql = LCCB.select(N.asMap("firstName", "fn")).from("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
        assertTrue(cql.contains("\"fn\""), cql);
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
    public void testNAC_countClass() {
        final String cql = NAC.count(Users.class).build().query();
        assertEquals("SELECT count(*) FROM simplex.users", cql);
    }

    @Test
    public void testNAC_countString() {
        final String cql = NAC.count("account").build().query();
        assertEquals("SELECT count(*) FROM account", cql);
    }

    @Test
    public void testNAC_selectClass() {
        final String cql = NAC.select(Account.class).from("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testNAC_selectClassFull() {
        final String cql = NAC.select(Account.class, false, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testNAC_selectClassWithExcluded() {
        final String cql = NAC.select(Account.class, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testNAC_selectClassWithSubEntities() {
        final String cql = NAC.select(Account.class, false).from("account").build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testNAC_selectCollection() {
        final String cql = NAC.select(N.asList("firstName")).from("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testNAC_selectFromClass() {
        final String cql = NAC.selectFrom(Users.class).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testNAC_selectFromClassAliasExcluded() {
        final String cql = NAC.selectFrom(Users.class, null, N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("CREATED_TIME"), cql);
    }

    @Test
    public void testNAC_selectFromClassAliasSubEntities() {
        final String cql = NAC.selectFrom(Users.class, null, false).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testNAC_selectFromClassFull() {
        final String cql = NAC.selectFrom(Users.class, null, false, N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("CREATED_TIME"), cql);
    }

    @Test
    public void testNAC_selectFromClassSubEntitiesExcluded() {
        final String cql = NAC.selectFrom(Account.class, false, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testNAC_selectFromClassWithAlias() {
        final String cql = NAC.selectFrom(Users.class, (String) null).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testNAC_selectFromClassWithExcluded() {
        final String cql = NAC.selectFrom(Account.class, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testNAC_selectFromClassWithSubEntities() {
        final String cql = NAC.selectFrom(Account.class, false).build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testNAC_selectMap() {
        final String cql = NAC.select(N.asMap("firstName", "fn")).from("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
        assertTrue(cql.contains("\"fn\""), cql);
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
    public void testNLC_countClass() {
        final String cql = NLC.count(Users.class).build().query();
        assertEquals("SELECT count(*) FROM simplex.users", cql);
    }

    @Test
    public void testNLC_countString() {
        final String cql = NLC.count("account").build().query();
        assertEquals("SELECT count(*) FROM account", cql);
    }

    @Test
    public void testNLC_selectClass() {
        final String cql = NLC.select(Account.class).from("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testNLC_selectClassFull() {
        final String cql = NLC.select(Account.class, false, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testNLC_selectClassWithExcluded() {
        final String cql = NLC.select(Account.class, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testNLC_selectClassWithSubEntities() {
        final String cql = NLC.select(Account.class, false).from("account").build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testNLC_selectCollection() {
        final String cql = NLC.select(N.asList("firstName")).from("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testNLC_selectFromClass() {
        final String cql = NLC.selectFrom(Users.class).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testNLC_selectFromClassAliasExcluded() {
        final String cql = NLC.selectFrom(Users.class, null, N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("createdTime"), cql);
    }

    @Test
    public void testNLC_selectFromClassAliasSubEntities() {
        final String cql = NLC.selectFrom(Users.class, null, false).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testNLC_selectFromClassFull() {
        final String cql = NLC.selectFrom(Users.class, null, false, N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("createdTime"), cql);
    }

    @Test
    public void testNLC_selectFromClassSubEntitiesExcluded() {
        final String cql = NLC.selectFrom(Account.class, false, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testNLC_selectFromClassWithAlias() {
        final String cql = NLC.selectFrom(Users.class, (String) null).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testNLC_selectFromClassWithExcluded() {
        final String cql = NLC.selectFrom(Account.class, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testNLC_selectFromClassWithSubEntities() {
        final String cql = NLC.selectFrom(Account.class, false).build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testNLC_selectMap() {
        final String cql = NLC.select(N.asMap("firstName", "fn")).from("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
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
    public void testNSB_countClass() {
        final String cql = NSB.count(Users.class).where(Filters.eq("id", Filters.QME)).build().query();
        assertTrue(cql.startsWith("SELECT count(*) FROM simplex.users"), cql);
    }

    @Test
    public void testNSB_countString() {
        final String cql = NSB.count("account").where(Filters.eq("id", 1)).build().query();
        assertEquals("SELECT count(*) FROM account WHERE id = :id", cql);
    }

    @Test
    public void testNSB_selectClass() {
        final String cql = NSB.select(Account.class).from("account").build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testNSB_selectClassFull() {
        final String cql = NSB.select(Account.class, false, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testNSB_selectClassWithExcluded() {
        final String cql = NSB.select(Account.class, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testNSB_selectClassWithSubEntities() {
        final String cql = NSB.select(Account.class, false).from("account").build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testNSB_selectCollection() {
        final String cql = NSB.select(N.asList("firstName", "lastName")).from("account").build().query();
        assertEquals("SELECT firstName, lastName FROM account", cql);
    }

    @Test
    public void testNSB_selectFromClass() {
        final String cql = NSB.selectFrom(Users.class).build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testNSB_selectFromClassAliasExcluded() {
        final String cql = NSB.selectFrom(Users.class, null, N.asSet("createdTime")).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
        assertTrue(!cql.contains("createdTime"), cql);
    }

    @Test
    public void testNSB_selectFromClassAliasSubEntities() {
        final String cql = NSB.selectFrom(Users.class, null, false).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testNSB_selectFromClassFull() {
        final String cql = NSB.selectFrom(Users.class, null, false, N.asSet("createdTime")).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
        assertTrue(!cql.contains("createdTime"), cql);
    }

    @Test
    public void testNSB_selectFromClassSubEntitiesExcluded() {
        final String cql = NSB.selectFrom(Account.class, false, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testNSB_selectFromClassWithAlias() {
        final String cql = NSB.selectFrom(Users.class, (String) null).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testNSB_selectFromClassWithExcluded() {
        final String cql = NSB.selectFrom(Account.class, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testNSB_selectFromClassWithSubEntities() {
        final String cql = NSB.selectFrom(Account.class, false).build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testNSB_selectMap() {
        final String cql = NSB.select(N.asMap("firstName", "fn")).from("account").build().query();
        assertTrue(cql.startsWith("SELECT firstName"), cql);
        assertTrue(cql.contains("\"fn\""), cql);
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
    public void testNSC_countClass() {
        final String cql = NSC.count(Users.class).where(Filters.eq("id", Filters.QME)).build().query();
        assertTrue(cql.startsWith("SELECT count(*) FROM simplex.users"), cql);
    }

    @Test
    public void testNSC_countString() {
        final String cql = NSC.count("account").where(Filters.eq("id", 1)).build().query();
        assertEquals("SELECT count(*) FROM account WHERE id = :id", cql);
    }

    @Test
    public void testNSC_selectClass() {
        final String cql = NSC.select(Account.class).from("account").build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
        assertTrue(cql.contains("first_name"), cql);
    }

    @Test
    public void testNSC_selectClassFull() {
        final String cql = NSC.select(Account.class, false, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains(" first_name"), cql);
    }

    @Test
    public void testNSC_selectClassWithExcluded() {
        final String cql = NSC.select(Account.class, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains(" first_name"), cql);
    }

    @Test
    public void testNSC_selectClassWithSubEntities() {
        final String cql = NSC.select(Account.class, false).from("account").build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testNSC_selectCollection() {
        final String cql = NSC.select(N.asList("firstName", "lastName")).from("account").build().query();
        assertEquals("SELECT first_name AS \"firstName\", last_name AS \"lastName\" FROM account", cql);
    }

    @Test
    public void testNSC_selectFromClass() {
        final String cql = NSC.selectFrom(Users.class).build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testNSC_selectFromClassAliasExcluded() {
        final String cql = NSC.selectFrom(Users.class, null, N.asSet("createdTime")).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
        assertTrue(!cql.contains("created_time"), cql);
    }

    @Test
    public void testNSC_selectFromClassAliasSubEntities() {
        final String cql = NSC.selectFrom(Users.class, null, false).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testNSC_selectFromClassFull() {
        final String cql = NSC.selectFrom(Users.class, null, false, N.asSet("createdTime")).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
        assertTrue(!cql.contains("created_time"), cql);
    }

    @Test
    public void testNSC_selectFromClassSubEntitiesExcluded() {
        final String cql = NSC.selectFrom(Account.class, false, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains(" first_name"), cql);
    }

    @Test
    public void testNSC_selectFromClassWithAlias() {
        final String cql = NSC.selectFrom(Users.class, (String) null).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testNSC_selectFromClassWithExcluded() {
        final String cql = NSC.selectFrom(Account.class, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains(" first_name"), cql);
    }

    @Test
    public void testNSC_selectFromClassWithSubEntities() {
        final String cql = NSC.selectFrom(Account.class, false).build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testNSC_selectMap() {
        final String cql = NSC.select(N.asMap("firstName", "fn")).from("account").build().query();
        assertTrue(cql.startsWith("SELECT first_name"), cql);
        assertTrue(cql.contains("\"fn\""), cql);
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
    public void testPAC_countClass() {
        final String cql = PAC.count(Users.class).build().query();
        assertEquals("SELECT count(*) FROM simplex.users", cql);
    }

    @Test
    public void testPAC_countString() {
        final String cql = PAC.count("account").build().query();
        assertEquals("SELECT count(*) FROM account", cql);
    }

    @Test
    public void testPAC_selectClass() {
        final String cql = PAC.select(Account.class).from("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testPAC_selectClassFull() {
        final String cql = PAC.select(Account.class, false, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testPAC_selectClassWithExcluded() {
        final String cql = PAC.select(Account.class, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testPAC_selectClassWithSubEntities() {
        final String cql = PAC.select(Account.class, false).from("account").build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testPAC_selectCollection() {
        final String cql = PAC.select(N.asList("firstName")).from("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testPAC_selectFromClass() {
        final String cql = PAC.selectFrom(Users.class).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testPAC_selectFromClassAliasExcluded() {
        final String cql = PAC.selectFrom(Users.class, null, N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("CREATED_TIME"), cql);
    }

    @Test
    public void testPAC_selectFromClassAliasSubEntities() {
        final String cql = PAC.selectFrom(Users.class, null, false).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testPAC_selectFromClassFull() {
        final String cql = PAC.selectFrom(Users.class, null, false, N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("CREATED_TIME"), cql);
    }

    @Test
    public void testPAC_selectFromClassSubEntitiesExcluded() {
        final String cql = PAC.selectFrom(Account.class, false, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testPAC_selectFromClassWithAlias() {
        final String cql = PAC.selectFrom(Users.class, (String) null).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testPAC_selectFromClassWithExcluded() {
        final String cql = PAC.selectFrom(Account.class, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains("FIRST_NAME"), cql);
    }

    @Test
    public void testPAC_selectFromClassWithSubEntities() {
        final String cql = PAC.selectFrom(Account.class, false).build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testPAC_selectMap() {
        final String cql = PAC.select(N.asMap("firstName", "fn")).from("account").build().query();
        assertTrue(cql.contains("FIRST_NAME"), cql);
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
    public void testPLC_countClass() {
        final String cql = PLC.count(Users.class).build().query();
        assertEquals("SELECT count(*) FROM simplex.users", cql);
    }

    @Test
    public void testPLC_countString() {
        final String cql = PLC.count("account").build().query();
        assertEquals("SELECT count(*) FROM account", cql);
    }

    @Test
    public void testPLC_selectClass() {
        final String cql = PLC.select(Account.class).from("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testPLC_selectClassFull() {
        final String cql = PLC.select(Account.class, false, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testPLC_selectClassWithExcluded() {
        final String cql = PLC.select(Account.class, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testPLC_selectClassWithSubEntities() {
        final String cql = PLC.select(Account.class, false).from("account").build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testPLC_selectCollection() {
        final String cql = PLC.select(N.asList("firstName")).from("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testPLC_selectFromClass() {
        final String cql = PLC.selectFrom(Users.class).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testPLC_selectFromClassAliasExcluded() {
        final String cql = PLC.selectFrom(Users.class, null, N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("createdTime"), cql);
    }

    @Test
    public void testPLC_selectFromClassAliasSubEntities() {
        final String cql = PLC.selectFrom(Users.class, null, false).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testPLC_selectFromClassFull() {
        final String cql = PLC.selectFrom(Users.class, null, false, N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("createdTime"), cql);
    }

    @Test
    public void testPLC_selectFromClassSubEntitiesExcluded() {
        final String cql = PLC.selectFrom(Account.class, false, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testPLC_selectFromClassWithAlias() {
        final String cql = PLC.selectFrom(Users.class, (String) null).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testPLC_selectFromClassWithExcluded() {
        final String cql = PLC.selectFrom(Account.class, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testPLC_selectFromClassWithSubEntities() {
        final String cql = PLC.selectFrom(Account.class, false).build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testPLC_selectMap() {
        final String cql = PLC.select(N.asMap("firstName", "fn")).from("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
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
    public void testPSB_countClass() {
        final String cql = PSB.count(Users.class).build().query();
        assertEquals("SELECT count(*) FROM simplex.users", cql);
    }

    @Test
    public void testPSB_countString() {
        final String cql = PSB.count("account").where(Filters.eq("id", 1)).build().query();
        assertEquals("SELECT count(*) FROM account WHERE id = ?", cql);
    }

    @Test
    public void testPSB_selectClass() {
        final String cql = PSB.select(Account.class).from("account").build().query();
        assertTrue(cql.contains("firstName"), cql);
    }

    @Test
    public void testPSB_selectClassFull() {
        final String cql = PSB.select(Account.class, false, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testPSB_selectClassWithExcluded() {
        final String cql = PSB.select(Account.class, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testPSB_selectClassWithSubEntities() {
        final String cql = PSB.select(Account.class, false).from("account").build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testPSB_selectCollection() {
        final String cql = PSB.select(N.asList("firstName", "lastName")).from("account").build().query();
        assertEquals("SELECT firstName, lastName FROM account", cql);
    }

    @Test
    public void testPSB_selectFromClass() {
        final String cql = PSB.selectFrom(Users.class).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testPSB_selectFromClassAliasExcluded() {
        final String cql = PSB.selectFrom(Users.class, null, N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("createdTime"), cql);
    }

    @Test
    public void testPSB_selectFromClassAliasSubEntities() {
        final String cql = PSB.selectFrom(Users.class, null, false).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testPSB_selectFromClassFull() {
        final String cql = PSB.selectFrom(Users.class, null, false, N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("createdTime"), cql);
    }

    @Test
    public void testPSB_selectFromClassSubEntitiesExcluded() {
        final String cql = PSB.selectFrom(Account.class, false, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testPSB_selectFromClassWithAlias() {
        final String cql = PSB.selectFrom(Users.class, (String) null).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testPSB_selectFromClassWithExcluded() {
        final String cql = PSB.selectFrom(Account.class, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains(" firstName"), cql);
    }

    @Test
    public void testPSB_selectFromClassWithSubEntities() {
        final String cql = PSB.selectFrom(Account.class, false).build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testPSB_selectMap() {
        final String cql = PSB.select(N.asMap("firstName", "fn")).from("account").build().query();
        assertTrue(cql.contains("\"fn\""), cql);
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
    public void testPSC_countClass() {
        final String cql = PSC.count(Users.class).build().query();
        assertEquals("SELECT count(*) FROM simplex.users", cql);
    }

    @Test
    public void testPSC_countString() {
        final String cql = PSC.count("account").where(Filters.eq("id", 1)).build().query();
        assertEquals("SELECT count(*) FROM account WHERE id = ?", cql);
    }

    @Test
    public void testPSC_selectClass() {
        final String cql = PSC.select(Account.class).from("account").build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
        assertTrue(cql.contains("first_name"), cql);
    }

    @Test
    public void testPSC_selectClassFull() {
        final String cql = PSC.select(Account.class, false, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains(" first_name"), cql);
    }

    @Test
    public void testPSC_selectClassWithExcluded() {
        final String cql = PSC.select(Account.class, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains(" first_name"), cql);
    }

    @Test
    public void testPSC_selectClassWithSubEntities() {
        final String cql = PSC.select(Account.class, false).from("account").build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testPSC_selectCollection() {
        final String cql = PSC.select(N.asList("firstName", "lastName")).from("account").build().query();
        assertEquals("SELECT first_name AS \"firstName\", last_name AS \"lastName\" FROM account", cql);
    }

    @Test
    public void testPSC_selectFromClass() {
        final String cql = PSC.selectFrom(Users.class).build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testPSC_selectFromClassAliasExcluded() {
        final String cql = PSC.selectFrom(Users.class, null, N.asSet("createdTime")).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
        assertTrue(!cql.contains("created_time"), cql);
    }

    @Test
    public void testPSC_selectFromClassAliasSubEntities() {
        final String cql = PSC.selectFrom(Users.class, null, false).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testPSC_selectFromClassFull() {
        final String cql = PSC.selectFrom(Users.class, null, false, N.asSet("createdTime")).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
        assertTrue(!cql.contains("created_time"), cql);
    }

    @Test
    public void testPSC_selectFromClassSubEntitiesExcluded() {
        final String cql = PSC.selectFrom(Account.class, false, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains(" first_name"), cql);
    }

    @Test
    public void testPSC_selectFromClassWithAlias() {
        final String cql = PSC.selectFrom(Users.class, (String) null).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testPSC_selectFromClassWithExcluded() {
        final String cql = PSC.selectFrom(Account.class, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains(" first_name"), cql);
    }

    @Test
    public void testPSC_selectFromClassWithSubEntities() {
        final String cql = PSC.selectFrom(Account.class, false).build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testPSC_selectMap() {
        final String cql = PSC.select(N.asMap("firstName", "fn")).from("account").build().query();
        assertTrue(cql.startsWith("SELECT first_name"), cql);
        assertTrue(cql.contains("\"fn\""), cql);
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
    public void testSCCB_countClass() {
        final String cql = SCCB.count(Users.class).build().query();
        assertEquals("SELECT count(*) FROM simplex.users", cql);
    }

    @Test
    public void testSCCB_countString() {
        final String cql = SCCB.count("account").build().query();
        assertEquals("SELECT count(*) FROM account", cql);
    }

    @Test
    public void testSCCB_selectClass() {
        final String cql = SCCB.select(Account.class).from("account").build().query();
        assertTrue(cql.contains("first_name"), cql);
    }

    @Test
    public void testSCCB_selectClassFull() {
        final String cql = SCCB.select(Account.class, false, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains("first_name"), cql);
    }

    @Test
    public void testSCCB_selectClassWithExcluded() {
        final String cql = SCCB.select(Account.class, N.asSet("firstName")).from("account").build().query();
        assertTrue(!cql.contains("first_name"), cql);
    }

    @Test
    public void testSCCB_selectClassWithSubEntities() {
        final String cql = SCCB.select(Account.class, false).from("account").build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testSCCB_selectCollection() {
        final String cql = SCCB.select(N.asList("firstName")).from("account").build().query();
        assertTrue(cql.contains("first_name"), cql);
    }

    @Test
    public void testSCCB_selectFromClass() {
        final String cql = SCCB.selectFrom(Users.class).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testSCCB_selectFromClassAliasExcluded() {
        final String cql = SCCB.selectFrom(Users.class, null, N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("created_time"), cql);
    }

    @Test
    public void testSCCB_selectFromClassAliasSubEntities() {
        final String cql = SCCB.selectFrom(Users.class, null, false).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testSCCB_selectFromClassFull() {
        final String cql = SCCB.selectFrom(Users.class, null, false, N.asSet("createdTime")).build().query();
        assertTrue(!cql.contains("created_time"), cql);
    }

    @Test
    public void testSCCB_selectFromClassSubEntitiesExcluded() {
        final String cql = SCCB.selectFrom(Account.class, false, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains("first_name"), cql);
    }

    @Test
    public void testSCCB_selectFromClassWithAlias() {
        final String cql = SCCB.selectFrom(Users.class, (String) null).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
    }

    @Test
    public void testSCCB_selectFromClassWithExcluded() {
        final String cql = SCCB.selectFrom(Account.class, N.asSet("firstName")).build().query();
        assertTrue(!cql.contains("first_name"), cql);
    }

    @Test
    public void testSCCB_selectFromClassWithSubEntities() {
        final String cql = SCCB.selectFrom(Account.class, false).build().query();
        assertTrue(cql.startsWith("SELECT "), cql);
    }

    @Test
    public void testSCCB_selectMap() {
        final String cql = SCCB.select(N.asMap("firstName", "fn")).from("account").build().query();
        assertTrue(cql.contains("first_name"), cql);
        assertTrue(cql.contains("\"fn\""), cql);
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

    // Resource lifecycle

    /**
     * A second {@code from(...)} in the same statement would silently emit a second operation
     * fragment; the builder must reject it up front (mirrors the parent builder's 4.8.9 guard).
     */
    @Test
    public void test_from_calledTwice_throwsIllegalStateException() {
        final CqlBuilder selectBuilder = PSC.select("firstName").from("account");
        assertThrows(IllegalStateException.class, () -> selectBuilder.from("account2"));

        final CqlBuilder deleteBuilder = PSC.delete("firstName").from(Account.class);
        assertThrows(IllegalStateException.class, () -> deleteBuilder.from(Account.class));
    }
}
