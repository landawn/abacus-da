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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.QueryBuilderLeakAssertions;
import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.da.entity.Account;
import com.landawn.abacus.da.entity.Users;
import com.landawn.abacus.query.AbstractQueryBuilder.SP;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.util.N;

public class CqlBuilderValidationTest extends TestBase {

    // Row and entity mapping

    @Test
    public void testNSC_parse_NullCondition() {
        assertThrows(IllegalArgumentException.class, () -> NSC.renderCondition(null, Account.class));
    }

    // Factories and insert operations

    /**
     * Regression guard: the batch-insert {@code into()} override dereferences the internal buffer
     * directly ({@code _propsList} survives {@code build()}), so without an up-front
     * {@code assertNotClosed()} a closed batch builder threw NullPointerException instead of the
     * documented closed-state IllegalStateException.
     */
    @Test
    public void test_batchInsert_intoAfterBuild_throwsIllegalStateException() {
        final CqlBuilder builder = PSC.batchInsert(N.asList(N.asMap("id", 1), N.asMap("id", 2))).into("account");
        builder.build();
        assertThrows(IllegalStateException.class, () -> builder.into("account2"));
    }

    @Test
    public void test_batchInsert_rejectsEmptyRows() {
        assertThrows(IllegalArgumentException.class, () -> PSC.batchInsert(N.asList(N.<String, Object> newHashMap())).into("account"));
    }

    @Test
    public void test_batchInsert_rejectsRowsWithDifferentPropertySets() {
        assertThrows(IllegalArgumentException.class,
                () -> PSC.batchInsert(N.asList(N.asMap("id", 1, "name", "a"), N.asMap("id", 2, "email", "b"))).into("account"));
    }

    // Argument-validation smoke tests for each variant (exercise the precondition branches).

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
    public void testLCCB_insert_EmptyString() {
        assertThrows(IllegalArgumentException.class, () -> LCCB.insert(""));
    }

    @Test
    public void testNSB_insert_EmptyString() {
        assertThrows(IllegalArgumentException.class, () -> NSB.insert(""));
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
    public void testPSC_insert_EmptyString() {
        assertThrows(IllegalArgumentException.class, () -> PSC.insert(""));
    }

    @Test
    public void testSCCB_insert_EmptyString() {
        assertThrows(IllegalArgumentException.class, () -> SCCB.insert(""));
    }

    /**
     * A Dsl factory that failed after allocating its builder (non-bean class, unsupported condition, mismatched batch
     * rows, ...) never released it, permanently inflating the parent's active-builder count (which logs a warning/error
     * on every builder once it passes 512/1024). The parent SqlBuilder Dsl releases such builders.
     */
    @Test
    public void test_sliceQ_failedDslFactory_releasesBuilder() throws Exception {
        final java.util.concurrent.atomic.AtomicInteger counter = QueryBuilderLeakAssertions.activeStringBuilderCounter();
        final int before = counter.get();

        assertThrows(IllegalArgumentException.class, () -> PSC.insert(Integer.valueOf(3)));
        assertThrows(IllegalArgumentException.class, () -> PSC.insert(Integer.class));
        assertThrows(IllegalArgumentException.class, () -> PSC.insertInto(Integer.class));
        assertThrows(IllegalArgumentException.class, () -> PSC.batchInsert(N.asList(N.asMap("a", 1), N.asMap("b", 2))));
        assertThrows(IllegalArgumentException.class, () -> PSC.update(Integer.class));
        assertThrows(IllegalArgumentException.class, () -> PSC.delete(Integer.class));
        assertThrows(IllegalArgumentException.class, () -> PSC.deleteFrom(Integer.class));
        assertThrows(IllegalArgumentException.class, () -> PSC.select(Integer.class));
        assertThrows(IllegalArgumentException.class, () -> PSC.selectFrom(Integer.class));
        assertThrows(IllegalArgumentException.class, () -> PSC.count(Integer.class));
        assertThrows(IllegalArgumentException.class, () -> PSC.renderCondition(Filters.or(Filters.eq("a", 1), Filters.eq("b", 2)), Account.class));

        assertEquals(before, counter.get());
    }

    /** Factories whose set-up fails in a later step (entity metadata, derived table name) release their builder too. */
    @Test
    public void test_verifyQB_failedDslFactory_releasesBuilder_afterLaterSetUpStep() throws Exception {
        final java.util.concurrent.atomic.AtomicInteger counter = QueryBuilderLeakAssertions.activeStringBuilderCounter();
        final int before = counter.get();

        assertThrows(UnsupportedOperationException.class, () -> PSC.update("t", VerifyQbLongDateEntity.class));
        assertThrows(UnsupportedOperationException.class, () -> PSC.deleteFrom("t", VerifyQbLongDateEntity.class));
        assertThrows(UnsupportedOperationException.class, () -> NSC.update(VerifyQbLongDateEntity.class));
        assertThrows(UnsupportedOperationException.class, () -> NSC.insert(new VerifyQbLongDateEntity()));
        assertThrows(UnsupportedOperationException.class, () -> PSC.renderCondition(Filters.eq("id", 1), VerifyQbLongDateEntity.class));

        // insert(Class)/select(..) succeed; the derived table "a b" is rejected by the factory's own into(..)/from(..) step.
        assertThrows(IllegalArgumentException.class, () -> PSC.insertInto(VerifyQbBadTableEntity.class));
        assertThrows(IllegalArgumentException.class, () -> PSC.selectFrom(VerifyQbBadTableEntity.class));
        assertThrows(IllegalArgumentException.class, () -> PSC.count(VerifyQbBadTableEntity.class));

        // update(Class)/deleteFrom(Class) also reject the derived table "a b" up front; previously they assigned
        // it without validation and silently rendered invalid CQL.
        assertThrows(IllegalArgumentException.class, () -> PSC.update(VerifyQbBadTableEntity.class));
        assertThrows(IllegalArgumentException.class, () -> PSC.update(VerifyQbBadTableEntity.class, null));
        assertThrows(IllegalArgumentException.class, () -> PSC.deleteFrom(VerifyQbBadTableEntity.class));
        assertThrows(IllegalArgumentException.class, () -> NSC.update(VerifyQbBadTableEntity.class));
        assertThrows(IllegalArgumentException.class, () -> NSC.deleteFrom(VerifyQbBadTableEntity.class));

        assertEquals(before, counter.get());

        // A successful factory still hands its live builder to the caller, whose build() releases it.
        final CqlBuilder builder = PSC.selectFrom(Account.class);
        assertEquals(before + 1, counter.get());
        builder.build();
        assertEquals(before, counter.get());
    }

    // Updates and deletes

    /**
     * A column DELETE whose {@code from(...)} was never called used to build as "DELETE FROM null ...", silently
     * dropping the columns; it is now rejected like a SELECT without FROM. Whole-row {@code deleteFrom(...)} is unaffected.
     */
    @Test
    public void test_deleteColumns_withoutFrom_isRejectedInsteadOfDeleteFromNull() {
        assertThrows(IllegalStateException.class, () -> PSC.delete("firstName").where(Filters.eq("id", 1)));
        assertThrows(IllegalStateException.class, () -> PSC.delete("firstName").build());
        assertThrows(IllegalStateException.class, () -> PSC.delete(Account.class).where(Filters.eq("id", 1)));

        assertEquals("DELETE FROM account WHERE id = ?", PSC.deleteFrom("account").where(Filters.eq("id", 1)).build().query());
        assertEquals("DELETE FROM account WHERE id = ?", PSC.deleteFrom(Account.class).where(Filters.eq("id", 1)).build().query());
        assertEquals("DELETE FROM account", PSC.deleteFrom("account").build().query());
    }

    /**
     * {@code deleteFrom(...)} already names the table, but a following {@code from(...)} was accepted: before any
     * other clause it silently retargeted the statement, and after {@code where(...)} it appended a second statement
     * ("DELETE FROM account WHERE id = ?DELETE FROM other"). It is now rejected and the builder is left usable.
     */
    @Test
    public void test_deleteFrom_thenFrom_isRejected() {
        final CqlBuilder afterWhere = PSC.deleteFrom("account").where(Filters.eq("id", 1));
        assertThrows(IllegalStateException.class, () -> afterWhere.from("other"));
        assertEquals("DELETE FROM account WHERE id = ?", afterWhere.build().query());

        final CqlBuilder beforeWhere = PSC.deleteFrom(Account.class);
        assertThrows(IllegalStateException.class, () -> beforeWhere.from("other"));
        assertThrows(IllegalStateException.class, () -> beforeWhere.from(Account.class));
        assertThrows(IllegalStateException.class, () -> beforeWhere.from("other", Account.class));
        assertThrows(IllegalStateException.class, () -> beforeWhere.from(N.asList("other")));
        assertEquals("DELETE FROM account WHERE id = ?", beforeWhere.where(Filters.eq("id", 1)).build().query());

        // A column DELETE still takes its table from from(...).
        assertEquals("DELETE first_name FROM account WHERE id = ?", PSC.delete("firstName").from("account").where(Filters.eq("id", 1)).build().query());
    }

    @Test
    public void testNSC_deleteFrom_NullString() {
        assertThrows(IllegalArgumentException.class, () -> NSC.deleteFrom((String) null));
    }

    @Test
    public void testNSC_update_NullString() {
        assertThrows(IllegalArgumentException.class, () -> NSC.update((String) null));
    }

    // Queries and results

    @Test
    public void testNSC_count_NullClass() {
        assertThrows(IllegalArgumentException.class, () -> NSC.count((Class<?>) null));
    }

    @Test
    public void testNSC_select_NullString() {
        assertThrows(IllegalArgumentException.class, () -> NSC.select((String) null));
    }

    @Test
    public void testOnlyIfRejectsMissingCondition() {
        assertThrows(IllegalArgumentException.class, () -> PSC.update("account").set("name").where(Filters.eq("id", 1)).onlyIf((String) null));
        assertThrows(IllegalArgumentException.class, () -> PSC.update("account").set("name").where(Filters.eq("id", 1)).onlyIf("   "));
        assertThrows(IllegalArgumentException.class, () -> PSC.update("account").set("name").where(Filters.eq("id", 1)).onlyIf((Condition) null));
    }

    /**
     * onlyIf(Condition) renders the IF clause atomically. appendCondition rejects the constructs CQL has no
     * syntax for; without a rollback the half-emitted clause would remain and build() would silently return
     * corrupt CQL such as "... IF ", "... IF s = ? AND " or "... IF s IN (?, ".
     */
    @Test
    public void test_onlyIf_rejectedCondition_leavesBuilderUnchanged() {
        final String expected = "UPDATE account SET first_name = ? WHERE id = ?";

        final CqlBuilder afterBetween = PSC.update("account").set("firstName").where(Filters.eq("id", 1));
        assertThrows(IllegalArgumentException.class, () -> afterBetween.onlyIf(Filters.between("age", 1, 2)));
        assertEquals(expected, afterBetween.build().query());

        final CqlBuilder afterNullInJunction = PSC.update("account").set("firstName").where(Filters.eq("id", 1));
        assertThrows(IllegalArgumentException.class, () -> afterNullInJunction.onlyIf(Filters.and(Filters.eq("status", "x"), Filters.gt("version", null))));
        assertEquals(expected, afterNullInJunction.build().query());

        final CqlBuilder afterNullInElement = PSC.update("account").set("firstName").where(Filters.eq("id", 1));
        assertThrows(IllegalArgumentException.class, () -> afterNullInElement.onlyIf(Filters.in("status", N.asList("a", null))));
        assertEquals(expected, afterNullInElement.build().query());

        // After a rejected onlyIf the builder is still usable: a valid IF clause renders normally,
        // and no stale bind value was left behind by the rejected attempt.
        final CqlBuilder recovered = PSC.update("account").set("firstName").where(Filters.eq("id", 1));
        assertThrows(IllegalArgumentException.class, () -> recovered.onlyIf(Filters.between("age", 1, 2)));
        final SP recoveredSp = recovered.onlyIf(Filters.eq("status", "inactive")).build();
        assertEquals(expected + " IF status = ?", recoveredSp.query());
        assertEquals(2, recoveredSp.parameters().size());
    }

    @Test
    public void test_onlyIf_rejectedCondition_restoresNamedParameterState() {
        final CqlBuilder recovered = NSC.update("account").set(Map.of("name", "new")).where(Filters.eq("id", 1));
        final Condition rejected = Filters.and(Filters.eq("status", "unused"), Filters.between("version", 1, 2));

        assertThrows(IllegalArgumentException.class, () -> recovered.onlyIf(rejected));

        final SP actual = recovered.onlyIf(Filters.eq("status", "old")).build();
        final SP expected = NSC.update("account").set(Map.of("name", "new")).where(Filters.eq("id", 1)).onlyIf(Filters.eq("status", "old")).build();
        assertEquals(expected.query(), actual.query());
        assertEquals(expected.parameters(), actual.parameters());
        assertEquals(Map.of(0, "name", 1, "id", 2, "status"), ParsedCql.parse(actual.query()).namedParameters());
    }

    /**
     * {@code renderCondition(cond, null)} is a documented null-as-default: without an entity class the
     * property names are still normalized by the DSL's naming policy, only the entity-based
     * column mapping is skipped. A null condition remains an illegal argument.
     */
    @Test
    public void test_renderCondition_nullEntityClass_isAccepted() {
        final String cql = PSC.renderCondition(Filters.eq("firstName", "John"), null).build().query();
        assertTrue(cql.contains("first_name"), cql);
        assertTrue(cql.contains("?"), cql);

        assertEquals("firstName = :firstName", NSB.renderCondition(Filters.eq("firstName", "John"), null).build().query().trim());

        assertThrows(IllegalArgumentException.class, () -> PSC.renderCondition(null, null));
    }

    /** Cassandra CQL has no subqueries; both positional and named builders must fail fast. */
    @Test
    public void test_subQuery_isRejected() {
        final Condition cond = Filters.in("id", Filters.subQuery("account", N.asList("id"), Filters.eq("lastName", "Smith")));
        assertThrows(IllegalArgumentException.class, () -> PSC.select("firstName").from("account").where(cond).build());

        final Condition namedCond = Filters.in("id", Filters.subQuery("account", N.asList("id"), Filters.eq("lastName", "Smith")));
        assertThrows(IllegalArgumentException.class, () -> NSC.select("firstName").from("account").where(namedCond).build());
    }

    @Test
    public void test_subQuery_withNullCondition_isRejected() {
        final Condition cond = Filters.in("id", Filters.subQuery("account", N.asList("id"), (Condition) null));
        assertThrows(IllegalArgumentException.class, () -> PSC.select("firstName").from("account").where(cond).build());
    }

    // Resource lifecycle

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

    // Additional contracts

    /** BETWEEN is SQL syntax, not a Cassandra CQL relation. */
    @Test
    public void test_between_namedParams_areRejected() {
        assertThrows(IllegalArgumentException.class, () -> NSC.select("firstName").from("account").where(Filters.between("o.orderDate", "a", "b")).build());
        assertThrows(IllegalArgumentException.class, () -> NSC.select("id").from("account").where(Filters.between("age", 18, 65)).build());
    }

    @Test
    public void testComposableCellPredicatesAreRejected() {
        final IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> PSC.select("id").from("account").where(Filters.not(Filters.eq("status", "X"))));

        assertTrue(error.getMessage().contains("does not support composable predicate"), error.getMessage());
    }

    /**
     * A FROM rejected while rendering the staged columns must leave the builder as it was: no partial
     * "DELETE ..." text, no FROM flag, and no entity mapping installed by {@code from(Class)} /
     * {@code from(String, Class)} (they used to call {@code setEntityClass} before the rendering that failed).
     */
    @Test
    public void test_from_rejectedColumn_leavesBuilderUnchanged() throws Exception {
        final CqlBuilder deleteBuilder = PSC.delete("first--name");

        assertThrows(IllegalArgumentException.class, () -> deleteBuilder.from(Account.class));
        assertNull(entityClassOf(deleteBuilder));
        assertThrows(IllegalArgumentException.class, () -> deleteBuilder.from("account", Account.class));
        assertNull(entityClassOf(deleteBuilder));
        // Still no FROM: a retry fails on the column again (not on "from() has already been called"), and build()
        // reports the missing FROM instead of returning a truncated statement.
        assertThrows(IllegalArgumentException.class, () -> deleteBuilder.from("account"));
        assertThrows(IllegalStateException.class, deleteBuilder::build);

        final CqlBuilder selectBuilder = PSC.select("first--name");

        assertThrows(IllegalArgumentException.class, () -> selectBuilder.from(Account.class));
        assertNull(entityClassOf(selectBuilder));
        assertThrows(IllegalArgumentException.class, () -> selectBuilder.from("account", Account.class));
        assertNull(entityClassOf(selectBuilder));
    }

    /**
     * abacus-query 4.9.3 added {@code from(builder, alias)} (derived table), which rendered
     * {@code SELECT * FROM (SELECT ...) u} — CQL has neither sub-queries in FROM nor table aliases. It is rejected
     * without consuming the child builder, and the rejected call leaves the outer builder usable.
     */
    @Test
    public void test_fromDerivedTable_isRejected() {
        final CqlBuilder child = PSC.select("id").from("users").where(Filters.eq("id", 1));
        final CqlBuilder outer = PSC.select("*");

        assertThrows(IllegalArgumentException.class, () -> outer.from(child, "u"));

        assertEquals("SELECT id FROM users WHERE id = ?", child.build().query());
        assertEquals("SELECT * FROM users", outer.from("users").build().query());

        // Builder state is still checked first.
        final CqlBuilder closed = PSC.select("id").from("users");
        closed.build();
        assertThrows(IllegalStateException.class, () -> closed.from(PSC.select("id"), "u"));
        assertThrows(IllegalStateException.class, () -> PSC.update("account").from(PSC.select("id"), "u"));
    }

    /**
     * {@code into(String, Class)} applies the same single-table check as {@code into(String)}; with abacus-query
     * 4.9.3 it rendered {@code INSERT INTO account a (...)}. A rejected call leaves the builder usable.
     */
    @Test
    public void test_intoTableNameAndEntityClass_rejectsAlias() {
        final CqlBuilder builder = PSC.insert("firstName");
        assertThrows(IllegalArgumentException.class, () -> builder.into("account a", Account.class));
        assertEquals("INSERT INTO account (first_name) VALUES (?)", builder.into("account", Account.class).build().query());

        assertThrows(IllegalArgumentException.class, () -> PSC.insert("firstName").into((Class<?>) null));
        assertThrows(IllegalStateException.class, () -> PSC.update("account").into(Account.class));
    }

    /**
     * abacus-query 4.9.4 rejects a comment-only raw expression, but {@code onlyIf(String)} had already emitted
     * " IF ": build() returned "... IF " and a retry produced "IF  IF ...". The call is now atomic.
     */
    @Test
    public void test_onlyIfString_rejectedExpression_leavesBuilderUnchanged() {
        final CqlBuilder builder = PSC.update("account").set("firstName").where(Filters.eq("id", 1));

        assertThrows(IllegalArgumentException.class, () -> builder.onlyIf("/* c */"));
        assertEquals("UPDATE account SET first_name = ? WHERE id = ? IF status = 1", builder.onlyIf("status = 1").build().query());

        final CqlBuilder unchanged = PSC.deleteFrom("account").where(Filters.eq("id", 1));
        assertThrows(IllegalArgumentException.class, () -> unchanged.onlyIf("-- c"));
        assertEquals("DELETE FROM account WHERE id = ?", unchanged.build().query());
    }

    /**
     * The protected rendering hooks reject a {@code null} argument with {@link IllegalArgumentException}
     * (not a bare NullPointerException), matching the public entry points ({@code onlyIf(Condition)},
     * {@code renderCondition}, {@code from(...)}) and the parent builder's hook contract. The rejected call
     * leaves the builder usable.
     */
    @Test
    public void test_protectedHooks_nullArgument_throwIAE() {
        final CqlBuilder conditionBuilder = PSC.select("id").from("account");
        assertThrows(IllegalArgumentException.class, () -> conditionBuilder.appendCondition(null));
        assertEquals("SELECT id FROM account", conditionBuilder.build().query());

        final CqlBuilder fromBuilder = PSC.select("id");
        assertThrows(IllegalArgumentException.class, () -> fromBuilder.appendOperationBeforeFrom(null));
        assertEquals("SELECT id FROM account", fromBuilder.from("account").build().query());

        // State is still checked before the argument: an UPDATE builder reports IllegalStateException.
        final CqlBuilder updateBuilder = PSC.update("account").set("firstName");
        assertThrows(IllegalStateException.class, () -> updateBuilder.appendOperationBeforeFrom(null));
        updateBuilder.build();
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

    @Test
    public void test_tableAliases_areRejectedButNullAliasIsAccepted() {
        assertThrows(IllegalArgumentException.class, () -> PSC.selectFrom(Users.class, "u"));
        assertThrows(IllegalArgumentException.class, () -> PSC.select("id").from(Users.class, "u"));
        assertThrows(IllegalArgumentException.class, () -> PSC.select("id").from("account a"));
        assertThrows(IllegalArgumentException.class, () -> PSC.select("id").from("account AS a"));
        assertThrows(IllegalArgumentException.class, () -> PSC.select("id").from("account a", Account.class));
        assertThrows(IllegalArgumentException.class, () -> PSC.select("id").from(new String[] { "account", "other_account" }));
        assertThrows(IllegalArgumentException.class, () -> PSC.select("id").from("account", "other_account"));
        assertThrows(IllegalArgumentException.class, () -> PSC.select("id").from(N.asList("account", "other_account")));
        assertThrows(IllegalArgumentException.class, () -> PSC.select("id").from("account, other_account"));
        assertThrows(IllegalArgumentException.class, () -> PSC.select("id").from("account JOIN other_account ON account.id = other_account.id"));
        assertThrows(IllegalArgumentException.class, () -> PSC.select("id").from("(SELECT id FROM account) nested"));
        assertThrows(IllegalArgumentException.class, () -> PSC.delete("name").from("account a"));
        assertThrows(IllegalArgumentException.class, () -> PSC.insert("id").into("account a"));
        assertThrows(IllegalArgumentException.class, () -> PSC.update("account a"));
        assertThrows(IllegalArgumentException.class, () -> PSC.deleteFrom("account a"));

        final String emptyQuotedTable = String.valueOf('"') + '"';
        final String unclosedQuotedTable = String.valueOf('"') + "account";
        assertThrows(IllegalArgumentException.class, () -> PSC.select("id").from(emptyQuotedTable));
        assertThrows(IllegalArgumentException.class, () -> PSC.select("id").from(unclosedQuotedTable));

        final String cql = PSC.selectFrom(Users.class, (String) null).build().query();
        assertTrue(cql.contains("FROM simplex.users"), cql);
        assertFalse(cql.contains("simplex.users u"), cql);

        final String quotedTable = '"' + "key space" + '"' + "." + '"' + "user table" + '"';
        assertEquals("SELECT id FROM " + quotedTable, PSC.select("id").from(quotedTable).build().query());
        assertEquals("SELECT id FROM simplex . users", PSC.select("id").from("simplex . users").build().query());
    }

    /** Regression guard for operation-specific trailing clauses and required FROM/INTO ordering. */
    @Test
    public void testTrailingClausesRejectInvalidOperationOrMissingTable() {
        assertThrows(IllegalStateException.class, () -> PSC.insert("id").usingTTL(60));
        assertThrows(IllegalStateException.class, () -> PSC.insert("id").ifNotExists());
        assertThrows(IllegalStateException.class, () -> PSC.insert("id").onlyIf(Filters.isNull("name")));
        assertThrows(IllegalStateException.class, () -> PSC.update("account").set("name").ifNotExists());
        assertThrows(IllegalStateException.class, () -> PSC.select("id").from("account").ifExists());
        assertThrows(IllegalStateException.class, () -> PSC.deleteFrom("account").allowFiltering());
        assertThrows(IllegalStateException.class, () -> PSC.select("id").allowFiltering());
        assertThrows(IllegalStateException.class, () -> PSC.delete("name").usingTimestamp("42"));
        assertThrows(IllegalStateException.class, () -> PSC.delete("name").onlyIf("name = 'old'"));
        assertThrows(IllegalStateException.class, () -> PSC.update("account").set("name").ifExists());
        assertThrows(IllegalStateException.class, () -> PSC.deleteFrom("account").onlyIf("name = 'old'"));
    }

    @Test
    public void testTrailingClausesRejectDuplicates() {
        assertThrows(IllegalStateException.class, () -> PSC.insert("id").into("account").usingTTL(60).usingTTL(120));
        assertThrows(IllegalStateException.class, () -> PSC.update("account").set("name").usingTimestamp("1").usingTimestamp("2"));
        assertThrows(IllegalStateException.class, () -> PSC.update("account").set("name").where(Filters.eq("id", 1)).ifExists().onlyIf("name = 'old'"));
        assertThrows(IllegalStateException.class, () -> PSC.insert("id").into("account").ifNotExists().ifNotExists());
        assertThrows(IllegalStateException.class, () -> PSC.select("id").from("account").allowFiltering().allowFiltering());
    }

    @Test
    public void testUsingOptionsRejectUnsupportedStatementTypes() {
        assertThrows(IllegalStateException.class, () -> PSC.select("id").from("account").usingTTL(60));
        assertThrows(IllegalStateException.class, () -> PSC.select("id").from("account").usingTimestamp("42"));
        assertThrows(IllegalStateException.class, () -> PSC.deleteFrom("account").usingTTL(60));

        assertEquals("DELETE FROM account USING TIMESTAMP 42 WHERE id = ?",
                PSC.deleteFrom("account").usingTimestamp("42").where(Filters.eq("id", 1)).build().query());
    }

    // ---- slice Q (2026-09-27): abacus-query 4.9.4 column-DELETE regression and FROM atomicity ----

    private static Object entityClassOf(final CqlBuilder builder) throws Exception {
        final java.lang.reflect.Field field = com.landawn.abacus.query.AbstractQueryBuilder.class.getDeclaredField("_entityClass");
        field.setAccessible(true);
        return field.get(builder);
    }

    /** Bean whose metadata cannot be loaded (a LocalDate with the "long" date format), so setEntityClass fails. */
    public static class VerifyQbLongDateEntity {
        private long id;
        @com.landawn.abacus.annotation.JsonXmlField(dateFormat = "long")
        private java.time.LocalDate day;

        public long getId() {
            return id;
        }

        public void setId(final long id) {
            this.id = id;
        }

        public java.time.LocalDate getDay() {
            return day;
        }

        public void setDay(final java.time.LocalDate day) {
            this.day = day;
        }
    }

    /** Valid bean whose derived table name is not one CQL table, so only the factory's later into(..)/from(..) step fails. */
    @com.landawn.abacus.annotation.Table(name = "a b")
    public static class VerifyQbBadTableEntity {
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
}
