package com.landawn.abacus.da.cassandra;

import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.PSC;
import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.SCCB;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import com.landawn.abacus.da.entity.Account;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.SortDirection;
import com.landawn.abacus.query.condition.Criteria;

class CqlBuilderDeepReview2026UnsupportedStatementTest {

    @Test
    void insertSelectIsRejectedByEveryIntoOverload() {
        final CqlBuilder byName = PSC.select("id");
        assertThrows(IllegalStateException.class, () -> byName.into("archived_items"));
        assertEquals("SELECT id FROM items", byName.from("items").build().query());

        final CqlBuilder byClass = PSC.select("id");
        assertThrows(IllegalStateException.class, () -> byClass.into(Account.class));
        assertEquals("SELECT id FROM items", byClass.from("items").build().query());

        final CqlBuilder byNameAndClass = PSC.select("id");
        assertThrows(IllegalStateException.class, () -> byNameAndClass.into("archived_items", Account.class));
        assertEquals("SELECT id FROM items", byNameAndClass.from("items").build().query());
    }

    // ---- 2026-10-19 cqlfix ----

    /** An inherited clause method that CQL lacks: no argument could work, so the operation is unsupported. */
    private static Executable unsupported(final String keyword, final Executable call) {
        return rejected(UnsupportedOperationException.class, keyword, call);
    }

    /** A supported method given a condition CQL cannot express: the argument is invalid, as where(...) reports it. */
    private static Executable invalid(final String keyword, final Executable call) {
        return rejected(IllegalArgumentException.class, keyword, call);
    }

    private static Executable rejected(final Class<? extends RuntimeException> type, final String keyword, final Executable call) {
        return () -> {
            final RuntimeException e = assertThrows(type, call);
            assertTrue(e.getMessage().contains("does not support " + keyword), e.getMessage());
        };
    }

    /**
     * CQL has no OFFSET clause: every inherited path rendered "LIMIT 10 OFFSET 5" / "OFFSET 5" / "OFFSET 5 ROWS", which
     * Cassandra rejects as a syntax error ("mismatched input 'OFFSET' expecting EOF"), only when the statement ran.
     */
    @Test
    void cqlfix_offsetIsRejectedOnEveryPath() {
        assertAll(unsupported("OFFSET", () -> PSC.select("id").from("t").limit(10, 5)), unsupported("OFFSET", () -> PSC.select("id").from("t").limit(10, 0)),
                unsupported("OFFSET", () -> SCCB.select("id").from("t").limit(10, 5)), unsupported("OFFSET", () -> PSC.select("id").from("t").offset(5)),
                unsupported("OFFSET", () -> PSC.select("id").from("t").limit(10).offset(5)),
                unsupported("OFFSET", () -> PSC.select("id").from("t").offsetRows(5)),
                invalid("OFFSET", () -> PSC.select("id").from("t").append(Filters.limit(10, 5))),
                invalid("OFFSET", () -> PSC.select("id").from("t").append(Filters.limit("LIMIT 10 OFFSET 5"))),
                invalid("OFFSET", () -> PSC.select("id").from("t").append(Filters.limit("5, 10"))),
                invalid("OFFSET", () -> PSC.select("id").from("t").append(Filters.limit("OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY"))),
                // Out-of-int-range expressions are rendered verbatim by the parent, bypassing limit(int, int).
                invalid("OFFSET", () -> PSC.select("id").from("t").append(Filters.limit("LIMIT 10 OFFSET 99999999999"))),
                invalid("OFFSET", () -> PSC.select("id").from("t").append(Filters.limit("99999999999, 10"))),
                invalid("OFFSET", () -> PSC.select("id").from("t").append(Filters.limit("OFFSET 99999999999 ROWS FETCH NEXT 10 ROWS ONLY"))),
                invalid("OFFSET", () -> PSC.select("id").from("t").append(Criteria.builder().where(Filters.eq("id", 1)).limit(10, 5).build())),
                invalid("OFFSET", () -> PSC.select("id").from("t").append(Criteria.builder().limit("LIMIT 10 OFFSET 99999999999").build())),
                invalid("OFFSET", () -> PSC.select("id").from("t").appendIf(true, Filters.limit(10, 5))),
                invalid("OFFSET", () -> PSC.renderCondition(Filters.limit(1, 2), null)));
    }

    /** CQL has no HAVING clause: having(...) and appended HAVING clauses rendered "... HAVING ...", a Cassandra syntax error. */
    @Test
    void cqlfix_havingIsRejectedOnEveryPath() {
        assertAll(unsupported("HAVING", () -> PSC.select("id").from("t").groupBy("id").having("COUNT(*) > 1")),
                unsupported("HAVING", () -> PSC.select("id").from("t").groupBy("id").having(Filters.gt("cnt", 1))),
                unsupported("HAVING", () -> SCCB.select("id").from("t").groupBy("id").having(Filters.gt("cnt", 1))),
                invalid("HAVING", () -> PSC.select("id").from("t").groupBy("id").append(Filters.having(Filters.gt("cnt", 1)))),
                invalid("HAVING", () -> PSC.select("id").from("t").groupBy("id").append(Filters.having("COUNT(*) > 1"))),
                invalid("HAVING", () -> PSC.select("id").from("t").groupBy("id").appendIf(true, Filters.having(Filters.gt("cnt", 1)))),
                invalid("HAVING", () -> PSC.select("id").from("t").groupBy("id").appendIfOrElse(false, Filters.limit(1), Filters.having(Filters.gt("cnt", 1)))),
                invalid("HAVING", () -> PSC.select("id").from("t").append(Criteria.builder().groupBy("id").having(Filters.gt("cnt", 1)).build())),
                invalid("HAVING", () -> PSC.select("id").from("t").append(Criteria.builder().groupBy("id").having("COUNT(*) > 1").build())),
                invalid("HAVING", () -> PSC.renderCondition(Filters.having(Filters.gt("cnt", 1)), null)));
    }

    /** Plain LIMIT keeps working, a rejected call leaves the builder usable, and a closed builder still reports ISE first. */
    @Test
    void cqlfix_plainLimitStillWorksAndRejectionLeavesBuilderUnchanged() {
        final CqlBuilder rejected = PSC.select("id").from("t");
        assertThrows(IllegalArgumentException.class, () -> rejected.append(Criteria.builder().where(Filters.eq("id", 1)).limit(10, 5).build()));
        assertThrows(UnsupportedOperationException.class, () -> rejected.limit(10, 5));
        assertThrows(UnsupportedOperationException.class, () -> rejected.having("COUNT(*) > 1"));

        final CqlBuilder closed = PSC.select("id").from("t");
        closed.build();

        assertAll(() -> assertEquals("SELECT id FROM t LIMIT 10", PSC.select("id").from("t").limit(10).build().query()),
                () -> assertEquals("SELECT id FROM t LIMIT 10", SCCB.select("id").from("t").limit(10).build().query()),
                () -> assertEquals("SELECT id FROM t LIMIT 10", PSC.select("id").from("t").append(Filters.limit(10)).build().query()),
                // A zero-offset Limit renders a plain LIMIT and is accepted.
                () -> assertEquals("SELECT id FROM t LIMIT 10", PSC.select("id").from("t").append(Filters.limit(10, 0)).build().query()),
                () -> assertEquals("SELECT id FROM t GROUP BY id LIMIT 3", PSC.select("id").from("t").groupBy("id").limit(3).build().query()),
                () -> assertEquals("SELECT id FROM t WHERE id = ? LIMIT 10",
                        rejected.append(Criteria.builder().where(Filters.eq("id", 1)).limit(10).build()).build().query()),
                () -> assertThrows(IllegalStateException.class, () -> closed.limit(10, 5)),
                () -> assertThrows(IllegalStateException.class, () -> closed.offset(5)),
                () -> assertThrows(IllegalStateException.class, () -> closed.offsetRows(5)),
                () -> assertThrows(IllegalStateException.class, () -> closed.having("x > 1")),
                () -> assertThrows(IllegalStateException.class, () -> closed.having(Filters.gt("x", 1))),
                () -> assertThrows(IllegalStateException.class, () -> closed.append(Filters.having(Filters.gt("x", 1)))),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> PSC.select("id").from("t").append((com.landawn.abacus.query.condition.Condition) null)));
    }

    /**
     * CQL has no FETCH clause: fetchFirstRows/fetchNextRows rendered "FETCH FIRST|NEXT n ROWS ONLY", a Cassandra syntax
     * error; an out-of-int-range FETCH Limit was appended verbatim. A resolved FETCH Limit renders a plain LIMIT.
     */
    @Test
    void cqlfix_fetchIsRejectedOnEveryPath() {
        final CqlBuilder closed = PSC.select("id").from("t");
        closed.build();

        assertAll(unsupported("FETCH", () -> PSC.select("id").from("t").fetchFirstRows(10)),
                unsupported("FETCH", () -> PSC.select("id").from("t").fetchNextRows(10)),
                unsupported("FETCH", () -> SCCB.select("id").from("t").fetchFirstRows(10)),
                invalid("FETCH", () -> PSC.select("id").from("t").append(Filters.limit("FETCH FIRST 99999999999 ROWS ONLY"))),
                invalid("FETCH", () -> PSC.select("id").from("t").append(Criteria.builder().limit("FETCH NEXT 99999999999 ROWS ONLY").build())),
                () -> assertEquals("SELECT id FROM t LIMIT 10", PSC.select("id").from("t").append(Filters.limit("FETCH FIRST 10 ROWS ONLY")).build().query()),
                () -> assertThrows(IllegalStateException.class, () -> closed.fetchFirstRows(10)),
                () -> assertThrows(IllegalStateException.class, () -> closed.fetchNextRows(10)));
    }

    // ---- 2026-10-20 cassfix ----

    /** Documents the class Javadoc: only the listed clauses fail fast; a JOIN is still rendered as written (Cassandra rejects it at execution). */
    @Test
    void cassfix_joinIsNotRejectedByTheBuilder() {
        assertAll(() -> assertEquals("SELECT a FROM t JOIN u ON t.id = u.id", PSC.select("a").from("t").join("u").on("t.id = u.id").build().query()),
                () -> assertEquals("SELECT a FROM t INNER JOIN u ON t.id = u.id", SCCB.select("a").from("t").innerJoin("u").on("t.id = u.id").build().query()));
    }

    // ---- 2026-10-21 cassfix2 ----

    private static final String SUB_SELECT = "SELECT id FROM u";

    /** CQL has no FOR UPDATE (no row locking): forUpdate() rendered "... FOR UPDATE", a Cassandra syntax error. */
    @Test
    void cassfix2_forUpdateIsRejected() {
        final CqlBuilder closed = PSC.select("id").from("t");
        closed.build();

        assertAll(unsupported("FOR UPDATE", () -> PSC.select("id").from("t").forUpdate()),
                unsupported("FOR UPDATE", () -> SCCB.select("id").from("t").where(Filters.eq("id", 1)).forUpdate()),
                () -> assertThrows(IllegalStateException.class, closed::forUpdate));
    }

    /**
     * CQL has no set operations: every union/unionAll/intersect/except/minus overload (String, builder and the *Select
     * variants) rendered "... UNION SELECT ...", and an appended Union/UnionAll/Intersect/Except/Minus clause (standalone or
     * inside a Criteria) was rejected only incidentally, with a message about sub-queries.
     */
    @Test
    void cassfix2_setOperationsAreRejectedOnEveryPath() {
        final CqlBuilder closed = PSC.select("id").from("t");
        closed.build();
        final List<String> cols = Arrays.asList("id");

        assertAll(unsupported("UNION", () -> PSC.select("id").from("t").union(SUB_SELECT)),
                unsupported("UNION", () -> PSC.select("id").from("t").union(PSC.select("id").from("u"))),
                unsupported("UNION", () -> PSC.select("id").from("t").unionSelect(cols)),
                unsupported("UNION ALL", () -> PSC.select("id").from("t").unionAll(SUB_SELECT)),
                unsupported("UNION ALL", () -> PSC.select("id").from("t").unionAll(PSC.select("id").from("u"))),
                unsupported("UNION ALL", () -> PSC.select("id").from("t").unionAllSelect(cols)),
                unsupported("INTERSECT", () -> PSC.select("id").from("t").intersect(SUB_SELECT)),
                unsupported("INTERSECT", () -> PSC.select("id").from("t").intersect(PSC.select("id").from("u"))),
                unsupported("INTERSECT", () -> PSC.select("id").from("t").intersectSelect(cols)),
                unsupported("EXCEPT", () -> PSC.select("id").from("t").except(SUB_SELECT)),
                unsupported("EXCEPT", () -> PSC.select("id").from("t").except(PSC.select("id").from("u"))),
                unsupported("EXCEPT", () -> PSC.select("id").from("t").exceptSelect(cols)),
                unsupported("MINUS", () -> PSC.select("id").from("t").minus(SUB_SELECT)),
                unsupported("MINUS", () -> PSC.select("id").from("t").minus(PSC.select("id").from("u"))),
                unsupported("MINUS", () -> PSC.select("id").from("t").minusSelect(cols)),
                unsupported("UNION", () -> SCCB.select("id").from("t").union(SUB_SELECT)),
                invalid("UNION", () -> PSC.select("id").from("t").append(Filters.union(Filters.subQuery(SUB_SELECT)))),
                invalid("UNION ALL", () -> PSC.select("id").from("t").append(Filters.unionAll(Filters.subQuery(SUB_SELECT)))),
                invalid("INTERSECT", () -> PSC.select("id").from("t").append(Filters.intersect(Filters.subQuery(SUB_SELECT)))),
                invalid("EXCEPT", () -> PSC.select("id").from("t").append(Filters.except(Filters.subQuery(SUB_SELECT)))),
                invalid("MINUS", () -> PSC.select("id").from("t").append(Filters.minus(Filters.subQuery(SUB_SELECT)))),
                invalid("UNION", () -> PSC.select("id").from("t").appendIf(true, Filters.union(Filters.subQuery(SUB_SELECT)))),
                invalid("UNION", () -> PSC.select("id").from("t").appendIfOrElse(true, Filters.union(Filters.subQuery(SUB_SELECT)), Filters.limit(1))),
                invalid("EXCEPT", () -> PSC.select("id").from("t").appendIfOrElse(false, Filters.limit(1), Filters.except(Filters.subQuery(SUB_SELECT)))),
                invalid("UNION",
                        () -> PSC.select("id").from("t").append(Criteria.builder().where(Filters.eq("id", 1)).union(Filters.subQuery(SUB_SELECT)).build())),
                invalid("MINUS", () -> PSC.select("id").from("t").append(Criteria.builder().minus(Filters.subQuery(SUB_SELECT)).build())),
                invalid("INTERSECT", () -> PSC.renderCondition(Filters.intersect(Filters.subQuery(SUB_SELECT)), null)),
                invalid("UNION ALL", () -> PSC.renderCondition(Criteria.builder().unionAll(Filters.subQuery(SUB_SELECT)).build(), null)),
                () -> assertThrows(IllegalStateException.class, () -> closed.union(SUB_SELECT)),
                () -> assertThrows(IllegalStateException.class, () -> closed.unionAllSelect(cols)),
                () -> assertThrows(IllegalStateException.class, () -> closed.minus(PSC.select("id").from("u"))),
                () -> assertThrows(IllegalStateException.class, () -> closed.append(Filters.union(Filters.subQuery(SUB_SELECT)))));
    }

    /**
     * CQL GROUP BY has no ASC/DESC: the groupBy overloads taking a SortDirection and an appended GroupBy clause whose sort
     * list carries a direction (standalone or inside a Criteria) rendered "GROUP BY a DESC", a Cassandra syntax error.
     */
    @Test
    void cassfix2_groupByWithSortDirectionIsRejectedOnEveryPath() {
        final String keyword = "GROUP BY with a sort direction";
        final CqlBuilder closed = PSC.select("id").from("t");
        closed.build();
        final Map<String, SortDirection> groupings = new LinkedHashMap<>();
        groupings.put("a", SortDirection.ASC);
        groupings.put("b", SortDirection.DESC);

        assertAll(unsupported(keyword, () -> PSC.select("id").from("t").groupBy("a", SortDirection.DESC)),
                unsupported(keyword, () -> PSC.select("id").from("t").groupBy(Arrays.asList("a", "b"), SortDirection.ASC)),
                unsupported(keyword, () -> PSC.select("id").from("t").groupBy(groupings)),
                unsupported(keyword, () -> SCCB.select("id").from("t").groupBy("a", SortDirection.ASC)),
                invalid(keyword, () -> PSC.select("id").from("t").append(Filters.groupByDesc("a"))),
                invalid(keyword, () -> PSC.select("id").from("t").append(Filters.groupByAsc("a", "b"))),
                invalid(keyword, () -> PSC.select("id").from("t").append(Filters.groupBy("a", SortDirection.ASC))),
                invalid(keyword, () -> PSC.select("id").from("t").append(Filters.groupBy(Arrays.asList("a", "b"), SortDirection.DESC))),
                invalid(keyword, () -> PSC.select("id").from("t").append(Filters.groupBy(groupings))),
                // Only the last item carries a direction, in lower case, after a function item with a comma of its own.
                invalid(keyword, () -> PSC.select("id").from("t").append(Filters.groupBy(Filters.expr("floor(ts, 1h), a desc")))),
                invalid(keyword, () -> PSC.select("id").from("t").appendIf(true, Filters.groupByDesc("a"))),
                invalid(keyword, () -> PSC.select("id").from("t").appendIfOrElse(true, Filters.groupByDesc("a"), Filters.groupBy("a"))),
                invalid(keyword,
                        () -> PSC.select("id").from("t").append(Criteria.builder().where(Filters.eq("id", 1)).groupBy("a", SortDirection.DESC).build())),
                invalid(keyword, () -> PSC.select("id").from("t").append(Criteria.builder().groupByDesc("a", "b").build())),
                invalid(keyword, () -> PSC.renderCondition(Filters.groupByDesc("a"), null)),
                invalid(keyword, () -> PSC.renderCondition(Criteria.builder().groupBy(groupings).build(), null)),
                () -> assertThrows(IllegalStateException.class, () -> closed.groupBy("a", SortDirection.DESC)),
                () -> assertThrows(IllegalStateException.class, () -> closed.groupBy(Arrays.asList("a"), SortDirection.DESC)),
                () -> assertThrows(IllegalStateException.class, () -> closed.groupBy(groupings)));
    }

    /**
     * Nothing else is rejected: plain GROUP BY (method, clause and Criteria), LIMIT, ORDER BY with a direction and JOIN
     * still render, the TRUE branch of appendIfOrElse appends its first condition, and a rejected condition leaves the
     * builder usable.
     */
    @Test
    void cassfix2_supportedClausesStillRenderAndRejectionLeavesBuilderUnchanged() {
        final CqlBuilder rejected = PSC.select("id").from("t");
        assertThrows(IllegalArgumentException.class, () -> rejected.append(Filters.groupByDesc("a")));
        assertThrows(IllegalArgumentException.class, () -> rejected.append(Filters.union(Filters.subQuery(SUB_SELECT))));
        assertThrows(UnsupportedOperationException.class, () -> rejected.groupBy("a", SortDirection.DESC));
        assertThrows(UnsupportedOperationException.class, () -> rejected.union(SUB_SELECT));
        assertThrows(UnsupportedOperationException.class, rejected::forUpdate);

        assertAll(() -> assertEquals("SELECT id FROM t GROUP BY a, b", PSC.select("id").from("t").groupBy("a", "b").build().query()),
                () -> assertEquals("SELECT id FROM t GROUP BY a, b", PSC.select("id").from("t").groupBy(Arrays.asList("a", "b")).build().query()),
                () -> assertEquals("SELECT id FROM t GROUP BY a, b", PSC.select("id").from("t").append(Filters.groupBy("a", "b")).build().query()),
                () -> assertEquals("SELECT id FROM t GROUP BY a, b",
                        PSC.select("id").from("t").append(Filters.groupBy(Arrays.asList("a", "b"))).build().query()),
                () -> assertEquals("SELECT id FROM t GROUP BY floor(ts, 1h), a",
                        PSC.select("id").from("t").append(Filters.groupBy(Filters.expr("floor(ts, 1h), a"))).build().query()),
                () -> assertEquals("SELECT id FROM t WHERE id = ? GROUP BY a, b LIMIT 10",
                        PSC.select("id").from("t").append(Criteria.builder().where(Filters.eq("id", 1)).groupBy("a", "b").limit(10).build()).build().query()),
                () -> assertEquals("GROUP BY a, b", PSC.renderCondition(Filters.groupBy("a", "b"), null).build().query()),
                () -> assertEquals("SELECT id FROM t LIMIT 10", PSC.select("id").from("t").limit(10).build().query()),
                () -> assertEquals("SELECT id FROM t ORDER BY a DESC", PSC.select("id").from("t").orderBy("a", SortDirection.DESC).build().query()),
                () -> assertEquals("SELECT id FROM t JOIN u ON t.id = u.id", PSC.select("id").from("t").join("u").on("t.id = u.id").build().query()),
                // appendIfOrElse: the TRUE branch appends the first condition, the FALSE branch the second.
                () -> assertEquals("SELECT id FROM t WHERE id = ? LIMIT 1",
                        PSC.select("id").from("t").where(Filters.eq("id", 1)).appendIfOrElse(true, Filters.limit(1), Filters.limit(2)).build().query()),
                () -> assertEquals("SELECT id FROM t WHERE id = ? LIMIT 2",
                        PSC.select("id").from("t").where(Filters.eq("id", 1)).appendIfOrElse(false, Filters.limit(1), Filters.limit(2)).build().query()),
                () -> assertEquals("SELECT id FROM t WHERE id = ?",
                        PSC.select("id").from("t").appendIfOrElse(true, Filters.eq("id", 1), Filters.union(Filters.subQuery(SUB_SELECT))).build().query()),
                () -> assertEquals("SELECT id FROM t GROUP BY a", rejected.groupBy("a").build().query()));
    }

    // ---- 2026-10-21 fixQ ----

    /**
     * The direction check split the last word of each GROUP BY item on ' ' and '\t' only, so a user-supplied expression
     * with a newline, carriage return, form feed or no-break space before ASC/DESC ("a\nDESC") slipped through and was
     * rendered verbatim. Any whitespace or space character now separates the word; a function item and a plain list still pass.
     */
    @Test
    void fixQ_groupByWithSortDirectionAfterAnyWhitespaceIsRejected() {
        final String keyword = "GROUP BY with a sort direction";

        assertAll(invalid(keyword, () -> PSC.select("id").from("t").append(Filters.groupBy("a\nDESC"))),
                invalid(keyword, () -> PSC.select("id").from("t").append(Filters.groupBy("a\r\nasc"))),
                invalid(keyword, () -> PSC.select("id").from("t").append(Filters.groupBy("a" + (char) 0xA0 + "desc"))), // no-break space
                invalid(keyword, () -> PSC.select("id").from("t").append(Filters.groupBy("a\fDesc"))),
                invalid(keyword, () -> PSC.select("id").from("t").append(Filters.groupBy("a \nDESC \n"))),
                invalid(keyword, () -> PSC.select("id").from("t").append(Filters.groupBy(Filters.expr("a\nDESC")))),
                invalid(keyword, () -> PSC.select("id").from("t").append(Filters.groupBy(Filters.expr("floor(ts, 1h),\nb\tASC")))),
                invalid(keyword, () -> SCCB.select("id").from("t").append(Filters.groupBy("a\nDESC"))),
                invalid(keyword, () -> PSC.select("id").from("t").append(Criteria.builder().groupBy("a\nDESC").build())),
                invalid(keyword, () -> PSC.renderCondition(Filters.groupBy("a" + (char) 0x2007 + "desc"), null)), // figure space
                () -> assertEquals("SELECT id FROM t GROUP BY floor(ts, 1h)",
                        PSC.select("id").from("t").append(Filters.groupBy(Filters.expr("floor(ts, 1h)"))).build().query()),
                () -> assertEquals("SELECT id FROM t GROUP BY a, b", PSC.select("id").from("t").append(Filters.groupBy("a, b")).build().query()),
                // A newline that is not followed by a direction word is not one (the parent re-renders the list as "a, b").
                () -> assertEquals("SELECT id FROM t GROUP BY a, b", PSC.select("id").from("t").append(Filters.groupBy(Filters.expr("a,\nb"))).build().query()),
                // A column merely named like a direction is not one.
                () -> assertEquals("SELECT id FROM t GROUP BY descending", PSC.select("id").from("t").append(Filters.groupBy("descending")).build().query()));
    }

    // ---- 2026-10-24 sliceI ----

    /** A LIMIT or ORDER BY clause claimed on an UPDATE/DELETE statement: rejected as the parent rejects GROUP BY there. */
    private static Executable notOnWrite(final String clause, final Executable call) {
        return () -> {
            final IllegalStateException e = assertThrows(IllegalStateException.class, call);
            assertTrue(e.getMessage().contains("'" + clause + "' is only valid for SELECT statements"), e.getMessage());
        };
    }

    /**
     * CQL has only a plain DISTINCT: distinctOn(...) rendered "SELECT DISTINCT ON (id) ..." and distinctRow() "SELECT
     * DISTINCTROW ...", which Cassandra 5.0's parser rejects ("no viable alternative at input 'ON'"), and so did a Criteria
     * carrying either modifier. Plain distinct() (method and Criteria) and a raw selectModifier still render.
     */
    @Test
    void sliceI_distinctOnAndDistinctRowAreRejectedOnEveryPath() {
        final CqlBuilder closed = PSC.select("id").from("t");
        closed.build();
        final CqlBuilder rejected = PSC.select("id").from("t");
        assertThrows(UnsupportedOperationException.class, () -> rejected.distinctOn("id"));
        assertThrows(UnsupportedOperationException.class, rejected::distinctRow);
        assertThrows(IllegalArgumentException.class, () -> rejected.append(Criteria.builder().distinctOn("id").where(Filters.eq("id", 1)).build()));

        assertAll(unsupported("DISTINCT ON", () -> PSC.select("id").from("t").distinctOn("id")),
                // Before from(), where the parent stages the modifier for appendOperationBeforeFrom.
                unsupported("DISTINCT ON", () -> PSC.select("id").distinctOn("id")),
                // A blank list would have been a plain DISTINCT in the parent; it fails all the same.
                unsupported("DISTINCT ON", () -> PSC.select("id").from("t").distinctOn("")),
                unsupported("DISTINCT ON", () -> SCCB.select("id").from("t").distinctOn("id, name")),
                unsupported("DISTINCTROW", () -> PSC.select("id").from("t").distinctRow()), unsupported("DISTINCTROW", () -> PSC.select("id").distinctRow()),
                invalid("DISTINCT ON", () -> PSC.select("id").from("t").append(Criteria.builder().distinctOn("id").where(Filters.eq("id", 1)).build())),
                invalid("DISTINCT ON", () -> PSC.select("id").from("t").appendIf(true, Criteria.builder().selectModifier("distinct on (id)").build())),
                invalid("DISTINCTROW", () -> PSC.select("id").from("t").append(Criteria.builder().distinctRow().build())),
                invalid("DISTINCTROW", () -> PSC.renderCondition(Criteria.builder().distinctRow().where(Filters.eq("id", 1)).build(), null)),
                () -> assertThrows(IllegalStateException.class, () -> closed.distinctOn("id")),
                () -> assertThrows(IllegalStateException.class, closed::distinctRow),
                () -> assertEquals("SELECT DISTINCT id FROM t", PSC.select("id").from("t").distinct().build().query()),
                () -> assertEquals("SELECT DISTINCT id FROM t WHERE id = ?",
                        PSC.select("id").from("t").append(Criteria.builder().distinct().where(Filters.eq("id", 1)).build()).build().query()),
                () -> assertEquals("SELECT JSON id FROM t", PSC.select("id").from("t").selectModifier("JSON").build().query()),
                () -> assertEquals("SELECT id FROM t", rejected.build().query()));
    }

    /**
     * CQL UPDATE and DELETE end with the WHERE and IF clauses: limit(count), the orderBy(...) methods and an appended
     * Limit/OrderBy clause (standalone or in a Criteria) rendered "UPDATE t SET a = ? WHERE id = ? LIMIT 5" / "DELETE FROM t
     * WHERE id = ? ORDER BY a", which Cassandra 5.0's parser rejects ("mismatched input 'LIMIT' expecting EOF"). They now fail
     * like the parent's GROUP BY on those statements, leave the builder unchanged, and still render on a SELECT.
     */
    @Test
    void sliceI_limitAndOrderByAreRejectedOnUpdateAndDelete() {
        final Map<String, SortDirection> orders = new LinkedHashMap<>();
        orders.put("a", SortDirection.DESC);
        final CqlBuilder update = PSC.update("t").set("a").where(Filters.eq("id", 1));
        assertThrows(IllegalStateException.class, () -> update.limit(5));
        assertThrows(IllegalStateException.class, () -> update.append(Criteria.builder().orderBy("a").limit(5).build()));
        final CqlBuilder delete = PSC.deleteFrom("t");
        assertThrows(IllegalStateException.class, () -> delete.limit(5));
        final CqlBuilder closed = PSC.update("t").set("a").where(Filters.eq("id", 1));
        closed.build();

        assertAll(notOnWrite("LIMIT", () -> PSC.update("t").set("a").where(Filters.eq("id", 1)).limit(5)),
                notOnWrite("LIMIT", () -> PSC.deleteFrom("t").where(Filters.eq("id", 1)).limit(5)),
                notOnWrite("LIMIT", () -> PSC.delete("a").from("t").where(Filters.eq("id", 1)).limit(5)),
                notOnWrite("LIMIT", () -> PSC.deleteFrom("t").where(Filters.eq("id", 1)).ifExists().limit(5)),
                notOnWrite("LIMIT", () -> SCCB.update("t").set("a", 1).where(Filters.eq("id", 1)).append(Filters.limit(5))),
                notOnWrite("LIMIT", () -> PSC.deleteFrom("t").where(Filters.eq("id", 1)).appendIf(true, Filters.limit(5))),
                notOnWrite("LIMIT", () -> PSC.deleteFrom("t").append(Criteria.builder().where(Filters.eq("id", 1)).limit(5).build())),
                notOnWrite("ORDER BY", () -> PSC.update("t").set("a").where(Filters.eq("id", 1)).orderBy("a")),
                notOnWrite("ORDER BY", () -> PSC.update("t").set("a").where(Filters.eq("id", 1)).orderBy("a", SortDirection.DESC)),
                notOnWrite("ORDER BY", () -> PSC.update("t").set("a").where(Filters.eq("id", 1)).orderByDesc("a", "b")),
                notOnWrite("ORDER BY", () -> PSC.deleteFrom("t").where(Filters.eq("id", 1)).orderBy(Arrays.asList("a", "b"))),
                notOnWrite("ORDER BY", () -> PSC.deleteFrom("t").where(Filters.eq("id", 1)).orderBy(orders)),
                notOnWrite("ORDER BY", () -> PSC.deleteFrom("t").where(Filters.eq("id", 1)).append(Filters.orderBy("a"))),
                notOnWrite("ORDER BY", () -> PSC.update("t").set("a").where(Filters.eq("id", 1)).append(Criteria.builder().orderBy("a").build())),
                () -> assertThrows(IllegalStateException.class, () -> closed.limit(5)),
                () -> assertThrows(IllegalStateException.class, () -> closed.orderBy("a")),
                () -> assertEquals("UPDATE t SET a = ? WHERE id = ? IF EXISTS", update.ifExists().build().query()),
                () -> assertEquals("DELETE FROM t WHERE id = ?", delete.where(Filters.eq("id", 1)).build().query()),
                () -> assertEquals("SELECT id FROM t WHERE id = ? ORDER BY a DESC LIMIT 5",
                        PSC.select("id").from("t").where(Filters.eq("id", 1)).orderBy("a", SortDirection.DESC).limit(5).build().query()),
                () -> assertEquals("SELECT id FROM t WHERE id = ? ORDER BY a LIMIT 5",
                        PSC.select("id").from("t").append(Criteria.builder().where(Filters.eq("id", 1)).orderBy("a").limit(5).build()).build().query()));
    }

    /**
     * A bare sub-query (the value of Filters.eq("id", subQuery)) has the EMPTY operator, so its rejection ended in
     * "sub-queries: " with nothing after it, and an IN sub-query read as "sub-queries: IN", as if IN itself were unsupported.
     */
    @Test
    void sliceI_subQueryRejectionNamesTheRejectedConstruct() {
        final com.landawn.abacus.query.condition.SubQuery sub = Filters.subQuery(SUB_SELECT);

        assertAll(subQueryMessage("sub-query", () -> PSC.select("id").from("t").where(Filters.eq("id", sub))),
                subQueryMessage("IN (sub-query)", () -> PSC.select("id").from("t").where(Filters.in("id", sub))),
                subQueryMessage("NOT IN (sub-query)", () -> PSC.select("id").from("t").where(Filters.notIn("id", sub))),
                subQueryMessage("NOT IN", () -> PSC.select("id").from("t").where(Filters.notIn("id", Arrays.asList(1, 2)))));
    }

    private static Executable subQueryMessage(final String rejected, final Executable call) {
        return () -> assertEquals("Cassandra CQL does not support NOT IN or sub-queries: " + rejected,
                assertThrows(IllegalArgumentException.class, call).getMessage());
    }
}
