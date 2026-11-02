package com.landawn.abacus.da.azure;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.models.SqlQuerySpec;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.landawn.abacus.annotation.Table;
import com.landawn.abacus.da.MockitoTestBase;
import com.landawn.abacus.query.AbstractQueryBuilder.SP;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.util.ImmutableList;

/**
 * Line-coverage for the Cosmos SQL projection and condition helpers that the existing executor tests do not reach.
 * Private helpers are invoked only to assert their own return value or thrown exception.
 */
public class CosmosContainerExecutorLineCoverageTest extends MockitoTestBase {

    @Mock
    private CosmosContainer container;

    private CosmosPagedIterable<Item> rows;

    private ArgumentCaptor<SqlQuerySpec> query;

    private CosmosContainerExecutor executor;

    @BeforeEach
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void setUp() {
        rows = mock(CosmosPagedIterable.class);
        query = ArgumentCaptor.forClass(SqlQuerySpec.class);
        when(rows.stream()).thenAnswer(invocation -> java.util.stream.Stream.empty());
        when(container.queryItems(query.capture(), any(), any())).thenReturn((CosmosPagedIterable) rows);
        executor = new CosmosContainerExecutor(container);
    }

    /** {@code prepareQuery(..., count)} applies LIMIT, and a FROM clause that no longer ends on the alias is left unchanged. */
    @Test
    public void testPositiveCountAddsLimitAndLeavesFromSourceWhenAliasIsNotAtTheEnd() throws Exception {
        final SP sp = (SP) prepareQuery().invoke(executor, Item.class, null, null, 1);

        assertEquals("SELECT * FROM item c LIMIT 1", sp.query());
        assertEquals(0, sp.parameters().size());
    }

    /** A query that never contains {@code FROM} is not a Cosmos container source and is returned as-is. */
    @Test
    public void testFromSourceWithoutFromClauseIsUnchanged() throws Exception {
        final Method method = CosmosContainerExecutor.class.getDeclaredMethod("toCosmosFromSource", SP.class);
        method.setAccessible(true);
        final SP original = new SP("SELECT 1", ImmutableList.empty());

        assertSame(original, method.invoke(null, original));
    }

    /** A table name with whitespace is not one source token, so the generated {@code FROM} text is kept. */
    @Test
    public void testSpacedTableNameStaysInTheFromClause() {
        final String sql = capture(Filters.eq("id", "1"), SpacedTableItem.class);

        assertEquals("SELECT * FROM sales orders c WHERE c.id = @p0", sql);
        assertEquals("1", query.getValue().getParameters().get(0).getValue(String.class));
    }

    /** The identifier grammar rejects an empty name before it inspects any character. */
    @Test
    public void testEmptyNameIsNotAPlainCosmosIdentifier() throws Exception {
        final Method method = CosmosContainerExecutor.class.getDeclaredMethod("isPlainCosmosIdentifier", String.class);
        method.setAccessible(true);

        assertEquals(Boolean.FALSE, method.invoke(null, ""));
    }

    /** {@code IS ?} and {@code IS NOT ?} are not Cosmos predicates; they become comparisons and the placeholder stays literal. */
    @Test
    public void testIsQuestionMarkPredicatesBecomeComparisons() {
        assertAll(() -> assertEquals("SELECT * FROM item c WHERE c.active = ?", capture(Filters.is("active", Filters.expr("?")))),
                () -> assertEquals("SELECT * FROM item c WHERE c.active != ?", capture(Filters.isNot("active", Filters.expr("?")))),
                () -> assertEquals("SELECT * FROM item c WHERE c.active != ?", capture(Filters.expr("active IS NOT ?"))),
                () -> assertEquals("SELECT * FROM item c WHERE c.active = ?", capture(Filters.expr("active IS ?"))));
    }

    /**
     * An {@code IS NULL} / {@code IS NOT NULL} written inside an unclosed bracket has no operand boundary, so the phrase is copied
     * through instead of being wrapped in {@code IS_NULL}.
     */
    @Test
    public void testIsNullInsideUnclosedBracketIsLeftInPlace() {
        // NULL is a Cosmos literal, so the qualifier lower-cases it before the IS NULL rewrite sees the phrase.
        assertAll(() -> assertEquals("SELECT * FROM item c WHERE c.tags[c.name IS null]", capture(Filters.expr("tags[name IS NULL]"))),
                () -> assertEquals("SELECT * FROM item c WHERE c.tags[c.name IS NOT null]", capture(Filters.expr("tags[name IS NOT NULL]"))));
    }

    /**
     * {@code a IS NULL} is emitted as the whole projection, with no quoted alias. The IS NULL scan still walks that short
     * prefix and leaves the predicate text in place, and the projection rewrite then rejects it.
     */
    @Test
    public void testIsNullInSelectListKeepsThePredicateText() {
        final IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> executor.streamItems(List.of("a IS NULL"), null, Item.class).toList());

        assertEquals("Unexpected generated projection: a IS NULL", error.getMessage());
    }

    /** Opposite-case {@code SELECT} with nothing after it is not a FROM-less subquery, and the name stays a property. */
    @Test
    public void testSelectAtEndOfConditionIsAProperty() {
        assertEquals("SELECT * FROM item c WHERE (c[\"SELECT\"]", capture(Filters.expr("(SELECT")));
    }

    /** Quoted text, an unclosed bracket, and rejected numeric tails decide whether {@code SELECT ...} is a subquery or a property. */
    @Test
    public void testFromLessProjectionSyntaxEdges() {
        assertAll(
                () -> assertEquals("SELECT * FROM item c WHERE IS_DEFINED((SELECT ['a\\'b', 'c''d']))",
                        capture(Filters.expr("IS_DEFINED((SELECT ['a\\'b', 'c''d']))"))),
                () -> assertEquals("SELECT * FROM item c WHERE (c[\"SELECT\"] [c.x) > 0", capture(Filters.expr("(SELECT [x) > 0"))),
                () -> assertEquals("SELECT * FROM item c WHERE (c[\"SELECT\"] - 1) > 0", capture(Filters.expr("(SELECT - 1) > 0"))),
                () -> assertEquals("SELECT * FROM item c WHERE (c[\"SELECT\"] -c.x) > 0", capture(Filters.expr("(SELECT -x) > 0"))),
                () -> assertEquals("SELECT * FROM item c WHERE IS_DEFINED((SELECT -1e+2))", capture(Filters.expr("IS_DEFINED((SELECT -1e+2))"))),
                () -> assertEquals("SELECT * FROM item c WHERE (c[\"SELECT\"] -1e) > 0", capture(Filters.expr("(SELECT -1e) > 0"))),
                () -> assertEquals("SELECT * FROM item c WHERE (c[\"SELECT\"] -1e+) > 0", capture(Filters.expr("(SELECT -1e+) > 0"))),
                () -> assertEquals("SELECT * FROM item c WHERE (c[\"SELECT\"] -1x) > 0", capture(Filters.expr("(SELECT -1x) > 0"))),
                () -> assertEquals("SELECT * FROM item c WHERE (c[\"SELECT\"] -1 c.foo) > 0", capture(Filters.expr("(SELECT -1 foo) > 0"))),
                () -> assertEquals("SELECT * FROM item c WHERE (c[\"SELECT\"] -1 AS 1) > 0", capture(Filters.expr("(SELECT -1 AS 1) > 0"))));
    }

    /** A {@code FROM} whose source is missing, quoted text that only looks like SQL, and a signed subscript are not subquery sources. */
    @Test
    public void testSubqueryFromClauseEdges() throws Exception {
        assertAll(
                () -> assertEquals("SELECT * FROM item c WHERE (select * 'a\\'b' 'c''d' FROM t)",
                        capture(Filters.expr("(select * 'a\\'b' 'c''d' FROM t)"))),
                () -> assertEquals("SELECT * FROM item c WHERE (c[\"select\"] * c[\"from\"] [-1]) > 0 AND NOT c[\"group\"]",
                        capture(Filters.expr("(select * from [-1]) > 0 AND NOT group"))),
                () -> assertEquals("SELECT * FROM item c WHERE (c[\"select\"] * c[\"from\"] [+2]) > 0 AND NOT c[\"group\"]",
                        capture(Filters.expr("(select * from [+2]) > 0 AND NOT group"))));

        final Method method = CosmosContainerExecutor.class.getDeclaredMethod("hasCosmosSubqueryFromClause", String.class, int.class);
        method.setAccessible(true);
        final String queryText = "(SELECT * FROM   ";
        final int selectEnd = queryText.indexOf("SELECT") + "SELECT".length();

        assertEquals(Boolean.FALSE, method.invoke(null, queryText, selectEnd));
    }

    /** {@code NOT} at the end of the text does not start an operand, and a leading {@code AND} does not follow one. */
    @Test
    public void testNegatedOperandAtEndAndLeadingAnd() throws Exception {
        assertEquals("SELECT * FROM item c WHERE AND c.active", capture(Filters.expr("AND active")));

        final Method method = CosmosContainerExecutor.class.getDeclaredMethod("startsCosmosNegatedOperand", String.class, int.class);
        method.setAccessible(true);
        final String queryText = "score = NOT";

        assertEquals(Boolean.FALSE, method.invoke(null, queryText, queryText.length()));
    }

    /** Whitespace before a UDF name is skipped; whitespace before a non-name means {@code udf.} is a property path. */
    @Test
    public void testUdfDotSpacing() {
        assertAll(() -> assertEquals("SELECT * FROM item c WHERE udf. tax(c.price) > 1", capture(Filters.expr("udf. tax(price) > 1"))),
                () -> assertEquals("SELECT * FROM item c WHERE c.score > c[\"udf\"]. (", capture(Filters.expr("score > udf. ("))));
    }

    /** A caller-supplied {@code AS} alias is emitted unquoted, which is not a Cosmos VALUE projection. */
    @Test
    public void testUnquotedSelectAliasIsRejected() {
        final IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> executor.streamItems(List.of("id AS label"), null, Item.class).toList());

        assertEquals("Unexpected generated projection: c.id AS label", error.getMessage());
    }

    /** A projection that is not {@code SELECT ... FROM ...} is rejected before any alias is read. */
    @Test
    public void testProjectionWithoutFromIsRejected() throws Exception {
        final Method method = CosmosContainerExecutor.class.getDeclaredMethod("toCosmosProjection", SP.class);
        method.setAccessible(true);
        final InvocationTargetException thrown = assertThrows(InvocationTargetException.class,
                () -> method.invoke(null, new SP("SELECT 1", ImmutableList.empty())));
        final IllegalStateException error = assertInstanceOf(IllegalStateException.class, thrown.getCause());

        assertEquals("Unexpected generated SELECT query: SELECT 1", error.getMessage());
    }

    private String capture(final Condition condition) {
        return capture(condition, Item.class);
    }

    private String capture(final Condition condition, final Class<?> type) {
        executor.streamItems(condition, type).toList();
        return query.getValue().getQueryText();
    }

    private static Method prepareQuery() throws NoSuchMethodException {
        final Method method = CosmosContainerExecutor.class.getDeclaredMethod("prepareQuery", Class.class, Collection.class, Condition.class, int.class);
        method.setAccessible(true);
        return method;
    }

    public static class Item {
        public String id;
    }

    @Table("sales orders")
    public static class SpacedTableItem {
        public String id;
    }
}
