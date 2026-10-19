package com.landawn.abacus.da.azure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.models.SqlQuerySpec;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.util.NamingPolicy;

class CosmosContainerExecutorDeepReview2026SubqueryScopeTest {

    private CosmosContainer container;
    private CosmosPagedIterable<Item> rows;
    private ArgumentCaptor<SqlQuerySpec> query;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        container = mock(CosmosContainer.class);
        rows = mock(CosmosPagedIterable.class);
        query = ArgumentCaptor.forClass(SqlQuerySpec.class);
        when(rows.stream()).thenAnswer(invocation -> Stream.empty());
        when(container.queryItems(query.capture(), any(), eq(Item.class))).thenReturn(rows);
    }

    @Test
    void structuredSubqueryRetainsLocalAliasesParametersAndNullRewrite() {
        final Condition condition = Filters.and(Filters.eq("id", "root"),
                Filters.exists(Filters.subQuery("SELECT VALUE t FROM t IN c.tags WHERE (t.score > 0) AND t.name IS NULL AND t.value = ?", List.of("child"))));

        final SqlQuerySpec spec = capture(NamingPolicy.SNAKE_CASE, condition);

        assertEquals(
                "SELECT * FROM item c WHERE (c.id = @p0) AND (EXISTS (SELECT VALUE t FROM t IN c.tags WHERE (t.score > 0) AND IS_NULL(t.name) AND t.value = @p1))",
                spec.getQueryText());
        assertEquals(List.of("@p0", "@p1"), spec.getParameters().stream().map(parameter -> parameter.getName()).toList());
        assertEquals(List.of("root", "child"), spec.getParameters().stream().map(parameter -> parameter.getValue(String.class)).toList());
    }

    @Test
    void nestedSubqueriesAndQuotedParenthesesPreserveScopeBeforeOuterQualificationResumes() {
        final Condition condition = Filters
                .expr("EXISTS (SELECT * FROM c WHERE c.id = ')' AND EXISTS (SELECT VALUE t FROM t IN c.tags WHERE t = 1)) AND NOT group");

        final SqlQuerySpec spec = capture(NamingPolicy.SNAKE_CASE, condition);

        assertEquals(
                "SELECT * FROM item c WHERE EXISTS (SELECT * FROM c WHERE c.id = ')' AND EXISTS (SELECT value t FROM t IN c.tags WHERE t = 1)) AND NOT c.group",
                spec.getQueryText());
    }

    @Test
    void subqueryAliasesSurviveScreamingSnakeLiteralNormalization() {
        final Condition condition = Filters.exists(
                Filters.subQuery("SELECT VALUE t FROM t IN c.tags WHERE t.active = TRUE AND t.deleted = FALSE AND t.missing = NULL AND t.other = UNDEFINED"));

        final SqlQuerySpec spec = capture(NamingPolicy.SCREAMING_SNAKE_CASE, condition);

        assertEquals(
                "SELECT * FROM ITEM c WHERE EXISTS (SELECT VALUE t FROM t IN c.tags WHERE t.active = true AND t.deleted = false AND t.missing = null AND t.other = undefined)",
                spec.getQueryText());
    }

    @ParameterizedTest
    @ValueSource(strings = { "SELECT VALUE c.name", "SELECT VALUE 1", "SELECT c.name", "SELECT c.name AS name WHERE c.active = true",
            "SELECT VALUE (SELECT VALUE c.name)", "SELECT VALUE {'name': c.name, 'close': ')'}", "SELECT VALUE [c.name, c.id]",
            "SELECT VALUE CONCAT(c.name, ')')", "SELECT undefined", "SELECT -1", "SELECT +1", "SELECT [c.id]", "SELECT []", "SELECT (c.id) AS id",
            "SELECT(c.id) AS id", "SELECT [c.id, ')'] AS ids WHERE c.active = true" })
    void subqueriesWithoutFromPreserveProjectionAndResumeOuterQualification(final String subquery) {
        final SqlQuerySpec spec = capture(NamingPolicy.SNAKE_CASE, Filters.and(Filters.exists(Filters.subQuery(subquery)), Filters.eq("id", "root")));

        assertEquals("SELECT * FROM item c WHERE (EXISTS (" + subquery + ")) AND (c.id = @p0)", spec.getQueryText());
        assertEquals(1, spec.getParameters().size());
        assertEquals("root", spec.getParameters().getFirst().getValue(String.class));
    }

    @Test
    void scalarSubqueryWithoutFromRetainsParameterAndLiteralNormalization() {
        final SqlQuerySpec spec = capture(NamingPolicy.SCREAMING_SNAKE_CASE,
                Filters.exists(Filters.subQuery("SELECT VALUE TRUE WHERE c.id = ?", List.of("root"))));

        assertEquals("SELECT * FROM ITEM c WHERE EXISTS (SELECT VALUE true WHERE c.id = @p0)", spec.getQueryText());
        assertEquals(List.of("@p0"), spec.getParameters().stream().map(parameter -> parameter.getName()).toList());
        assertEquals("root", spec.getParameters().getFirst().getValue(String.class));
    }

    @ParameterizedTest
    @ValueSource(strings = { "SELECT -1", "SELECT [c.id]", "SELECT (c.id) AS id" })
    void notExistsWithoutFromPreservesScopeAndParametersForEveryNamingPolicy(final String projection) {
        for (final NamingPolicy policy : List.of(NamingPolicy.SNAKE_CASE, NamingPolicy.SCREAMING_SNAKE_CASE, NamingPolicy.CAMEL_CASE)) {
            final Condition condition = Filters.and(Filters.notExists(Filters.subQuery(projection + " WHERE c.id = ?", List.of("child"))),
                    Filters.eq("id", "root"));
            final SqlQuerySpec spec = capture(policy, condition);
            final String table = policy == NamingPolicy.SCREAMING_SNAKE_CASE ? "ITEM" : "item";
            final String outerId = policy == NamingPolicy.SCREAMING_SNAKE_CASE ? "ID" : "id";

            assertEquals("SELECT * FROM " + table + " c WHERE (NOT EXISTS (" + projection + " WHERE c.id = @p0)) AND (c." + outerId + " = @p1)",
                    spec.getQueryText());
            assertEquals(List.of("child", "root"), spec.getParameters().stream().map(parameter -> parameter.getValue(String.class)).toList());
        }
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = { "(SELECT VALUE 1) = 1 | (SELECT value 1) = 1", "EXISTS (SELECT VALUE c.name) | EXISTS (SELECT value c.name)",
            "(select * 2) > 1 | (c.select * 2) > 1", "(select + 2) > 1 | (c.select + 2) > 1", "(select IS NULL) | (IS_NULL(c.select))",
            "(select AND active) | (c.select AND c.active)", "(select[0] = 1) | (c.select[0] = 1)", "(select - 1) > 0 | (c.select - 1) > 0",
            "EXISTS (SELECT -1) | EXISTS (SELECT -1)", "EXISTS (SELECT [c.id]) | EXISTS (SELECT [c.id])",
            "NOT EXISTS (SELECT (c.id) AS projection) | NOT EXISTS (SELECT (c.id) AS projection)", "exists(SELECT -1) | exists(SELECT -1)" })
    void rawSubqueriesAndSelectPropertiesRemainDistinct(final String condition, final String expected) {
        final SqlQuerySpec spec = capture(NamingPolicy.SNAKE_CASE, Filters.expr(condition + " AND NOT group"));

        assertEquals("SELECT * FROM item c WHERE " + expected + " AND NOT c.group", spec.getQueryText());
    }

    @ParameterizedTest
    @ValueSource(strings = { "SELECT []", "SELECT [ ]", "SELECT [  ]" })
    void emptyArrayFromLessSubqueryIsUnambiguousForEveryNamingPolicy(final String subquery) {
        for (final NamingPolicy policy : List.of(NamingPolicy.SNAKE_CASE, NamingPolicy.SCREAMING_SNAKE_CASE, NamingPolicy.CAMEL_CASE)) {
            final Condition condition = Filters.and(Filters.expr("IS_DEFINED((" + subquery + "))"), Filters.eq("id", "root"));
            final SqlQuerySpec spec = capture(policy, condition);
            final String table = policy == NamingPolicy.SCREAMING_SNAKE_CASE ? "ITEM" : "item";
            final String outerId = policy == NamingPolicy.SCREAMING_SNAKE_CASE ? "ID" : "id";

            assertEquals("SELECT * FROM " + table + " c WHERE (IS_DEFINED((" + subquery + "))) AND (c." + outerId + " = @p0)", spec.getQueryText());
            assertEquals("root", spec.getParameters().getFirst().getValue(String.class));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "SELECT -1", "SELECT +1", "SELECT -0.5", "SELECT +1.25", "SELECT -1e3" })
    void unaryFromLessSubqueriesUseKeywordCasingToAvoidPropertyAmbiguity(final String subquery) {
        for (final NamingPolicy policy : List.of(NamingPolicy.SNAKE_CASE, NamingPolicy.CAMEL_CASE)) {
            final Condition condition = Filters.and(Filters.expr("IS_DEFINED((" + subquery + "))"), Filters.eq("id", "root"));
            final SqlQuerySpec spec = capture(policy, condition);
            final String outerId = policy == NamingPolicy.SCREAMING_SNAKE_CASE ? "ID" : "id";

            assertEquals("SELECT * FROM item c WHERE (IS_DEFINED((" + subquery + "))) AND (c." + outerId + " = @p0)", spec.getQueryText());
            assertEquals("root", spec.getParameters().getFirst().getValue(String.class));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "SELECT [c.id]", "SELECT [c.id] AS ids", "SELECT [c.id] WHERE c.id = 'x'", "SELECT [c.id] AS ids WHERE c.id = 'x'",
            "SELECT -1 AS value" })
    void nonExistsFromLessProjectionsRetainLocalAliasesAndClauses(final String subquery) {
        final SqlQuerySpec spec = capture(NamingPolicy.SNAKE_CASE, Filters.expr("IS_DEFINED((" + subquery + ")) AND NOT group"));

        assertEquals("SELECT * FROM item c WHERE IS_DEFINED((" + subquery + ")) AND NOT c.group", spec.getQueryText());
    }

    @Test
    void nonExistsUnarySubqueryCanBeUsedAsScalarOperand() {
        final SqlQuerySpec spec = capture(NamingPolicy.SNAKE_CASE, Filters.expr("(SELECT -1) = -1 AND NOT group"));

        assertEquals("SELECT * FROM item c WHERE (SELECT -1) = -1 AND NOT c.group", spec.getQueryText());
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = { "SELECT (id) | SELECT (c.id)", "SELECT (id) AS id | SELECT (c.id) AS id" })
    void parenthesizedFromLessProjectionKeepsAliasBare(final String subquery, final String expectedSubquery) {
        // abacus-query qualifies the raw projection with the outer alias before this pass. A projection qualification
        // is a valid correlated reference, but the same rewrite must never reach the AS alias: "AS c.id" is invalid.
        final SqlQuerySpec spec = capture(NamingPolicy.SNAKE_CASE, Filters.expr("IS_DEFINED((" + subquery + ")) AND NOT group"));

        assertEquals("SELECT * FROM item c WHERE IS_DEFINED((" + expectedSubquery + ")) AND NOT c.group", spec.getQueryText());
        assertFalse(spec.getQueryText().contains("AS c."), spec.getQueryText());
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = { "SELECT (category) AS label | SELECT (c.category) AS label", "SELECT (id + bonus) | SELECT (c.id + c.bonus)" })
    void parenthesizedFromLessProjectionQualifiesNamesLeftBareByAbacusQuery(final String subquery, final String expectedSubquery) {
        // abacus-query qualifies mapped properties (id) but leaves other names (category, bonus) bare. A from-less
        // subquery still refers to the outer item, so the Cosmos pass must qualify those names as well.
        final SqlQuerySpec spec = capture(NamingPolicy.SNAKE_CASE, Filters.expr("IS_DEFINED((" + subquery + ")) AND NOT group"));

        assertEquals("SELECT * FROM item c WHERE IS_DEFINED((" + expectedSubquery + ")) AND NOT c.group", spec.getQueryText());
    }

    @Test
    void quotedTextThatLooksLikeAQualifiedAliasIsPreserved() {
        final SqlQuerySpec spec = capture(NamingPolicy.SNAKE_CASE, Filters.expr("IS_DEFINED((SELECT (id) AS id)) AND note = 'AS c.x'"));

        assertEquals("SELECT * FROM item c WHERE IS_DEFINED((SELECT (c.id) AS id)) AND c.note = 'AS c.x'", spec.getQueryText());
    }

    @Test
    void lowercaseAsAliasIsNormalizedToBareAlias() {
        final SqlQuerySpec spec = capture(NamingPolicy.SNAKE_CASE, Filters.expr("IS_DEFINED((SELECT (id) as id))"));

        assertEquals("SELECT * FROM item c WHERE IS_DEFINED((SELECT (c.id) AS id))", spec.getQueryText());
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = { "(select -1) > 0 | (c.select -1) > 0", "(select [0]) = 1 | (c.select [0]) = 1" })
    void ambiguousFromLessPropertyFormsRemainQualified(final String condition, final String expected) {
        final SqlQuerySpec spec = capture(NamingPolicy.SNAKE_CASE, Filters.expr(condition + " AND NOT group"));

        assertEquals("SELECT * FROM item c WHERE " + expected + " AND NOT c.group", spec.getQueryText());
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = { "(SELECT -1) > 0 | (c.SELECT -1) > 0", "(SELECT [0]) = 1 | (c.SELECT [0]) = 1" })
    void ambiguousUppercaseFormsRemainPropertiesUnderScreamingSnake(final String condition, final String expected) {
        final SqlQuerySpec spec = capture(NamingPolicy.SCREAMING_SNAKE_CASE, Filters.expr(condition + " AND NOT group"));

        assertEquals("SELECT * FROM ITEM c WHERE " + expected + " AND NOT c.GROUP", spec.getQueryText());
    }

    @ParameterizedTest
    @ValueSource(strings = { "+", "-", "*", "/" })
    void fromPropertyFollowedByArithmeticDoesNotStartSubqueryScope(final String operator) {
        final String expression = "(select * from " + operator + " 1) > 0 AND NOT group";
        final String expected = "(c.select * c.from " + operator + " 1) > 0 AND NOT c.group";
        final SqlQuerySpec spec = capture(NamingPolicy.SNAKE_CASE, Filters.expr(expression));

        assertEquals("SELECT * FROM item c WHERE " + expected, spec.getQueryText());
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = { "(select * from IN (1, 2)) | (c.select * c.from IN (1, 2))",
            "(select * from LIKE 'x%') | (c.select * c.from LIKE 'x%')" })
    void fromPropertyFollowedByPredicateDoesNotStartSubqueryScope(final String condition, final String expected) {
        final SqlQuerySpec spec = capture(NamingPolicy.SNAKE_CASE, Filters.expr(condition + " AND NOT group"));

        assertEquals("SELECT * FROM item c WHERE " + expected + " AND NOT c.group", spec.getQueryText());
    }

    @ParameterizedTest
    @ValueSource(strings = { "[0]", "[ 0 ]", "['x']" })
    void fromPropertySubscriptDoesNotStartSubqueryScope(final String subscript) {
        final String expression = "(select * from " + subscript + ") > 0 AND NOT group";
        final String expected = "(c.select * c.from " + subscript + ") > 0 AND NOT c.group";
        final SqlQuerySpec spec = capture(NamingPolicy.SNAKE_CASE, Filters.expr(expression));

        assertEquals("SELECT * FROM item c WHERE " + expected, spec.getQueryText());
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = { "SELECT * FROM c | SELECT * FROM c",
            "SELECT * FROM (SELECT VALUE 1) AS nested | SELECT * FROM (SELECT value 1) AS nested" })
    void validFromSourcesStillEnableNonExistsSubqueryScope(final String subquery, final String expectedSubquery) {
        final SqlQuerySpec spec = capture(NamingPolicy.SNAKE_CASE, Filters.expr("IS_DEFINED((" + subquery + ")) AND NOT group"));

        assertEquals("SELECT * FROM item c WHERE IS_DEFINED((" + expectedSubquery + ")) AND NOT c.group", spec.getQueryText());
    }

    private SqlQuerySpec capture(final NamingPolicy policy, final Condition condition) {
        new CosmosContainerExecutor(container, policy).streamItems(condition, Item.class).toList();
        return query.getValue();
    }

    public static class Item {
        public String id;
    }
}
