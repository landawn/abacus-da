package com.landawn.abacus.da.azure;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.mockito.ArgumentCaptor;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.models.SqlQuerySpec;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.util.NamingPolicy;

class CosmosContainerExecutorReview20261103Test {

    /** U+20000, a CJK letter outside the BMP: a valid Java identifier character, written in UTF-16 as a surrogate pair. */
    private static final String SUPPLEMENTARY_LETTER = "𠀀";

    private CosmosContainer container;
    private CosmosPagedIterable<Item> rows;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        container = mock(CosmosContainer.class);
        rows = mock(CosmosPagedIterable.class);
        when(rows.streamByPage()).thenAnswer(CosmosPagedTestSupport.noPages());
    }

    private String query(final NamingPolicy namingPolicy, final Condition condition) {
        final ArgumentCaptor<SqlQuerySpec> spec = ArgumentCaptor.forClass(SqlQuerySpec.class);
        when(container.queryItems(spec.capture(), any(), eq(Item.class))).thenReturn(rows);

        new CosmosContainerExecutor(container, namingPolicy).streamItems(condition, Item.class).count();

        return spec.getValue().getQueryText();
    }

    /**
     * A property name containing a character outside the BMP was split at the surrogate pair, because
     * {@code Character.isJavaIdentifierPart(char)} is false for every surrogate: {@code a𠀀b = ?} rendered {@code c.a𠀀c.b = @p0},
     * which Cosmos DB rejects. Such a name is now scanned as one name and, not being a plain ASCII identifier, written with the
     * bracket accessor.
     */
    @ParameterizedTest
    @EnumSource(value = NamingPolicy.class, names = { "CAMEL_CASE", "SNAKE_CASE" }, mode = Mode.INCLUDE)
    void propertyNameWithSupplementaryCharacterIsScannedAsOneName(final NamingPolicy namingPolicy) {
        final String name = "a" + SUPPLEMENTARY_LETTER + "b";

        assertAll(() -> assertEquals("SELECT * FROM item c WHERE c[\"" + name + "\"] = @p0", query(namingPolicy, Filters.eq(name, 1))),
                () -> assertEquals("SELECT * FROM item c WHERE IS_NULL(c[\"" + name + "\"])", query(namingPolicy, Filters.isNull(name))),
                () -> assertEquals("SELECT * FROM item c WHERE c.x[\"" + name + "\"] = 1", query(namingPolicy, Filters.expr("x." + name + " = 1"))),
                () -> assertEquals("SELECT * FROM item c WHERE c[\"" + SUPPLEMENTARY_LETTER + "x\"] = 1",
                        query(namingPolicy, Filters.expr(SUPPLEMENTARY_LETTER + "x = 1"))),
                // NOT before such a name is the unary keyword, as before any other name
                () -> assertEquals("SELECT * FROM item c WHERE c.active = NOT c[\"" + SUPPLEMENTARY_LETTER + "x\"]",
                        query(namingPolicy, Filters.expr("active = NOT " + SUPPLEMENTARY_LETTER + "x"))),
                // a quoted value holding such a name is left alone
                () -> assertEquals("SELECT * FROM item c WHERE c.id = '" + name + "'", query(namingPolicy, Filters.expr("id = '" + name + "'"))));
    }

    // Every helper that scans names was converted from char-based to code-point-based tests (isCosmosNamePart/isCosmosNameStart).
    // The tests below pin each converted site with a supplementary character (U+20000 𠀀, a letter; U+1D7CE 𝟎, a digit) placed
    // where the char-based test, false for every surrogate, took a different path; each fails if its site is reverted.

    /**
     * Member access ({@code escapeCosmosReservedPropertyNames}): a path segment starting with, or following a name ending
     * with, a supplementary letter is escaped like any other non-ASCII segment; the char-based scan left {@code c.x.𠀀b} and
     * {@code c["a𠀀"].b𠀀} as is (a syntax error).
     */
    @ParameterizedTest
    @EnumSource(value = NamingPolicy.class, names = { "CAMEL_CASE", "SNAKE_CASE" }, mode = Mode.INCLUDE)
    void memberAccessNextToSupplementaryCharacterIsEscaped(final NamingPolicy namingPolicy) {
        assertAll(() -> assertEquals("SELECT * FROM item c WHERE c.x[\"𠀀b\"] = 1", query(namingPolicy, Filters.expr("x.𠀀b = 1"))),
                () -> assertEquals("SELECT * FROM item c WHERE c[\"a𠀀\"][\"b𠀀\"] = 1", query(namingPolicy, Filters.expr("a𠀀.b𠀀 = 1"))));
    }

    /**
     * {@code removeQualifiedAliases}: an {@code AS} glued to a supplementary letter ends a name ({@code a𠀀AS}), it is not the
     * alias keyword, so the qualified name after it keeps its alias ({@code c.b}, not {@code b}).
     */
    @ParameterizedTest
    @EnumSource(value = NamingPolicy.class, names = { "CAMEL_CASE", "SNAKE_CASE" }, mode = Mode.INCLUDE)
    void asGluedToSupplementaryCharacterIsNotAnAlias(final NamingPolicy namingPolicy) {
        assertEquals("SELECT * FROM item c WHERE c[\"a𠀀as\"] c.b", query(namingPolicy, Filters.expr("a𠀀AS b")));
    }

    /**
     * The qualification scan ({@code attachedToPreviousIdentifier}): a name right after a supplementary digit belongs to that
     * token and is left alone, as after an ASCII digit ({@code 2x}); it was qualified as {@code 𝟎c.x}.
     */
    @ParameterizedTest
    @EnumSource(value = NamingPolicy.class, names = { "CAMEL_CASE", "SNAKE_CASE" }, mode = Mode.INCLUDE)
    void nameAfterSupplementaryDigitBelongsToThatToken(final NamingPolicy namingPolicy) {
        assertAll(() -> assertEquals("SELECT * FROM item c WHERE 2x = 1", query(namingPolicy, Filters.expr("2x = 1"))),
                () -> assertEquals("SELECT * FROM item c WHERE 𝟎x = 1", query(namingPolicy, Filters.expr("𝟎x = 1"))));
    }

    /**
     * Keyword recognition: a keyword glued to a supplementary letter, before it ({@code x𠀀AND}) or after it
     * ({@code AND𠀀}), is part of a name, not the keyword. Covers {@code endsWithLogicalKeyword} (both ends: an IS NULL operand
     * is not cut at such an AND/NOT), {@code startsWithCosmosKeyword} ({@code order} before {@code AND𠀀} is not taken for an
     * operand of AND), the previous-token scan of {@code isCosmosKeywordPropertyReference} ({@code order} after {@code x𠀀OR}),
     * and {@code followsCompleteCosmosOperand} (a name ending with, or in {@code AND} after, a supplementary letter completes
     * an operand, so the following BETWEEN is the keyword, not {@code c["BETWEEN"] -1}).
     */
    @ParameterizedTest
    @EnumSource(value = NamingPolicy.class, names = { "CAMEL_CASE", "SNAKE_CASE" }, mode = Mode.INCLUDE)
    void keywordGluedToSupplementaryCharacterIsPartOfTheName(final NamingPolicy namingPolicy) {
        assertAll(() -> assertEquals("SELECT * FROM item c WHERE IS_NULL(c[\"x𠀀and\"])", query(namingPolicy, Filters.isNull("x𠀀AND"))),
                () -> assertEquals("SELECT * FROM item c WHERE NOT IS_NULL(c[\"x𠀀not\"])", query(namingPolicy, Filters.isNotNull("x𠀀NOT"))),
                () -> assertEquals("SELECT * FROM item c WHERE c.a = 1 AND IS_NULL(NOT𠀀(c.b))",
                        query(namingPolicy, Filters.expr("a = 1 AND NOT𠀀(b) IS NULL"))),
                () -> assertEquals("SELECT * FROM item c WHERE order c[\"and𠀀\"] = 1", query(namingPolicy, Filters.expr("order AND𠀀 = 1"))),
                () -> assertEquals("SELECT * FROM item c WHERE c[\"x𠀀or\"] order", query(namingPolicy, Filters.expr("x𠀀OR order"))),
                () -> assertEquals("SELECT * FROM item c WHERE c[\"price𠀀\"] BETWEEN -1 AND 1", query(namingPolicy, Filters.expr("price𠀀 BETWEEN -1 AND 1"))),
                () -> assertEquals("SELECT * FROM item c WHERE c[\"x𠀀and\"] BETWEEN -1 AND 1", query(namingPolicy, Filters.expr("x𠀀AND BETWEEN -1 AND 1"))));
    }

    /**
     * {@code startsCosmosNegatedOperand}: NOT before a name that starts with a supplementary letter, or that only starts like a
     * keyword ({@code in𠀀}), is the unary keyword; it was read as a property juxtaposed with that name ({@code c["NOT"] ...}).
     */
    @ParameterizedTest
    @EnumSource(value = NamingPolicy.class, names = { "CAMEL_CASE", "SNAKE_CASE" }, mode = Mode.INCLUDE)
    void notBeforeSupplementaryNameIsUnary(final NamingPolicy namingPolicy) {
        assertAll(() -> assertEquals("SELECT * FROM item c WHERE c.active = NOT c[\"𠀀x\"]", query(namingPolicy, Filters.expr("active = NOT 𠀀x"))),
                () -> assertEquals("SELECT * FROM item c WHERE c.active = NOT c[\"in𠀀\"]", query(namingPolicy, Filters.expr("active = NOT in𠀀"))));
    }

    /**
     * Subquery detection: the FROM source ({@code hasCosmosSubqueryFromClause}) and the AS alias of a FROM-less projection
     * ({@code isCosmosFromLessProjectionEnd}) may start with, end with, or only start like a keyword before ({@code in𠀀}), a
     * supplementary letter: the subquery is still left unqualified (it became {@code c["SELECT"] * c["FROM"] ...}). An EXISTS
     * glued to a supplementary letter ({@code isCosmosExistsSubquery}) is a function name, not the keyword.
     */
    @ParameterizedTest
    @EnumSource(value = NamingPolicy.class, names = { "CAMEL_CASE", "SNAKE_CASE" }, mode = Mode.INCLUDE)
    void subqueryWithSupplementaryNamesIsRecognized(final NamingPolicy namingPolicy) {
        assertAll(() -> assertEquals("SELECT * FROM item c WHERE c.a IN (SELECT * FROM 𠀀t)", query(namingPolicy, Filters.expr("a IN (SELECT * FROM 𠀀t)"))),
                () -> assertEquals("SELECT * FROM item c WHERE c.a IN (SELECT * FROM in𠀀)", query(namingPolicy, Filters.expr("a IN (SELECT * FROM in𠀀)"))),
                () -> assertEquals("SELECT * FROM item c WHERE c.a IN (SELECT [] AS 𠀀x)", query(namingPolicy, Filters.expr("a IN (SELECT [] AS 𠀀x)"))),
                () -> assertEquals("SELECT * FROM item c WHERE c.a IN (SELECT [] AS x𠀀)", query(namingPolicy, Filters.expr("a IN (SELECT [] AS x𠀀)"))),
                () -> assertEquals("SELECT * FROM item c WHERE 𠀀EXISTS(c[\"SELECT\"] * 2) = 1",
                        query(namingPolicy, Filters.expr("𠀀EXISTS(SELECT * 2) = 1"))));
    }

    /**
     * {@code isFollowedByCosmosFunctionName}: a user-defined function whose name starts or ends with a supplementary letter is
     * called through {@code udf}, which stays unqualified (it became {@code c["udf"].𠀀f(...)}).
     */
    @ParameterizedTest
    @EnumSource(value = NamingPolicy.class, names = { "CAMEL_CASE", "SNAKE_CASE" }, mode = Mode.INCLUDE)
    void udfCallWithSupplementaryFunctionNameIsNotQualified(final NamingPolicy namingPolicy) {
        assertAll(() -> assertEquals("SELECT * FROM item c WHERE udf.𠀀f(c.x) = 1", query(namingPolicy, Filters.expr("udf.𠀀f(x) = 1"))),
                () -> assertEquals("SELECT * FROM item c WHERE udf.f𠀀(c.x) = 1", query(namingPolicy, Filters.expr("udf.f𠀀(x) = 1"))));
    }

    public static class Item {
        private String id;
        private boolean active;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public boolean isActive() {
            return active;
        }

        public void setActive(final boolean active) {
            this.active = active;
        }
    }
}
