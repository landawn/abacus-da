package com.landawn.abacus.da.azure;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.models.SqlQuerySpec;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.landawn.abacus.query.Filters;

class CosmosConditionSyntaxTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("predicates")
    @SuppressWarnings("unchecked")
    void nullPredicatesPreserveQuotedPropertiesAndNestedExpressions(final String input, final String expected) {
        final CosmosContainer container = mock(CosmosContainer.class);
        final CosmosPagedIterable<Item> rows = mock(CosmosPagedIterable.class);
        when(rows.stream()).thenAnswer(invocation -> Stream.empty());
        final ArgumentCaptor<SqlQuerySpec> query = ArgumentCaptor.forClass(SqlQuerySpec.class);
        when(container.queryItems(query.capture(), any(), eq(Item.class))).thenReturn(rows);
        final CosmosContainerExecutor executor = new CosmosContainerExecutor(container);
        try (var stream = executor.streamItems(Filters.expr(input), Item.class)) {
            assertTrue(stream.toList().isEmpty());
        }
        final String actual = query.getValue().getQueryText();
        assertTrue(actual.endsWith(" WHERE " + expected), actual);
    }

    static Stream<Arguments> predicates() {
        return Stream.of(Arguments.of("c['a b'] IS NULL", "IS_NULL(c['a b'])"),
                Arguments.of("c[\"parent\"][\"child\"] IS NOT NULL", "NOT IS_NULL(c[\"parent\"][\"child\"])"),
                Arguments.of("c[\"a\\\"b\"] IS NULL", "IS_NULL(c[\"a\\\"b\"])"),
                Arguments.of("COALESCE(c['x'], c['y']) IS NULL", "IS_NULL(COALESCE(c['x'], c['y']))"),
                Arguments.of("name = 'O''Brien' AND c['a b'] IS NULL", "c.name = 'O''Brien' AND IS_NULL(c['a b'])"),
                Arguments.of("name = \"a\\\"b\" AND value IS NULL", "c.name = \"a\\\"b\" AND IS_NULL(c[\"value\"])"),
                Arguments.of("'literal' IS NULL", "IS_NULL('literal')"),
                Arguments.of("c[0] IS NULL OR c[1] IS NOT NULL", "IS_NULL(c[0]) OR NOT IS_NULL(c[1])"));
    }

    public static class Item {
        private String id;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }
    }
}
