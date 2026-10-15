package com.landawn.abacus.da.cassandra;

import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.PSC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.landawn.abacus.query.Filters;

class CqlBuilderBoundaryTest {

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = { "", " ", "a\"b", "a`b", "a\rb", "a\nb", "a--b", "a/*b", "a*/b" })
    void unsafeColumnAliasesAreRejectedBeforeRendering(final String alias) {
        assertThrows(IllegalArgumentException.class, () -> PSC.select(Collections.singletonMap("value", alias)).from("items").build());
    }

    @Test
    void repeatedIntoCallsKeepTheOriginalSingleAndBatchStatements() {
        final CqlBuilder single = PSC.insert("value").into("items", null);
        assertThrows(IllegalStateException.class, () -> single.into("other_items"));
        assertEquals("INSERT INTO items (value) VALUES (?)", single.build().query());
        final CqlBuilder batch = PSC.batchInsert(List.of(Map.of("value", 1), Map.of("value", 2))).into("items");
        assertThrows(IllegalStateException.class, () -> batch.into("other_items"));
        assertEquals("BEGIN BATCH INSERT INTO items (value) VALUES (?); INSERT INTO items (value) VALUES (?); APPLY BATCH", batch.build().query());
    }

    @Test
    void quotedTableIdentifiersRetainEscapedQuotes() {
        final String table = "\"key\"\"space\".\"table\"\"name\"";
        assertEquals("SELECT value FROM " + table, PSC.select("value").from(table).build().query());
    }

    @Test
    void invalidSetClassesFailWithoutChangingTheUpdate() {
        final CqlBuilder builder = PSC.update("items");
        assertThrows(IllegalArgumentException.class, () -> builder.set((Class<?>) null, Set.of()));
        assertThrows(IllegalArgumentException.class, () -> builder.set(String.class, Set.of()));
        assertEquals("UPDATE items SET value = ?", builder.set(Value.class, Set.of()).build().query());
    }

    @Test
    void ifConditionsRejectSelectsAndNullOrderingComparisons() {
        final CqlBuilder select = PSC.select("value").from("items").where(Filters.eq("value", 1));
        assertThrows(IllegalStateException.class, () -> select.onlyIf(Filters.eq("value", 2)));
        assertEquals("SELECT value FROM items WHERE value = ?", select.build().query());
        final CqlBuilder update = PSC.update("items").set("value").where(Filters.eq("key", 1));
        assertThrows(IllegalArgumentException.class, () -> update.onlyIf(Filters.gt("value", null)));
        assertEquals("UPDATE items SET value = ? WHERE key = ?", update.build().query());
    }

    public static class Value {
        private String value;

        public String getValue() {
            return value;
        }

        public void setValue(final String value) {
            this.value = value;
        }
    }
}
