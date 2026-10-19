package com.landawn.abacus.da.cassandra;

import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.PSC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CqlBuilderDeepReview2026TableReferenceTest {

    @ParameterizedTest
    @ValueSource(strings = { "1items", "_items", "1ks.items", "ks.1items", "ks._items" })
    void unquotedTablePartsMustStartWithALetter(final String tableName) {
        final CqlBuilder select = PSC.select("id");
        assertThrows(IllegalArgumentException.class, () -> select.from(tableName));
        assertEquals("SELECT id FROM items", select.from("items").build().query());

        final CqlBuilder insert = PSC.insert("id");
        assertThrows(IllegalArgumentException.class, () -> insert.into(tableName));
        assertEquals("INSERT INTO items (id) VALUES (?)", insert.into("items").build().query());

        assertThrows(IllegalArgumentException.class, () -> PSC.update(tableName));
        assertThrows(IllegalArgumentException.class, () -> PSC.deleteFrom(tableName));
    }

    @Test
    void unquotedTablePartsMayContainDigitsAndUnderscoresAfterTheirFirstLetter() {
        assertEquals("SELECT id FROM item_2", PSC.select("id").from("item_2").build().query());
        assertEquals("SELECT id FROM ks_2.item_2", PSC.select("id").from("ks_2.item_2").build().query());
    }
}
