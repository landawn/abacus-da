package com.landawn.abacus.da.cassandra;

import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.PSC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.entity.Account;

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
}
