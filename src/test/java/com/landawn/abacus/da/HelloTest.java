package com.landawn.abacus.da;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.cassandra.CqlBuilder.PSC;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.util.N;

class HelloTest {

    /**
     */
    @Test
    public void test_expr() {

        String cql = PSC.update(Account.class)
                .set(N.asMap("firstName", Filters.expr("first_name + 'abc'")))
                .where(Filters.eq("firstName", Filters.expr("first_name + 'abc'")))
                .build()
                .query();

        N.println(cql);

        assertEquals("UPDATE account SET first_name = first_name + 'abc' WHERE first_name = first_name + 'abc'", cql);
    }

}
