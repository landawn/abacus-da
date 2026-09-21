/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */
package com.landawn.abacus.da.cassandra;

import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.PSC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.Collection;
import java.util.Date;

import org.junit.jupiter.api.Test;

/** Regression coverage for validation precedence without a Cassandra server. */
class CassandraValidationOrderTest {

    @Test
    void closedBuilderStatePrecedesInvalidArgumentsAndTimestampOverflow() {
        final CqlBuilder builder = PSC.insert("id").into("users");
        builder.build();

        assertThrows(IllegalStateException.class, () -> builder.usingTTL(-1));
        assertThrows(IllegalStateException.class, () -> builder.usingTimestamp((Date) null));
        assertThrows(IllegalStateException.class, () -> builder.usingTimestamp(Long.MAX_VALUE));
        assertThrows(IllegalStateException.class, () -> builder.from((String) null));
        assertThrows(IllegalStateException.class, () -> builder.from((String[]) null));
        assertThrows(IllegalStateException.class, () -> builder.from((Collection<String>) null));
        assertThrows(IllegalStateException.class, () -> builder.from((Class<?>) null));
    }

    @Test
    void incompatibleOperationPrecedesInvalidUsingArgument() {
        final CqlBuilder builder = PSC.select("id").from("users");

        assertThrows(IllegalStateException.class, () -> builder.usingTTL(-1));
        assertThrows(IllegalStateException.class, () -> builder.usingTimestamp((Date) null));
        assertEquals("SELECT id FROM users", builder.build().query());
    }

    @Test
    void mapperValidatesAllFilesBeforeOpeningAnyFile() {
        final File missing = new File("target/cassandra-review-missing-mapper.xml");

        assertThrows(IllegalArgumentException.class, () -> CqlMapper.loadFrom(missing, null));
    }

    @Test
    void mapperValidatesIdBeforeParsedCql() {
        final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> new CqlMapper().add("", (ParsedCql) null));

        assertTrue(failure.getMessage().contains("id"));
    }
}
