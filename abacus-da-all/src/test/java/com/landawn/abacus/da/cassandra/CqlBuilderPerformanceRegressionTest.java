/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.cassandra;

import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.NSC;
import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.PSC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.landawn.abacus.query.AbstractQueryBuilder.SP;

class CqlBuilderPerformanceRegressionTest {

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void largeBatchKeepsEveryRowAndBindingWhenAddingConditionalTtl(final boolean ttlFirst) {
        final int rowCount = 2_000;
        final List<Map<String, Object>> rows = new ArrayList<>(rowCount);
        final List<Object> expectedParameters = new ArrayList<>(rowCount * 2);

        for (int i = 0; i < rowCount; i++) {
            final Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", i);
            row.put("value", "row " + i + "; USING ignored");
            rows.add(row);
            expectedParameters.addAll(row.values());
        }

        final CqlBuilder builder = PSC.batchInsert(rows).into("items");

        if (ttlFirst) {
            builder.usingTTL(60).ifNotExists();
        } else {
            builder.ifNotExists().usingTTL(60);
        }

        assertThrows(IllegalStateException.class, () -> builder.usingTTL(120));
        final SP result = builder.build();
        final String statement = "INSERT INTO items (id, value) VALUES (?, ?) IF NOT EXISTS USING TTL 60; ";
        assertEquals("BEGIN BATCH " + statement.repeat(rowCount) + "APPLY BATCH", result.query());
        assertEquals(expectedParameters, result.parameters());
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void combiningBatchOptionsPreservesNamedBindingsAndOptionOrder(final boolean ttlFirst) {
        final List<Map<String, Object>> rows = List.of(Map.of("id", 11), Map.of("id", 22), Map.of("id", 33));
        final SP original = NSC.batchInsert(rows).into("items").build();
        final CqlBuilder builder = NSC.batchInsert(rows).into("items");

        if (ttlFirst) {
            builder.usingTTL(60).usingTimestampMicros(123);
        } else {
            builder.usingTimestampMicros(123).usingTTL(60);
        }

        final SP result = builder.build();
        final String clause = ttlFirst ? " USING TTL 60 AND TIMESTAMP 123" : " USING TIMESTAMP 123 AND TTL 60";
        assertEquals(original.query().replace(";", clause + ";"), result.query());
        assertEquals(original.parameters(), result.parameters());
        assertEquals(ParsedCql.parse(original.query()).namedParameters(), ParsedCql.parse(result.query()).namedParameters());
    }
}
