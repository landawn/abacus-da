/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.MockitoTestBase;
import com.landawn.abacus.util.ClassUtil;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Covers the AnyPut row-key rejection. The setTimeRange IOException handlers in
 * AnyGet, AnyScan, and AnyIncrement are not reachable on HBase 2.6: TimeRange
 * rejects bad bounds with IllegalArgumentException.
 */
public class HBaseAnyLineCoverageTest extends MockitoTestBase {

    @Test
    public void createRejectsEntityWithoutRowKey() {
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> AnyPut.create(new NoRowKeyEntity("label")));

        assertEquals("Row key property is required to create AnyPut instance. But no row key property found in class: "
                + ClassUtil.getCanonicalClassName(NoRowKeyEntity.class), ex.getMessage());
    }

    @Data
    @AllArgsConstructor
    public static class NoRowKeyEntity {
        private String label;
    }
}
