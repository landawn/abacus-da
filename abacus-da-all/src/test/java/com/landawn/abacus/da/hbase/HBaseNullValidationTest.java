/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */
package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.util.NamingPolicy;

class HBaseNullValidationTest {

    @Test
    void requiredConversionArgumentsUseArgumentValidationInSignatureOrder() {
        assertTrue(assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.toValue(null, String.class)).getMessage().contains("result"));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.toValue(null, null)).getMessage().contains("result"));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.toList((List<Result>) null, String.class))
                .getMessage().contains("results"));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.toList((List<Result>) null, null))
                .getMessage().contains("results"));
    }

    @Test
    void metadataArgumentsAreValidatedBeforeCacheAccess() {
        assertTrue(assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.getRowKeySetMethod(null)).getMessage().contains("targetType"));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.getClassFamilyColumnNameMap(null, NamingPolicy.CAMEL_CASE))
                .getMessage().contains("entityClass"));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.getClassFamilyColumnNameMap(String.class, null))
                .getMessage().contains("namingPolicy"));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.getClassFamilyColumnNameMap(null, null))
                .getMessage().contains("entityClass"));
    }

    @Test
    void cellDecodersRejectNullCellsBeforeReadingTheirArrays() {
        assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.getRowKeyString(null));
        assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.getFamilyString(null));
        assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.getQualifierString(null));
        assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.getValueString(null));
    }

    @Test
    void scannerIsRequiredOnlyWhenTheWindowReadsRows() {
        assertTrue(assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.toList((ResultScanner) null, String.class))
                .getMessage().contains("resultScanner"));
        assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.toList(null, 0, 1, String.class));
        assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.toList(null, 1, 0, String.class));
        assertTrue(HBaseExecutor.toList(null, 0, 0, String.class).isEmpty());
        assertTrue(assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.toList(null, 0, 0, null))
                .getMessage().contains("targetType"));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.toList(null, -1, 0, String.class))
                .getMessage().contains("Offset"));
    }

    @Test
    void scannerClosesWithoutReadingWhenTargetValidationFails() throws IOException {
        final ResultScanner scanner = mock(ResultScanner.class);

        assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.toList(scanner, (Class<String>) null));

        verify(scanner, never()).next();
        verify(scanner).close();
    }

    @Test
    @SuppressWarnings("deprecation")
    void inheritedAndConditionalNullContractsRemainIntact() {
        assertThrows(NullPointerException.class, () -> AnyGet.of("row").compareTo(null));
        assertThrows(NullPointerException.class, () -> AnyPut.of("row").compareTo(null));
        assertThrows(NullPointerException.class, () -> AnyRowMutations.of("row").compareTo(null));

        final AnyPut put = AnyPut.of("row").addColumn("family", "qualifier", "value");
        assertFalse(put.has("missing", "qualifier", (Object) null));
        assertFalse(put.has("family", "missing", (Object) null));
        assertThrows(NullPointerException.class, () -> put.has("family", "qualifier", (Object) null));
    }
}
