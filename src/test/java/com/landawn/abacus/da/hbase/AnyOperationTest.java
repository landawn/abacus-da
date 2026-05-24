/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.TestBase;

/**
 * Dedicated unit tests for the abstract base class {@link AnyOperation}.
 * Tests are exercised via {@link AnyGet} as a concrete subclass to drive
 * coverage of the inherited methods (fingerprint / toMap / toJson / toString).
 */
public class AnyOperationTest extends TestBase {

    // getFingerprint
    @Test
    public void testGetFingerprint_returnsNonNullMap() {
        AnyGet get = AnyGet.of("row");
        Map<String, Object> fp = get.getFingerprint();
        assertNotNull(fp);
    }

    @Test
    public void testGetFingerprint_containsTableData() {
        AnyGet get = AnyGet.of("row").addFamily("cf");
        Map<String, Object> fp = get.getFingerprint();
        assertNotNull(fp);
        assertTrue(fp.size() > 0);
    }

    // toMap (no-arg and maxCols)
    @Test
    public void testToMap_returnsNonNullMap() {
        AnyGet get = AnyGet.of("row");
        Map<String, Object> map = get.toMap();
        assertNotNull(map);
    }

    @Test
    public void testToMap_containsRowKey() {
        AnyGet get = AnyGet.of("the-row");
        Map<String, Object> map = get.toMap();
        assertNotNull(map);
        assertTrue(map.containsKey("row"));
    }

    @Test
    public void testToMap_withMaxCols_returnsNonNullMap() {
        AnyGet get = AnyGet.of("row").addColumn("cf", "q1").addColumn("cf", "q2");
        Map<String, Object> map = get.toMap(1);
        assertNotNull(map);
    }

    @Test
    public void testToMap_withMaxColsZero() {
        AnyGet get = AnyGet.of("row").addColumn("cf", "q");
        Map<String, Object> map = get.toMap(0);
        assertNotNull(map);
    }

    @Test
    public void testToMap_withLargeMaxCols() {
        AnyGet get = AnyGet.of("row").addColumn("cf", "q");
        Map<String, Object> map = get.toMap(Integer.MAX_VALUE);
        assertNotNull(map);
    }

    // toJson (no-arg and maxCols)
    @Test
    public void testToJson_returnsNonEmptyString() {
        AnyGet get = AnyGet.of("row");
        String json = get.toJson();
        assertNotNull(json);
        assertTrue(json.length() > 0);
    }

    @Test
    public void testToJson_isValidJsonShape() {
        AnyGet get = AnyGet.of("row").addColumn("cf", "q");
        String json = get.toJson();
        assertNotNull(json);
        assertTrue(json.trim().startsWith("{"));
    }

    @Test
    public void testToJson_withMaxCols_returnsNonEmptyString() {
        AnyGet get = AnyGet.of("row").addColumn("cf", "q1").addColumn("cf", "q2");
        String json = get.toJson(1);
        assertNotNull(json);
        assertTrue(json.length() > 0);
    }

    @Test
    public void testToJson_withMaxColsZero() {
        AnyGet get = AnyGet.of("row").addColumn("cf", "q");
        String json = get.toJson(0);
        assertNotNull(json);
    }

    @Test
    public void testToJson_withMaxColsLarge() {
        AnyGet get = AnyGet.of("row").addColumn("cf", "q");
        String json = get.toJson(Integer.MAX_VALUE);
        assertNotNull(json);
    }

    // toString (no-arg and maxCols)
    @Test
    public void testToString_returnsNonNull() {
        AnyGet get = AnyGet.of("row");
        String s = get.toString();
        assertNotNull(s);
        assertTrue(s.length() > 0);
    }

    @Test
    public void testToString_withMaxCols_returnsNonNull() {
        AnyGet get = AnyGet.of("row").addColumn("cf", "q1").addColumn("cf", "q2");
        String s = get.toString(1);
        assertNotNull(s);
        assertTrue(s.length() > 0);
    }

    @Test
    public void testToString_withMaxColsZero() {
        AnyGet get = AnyGet.of("row").addColumn("cf", "q");
        String s = get.toString(0);
        assertNotNull(s);
    }

    @Test
    public void testToString_withMaxColsLarge() {
        AnyGet get = AnyGet.of("row").addColumn("cf", "q");
        String s = get.toString(Integer.MAX_VALUE);
        assertNotNull(s);
    }

    // Constructor null-check
    @Test
    public void testConstructor_nullOperation_throws() {
        assertThrows(IllegalArgumentException.class, () -> new TestOperation(null));
    }

    @Test
    public void testConstructor_nonNullOperation_succeeds() {
        Get g = new Get(Bytes.toBytes("row"));
        TestOperation op = new TestOperation(g);
        assertNotNull(op);
        assertNotNull(op.toMap());
    }

    @Test
    public void testToString_matchesUnderlyingOperationToString() {
        Get g = new Get(Bytes.toBytes("row"));
        TestOperation op = new TestOperation(g);
        assertEquals(g.toString(), op.toString());
    }

    // Minimal concrete subclass for direct constructor / smoke testing.
    private static final class TestOperation extends AnyOperation<TestOperation> {
        protected TestOperation(final org.apache.hadoop.hbase.client.Operation op) {
            super(op);
        }
    }
}
