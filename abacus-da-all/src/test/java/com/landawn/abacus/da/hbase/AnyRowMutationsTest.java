/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.TestBase;

/**
 * Pure unit tests for {@link AnyRowMutations} (no live HBase). Exercises
 * factory methods, mutation accumulation, and Row interface methods.
 */
public class AnyRowMutationsTest extends TestBase {

    // ---------------------------------------------------------------------
    // Factory methods
    // ---------------------------------------------------------------------

    @Test
    public void testOf_stringRowKey() {
        AnyRowMutations rm = AnyRowMutations.of("row-1");
        assertNotNull(rm);
        assertArrayEquals(Bytes.toBytes("row-1"), rm.getRow());
    }

    @Test
    public void testOf_byteArrayRowKey() {
        byte[] row = Bytes.toBytes("row-bytes");
        AnyRowMutations rm = AnyRowMutations.of(row);
        assertArrayEquals(row, rm.getRow());
    }

    @Test
    public void testOf_stringRowKeyWithCapacity() {
        AnyRowMutations rm = AnyRowMutations.of("row-1", 10);
        assertNotNull(rm);
        assertArrayEquals(Bytes.toBytes("row-1"), rm.getRow());
    }

    @Test
    public void testOf_byteArrayWithCapacity() {
        byte[] row = Bytes.toBytes("row-bytes");
        AnyRowMutations rm = AnyRowMutations.of(row, 5);
        assertArrayEquals(row, rm.getRow());
    }

    // ---------------------------------------------------------------------
    // val()
    // ---------------------------------------------------------------------

    @Test
    public void testVal_returnsUnderlyingRowMutations() {
        AnyRowMutations rm = AnyRowMutations.of("row");
        RowMutations underlying = rm.val();
        assertNotNull(underlying);
        assertArrayEquals(Bytes.toBytes("row"), underlying.getRow());
    }

    @Test
    public void testVal_consistentAcrossCalls() {
        AnyRowMutations rm = AnyRowMutations.of("row");
        assertSame(rm.val(), rm.val());
    }

    // ---------------------------------------------------------------------
    // add(Mutation)
    // ---------------------------------------------------------------------

    @Test
    public void testAdd_putMutation_returnsSelf() throws IOException {
        AnyRowMutations rm = AnyRowMutations.of("row");
        Put put = new Put(Bytes.toBytes("row")).addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("v"));
        AnyRowMutations returned = rm.add(put);
        assertSame(rm, returned);
        assertEquals(1, rm.val().getMutations().size());
    }

    @Test
    public void testAdd_deleteMutation() throws IOException {
        AnyRowMutations rm = AnyRowMutations.of("row");
        Delete delete = new Delete(Bytes.toBytes("row")).addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q"));
        rm.add(delete);
        assertEquals(1, rm.val().getMutations().size());
    }

    @Test
    public void testAdd_mismatchedRowKey_throws() {
        // Adding a mutation with a different row key should fail.
        AnyRowMutations rm = AnyRowMutations.of("row-a");
        Put put = new Put(Bytes.toBytes("row-b")).addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("v"));
        assertThrows(IOException.class, () -> rm.add(put));
    }

    // ---------------------------------------------------------------------
    // add(List<Mutation>)
    // ---------------------------------------------------------------------

    @Test
    public void testAdd_listOfMutations() throws IOException {
        AnyRowMutations rm = AnyRowMutations.of("row");
        Put put = new Put(Bytes.toBytes("row")).addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q1"), Bytes.toBytes("v1"));
        Delete delete = new Delete(Bytes.toBytes("row")).addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q2"));

        rm.add(Arrays.<Mutation> asList(put, delete));
        assertEquals(2, rm.val().getMutations().size());
    }

    @Test
    public void testAdd_listOfMutations_returnsSelf() throws IOException {
        AnyRowMutations rm = AnyRowMutations.of("row");
        Put put = new Put(Bytes.toBytes("row")).addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("v"));
        AnyRowMutations returned = rm.add(Arrays.<Mutation> asList(put));
        assertSame(rm, returned);
    }

    // ---------------------------------------------------------------------
    // getRow / compareTo
    // ---------------------------------------------------------------------

    @Test
    public void testGetRow_matchesConstructorRowKey() {
        AnyRowMutations rm = AnyRowMutations.of("the-row-key");
        assertArrayEquals(Bytes.toBytes("the-row-key"), rm.getRow());
    }

    @Test
    public void testCompareTo_sameRow_returnsZero() {
        AnyRowMutations a = AnyRowMutations.of("k");
        AnyRowMutations b = AnyRowMutations.of("k");
        assertEquals(0, a.compareTo(b));
    }

    @Test
    public void testCompareTo_differentRows() {
        AnyRowMutations a = AnyRowMutations.of("a");
        AnyRowMutations b = AnyRowMutations.of("b");
        assertTrue(a.compareTo(b) < 0);
        assertTrue(b.compareTo(a) > 0);
    }

    // ---------------------------------------------------------------------
    // equals, hashCode, toString
    // ---------------------------------------------------------------------

    @Test
    public void testEquals_sameInstance() {
        AnyRowMutations rm = AnyRowMutations.of("row");
        assertEquals(rm, rm);
    }

    @Test
    public void testEquals_matchesUnderlyingRowMutationsEquals() {
        AnyRowMutations a = AnyRowMutations.of("row");
        AnyRowMutations b = AnyRowMutations.of("row");
        assertEquals(a.val().equals(b.val()), a.equals(b));
    }

    @Test
    public void testEquals_nonAnyRowMutationsObject_returnsFalse() {
        AnyRowMutations rm = AnyRowMutations.of("row");
        assertFalse(rm.equals("not an AnyRowMutations"));
        assertFalse(rm.equals(null));
    }

    @Test
    public void testHashCode_matchesUnderlyingRowMutations() {
        AnyRowMutations rm = AnyRowMutations.of("row");
        assertEquals(rm.val().hashCode(), rm.hashCode());
    }

    @Test
    public void testToString_notNull() {
        AnyRowMutations rm = AnyRowMutations.of("row");
        String s = rm.toString();
        assertNotNull(s);
    }

    // ---------------------------------------------------------------------
    // Independence: different instances are not aliased
    // ---------------------------------------------------------------------

    @Test
    public void testTwoInstances_differentUnderlyingRowMutations() {
        AnyRowMutations a = AnyRowMutations.of("row");
        AnyRowMutations b = AnyRowMutations.of("row");
        assertNotSame(a.val(), b.val());
    }
}
