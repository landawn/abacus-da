/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.function.Consumer;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class AnyDeleteReview20261103Test {

    // Pins the 2026-11-02 @throws of the six no-timestamp marker overloads for their second documented path: of(Delete) of a
    // native Delete built with Delete(byte[], long, NavigableMap), which does not validate the timestamp, copies the negative
    // default timestamp; each no-timestamp overload then fails with HBase's IAE and leaves no family entry, while the
    // explicit-timestamp overloads (and addFamilyVersion, which always takes one) still work.
    @Test
    void copiedDeleteWithNegativeDefaultTimestampIsRejectedByEveryNoTimestampMarker() {
        final byte[] f = Bytes.toBytes("f");
        final byte[] q = Bytes.toBytes("q");
        final Delete source = new Delete(Bytes.toBytes("row"), -1L, new TreeMap<>(Bytes.BYTES_COMPARATOR));

        final List<Consumer<AnyDelete>> noTimestampAdders = List.of(d -> d.addFamily("f"), d -> d.addFamily(f), d -> d.addColumn("f", "q"),
                d -> d.addColumn(f, q), d -> d.addColumns("f", "q"), d -> d.addColumns(f, q));

        final List<Executable> checks = new ArrayList<>();
        checks.add(() -> assertEquals(-1L, AnyDelete.of(source).getTimestamp()));

        for (int i = 0; i < noTimestampAdders.size(); i++) {
            final Consumer<AnyDelete> adder = noTimestampAdders.get(i);
            final String msg = "adder " + i;

            checks.add(() -> {
                final AnyDelete delete = AnyDelete.of(source);
                final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> adder.accept(delete), msg);

                assertTrue(e.getMessage().contains("Timestamp cannot be negative"), msg + ": " + e.getMessage());
                assertEquals(0, delete.numFamilies(), msg);
            });
        }

        checks.add(() -> assertEquals(1, AnyDelete.of(source).addFamily("f", 5L).size()));
        checks.add(() -> assertEquals(1, AnyDelete.of(source).addColumn(f, q, 5L).size()));
        checks.add(() -> assertEquals(1, AnyDelete.of(source).addColumns("f", "q", 5L).size()));
        checks.add(() -> assertEquals(1, AnyDelete.of(source).addFamilyVersion(f, 5L).size()));
        // The copy never touches the source's family map.
        checks.add(() -> assertEquals(0, source.numFamilies()));

        assertAll(checks);
    }
}
