/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.TreeMap;

import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

class AnyAppendReview20261103Test {

    // Re-verifies s03's "no twin needed" for the AnyPut negative-default-timestamp @throws: Append.addColumn never checks the
    // default timestamp (unlike Put.addColumn), so an append built or copied with a negative one queues its cells with it.
    @Test
    void negativeDefaultTimestampIsNotRejectedByAddColumn() {
        final AnyAppend built = AnyAppend.of(Bytes.toBytes("r"), -1L, new TreeMap<>(Bytes.BYTES_COMPARATOR));
        final AnyAppend copied = AnyAppend.of(new Append(Bytes.toBytes("r"), -1L, new TreeMap<>(Bytes.BYTES_COMPARATOR)));

        for (final AnyAppend append : new AnyAppend[] { built, copied }) {
            append.addColumn("cf", "q", "v").addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q2"), Bytes.toBytes("w"));
        }

        assertAll(() -> assertEquals(-1L, built.getTimestamp()), () -> assertEquals(-1L, built.get("cf", "q").get(0).getTimestamp()),
                () -> assertEquals(-1L, built.get("cf", "q2").get(0).getTimestamp()), () -> assertEquals(2, built.size()),
                () -> assertEquals(-1L, copied.getTimestamp()), () -> assertEquals(-1L, copied.get("cf", "q").get(0).getTimestamp()),
                () -> assertEquals(2, copied.size()));
    }
}
