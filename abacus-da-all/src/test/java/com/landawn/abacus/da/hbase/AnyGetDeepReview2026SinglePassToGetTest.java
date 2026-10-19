/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.TestBase;

public class AnyGetDeepReview2026SinglePassToGetTest extends TestBase {

    @Test
    public void testToGet_iteratesInputOnlyOnce() {
        AnyGet first = AnyGet.of("row-1");
        AnyGet second = AnyGet.of("row-2");
        CountingCollection anyGets = new CountingCollection(Arrays.asList(first, second));

        List<Get> gets = AnyGet.toGet(anyGets);

        assertEquals(1, anyGets.iteratorCalls);
        assertEquals(2, gets.size());
        assertSame(first.val(), gets.get(0));
        assertSame(second.val(), gets.get(1));
    }

    private static final class CountingCollection extends AbstractCollection<AnyGet> {
        private final List<AnyGet> delegate;
        private int iteratorCalls;

        private CountingCollection(final List<AnyGet> delegate) {
            this.delegate = delegate;
        }

        @Override
        public Iterator<AnyGet> iterator() {
            iteratorCalls++;
            return delegate.iterator();
        }

        @Override
        public int size() {
            return delegate.size();
        }
    }
}
