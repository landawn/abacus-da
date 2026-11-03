/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.client.Durability;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.IOUtil;

class AsyncHBaseExecutorReview20261102Test {

    // Sync-only API: executor plumbing, static helpers and the id-keyed mapper facade.
    private static final Set<String> SYNC_ONLY = Set.of("admin", "connection", "async", "getTable", "mapper", "close", "registerRowKeyProperty", "toList",
            "toEntity");

    private static int counter = 0;

    private static Object sample(final Class<?> type) {
        final int n = counter++;

        if (type == String.class) {
            return "s" + n;
        } else if (type == long.class) {
            return 1000L + n;
        } else if (type == byte[].class) {
            return new byte[] { (byte) n };
        } else if (type == Class.class) {
            return Integer.class;
        } else if (type == Object.class) {
            return "o" + n; // distinct per parameter, so swapped Object arguments are caught
        } else if (type == Durability.class) {
            return Durability.SKIP_WAL;
        } else if (type == List.class || type == java.util.Collection.class) {
            return new ArrayList<>(List.of("e" + n));
        } else {
            return mock(type);
        }
    }

    // Every async operation must hand exactly its own arguments to the same-signature synchronous overload, once,
    // and call nothing else on the synchronous executor (a wrong overload or a dropped/swapped argument fails here).
    @Test
    void everyAsyncMethodDelegatesToTheSameSignatureSyncMethodWithIdenticalArguments() throws Exception {
        final AsyncExecutor direct = new AsyncExecutor(Runnable::run);
        int checked = 0;

        for (final Method asyncMethod : AsyncHBaseExecutor.class.getDeclaredMethods()) {
            if (!Modifier.isPublic(asyncMethod.getModifiers()) || asyncMethod.getName().equals("sync")) {
                continue;
            }

            final Class<?>[] types = asyncMethod.getParameterTypes();
            final Method syncMethod = HBaseExecutor.class.getMethod(asyncMethod.getName(), types);
            final Object[] args = Arrays.stream(types).map(AsyncHBaseExecutorReview20261102Test::sample).toArray();

            final HBaseExecutor sync = mock(HBaseExecutor.class);
            final AsyncHBaseExecutor async = new AsyncHBaseExecutor(sync, direct);

            assertEquals(ContinuableFuture.class, asyncMethod.getReturnType(), asyncMethod.toString());
            ((ContinuableFuture<?>) asyncMethod.invoke(async, args)).get();

            syncMethod.invoke(verify(sync), args);
            verifyNoMoreInteractions(sync);
            checked++;
        }

        assertEquals(47, checked);
    }

    @Test
    void everyPublicSyncOperationHasAnAsyncTwin() {
        final Set<String> asyncSignatures = Arrays.stream(AsyncHBaseExecutor.class.getDeclaredMethods())
                .filter(m -> Modifier.isPublic(m.getModifiers()))
                .map(m -> m.getName() + Arrays.toString(m.getParameterTypes()))
                .collect(Collectors.toSet());

        final Set<String> missing = new TreeSet<>();

        for (final Method m : HBaseExecutor.class.getDeclaredMethods()) {
            if (Modifier.isPublic(m.getModifiers()) && !Modifier.isStatic(m.getModifiers()) && !SYNC_ONLY.contains(m.getName())
                    && !asyncSignatures.contains(m.getName() + Arrays.toString(m.getParameterTypes()))) {
                missing.add(m.toString());
            }
        }

        assertTrue(missing.isEmpty(), "sync operations without an async twin: " + missing);
    }

    // Pins the class Javadoc: the default executor's maximum pool size never takes effect, because AsyncExecutor queues
    // tasks in an unbounded LinkedBlockingQueue (a ThreadPoolExecutor only grows past its core size when the queue is full).
    @Test
    void defaultAsyncExecutorRunsAtMostCoreSizeTasksAndQueuesTheRest() {
        final ThreadPoolExecutor pool = assertInstanceOf(ThreadPoolExecutor.class, HBaseExecutor.DEFAULT_ASYNC_EXECUTOR.getExecutor());

        assertEquals(Math.max(64, IOUtil.CPU_CORES * 8), pool.getCorePoolSize());
        assertInstanceOf(LinkedBlockingQueue.class, pool.getQueue());
        assertEquals(Integer.MAX_VALUE, pool.getQueue().remainingCapacity() + pool.getQueue().size());
    }
}
