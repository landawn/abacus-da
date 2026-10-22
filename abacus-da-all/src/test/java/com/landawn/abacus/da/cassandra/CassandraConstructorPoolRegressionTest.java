package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.datastax.oss.driver.api.core.CqlSession;
import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.pool.KeyedObjectPool;
import com.landawn.abacus.pool.PoolFactory;

public class CassandraConstructorPoolRegressionTest extends TestBase {

    @Test
    void failedModernConstructorDoesNotRetainPool() throws Exception {
        final CqlSession failedSession = (CqlSession) Proxy.newProxyInstance(CqlSession.class.getClassLoader(), new Class<?>[] { CqlSession.class },
                (proxy, method, args) -> {
                    throw new IllegalStateException("session context unavailable");
                });

        assertNoRetainedPools(CassandraExecutor.class, () -> new CassandraExecutor(null), () -> new CassandraExecutor(failedSession));
    }

    @Test
    @SuppressWarnings("deprecation")
    void failedLegacyConstructorDoesNotRetainPool() throws Exception {
        final com.datastax.driver.core.Session failedSession = (com.datastax.driver.core.Session) Proxy.newProxyInstance(
                com.datastax.driver.core.Session.class.getClassLoader(), new Class<?>[] { com.datastax.driver.core.Session.class }, (proxy, method, args) -> {
                    throw new IllegalStateException("session cluster unavailable");
                });

        assertNoRetainedPools(com.landawn.abacus.da.cassandra.v3.CassandraExecutor.class, () -> new com.landawn.abacus.da.cassandra.v3.CassandraExecutor(null),
                () -> new com.landawn.abacus.da.cassandra.v3.CassandraExecutor(failedSession));
    }

    // ---- 2026-10-19 sliceH ----

    @Test
    void closedStatementCacheIsTreatedAsMiss_sliceH() throws Exception {
        // The prepared-statement pool closes itself from its own JVM shutdown hook; CQL run by an application shutdown hook
        // then failed with "GenericKeyedObjectPool has been closed" although the session was still open.
        final CqlSession session = Mockito.mock(CqlSession.class);
        final com.datastax.oss.driver.api.core.context.DriverContext context = Mockito.mock(com.datastax.oss.driver.api.core.context.DriverContext.class);
        Mockito.when(session.getContext()).thenReturn(context);
        Mockito.when(context.getCodecRegistry()).thenReturn(com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry.DEFAULT);
        final com.datastax.oss.driver.api.core.cql.PreparedStatement prepared = Mockito.mock(com.datastax.oss.driver.api.core.cql.PreparedStatement.class);
        final com.datastax.oss.driver.api.core.cql.ColumnDefinitions noVariables = Mockito.mock(com.datastax.oss.driver.api.core.cql.ColumnDefinitions.class);
        Mockito.when(noVariables.size()).thenReturn(0);
        Mockito.when(prepared.getVariableDefinitions()).thenReturn(noVariables);
        Mockito.when(prepared.bind(Mockito.any(Object[].class))).thenReturn(Mockito.mock(com.datastax.oss.driver.api.core.cql.BoundStatement.class));
        Mockito.when(session.prepare(Mockito.anyString())).thenReturn(prepared);

        final CassandraExecutor executor = new CassandraExecutor(session);

        try {
            executor.execute("SELECT * FROM sliceh_cache");
            Mockito.verify(session, Mockito.times(1)).prepare(Mockito.anyString());
            executor.execute("SELECT * FROM sliceh_cache");
            Mockito.verify(session, Mockito.times(1)).prepare(Mockito.anyString()); // served from the cache

            final java.lang.reflect.Field poolField = CassandraExecutor.class.getDeclaredField("preparedStatementPool");
            poolField.setAccessible(true);
            ((KeyedObjectPool<?, ?>) poolField.get(executor)).close(); // what the pool's own shutdown hook does

            org.junit.jupiter.api.Assertions.assertAll(() -> executor.execute("SELECT * FROM sliceh_cache"),
                    () -> executor.execute("SELECT k FROM sliceh_cache", new Object[0]),
                    () -> Mockito.verify(session, Mockito.times(3)).prepare(Mockito.anyString()));
        } finally {
            executor.close();
        }
    }

    // ---- 2026-10-20 cassfix ----

    @Test
    @SuppressWarnings("unchecked")
    void onlyAClosedStatementCacheIsAMiss_cassfix() throws Exception {
        // getOrPrepareStatement swallowed EVERY IllegalStateException from the cache's get/put, whereas the driver 3 executor
        // rethrows unless the pool is closed. A failing (open) cache now propagates; a cache closed concurrently is still a miss.
        final CqlSession session = Mockito.mock(CqlSession.class);
        final com.datastax.oss.driver.api.core.context.DriverContext context = Mockito.mock(com.datastax.oss.driver.api.core.context.DriverContext.class);
        Mockito.when(session.getContext()).thenReturn(context);
        Mockito.when(context.getCodecRegistry()).thenReturn(com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry.DEFAULT);
        final com.datastax.oss.driver.api.core.cql.PreparedStatement prepared = Mockito.mock(com.datastax.oss.driver.api.core.cql.PreparedStatement.class);
        final com.datastax.oss.driver.api.core.cql.ColumnDefinitions noVariables = Mockito.mock(com.datastax.oss.driver.api.core.cql.ColumnDefinitions.class);
        Mockito.when(noVariables.size()).thenReturn(0);
        Mockito.when(prepared.getVariableDefinitions()).thenReturn(noVariables);
        Mockito.when(prepared.bind(Mockito.any(Object[].class))).thenReturn(Mockito.mock(com.datastax.oss.driver.api.core.cql.BoundStatement.class));
        Mockito.when(session.prepare(Mockito.anyString())).thenReturn(prepared);

        final CassandraExecutor executor = new CassandraExecutor(session);
        final java.lang.reflect.Field poolField = CassandraExecutor.class.getDeclaredField("preparedStatementPool");
        poolField.setAccessible(true);
        final KeyedObjectPool<String, com.landawn.abacus.pool.PoolableAdapter<com.datastax.oss.driver.api.core.cql.PreparedStatement>> realPool = (KeyedObjectPool<String, com.landawn.abacus.pool.PoolableAdapter<com.datastax.oss.driver.api.core.cql.PreparedStatement>>) poolField
                .get(executor);
        final KeyedObjectPool<String, com.landawn.abacus.pool.PoolableAdapter<com.datastax.oss.driver.api.core.cql.PreparedStatement>> failingPool = Mockito
                .mock(KeyedObjectPool.class);
        final IllegalStateException getFailure = new IllegalStateException("cache get failed while open");
        final IllegalStateException putFailure = new IllegalStateException("cache put failed while open");

        try {
            poolField.set(executor, failingPool);

            // 1. Open pool, get() throws: propagates, nothing is prepared.
            Mockito.when(failingPool.isClosed()).thenReturn(false);
            Mockito.when(failingPool.get(Mockito.anyString())).thenThrow(getFailure);
            org.junit.jupiter.api.Assertions.assertSame(getFailure,
                    assertThrows(IllegalStateException.class, () -> executor.execute("SELECT * FROM cassfix_cache")));
            Mockito.verify(session, Mockito.never()).prepare(Mockito.anyString());

            // 2. Open pool, get() misses and put() throws: the statement was prepared, the failure still propagates.
            Mockito.reset(failingPool);
            Mockito.when(failingPool.isClosed()).thenReturn(false);
            Mockito.when(failingPool.get(Mockito.anyString())).thenReturn(null);
            Mockito.when(failingPool.put(Mockito.anyString(), Mockito.any())).thenThrow(putFailure);
            org.junit.jupiter.api.Assertions.assertSame(putFailure,
                    assertThrows(IllegalStateException.class, () -> executor.execute("SELECT * FROM cassfix_cache")));
            Mockito.verify(session, Mockito.times(1)).prepare(Mockito.anyString());

            // 3. Pool closed between the isClosed() precheck and get()/put(): a miss, the statement runs uncached.
            Mockito.reset(failingPool);
            Mockito.when(failingPool.isClosed()).thenReturn(false, true, true, true, true, true);
            Mockito.when(failingPool.get(Mockito.anyString())).thenThrow(new IllegalStateException("GenericKeyedObjectPool has been closed"));
            Mockito.when(failingPool.put(Mockito.anyString(), Mockito.any())).thenThrow(new IllegalStateException("GenericKeyedObjectPool has been closed"));
            executor.execute("SELECT * FROM cassfix_cache");
            Mockito.verify(session, Mockito.times(2)).prepare(Mockito.anyString());

            // 4. Pool already closed: prepared without touching the cache at all.
            Mockito.reset(failingPool);
            Mockito.when(failingPool.isClosed()).thenReturn(true);
            executor.execute("SELECT k FROM cassfix_cache", new Object[0]);
            Mockito.verify(session, Mockito.times(3)).prepare(Mockito.anyString());
            Mockito.verify(failingPool, Mockito.never()).get(Mockito.anyString());
            Mockito.verify(failingPool, Mockito.never()).put(Mockito.anyString(), Mockito.any());
        } finally {
            poolField.set(executor, realPool);
            executor.close();
        }
    }

    // ---- 2026-10-20 rounify ----

    @Test
    @SuppressWarnings({ "unchecked", "deprecation" })
    void legacyOnlyAClosedStatementCacheIsAMiss_rounify() throws Exception {
        // Driver 3 twin of onlyAClosedStatementCacheIsAMiss_cassfix: getCachedStatement/cacheStatement rethrow an open cache's
        // IllegalStateException (the same instance) and treat only a closed cache as a miss. The driver 3 executor has no
        // isClosed() precheck, so "closed between precheck and get()/put()" is a get()/put() that throws while isClosed() is true.
        final com.datastax.driver.core.Session session = Mockito.mock(com.datastax.driver.core.Session.class);
        final com.datastax.driver.core.Cluster cluster = Mockito.mock(com.datastax.driver.core.Cluster.class);
        final com.datastax.driver.core.Configuration configuration = Mockito.mock(com.datastax.driver.core.Configuration.class);
        final com.datastax.driver.core.ProtocolOptions protocolOptions = Mockito.mock(com.datastax.driver.core.ProtocolOptions.class);
        Mockito.when(session.init()).thenReturn(session);
        Mockito.when(session.getCluster()).thenReturn(cluster);
        Mockito.when(cluster.getConfiguration()).thenReturn(configuration);
        Mockito.when(configuration.getCodecRegistry()).thenReturn(new com.datastax.driver.core.CodecRegistry());
        Mockito.when(configuration.getProtocolOptions()).thenReturn(protocolOptions);
        Mockito.when(protocolOptions.getProtocolVersion()).thenReturn(com.datastax.driver.core.ProtocolVersion.V4);
        final com.datastax.driver.core.PreparedStatement prepared = Mockito.mock(com.datastax.driver.core.PreparedStatement.class);
        final com.datastax.driver.core.ColumnDefinitions noVariables = Mockito.mock(com.datastax.driver.core.ColumnDefinitions.class);
        Mockito.when(noVariables.size()).thenReturn(0);
        Mockito.when(prepared.getVariables()).thenReturn(noVariables);
        Mockito.when(prepared.bind(Mockito.any(Object[].class)))
                .thenAnswer(invocation -> Mockito.mock(com.datastax.driver.core.BoundStatement.class, Mockito.RETURNS_SELF));
        Mockito.when(session.prepare(Mockito.anyString())).thenReturn(prepared);

        final com.landawn.abacus.da.cassandra.v3.CassandraExecutor executor = new com.landawn.abacus.da.cassandra.v3.CassandraExecutor(session);
        final java.lang.reflect.Field poolField = com.landawn.abacus.da.cassandra.v3.CassandraExecutor.class.getDeclaredField("preparedStatementPool");
        poolField.setAccessible(true);
        final KeyedObjectPool<String, com.landawn.abacus.pool.PoolableAdapter<com.datastax.driver.core.PreparedStatement>> realPool = (KeyedObjectPool<String, com.landawn.abacus.pool.PoolableAdapter<com.datastax.driver.core.PreparedStatement>>) poolField
                .get(executor);
        final KeyedObjectPool<String, com.landawn.abacus.pool.PoolableAdapter<com.datastax.driver.core.PreparedStatement>> failingPool = Mockito
                .mock(KeyedObjectPool.class);
        final IllegalStateException getFailure = new IllegalStateException("cache get failed while open");
        final IllegalStateException putFailure = new IllegalStateException("cache put failed while open");

        try {
            poolField.set(executor, failingPool);

            // 1. Open pool, get() throws: the same instance propagates, nothing is prepared.
            Mockito.when(failingPool.isClosed()).thenReturn(false);
            Mockito.when(failingPool.get(Mockito.anyString())).thenThrow(getFailure);
            org.junit.jupiter.api.Assertions.assertSame(getFailure,
                    assertThrows(IllegalStateException.class, () -> executor.execute("SELECT * FROM rounify_cache")));
            Mockito.verify(session, Mockito.never()).prepare(Mockito.anyString());

            // 2. Open pool, get() misses and put() throws: the statement was prepared, the failure still propagates.
            Mockito.reset(failingPool);
            Mockito.when(failingPool.isClosed()).thenReturn(false);
            Mockito.when(failingPool.get(Mockito.anyString())).thenReturn(null);
            Mockito.when(failingPool.put(Mockito.anyString(), Mockito.any())).thenThrow(putFailure);
            org.junit.jupiter.api.Assertions.assertSame(putFailure,
                    assertThrows(IllegalStateException.class, () -> executor.execute("SELECT * FROM rounify_cache")));
            Mockito.verify(session, Mockito.times(1)).prepare(Mockito.anyString());

            // 3. Pool closed by the time get()/put() run: a miss, the statement runs uncached.
            Mockito.reset(failingPool);
            Mockito.when(failingPool.isClosed()).thenReturn(true);
            Mockito.when(failingPool.get(Mockito.anyString())).thenThrow(new IllegalStateException("GenericKeyedObjectPool has been closed"));
            Mockito.when(failingPool.put(Mockito.anyString(), Mockito.any())).thenThrow(new IllegalStateException("GenericKeyedObjectPool has been closed"));
            executor.execute("SELECT * FROM rounify_cache");
            executor.execute("SELECT k FROM rounify_cache", new Object[0]);
            Mockito.verify(session, Mockito.times(3)).prepare(Mockito.anyString());
            Mockito.verify(failingPool, Mockito.times(2)).get(Mockito.anyString());
            Mockito.verify(failingPool, Mockito.times(2)).put(Mockito.anyString(), Mockito.any());
        } finally {
            poolField.set(executor, realPool);
            executor.close();
        }
    }

    private static void assertNoRetainedPools(final Class<?> executorClass, final Runnable... constructorAttempts) throws Exception {
        // Initialize global caches before observing allocations owned by these constructor attempts.
        Class.forName(executorClass.getName(), true, executorClass.getClassLoader());
        PoolFactory.createKeyedObjectPool(1, 0).close();
        final List<KeyedObjectPool<?, ?>> created = new ArrayList<>();

        try (MockedStatic<PoolFactory> factory = Mockito.mockStatic(PoolFactory.class, invocation -> {
            final Object result = Mockito.CALLS_REAL_METHODS.answer(invocation);
            if (invocation.getMethod().getName().equals("createKeyedObjectPool") && invocation.getArguments().length == 2) {
                created.add((KeyedObjectPool<?, ?>) result);
            }
            return result;
        })) {
            for (final Runnable attempt : constructorAttempts) {
                assertThrows(RuntimeException.class, attempt::run);
            }
            assertEquals(0L, created.stream().filter(pool -> !pool.isClosed()).count(),
                    "Failed constructors must not leave pools retained by eviction tasks and shutdown hooks");
        } finally {
            // Even a red run closes every observed pool, cancelling eviction and removing its shutdown hook.
            for (final KeyedObjectPool<?, ?> pool : created) {
                pool.close();
            }
        }
    }
}
