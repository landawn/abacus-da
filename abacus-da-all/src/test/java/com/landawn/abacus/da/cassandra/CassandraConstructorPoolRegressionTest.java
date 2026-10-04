package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.datastax.oss.driver.api.core.CqlSession;
import com.landawn.abacus.pool.KeyedObjectPool;
import com.landawn.abacus.pool.PoolFactory;

@Tag("base-test")
class CassandraConstructorPoolRegressionTest {

    @Test
    void failedModernConstructorDoesNotRetainPool() throws Exception {
        final CqlSession failedSession = (CqlSession) Proxy.newProxyInstance(CqlSession.class.getClassLoader(), new Class<?>[] { CqlSession.class },
                (proxy, method, args) -> { throw new IllegalStateException("session context unavailable"); });

        assertNoRetainedPools(CassandraExecutor.class, () -> new CassandraExecutor(null), () -> new CassandraExecutor(failedSession));
    }

    @Test
    @SuppressWarnings("deprecation")
    void failedLegacyConstructorDoesNotRetainPool() throws Exception {
        final com.datastax.driver.core.Session failedSession = (com.datastax.driver.core.Session) Proxy.newProxyInstance(
                com.datastax.driver.core.Session.class.getClassLoader(), new Class<?>[] { com.datastax.driver.core.Session.class },
                (proxy, method, args) -> { throw new IllegalStateException("session cluster unavailable"); });

        assertNoRetainedPools(com.landawn.abacus.da.cassandra.v3.CassandraExecutor.class,
                () -> new com.landawn.abacus.da.cassandra.v3.CassandraExecutor(null),
                () -> new com.landawn.abacus.da.cassandra.v3.CassandraExecutor(failedSession));
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
