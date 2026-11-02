package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.da.MockitoTestBase;
import com.landawn.abacus.da.cassandra.CassandraExecutorBaseTestSupport.SliceNKeylessEntity;
import com.landawn.abacus.da.cassandra.CassandraExecutorBaseTestSupport.TestCassandraExecutor;
import com.landawn.abacus.util.ClassUtil;

/**
 * Line coverage for {@link CassandraExecutorBase} branches that the October 2026 suites left unexecuted:
 * the {@code putIfAbsent} loser, keyless and blank-key conditions, a non-String map-entity key, and the
 * batch-map update guards. Naming-policy {@code default} arms stay untested: the constructor rejects every
 * policy other than the three named ones, and the field is final.
 */
class CassandraExecutorBaseLineCoverageTest extends MockitoTestBase {

    @Test
    void getKeyNamesReturnsTheListPublishedByTheThreadThatWonTheCache() throws Exception {
        final List<String> names = loadKeyNamesConcurrently(RaceKeyNamesEntity.class);

        assertEquals(List.of("id"), names);
    }

    @Test
    void getKeyNameSetReturnsTheSetPublishedByTheThreadThatWonTheCache() throws Exception {
        CassandraExecutorBase.entityKeyNamesMap.remove(RaceKeyNameSetEntity.class);
        final int threads = 8;
        final CyclicBarrier start = new CyclicBarrier(threads);
        final ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            final List<Future<Set<String>>> futures = new ArrayList<>(threads);

            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await(10, TimeUnit.SECONDS);
                    return CassandraExecutorBase.getKeyNameSet(RaceKeyNameSetEntity.class);
                }));
            }

            final Set<String> winner = futures.get(0).get(30, TimeUnit.SECONDS);

            for (final Future<Set<String>> future : futures) {
                assertSame(winner, future.get(30, TimeUnit.SECONDS));
            }

            assertEquals(Set.of("id"), winner);
        } finally {
            pool.shutdownNow();
            CassandraExecutorBase.entityKeyNamesMap.remove(RaceKeyNameSetEntity.class);
        }
    }

    @Test
    void entityToConditionRejectsABeanThatDeclaresNoKey() {
        final SliceNKeylessEntity entity = new SliceNKeylessEntity();
        entity.setName("ada");

        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> TestCassandraExecutor.exposedEntityToCondition(entity));

        assertEquals("No key names defined for entity class: SliceNKeylessEntity", exception.getMessage());
    }

    @Test
    void entityToConditionRejectsACollectionOfABeanThatDeclaresNoKey() {
        final SliceNKeylessEntity entity = new SliceNKeylessEntity();
        entity.setName("ada");

        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> TestCassandraExecutor.exposedEntityToConditionCollection(SliceNKeylessEntity.class, List.of(entity)));

        assertEquals("No key names defined for entity class: SliceNKeylessEntity", exception.getMessage());
    }

    @Test
    void entityToConditionRejectsABlankOrNullSingleKeyInsideACollection() {
        final BlankCodeEntity blank = new BlankCodeEntity();
        blank.setCode("");
        final BlankCodeEntity missing = new BlankCodeEntity();

        final IllegalArgumentException blankKey = assertThrows(IllegalArgumentException.class,
                () -> TestCassandraExecutor.exposedEntityToConditionCollection(BlankCodeEntity.class, List.of(blank)));
        final IllegalArgumentException nullKey = assertThrows(IllegalArgumentException.class,
                () -> TestCassandraExecutor.exposedEntityToConditionCollection(BlankCodeEntity.class, List.of(missing)));

        assertEquals("No property value specified in entity for key name: code", blankKey.getMessage());
        assertEquals("No property value specified in entity for key name: code", nullKey.getMessage());
    }

    @Test
    void defaultUpdatePropNamesOfRejectsAMapEntityWhoseKeyIsNotAPropertyName() {
        final IntegerKeyedMapEntity entity = new IntegerKeyedMapEntity();
        entity.put(7, "x");

        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> CassandraExecutorBase.defaultUpdatePropNamesOf(entity));

        assertEquals(
                "Map entity keys must be property names, but found: 7 in Map entity of class: " + ClassUtil.getCanonicalClassName(IntegerKeyedMapEntity.class),
                exception.getMessage());
    }

    @Test
    void prepareBatchMapUpdateRejectsAClassThatDeclaresNoKey() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> new TestCassandraExecutor().prepareBatchMapUpdateForTest(SliceNKeylessEntity.class, Map.of("name", "ada")));

        assertEquals("No key names defined for entity class: SliceNKeylessEntity", exception.getMessage());
    }

    @Test
    void prepareBatchMapUpdateRejectsABlankCharSequenceKey() {
        final Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", "");
        row.put("name", "ada");

        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> new TestCassandraExecutor().prepareBatchMapUpdateForTest(BlankIdEntity.class, row));

        assertEquals("No property value specified for key name: id", exception.getMessage());
    }

    private static List<String> loadKeyNamesConcurrently(final Class<?> entityClass) throws Exception {
        CassandraExecutorBase.entityKeyNamesMap.remove(entityClass);
        final int threads = 8;
        final CyclicBarrier start = new CyclicBarrier(threads);
        final ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            final List<Future<List<String>>> futures = new ArrayList<>(threads);

            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await(10, TimeUnit.SECONDS);
                    return CassandraExecutorBase.getKeyNames(entityClass);
                }));
            }

            final List<String> winner = futures.get(0).get(30, TimeUnit.SECONDS);

            for (final Future<List<String>> future : futures) {
                assertSame(winner, future.get(30, TimeUnit.SECONDS));
            }

            return winner;
        } finally {
            pool.shutdownNow();
            CassandraExecutorBase.entityKeyNamesMap.remove(entityClass);
        }
    }

    /** Cold bean: the first {@code idPropNames} call is the window in which the other threads lose {@code putIfAbsent}. */
    public static class RaceKeyNamesEntity {
        @Id
        private Long id;

        public Long getId() {
            return id;
        }

        public void setId(final Long id) {
            this.id = id;
        }
    }

    public static class RaceKeyNameSetEntity {
        @Id
        private Long id;

        public Long getId() {
            return id;
        }

        public void setId(final Long id) {
            this.id = id;
        }
    }

    public static class BlankCodeEntity {
        @Id
        private String code;

        public String getCode() {
            return code;
        }

        public void setCode(final String code) {
            this.code = code;
        }
    }

    public static class BlankIdEntity {
        @Id
        private String id;

        private String name;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    @com.landawn.abacus.annotation.Entity
    public static class IntegerKeyedMapEntity extends LinkedHashMap<Integer, Object> {
        private static final long serialVersionUID = 1L;

        public Long getId() {
            return (Long) get(0);
        }

        public void setId(final Long id) {
            put(0, id);
        }
    }
}
