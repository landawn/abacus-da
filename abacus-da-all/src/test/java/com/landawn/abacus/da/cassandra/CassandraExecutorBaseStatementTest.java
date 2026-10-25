package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.annotation.Table;
import com.landawn.abacus.query.AbstractQueryBuilder.SP;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.stream.Stream;

public class CassandraExecutorBaseStatementTest extends CassandraExecutorBaseTestSupport {

    @ParameterizedTest(name = "{0}")
    @MethodSource("namingPolicies")
    void preparesAllPropertyQueryForNamingPolicy(NamingCase names) {
        TestCassandraExecutor current = new TestCassandraExecutor(names.policy());
        SP statement = current.exposedPrepareQuery(NamingEntity.class, null, Filters.eq("accountId", 1L), 0);

        assertEquals("SELECT " + names.projection() + " FROM people WHERE " + names.keyColumn() + " = :accountId", statement.query());
        assertEquals(List.of(1L), statement.parameters());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("namingPolicies")
    void preparesColumnDeleteForNamingPolicy(NamingCase names) {
        TestCassandraExecutor current = new TestCassandraExecutor(names.policy());
        SP statement = current.exposedPrepareDelete(NamingEntity.class, List.of("displayName"), Filters.eq("accountId", 1L));

        assertEquals("DELETE " + names.valueColumn() + " FROM people WHERE " + names.keyColumn() + " = :accountId", statement.query());
        assertEquals(List.of(1L), statement.parameters());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("namingPolicies")
    void preparesEntityInsertForNamingPolicy(NamingCase names) {
        TestCassandraExecutor current = new TestCassandraExecutor(names.policy());
        SP statement = current.exposedPrepareInsert(namingEntity());

        assertEquals("INSERT INTO people (" + names.keyColumn() + ", " + names.valueColumn() + ") VALUES (:accountId, :displayName)", statement.query());
        assertEquals(List.of(1L, "Ada"), statement.parameters());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("namingPolicies")
    void preparesEntityUpdateForNamingPolicy(NamingCase names) {
        TestCassandraExecutor current = new TestCassandraExecutor(names.policy());
        SP statement = current.exposedPrepareUpdate(namingEntity(), List.of("displayName"));

        assertEquals("UPDATE people SET " + names.valueColumn() + " = :displayName WHERE " + names.keyColumn() + " = :accountId", statement.query());
        assertEquals(List.of("Ada", 1L), statement.parameters());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("namingPolicies")
    void preparesMapInsertForNamingPolicy(NamingCase names) {
        TestCassandraExecutor current = new TestCassandraExecutor(names.policy());
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("accountId", 1L);
        properties.put("displayName", "Ada");
        SP statement = current.exposedPrepareInsert(NamingEntity.class, properties);

        assertEquals("INSERT INTO people (" + names.keyColumn() + ", " + names.valueColumn() + ") VALUES (:accountId, :displayName)", statement.query());
        assertEquals(List.of(1L, "Ada"), statement.parameters());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("namingPolicies")
    void preparesMapUpdateForNamingPolicy(NamingCase names) {
        TestCassandraExecutor current = new TestCassandraExecutor(names.policy());
        SP statement = current.exposedPrepareUpdate(NamingEntity.class, Map.of("displayName", "Ada"), Filters.eq("accountId", 1L));

        assertEquals("UPDATE people SET " + names.valueColumn() + " = :displayName WHERE " + names.keyColumn() + " = :accountId", statement.query());
        assertEquals(List.of("Ada", 1L), statement.parameters());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("namingPolicies")
    void preparesRowDeleteForNamingPolicy(NamingCase names) {
        TestCassandraExecutor current = new TestCassandraExecutor(names.policy());
        SP statement = current.exposedPrepareDelete(NamingEntity.class, null, Filters.eq("accountId", 1L));

        assertEquals("DELETE FROM people WHERE " + names.keyColumn() + " = :accountId", statement.query());
        assertEquals(List.of(1L), statement.parameters());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("namingPolicies")
    void preparesSelectedPropertyQueryAndLimitForNamingPolicy(NamingCase names) {
        TestCassandraExecutor current = new TestCassandraExecutor(names.policy());
        SP statement = current.exposedPrepareQuery(NamingEntity.class, List.of("accountId", "displayName"), Filters.eq("accountId", 1L), 5);

        assertEquals("SELECT " + names.projection() + " FROM people WHERE " + names.keyColumn() + " = :accountId LIMIT 5", statement.query());
        assertEquals(List.of(1L), statement.parameters());
    }

    @Test
    public void test_failedDeleteConditionReleasesInternalBuilder() throws Exception {
        for (final NamingPolicy policy : List.of(NamingPolicy.SNAKE_CASE, NamingPolicy.SCREAMING_SNAKE_CASE, NamingPolicy.CAMEL_CASE)) {
            final TestCassandraExecutor current = new TestCassandraExecutor(policy);
            for (final Collection<String> properties : Arrays.asList(null, List.of("name"))) {
                assertRejectedWithoutBuilderLeak(IllegalArgumentException.class,
                        () -> current.exposedPrepareDelete(TestEntity.class, properties, Filters.or(Filters.eq("id", 1), Filters.eq("id", 2))));
            }
        }
    }

    @Test
    public void test_failedInsertReleasesInternalBuilder() throws Exception {
        for (final NamingPolicy policy : List.of(NamingPolicy.SNAKE_CASE, NamingPolicy.SCREAMING_SNAKE_CASE, NamingPolicy.CAMEL_CASE)) {
            final TestCassandraExecutor current = new TestCassandraExecutor(policy);
            assertRejectedWithoutBuilderLeak(IllegalArgumentException.class, () -> current.exposedPrepareInsert(new TestEntity()));
        }
    }

    @Test
    public void test_failedQueryReleasesInternalBuilder() throws Exception {
        for (final NamingPolicy policy : List.of(NamingPolicy.SNAKE_CASE, NamingPolicy.SCREAMING_SNAKE_CASE, NamingPolicy.CAMEL_CASE)) {
            final TestCassandraExecutor current = new TestCassandraExecutor(policy);
            for (final Collection<String> properties : Arrays.asList(null, List.of("name"))) {
                assertRejectedWithoutBuilderLeak(IllegalArgumentException.class,
                        () -> current.exposedPrepareQuery(TestEntity.class, properties, Filters.or(Filters.eq("id", 1), Filters.eq("id", 2)), 1));
            }
        }
    }

    @Test
    public void test_failedTableMappingReleasesInternalBuilder() throws Exception {
        for (final NamingPolicy policy : List.of(NamingPolicy.SNAKE_CASE, NamingPolicy.SCREAMING_SNAKE_CASE, NamingPolicy.CAMEL_CASE)) {
            final TestCassandraExecutor current = new TestCassandraExecutor(policy);
            assertRejectedWithoutBuilderLeak(IllegalArgumentException.class, () -> current.exposedPrepareInsert(BadTableEntity.class, Map.of("name", "value")));
            assertRejectedWithoutBuilderLeak(IllegalArgumentException.class, () -> current.exposedPrepareQuery(BadTableEntity.class, List.of("name"), null, 0));
            assertRejectedWithoutBuilderLeak(IllegalArgumentException.class,
                    () -> current.exposedPrepareDelete(BadTableEntity.class, List.of("name"), Filters.eq("id", 1)));
            // The update and whole-row delete factories validate the derived table name up front (CqlBuilder
            // update(Class)/deleteFrom(Class) checkCqlTableReference), so these fail at factory time too.
            assertRejectedWithoutBuilderLeak(IllegalArgumentException.class,
                    () -> current.exposedPrepareUpdate(BadTableEntity.class, Map.of("name", "value"), Filters.eq("id", 1)));
            assertRejectedWithoutBuilderLeak(IllegalArgumentException.class,
                    () -> current.exposedPrepareDelete(BadTableEntity.class, null, Filters.eq("id", 1)));
        }
    }

    @Test
    public void test_failedUpdateConditionReleasesInternalBuilder() throws Exception {
        for (final NamingPolicy policy : List.of(NamingPolicy.SNAKE_CASE, NamingPolicy.SCREAMING_SNAKE_CASE, NamingPolicy.CAMEL_CASE)) {
            final TestCassandraExecutor current = new TestCassandraExecutor(policy);
            assertRejectedWithoutBuilderLeak(IllegalArgumentException.class,
                    () -> current.exposedPrepareUpdate(TestEntity.class, Map.of("name", "value"), Filters.or(Filters.eq("id", 1), Filters.eq("id", 2))));
        }
    }

    @Test
    public void test_failedUpdateGetterReleasesInternalBuilder() throws Exception {
        final ThrowingUpdateEntity entity = new ThrowingUpdateEntity();
        entity.setId(1L);
        for (final NamingPolicy policy : List.of(NamingPolicy.SNAKE_CASE, NamingPolicy.SCREAMING_SNAKE_CASE, NamingPolicy.CAMEL_CASE)) {
            final TestCassandraExecutor current = new TestCassandraExecutor(policy);
            assertRejectedWithoutBuilderLeak(RuntimeException.class, () -> current.exposedPrepareUpdate(entity, List.of("name")));
        }
    }

    @Test
    public void testParseCql_passThrough() {
        // parseCql() through the executor; defaults (no cqlMapper) -> ParsedCql.parse
        com.landawn.abacus.da.cassandra.ParsedCql parsed = executor.exposedParseCql("SELECT * FROM t WHERE id = ?");
        assertNotNull(parsed);
        assertEquals(1, parsed.parameterCount());
    }

    @Test
    public void testParseCql_withMapper_fallbackToParse() {
        CqlMapper mapper = new CqlMapper();
        TestCassandraExecutor mapExec = new TestCassandraExecutor(mapper, NamingPolicy.SNAKE_CASE);
        ParsedCql parsed = mapExec.exposedParseCql("SELECT * FROM t WHERE id = ?");
        assertNotNull(parsed);
        assertEquals(1, parsed.parameterCount());
    }

    @Test
    public void testParseCql_withMapper_passesThroughMapper() {
        CqlMapper mapper = new CqlMapper();
        mapper.add("findById", "SELECT * FROM t WHERE id = ?", new HashMap<>());
        TestCassandraExecutor mapExec = new TestCassandraExecutor(mapper, NamingPolicy.SNAKE_CASE);
        ParsedCql parsed = mapExec.exposedParseCql("findById");
        assertNotNull(parsed);
        assertEquals(1, parsed.parameterCount());
    }

    @Test
    public void testPrepareBatchMapUpdateValidatesPrimaryKeys() {
        final Map<String, Object> valid = new HashMap<>();
        valid.put("id", 7L);
        valid.put("name", "updated");
        final SP statement = executor.prepareBatchMapUpdateForTest(TestEntity.class, valid);
        assertTrue(statement.query().contains("WHERE id = :id"), statement.query());

        assertThrows(IllegalArgumentException.class, () -> executor.prepareBatchMapUpdateForTest(TestEntity.class, Map.of("name", "updated")));
        assertThrows(IllegalArgumentException.class, () -> executor.prepareBatchMapUpdateForTest(TestEntity.class, Map.of("id", 7L)));
    }

    @Test
    public void testPrepareDelete_nullCondition() {
        assertThrows(IllegalArgumentException.class, () -> executor.exposedPrepareDelete(TestEntity.class, null, null));
    }

    @Test
    public void testPrepareDelete_rejectsPrimaryKeyProperty() {
        assertThrows(IllegalArgumentException.class, () -> executor.exposedPrepareDelete(TestEntity.class, Arrays.asList("id"), Filters.eq("id", 1L)));
    }

    @Test
    public void testPrepareQuery_nullCondition_noLimit() {
        SP sp = executor.exposedPrepareQuery(TestEntity.class, null, null, 0);
        assertNotNull(sp);
        assertTrue(sp.query().toUpperCase().contains("SELECT"));
    }

    @Test
    public void testPrepareQuery_threeArgsOverload() {
        SP sp = executor.exposedPrepareQueryThreeArgs(TestEntity.class, null, Filters.eq("id", 1L));
        assertNotNull(sp);
        assertTrue(sp.query().toUpperCase().contains("SELECT"));
    }

    @Test
    public void testPrepareUpdate_classMapNullCondition() {
        Map<String, Object> props = new HashMap<>();
        props.put("name", "x");

        assertThrows(IllegalArgumentException.class, () -> executor.exposedPrepareUpdate(TestEntity.class, props, null));
    }

    @Test
    public void testPrepareUpdate_classMapRejectsPrimaryKeyProperty() {
        assertThrows(IllegalArgumentException.class, () -> executor.exposedPrepareUpdate(TestEntity.class, Map.of("id", 2L), Filters.eq("id", 1L)));
    }

    @Test
    public void testPrepareUpdate_entityMissingKey_throwsIAE() {
        TestEntity e = new TestEntity();
        e.setName("u1");

        assertThrows(IllegalArgumentException.class, () -> executor.exposedPrepareUpdate(e, Arrays.asList("name")));
    }

    @Test
    public void testPrepareUpdate_entityRejectsPrimaryKeyProperty() {
        TestEntity e = new TestEntity();
        e.setId(1L);

        assertThrows(IllegalArgumentException.class, () -> executor.exposedPrepareUpdate(e, Arrays.asList("id")));
    }

    // by insert() before calling into()") for an all-null bean instead.
    @Test
    public void testSliceN_insert_entityWithNoInsertableProperties_throwsIAE() {
        assertThrows(IllegalArgumentException.class, () -> executor.insert(new TestEntity()));
        assertThrows(IllegalArgumentException.class, () -> executor.exposedPrepareInsert(new TestEntity()));
    }

    // ---- 2026-10-24 sliceF ----

    // The query builder's insert(entity) dropped the key columns when every key value was 0 (SQL's "generated id" convention);
    // Cassandra has no generated keys and rejects an INSERT without a primary-key column, so key 0 could not be inserted.
    @Test
    public void test_sliceF_insertEntityKeepsZeroValuedKeyColumns() {
        final SliceFZeroKeyEntity single = new SliceFZeroKeyEntity();
        single.setName("zero");

        final SliceFZeroCompositeKeyEntity composite = new SliceFZeroCompositeKeyEntity();
        composite.setValue("v");

        final SliceFZeroCompositeKeyEntity partlyZero = new SliceFZeroCompositeKeyEntity();
        partlyZero.setClusteringKey(5L);
        partlyZero.setValue("v");

        final SliceFNullableKeyEntity nullKey = new SliceFNullableKeyEntity();
        nullKey.setName("n");

        final SliceFNullableKeyEntity conventionalZeroId = new SliceFNullableKeyEntity();
        conventionalZeroId.setId(0L);
        conventionalZeroId.setName("n");

        final TestCassandraExecutor snake = new TestCassandraExecutor(NamingPolicy.SNAKE_CASE);
        final TestCassandraExecutor screaming = new TestCassandraExecutor(NamingPolicy.SCREAMING_SNAKE_CASE);
        final TestCassandraExecutor camel = new TestCassandraExecutor(NamingPolicy.CAMEL_CASE);

        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertInsert("INSERT INTO slicef_zero_keys (bucket, name) VALUES (:bucket, :name)", List.of(0, "zero"),
                        snake.exposedPrepareInsert(single)),
                () -> assertInsert("INSERT INTO slicef_zero_keys (BUCKET, NAME) VALUES (:bucket, :name)", List.of(0, "zero"),
                        screaming.exposedPrepareInsert(single)),
                () -> assertInsert("INSERT INTO slicef_zero_keys (bucket, name) VALUES (:bucket, :name)", List.of(0, "zero"),
                        camel.exposedPrepareInsert(single)),
                () -> assertInsert("INSERT INTO slicef_zero_pairs (partition_key, clustering_key, value) VALUES (:partitionKey, :clusteringKey, :value)",
                        List.of(0, 0L, "v"), snake.exposedPrepareInsert(composite)),
                () -> assertInsert("INSERT INTO slicef_zero_pairs (partition_key, clustering_key, value) VALUES (:partitionKey, :clusteringKey, :value)",
                        List.of(0, 5L, "v"), snake.exposedPrepareInsert(partlyZero)),
                // A property named "id" is the key by convention (no @Id): its 0 is kept too, while a null key is still left out.
                () -> assertInsert("INSERT INTO slicef_nullable_keys (id, name) VALUES (:id, :name)", List.of(0L, "n"),
                        snake.exposedPrepareInsert(conventionalZeroId)),
                () -> assertInsert("INSERT INTO slicef_nullable_keys (name) VALUES (:name)", List.of("n"), snake.exposedPrepareInsert(nullKey)));
    }

    @Test
    public void test_sliceF_insertAndAsyncInsertSendZeroValuedKey() {
        final SliceFZeroKeyEntity entity = new SliceFZeroKeyEntity();
        entity.setName("zero");

        final RecordingExecutor recording = new RecordingExecutor();
        recording.insert(entity);
        recording.recordingAsync().insert(entity);

        assertEquals(List.of("INSERT INTO slicef_zero_keys (bucket, name) VALUES (:bucket, :name) [0, zero]",
                "INSERT INTO slicef_zero_keys (bucket, name) VALUES (:bucket, :name) [0, zero]"), recording.recorded);
    }

    @Test
    public void test_sliceF_insertEntityStillRejectsNonBeanAndValuelessEntity() {
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("bucket", 1);

        org.junit.jupiter.api.Assertions.assertAll(() -> assertThrows(IllegalArgumentException.class, () -> executor.exposedPrepareInsert(map)),
                () -> assertThrows(IllegalArgumentException.class, () -> executor.exposedPrepareInsert("bucket")),
                () -> assertTrue(assertThrows(IllegalArgumentException.class, () -> executor.exposedPrepareInsert(new SliceFNullableKeyEntity())).getMessage()
                        .startsWith("No insertable property value specified in entity of class: ")));
    }

    private static void assertInsert(final String expectedQuery, final List<Object> expectedParameters, final SP statement) {
        assertEquals(expectedQuery, statement.query());
        assertEquals(expectedParameters, statement.parameters());
    }

    @Table("slicef_zero_keys")
    public static class SliceFZeroKeyEntity {
        @Id
        private int bucket;
        private String name;

        public int getBucket() {
            return bucket;
        }

        public void setBucket(final int bucket) {
            this.bucket = bucket;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    @Table("slicef_zero_pairs")
    public static class SliceFZeroCompositeKeyEntity {
        @Id
        private int partitionKey;
        @Id
        private long clusteringKey;
        private String value;

        public int getPartitionKey() {
            return partitionKey;
        }

        public void setPartitionKey(final int partitionKey) {
            this.partitionKey = partitionKey;
        }

        public long getClusteringKey() {
            return clusteringKey;
        }

        public void setClusteringKey(final long clusteringKey) {
            this.clusteringKey = clusteringKey;
        }

        public String getValue() {
            return value;
        }

        public void setValue(final String value) {
            this.value = value;
        }
    }

    @Table("slicef_nullable_keys")
    public static class SliceFNullableKeyEntity {
        private Long id;
        private String name;

        public Long getId() {
            return id;
        }

        public void setId(final Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    // ---- 2026-10-24 verify2 ----

    // Beans.isBeanClass accepts an @Entity-annotated Map subclass. The sliceF bean path read its getters instead of its entries
    // (INSERT INTO verify2_map_rows (empty) VALUES (:empty)); its entries are the columns, as the builder's insert(entity) writes them.
    @Test
    public void test_verify2_insertEntityMapSubclassWritesItsEntries() {
        final Verify2MapEntity entity = new Verify2MapEntity();
        entity.put("bucket", 0);
        entity.put("name", "x");

        final TestCassandraExecutor snake = new TestCassandraExecutor(NamingPolicy.SNAKE_CASE);
        final TestCassandraExecutor screaming = new TestCassandraExecutor(NamingPolicy.SCREAMING_SNAKE_CASE);
        final TestCassandraExecutor camel = new TestCassandraExecutor(NamingPolicy.CAMEL_CASE);
        final RecordingExecutor recording = new RecordingExecutor();
        recording.insert(entity);
        recording.recordingAsync().insert(entity);

        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertInsert("INSERT INTO verify2_map_rows (bucket, name) VALUES (:bucket, :name)", List.of(0, "x"), snake.exposedPrepareInsert(entity)),
                () -> assertInsert("INSERT INTO verify2_map_rows (BUCKET, NAME) VALUES (:bucket, :name)", List.of(0, "x"),
                        screaming.exposedPrepareInsert(entity)),
                () -> assertInsert("INSERT INTO verify2_map_rows (bucket, name) VALUES (:bucket, :name)", List.of(0, "x"), camel.exposedPrepareInsert(entity)),
                () -> assertEquals(List.of("INSERT INTO verify2_map_rows (bucket, name) VALUES (:bucket, :name) [0, x]",
                        "INSERT INTO verify2_map_rows (bucket, name) VALUES (:bucket, :name) [0, x]"), recording.recorded),
                // An empty one is still rejected, with the builder's message.
                () -> assertEquals("entity map",
                        assertThrows(IllegalArgumentException.class, () -> snake.exposedPrepareInsert(new Verify2MapEntity())).getMessage()));
    }

    @com.landawn.abacus.annotation.Entity
    @Table("verify2_map_rows")
    public static class Verify2MapEntity extends LinkedHashMap<String, Object> {
        private static final long serialVersionUID = 1L;
    }

    // ---- 2026-10-24 r2 sliceF ----

    // Beans.isBeanClass accepts an @Entity-annotated Map subclass. With a key accessor (here the conventional Long "id"),
    // update(entity) and batchUpdate(entities, type) set the Map's own getters (UPDATE r2f_map_rows SET empty = :empty WHERE
    // id = :id) and lost the entries, and update(entity, names) rejected an entry name ("not found in bean class"). As for insert,
    // a Map entity's entries are its columns.
    @Test
    public void test_r2sliceF_updateMapEntitySetsItsEntries() {
        final R2FKeyedMapEntity entity = new R2FKeyedMapEntity();
        entity.setId(7L);
        entity.put("name", "x");
        entity.put("displayName", "Ada");

        final RecordingExecutor recording = new RecordingExecutor() {
            @Override
            protected TestStatement prepareBatchUpdateStatement(final Collection<?> entities, final Collection<String> propNamesToUpdate,
                    final TestBatchType type) {
                for (final Object e : entities) {
                    final SP cp = prepareUpdate(e, propNamesToUpdate);
                    recorded.add("batch " + cp.query() + " " + cp.parameters());
                }

                return new TestStatement();
            }
        };

        recording.update(entity);
        recording.update(entity, List.of("displayName"));
        recording.recordingAsync().update(entity, List.of("name"));
        recording.batchUpdate(List.of(entity), TestBatchType.LOGGED);
        recording.batchUpdate(List.of(entity), List.of("name"), TestBatchType.LOGGED);

        final R2FKeyedMapEntity keyOnly = new R2FKeyedMapEntity();
        keyOnly.setId(1L);

        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertEquals(List.of("UPDATE r2f_map_rows SET name = :name, display_name = :displayName WHERE id = :id [x, Ada, 7]",
                        "UPDATE r2f_map_rows SET display_name = :displayName WHERE id = :id [Ada, 7]",
                        "UPDATE r2f_map_rows SET name = :name WHERE id = :id [x, 7]",
                        "batch UPDATE r2f_map_rows SET name = :name, display_name = :displayName WHERE id = :id [x, Ada, 7]",
                        "batch UPDATE r2f_map_rows SET name = :name WHERE id = :id [x, 7]"), recording.recorded),
                () -> assertEquals("UPDATE r2f_map_rows SET NAME = :name, DISPLAY_NAME = :displayName WHERE ID = :id",
                        new TestCassandraExecutor(NamingPolicy.SCREAMING_SNAKE_CASE).exposedPrepareUpdate(entity, List.of("name", "displayName")).query()),
                // A listed name without an entry is rejected (as an unknown bean property is), not bound as null.
                () -> assertEquals("No entry for property: missing in Map entity of class: " + R2FKeyedMapEntity.class.getCanonicalName(),
                        assertThrows(IllegalArgumentException.class, () -> recording.update(entity, List.of("missing"))).getMessage()),
                () -> assertThrows(IllegalArgumentException.class, () -> recording.update(entity, List.of("id"))),
                () -> assertEquals("No non-key entry to update in Map entity of class: " + R2FKeyedMapEntity.class.getCanonicalName(),
                        assertThrows(IllegalArgumentException.class, () -> recording.update(keyOnly)).getMessage()));
    }

    // Pins the update(String, Object...) overload note: with only the query, or the query and one Collection<String>, Java binds the
    // call to update(Object) / update(Object, Collection), which reject the String as a non-bean entity before any CQL runs.
    @Test
    public void test_r2sliceF_updateQueryOverloadResolution() {
        final RecordingExecutor recording = new RecordingExecutor();
        final String query = "UPDATE people SET display_name = ? WHERE account_id = ?";

        org.junit.jupiter.api.Assertions.assertAll(() -> assertTrue(
                assertThrows(IllegalArgumentException.class, () -> recording.update("UPDATE people SET display_name = 'x' WHERE account_id = 1")).getMessage()
                        .startsWith("Not a bean class: java.lang.String")),
                () -> assertTrue(assertThrows(IllegalArgumentException.class, () -> recording.update(query, Arrays.asList("x", "1"))).getMessage()
                        .startsWith("Not a bean class: java.lang.String")),
                () -> assertEquals(List.of(), recording.recorded));

        recording.update(query, new Object[] { "x", 1L });
        recording.update(query, "x", 1L);

        assertEquals(List.of(query + " [x, 1]", query + " [x, 1]"), recording.recorded);
    }

    // Pins the update docs: a null property value is bound as null (it clears the column), while insert leaves it out.
    @Test
    public void test_r2sliceF_updateBindsNullPropertyValues() {
        final NamingEntity entity = new NamingEntity();
        entity.setAccountId(1L);

        final RecordingExecutor recording = new RecordingExecutor();
        recording.update(entity);
        recording.update(entity, List.of("displayName"));
        recording.insert(entity);

        assertEquals(Arrays.asList("UPDATE people SET display_name = :displayName WHERE account_id = :accountId [null, 1]",
                "UPDATE people SET display_name = :displayName WHERE account_id = :accountId [null, 1]",
                "INSERT INTO people (account_id) VALUES (:accountId) [1]"), recording.recorded);
    }

    @com.landawn.abacus.annotation.Entity
    @Table("r2f_map_rows")
    public static class R2FKeyedMapEntity extends LinkedHashMap<String, Object> {
        private static final long serialVersionUID = 1L;

        public Long getId() {
            return (Long) get("id");
        }

        public void setId(final Long id) {
            put("id", id);
        }
    }

    private static java.util.stream.Stream<NamingCase> namingPolicies() {
        return java.util.stream.Stream.of(
                new NamingCase(NamingPolicy.SNAKE_CASE, "account_id", "display_name", "account_id AS \"accountId\", display_name AS \"displayName\""),
                new NamingCase(NamingPolicy.SCREAMING_SNAKE_CASE, "ACCOUNT_ID", "DISPLAY_NAME", "ACCOUNT_ID AS \"accountId\", DISPLAY_NAME AS \"displayName\""),
                new NamingCase(NamingPolicy.CAMEL_CASE, "accountId", "displayName", "accountId AS \"accountId\", displayName AS \"displayName\""));
    }

    private static NamingEntity namingEntity() {
        NamingEntity entity = new NamingEntity();
        entity.setAccountId(1L);
        entity.setDisplayName("Ada");
        return entity;
    }

    private record NamingCase(NamingPolicy policy, String keyColumn, String valueColumn, String projection) {
        @Override
        public String toString() {
            return policy.name();
        }
    }

    @Table("people")
    public static class NamingEntity {
        @Id
        private Long accountId;
        private String displayName;

        public Long getAccountId() {
            return accountId;
        }

        public void setAccountId(Long accountId) {
            this.accountId = accountId;
        }

        public String getDisplayName() {
            return displayName;
        }

        public void setDisplayName(String displayName) {
            this.displayName = displayName;
        }
    }
}
