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
