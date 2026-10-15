package com.landawn.abacus.da.gcp;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.QueryBuilderLeakAssertions;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.util.NamingPolicy;

public class BigQueryBuilderLifecycleTest extends BigQueryExecutorTestSupport {

    @Test
    public void testRejectedQueryConditionReleasesInternalSqlBuilder() throws Exception {
        final Condition invalidCondition = Filters.expr("/* only a comment */");
        for (final NamingPolicy policy : List.of(NamingPolicy.SNAKE_CASE, NamingPolicy.SCREAMING_SNAKE_CASE, NamingPolicy.CAMEL_CASE)) {
            final BigQueryExecutor current = new BigQueryExecutor(mockBigQuery, policy);
            for (final Collection<String> properties : Arrays.asList(null, List.of("id"))) {
                assertRejectedWithoutBuilderLeak(() -> current.query(TestEntity.class, properties, invalidCondition));
            }
        }
        org.mockito.Mockito.verifyNoInteractions(mockBigQuery);
    }

    @Test
    public void testRejectedQueryProjectionReleasesInternalSqlBuilder() throws Exception {
        for (final NamingPolicy policy : List.of(NamingPolicy.SNAKE_CASE, NamingPolicy.SCREAMING_SNAKE_CASE, NamingPolicy.CAMEL_CASE)) {
            final BigQueryExecutor current = new BigQueryExecutor(mockBigQuery, policy);
            assertRejectedWithoutBuilderLeak(() -> current.query(TestEntity.class, List.of("id /* invalid column */"), null));
        }
        org.mockito.Mockito.verifyNoInteractions(mockBigQuery);
    }

    @Test
    public void testRejectedUpdateConditionReleasesInternalSqlBuilder() throws Exception {
        final Condition invalidCondition = Filters.expr("/* only a comment */");
        for (final NamingPolicy policy : List.of(NamingPolicy.SNAKE_CASE, NamingPolicy.SCREAMING_SNAKE_CASE, NamingPolicy.CAMEL_CASE)) {
            final BigQueryExecutor current = new BigQueryExecutor(mockBigQuery, policy);
            assertRejectedWithoutBuilderLeak(() -> current.update(TestEntity.class, Map.of("name", "updated"), invalidCondition));
        }
        org.mockito.Mockito.verifyNoInteractions(mockBigQuery);
    }

    @Test
    public void testRejectedUpdatePropertyReleasesInternalSqlBuilder() throws Exception {
        for (final NamingPolicy policy : List.of(NamingPolicy.SNAKE_CASE, NamingPolicy.SCREAMING_SNAKE_CASE, NamingPolicy.CAMEL_CASE)) {
            final BigQueryExecutor current = new BigQueryExecutor(mockBigQuery, policy);
            assertRejectedWithoutBuilderLeak(() -> current.update(TestEntity.class, Map.of("name /* invalid column */", "updated"), Filters.eq("id", 1)));
        }
        org.mockito.Mockito.verifyNoInteractions(mockBigQuery);
    }

    @Test
    public void testRejectedDeleteConditionReleasesInternalSqlBuilder() throws Exception {
        final Condition invalidCondition = Filters.expr("/* only a comment */");
        for (final NamingPolicy policy : List.of(NamingPolicy.SNAKE_CASE, NamingPolicy.SCREAMING_SNAKE_CASE, NamingPolicy.CAMEL_CASE)) {
            final BigQueryExecutor current = new BigQueryExecutor(mockBigQuery, policy);
            assertRejectedWithoutBuilderLeak(() -> current.delete(TestEntity.class, invalidCondition));
        }
        org.mockito.Mockito.verifyNoInteractions(mockBigQuery);
    }

    @Test
    public void testRejectedInsertValueReleasesInternalSqlBuilder() throws Exception {
        final Condition invalidCondition = Filters.expr("/* only a comment */");
        for (final NamingPolicy policy : List.of(NamingPolicy.SNAKE_CASE, NamingPolicy.SCREAMING_SNAKE_CASE, NamingPolicy.CAMEL_CASE)) {
            final BigQueryExecutor current = new BigQueryExecutor(mockBigQuery, policy);
            assertRejectedWithoutBuilderLeak(() -> current.insert(TestEntity.class, Map.of("name", invalidCondition)));
        }
        org.mockito.Mockito.verifyNoInteractions(mockBigQuery);
    }

    @Test
    public void testRejectedEntityInsertValueReleasesInternalSqlBuilder() throws Exception {
        final SqlValueEntity entity = new SqlValueEntity();
        entity.setId(1L);
        entity.setValue(Filters.expr("/* only a comment */"));
        for (final NamingPolicy policy : List.of(NamingPolicy.SNAKE_CASE, NamingPolicy.SCREAMING_SNAKE_CASE, NamingPolicy.CAMEL_CASE)) {
            final BigQueryExecutor current = new BigQueryExecutor(mockBigQuery, policy);
            assertRejectedWithoutBuilderLeak(() -> current.insert(entity));
        }
        org.mockito.Mockito.verifyNoInteractions(mockBigQuery);
    }

    @Test
    public void testRejectedEntityUpdateValueReleasesInternalSqlBuilder() throws Exception {
        final SqlValueEntity entity = new SqlValueEntity();
        entity.setId(1L);
        entity.setValue(Filters.expr("/* only a comment */"));
        for (final NamingPolicy policy : List.of(NamingPolicy.SNAKE_CASE, NamingPolicy.SCREAMING_SNAKE_CASE, NamingPolicy.CAMEL_CASE)) {
            final BigQueryExecutor current = new BigQueryExecutor(mockBigQuery, policy);
            assertRejectedWithoutBuilderLeak(() -> current.update(entity, Set.of("id")));
        }
        org.mockito.Mockito.verifyNoInteractions(mockBigQuery);
    }

    @Test
    public void testRejectedDslFactoryCallDoesNotLeakSqlBuilder() throws Exception {
        // Creation-time failures: the rejection happens inside the Dsl factory (PSC/PAC/PLC.insert/select), which
        // validates and snapshots its arguments before allocating a pooled builder (and releases an allocated builder
        // on failure), so buildSql is never reached. These calls pin that factory contract.
        for (final NamingPolicy policy : List.of(NamingPolicy.SNAKE_CASE, NamingPolicy.SCREAMING_SNAKE_CASE, NamingPolicy.CAMEL_CASE)) {
            final BigQueryExecutor current = new BigQueryExecutor(mockBigQuery, policy);
            // A blank property name is rejected inside PSC.select(...) before a builder is allocated.
            assertRejectedWithoutBuilderLeak(() -> current.query(TestEntity.class, List.of(""), null));
            // A blank key is rejected inside PSC.insert(props) before a builder is allocated.
            assertRejectedWithoutBuilderLeak(() -> current.insert(TestEntity.class, Map.of("", "updated")));
            // A bean with no insertable value is rejected inside PSC.insert(entity) before a builder is allocated.
            assertRejectedWithoutBuilderLeak(() -> current.insert(new SqlValueEntity()));
        }
        org.mockito.Mockito.verifyNoInteractions(mockBigQuery);
    }

    // The counter assertions are the empirical canary for buildSql's documented assumption that
    // AbstractQueryBuilder.build() releases the pooled StringBuilder even for an incomplete statement (and that its
    // closed-state guard prevents a second decrement); the Dsl factories' creation-time release discipline is pinned
    // the same way by testRejectedDslFactoryCallDoesNotLeakSqlBuilder.
    private static void assertRejectedWithoutBuilderLeak(final org.junit.jupiter.api.function.Executable action) {
        QueryBuilderLeakAssertions.assertRejectedWithoutBuilderLeak(IllegalArgumentException.class, action,
                "A rejected query must release the executor-owned builder");
    }

    @lombok.Data
    public static class SqlValueEntity {
        private Long id;
        private Condition value;
    }
}
