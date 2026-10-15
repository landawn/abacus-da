package com.landawn.abacus.da.cassandra;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.da.QueryBuilderLeakAssertions;
import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.exception.DuplicateResultException;
import com.landawn.abacus.query.AbstractQueryBuilder.SP;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.u.Nullable;
import com.landawn.abacus.util.u.Optional;

abstract class CassandraExecutorBaseTestSupport extends TestBase {

    protected TestCassandraExecutor executor;

    @BeforeEach
    public void setUp() {
        CassandraExecutorBase.entityKeyNamesMap.remove(TestEntity.class);
        executor = new TestCassandraExecutor();
    }

    // Composite-key entity for entityToCondition / idsToCondition tests
    public static class CompositeKeyEntity {
        @Id
        private String userId;
        @Id
        private String sessionId;
        private String value;

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public String getSessionId() {
            return sessionId;
        }

        public void setSessionId(String sessionId) {
            this.sessionId = sessionId;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    // Test implementation of CassandraExecutorBase
    protected static class TestCassandraExecutor extends CassandraExecutorBase<TestRow, TestResultSet, TestStatement, TestPreparedStatement, TestBatchType> {

        // Expose protected static helpers for tests
        public static Condition exposedIdsToCondition(Class<?> targetClass, Object... ids) {
            return CassandraExecutorBase.idsToCondition(targetClass, ids);
        }

        public static Condition exposedEntityToCondition(Object entity) {
            return CassandraExecutorBase.entityToCondition(entity);
        }

        public static Condition exposedEntityToConditionCollection(Class<?> entityClass, Collection<?> entities) {
            return CassandraExecutorBase.entityToCondition(entityClass, entities);
        }

        public SP exposedPrepareInsert(Object entity) {
            return prepareInsert(entity);
        }

        public SP exposedPrepareInsert(Class<?> targetClass, Map<String, Object> props) {
            return prepareInsert(targetClass, props);
        }

        public SP exposedPrepareUpdate(Object entity, Collection<String> propNamesToUpdate) {
            return prepareUpdate(entity, propNamesToUpdate);
        }

        public SP exposedPrepareUpdate(Class<?> targetClass, Map<String, Object> props, Condition whereClause) {
            return prepareUpdate(targetClass, props, whereClause);
        }

        public SP exposedPrepareDelete(Class<?> targetClass, Collection<String> propNamesToDelete, Condition whereClause) {
            return prepareDelete(targetClass, propNamesToDelete, whereClause);
        }

        public SP exposedPrepareQuery(Class<?> targetClass, Collection<String> selectPropNames, Condition whereClause, int count) {
            return prepareQuery(targetClass, selectPropNames, whereClause, count);
        }

        public com.landawn.abacus.da.cassandra.ParsedCql exposedParseCql(String cql) {
            return parseCql(cql);
        }

        public TestCassandraExecutor() {
            super(null, NamingPolicy.SNAKE_CASE);
        }

        public TestCassandraExecutor(NamingPolicy namingPolicy) {
            super(null, namingPolicy);
        }

        public TestCassandraExecutor(CqlMapper cqlMapper, NamingPolicy namingPolicy) {
            super(cqlMapper, namingPolicy);
        }

        public SP exposedPrepareQueryThreeArgs(Class<?> targetClass, Collection<String> selectPropNames, Condition whereClause) {
            return prepareQuery(targetClass, selectPropNames, whereClause);
        }

        private final AsyncCassandraExecutorBase<TestRow, TestResultSet, TestStatement, TestPreparedStatement, TestBatchType> asyncExecutor = new AsyncCassandraExecutorBase<>(
                this) {
            @Override
            public ContinuableFuture<TestResultSet> execute(String query) {
                return ContinuableFuture.completed(new TestResultSet());
            }

            @Override
            public ContinuableFuture<TestResultSet> execute(String query, Object... parameters) {
                return ContinuableFuture.completed(new TestResultSet());
            }

            @Override
            public ContinuableFuture<TestResultSet> execute(String query, Map<String, Object> parameters) {
                return ContinuableFuture.completed(new TestResultSet());
            }

            @Override
            public ContinuableFuture<TestResultSet> execute(TestStatement statement) {
                return ContinuableFuture.completed(new TestResultSet());
            }
        };

        public AsyncCassandraExecutorBase<TestRow, TestResultSet, TestStatement, TestPreparedStatement, TestBatchType> async() {
            return asyncExecutor;
        }

        SP prepareBatchMapUpdateForTest(final Class<?> targetClass, final Map<String, Object> props) {
            return prepareBatchMapUpdate(targetClass, props);
        }

        @Override
        public <T> T gett(Class<T> targetClass, Collection<String> selectPropNames, Condition whereClause) throws DuplicateResultException {
            try {
                return targetClass.newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public <E> Nullable<E> queryForSingleValue(Class<E> valueClass, String query, Object... parameters) {
            return Nullable.empty();
        }

        @Override
        public <E> Optional<E> queryForSingleNonNull(Class<E> valueClass, String query, Object... parameters) {
            return Optional.empty();
        }

        @Override
        public <T> Optional<T> findFirst(Class<T> targetClass, String query, Object... parameters) {
            try {
                return Optional.of(targetClass.newInstance());
            } catch (Exception e) {
                return Optional.empty();
            }
        }

        @Override
        public TestResultSet execute(String query) {
            return new TestResultSet();
        }

        @Override
        public TestResultSet execute(String query, Object... parameters) {
            return new TestResultSet();
        }

        @Override
        public TestResultSet execute(String query, Map<String, Object> parameters) {
            return new TestResultSet();
        }

        @Override
        public TestResultSet execute(TestStatement statement) {
            return new TestResultSet();
        }

        public void close() {
            // No-op
        }

        @Override
        protected TestStatement prepareBatchStatement(TestBatchType type) {
            return new TestStatement();
        }

        @Override
        protected TestStatement prepareBatchInsertStatement(Collection<?> entities, TestBatchType type) {
            return new TestStatement();
        }

        @Override
        protected TestStatement prepareBatchInsertStatement(Class<?> targetClass, Collection<? extends Map<String, Object>> propsList, TestBatchType type) {
            return new TestStatement();
        }

        @Override
        protected TestStatement prepareBatchUpdateStatement(Collection<?> entities, Collection<String> propNamesToUpdate, TestBatchType type) {
            return new TestStatement();
        }

        @Override
        protected TestStatement prepareBatchUpdateStatement(Class<?> targetClass, Collection<? extends Map<String, Object>> propsList, TestBatchType type) {
            return new TestStatement();
        }

        @Override
        protected TestStatement prepareBatchUpdateStatement(String query, Collection<?> parametersList, TestBatchType type) {
            return new TestStatement();
        }

        @Override
        protected TestStatement prepareStatement(String query) {
            return new TestStatement();
        }

        @Override
        protected TestStatement prepareStatement(String query, Object... parameters) {
            return new TestStatement();
        }

        @Override
        protected TestPreparedStatement prepare(String query) {
            return new TestPreparedStatement();
        }

        @Override
        protected TestStatement bind(TestPreparedStatement preStmt, Object... parameters) {
            return new TestStatement();
        }

        @Override
        protected <T> T fetchOnlyOne(Class<T> targetClass, TestResultSet resultSet) {
            try {
                return targetClass.newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected <T> List<T> toList(Class<T> targetClass, TestResultSet execute) {
            return new ArrayList<>();
        }

        @Override
        protected Dataset extractData(Class<?> targetClass, TestResultSet execute) {
            return Dataset.empty();
        }

        @Override
        protected <T> Function<TestRow, T> createRowMapper(Class<T> targetClass) {
            return row -> {
                try {
                    return targetClass.newInstance();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            };
        }

        @Override
        protected <T> T readFirstColumn(TestRow row, Class<T> targetClass) {
            return null;
        }
    }

    protected static void assertRejectedWithoutBuilderLeak(final Class<? extends Throwable> exceptionClass, final Runnable action) {
        QueryBuilderLeakAssertions.assertRejectedWithoutBuilderLeak(exceptionClass, action::run,
                "A failed executor preparation must release its unreachable builder");
    }

    @com.landawn.abacus.annotation.Table(name = "invalid table")
    public static class BadTableEntity extends TestEntity {
    }

    public static class ThrowingUpdateEntity {
        @Id
        private Long id;

        public Long getId() {
            return id;
        }

        public void setId(final Long id) {
            this.id = id;
        }

        public String getName() {
            throw new IllegalStateException("bad update getter");
        }
    }

    public static class SliceNKeylessEntity {
        private String name;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    /** Records every CQL/parameter pair handed to the driver-level execute, for both the sync and the async façade. */
    protected static class RecordingExecutor extends TestCassandraExecutor {
        final List<String> recorded = new ArrayList<>();

        private String record(final String query, final Object... parameters) {
            final String rec = query + " " + Arrays.asList(parameters == null ? new Object[0] : parameters);
            recorded.add(rec);
            return rec;
        }

        @Override
        public TestResultSet execute(String query, Object... parameters) {
            record(query, parameters);
            return new TestResultSet();
        }

        @Override
        public <E> Nullable<E> queryForSingleValue(Class<E> valueClass, String query, Object... parameters) {
            record(query, parameters);
            return Nullable.empty();
        }

        @Override
        public <E> Optional<E> queryForSingleNonNull(Class<E> valueClass, String query, Object... parameters) {
            record(query, parameters);
            return Optional.empty();
        }

        @Override
        public <T> Optional<T> findFirst(Class<T> targetClass, String query, Object... parameters) {
            record(query, parameters);
            return Optional.empty();
        }

        AsyncCassandraExecutorBase<TestRow, TestResultSet, TestStatement, TestPreparedStatement, TestBatchType> recordingAsync() {
            return new AsyncCassandraExecutorBase<>(this) {
                @Override
                public ContinuableFuture<TestResultSet> execute(String query) {
                    record(query);
                    return ContinuableFuture.completed(new TestResultSet());
                }

                @Override
                public ContinuableFuture<TestResultSet> execute(String query, Object... parameters) {
                    record(query, parameters);
                    return ContinuableFuture.completed(new TestResultSet());
                }

                @Override
                public ContinuableFuture<TestResultSet> execute(String query, Map<String, Object> parameters) {
                    record(query, parameters);
                    return ContinuableFuture.completed(new TestResultSet());
                }

                @Override
                public ContinuableFuture<TestResultSet> execute(TestStatement statement) {
                    recorded.add("statement");
                    return ContinuableFuture.completed(new TestResultSet());
                }
            };
        }
    }

    // Test classes
    public static class TestEntity {
        private Long id;
        private String name;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    protected static class TestRow {
    }

    protected static class TestResultSet implements Iterable<TestRow> {
        @Override
        public Iterator<TestRow> iterator() {
            return Arrays.asList(new TestRow()).iterator();
        }
    }

    protected static class TestStatement {
    }

    protected static class TestPreparedStatement {
    }

    protected enum TestBatchType {
        LOGGED, UNLOGGED, COUNTER
    }
}
