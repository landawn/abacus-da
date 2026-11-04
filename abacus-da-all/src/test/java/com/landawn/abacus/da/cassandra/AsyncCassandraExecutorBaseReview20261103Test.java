package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.annotation.Table;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.query.condition.Criteria;

/**
 * Review 2026-11-03 (slice s08): pins the per-method {@code @throws IllegalStateException} added to the twenty
 * {@link Condition}-based async methods that append their own {@code LIMIT}, and the {@code null} WHERE-clause contract
 * added to the {@code @param whereClause} docs of exists/count/list/query/stream/findFirst.
 */
public class AsyncCassandraExecutorBaseReview20261103Test extends CassandraExecutorBaseTestSupport {

    // Every one of the twenty methods fails on the calling thread (assertThrows on the call itself, no get()), and nothing is
    // submitted: the LIMIT conflict is detected while the CQL is built, before execute(...) creates a future.
    @Test
    public void test_everyLimitAppendingConditionMethodThrowsIllegalStateExceptionAtTheCallSite() {
        final Condition limited = Criteria.builder().where(Filters.eq("orderId", 7L)).limit(5).build();
        final Condition bareLimit = Filters.limit(3);
        final RecordingExecutor recording = new RecordingExecutor();
        final AsyncCassandraExecutorBase<TestRow, TestResultSet, TestStatement, TestPreparedStatement, TestBatchType> async = recording.recordingAsync();
        final Class<S08Order> c = S08Order.class;
        final List<String> labels = List.of("label");

        final List<Supplier<Object>> calls = List.of( //
                () -> async.get(c, limited), //
                () -> async.get(c, labels, limited), //
                () -> async.gett(c, limited), //
                () -> async.gett(c, labels, limited), //
                () -> async.exists(c, limited), //
                () -> async.findFirst(c, limited), //
                () -> async.findFirst(c, labels, limited), //
                () -> async.queryForBoolean(c, "label", limited), //
                () -> async.queryForChar(c, "label", limited), //
                () -> async.queryForByte(c, "label", limited), //
                () -> async.queryForShort(c, "label", limited), //
                () -> async.queryForInt(c, "label", limited), //
                () -> async.queryForLong(c, "label", limited), //
                () -> async.queryForFloat(c, "label", limited), //
                () -> async.queryForDouble(c, "label", limited), //
                () -> async.queryForString(c, "label", limited), //
                () -> async.queryForDate(c, "label", limited), //
                () -> async.queryForDate(c, Timestamp.class, "label", bareLimit), //
                () -> async.queryForSingleValue(c, String.class, "label", bareLimit), //
                () -> async.queryForSingleNonNull(c, String.class, "label", bareLimit));

        final List<Executable> checks = new ArrayList<>();

        for (int i = 0; i < calls.size(); i++) {
            final Supplier<Object> call = calls.get(i);
            checks.add(() -> assertThrows(IllegalStateException.class, call::get));
        }

        checks.add(() -> assertEquals(20, calls.size()));
        checks.add(() -> assertEquals(List.of(), recording.recorded, "no statement may be submitted"));

        assertAll(checks);
    }

    // The sync docs say a null whereClause is accepted by these methods; the async twins now say so too. The CQL carries no
    // WHERE clause and a future is returned.
    @Test
    public void test_nullWhereClauseIsAcceptedByExistsCountListQueryStreamFindFirst() {
        final RecordingExecutor recording = new RecordingExecutor();
        final AsyncCassandraExecutorBase<TestRow, TestResultSet, TestStatement, TestPreparedStatement, TestBatchType> async = recording.recordingAsync();
        final Class<S08Order> c = S08Order.class;

        assertAll(() -> assertNotNull(async.exists(c, (Condition) null)), //
                () -> assertNotNull(async.count(c, null)), //
                () -> assertNotNull(async.list(c, (Condition) null)), //
                () -> assertNotNull(async.query(c, (Condition) null)), //
                () -> assertNotNull(async.stream(c, (Condition) null)), //
                () -> assertNotNull(async.findFirst(c, (Condition) null)));

        assertEquals(6, recording.recorded.size());

        for (final String cql : recording.recorded) {
            assertTrue(cql.startsWith("SELECT "), cql);
            assertTrue(cql.contains(" FROM orders_s08"), cql);
            assertFalse(cql.contains("WHERE"), cql);
        }

        // exists and findFirst still append their LIMIT 1; list/query/stream/count append none.
        assertAll(() -> assertTrue(recording.recorded.get(0).contains(" LIMIT 1 "), recording.recorded.get(0)),
                () -> assertFalse(recording.recorded.get(1).contains("LIMIT"), recording.recorded.get(1)),
                () -> assertFalse(recording.recorded.get(2).contains("LIMIT"), recording.recorded.get(2)),
                () -> assertTrue(recording.recorded.get(5).contains(" LIMIT 1 "), recording.recorded.get(5)));
    }

    @Table("orders_s08")
    public static class S08Order {
        @Id
        private Long orderId;
        private String label;

        public Long getOrderId() {
            return orderId;
        }

        public void setOrderId(final Long orderId) {
            this.orderId = orderId;
        }

        public String getLabel() {
            return label;
        }

        public void setLabel(final String label) {
            this.label = label;
        }
    }
}
