package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.annotation.Table;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.query.condition.Criteria;

public class AsyncCassandraExecutorBaseReview20261102Test extends CassandraExecutorBaseTestSupport {

    // Pins the class documentation's row-limit paragraph: the async methods that append their own LIMIT reject a condition that
    // already carries one at the call site (no future, nothing submitted), and list/query/stream/count apply its LIMIT as given.
    @Test
    public void test_conditionWithLimitRejectedAtCallSiteOnlyByMethodsThatAppendTheirOwn() {
        final Condition limited = Criteria.builder().where(Filters.eq("orderId", 7L)).limit(5).build();
        final RecordingExecutor recording = new RecordingExecutor();
        final AsyncCassandraExecutorBase<TestRow, TestResultSet, TestStatement, TestPreparedStatement, TestBatchType> async = recording.recordingAsync();

        assertAll(() -> assertThrows(IllegalStateException.class, () -> async.get(ReviewOrder.class, limited)),
                () -> assertThrows(IllegalStateException.class, () -> async.gett(ReviewOrder.class, List.of("label"), limited)),
                () -> assertThrows(IllegalStateException.class, () -> async.findFirst(ReviewOrder.class, limited)),
                () -> assertThrows(IllegalStateException.class, () -> async.exists(ReviewOrder.class, Filters.limit(1))),
                () -> assertThrows(IllegalStateException.class, () -> async.queryForSingleValue(ReviewOrder.class, String.class, "label", limited)),
                () -> assertThrows(IllegalStateException.class, () -> async.queryForSingleNonNull(ReviewOrder.class, String.class, "label", limited)),
                () -> assertThrows(IllegalStateException.class, () -> async.queryForLong(ReviewOrder.class, "orderId", limited)),
                () -> assertEquals(List.of(), recording.recorded));

        async.list(ReviewOrder.class, List.of("label"), limited);
        async.query(ReviewOrder.class, List.of("label"), limited);
        async.stream(ReviewOrder.class, List.of("label"), limited);
        async.count(ReviewOrder.class, limited);

        final String select = "SELECT label AS \"label\" FROM orders_review WHERE order_id = :orderId LIMIT 5 [7]";
        assertEquals(List.of(select, select, select, "SELECT count(*) FROM orders_review WHERE order_id = :orderId LIMIT 5 [7]"), recording.recorded);
    }

    @Table("orders_review")
    public static class ReviewOrder {
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
