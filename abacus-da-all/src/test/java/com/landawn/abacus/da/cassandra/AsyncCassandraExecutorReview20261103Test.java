package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.driver.internal.core.util.concurrent.BlockingOperation;
import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.annotation.Table;
import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.query.condition.Criteria;

/**
 * Review 2026-11-03 (slice s08): pins two Javadoc claims of the shipped v4 {@link AsyncCassandraExecutor}, over a real
 * {@link CassandraExecutor} and a mocked session.
 */
public class AsyncCassandraExecutorReview20261103Test extends TestBase {

    private static CqlSession newSession() {
        final CqlSession session = mock(CqlSession.class);
        final DriverContext context = mock(DriverContext.class);
        when(session.getContext()).thenReturn(context);
        when(context.getCodecRegistry()).thenReturn(mock(MutableCodecRegistry.class));
        return session;
    }

    // @throws IllegalStateException on the twenty LIMIT-appending Condition methods: thrown at the call site, before the
    // session is asked to prepare or execute anything (so no future exists that could report it).
    @Test
    public void test_limitInConditionThrowsAtCallSiteWithoutTouchingTheSession() {
        final CqlSession session = newSession();
        final AsyncCassandraExecutor async = new CassandraExecutor(session).async();
        clearInvocations(session);

        final Condition limited = Criteria.builder().where(Filters.eq("orderId", 7L)).limit(5).build();
        final Class<S08V4Order> c = S08V4Order.class;
        final List<String> labels = List.of("label");

        final List<Supplier<Object>> calls = List.of(() -> async.get(c, limited), () -> async.get(c, labels, limited), () -> async.gett(c, limited),
                () -> async.gett(c, labels, limited), () -> async.exists(c, limited), () -> async.findFirst(c, limited),
                () -> async.findFirst(c, labels, limited), () -> async.queryForBoolean(c, "label", limited), () -> async.queryForChar(c, "label", limited),
                () -> async.queryForByte(c, "label", limited), () -> async.queryForShort(c, "label", limited), () -> async.queryForInt(c, "label", limited),
                () -> async.queryForLong(c, "label", limited), () -> async.queryForFloat(c, "label", limited), () -> async.queryForDouble(c, "label", limited),
                () -> async.queryForString(c, "label", limited), () -> async.queryForDate(c, "label", limited),
                () -> async.queryForDate(c, Timestamp.class, "label", limited), () -> async.queryForSingleValue(c, String.class, "label", limited),
                () -> async.queryForSingleNonNull(c, String.class, "label", limited));

        final List<Executable> checks = new ArrayList<>();

        for (final Supplier<Object> call : calls) {
            checks.add(() -> assertThrows(IllegalStateException.class, call::get));
        }

        checks.add(() -> assertEquals(20, calls.size()));
        checks.add(() -> verifyNoInteractions(session));

        assertAll(checks);
    }

    // Class doc: a ResultSet consumed past its first page on a driver thread throws IllegalStateException (the next page is
    // fetched with a blocking wait), as the v3 class doc says for driver 3.
    @Test
    public void test_resultSetConsumedPastFirstPageOnDriverThreadThrowsIllegalStateException() throws Exception {
        final CqlSession session = newSession();
        final AsyncCassandraExecutor async = new CassandraExecutor(session).async();
        final AsyncResultSet first = mock(AsyncResultSet.class);
        final AsyncResultSet second = mock(AsyncResultSet.class);
        when(first.currentPage()).thenReturn(List.of());
        when(first.hasMorePages()).thenReturn(true);
        when(first.fetchNextPage()).thenReturn(CompletableFuture.completedFuture(second));
        when(second.currentPage()).thenReturn(List.of());
        when(session.executeAsync(any(Statement.class))).thenReturn(CompletableFuture.completedFuture(first));

        final ResultSet rs = async.execute(SimpleStatement.newInstance("SELECT id FROM t")).get(); // first page: application thread
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread driverThread = new BlockingOperation.SafeThreadFactory().newThread(() -> {
            try {
                rs.iterator().hasNext();
            } catch (final Throwable e) {
                failure.set(e);
            }
        });

        driverThread.start();
        driverThread.join(10_000);

        assertFalse(driverThread.isAlive());
        assertInstanceOf(IllegalStateException.class, failure.get());
        verify(first, never()).fetchNextPage();
    }

    @Table("orders_s08_v4")
    public static class S08V4Order {
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
