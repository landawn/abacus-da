package com.landawn.abacus.da.cassandra.v3;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.annotation.Table;
import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.query.condition.Criteria;

/**
 * Review 2026-11-03 (slice s08): pins the per-method {@code @throws IllegalStateException} of the twenty LIMIT-appending
 * {@code Condition} methods for the shipped driver-3 {@link AsyncCassandraExecutor}, over a real {@link CassandraExecutor}
 * and a mocked session.
 */
@SuppressWarnings("deprecation")
public class AsyncCassandraExecutorReview20261103Test extends TestBase {

    private static Session newSession() {
        final Session session = mock(Session.class);
        final Cluster cluster = mock(Cluster.class);
        final Configuration configuration = mock(Configuration.class);
        final ProtocolOptions protocolOptions = mock(ProtocolOptions.class);
        when(session.init()).thenReturn(session);
        when(session.getCluster()).thenReturn(cluster);
        when(cluster.getConfiguration()).thenReturn(configuration);
        when(configuration.getCodecRegistry()).thenReturn(new CodecRegistry());
        when(configuration.getProtocolOptions()).thenReturn(protocolOptions);
        when(protocolOptions.getProtocolVersion()).thenReturn(ProtocolVersion.V4);
        return session;
    }

    // Thrown at the call site, before the session is asked to prepare or execute anything (so no future exists that could
    // report it).
    @Test
    public void test_limitInConditionThrowsAtCallSiteWithoutTouchingTheSession() {
        final Session session = newSession();
        final AsyncCassandraExecutor async = new CassandraExecutor(session).async();
        clearInvocations(session);

        final Condition limited = Criteria.builder().where(Filters.eq("orderId", 7L)).limit(5).build();
        final Class<S08V3Order> c = S08V3Order.class;
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

    @Table("orders_s08_v3")
    public static class S08V3Order {
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
