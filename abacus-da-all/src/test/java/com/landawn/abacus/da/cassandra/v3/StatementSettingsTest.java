package com.landawn.abacus.da.cassandra.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.da.cassandra.v3.CassandraExecutor.StatementSettings;

public class StatementSettingsTest extends TestBase {

    // ---------------------------------------------------------------------
    //  StatementSettings (v3) tests - no DB required
    // ---------------------------------------------------------------------

    @Test
    public void test_StatementSettings_defaultsAreNull() {
        StatementSettings settings = new StatementSettings();
        assertNull(settings.consistency());
        assertNull(settings.serialConsistency());
        assertNull(settings.retryPolicy());
        assertNull(settings.fetchSize());
        assertNull(settings.readTimeoutMillis());
        assertNull(settings.traceQuery());
    }

    @Test
    public void test_StatementSettings_allArgsConstructor() {
        StatementSettings settings = new StatementSettings(ConsistencyLevel.QUORUM, ConsistencyLevel.SERIAL, DefaultRetryPolicy.INSTANCE, 500, 7000,
                Boolean.TRUE);
        assertEquals(ConsistencyLevel.QUORUM, settings.consistency());
        assertEquals(ConsistencyLevel.SERIAL, settings.serialConsistency());
        assertEquals(DefaultRetryPolicy.INSTANCE, settings.retryPolicy());
        assertEquals(500, settings.fetchSize().intValue());
        assertEquals(7000, settings.readTimeoutMillis().intValue());
        assertTrue(settings.traceQuery());
    }

    @Test
    public void test_StatementSettings_builderAndFluentSetters() {
        StatementSettings settings = StatementSettings.builder()
                .consistency(ConsistencyLevel.ONE)
                .serialConsistency(ConsistencyLevel.LOCAL_SERIAL)
                .retryPolicy(DefaultRetryPolicy.INSTANCE)
                .fetchSize(100)
                .readTimeoutMillis(2000)
                .traceQuery(Boolean.FALSE)
                .build();

        assertEquals(ConsistencyLevel.ONE, settings.consistency());
        assertEquals(ConsistencyLevel.LOCAL_SERIAL, settings.serialConsistency());
        assertEquals(DefaultRetryPolicy.INSTANCE, settings.retryPolicy());
        assertEquals(100, settings.fetchSize().intValue());
        assertEquals(2000, settings.readTimeoutMillis().intValue());
        assertFalse(settings.traceQuery());

        // Verify fluent setters return same instance
        StatementSettings s2 = new StatementSettings();
        assertTrue(s2 == s2.consistency(ConsistencyLevel.TWO));
        assertEquals(ConsistencyLevel.TWO, s2.consistency());
    }

    // ---------------------------------------------------------------------
    //  StatementSettings (v3) — extra equals/hashCode/toString coverage
    // ---------------------------------------------------------------------

    @Test
    public void test_v3_StatementSettings_equalsAndHashCode() {
        StatementSettings a = new StatementSettings(ConsistencyLevel.ONE, ConsistencyLevel.SERIAL, DefaultRetryPolicy.INSTANCE, 5, 1000, Boolean.TRUE);
        StatementSettings b = new StatementSettings(ConsistencyLevel.ONE, ConsistencyLevel.SERIAL, DefaultRetryPolicy.INSTANCE, 5, 1000, Boolean.TRUE);
        StatementSettings c = new StatementSettings(ConsistencyLevel.TWO, ConsistencyLevel.SERIAL, DefaultRetryPolicy.INSTANCE, 5, 1000, Boolean.TRUE);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertFalse(a.equals(c));
    }

    @Test
    public void test_v3_StatementSettings_toString_containsValues() {
        StatementSettings s = new StatementSettings();
        s.fetchSize(456);
        String str = s.toString();
        assertNotNull(str);
        assertTrue(str.contains("456"));
    }

    @Test
    public void test_v3_StatementSettings_allFluentSetters_returnSelf() {
        StatementSettings s = new StatementSettings();
        assertTrue(s == s.consistency(ConsistencyLevel.ONE));
        assertTrue(s == s.serialConsistency(ConsistencyLevel.SERIAL));
        assertTrue(s == s.retryPolicy(DefaultRetryPolicy.INSTANCE));
        assertTrue(s == s.fetchSize(11));
        assertTrue(s == s.readTimeoutMillis(2222));
        assertTrue(s == s.traceQuery(Boolean.TRUE));
        assertEquals(11, s.fetchSize().intValue());
        assertEquals(2222, s.readTimeoutMillis().intValue());
    }
}
