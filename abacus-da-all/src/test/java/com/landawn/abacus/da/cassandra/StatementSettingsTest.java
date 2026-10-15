package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.da.cassandra.CassandraExecutor.StatementSettings;

public class StatementSettingsTest extends TestBase {

    // ---------------------------------------------------------------------
    //  StatementSettings (v2) tests - no DB required
    // ---------------------------------------------------------------------

    @Test
    public void test_StatementSettings_defaultsAreNull() {
        StatementSettings settings = new StatementSettings();
        assertNull(settings.consistency());
        assertNull(settings.serialConsistency());
        assertNull(settings.fetchSize());
        assertNull(settings.timeout());
        assertNull(settings.traceQuery());
    }

    @Test
    public void test_StatementSettings_allArgsConstructorAndBuilder() {
        java.time.Duration d = java.time.Duration.ofSeconds(15);
        StatementSettings s1 = new StatementSettings(ConsistencyLevel.QUORUM, ConsistencyLevel.LOCAL_SERIAL, 250, d, Boolean.TRUE);
        assertEquals(ConsistencyLevel.QUORUM, s1.consistency());
        assertEquals(ConsistencyLevel.LOCAL_SERIAL, s1.serialConsistency());
        assertEquals(250, s1.fetchSize().intValue());
        assertEquals(d, s1.timeout());
        assertTrue(s1.traceQuery());

        StatementSettings s2 = StatementSettings.builder()
                .consistency(ConsistencyLevel.ONE)
                .serialConsistency(ConsistencyLevel.SERIAL)
                .fetchSize(99)
                .timeout(java.time.Duration.ofMillis(500))
                .traceQuery(Boolean.FALSE)
                .build();
        assertEquals(ConsistencyLevel.ONE, s2.consistency());
        assertEquals(99, s2.fetchSize().intValue());
        assertFalse(s2.traceQuery());
    }

    // ---------------------------------------------------------------------
    //  StatementSettings (v4) — extra fluent-setter and equals/hashCode coverage
    // ---------------------------------------------------------------------

    @Test
    public void test_StatementSettings_fluentSetters_returnSelf() {
        StatementSettings s = new StatementSettings();
        assertTrue(s == s.consistency(ConsistencyLevel.ONE));
        assertTrue(s == s.serialConsistency(ConsistencyLevel.SERIAL));
        assertTrue(s == s.fetchSize(10));
        assertTrue(s == s.timeout(Duration.ofSeconds(1)));
        assertTrue(s == s.traceQuery(Boolean.TRUE));
        assertEquals(ConsistencyLevel.ONE, s.consistency());
        assertEquals(10, s.fetchSize().intValue());
    }

    @Test
    public void test_StatementSettings_equalsAndHashCode() {
        Duration d = Duration.ofSeconds(5);
        StatementSettings a = new StatementSettings(ConsistencyLevel.ONE, ConsistencyLevel.SERIAL, 5, d, Boolean.TRUE);
        StatementSettings b = new StatementSettings(ConsistencyLevel.ONE, ConsistencyLevel.SERIAL, 5, d, Boolean.TRUE);
        StatementSettings c = new StatementSettings(ConsistencyLevel.TWO, ConsistencyLevel.SERIAL, 5, d, Boolean.TRUE);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertFalse(a.equals(c));
    }

    @Test
    public void test_StatementSettings_toString_containsValues() {
        StatementSettings s = new StatementSettings();
        s.fetchSize(123);
        String str = s.toString();
        assertNotNull(str);
        assertTrue(str.contains("123"));
    }
}
