/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */
package com.landawn.abacus.da.cassandra.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.landawn.abacus.da.cassandra.v3.CassandraExecutor.StatementSettings;

class CassandraNullValidationTest {
    private static CassandraExecutor executor(final Session session, final StatementSettings settings) {
        final Cluster cluster = mock(Cluster.class);
        final Configuration configuration = mock(Configuration.class);
        final ProtocolOptions protocolOptions = mock(ProtocolOptions.class);
        when(session.init()).thenReturn(session);
        when(session.getCluster()).thenReturn(cluster);
        when(cluster.getConfiguration()).thenReturn(configuration);
        when(configuration.getCodecRegistry()).thenReturn(new CodecRegistry());
        when(configuration.getProtocolOptions()).thenReturn(protocolOptions);
        when(protocolOptions.getProtocolVersion()).thenReturn(ProtocolVersion.V4);
        return new CassandraExecutor(session, settings);
    }

    @Test
    void requiredHelperArgumentsFailBeforeDriverOrRowAccess() {
        final Session session = mock(Session.class);
        final CassandraExecutor executor = executor(session, null);
        final Row row = mock(Row.class);

        assertThrows(IllegalArgumentException.class, () -> executor.prepare(null));
        assertThrows(IllegalArgumentException.class, () -> executor.bind(null));
        assertThrows(IllegalArgumentException.class, () -> executor.fetchOnlyOne(null, null));
        assertThrows(IllegalArgumentException.class, () -> executor.readFirstColumn(null, String.class));
        assertThrows(IllegalArgumentException.class, () -> executor.readFirstColumn(row, null));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> executor.toList(null, (ResultSet) null)).getMessage().contains("targetClass"));

        verify(session, never()).prepare((String) null);
        verify(row, never()).getObject(0);
    }

    @Test
    void nullStatementRetainsNoOpBehaviorUntilASettingIsApplied() {
        final Session session = mock(Session.class);
        executor(session, null).configStatement(null);
        executor(session, new StatementSettings()).configStatement(null);

        final CassandraExecutor configured = executor(session, new StatementSettings().traceQuery(true));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> configured.configStatement(null)).getMessage().contains("stmt"));
    }

    @Test
    void codecRejectsMissingJavaClassAndRetainsNullableValues() {
        assertThrows(IllegalArgumentException.class, () -> new CassandraExecutor.StringCodec<>(null));

        final CassandraExecutor.StringCodec<String> codec = new CassandraExecutor.StringCodec<>(String.class);
        assertEquals("NULL", codec.format(null));
        assertNull(codec.serialize(null, null));
    }

    @Test
    void emptyResultStillAcceptsAnUnspecifiedMappingClass() {
        final CassandraExecutor executor = executor(mock(Session.class), null);
        final ResultSet resultSet = mock(ResultSet.class);

        assertNull(executor.fetchOnlyOne(null, resultSet));
    }
}
