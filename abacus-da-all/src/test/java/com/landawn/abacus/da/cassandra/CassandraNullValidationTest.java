/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */
package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.landawn.abacus.da.cassandra.CassandraExecutor.StatementSettings;

class CassandraNullValidationTest {
    private static CassandraExecutor executor(final CqlSession session, final StatementSettings settings) {
        final DriverContext context = mock(DriverContext.class);
        when(session.getContext()).thenReturn(context);
        when(context.getCodecRegistry()).thenReturn(mock(CodecRegistry.class));
        return new CassandraExecutor(session, settings);
    }

    @Test
    void requiredHelperArgumentsFailBeforeDriverOrRowAccess() {
        final CqlSession session = mock(CqlSession.class);
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
        final CqlSession session = mock(CqlSession.class);
        assertNull(executor(session, null).configStatement(null));
        assertNull(executor(session, new StatementSettings()).configStatement(null));

        final CassandraExecutor configured = executor(session, new StatementSettings().traceQuery(true));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> configured.configStatement(null)).getMessage().contains("stmt"));
    }

    @Test
    void codecsRejectMissingJavaClassAndRetainNullableValuesAndDeferredUdtState() {
        assertThrows(IllegalArgumentException.class, () -> new CassandraExecutor.StringCodec<>(null));

        final CassandraExecutor.StringCodec<String> codec = new CassandraExecutor.StringCodec<>(String.class);
        assertEquals("NULL", codec.format(null));
        assertNull(codec.encode(null, null));

        final CassandraExecutor.UDTCodec<?> udtCodec = CassandraExecutor.UDTCodec.create((UserDefinedType) null, List.class);
        assertEquals("NULL", udtCodec.format(null));
        assertThrows(NullPointerException.class, udtCodec::newUDTValue);
    }

    @Test
    void emptyResultStillAcceptsAnUnspecifiedMappingClass() {
        final CassandraExecutor executor = executor(mock(CqlSession.class), null);
        final ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.iterator()).thenReturn(List.<Row>of().iterator());

        assertNull(executor.fetchOnlyOne(null, resultSet));
    }

    @Test
    void requiredDslAndDocumentArgumentsUseArgumentValidation() {
        assertThrows(IllegalArgumentException.class, () -> new CqlBuilder.Dsl(null));
        assertThrows(IllegalArgumentException.class, () -> CqlMapper.requireCqlMapperRoot(null, null));
    }
}
