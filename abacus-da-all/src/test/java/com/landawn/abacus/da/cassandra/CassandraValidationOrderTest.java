/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */
package com.landawn.abacus.da.cassandra;

import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.PSC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.landawn.abacus.annotation.Id;

/** Regression coverage for validation precedence without a Cassandra server. */
class CassandraValidationOrderTest {

    @Test
    void closedBuilderStatePrecedesInvalidArgumentsAndTimestampOverflow() {
        final CqlBuilder builder = PSC.insert("id").into("users");
        builder.build();

        assertThrows(IllegalStateException.class, () -> builder.usingTTL(-1));
        assertThrows(IllegalStateException.class, () -> builder.usingTimestamp((Date) null));
        assertThrows(IllegalStateException.class, () -> builder.usingTimestamp(Long.MAX_VALUE));
        assertThrows(IllegalStateException.class, () -> builder.from((String) null));
        assertThrows(IllegalStateException.class, () -> builder.from((String[]) null));
        assertThrows(IllegalStateException.class, () -> builder.from((Collection<String>) null));
        assertThrows(IllegalStateException.class, () -> builder.from((Class<?>) null));
    }

    @Test
    void incompatibleOperationPrecedesInvalidUsingArgument() {
        final CqlBuilder builder = PSC.select("id").from("users");

        assertThrows(IllegalStateException.class, () -> builder.usingTTL(-1));
        assertThrows(IllegalStateException.class, () -> builder.usingTimestamp((Date) null));
        assertEquals("SELECT id FROM users", builder.build().query());
    }

    @Test
    void mapperValidatesAllFilesBeforeOpeningAnyFile() {
        final File missing = new File("target/cassandra-review-missing-mapper.xml");

        assertThrows(IllegalArgumentException.class, () -> CqlMapper.loadFrom(missing, null));
    }

    @Test
    void mapperValidatesIdBeforeParsedCql() {
        final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> new CqlMapper().add("", (ParsedCql) null));

        assertTrue(failure.getMessage().contains("id"));
    }

    @Test
    void scalarColumnCountIsValidatedBeforeFetchingRows() {
        final ResultSet resultSet = mock(ResultSet.class);
        final ColumnDefinitions columns = mock(ColumnDefinitions.class);
        when(resultSet.getColumnDefinitions()).thenReturn(columns);

        for (final int count : new int[] { 0, 2 }) {
            when(columns.size()).thenReturn(count);
            assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toList(resultSet, String.class));
        }

        verify(resultSet, never()).all();
    }

    @Test
    void legacyScalarColumnCountIsValidatedBeforeFetchingRows() {
        final com.datastax.driver.core.ResultSet resultSet = mock(com.datastax.driver.core.ResultSet.class);
        final com.datastax.driver.core.ColumnDefinitions columns = mock(com.datastax.driver.core.ColumnDefinitions.class);
        when(resultSet.getColumnDefinitions()).thenReturn(columns);

        for (final int count : new int[] { 0, 2 }) {
            when(columns.size()).thenReturn(count);
            assertThrows(IllegalArgumentException.class,
                    () -> com.landawn.abacus.da.cassandra.v3.CassandraExecutor.toList(resultSet, String.class));
        }

        verify(resultSet, never()).all();
    }

    @Test
    void allEntitiesAreValidatedBeforeInvokingAnyKeyGetter() {
        final KeyEntity entity = new KeyEntity();

        assertThrows(IllegalArgumentException.class,
                () -> CassandraExecutorBase.entityToCondition(KeyEntity.class, Arrays.asList(entity, null)));

        assertEquals(0, entity.getterCalls);
    }

    public static class KeyEntity {
        @Id
        private long id = 1;
        private int getterCalls;

        public long getId() {
            getterCalls++;
            return id;
        }

        public void setId(final long id) {
            this.id = id;
        }
    }
}
