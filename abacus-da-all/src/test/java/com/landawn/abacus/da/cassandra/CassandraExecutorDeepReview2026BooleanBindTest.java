package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

/**
 * Bind-path half of the <i>Boolean Conversion</i> contract documented on {@link CassandraExecutorBase} and {@link CassandraExecutor}:
 * a bind value for a {@code boolean} column is converted by abacus-common's strict rule, and a value the rule rejects is
 * bound unchanged, so the driver's {@link CodecNotFoundException} is what the caller sees. Driver objects are mocked; the
 * executor's parameter conversion is real.
 */
class CassandraExecutorDeepReview2026BooleanBindTest {

    private static final String UPDATE = "UPDATE flags SET flag = ? WHERE id = 1";

    @Test
    void acceptedValuesAreConvertedAndRejectedValuesAreBoundUnchanged() {
        try (Fixture fixture = new Fixture()) {
            fixture.executor.execute(UPDATE, 1);
            fixture.executor.execute(UPDATE, (short) 0);
            fixture.executor.execute(UPDATE, -1);
            fixture.executor.execute(UPDATE, " Y ");
            fixture.executor.execute(UPDATE, "FALSE");
            fixture.executor.execute(UPDATE, "");
            fixture.executor.execute(UPDATE, "   ");
            fixture.executor.execute(UPDATE, Boolean.TRUE);
            // Rejected by the rule: kept as-is for the driver to refuse.
            fixture.executor.execute(UPDATE, 1L);
            fixture.executor.execute(UPDATE, "yes");
            fixture.executor.execute(UPDATE, 2);
            fixture.executor.execute(UPDATE, 1.0d);

            final List<Object> bound = fixture.boundValues();

            assertAll(() -> assertEquals(12, bound.size()), () -> assertEquals(Boolean.TRUE, bound.get(0)), () -> assertEquals(Boolean.FALSE, bound.get(1)),
                    () -> assertEquals(Boolean.FALSE, bound.get(2)), () -> assertEquals(Boolean.TRUE, bound.get(3)),
                    () -> assertEquals(Boolean.FALSE, bound.get(4)),
                    // Blank text is false (for a Boolean target too), not a failure.
                    () -> assertEquals(Boolean.FALSE, bound.get(5)), () -> assertEquals(Boolean.FALSE, bound.get(6)),
                    () -> assertEquals(Boolean.TRUE, bound.get(7)), () -> assertEquals(1L, bound.get(8)), () -> assertEquals("yes", bound.get(9)),
                    () -> assertEquals(2, bound.get(10)), () -> assertEquals(1.0d, bound.get(11)));
        }
    }

    @Test
    void driverCodecFailureForARejectedValuePropagates() {
        try (Fixture fixture = new Fixture()) {
            final CodecNotFoundException failure = new CodecNotFoundException(DataTypes.BOOLEAN, GenericType.of(Long.class));
            when(fixture.prepared.bind(any(Object[].class))).thenThrow(failure);

            assertAll(() -> assertSame(failure, assertThrows(CodecNotFoundException.class, () -> fixture.executor.execute(UPDATE, 1L))),
                    () -> assertSame(failure, assertThrows(CodecNotFoundException.class, () -> fixture.executor.execute(UPDATE, "yes"))),
                    () -> assertSame(failure, assertThrows(CodecNotFoundException.class, () -> fixture.executor.async().execute(UPDATE, 1L))));
        }
    }

    /** One {@code boolean} bind variable; every {@code bind(...)} call records the values it receives. */
    private static final class Fixture implements AutoCloseable {
        final CqlSession session = mock(CqlSession.class);
        final PreparedStatement prepared = mock(PreparedStatement.class);
        final List<Object[]> binds = new ArrayList<>();
        final CassandraExecutor executor;

        Fixture() {
            final DriverContext context = mock(DriverContext.class);
            when(session.getContext()).thenReturn(context);
            when(context.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT);
            final ColumnDefinitions variables = mock(ColumnDefinitions.class);
            final ColumnDefinition flag = mock(ColumnDefinition.class);
            when(variables.size()).thenReturn(1);
            when(variables.get(0)).thenReturn(flag);
            when(flag.getName()).thenReturn(CqlIdentifier.fromInternal("flag"));
            when(flag.getType()).thenReturn(DataTypes.BOOLEAN);
            when(prepared.getVariableDefinitions()).thenReturn(variables);
            when(prepared.bind(any(Object[].class))).thenAnswer(invocation -> {
                binds.add(((Object[]) invocation.getRawArguments()[0]).clone());
                return mock(BoundStatement.class);
            });
            when(session.prepare(anyString())).thenReturn(prepared);
            when(session.execute(any(Statement.class))).thenReturn(mock(ResultSet.class));
            executor = new CassandraExecutor(session);
        }

        List<Object> boundValues() {
            final List<Object> values = new ArrayList<>(binds.size());

            for (final Object[] bind : binds) {
                assertEquals(1, bind.length);
                values.add(bind[0]);
            }

            return values;
        }

        @Override
        public void close() {
            executor.close();
        }
    }
}
