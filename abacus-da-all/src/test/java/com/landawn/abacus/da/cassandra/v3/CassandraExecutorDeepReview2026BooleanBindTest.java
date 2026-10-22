package com.landawn.abacus.da.cassandra.v3;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.google.common.reflect.TypeToken;

/**
 * Bind-path half of the <i>Boolean Conversion</i> contract documented on {@link CassandraExecutor}: a bind value for a
 * {@code boolean} column is converted by abacus-common's strict rule, and a value the rule rejects is bound unchanged, so the
 * driver's {@link CodecNotFoundException} is what the caller sees (v4 twin:
 * {@code cassandra.CassandraExecutorDeepReview2026BooleanBindTest}). Driver objects are mocked; parameter conversion is real.
 */
class CassandraExecutorDeepReview2026BooleanBindTest {

    private static final String UPDATE = "UPDATE flags SET flag = ? WHERE id = 1";

    @Test
    void acceptedValuesAreConvertedAndRejectedValuesAreBoundUnchanged() {
        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            fixture.columns(DataType.cboolean(), "flag");

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

            final List<Object> bound = new ArrayList<>(fixture.values.size());

            for (final Object[] bind : fixture.values) {
                assertEquals(1, bind.length);
                bound.add(bind[0]);
            }

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
        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            fixture.columns(DataType.cboolean(), "flag");
            final CodecNotFoundException failure = new CodecNotFoundException("no codec", DataType.cboolean(), TypeToken.of(Long.class));
            when(fixture.prepared.bind(any(Object[].class))).thenThrow(failure);

            assertAll(() -> assertSame(failure, assertThrows(CodecNotFoundException.class, () -> fixture.executor.execute(UPDATE, 1L))),
                    () -> assertSame(failure, assertThrows(CodecNotFoundException.class, () -> fixture.executor.execute(UPDATE, "yes"))),
                    () -> assertSame(failure, assertThrows(CodecNotFoundException.class, () -> fixture.executor.async().execute(UPDATE, 1L))));
        }
    }
}
