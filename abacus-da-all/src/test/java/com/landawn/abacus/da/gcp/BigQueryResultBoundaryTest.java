package com.landawn.abacus.da.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.JobException;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.landawn.abacus.util.N;

class BigQueryResultBoundaryTest extends BigQueryExecutorTestSupport {
    enum QueryOperation {
        EXECUTE, TYPED_STREAM, RAW_STREAM
    }

    @ParameterizedTest
    @EnumSource(QueryOperation.class)
    void failedJobsPreserveTheDriverException(final QueryOperation operation) throws Exception {
        // The SDK's JobException constructor is package-private.
        final JobException failure = mock(JobException.class);
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenThrow(failure);
        final QueryJobConfiguration configuration = QueryJobConfiguration.newBuilder("SELECT value FROM items").build();
        assertSame(failure, assertThrows(JobException.class, () -> {
            switch (operation) {
                case EXECUTE -> executor.execute(configuration.getQuery());
                case TYPED_STREAM -> executor.stream(String.class, configuration);
                case RAW_STREAM -> executor.stream(configuration);
            }
        }));
    }

    @ParameterizedTest
    @ValueSource(longs = { 0, 3 })
    void nonNullSingleValueIsEmptyForNoRowsOrDmlCounts(final long reportedRows) throws Exception {
        when(mockTableResult.getTotalRows()).thenReturn(reportedRows);
        when(mockTableResult.getValues()).thenReturn(List.of());
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);
        assertFalse(executor.queryForSingleNonNull(Long.class, "SELECT value FROM items").isPresent());
    }

    @Test
    void nonNullSingleValueDistinguishesSqlNullFromPrimitiveDefaults() throws Exception {
        final FieldList fields = FieldList.of(Field.of("value", StandardSQLTypeName.INT64));
        final FieldValueList row = FieldValueList.of(List.of(FieldValue.of(FieldValue.Attribute.PRIMITIVE, null)), fields);
        fixBqStubResult(fields, row);
        assertThrows(NullPointerException.class, () -> executor.queryForSingleNonNull(Long.class, "SELECT value FROM items"));
        assertEquals(0L, executor.queryForSingleNonNull(long.class, "SELECT value FROM items").get());
        assertThrows(IllegalArgumentException.class, () -> executor.queryForSingleNonNull(null, "SELECT value FROM items"));
    }

    @Test
    void registeredRowConverterRejectsMultipleColumnsForAScalar() {
        // Initialize the executor's public FieldValueList converter through the fixture.
        final FieldList fields = FieldList.of(Field.of("first", StandardSQLTypeName.INT64), Field.of("second", StandardSQLTypeName.INT64));
        final FieldValueList row = FieldValueList.of(List.of(verifyCell("1"), verifyCell("2")), fields);
        assertThrows(IllegalArgumentException.class, () -> N.convert(row, Long.class));
    }
}
