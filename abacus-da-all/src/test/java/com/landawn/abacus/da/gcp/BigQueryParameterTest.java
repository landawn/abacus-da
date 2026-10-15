package com.landawn.abacus.da.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.StandardSQLTypeName;

public class BigQueryParameterTest extends BigQueryExecutorTestSupport {

    //    @Test
    //    public void testExecuteWithExceptionHandling() throws Exception {
    //        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenThrow(new JobException("Test exception"));
    //
    //        assertThrows(RuntimeException.class, () -> {
    //            executor.execute("SELECT * FROM test_table", new Object[0]);
    //        });
    //    }

    @Test
    public void testBuildQueryParameterValueWithDifferentTypes() {
        Object[] parameters = new Object[] { "string", true, 'c', (byte) 1, (short) 2, 3, 4L, 5.0f, 6.0d, new BigDecimal("7.0"), new java.util.Date(),
                new byte[] { 1, 2, 3 } };

        List<QueryParameterValue> values = BigQueryExecutor.buildQueryParameterValue(parameters);

        assertNotNull(values);
        assertEquals(parameters.length, values.size());
    }

    @Test
    public void testBuildQueryParameterValueWithEmptyParameters() {
        List<QueryParameterValue> values = BigQueryExecutor.buildQueryParameterValue();

        assertNotNull(values);
        assertEquals(0, values.size());
    }

    @Test
    public void testBuildQueryParameterValueWithComplexTypes() {
        List<String> list = Arrays.asList("a", "b", "c");
        Map<String, Object> map = new HashMap<>();
        map.put("key", "value");
        TestEntity entity = new TestEntity();
        entity.setId(1);

        Object[] parameters = new Object[] { list, map, entity, new String[] { "x", "y" } };

        List<QueryParameterValue> values = BigQueryExecutor.buildQueryParameterValue(parameters);

        assertNotNull(values);
        assertEquals(parameters.length, values.size());
    }

    @Test
    public void testBuildQueryParameterValueRejectsUntypedNull() {
        assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.buildQueryParameterValue("abc", null));
    }

    @Test
    public void testBuildQueryParameterValueAcceptsTypedNull() {
        final QueryParameterValue typedNull = QueryParameterValue.int64((Long) null);

        List<QueryParameterValue> values = BigQueryExecutor.buildQueryParameterValue(typedNull);

        assertEquals(1, values.size());
        assertTrue(values.get(0) == typedNull);
    }

    // ---------- buildQueryParameterValue: additional type coverage ----------

    @Test
    public void testBuildQueryParameterValueWithSqlTypes() {
        // Drives the java.sql.Date / Time / Timestamp branches and com.google.cloud.Timestamp.
        Object[] params = new Object[] { java.sql.Date.valueOf("2024-01-01"), java.sql.Time.valueOf("12:34:56"), new java.sql.Timestamp(1000L),
                com.google.cloud.Timestamp.ofTimeSecondsAndNanos(1L, 0) };

        List<QueryParameterValue> values = BigQueryExecutor.buildQueryParameterValue(params);
        assertEquals(4, values.size());
        assertEquals(StandardSQLTypeName.DATE, values.get(0).getType());
        assertEquals("2024-01-01", values.get(0).getValue());
        assertEquals(StandardSQLTypeName.TIME, values.get(1).getType());
        assertEquals("12:34:56.000000", values.get(1).getValue());
        assertEquals(StandardSQLTypeName.TIMESTAMP, values.get(2).getType());
        assertEquals(QueryParameterValue.timestamp(1_000_000L).getValue(), values.get(2).getValue());
        assertEquals(QueryParameterValue.timestamp(1_000_000L).getValue(), values.get(3).getValue());
    }

    @Test
    public void testBuildQueryParameterValueWithUtilDateUsesTimestampMicros() {
        final List<QueryParameterValue> values = BigQueryExecutor.buildQueryParameterValue(new java.util.Date(1000L));

        assertEquals(1, values.size());
        assertEquals(StandardSQLTypeName.TIMESTAMP, values.get(0).getType());
        assertEquals(QueryParameterValue.timestamp(1_000_000L).getValue(), values.get(0).getValue());
    }

    /**
     * Regression: binding a com.google.cloud.Timestamp previously went through toDate().getTime(),
     * which truncates to millisecond precision — the microsecond component that BigQuery TIMESTAMP
     * carries was silently dropped (...123456 µs became ...123000 µs), so a WHERE equality on a value
     * read back from BigQuery could fail to match. The binding now computes
     * seconds * 1_000_000 + nanos / 1_000.
     */
    @Test
    public void testBuildQueryParameterValueWithGoogleTimestampPreservesMicros() {
        final com.google.cloud.Timestamp ts = com.google.cloud.Timestamp.ofTimeSecondsAndNanos(1_700_000_000L, 123_456_000);

        final List<QueryParameterValue> values = BigQueryExecutor.buildQueryParameterValue(ts);

        assertEquals(1, values.size());
        assertEquals(StandardSQLTypeName.TIMESTAMP, values.get(0).getType());
        // 1_700_000_000 s + 123_456_000 ns == 1_700_000_000_123_456 µs (not ...123_000 µs).
        assertEquals(QueryParameterValue.timestamp(1_700_000_000_123_456L).getValue(), values.get(0).getValue());
    }

    @Test
    public void testBuildQueryParameterValueWithGoogleDate() {
        Object[] params = new Object[] { com.google.cloud.Date.fromYearMonthDay(2024, 1, 1) };
        List<QueryParameterValue> values = BigQueryExecutor.buildQueryParameterValue(params);
        assertEquals(1, values.size());
    }

    @Test
    public void testBuildQueryParameterValue_NullParametersArray() {
        // Passing a null array is treated as no parameters.
        List<QueryParameterValue> values = BigQueryExecutor.buildQueryParameterValue((Object[]) null);
        assertNotNull(values);
        assertEquals(0, values.size());
    }

    @Test
    public void testBuildQueryParameterValue_ByteBufferBindsRemainingBytes() {
        final java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(new byte[] { 9, 1, 2, 3 });
        buffer.get(); // position 1: the remaining bytes are {1, 2, 3}

        final List<QueryParameterValue> values = BigQueryExecutor.buildQueryParameterValue(buffer);

        assertEquals(StandardSQLTypeName.BYTES, values.get(0).getType());
        assertEquals(QueryParameterValue.bytes(new byte[] { 1, 2, 3 }).getValue(), values.get(0).getValue());
        assertEquals(1, buffer.position()); // not consumed
    }
}
