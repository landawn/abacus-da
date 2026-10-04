package com.landawn.abacus.da.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableResult;
import com.landawn.abacus.da.TestBase;

@Tag("base-test")
public class BigQueryScalarMappingRegressionTest extends TestBase {

    @Test
    void binaryScalarWithBeanAccessorsIsDecodedForListsAndStreams() throws Exception {
        final TableResult result = scalarResult(StandardSQLTypeName.BYTES, "AQID");
        final BigQuery client = mock(BigQuery.class);
        when(client.query(any(QueryJobConfiguration.class))).thenReturn(result);
        final BigQueryExecutor executor = new BigQueryExecutor(client);
        final ByteBuffer expected = ByteBuffer.wrap(new byte[] { 1, 2, 3 });

        assertEquals(expected, BigQueryExecutor.toList(result, ByteBuffer.class).get(0));
        assertEquals(expected, executor.stream(ByteBuffer.class, "SELECT payload FROM t").toList().get(0));
        assertEquals(expected, executor.stream(ByteBuffer.class, QueryJobConfiguration.of("SELECT payload FROM t")).toList().get(0));
    }

    @Test
    void calendarScalarWithBeanAccessorsUsesReturnedTimestamp() {
        final TableResult result = scalarResult(StandardSQLTypeName.TIMESTAMP, "1718900000.123456");

        assertEquals(1_718_900_000_123L, BigQueryExecutor.toList(result, GregorianCalendar.class).get(0).getTimeInMillis());
    }

    private static TableResult scalarResult(final StandardSQLTypeName type, final String value) {
        return scalarResult("payload", type, value);
    }

    @Test
    void datasetScalarHintsKeepValuesRawEvenWhenColumnNamesMatchAccessors() throws Exception {
        final BigQuery client = mock(BigQuery.class);
        final BigQueryExecutor executor = new BigQueryExecutor(client);

        for (final String column : List.of("position", "limit", "time", "timeInMillis")) {
            final TableResult result = scalarResult(column, StandardSQLTypeName.INT64, "5");
            when(client.query(any(QueryJobConfiguration.class))).thenReturn(result);

            for (final Class<?> target : Arrays.asList(ByteBuffer.class, GregorianCalendar.class, Map.class, null)) {
                assertEquals(List.of("5"), BigQueryExecutor.extractData(result, target).getColumn(column), column + ": " + target);
                assertEquals(List.of("5"), executor.query(target, "SELECT " + column + " FROM t").getColumn(column), column + ": " + target);
            }
        }
    }

    @Test
    void datasetBeanHintStillConvertsMatchingProperties() {
        final TableResult result = scalarResult("position", StandardSQLTypeName.INT64, "5");

        assertEquals(List.of(5), BigQueryExecutor.extractData(result, Position.class).getColumn("position"));
    }

    public record Position(int position) {
    }

    private static TableResult scalarResult(final String name, final StandardSQLTypeName type, final String value) {
        final FieldList fields = FieldList.of(Field.of(name, type));
        final FieldValueList row = FieldValueList.of(List.of(FieldValue.of(FieldValue.Attribute.PRIMITIVE, value)), fields);
        final TableResult result = mock(TableResult.class);
        when(result.getSchema()).thenReturn(Schema.of(fields));
        when(result.getTotalRows()).thenReturn(1L);
        when(result.iterateAll()).thenReturn(List.of(row));
        return result;
    }
}
