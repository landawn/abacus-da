package com.landawn.abacus.da.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableResult;
import com.landawn.abacus.da.MockitoTestBase;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.IntFunctions;

/**
 * Line coverage for the BigQuery paths that are reachable with the client library on the test classpath.
 */
class BigQueryExecutorLineCoverageTest extends MockitoTestBase {

    private static final String NAME_SQL = "SELECT name FROM dataset.customers WHERE id = 7";

    private static final String STATUS_SQL = "SELECT name FROM dataset.customers WHERE status = ?";

    @Mock
    private BigQuery bigQuery;

    @Mock
    private TableResult tableResult;

    private BigQueryExecutor executor;

    @BeforeEach
    void setUp() {
        executor = new BigQueryExecutor(bigQuery);
    }

    @Test
    void plainRepeatedColumnWithoutSchemaStaysText() throws Exception {
        final FieldList fields = tagsField();
        final FieldValueList row = plainTagsRow(fields);
        when(tableResult.getSchema()).thenReturn(Schema.of(fields));
        when(tableResult.getTotalRows()).thenReturn(1L);
        when(tableResult.iterateAll()).thenReturn(List.of(row));

        final Dataset dataset = BigQueryExecutor.extractData(tableResult, null);

        assertEquals(1, dataset.size());
        assertEquals(List.of("alpha", "beta"), dataset.getColumn("tags").get(0));

        final FieldValueList schemaless = FieldValueList.of(List.of(FieldValue.of(FieldValue.Attribute.REPEATED, Arrays.asList("alpha", "beta"))));
        when(tableResult.getSchema()).thenReturn(null);
        when(tableResult.getValues()).thenReturn(List.of(schemaless));
        when(bigQuery.query(any(QueryJobConfiguration.class))).thenReturn(tableResult);

        final String[] tags = executor.queryForSingleValue(String[].class, "SELECT tags FROM dataset.t").get();

        assertEquals("alpha", tags[0]);
        assertEquals("beta", tags[1]);
        assertEquals(2, tags.length);
        final ArgumentCaptor<QueryJobConfiguration> captor = ArgumentCaptor.forClass(QueryJobConfiguration.class);
        verify(bigQuery).query(captor.capture());
        assertEquals("SELECT tags FROM dataset.t", captor.getValue().getQuery());
        assertEquals(List.of(), captor.getValue().getPositionalParameters());
    }

    @Test
    void toMapKeepsPlainRepeatedElements() {
        final FieldList fields = tagsField();

        final Map<String, Object> map = BigQueryExecutor.toMap(fields, plainTagsRow(fields), IntFunctions.ofLinkedHashMap());

        assertEquals(1, map.size());
        assertEquals(List.of("alpha", "beta"), map.get("tags"));
    }

    @Test
    void nullRowReadsTheDeclaredDefault() throws Exception {
        assertNull(invokeReadRow(null));
        assertNull(invokeReadRow(String.class));
        assertEquals(Integer.valueOf(0), invokeReadRow(int.class));
        assertEquals(Boolean.FALSE, invokeReadRow(boolean.class));
    }

    @Test
    void localDateParameterBindsAsItsText() {
        final LocalDate date = LocalDate.of(2026, 11, 1);

        final List<QueryParameterValue> values = BigQueryExecutor.buildQueryParameterValue(date);

        assertEquals(1, values.size());
        assertEquals(StandardSQLTypeName.STRING, values.get(0).getType());
        assertEquals("2026-11-01", values.get(0).getValue());
    }

    @Test
    void debugEnabledQueriesLogTheSubmittedSql() throws InterruptedException {
        final FieldList fields = FieldList.of(Field.of("name", StandardSQLTypeName.STRING));
        final FieldValueList row = FieldValueList.of(List.of(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Ada")), fields);
        final QueryJobConfiguration config = QueryJobConfiguration.newBuilder(NAME_SQL).build();
        when(tableResult.getSchema()).thenReturn(Schema.of(fields));
        when(tableResult.iterateAll()).thenReturn(List.of(row));
        when(bigQuery.query(any(QueryJobConfiguration.class))).thenReturn(tableResult);

        withDebug(BigQueryExecutor.class, () -> {
            assertEquals(List.of("Ada"), executor.stream(String.class, config).toList());
            final List<FieldValueList> rows = executor.stream(config).toList();
            assertEquals(1, rows.size());
            assertEquals("Ada", rows.get(0).get(0).getValue());
            assertSame(tableResult, executor.execute(STATUS_SQL, "active"));
        });

        final ArgumentCaptor<QueryJobConfiguration> captor = ArgumentCaptor.forClass(QueryJobConfiguration.class);
        verify(bigQuery, times(3)).query(captor.capture());
        assertSame(config, captor.getAllValues().get(0));
        assertSame(config, captor.getAllValues().get(1));
        assertEquals(NAME_SQL, config.getQuery());
        final QueryJobConfiguration executed = captor.getAllValues().get(2);
        assertEquals(STATUS_SQL, executed.getQuery());
        assertEquals(1, executed.getPositionalParameters().size());
        assertEquals(StandardSQLTypeName.STRING, executed.getPositionalParameters().get(0).getType());
        assertEquals("active", executed.getPositionalParameters().get(0).getValue());
    }

    private static FieldList tagsField() {
        return FieldList.of(Field.newBuilder("tags", StandardSQLTypeName.STRING).setMode(Field.Mode.REPEATED).build());
    }

    private static FieldValueList plainTagsRow(final FieldList fields) {
        return FieldValueList.of(List.of(FieldValue.of(FieldValue.Attribute.REPEATED, Arrays.asList("alpha", "beta"))), fields);
    }

    /** {@code readRow}'s null-row contract. No public caller passes a null row. */
    private static Object invokeReadRow(final Class<?> rowClass) throws Exception {
        final Method method = BigQueryExecutor.class.getDeclaredMethod("readRow", FieldList.class, FieldValueList.class, Class.class);
        method.setAccessible(true);

        return method.invoke(null, null, null, rowClass);
    }

    private static void withDebug(final Class<?> type, final Runnable action) {
        final java.util.logging.Logger jul = java.util.logging.Logger.getLogger(type.getName());
        final java.util.logging.Level previous = jul.getLevel();
        jul.setLevel(java.util.logging.Level.FINE);

        try {
            action.run();
        } finally {
            jul.setLevel(previous);
        }
    }
}
