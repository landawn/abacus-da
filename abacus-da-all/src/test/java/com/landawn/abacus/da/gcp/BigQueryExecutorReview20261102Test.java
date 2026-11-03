package com.landawn.abacus.da.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.annotation.NonUpdatable;
import com.landawn.abacus.annotation.ReadOnly;
import com.landawn.abacus.annotation.Table;

/**
 * Review 2026-11-02 regressions for {@link BigQueryExecutor}.
 */
public class BigQueryExecutorReview20261102Test extends BigQueryExecutorTestSupport {

    private static final long MILLIS = 1_718_900_000_123L;

    @Table("ds.events")
    public static class CalendarEvent {
        @Id
        private String id;
        private Calendar happenedAt;
        @ReadOnly
        private String computed;
        @NonUpdatable
        private String createdBy;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public Calendar getHappenedAt() {
            return happenedAt;
        }

        public void setHappenedAt(final Calendar happenedAt) {
            this.happenedAt = happenedAt;
        }

        public String getComputed() {
            return computed;
        }

        public void setComputed(final String computed) {
            this.computed = computed;
        }

        public String getCreatedBy() {
            return createdBy;
        }

        public void setCreatedBy(final String createdBy) {
            this.createdBy = createdBy;
        }
    }

    private static GregorianCalendar calendar() {
        final GregorianCalendar calendar = new GregorianCalendar(TimeZone.getTimeZone("America/Los_Angeles"));
        calendar.setTimeInMillis(MILLIS);
        return calendar;
    }

    private List<QueryParameterValue> capturedParameters() throws Exception {
        final ArgumentCaptor<QueryJobConfiguration> captor = ArgumentCaptor.forClass(QueryJobConfiguration.class);
        verify(mockBigQuery).query(captor.capture());
        return captor.getValue().getPositionalParameters();
    }

    // A Calendar fell through to the STRING fallback, whose text ("2024-06-20T09:13:20.123-07:00[America/Los_Angeles]") BigQuery
    // cannot coerce to TIMESTAMP. It now binds like java.util.Date: a TIMESTAMP of its instant.
    @Test
    public void calendarParameterBindsAsTimestamp() {
        final QueryParameterValue expected = QueryParameterValue.timestamp(MILLIS * 1000L);

        for (final Calendar value : Arrays.asList(calendar(), Calendar.getInstance(TimeZone.getTimeZone("UTC")))) {
            value.setTimeInMillis(MILLIS);
            final QueryParameterValue actual = BigQueryExecutor.buildQueryParameterValue(value).get(0);

            assertEquals(StandardSQLTypeName.TIMESTAMP, actual.getType());
            assertEquals(expected.getValue(), actual.getValue());
        }

        // Same instant as the java.util.Date binding.
        assertEquals(BigQueryExecutor.buildQueryParameterValue(new java.util.Date(MILLIS)).get(0).getValue(),
                BigQueryExecutor.buildQueryParameterValue(calendar()).get(0).getValue());
    }

    // The round trip the fix is for: a TIMESTAMP cell is read into a Calendar property, and update(entity) writes it back.
    @Test
    public void calendarPropertyReadFromTimestampIsWrittenBackAsTimestamp() throws Exception {
        final FieldList fields = FieldList.of(Field.of("id", StandardSQLTypeName.STRING), Field.of("happened_at", StandardSQLTypeName.TIMESTAMP));
        final FieldValueList row = FieldValueList.of(Arrays.asList(verifyCell("e1"), verifyCell("1718900000.123456")), fields);
        final CalendarEvent event = BigQueryExecutor.toEntity(fields, row, CalendarEvent.class);

        assertEquals(MILLIS, event.getHappenedAt().getTimeInMillis());

        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);
        executor.update(event);

        final List<QueryParameterValue> params = capturedParameters();
        assertEquals(2, params.size());
        assertEquals(StandardSQLTypeName.TIMESTAMP, params.get(0).getType());
        assertEquals(QueryParameterValue.timestamp(MILLIS * 1000L).getValue(), params.get(0).getValue());
        assertEquals("e1", params.get(1).getValue());
    }

    @Test
    public void calendarPropertyIsInsertedAsTimestamp() throws Exception {
        final CalendarEvent event = new CalendarEvent();
        event.setId("e2");
        event.setHappenedAt(calendar());

        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);
        executor.insert(event);

        final List<QueryParameterValue> params = capturedParameters();
        assertEquals(StandardSQLTypeName.STRING, params.get(0).getType());
        assertEquals(StandardSQLTypeName.TIMESTAMP, params.get(1).getType());
        assertEquals(QueryParameterValue.timestamp(MILLIS * 1000L).getValue(), params.get(1).getValue());
    }

    // Pins the documented fallback for value wrappers: the text of the content, or a STRING NULL for an empty wrapper.
    @Test
    public void valueWrappersBindAsStringOfTheirContent() {
        final List<QueryParameterValue> values = BigQueryExecutor.buildQueryParameterValue(java.util.Optional.of(5L),
                com.landawn.abacus.util.u.Optional.of("x"), java.util.Optional.empty(), com.landawn.abacus.util.u.Optional.empty(),
                com.landawn.abacus.util.u.Nullable.empty(), java.math.BigInteger.TEN);

        for (final QueryParameterValue value : values) {
            assertEquals(StandardSQLTypeName.STRING, value.getType());
        }

        assertEquals("5", values.get(0).getValue());
        assertEquals("x", values.get(1).getValue());
        assertNull(values.get(2).getValue());
        assertNull(values.get(3).getValue());
        assertNull(values.get(4).getValue());
        assertEquals("10", values.get(5).getValue());
    }

    // Pins the documented JSON binding of collections and arrays.
    @Test
    public void collectionsAndArraysBindAsJson() {
        final List<QueryParameterValue> values = BigQueryExecutor.buildQueryParameterValue(List.of(1, 2), new String[] { "a" }, new int[] { 3 });

        for (final QueryParameterValue value : values) {
            assertEquals(StandardSQLTypeName.JSON, value.getType());
        }
    }

    // Pins the documented update(entity) contract: non-null values only in non-updatable properties leave nothing to update.
    @Test
    public void updateWithOnlyNonUpdatableValuesIsRejectedBeforeAnyQuery() throws Exception {
        final CalendarEvent readOnlyOnly = new CalendarEvent();
        readOnlyOnly.setId("e3");
        readOnlyOnly.setComputed("c");

        final CalendarEvent nonUpdatableOnly = new CalendarEvent();
        nonUpdatableOnly.setId("e3");
        nonUpdatableOnly.setCreatedBy("me");

        assertThrows(IllegalArgumentException.class, () -> executor.update(readOnlyOnly));
        assertThrows(IllegalArgumentException.class, () -> executor.update(nonUpdatableOnly));
        verify(mockBigQuery, never()).query(any(QueryJobConfiguration.class));

        final CalendarEvent keyOnly = new CalendarEvent();
        keyOnly.setId("e3");
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> executor.update(keyOnly));
        assertTrue(e.getMessage().contains("No non-null non-key properties"), e.getMessage());
    }
}
