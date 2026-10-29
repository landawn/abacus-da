package com.landawn.abacus.da.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Time;
import java.util.Locale;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

@ResourceLock(Resources.LOCALE)
class BigQueryTimeParameterRegressionTest {

    @Test
    void timeParametersUseAsciiDigitsRegardlessOfDefaultLocale() {
        final Locale original = Locale.getDefault(Locale.Category.FORMAT);

        try {
            Locale.setDefault(Locale.Category.FORMAT, Locale.forLanguageTag("ar-EG"));

            for (final int millis : new int[] { 0, 7, 123, 999 }) {
                final Time time = Time.valueOf("12:34:56");
                time.setTime(time.getTime() + millis);
                final String expected = String.format(Locale.ROOT, "12:34:56.%03d000", millis);

                assertEquals(expected, BigQueryExecutor.buildQueryParameterValue(time).get(0).getValue());
            }
        } finally {
            Locale.setDefault(Locale.Category.FORMAT, original);
        }
    }

    @Test
    void preEpochTimeParametersKeepMillisecondsInLocalizedJvm() {
        final Locale original = Locale.getDefault(Locale.Category.FORMAT);

        try {
            Locale.setDefault(Locale.Category.FORMAT, Locale.forLanguageTag("ar-EG"));
            final long[] epochs = { -1001L, -1000L, -993L, -1L, 0L };
            final String[] fractions = { "999000", "000000", "007000", "999000", "000000" };

            for (int i = 0; i < epochs.length; i++) {
                final Time time = new Time(epochs[i]);
                assertEquals(time + "." + fractions[i], BigQueryExecutor.buildQueryParameterValue(time).get(0).getValue());
                assertEquals(epochs[i], time.getTime());
            }
        } finally {
            Locale.setDefault(Locale.Category.FORMAT, original);
        }
    }

    // ---- 2026-10-27 sliceL ----

    /**
     * {@code com.google.cloud.Date.toString()} formats with the JVM's formatting locale, so under a locale with other digits
     * the DATE parameter text had non-ASCII digits and {@code QueryParameterValue.date} rejected it with
     * IllegalArgumentException: binding any {@code com.google.cloud.Date} failed. RED on HEAD for every locale.
     */
    @Test
    void googleCloudDateParametersUseAsciiDigitsRegardlessOfDefaultLocale_sliceL() {
        final Locale original = Locale.getDefault(Locale.Category.FORMAT);

        try {
            final java.util.List<org.junit.jupiter.api.function.Executable> checks = new java.util.ArrayList<>();

            for (final String tag : new String[] { "ar-EG", "fa-IR", "th-TH-u-nu-thai", "hi-IN-u-nu-deva" }) {
                checks.add(() -> {
                    Locale.setDefault(Locale.Category.FORMAT, Locale.forLanguageTag(tag));

                    final com.google.cloud.bigquery.QueryParameterValue value = BigQueryExecutor
                            .buildQueryParameterValue(com.google.cloud.Date.fromYearMonthDay(2024, 1, 5))
                            .get(0);

                    assertEquals("2024-01-05", value.getValue(), tag);
                    assertEquals(com.google.cloud.bigquery.StandardSQLTypeName.DATE, value.getType(), tag);
                });
            }

            checks.add(() -> {
                Locale.setDefault(Locale.Category.FORMAT, Locale.forLanguageTag("ar-EG"));
                assertEquals("0099-12-31", BigQueryExecutor.buildQueryParameterValue(com.google.cloud.Date.fromYearMonthDay(99, 12, 31)).get(0).getValue());
            });

            org.junit.jupiter.api.Assertions.assertAll(checks);
        } finally {
            Locale.setDefault(Locale.Category.FORMAT, original);
        }
    }

    /**
     * Review 2026-10-27 r2E: guards the locale fix against over-reach. Under an ASCII-digit locale the DATE parameter text is exactly
     * {@code com.google.cloud.Date.toString()} (which the parameter used before), at the year bounds too, and a five-digit year is
     * still rejected by {@code QueryParameterValue.date} (com.google.cloud.Date accepts it; BigQuery's DATE ends at 9999).
     */
    @Test
    void googleCloudDateParameterTextIsUnchangedUnderAsciiLocales_r2E() {
        final Locale original = Locale.getDefault(Locale.Category.FORMAT);

        try {
            final java.util.List<org.junit.jupiter.api.function.Executable> checks = new java.util.ArrayList<>();

            for (final Locale locale : new Locale[] { Locale.US, Locale.ROOT }) {
                for (final int[] ymd : new int[][] { { 1, 1, 1 }, { 999, 12, 31 }, { 2024, 2, 29 }, { 9999, 12, 31 } }) {
                    checks.add(() -> {
                        Locale.setDefault(Locale.Category.FORMAT, locale);
                        final com.google.cloud.Date date = com.google.cloud.Date.fromYearMonthDay(ymd[0], ymd[1], ymd[2]);

                        assertEquals(date.toString(), BigQueryExecutor.buildQueryParameterValue(date).get(0).getValue(), locale + " " + date);
                    });
                }
            }

            checks.add(() -> {
                Locale.setDefault(Locale.Category.FORMAT, Locale.US);
                org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                        () -> BigQueryExecutor.buildQueryParameterValue(com.google.cloud.Date.fromYearMonthDay(10000, 1, 1)));
            });

            org.junit.jupiter.api.Assertions.assertAll(checks);
        } finally {
            Locale.setDefault(Locale.Category.FORMAT, original);
        }
    }
}
