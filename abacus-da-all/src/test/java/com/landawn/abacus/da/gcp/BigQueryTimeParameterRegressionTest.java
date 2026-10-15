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
}
