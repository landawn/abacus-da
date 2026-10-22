package com.landawn.abacus.da.gcp;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.landawn.abacus.da.TestBase;

public class BigQueryExecutorDeepReview2026MappingTest extends TestBase {

    @Test
    public void dottedPropertiesUseTerminalPropertyMetadataForBigQueryValues() {
        final FieldList locationFields = FieldList.of(Field.of("lat", StandardSQLTypeName.INT64), Field.of("lng", StandardSQLTypeName.INT64));
        final FieldList boxFields = FieldList.of(Field.of("value", StandardSQLTypeName.INT64));
        final FieldList fields = FieldList.of(Field.of("nested.count", StandardSQLTypeName.INT64), Field.of("nested.seen_at", StandardSQLTypeName.TIMESTAMP),
                Field.of("nested.data", StandardSQLTypeName.BYTES),
                Field.newBuilder("nested.codes", StandardSQLTypeName.INT64).setMode(Field.Mode.REPEATED).build(),
                Field.newBuilder("nested.location", StandardSQLTypeName.STRUCT, locationFields).build(),
                Field.newBuilder("nested.boxed", StandardSQLTypeName.STRUCT, boxFields).build());
        final FieldValueList row = FieldValueList.of(Arrays.asList(primitive("7"), primitive("1718900000.123456"), primitive("AQID"),
                repeated(primitive("1"), primitive("2")), record(primitive("10"), primitive("20")), record(primitive("9"))), fields);

        final DottedRoot result = BigQueryExecutor.toEntity(fields, row, DottedRoot.class);

        assertNotNull(result.getNested());
        assertEquals(7L, result.getNested().getCount());
        assertEquals(java.time.Instant.ofEpochSecond(1_718_900_000L, 123_456_000L), result.getNested().getSeenAt());
        assertArrayEquals(new byte[] { 1, 2, 3 }, result.getNested().getData());
        assertEquals(Arrays.asList(1L, 2L), result.getNested().getCodes());
        assertEquals(10L, result.getNested().getLocation().getLat());
        assertEquals(20L, result.getNested().getLocation().getLng());
        assertEquals(Long.valueOf(9), result.getNested().getBoxed().getValue());
    }

    @Test
    public void serializeOnlyReadOnlyPropertyWithBackingFieldIsSkipped() {
        final FieldList fields = FieldList.of(Field.of("id", StandardSQLTypeName.INT64), Field.of("computed", StandardSQLTypeName.STRING));
        final FieldValueList row = FieldValueList.of(Arrays.asList(primitive("7"), primitive("from query")), fields);

        // Without @Entity on the bean, "computed" would not be a property at all and this test would pass for the wrong reason.
        final com.landawn.abacus.parser.ParserUtil.PropInfo computed = com.landawn.abacus.parser.ParserUtil.getBeanInfo(ReadOnlyWithBackingField.class)
                .getPropInfo("computed");
        assertNotNull(computed);
        org.junit.jupiter.api.Assertions.assertTrue(computed.isReadOnlyProperty);
        assertEquals(com.landawn.abacus.annotation.JsonXmlField.Direction.SERIALIZE_ONLY, computed.jsonXmlExpose);

        final ReadOnlyWithBackingField result = BigQueryExecutor.toEntity(fields, row, ReadOnlyWithBackingField.class);

        assertEquals(7L, result.getId());
        assertEquals("local", result.getComputed());
    }

    // ---- 2026-10-20 ddbfix ----

    /**
     * A scalar column of a property toEntity ignores (read-only, SERIALIZE_ONLY) must not be decoded or converted for the
     * bean-class Dataset either: extractData failed with NumberFormatException on a getter-only Long under a STRING "abc"
     * column, and on a getter-only int under an INT64 "abc" column, both of which toEntity reads without error. The column
     * keeps its raw cell values like an unmatched column (a NULL cell stays null), while writable columns still convert.
     */
    @Test
    public void extractDataKeepsRawCellsOfScalarColumnWhosePropertyToEntityIgnores_ddbfix() {
        final FieldList fields = FieldList.of(Field.of("id", StandardSQLTypeName.INT64), Field.of("computed", StandardSQLTypeName.STRING),
                Field.of("count", StandardSQLTypeName.INT64), Field.of("total", StandardSQLTypeName.INT64));
        final FieldValueList badRow = FieldValueList.of(Arrays.asList(primitive("7"), primitive("abc"), primitive("abc"), primitive("9")), fields);
        final FieldValueList nullRow = FieldValueList.of(Arrays.asList(primitive("8"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, null),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, null), primitive("10")), fields);
        final com.google.cloud.bigquery.TableResult tableResult = org.mockito.Mockito.mock(com.google.cloud.bigquery.TableResult.class);
        org.mockito.Mockito.when(tableResult.getSchema()).thenReturn(com.google.cloud.bigquery.Schema.of(fields));
        org.mockito.Mockito.when(tableResult.getTotalRows()).thenReturn(2L);
        org.mockito.Mockito.when(tableResult.iterateAll()).thenReturn(List.of(badRow, nullRow));

        final GetterOnlyScalars entity = BigQueryExecutor.toEntity(fields, badRow, GetterOnlyScalars.class);
        final com.landawn.abacus.util.Dataset dataset = BigQueryExecutor.extractData(tableResult, GetterOnlyScalars.class);
        final List<GetterOnlyScalars> listed = BigQueryExecutor.toList(tableResult, GetterOnlyScalars.class);

        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals(7L, entity.getId()), () -> assertEquals(Long.valueOf(5), entity.getComputed()),
                () -> assertEquals(3, entity.getCount()), () -> assertEquals(Long.valueOf(9), entity.getTotal()), () -> assertEquals(2, dataset.size()),
                () -> assertEquals(Long.valueOf(7), dataset.get(0, 0)), () -> assertEquals("abc", dataset.get(0, 1)),
                () -> assertEquals("abc", dataset.get(0, 2)), () -> assertEquals(Long.valueOf(9), dataset.get(0, 3)),
                () -> assertEquals(Long.valueOf(8), dataset.get(1, 0)), () -> org.junit.jupiter.api.Assertions.assertNull(dataset.get(1, 1)),
                () -> org.junit.jupiter.api.Assertions.assertNull(dataset.get(1, 2)), () -> assertEquals(Long.valueOf(10), dataset.get(1, 3)),
                () -> assertEquals(2, listed.size()), () -> assertEquals(Long.valueOf(5), listed.get(0).getComputed()),
                () -> assertEquals(3, listed.get(1).getCount()), () -> assertEquals(Long.valueOf(10), listed.get(1).getTotal()));
    }

    // ---- 2026-10-20 sliceF ----

    /**
     * Pins the extractData doc's parenthetical for a STRUCT cell: a STRUCT under a column whose property toEntity ignores
     * (getter-only Long "computed") goes down the unmatched-column path and becomes an Object[] of the raw sub-cell values,
     * while toEntity leaves the property untouched.
     */
    @Test
    public void extractDataKeepsStructCellOfGetterOnlyColumnAsObjectArray_sliceF() {
        final FieldList subFields = FieldList.of(Field.of("value", StandardSQLTypeName.INT64), Field.of("label", StandardSQLTypeName.STRING));
        final FieldList fields = FieldList.of(Field.of("id", StandardSQLTypeName.INT64),
                Field.newBuilder("computed", StandardSQLTypeName.STRUCT, subFields).build(), Field.of("total", StandardSQLTypeName.INT64));
        final FieldValueList row = FieldValueList.of(Arrays.asList(primitive("7"), record(primitive("9"), primitive("nine")), primitive("11")), fields);
        final com.google.cloud.bigquery.TableResult tableResult = org.mockito.Mockito.mock(com.google.cloud.bigquery.TableResult.class);
        org.mockito.Mockito.when(tableResult.getSchema()).thenReturn(com.google.cloud.bigquery.Schema.of(fields));
        org.mockito.Mockito.when(tableResult.getTotalRows()).thenReturn(1L);
        org.mockito.Mockito.when(tableResult.iterateAll()).thenReturn(List.of(row));

        final GetterOnlyScalars entity = BigQueryExecutor.toEntity(fields, row, GetterOnlyScalars.class);
        final com.landawn.abacus.util.Dataset dataset = BigQueryExecutor.extractData(tableResult, GetterOnlyScalars.class);
        final Object structCell = dataset.get(0, 1);

        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals(Long.valueOf(5), entity.getComputed()),
                () -> assertEquals(Long.valueOf(11), entity.getTotal()), () -> assertEquals(1, dataset.size()),
                () -> assertEquals(Long.valueOf(7), dataset.get(0, 0)),
                () -> org.junit.jupiter.api.Assertions.assertTrue(structCell instanceof Object[], () -> "expected Object[] but was " + structCell),
                () -> assertArrayEquals(new Object[] { "9", "nine" }, (Object[]) structCell), () -> assertEquals(Long.valueOf(11), dataset.get(0, 2)));
    }

    /** Getter-only (read-only, implicitly SERIALIZE_ONLY) scalar properties next to writable ones. */
    @com.landawn.abacus.annotation.Entity
    public static class GetterOnlyScalars {
        private long id;
        @com.landawn.abacus.annotation.AccessFieldByMethod
        private Long computed = 5L;
        @com.landawn.abacus.annotation.AccessFieldByMethod
        private int count = 3;
        private Long total;

        public long getId() {
            return id;
        }

        public void setId(final long id) {
            this.id = id;
        }

        public Long getComputed() {
            return computed;
        }

        public int getCount() {
            return count;
        }

        public Long getTotal() {
            return total;
        }

        public void setTotal(final Long total) {
            this.total = total;
        }
    }

    @Test
    public void dottedReadOnlyPropertyIsSkippedWhileWritableSiblingIsMapped() {
        final FieldList fields = FieldList.of(Field.of("nested.computed", StandardSQLTypeName.STRING), Field.of("nested.id", StandardSQLTypeName.INT64));
        final FieldValueList row = FieldValueList.of(Arrays.asList(primitive("from query"), primitive("7")), fields);

        final ReadOnlyRoot result = BigQueryExecutor.toEntity(fields, row, ReadOnlyRoot.class);

        assertNotNull(result.getNested());
        assertEquals("local", result.getNested().getComputed());
        assertEquals(7L, result.getNested().getId());
    }

    private static FieldValue primitive(final String value) {
        return FieldValue.of(FieldValue.Attribute.PRIMITIVE, value);
    }

    private static FieldValue repeated(final FieldValue... values) {
        return FieldValue.of(FieldValue.Attribute.REPEATED, FieldValueList.of(Arrays.asList(values)));
    }

    private static FieldValue record(final FieldValue... values) {
        return FieldValue.of(FieldValue.Attribute.RECORD, FieldValueList.of(Arrays.asList(values)));
    }

    public static class DottedRoot {
        private DottedNested nested;

        public DottedNested getNested() {
            return nested;
        }

        public void setNested(final DottedNested nested) {
            this.nested = nested;
        }
    }

    public static class DottedNested {
        private long count;
        private java.time.Instant seenAt;
        private byte[] data;
        private List<Long> codes;
        private DottedLocation location;
        private DottedBox<Long> boxed;

        public long getCount() {
            return count;
        }

        public void setCount(final long count) {
            this.count = count;
        }

        public java.time.Instant getSeenAt() {
            return seenAt;
        }

        public void setSeenAt(final java.time.Instant seenAt) {
            this.seenAt = seenAt;
        }

        public byte[] getData() {
            return data;
        }

        public void setData(final byte[] data) {
            this.data = data;
        }

        public List<Long> getCodes() {
            return codes;
        }

        public void setCodes(final List<Long> codes) {
            this.codes = codes;
        }

        public DottedLocation getLocation() {
            return location;
        }

        public void setLocation(final DottedLocation location) {
            this.location = location;
        }

        public DottedBox<Long> getBoxed() {
            return boxed;
        }

        public void setBoxed(final DottedBox<Long> boxed) {
            this.boxed = boxed;
        }
    }

    public static class DottedLocation {
        private long lat;
        private long lng;

        public long getLat() {
            return lat;
        }

        public void setLat(final long lat) {
            this.lat = lat;
        }

        public long getLng() {
            return lng;
        }

        public void setLng(final long lng) {
            this.lng = lng;
        }
    }

    public static class DottedBox<T> {
        private T value;

        public T getValue() {
            return value;
        }

        public void setValue(final T value) {
            this.value = value;
        }
    }

    public static class ReadOnlyRoot {
        private ReadOnlyWithBackingField nested;

        public ReadOnlyWithBackingField getNested() {
            return nested;
        }

        public void setNested(final ReadOnlyWithBackingField nested) {
            this.nested = nested;
        }
    }

    // @Entity: only an @Entity class registers a getter-only (no setter) field as a property (abacus-common Beans rule).
    @com.landawn.abacus.annotation.Entity
    public static class ReadOnlyWithBackingField {
        private long id;

        @com.landawn.abacus.annotation.AccessFieldByMethod
        private String computed = "local";

        public long getId() {
            return id;
        }

        public void setId(final long id) {
            this.id = id;
        }

        public String getComputed() {
            return computed;
        }
    }
}
