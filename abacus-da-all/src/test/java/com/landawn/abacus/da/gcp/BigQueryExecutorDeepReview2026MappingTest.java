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

        final ReadOnlyWithBackingField result = BigQueryExecutor.toEntity(fields, row, ReadOnlyWithBackingField.class);

        assertEquals(7L, result.getId());
        assertEquals("local", result.getComputed());
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
