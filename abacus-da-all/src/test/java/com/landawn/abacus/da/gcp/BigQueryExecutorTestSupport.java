package com.landawn.abacus.da.gcp;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableResult;
import com.landawn.abacus.da.MockitoTestBase;

abstract class BigQueryExecutorTestSupport extends MockitoTestBase {

    @Mock
    protected BigQuery mockBigQuery;

    @Mock
    protected TableResult mockTableResult;

    protected BigQueryExecutor executor;

    @BeforeEach
    public void setUp() {
        executor = new BigQueryExecutor(mockBigQuery);
    }

    // ---------------------------------------------------------------------------------------------
    // Regression: DML must generate POSITIONAL ('?') SQL, not named (':name') SQL. execute(...) binds
    // parameters positionally via setPositionalParameters, so a ':name' placeholder cannot be bound and
    // BigQuery rejects the statement. (Bug: insert/update/delete used the named-SQL builders NSC/NAC/NLC
    // instead of the positional PSC/PAC/PLC used by the SELECT path.) The existing DML tests above only
    // verify delegation, which is why this stayed latent.
    // ---------------------------------------------------------------------------------------------

    protected String captureGeneratedSql() throws Exception {
        final ArgumentCaptor<QueryJobConfiguration> captor = ArgumentCaptor.forClass(QueryJobConfiguration.class);
        verify(mockBigQuery).query(captor.capture());
        return captor.getValue().getQuery();
    }

    public static class TestEntity {
        private int id;
        private String name;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static class Visit {
        private String city;
        private long zip;
        private java.time.Instant seenAt;
        private byte[] blob;

        public String getCity() {
            return city;
        }

        public void setCity(final String city) {
            this.city = city;
        }

        public long getZip() {
            return zip;
        }

        public void setZip(final long zip) {
            this.zip = zip;
        }

        public java.time.Instant getSeenAt() {
            return seenAt;
        }

        public void setSeenAt(final java.time.Instant seenAt) {
            this.seenAt = seenAt;
        }

        public byte[] getBlob() {
            return blob;
        }

        public void setBlob(final byte[] blob) {
            this.blob = blob;
        }
    }

    // ---- 2026-10-02 verifyBQ ----
    protected static FieldValue verifyCell(final String value) {
        return FieldValue.of(FieldValue.Attribute.PRIMITIVE, value);
    }

    protected static FieldList verifyVisitFields() {
        return FieldList.of(Field.of("city", StandardSQLTypeName.STRING), Field.of("zip", StandardSQLTypeName.INT64),
                Field.of("seen_at", StandardSQLTypeName.TIMESTAMP));
    }

    protected static Field verifyRepeatedStruct(final String name, final FieldList subFields) {
        return Field.newBuilder(name, StandardSQLTypeName.STRUCT, subFields).setMode(Field.Mode.REPEATED).build();
    }

    public record VisitRecord(String city, long zip, java.time.Instant seenAt) {
    }

    // ---- 2026-10-03 fixBQ ----
    // Payloads shaped exactly as google-cloud-bigquery (2.72.0, FieldValue.fromPb) decodes a result page: a REPEATED cell
    // holds a FieldValueList WITHOUT a schema, and a STRUCT inside a REPEATED value - and every STRUCT nested in it - has no
    // schema either; only a STRUCT outside any REPEATED value carries its sub-schema. Every dispatch site tested
    // `instanceof FieldValueList` before `instanceof List`, so a real REPEATED value was read as a STRUCT and failed ("No
    // schema is attached ..." / "'fields' cannot be null"), and the nested STRUCTs of REPEATED STRUCT bean elements were read
    // with the attached schema they don't have.
    protected static FieldValue fixBqRepeated(final FieldValue... elements) {
        return FieldValue.of(FieldValue.Attribute.REPEATED, FieldValueList.of(Arrays.asList(elements)));
    }

    protected static FieldValue fixBqRecord(final FieldValue... values) {
        return FieldValue.of(FieldValue.Attribute.RECORD, FieldValueList.of(Arrays.asList(values)));
    }

    protected static Field fixBqRepeatedField(final String name, final StandardSQLTypeName type) {
        return Field.newBuilder(name, type).setMode(Field.Mode.REPEATED).build();
    }

    protected static FieldList fixBqLocFields() {
        return FieldList.of(Field.of("lat", StandardSQLTypeName.FLOAT64), Field.of("lng", StandardSQLTypeName.FLOAT64));
    }

    protected void fixBqStubResult(final FieldList fields, final FieldValueList row) throws Exception {
        when(mockTableResult.getSchema()).thenReturn(Schema.of(fields));
        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockTableResult.iterateAll()).thenReturn(Arrays.asList(row));
        when(mockTableResult.getValues()).thenReturn(Arrays.asList(row));
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);
    }

    public static class FixBqLoc {
        private double lat;
        private double lng;

        public double getLat() {
            return lat;
        }

        public void setLat(final double lat) {
            this.lat = lat;
        }

        public double getLng() {
            return lng;
        }

        public void setLng(final double lng) {
            this.lng = lng;
        }
    }

    public static class FixBqHome {
        private String city;
        private FixBqLoc loc;
        private List<Long> codes;

        public String getCity() {
            return city;
        }

        public void setCity(final String city) {
            this.city = city;
        }

        public FixBqLoc getLoc() {
            return loc;
        }

        public void setLoc(final FixBqLoc loc) {
            this.loc = loc;
        }

        public List<Long> getCodes() {
            return codes;
        }

        public void setCodes(final List<Long> codes) {
            this.codes = codes;
        }
    }

    public static class FixBqHomeHolder {
        private FixBqHome home;

        public FixBqHome getHome() {
            return home;
        }

        public void setHome(final FixBqHome home) {
            this.home = home;
        }
    }

    protected static Field fixBq2Struct(final String name, final FieldList subFields) {
        return Field.newBuilder(name, StandardSQLTypeName.STRUCT, subFields).build();
    }

    protected static FieldList fixBq2ValueFields(final StandardSQLTypeName type) {
        return FieldList.of(Field.of("value", type));
    }

    public static class FixBq2Box<T> {
        private T value;

        public T getValue() {
            return value;
        }

        public void setValue(final T value) {
            this.value = value;
        }
    }

    public static class FixBq2LongBox extends FixBq2Box<Long> {
    }

    public static class FixBq2Series<T> {
        private List<T> items;
        private Map<String, T> byKey;
        private T first;

        public List<T> getItems() {
            return items;
        }

        public void setItems(final List<T> items) {
            this.items = items;
        }

        public Map<String, T> getByKey() {
            return byKey;
        }

        public void setByKey(final Map<String, T> byKey) {
            this.byKey = byKey;
        }

        public T getFirst() {
            return first;
        }

        public void setFirst(final T first) {
            this.first = first;
        }
    }

    public static class FixBq2Item {
        private String name;
        private FixBq2Box<Long> box;
        private FixBq2LongBox longBox;
        private FixBq2Box<java.time.Instant> seen;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public FixBq2Box<Long> getBox() {
            return box;
        }

        public void setBox(final FixBq2Box<Long> box) {
            this.box = box;
        }

        public FixBq2LongBox getLongBox() {
            return longBox;
        }

        public void setLongBox(final FixBq2LongBox longBox) {
            this.longBox = longBox;
        }

        public FixBq2Box<java.time.Instant> getSeen() {
            return seen;
        }

        public void setSeen(final FixBq2Box<java.time.Instant> seen) {
            this.seen = seen;
        }
    }

    public static class FixBq2LongBoxHolder {
        private List<FixBq2LongBox> boxes;
        private FixBq2LongBox[] boxArray;
        private Set<FixBq2LongBox> boxSet;

        public List<FixBq2LongBox> getBoxes() {
            return boxes;
        }

        public void setBoxes(final List<FixBq2LongBox> boxes) {
            this.boxes = boxes;
        }

        public FixBq2LongBox[] getBoxArray() {
            return boxArray;
        }

        public void setBoxArray(final FixBq2LongBox[] boxArray) {
            this.boxArray = boxArray;
        }

        public Set<FixBq2LongBox> getBoxSet() {
            return boxSet;
        }

        public void setBoxSet(final Set<FixBq2LongBox> boxSet) {
            this.boxSet = boxSet;
        }
    }

    public static class FixBq2BoxHolder {
        private FixBq2Box<Long> box;
        private FixBq2LongBox longBox;
        private FixBq2Item item;
        private List<FixBq2Item> items;
        private FixBq2Series<Long> series;
        private List<FixBq2Series<Long>> seriesList;
        private FixBq2Box<FixBq2Box<Long>> nested;
        private List<FixBq2Box<FixBq2Box<Long>>> nestedList;

        public FixBq2Box<Long> getBox() {
            return box;
        }

        public void setBox(final FixBq2Box<Long> box) {
            this.box = box;
        }

        public FixBq2LongBox getLongBox() {
            return longBox;
        }

        public void setLongBox(final FixBq2LongBox longBox) {
            this.longBox = longBox;
        }

        public FixBq2Item getItem() {
            return item;
        }

        public void setItem(final FixBq2Item item) {
            this.item = item;
        }

        public List<FixBq2Item> getItems() {
            return items;
        }

        public void setItems(final List<FixBq2Item> items) {
            this.items = items;
        }

        public FixBq2Series<Long> getSeries() {
            return series;
        }

        public void setSeries(final FixBq2Series<Long> series) {
            this.series = series;
        }

        public List<FixBq2Series<Long>> getSeriesList() {
            return seriesList;
        }

        public void setSeriesList(final List<FixBq2Series<Long>> seriesList) {
            this.seriesList = seriesList;
        }

        public FixBq2Box<FixBq2Box<Long>> getNested() {
            return nested;
        }

        public void setNested(final FixBq2Box<FixBq2Box<Long>> nested) {
            this.nested = nested;
        }

        public List<FixBq2Box<FixBq2Box<Long>>> getNestedList() {
            return nestedList;
        }

        public void setNestedList(final List<FixBq2Box<FixBq2Box<Long>>> nestedList) {
            this.nestedList = nestedList;
        }
    }

    public enum FixBq2Color {
        RED, GREEN
    }

    protected static FieldValue mxRecord(final boolean sdk, final FieldList sub, final FieldValue... values) {
        return FieldValue.of(FieldValue.Attribute.RECORD, sdk ? FieldValueList.of(Arrays.asList(values)) : FieldValueList.of(Arrays.asList(values), sub));
    }

    protected static FieldValue mxRepeated(final boolean sdk, final FieldValue... elements) {
        return FieldValue.of(FieldValue.Attribute.REPEATED, sdk ? FieldValueList.of(Arrays.asList(elements)) : new ArrayList<>(Arrays.asList(elements)));
    }
}
