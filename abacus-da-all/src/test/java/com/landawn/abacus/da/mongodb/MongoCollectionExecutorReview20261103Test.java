package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import com.mongodb.client.model.Filters;

/**
 * Review 2026-11-03 regressions for the synchronous {@link MongoCollectionExecutor}.
 */
public class MongoCollectionExecutorReview20261103Test extends MongoCollectionExecutorTestSupport {

    private static final Bson FILTER = Filters.eq("k", 1);

    private static final Date UTC_MIDNIGHT = new Date(LocalDate.of(2024, 3, 5).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());

    public static class Child {
        private List<Long> vals;

        public List<Long> getVals() {
            return vals;
        }

        public void setVals(final List<Long> vals) {
            this.vals = vals;
        }
    }

    public static class Inner {
        private List<Long> nums;
        private LocalDate day;
        private byte[] data;
        private Child child;

        public List<Long> getNums() {
            return nums;
        }

        public void setNums(final List<Long> nums) {
            this.nums = nums;
        }

        public LocalDate getDay() {
            return day;
        }

        public void setDay(final LocalDate day) {
            this.day = day;
        }

        public byte[] getData() {
            return data;
        }

        public void setData(final byte[] data) {
            this.data = data;
        }

        public Child getChild() {
            return child;
        }

        public void setChild(final Child child) {
            this.child = child;
        }
    }

    @com.landawn.abacus.annotation.Entity
    public static class EntityMap extends LinkedHashMap<String, Object> {
        private static final long serialVersionUID = 1L;
    }

    public static class IdBean {
        private String id;
        private String name;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    private static Document innerDocument() {
        return new Document("nums", Arrays.asList(1, 2)).append("day", UTC_MIDNIGHT)
                .append("data", new Binary(new byte[] { 1, 2 }))
                .append("child", new Document("vals", Arrays.asList(3, 4)));
    }

    private void stubFirst(final Document row) {
        when(mockFindIterable.first()).thenReturn(row);
    }

    private static void assertInner(final String label, final Inner inner) {
        assertAll(label, () -> assertEquals(Arrays.asList(1L, 2L), inner.getNums()), // List.equals: Integer 1 != Long 1
                () -> assertEquals(Long.class, ((List<?>) inner.getNums()).get(0).getClass()), () -> assertEquals(LocalDate.of(2024, 3, 5), inner.getDay()), // read in UTC, as the driver's codec writes it
                () -> assertArrayEquals(new byte[] { 1, 2 }, inner.getData()), () -> assertEquals(Arrays.asList(3L, 4L), inner.getChild().getVals()));
    }

    /**
     * An embedded document requested as a bean went through {@code N.convert} (a plain {@code Beans.mapToBean}): a
     * {@code byte[]} property holding BSON binary failed with {@code NumberFormatException}, {@code List<Long>} properties
     * kept the stored {@code Integer}s, and a {@code LocalDate} was read in the JVM's zone. It is now read as every other
     * document read as a bean (findFirst, list, readRow, distinct) is.
     */
    @Test
    public void testQueryForSingleValueReadsEmbeddedDocumentAsBean() {
        stubFirst(new Document("_id", new ObjectId()).append("inner", innerDocument()));

        assertAll(() -> assertInner("queryForSingleValue", executor.queryForSingleValue("inner", FILTER, Inner.class).get()),
                () -> assertInner("queryForSingleNonNull", executor.queryForSingleNonNull("inner", FILTER, Inner.class).get()),
                () -> assertInner("reference: findFirst of the embedded document", MongoDBBase.toEntity(innerDocument(), Inner.class)));
    }

    /** Without the binary property the old path did not fail but returned a {@code List<Long>} holding {@code Integer}s. */
    @Test
    public void testQueryForSingleValueConvertsTypedContainerElementsOfEmbeddedBean() {
        final Document inner = innerDocument();
        inner.remove("data");
        stubFirst(new Document("_id", 1).append("inner", inner));

        final Inner single = executor.queryForSingleValue("inner", FILTER, Inner.class).get();
        final Inner nonNull = executor.queryForSingleNonNull("inner", FILTER, Inner.class).get();

        assertAll(() -> assertEquals(Long.class, ((List<?>) single.getNums()).get(0).getClass()),
                () -> assertEquals(Long.class, ((List<?>) single.getChild().getVals()).get(0).getClass()),
                () -> assertEquals(Long.class, ((List<?>) nonNull.getNums()).get(0).getClass()), () -> assertEquals(LocalDate.of(2024, 3, 5), single.getDay()),
                () -> assertNull(single.getData()));
    }

    /** An array of embedded documents requested as a bean array: each element is read as {@code toEntity} reads a document. */
    @Test
    public void testQueryForSingleValueReadsArrayOfEmbeddedDocumentsAsBeanArray() {
        stubFirst(new Document("_id", new ObjectId()).append("inners", Arrays.asList(innerDocument(), innerDocument())));

        final Inner[] single = executor.queryForSingleValue("inners", FILTER, Inner[].class).get();
        final Inner[] nonNull = executor.queryForSingleNonNull("inners", FILTER, Inner[].class).get();

        assertAll(() -> assertEquals(2, single.length), () -> assertInner("queryForSingleValue element 0", single[0]),
                () -> assertInner("queryForSingleValue element 1", single[1]), () -> assertEquals(2, nonNull.length),
                () -> assertInner("queryForSingleNonNull element 0", nonNull[0]));
    }

    @Test
    public void testQueryForSingleValueReadsDottedEmbeddedDocumentAsBean() {
        stubFirst(new Document("_id", 1).append("outer", new Document("inner", innerDocument())));

        assertInner("dotted path", executor.queryForSingleValue("outer.inner", FILTER, Inner.class).get());
    }

    /**
     * Pins the documented id rule (v5 verifier): a String id property takes the embedded document's own {@code id} field over
     * its {@code _id} whatever their order (with {@code id} first, plain bean mapping let the later {@code _id} win), and the
     * {@code _id} (an ObjectId as its hex text) only when the document has no {@code id} field.
     */
    @Test
    public void testQueryForSingleValueStringIdTakesOwnIdFieldOverIdInAnyOrder() {
        final ObjectId embeddedId = new ObjectId();
        stubFirst(new Document("_id", 1).append("idFirst", new Document("id", "own-1").append("_id", embeddedId).append("name", "a"))
                .append("idLast", new Document("_id", embeddedId).append("id", "own-2").append("name", "b"))
                .append("implicit", new Document("_id", embeddedId).append("name", "c")));
        final MongoCollectionMapper<Document> mapper = new MongoCollectionMapper<>(executor, Document.class);

        assertAll(() -> assertEquals("own-1", executor.queryForSingleValue("idFirst", FILTER, IdBean.class).get().getId()),
                () -> assertEquals("own-1", executor.queryForSingleNonNull("idFirst", FILTER, IdBean.class).get().getId()),
                () -> assertEquals("own-1", mapper.queryForSingleValue("idFirst", FILTER, IdBean.class).get().getId()),
                () -> assertEquals("own-2", executor.queryForSingleValue("idLast", FILTER, IdBean.class).get().getId()),
                () -> assertEquals(embeddedId.toHexString(), executor.queryForSingleValue("implicit", FILTER, IdBean.class).get().getId()),
                () -> assertEquals("c", executor.queryForSingleValue("implicit", FILTER, IdBean.class).get().getName()));
    }

    /** Guards: values that are not an embedded document read as a bean keep the convertBsonValue rules. */
    @Test
    public void testQueryForSingleValueNonBeanTargetsKeepConvertBsonValueRules() {
        final Document inner = new Document("a", 1);
        stubFirst(new Document("_id", 1).append("inner", inner).append("code", new Document("code", "x")).append("missing", null));

        assertAll(() -> assertSame(inner, executor.queryForSingleValue("inner", FILTER, Map.class).get()),
                () -> assertSame(inner, executor.queryForSingleValue("inner", FILTER, Document.class).get()),
                () -> assertEquals(Map.of("a", 1), executor.queryForSingleValue("inner", FILTER, EntityMap.class).get()),
                () -> assertTrue(executor.queryForSingleValue("inner", FILTER, EntityMap.class).get() instanceof EntityMap),
                () -> assertEquals(new Code("x"), executor.queryForSingleValue("code", FILTER, Code.class).get()),
                () -> assertEquals("{\"a\": 1}", executor.queryForSingleValue("inner", FILTER, String.class).get()),
                () -> assertNull(executor.queryForSingleValue("missing", FILTER, Inner.class).get()),
                () -> assertNull(executor.queryForSingleValue("absent", FILTER, Inner.class).get()));
    }

    /**
     * Pins the documented object-array row rule of {@code list(Collection, ...)}: such a row type is read from the sole
     * non-{@code _id} field, so a document holding only its {@code _id} yields {@code null} when another document has the
     * field, but its converted {@code _id} when none does.
     */
    @Test
    public void testListObjectArrayRowTypeReadsIdOnlyDocumentsAsToListDoes() {
        final ObjectId id1 = new ObjectId();
        final ObjectId id2 = new ObjectId();

        stubFindRows(Arrays.asList(new Document("_id", id1), new Document("_id", id2).append("a", 5)));
        final List<Object[]> mixed = executor.list(List.of("a"), FILTER, Object[].class);

        stubFindRows(Arrays.asList(new Document("_id", id1), new Document("_id", id2)));
        final List<Object[]> idOnly = executor.list(List.of("a"), FILTER, Object[].class);

        assertAll(() -> assertNull(mixed.get(0)), () -> assertArrayEquals(new Object[] { 5 }, mixed.get(1)),
                () -> assertArrayEquals(new Object[] { id1 }, idOnly.get(0)), () -> assertArrayEquals(new Object[] { id2 }, idOnly.get(1)));
    }

    /**
     * Before the fix: IllegalArgumentException "Cannot convert java.util.ArrayList value [1, 2] to org.bson.RawBsonArray: it is
     * stored as a BSON ARRAY" from queryForSingleValue/queryForSingleNonNull (a BsonArray worked, and readRow returns
     * RawBsonArray rows).
     */
    @Test
    public void testQueryForSingleValueReadsArrayFieldAsRawBsonArray() {
        stubFirst(new Document("_id", new ObjectId()).append("arr", new java.util.ArrayList<>(List.of(1, 2))));
        final List<org.bson.BsonValue> expected = List.of(new org.bson.BsonInt32(1), new org.bson.BsonInt32(2));

        final org.bson.RawBsonArray value = executor.queryForSingleValue("arr", FILTER, org.bson.RawBsonArray.class).get();
        final org.bson.RawBsonArray nonNull = executor.queryForSingleNonNull("arr", FILTER, org.bson.RawBsonArray.class).get();

        assertAll(() -> assertEquals(expected, new java.util.ArrayList<>(value)), () -> assertEquals(expected, new java.util.ArrayList<>(nonNull)));
    }

    /**
     * Pins the reworded {@code @throws} of the list overloads: an object-array or {@code Collection} row type is read from the sole
     * non-{@code _id} field, so a document with several non-{@code _id} fields is rejected as for a scalar row type.
     */
    @Test
    public void testListObjectArrayOrCollectionRowTypeRejectsSeveralFields() {
        stubFindRows(Arrays.asList(new Document("_id", 1).append("a", 5).append("b", 6)));

        assertAll(() -> org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> executor.list(FILTER, Object[].class)),
                () -> org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> executor.list(FILTER, List.class)),
                () -> org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> executor.list(FILTER, Integer.class)));
    }
}
