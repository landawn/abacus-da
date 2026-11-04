package com.landawn.abacus.da.mongodb.reactivestreams;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
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
 * Review 2026-11-03 regressions for the reactive {@link MongoCollectionExecutor} (driver 5.14 / reactor-core 3.8.8). Offline:
 * every collection is a Mockito stub emitting a fixed row.
 */
public class MongoCollectionExecutorReview20261103Test extends MongoCollectionExecutorTestSupport {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

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

    @com.landawn.abacus.annotation.Entity
    public static class EntityMap extends LinkedHashMap<String, Object> {
        private static final long serialVersionUID = 1L;
    }

    private static Document innerDocument() {
        return new Document("nums", Arrays.asList(1, 2)).append("day", UTC_MIDNIGHT)
                .append("data", new Binary(new byte[] { 1, 2 }))
                .append("child", new Document("vals", Arrays.asList(3, 4)));
    }

    private static MongoCollectionExecutor executorReturning(final Document row) {
        return new MongoCollectionExecutor(mockCollectionReturning(row));
    }

    private static void assertInner(final String label, final Inner inner) {
        assertAll(label, () -> assertEquals(Arrays.asList(1L, 2L), inner.getNums()), // List.equals: Integer 1 != Long 1
                () -> assertEquals(Long.class, ((List<?>) inner.getNums()).get(0).getClass()), () -> assertEquals(LocalDate.of(2024, 3, 5), inner.getDay()), // read in UTC, as the driver's codec writes it
                () -> assertArrayEquals(new byte[] { 1, 2 }, inner.getData()), () -> assertEquals(Arrays.asList(3L, 4L), inner.getChild().getVals()));
    }

    /**
     * An embedded document requested as a bean went through {@code N.convert} (a plain {@code Beans.mapToBean}) in
     * {@code MongoDBBase.convertBsonValue}: a {@code byte[]} property holding BSON binary made the publisher signal
     * {@code NumberFormatException}, {@code List<Long>} properties kept the stored {@code Integer}s, and a {@code LocalDate} was
     * read in the JVM's zone. It is now read as every other document read as a bean (findFirst, list, aggregate) is.
     */
    @Test
    public void testQueryForSingleValueReadsEmbeddedDocumentAsBean() {
        final MongoCollectionExecutor executor = executorReturning(new Document("_id", new ObjectId()).append("inner", innerDocument()));

        assertAll(() -> assertInner("queryForSingleValue", executor.queryForSingleValue("inner", FILTER, Inner.class).block(TIMEOUT)),
                () -> assertInner("reference: the same document read as a row", MongoDB.readRow(innerDocument(), Inner.class)));
    }

    /** Without the binary property the old path did not fail but emitted a {@code List<Long>} holding {@code Integer}s. */
    @Test
    public void testQueryForSingleValueConvertsTypedContainerElementsOfEmbeddedBean() {
        final Document inner = innerDocument();
        inner.remove("data");

        final Inner single = executorReturning(new Document("_id", 1).append("inner", inner)).queryForSingleValue("inner", FILTER, Inner.class).block(TIMEOUT);

        assertAll(() -> assertEquals(Long.class, ((List<?>) single.getNums()).get(0).getClass()),
                () -> assertEquals(Long.class, ((List<?>) single.getChild().getVals()).get(0).getClass()),
                () -> assertEquals(LocalDate.of(2024, 3, 5), single.getDay()), () -> assertNull(single.getData()));
    }

    @Test
    public void testQueryForSingleValueReadsDottedEmbeddedDocumentAsBean() {
        final MongoCollectionExecutor executor = executorReturning(new Document("_id", 1).append("outer", new Document("inner", innerDocument())));

        assertInner("dotted path", executor.queryForSingleValue("outer.inner", FILTER, Inner.class).block(TIMEOUT));
    }

    /** An array of embedded documents requested as a bean array: each element is read as the bean above. */
    @Test
    public void testQueryForSingleValueReadsArrayOfEmbeddedDocumentsAsBeanArray() {
        final MongoCollectionExecutor executor = executorReturning(new Document("_id", 1).append("inners", Arrays.asList(innerDocument(), innerDocument())));

        final Inner[] inners = executor.queryForSingleValue("inners", FILTER, Inner[].class).block(TIMEOUT);

        assertAll(() -> assertEquals(2, inners.length), () -> assertInner("element 0", inners[0]), () -> assertInner("element 1", inners[1]));
    }

    /**
     * Pins the documented id rule: the selected document is read as the embedded document of a bean property, so its own
     * {@code id} field maps to the bean's id, and its {@code _id} (an ObjectId into a String id as its hex text) only when it has
     * none.
     */
    @Test
    public void testQueryForSingleValueEmbeddedBeanIdMapping() {
        final ObjectId embeddedId = new ObjectId();
        final MongoCollectionExecutor executor = executorReturning(
                new Document("_id", 1).append("own", new Document("_id", embeddedId).append("id", "own-id").append("name", "a"))
                        .append("implicit", new Document("_id", embeddedId).append("name", "b")));

        final IdBean own = executor.queryForSingleValue("own", FILTER, IdBean.class).block(TIMEOUT);
        final IdBean implicit = executor.queryForSingleValue("implicit", FILTER, IdBean.class).block(TIMEOUT);

        assertAll(() -> assertEquals("own-id", own.getId()), () -> assertEquals("a", own.getName()),
                () -> assertEquals(embeddedId.toHexString(), implicit.getId()), () -> assertEquals("b", implicit.getName()));
    }

    /** Guards: values that are not an embedded document read as a bean keep the convertBsonValue rules (empty Mono for no value). */
    @Test
    public void testQueryForSingleValueNonBeanTargetsKeepConvertBsonValueRules() {
        final Document inner = new Document("a", 1);
        final MongoCollectionExecutor executor = executorReturning(
                new Document("_id", 1).append("inner", inner).append("code", new Document("code", "x")).append("missing", null));

        assertAll(() -> assertSame(inner, executor.queryForSingleValue("inner", FILTER, Map.class).block(TIMEOUT)),
                () -> assertSame(inner, executor.queryForSingleValue("inner", FILTER, Document.class).block(TIMEOUT)),
                () -> assertEquals(Map.of("a", 1), executor.queryForSingleValue("inner", FILTER, EntityMap.class).block(TIMEOUT)),
                () -> assertTrue(executor.queryForSingleValue("inner", FILTER, EntityMap.class).block(TIMEOUT) instanceof EntityMap),
                () -> assertEquals(new Code("x"), executor.queryForSingleValue("code", FILTER, Code.class).block(TIMEOUT)),
                () -> assertEquals("{\"a\": 1}", executor.queryForSingleValue("inner", FILTER, String.class).block(TIMEOUT)),
                () -> assertNull(executor.queryForSingleValue("missing", FILTER, Inner.class).block(TIMEOUT)),
                () -> assertNull(executor.queryForSingleValue("absent", FILTER, Inner.class).block(TIMEOUT)));
    }
}
