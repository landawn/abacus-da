package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.util.AsyncExecutor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;

/**
 * Review 2026-11-03: pins the {@link AsyncMongoCollectionExecutor} Javadoc statements mirrored from the synchronous
 * executor this round, against a real sync executor over a mocked collection (the async methods delegate to it on a
 * same-thread {@link AsyncExecutor}).
 */
public class AsyncMongoCollectionExecutorReview20261103Test extends MongoCollectionExecutorTestSupport {

    private static final Bson FILTER = Filters.eq("k", 1);

    private static final Date UTC_MIDNIGHT = new Date(LocalDate.of(2024, 3, 5).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());

    private AsyncMongoCollectionExecutor async;

    public static class Meta {
        private List<Long> nums;
        private LocalDate day;
        private byte[] data;

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
    }

    @BeforeEach
    public void createAsync() {
        async = new AsyncMongoCollectionExecutor(executor, new AsyncExecutor(Runnable::run));
    }

    private static void assertMeta(final String label, final Meta meta) {
        assertAll(label, () -> assertEquals(Long.class, ((List<?>) meta.getNums()).get(0).getClass()),
                () -> assertEquals(Arrays.asList(1L, 2L), meta.getNums()), () -> assertEquals(LocalDate.of(2024, 3, 5), meta.getDay()),
                () -> assertArrayEquals(new byte[] { 1, 2 }, meta.getData()));
    }

    /**
     * The async {@code queryForSingleValue}/{@code queryForSingleNonNull} docs now say an embedded document requested as a
     * bean class is read as {@code MongoDBBase.toEntity} reads a document, including through a dotted path.
     */
    @Test
    public void testQueryForSingleValueReadsEmbeddedDocumentAsBean() throws Exception {
        final Document meta = new Document("nums", Arrays.asList(1, 2)).append("day", UTC_MIDNIGHT).append("data", new Binary(new byte[] { 1, 2 }));
        when(mockFindIterable.first()).thenReturn(new Document("_id", new ObjectId()).append("meta", meta).append("outer", new Document("meta", meta)));

        assertAll(() -> assertMeta("queryForSingleValue", async.queryForSingleValue("meta", FILTER, Meta.class).get().get()),
                () -> assertMeta("queryForSingleNonNull", async.queryForSingleNonNull("meta", FILTER, Meta.class).get().get()),
                () -> assertMeta("dotted path", async.queryForSingleValue("outer.meta", FILTER, Meta.class).get().get()));
    }

    /**
     * An array of embedded documents requested as a bean array: each element is read as an embedded bean, as for a single
     * bean class (before the fix the elements kept the stored {@code Integer}s, a zone-shifted day, or failed on the binary).
     */
    @Test
    public void testQueryForSingleValueReadsArrayOfEmbeddedDocumentsAsBeanArray() throws Exception {
        final Document meta = new Document("nums", Arrays.asList(1, 2)).append("day", UTC_MIDNIGHT).append("data", new Binary(new byte[] { 1, 2 }));
        when(mockFindIterable.first()).thenReturn(new Document("_id", new ObjectId()).append("metas", Arrays.asList(meta, meta)));

        final Meta[] single = async.queryForSingleValue("metas", FILTER, Meta[].class).get().get();
        final Meta[] nonNull = async.queryForSingleNonNull("metas", FILTER, Meta[].class).get().get();

        assertAll(() -> assertEquals(2, single.length), () -> assertMeta("queryForSingleValue element 0", single[0]),
                () -> assertMeta("queryForSingleValue element 1", single[1]), () -> assertEquals(2, nonNull.length),
                () -> assertMeta("queryForSingleNonNull element 0", nonNull[0]));
    }

    /**
     * The async 6-arg {@code list(Collection, ...)} doc now states the {@code _id}-only rule for an object-array row type:
     * {@code null} when another document has the field, the converted {@code _id} when none does.
     */
    @Test
    public void testSixArgListArrayRowTypeIdOnlyDocuments() throws Exception {
        final ObjectId id1 = new ObjectId();
        final ObjectId id2 = new ObjectId();

        stubFindRows(Arrays.asList(new Document("_id", id1), new Document("_id", id2).append("a", 5)));
        final List<Object[]> mixed = async.list(List.of("a"), FILTER, null, 0, 10, Object[].class).get();

        stubFindRows(Arrays.asList(new Document("_id", id1), new Document("_id", id2)));
        final List<Object[]> idOnly = async.list(List.of("a"), FILTER, null, 0, 10, Object[].class).get();

        assertAll(() -> assertNull(mixed.get(0)), () -> assertArrayEquals(new Object[] { 5 }, mixed.get(1)),
                () -> assertArrayEquals(new Object[] { id1 }, idOnly.get(0)), () -> assertArrayEquals(new Object[] { id2 }, idOnly.get(1)));
    }

    /**
     * The delegating async {@code list} overloads now point to the 6-arg rule: an object-array row type is read from the
     * sole non-{@code _id} field (a document with several fails the future), and a single field selected into a scalar type
     * yields {@code null} for a document lacking it.
     */
    @Test
    public void testDelegatingListOverloadsFollowSixArgRowRules() throws Exception {
        stubFindRows(Arrays.asList(new Document("_id", 1).append("a", 7)));

        assertAll(() -> assertArrayEquals(new Object[] { 7 }, async.list(FILTER, Object[].class).get().get(0)),
                () -> assertArrayEquals(new Object[] { 7 }, async.list(FILTER, 0, 10, Object[].class).get().get(0)),
                () -> assertArrayEquals(new Object[] { 7 }, async.list(List.of("a"), FILTER, Object[].class).get().get(0)),
                () -> assertArrayEquals(new Object[] { 7 }, async.list(List.of("a"), FILTER, 0, 10, Object[].class).get().get(0)),
                () -> assertArrayEquals(new Object[] { 7 }, async.list(List.of("a"), FILTER, null, Object[].class).get().get(0)),
                () -> assertArrayEquals(new Object[] { 7 }, async.list(Projections.include("a"), FILTER, null, Object[].class).get().get(0)),
                () -> assertEquals(List.of(7), async.list(FILTER, List.class).get().get(0)));

        stubFindRows(Arrays.asList(new Document("_id", 1).append("a", 7).append("b", 8)));

        final ExecutionException multiField = assertThrows(ExecutionException.class, () -> async.list(FILTER, Object[].class).get());
        assertTrue(multiField.getCause() instanceof IllegalArgumentException, String.valueOf(multiField.getCause()));

        stubFindRows(Arrays.asList(new Document("_id", 1), new Document("_id", 2).append("a", 5)));

        assertAll(() -> assertEquals(Arrays.asList(null, 5), async.list(List.of("a"), FILTER, Integer.class).get()),
                () -> assertEquals(Arrays.asList(null, 5), async.list(List.of("a"), FILTER, 0, 10, Integer.class).get()),
                () -> assertEquals(Arrays.asList(null, 5), async.list(List.of("a"), FILTER, null, Integer.class).get()));
    }
}
