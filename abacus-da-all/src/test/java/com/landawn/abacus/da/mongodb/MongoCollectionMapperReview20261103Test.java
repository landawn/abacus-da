package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.mongodb.client.model.Filters;

/**
 * Review 2026-11-03 of the blocking {@link MongoCollectionMapper}: pins the {@code queryForSingleValue}/{@code queryForSingleNonNull}
 * contract the mapper now documents (as its executor does), that an embedded document requested as a bean class is read as the
 * embedded document of a bean property is read, and an array of embedded documents as a bean array element by element.
 */
public class MongoCollectionMapperReview20261103Test extends MongoCollectionExecutorTestSupport {

    private static final Bson FILTER = Filters.eq("k", 1);

    private static final Date UTC_MIDNIGHT = new Date(LocalDate.of(2024, 3, 5).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());

    public static class Inner {
        private String id;
        private List<Long> nums;
        private LocalDate day;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

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
    }

    public record UserRecord(String id, String name) {
    }

    private static Document innerDocument(final String ownId) {
        return new Document("_id", "embedded-_id").append("id", ownId).append("nums", Arrays.asList(1, 2)).append("day", UTC_MIDNIGHT);
    }

    private static void assertInner(final String label, final String expectedId, final Inner inner) {
        assertAll(label, () -> assertEquals(expectedId, inner.getId()), // the embedded document's own id field, not its _id
                () -> assertEquals(Arrays.asList(1L, 2L), inner.getNums()), // List.equals: Integer 1 != Long 1
                () -> assertEquals(Long.class, ((List<?>) inner.getNums()).get(0).getClass()), () -> assertEquals(LocalDate.of(2024, 3, 5), inner.getDay())); // read in UTC, as the driver's codec writes it
    }

    @Test
    public void testQueryForSingleValueReadsEmbeddedDocumentAsBeanProperty() {
        when(mockFindIterable.first()).thenReturn(new Document("_id", 1).append("inner", innerDocument("own"))
                .append("outer", new Document("inner", innerDocument("dotted")))
                .append("items", Arrays.asList(innerDocument("a"), innerDocument("b"))));

        final MongoCollectionMapper<Document> mapper = new MongoCollectionMapper<>(executor, Document.class);

        final Inner[] items = mapper.queryForSingleValue("items", FILTER, Inner[].class).get();

        assertAll(() -> assertInner("queryForSingleValue", "own", mapper.queryForSingleValue("inner", FILTER, Inner.class).get()),
                () -> assertInner("queryForSingleNonNull", "own", mapper.queryForSingleNonNull("inner", FILTER, Inner.class).get()),
                () -> assertInner("dotted path", "dotted", mapper.queryForSingleValue("outer.inner", FILTER, Inner.class).get()),
                () -> assertEquals(2, items.length), () -> assertInner("bean array element 0", "a", items[0]),
                () -> assertInner("bean array element 1", "b", items[1]), () -> assertNull(mapper.queryForSingleValue("absent", FILTER, Inner.class).get()));
    }

    /**
     * Pins the class doc's record id caveat (v5 verifier): a non-null {@code id} component is stored under its own name (MongoDB
     * assigns a separate {@code _id}) and read back from that field; a {@code null} one is not stored, so the record reads back
     * with the generated {@code _id} in that component.
     */
    @Test
    public void testRecordIdComponentStoredUnderOwnNameAndNullIdReadsGeneratedId() {
        final String hex = "507f1f77bcf86cd799439011";
        final ObjectId generated = new ObjectId("65cccccccccccccccccccccc");
        final MongoCollectionMapper<UserRecord> mapper = new MongoCollectionMapper<>(executor, UserRecord.class);

        mapper.insertOne(new UserRecord(hex, "a"));
        mapper.insertOne(new UserRecord(null, "n"));

        final ArgumentCaptor<Document> inserted = ArgumentCaptor.forClass(Document.class);
        verify(mockCollection, times(2)).insertOne(inserted.capture());
        final Document withId = inserted.getAllValues().get(0);
        final Document withoutId = inserted.getAllValues().get(1);

        when(mockFindIterable.first()).thenReturn(new Document("_id", generated).append("id", hex).append("name", "a"));
        final UserRecord readWithId = mapper.findFirst(FILTER).get();
        when(mockFindIterable.first()).thenReturn(new Document("_id", generated).append("name", "n"));
        final UserRecord readWithoutId = mapper.findFirst(FILTER).get();

        assertAll(() -> assertEquals(hex, withId.get("id")), () -> assertFalse(withId.containsKey("_id")), () -> assertFalse(withoutId.containsKey("id")),
                () -> assertFalse(withoutId.containsKey("_id")), () -> assertEquals(new UserRecord(hex, "a"), readWithId), // from its own field, not the separate _id
                () -> assertEquals(new UserRecord(generated.toHexString(), "n"), readWithoutId));
    }

    public record LongIdRecord(Long id, String name) {
    }

    public record ObjectIdRecord(ObjectId id, String name) {
    }

    /**
     * Pins the corrected class doc sentence: only a String- or ObjectId-typed id component receives the generated {@code _id} of
     * a record inserted with a {@code null} id; one of another type (Long) reads back {@code null}.
     */
    @Test
    public void testNullIdRecordReadsGeneratedIdOnlyIntoStringOrObjectIdComponent() {
        final ObjectId generated = new ObjectId("65cccccccccccccccccccccc");
        when(mockFindIterable.first()).thenReturn(new Document("_id", generated).append("name", "n"));

        final LongIdRecord longId = new MongoCollectionMapper<>(executor, LongIdRecord.class).findFirst(FILTER).get();
        final ObjectIdRecord objectId = new MongoCollectionMapper<>(executor, ObjectIdRecord.class).findFirst(FILTER).get();

        assertAll(() -> assertEquals(new LongIdRecord(null, "n"), longId), () -> assertEquals(new ObjectIdRecord(generated, "n"), objectId));
    }

    /**
     * Pins the class doc caveat: the id-keyed methods ({@code get(String)}) filter on {@code _id}, while a record's non-null id
     * component is stored under its own {@code id} field (the server generates the document's {@code _id}), so they do not find it.
     */
    @Test
    public void testIdKeyedGetFiltersOnIdNotOnARecordsOwnIdComponent() {
        final String hex = "507f1f77bcf86cd799439011";
        final MongoCollectionMapper<UserRecord> mapper = new MongoCollectionMapper<>(executor, UserRecord.class);
        mapper.insertOne(new UserRecord(hex, "a"));
        final ArgumentCaptor<Document> inserted = ArgumentCaptor.forClass(Document.class);
        verify(mockCollection).insertOne(inserted.capture());

        mapper.get(hex);

        final ArgumentCaptor<Bson> filter = ArgumentCaptor.forClass(Bson.class);
        verify(mockCollection).find(filter.capture());
        final Document query = Document.parse(filter.getValue().toBsonDocument(Document.class, MongoDBBase.codecRegistry).toJson());

        assertAll(() -> assertEquals(new Document("_id", new ObjectId(hex)), query), // matches the _id ...
                () -> assertFalse(inserted.getValue().containsKey("_id")), // ... which the inserted document does not carry
                () -> assertEquals(hex, inserted.getValue().get("id")));
    }
}
