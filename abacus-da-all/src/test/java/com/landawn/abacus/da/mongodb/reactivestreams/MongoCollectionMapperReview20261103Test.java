package com.landawn.abacus.da.mongodb.reactivestreams;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
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

import com.landawn.abacus.da.MockitoTestBase;
import com.mongodb.client.model.Filters;
import com.mongodb.reactivestreams.client.MongoCollection;

import reactor.core.publisher.Mono;

/**
 * Review 2026-11-03 of the reactive {@link MongoCollectionMapper}: pins contracts its Javadoc now states, as its executor and the
 * blocking mapper do: {@code queryForSingleValue} reads an embedded document requested as a bean class as the embedded document of
 * a bean property is read; an object-array or {@code Collection} entity type receives each returned document's field values
 * positionally on every path; a record entity's {@code id} component is stored under its own name, not as {@code _id}.
 */
public class MongoCollectionMapperReview20261103Test extends MockitoTestBase {

    private static final Bson FILTER = Filters.eq("k", 1);

    private static final String HEX = "507f1f77bcf86cd799439011";

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

    private static <E> MongoCollectionMapper<E> mapperReturning(final Document row, final Class<E> entityClass) {
        return new MongoCollectionMapper<>(new MongoCollectionExecutor(MongoCollectionExecutorTestSupport.mockCollectionReturning(row)), entityClass);
    }

    @Test
    public void testQueryForSingleValueReadsEmbeddedDocumentAsBeanProperty() {
        final Document row = new Document("_id", 1).append("inner", innerDocument("own"))
                .append("outer", new Document("inner", innerDocument("dotted")))
                .append("items", Arrays.asList(innerDocument("a"), innerDocument("b")));
        final MongoCollectionMapper<Document> mapper = mapperReturning(row, Document.class);

        final Inner[] items = mapper.queryForSingleValue("items", FILTER, Inner[].class).block();

        assertAll(() -> assertInner("queryForSingleValue", "own", mapper.queryForSingleValue("inner", FILTER, Inner.class).block()),
                () -> assertInner("dotted path", "dotted", mapper.queryForSingleValue("outer.inner", FILTER, Inner.class).block()),
                () -> assertEquals(2, items.length), () -> assertInner("bean array element 0", "a", items[0]),
                () -> assertInner("bean array element 1", "b", items[1]),
                () -> assertNull(mapper.queryForSingleValue("absent", FILTER, Inner.class).block(), "a missing field completes empty"));
    }

    /** Positional rows on every path, also with one selected field (the blocking mapper's list reads the sole non-_id field). */
    @Test
    public void testContainerEntityTypeReceivesFieldValuesPositionally() {
        final Document row = new Document("_id", 7).append("a", 1);
        final MongoCollectionMapper<Object[]> arrays = mapperReturning(row, Object[].class);
        final MongoCollectionMapper<List> lists = mapperReturning(row, List.class);
        final List<Bson> pipeline = Arrays.asList(new Document("$match", new Document()));
        final Object[] expected = { 7, 1 };

        assertAll(() -> assertArrayEquals(expected, arrays.list(FILTER).blockFirst(), "list"),
                () -> assertArrayEquals(expected, arrays.list(Arrays.asList("a"), FILTER).blockFirst(), "list(one field)"),
                () -> assertArrayEquals(expected, arrays.findFirst(Arrays.asList("a"), FILTER).block(), "findFirst(one field)"),
                () -> assertArrayEquals(expected, arrays.get(new ObjectId(HEX), Arrays.asList("a")).block(), "get(one field)"),
                () -> assertArrayEquals(expected, arrays.aggregate(pipeline).blockFirst(), "aggregate"),
                () -> assertArrayEquals(expected, arrays.findOneAndDelete(FILTER).block(), "findOneAndDelete"),
                () -> assertEquals(Arrays.asList(7, 1), lists.list(Arrays.asList("a"), FILTER).blockFirst(), "List list(one field)"));
    }

    @Test
    public void testRecordIdComponentIsStoredUnderItsOwnNameNotAsId() {
        final MongoCollection<Document> collection = mock(MongoCollection.class);
        when(collection.insertOne(any(Document.class))).thenReturn(Mono.empty());
        final MongoCollectionMapper<UserRecord> mapper = new MongoCollectionMapper<>(new MongoCollectionExecutor(collection), UserRecord.class);

        mapper.insertOne(new UserRecord(HEX, "a"));

        final ArgumentCaptor<Document> inserted = ArgumentCaptor.forClass(Document.class);
        verify(collection).insertOne(inserted.capture());
        final Document doc = inserted.getValue();

        // MongoDB assigns a separate _id, which the id-keyed methods match ({_id: ObjectId(HEX)} does not find this document).
        final Document stored = new Document("_id", new ObjectId("65aaaaaaaaaaaaaaaaaaaaaa")).append("id", HEX).append("name", "a");
        final MongoCollectionMapper<UserRecord> reader = mapperReturning(stored, UserRecord.class);

        assertAll(() -> assertEquals(HEX, doc.get("id")), () -> assertEquals("a", doc.get("name")), () -> assertFalse(doc.containsKey("_id")),
                () -> assertEquals(new UserRecord(HEX, "a"), reader.list(FILTER).blockFirst(), "read back from its own field"));
    }

    public record LongIdRecord(Long id, String name) {
    }

    /**
     * Pins the class doc sentence (until now tested only for the blocking mapper): a record inserted with a {@code null} id stores
     * neither {@code id} nor {@code _id}, and reads back with the generated {@code _id} in a String-typed id component (as its hex
     * string), while a Long-typed one reads back {@code null}.
     */
    @Test
    public void testNullIdRecordReadsBackGeneratedIdOnlyIntoStringOrObjectIdComponent() {
        final MongoCollection<Document> collection = mock(MongoCollection.class);
        when(collection.insertOne(any(Document.class))).thenReturn(Mono.empty());
        new MongoCollectionMapper<>(new MongoCollectionExecutor(collection), UserRecord.class).insertOne(new UserRecord(null, "n"));

        final ArgumentCaptor<Document> inserted = ArgumentCaptor.forClass(Document.class);
        verify(collection).insertOne(inserted.capture());
        final Document doc = inserted.getValue();

        final ObjectId generated = new ObjectId("65cccccccccccccccccccccc");
        final Document stored = new Document("_id", generated).append("name", "n");

        assertAll(() -> assertFalse(doc.containsKey("id")), () -> assertFalse(doc.containsKey("_id")),
                () -> assertEquals(new UserRecord(generated.toHexString(), "n"), mapperReturning(stored, UserRecord.class).list(FILTER).blockFirst()),
                () -> assertEquals(new UserRecord(generated.toHexString(), "n"), mapperReturning(stored, UserRecord.class).findFirst(FILTER).block()),
                () -> assertEquals(new LongIdRecord(null, "n"), mapperReturning(stored, LongIdRecord.class).list(FILTER).blockFirst()));
    }
}
