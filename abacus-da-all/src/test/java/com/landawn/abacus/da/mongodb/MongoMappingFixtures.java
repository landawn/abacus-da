package com.landawn.abacus.da.mongodb;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;

import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.stream.Stream;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

/** Shared decoded BSON rows and mapping assertions for sync, async, reactive, and mapper tests. */
public final class MongoMappingFixtures {

    private MongoMappingFixtures() {
    }

    public static final String OID_HEX = "65f0a1b2c3d4e5f601234567";

    public static final Date DAY = new Date(java.time.Instant.parse("2024-01-02T00:00:00Z").toEpochMilli());

    public static final Date AT = new Date(java.time.Instant.parse("2024-01-02T03:04:05.006Z").toEpochMilli());

    public static final java.time.LocalDate LOCAL_DAY = java.time.LocalDate.of(2024, 1, 2);

    public static final java.time.LocalDateTime LOCAL_AT = java.time.LocalDateTime.of(2024, 1, 2, 3, 4, 5, 6_000_000);

    public static final java.time.LocalTime LOCAL_TIME = java.time.LocalTime.of(3, 4, 5, 6_000_000);

    public static Document entityDoc() {
        return new Document("_id", new ObjectId(OID_HEX)).append("nums", new java.util.ArrayList<>(Arrays.asList(1, 2)))
                .append("addresses", new java.util.ArrayList<>(Arrays.asList(new Document("city", "Paris"))))
                .append("addressByName", new Document("home", new Document("city", "Rome")))
                .append("countByYear", new Document("2024", 5))
                .append("day", DAY)
                .append("at", AT)
                .append("time", AT)
                .append("days", new java.util.ArrayList<>(Arrays.asList(DAY)))
                .append("box", new Document("value", 7))
                .append("boxes", new java.util.ArrayList<>(Arrays.asList(new Document("value", 8))));
    }

    public static Document recordDoc() {
        return new Document("_id", new ObjectId(OID_HEX)).append("count", 3).append("nums", new java.util.ArrayList<>(Arrays.asList(4))).append("day", DAY);
    }

    public static Document longBoxDoc() {
        return new Document("_id", 1).append("value", 9);
    }

    public static Document dayDoc() {
        return new Document("_id", 1).append("day", DAY);
    }

    public static MappedRecord expectedRecord() {
        return new MappedRecord(OID_HEX, 3L, Arrays.asList(4L), LOCAL_DAY);
    }

    public static void assertEntity(final String path, final MappedEntity entity) {
        Assertions.assertNotNull(entity, path);
        Assertions.assertAll(path, () -> Assertions.assertEquals(OID_HEX, entity.getId(), path + ": String id from _id"),
                () -> Assertions.assertEquals(Arrays.asList(1L, 2L), entity.getNums(), path + ": List<Long>"),
                () -> Assertions.assertEquals("Paris", entity.getAddresses().get(0).getCity(), path + ": List<Address>"),
                () -> Assertions.assertEquals("Rome", entity.getAddressByName().get("home").getCity(), path + ": Map<String, Address>"),
                () -> Assertions.assertEquals(java.util.Collections.singletonMap(2024, 5L), entity.getCountByYear(), path + ": Map<Integer, Long>"),
                () -> Assertions.assertEquals(LOCAL_DAY, entity.getDay(), path + ": LocalDate in UTC"),
                () -> Assertions.assertEquals(LOCAL_AT, entity.getAt(), path + ": LocalDateTime in UTC"),
                () -> Assertions.assertEquals(LOCAL_TIME, entity.getTime(), path + ": LocalTime in UTC"),
                () -> Assertions.assertEquals(Arrays.asList(LOCAL_DAY), entity.getDays(), path + ": List<LocalDate> in UTC"),
                () -> Assertions.assertEquals((Object) 7L, entity.getBox().getValue(), path + ": Box<Long> property"),
                () -> Assertions.assertEquals((Object) 8L, entity.getBoxes().get(0).getValue(), path + ": List<Box<Long>> element"));
    }

    // A bean Dataset column holds the property values toEntity produced.
    public static void assertEntityDataset(final String path, final Dataset dataset) {
        Assertions.assertEquals(1, dataset.size(), path);
        Assertions.assertAll(path, () -> Assertions.assertEquals(Arrays.asList(1L, 2L), dataset.getColumn("nums").get(0), path + ": List<Long>"),
                () -> Assertions.assertEquals("Paris", ((List<MappedAddress>) dataset.getColumn("addresses").get(0)).get(0).getCity(),
                        path + ": List<Address>"),
                () -> Assertions.assertEquals(LOCAL_DAY, dataset.getColumn("day").get(0), path + ": LocalDate in UTC"),
                () -> Assertions.assertEquals(7L, ((MappedBox<?>) dataset.getColumn("box").get(0)).getValue(), path + ": Box<Long>"));
    }

    @SuppressWarnings("unchecked")
    public static MongoCursor<Document> cursorOf(final Document... rows) {
        final java.util.Iterator<Document> iter = Arrays.asList(rows).iterator();
        final MongoCursor<Document> cursor = mock(MongoCursor.class);
        when(cursor.hasNext()).thenAnswer(invocation -> iter.hasNext());
        when(cursor.next()).thenAnswer(invocation -> iter.next());
        return cursor;
    }

    // A collection whose every read (find first/into/iterator, aggregate, findOneAndUpdate/Delete) returns the given row.
    @SuppressWarnings("unchecked")
    public static MongoCollection<Document> mockCollectionReturning(final Document row) {
        final MongoCollection<Document> collection = mock(MongoCollection.class);
        final FindIterable<Document> findIterable = mock(FindIterable.class);
        when(collection.find()).thenReturn(findIterable);
        when(collection.find(any(Bson.class))).thenReturn(findIterable);
        when(findIterable.projection(any())).thenReturn(findIterable);
        when(findIterable.sort(any())).thenReturn(findIterable);
        when(findIterable.skip(anyInt())).thenReturn(findIterable);
        when(findIterable.limit(anyInt())).thenReturn(findIterable);
        when(findIterable.first()).thenReturn(row);
        when(findIterable.into(any())).thenAnswer(invocation -> {
            final Collection<Object> target = invocation.getArgument(0);
            target.add(row);
            return target;
        });
        when(findIterable.iterator()).thenAnswer(invocation -> cursorOf(row));
        final AggregateIterable<Document> aggregateIterable = mock(AggregateIterable.class);
        when(aggregateIterable.iterator()).thenAnswer(invocation -> cursorOf(row));
        when(collection.aggregate(anyList(), eq(Document.class))).thenReturn(aggregateIterable);
        when(collection.findOneAndUpdate(any(Bson.class), any(Bson.class))).thenReturn(row);
        when(collection.findOneAndDelete(any(Bson.class))).thenReturn(row);
        return collection;
    }

    public static <T> T firstOf(final Stream<T> stream) {
        try (Stream<T> s = stream) {
            return s.toList().get(0);
        }
    }

    public static class MappedAddress {
        private String city;

        public String getCity() {
            return city;
        }

        public void setCity(final String city) {
            this.city = city;
        }
    }

    public static class MappedBox<T> {
        private T value;

        public T getValue() {
            return value;
        }

        public void setValue(final T value) {
            this.value = value;
        }
    }

    public static class LongBox extends MappedBox<Long> {
    }

    public record MappedRecord(String id, Long count, List<Long> nums, java.time.LocalDate day) {
    }

    public static class MappedEntity {
        private String id;
        private List<Long> nums;
        private List<MappedAddress> addresses;
        private java.util.Map<String, MappedAddress> addressByName;
        private java.util.Map<Integer, Long> countByYear;
        private java.time.LocalDate day;
        private java.time.LocalDateTime at;
        private java.time.LocalTime time;
        private List<java.time.LocalDate> days;
        private MappedBox<Long> box;
        private List<MappedBox<Long>> boxes;

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

        public List<MappedAddress> getAddresses() {
            return addresses;
        }

        public void setAddresses(final List<MappedAddress> addresses) {
            this.addresses = addresses;
        }

        public java.util.Map<String, MappedAddress> getAddressByName() {
            return addressByName;
        }

        public void setAddressByName(final java.util.Map<String, MappedAddress> addressByName) {
            this.addressByName = addressByName;
        }

        public java.util.Map<Integer, Long> getCountByYear() {
            return countByYear;
        }

        public void setCountByYear(final java.util.Map<Integer, Long> countByYear) {
            this.countByYear = countByYear;
        }

        public java.time.LocalDate getDay() {
            return day;
        }

        public void setDay(final java.time.LocalDate day) {
            this.day = day;
        }

        public java.time.LocalDateTime getAt() {
            return at;
        }

        public void setAt(final java.time.LocalDateTime at) {
            this.at = at;
        }

        public java.time.LocalTime getTime() {
            return time;
        }

        public void setTime(final java.time.LocalTime time) {
            this.time = time;
        }

        public List<java.time.LocalDate> getDays() {
            return days;
        }

        public void setDays(final List<java.time.LocalDate> days) {
            this.days = days;
        }

        public MappedBox<Long> getBox() {
            return box;
        }

        public void setBox(final MappedBox<Long> box) {
            this.box = box;
        }

        public List<MappedBox<Long>> getBoxes() {
            return boxes;
        }

        public void setBoxes(final List<MappedBox<Long>> boxes) {
            this.boxes = boxes;
        }
    }
}
