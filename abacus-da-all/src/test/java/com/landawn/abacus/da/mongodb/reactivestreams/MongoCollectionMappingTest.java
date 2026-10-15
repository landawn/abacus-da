package com.landawn.abacus.da.mongodb.reactivestreams;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.util.Dataset;

public class MongoCollectionMappingTest extends MongoCollectionExecutorTestSupport {

    @Test
    public void testEntityReadPathsConvertDecodedValuesToDeclaredTypes() {
        final MongoCollectionExecutor exec = new MongoCollectionExecutor(
                mockCollectionReturning(com.landawn.abacus.da.mongodb.MongoMappingFixtures.entityDoc()));
        final Bson filter = new Document();
        final Bson update = new Document("$set", new Document("x", 1));
        final List<Document> pipeline = Arrays.asList(new Document("$match", new Document()));
        final var type = com.landawn.abacus.da.mongodb.MongoMappingFixtures.MappedEntity.class;
        final String oid = com.landawn.abacus.da.mongodb.MongoMappingFixtures.OID_HEX;

        org.junit.jupiter.api.Assertions.assertAll(
                () -> com.landawn.abacus.da.mongodb.MongoMappingFixtures.assertEntity("list", exec.list(filter, type).blockFirst()),
                () -> com.landawn.abacus.da.mongodb.MongoMappingFixtures.assertEntity("list(Collection)",
                        exec.list(Arrays.asList("nums", "day"), filter, type).blockFirst()),
                () -> com.landawn.abacus.da.mongodb.MongoMappingFixtures.assertEntity("findFirst", exec.findFirst(filter, type).block()),
                () -> com.landawn.abacus.da.mongodb.MongoMappingFixtures.assertEntity("get(ObjectId)", exec.get(new ObjectId(oid), type).block()),
                () -> com.landawn.abacus.da.mongodb.MongoMappingFixtures.assertEntity("get(String)", exec.get(oid, type).block()),
                () -> com.landawn.abacus.da.mongodb.MongoMappingFixtures.assertEntity("aggregate", exec.aggregate(pipeline, type).blockFirst()),
                () -> com.landawn.abacus.da.mongodb.MongoMappingFixtures.assertEntity("findOneAndUpdate", exec.findOneAndUpdate(filter, update, type).block()),
                () -> com.landawn.abacus.da.mongodb.MongoMappingFixtures.assertEntity("findOneAndDelete", exec.findOneAndDelete(filter, type).block()),
                () -> com.landawn.abacus.da.mongodb.MongoMappingFixtures.assertEntityDataset("query(Bson, Class)", exec.query(filter, type).block()));
    }

    @Test
    public void testGenericSubclassRowsOnEveryReadPath() {
        final MongoCollectionExecutor exec = new MongoCollectionExecutor(
                mockCollectionReturning(com.landawn.abacus.da.mongodb.MongoMappingFixtures.longBoxDoc()));
        final Bson filter = new Document();
        final List<Document> pipeline = Arrays.asList(new Document("$match", new Document()));
        final var type = com.landawn.abacus.da.mongodb.MongoMappingFixtures.LongBox.class;

        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals((Object) 9L, exec.list(filter, type).blockFirst().getValue(), "list"),
                () -> assertEquals((Object) 9L, exec.findFirst(filter, type).block().getValue(), "findFirst"),
                () -> assertEquals((Object) 9L, exec.aggregate(pipeline, type).blockFirst().getValue(), "aggregate"),
                () -> assertEquals((Object) 9L, exec.findOneAndDelete(filter, type).block().getValue(), "findOneAndDelete"));
    }

    @Test
    public void testLocalJavaTimeSingleFieldRowsAreReadInUtcOnEveryReadPath() {
        final MongoCollectionExecutor exec = new MongoCollectionExecutor(mockCollectionReturning(com.landawn.abacus.da.mongodb.MongoMappingFixtures.dayDoc()));
        final Bson filter = new Document();
        final List<Document> pipeline = Arrays.asList(new Document("$project", new Document("day", 1)));
        final java.time.LocalDate day = com.landawn.abacus.da.mongodb.MongoMappingFixtures.LOCAL_DAY;

        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals(day, exec.list(filter, java.time.LocalDate.class).blockFirst(), "list"),
                () -> assertEquals(day, exec.findFirst(filter, java.time.LocalDate.class).block(), "findFirst"),
                () -> assertEquals(day, exec.aggregate(pipeline, java.time.LocalDate.class).blockFirst(), "aggregate"),
                () -> assertEquals(day, exec.findOneAndDelete(filter, java.time.LocalDate.class).block(), "findOneAndDelete"));
    }

    @Test
    public void testLocalJavaTimeSingleValuesAreReadInUtcOnEveryReadPath() {
        final MongoCollectionExecutor exec = new MongoCollectionExecutor(
                mockCollectionReturning(com.landawn.abacus.da.mongodb.MongoMappingFixtures.entityDoc()));
        final Bson filter = new Document();
        final java.time.LocalDate day = com.landawn.abacus.da.mongodb.MongoMappingFixtures.LOCAL_DAY;
        final Date dayDate = com.landawn.abacus.da.mongodb.MongoMappingFixtures.DAY;

        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertEquals(day, exec.queryForSingleValue("day", filter, java.time.LocalDate.class).block(), "queryForSingleValue LocalDate"),
                () -> assertEquals(com.landawn.abacus.da.mongodb.MongoMappingFixtures.LOCAL_AT,
                        exec.queryForSingleValue("at", filter, java.time.LocalDateTime.class).block(), "queryForSingleValue LocalDateTime"),
                () -> assertEquals(com.landawn.abacus.da.mongodb.MongoMappingFixtures.LOCAL_TIME,
                        exec.queryForSingleValue("time", filter, java.time.LocalTime.class).block(), "queryForSingleValue LocalTime"),
                () -> assertEquals(day, exec.findFirst(Arrays.asList("day"), filter, java.time.LocalDate.class).block(), "findFirst(Collection)"),
                () -> assertEquals(day, exec.list(Arrays.asList("day"), filter, java.time.LocalDate.class).blockFirst(), "list(Collection)"),
                // Previous behavior kept: other date targets keep the instant.
                () -> assertEquals(dayDate, exec.queryForSingleValue("day", filter, Date.class).block(), "Date"),
                () -> assertEquals(dayDate.toInstant(), exec.queryForSingleValue("day", filter, java.time.Instant.class).block(), "Instant"));
    }

    @Test
    public void testRecordRowsOnEveryReadPath() {
        final MongoCollectionExecutor exec = new MongoCollectionExecutor(
                mockCollectionReturning(com.landawn.abacus.da.mongodb.MongoMappingFixtures.recordDoc()));
        final Bson filter = new Document();
        final List<Document> pipeline = Arrays.asList(new Document("$match", new Document()));
        final var type = com.landawn.abacus.da.mongodb.MongoMappingFixtures.MappedRecord.class;
        final Object expected = com.landawn.abacus.da.mongodb.MongoMappingFixtures.expectedRecord();

        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals(expected, exec.list(filter, type).blockFirst(), "list"),
                () -> assertEquals(expected, exec.findFirst(filter, type).block(), "findFirst"),
                () -> assertEquals(expected, exec.get(com.landawn.abacus.da.mongodb.MongoMappingFixtures.OID_HEX, type).block(), "get(String)"),
                () -> assertEquals(expected, exec.aggregate(pipeline, type).blockFirst(), "aggregate"),
                () -> assertEquals(expected, exec.findOneAndDelete(filter, type).block(), "findOneAndDelete"),
                () -> assertEquals(Arrays.asList(4L), exec.query(filter, type).block().getColumn("nums").get(0), "query Dataset"));
    }
}
