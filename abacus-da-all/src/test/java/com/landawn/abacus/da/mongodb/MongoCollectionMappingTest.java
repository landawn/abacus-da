package com.landawn.abacus.da.mongodb;

import static com.landawn.abacus.da.mongodb.MongoMappingFixtures.DAY;
import static com.landawn.abacus.da.mongodb.MongoMappingFixtures.LOCAL_AT;
import static com.landawn.abacus.da.mongodb.MongoMappingFixtures.LOCAL_DAY;
import static com.landawn.abacus.da.mongodb.MongoMappingFixtures.LOCAL_TIME;
import static com.landawn.abacus.da.mongodb.MongoMappingFixtures.LongBox;
import static com.landawn.abacus.da.mongodb.MongoMappingFixtures.MappedEntity;
import static com.landawn.abacus.da.mongodb.MongoMappingFixtures.MappedRecord;
import static com.landawn.abacus.da.mongodb.MongoMappingFixtures.OID_HEX;
import static com.landawn.abacus.da.mongodb.MongoMappingFixtures.assertEntity;
import static com.landawn.abacus.da.mongodb.MongoMappingFixtures.assertEntityDataset;
import static com.landawn.abacus.da.mongodb.MongoMappingFixtures.dayDoc;
import static com.landawn.abacus.da.mongodb.MongoMappingFixtures.entityDoc;
import static com.landawn.abacus.da.mongodb.MongoMappingFixtures.expectedRecord;
import static com.landawn.abacus.da.mongodb.MongoMappingFixtures.firstOf;
import static com.landawn.abacus.da.mongodb.MongoMappingFixtures.longBoxDoc;
import static com.landawn.abacus.da.mongodb.MongoMappingFixtures.mockCollectionReturning;
import static com.landawn.abacus.da.mongodb.MongoMappingFixtures.recordDoc;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.util.Dataset;

public class MongoCollectionMappingTest extends MongoCollectionExecutorTestSupport {

    @Test
    public void testEntityReadPathsConvertDecodedValuesToDeclaredTypes() {
        final MongoCollectionExecutor exec = new MongoCollectionExecutor(mockCollectionReturning(entityDoc()), mockAsyncExecutor);
        final Bson filter = new Document();
        final Bson update = new Document("$set", new Document("x", 1));
        final List<Document> pipeline = Arrays.asList(new Document("$match", new Document()));
        final Class<MappedEntity> type = MappedEntity.class;

        Assertions.assertAll(() -> assertEntity("list", exec.list(filter, type).get(0)),
                () -> assertEntity("list(Collection)", exec.list(Arrays.asList("nums", "day"), filter, type).get(0)),
                () -> assertEntity("findFirst", exec.findFirst(filter, type).get()),
                () -> assertEntity("get(ObjectId)", exec.get(new ObjectId(OID_HEX), type).get()), () -> assertEntity("gett(String)", exec.gett(OID_HEX, type)),
                () -> assertEntity("stream", firstOf(exec.stream(filter, type))),
                () -> assertEntity("stream(Collection)", firstOf(exec.stream(Arrays.asList("nums", "day"), filter, type))),
                () -> assertEntity("aggregate", firstOf(exec.aggregate(pipeline, type))),
                () -> assertEntity("findOneAndUpdate", exec.findOneAndUpdate(filter, update, type)),
                () -> assertEntity("findOneAndDelete", exec.findOneAndDelete(filter, type)),
                () -> assertEntityDataset("query(Bson, Class)", exec.query(filter, type)));
    }

    @Test
    public void testGenericSubclassRowsOnEveryReadPath() {
        // LongBox extends Box<Long>: the value of its erased T field is converted to the bound Long (the previous conversion kept the Integer).
        final MongoCollectionExecutor boxExec = new MongoCollectionExecutor(mockCollectionReturning(longBoxDoc()), mockAsyncExecutor);
        final Bson filter = new Document();
        final List<Document> pipeline = Arrays.asList(new Document("$match", new Document()));
        final Class<LongBox> boxType = LongBox.class;

        Assertions.assertAll(() -> Assertions.assertEquals((Object) 9L, boxExec.list(filter, boxType).get(0).getValue(), "list"),
                () -> Assertions.assertEquals((Object) 9L, boxExec.findFirst(filter, boxType).get().getValue(), "findFirst"),
                () -> Assertions.assertEquals((Object) 9L, firstOf(boxExec.stream(filter, boxType)).getValue(), "stream"),
                () -> Assertions.assertEquals((Object) 9L, firstOf(boxExec.aggregate(pipeline, boxType)).getValue(), "aggregate"),
                () -> Assertions.assertEquals((Object) 9L, boxExec.findOneAndDelete(filter, boxType).getValue(), "findOneAndDelete"));
    }

    @Test
    public void testLocalJavaTimeSingleFieldRowsAreReadInUtcOnEveryReadPath() {
        // A document with one non-_id field read as a single value goes through readRow/toList's scalar conversion.
        final MongoCollectionExecutor dayExec = new MongoCollectionExecutor(mockCollectionReturning(dayDoc()), mockAsyncExecutor);
        final Bson filter = new Document();
        final List<Document> pipeline = Arrays.asList(new Document("$project", new Document("day", 1)));

        Assertions.assertAll(() -> Assertions.assertEquals(Arrays.asList(LOCAL_DAY), dayExec.list(filter, java.time.LocalDate.class), "list"),
                () -> Assertions.assertEquals(LOCAL_DAY, dayExec.findFirst(filter, java.time.LocalDate.class).get(), "findFirst"),
                () -> Assertions.assertEquals(LOCAL_DAY, firstOf(dayExec.stream(filter, java.time.LocalDate.class)), "stream"),
                () -> Assertions.assertEquals(LOCAL_DAY, firstOf(dayExec.aggregate(pipeline, java.time.LocalDate.class)), "aggregate"),
                () -> Assertions.assertEquals(LOCAL_DAY, dayExec.findOneAndDelete(filter, java.time.LocalDate.class), "findOneAndDelete"));
    }

    @Test
    public void testLocalJavaTimeSingleValuesAreReadInUtcOnEveryReadPath() {
        // convertBsonValue: a BSON date requested as LocalDate/LocalDateTime/LocalTime is read in UTC like the driver's codecs
        // write it (the previous conversion used the JVM zone: a LocalDate came back as the previous day west of UTC). The single-field-row paths
        // are in testLocalJavaTimeSingleFieldRowsAreReadInUtcOnEveryReadPath.
        final MongoCollectionExecutor exec = new MongoCollectionExecutor(mockCollectionReturning(entityDoc()), mockAsyncExecutor);
        final Bson filter = new Document();
        final List<String> day = Arrays.asList("day");

        Assertions.assertAll(
                () -> Assertions.assertEquals(LOCAL_DAY, exec.queryForSingleValue("day", filter, java.time.LocalDate.class).get(),
                        "queryForSingleValue LocalDate"),
                () -> Assertions.assertEquals(LOCAL_AT, exec.queryForSingleValue("at", filter, java.time.LocalDateTime.class).get(),
                        "queryForSingleValue LocalDateTime"),
                () -> Assertions.assertEquals(LOCAL_TIME, exec.queryForSingleValue("time", filter, java.time.LocalTime.class).get(),
                        "queryForSingleValue LocalTime"),
                () -> Assertions.assertEquals(LOCAL_DAY, exec.queryForSingleNonNull("day", filter, java.time.LocalDate.class).get(), "queryForSingleNonNull"),
                () -> Assertions.assertEquals(LOCAL_DAY, exec.findFirst(day, filter, java.time.LocalDate.class).get(), "findFirst(Collection)"),
                () -> Assertions.assertEquals(Arrays.asList(LOCAL_DAY), exec.list(day, filter, java.time.LocalDate.class), "list(Collection)"),
                () -> Assertions.assertEquals(LOCAL_DAY, firstOf(exec.stream(day, filter, java.time.LocalDate.class)), "stream(Collection)"),
                // Previous behavior kept: other date targets keep the instant.
                () -> Assertions.assertEquals(DAY, exec.queryForSingleValue("day", filter, Date.class).get(), "Date"),
                () -> Assertions.assertEquals(DAY.toInstant(), exec.queryForSingleValue("day", filter, java.time.Instant.class).get(), "Instant"));
    }

    @Test
    public void testRecordRowsOnEveryReadPath() {
        // Records: every value converted to its component type (int32 -> Long, List<Integer> -> List<Long>, Date -> LocalDate
        // in UTC) and the ObjectId _id passed as the String id component; the previous conversion failed with "argument type mismatch".
        final MongoCollectionExecutor recordExec = new MongoCollectionExecutor(mockCollectionReturning(recordDoc()), mockAsyncExecutor);
        final Bson filter = new Document();
        final List<Document> pipeline = Arrays.asList(new Document("$match", new Document()));
        final Class<MappedRecord> recordType = MappedRecord.class;
        final MappedRecord expected = expectedRecord();

        Assertions.assertAll(() -> Assertions.assertEquals(expected, recordExec.list(filter, recordType).get(0), "list"),
                () -> Assertions.assertEquals(expected, recordExec.findFirst(filter, recordType).get(), "findFirst"),
                () -> Assertions.assertEquals(expected, recordExec.get(OID_HEX, recordType).get(), "get(String)"),
                () -> Assertions.assertEquals(expected, firstOf(recordExec.stream(filter, recordType)), "stream"),
                () -> Assertions.assertEquals(expected, firstOf(recordExec.aggregate(pipeline, recordType)), "aggregate"),
                () -> Assertions.assertEquals(expected, recordExec.findOneAndDelete(filter, recordType), "findOneAndDelete"),
                () -> Assertions.assertEquals(Arrays.asList(4L), recordExec.query(filter, recordType).getColumn("nums").get(0), "query Dataset"));
    }

    @Test
    public void testTypedFindAndModifyWithDocumentAssignableRowTypeReturnsRawDocument() {
        // Regression: findOneAndUpdate/findOneAndReplace/findOneAndDelete(..., Object.class) threw
        // IllegalArgumentException for a multi-field document via readRow's scalar fallback.
        final Document filter = new Document("id", 1);
        final Document update = new Document("$set", new Document("a", 1));
        final Document returnedDoc = new Document("_id", new ObjectId()).append("a", 1).append("b", 2);

        when(mockCollection.findOneAndUpdate(any(Bson.class), any(Bson.class))).thenReturn(returnedDoc);
        when(mockCollection.findOneAndUpdate(any(Bson.class), anyList())).thenReturn(returnedDoc);
        when(mockCollection.findOneAndReplace(any(Bson.class), any(Document.class))).thenReturn(returnedDoc);
        when(mockCollection.findOneAndDelete(any(Bson.class))).thenReturn(returnedDoc);

        Assertions.assertSame(returnedDoc, executor.findOneAndUpdate(filter, update, Object.class));
        Assertions.assertSame(returnedDoc, executor.findOneAndUpdate(filter, Arrays.asList(update), Object.class));
        Assertions.assertSame(returnedDoc, executor.findOneAndReplace(filter, new Document("a", 1), Bson.class));
        Assertions.assertSame(returnedDoc, executor.findOneAndDelete(filter, java.util.Map.class));
        Assertions.assertSame(returnedDoc, executor.findOneAndDelete(filter, Document.class));
    }
}
