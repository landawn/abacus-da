package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.bson.BasicBSONObject;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.IntFunctions;
import com.landawn.abacus.util.stream.Stream;
import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;

/**
 * Additional tests for MongoDBBase targeting low-coverage branches/methods
 * such as toList variants, readRow, resetObjectId edge cases, type-conversion
 * paths in extractData, and ObjectId reset for Date/byte[]/String inputs.
 */
public class MongoDBBaseTest extends TestBase {

    @Mock
    private FindIterable<Document> mockFindIterable;

    @Mock
    private MongoCursor<Document> mockCursor;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    // -- toMap edge cases --

    @Test
    public void testToMapWithTreeMapSupplier() {
        Document doc = new Document("b", 1).append("a", 2);
        Map<String, Object> result = MongoDBBase.toMap(doc, IntFunctions.ofMap(TreeMap.class));

        assertTrue(result instanceof TreeMap);
        assertEquals(2, result.size());
        assertEquals(1, result.get("b"));
    }

    @Test
    public void testToMapWithLinkedHashMapSupplier() {
        Document doc = new Document("first", 1).append("second", 2);
        Map<String, Object> result = MongoDBBase.toMap(doc, IntFunctions.ofMap(LinkedHashMap.class));

        assertTrue(result instanceof LinkedHashMap);
        assertEquals(2, result.size());
    }

    // -- toEntity edge cases --

    @Test
    public void testToEntityWithNullDoc() {
        TestEntity result = MongoDBBase.toEntity(null, TestEntity.class);
        assertNull(result);
    }

    @Test
    public void testToEntityWithStringIdConvertedFromObjectId() {
        // Entity has String id; when document _id is ObjectId, it should be converted to its string form
        ObjectId oid = new ObjectId();
        Document doc = new Document("_id", oid).append("name", "test");

        TestEntity result = MongoDBBase.toEntity(doc, TestEntity.class);

        assertNotNull(result);
        assertEquals("test", result.getName());
        assertEquals(oid.toHexString(), result.getId());
    }

    @Test
    public void testToEntityPreservesNullObjectIdFieldInSourceDocument() {
        Document doc = new Document("_id", null).append("name", "test");

        TestEntity result = MongoDBBase.toEntity(doc, TestEntity.class);

        assertNotNull(result);
        assertEquals("test", result.getName());
        assertTrue(doc.containsKey("_id"));
        assertNull(doc.get("_id"));
    }

    @Test
    public void testToEntityWithObjectIdField() {
        // Entity with ObjectId field type must assign ObjectId directly
        ObjectId oid = new ObjectId();
        Document doc = new Document("_id", oid).append("name", "alpha");

        ObjectIdEntity result = MongoDBBase.toEntity(doc, ObjectIdEntity.class);

        assertNotNull(result);
        assertEquals(oid, result.getId());
        assertEquals("alpha", result.getName());
    }

    @Test
    public void testToEntityWithoutIdFieldOnEntity() {
        // NoIdEntity has no id getter/setter; toEntity must still work
        Document doc = new Document("value", "x");
        NoIdEntity result = MongoDBBase.toEntity(doc, NoIdEntity.class);
        assertNotNull(result);
        assertEquals("x", result.getValue());
    }

    // -- toList variants --

    @Test
    public void testToListWithMapClass() {
        // Documents are Maps already so the cast path returns them unchanged
        List<Document> docs = Arrays.asList(new Document("id", 1), new Document("id", 2));
        when(mockFindIterable.into(any())).thenReturn(docs);

        @SuppressWarnings("rawtypes")
        List<Map> result = MongoDBBase.toList(mockFindIterable, Map.class);

        assertEquals(2, result.size());
    }

    @Test
    public void testToListWithSingleValueExtraction() {
        // A doc with one non-_id field, requested as plain type (String) -- readRow takes value path
        List<Document> docs = Arrays.asList(new Document("name", "alice"), new Document("name", "bob"));
        when(mockFindIterable.into(any())).thenReturn(docs);

        List<String> result = MongoDBBase.toList(mockFindIterable, String.class);

        assertEquals(2, result.size());
        assertEquals("alice", result.get(0));
    }

    @Test
    public void testToListPrimitiveExtractionWithConversion() {
        // Documents have integer value; requested as Long -- needs convert path
        List<Document> docs = Arrays.asList(new Document("v", 10), new Document("v", 20));
        when(mockFindIterable.into(any())).thenReturn(docs);

        List<Long> result = MongoDBBase.toList(mockFindIterable, Long.class);

        assertEquals(2, result.size());
        assertEquals(10L, result.get(0));
        assertEquals(20L, result.get(1));
    }

    @Test
    public void testToListWithDocumentsAlreadyMatchingType() {
        // Documents returned exactly match rowType -> fast path returning rowList as-is
        List<Document> docs = Arrays.asList(new Document("a", 1));
        when(mockFindIterable.into(any())).thenReturn(docs);

        List<Document> result = MongoDBBase.toList(mockFindIterable, Document.class);

        assertEquals(1, result.size());
        assertEquals(1, result.get(0).getInteger("a"));
    }

    // -- extractData edge cases --

    @Test
    public void testExtractDataWithSelectPropNamesFromList() {
        // When selectPropNames is provided and rows are Maps
        List<Document> rows = Arrays.asList(new Document("a", 1).append("b", 2), new Document("a", 3).append("b", 4));

        Dataset result = MongoDBBase.extractData(Arrays.asList("a"), rows, Map.class);

        assertNotNull(result);
        assertEquals(2, result.size());
    }

    @Test
    public void testExtractDataWithEmptyList() {
        Dataset result = MongoDBBase.extractData(Collections.emptyList());
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    @Test
    public void testExtractDataFromListWithDocumentsAndEntityType() {
        // Document rows being converted to an entity row type
        List<Document> docs = Arrays.asList(new Document("value", "a"), new Document("value", "b"));

        Dataset result = MongoDBBase.extractData(docs, NoIdEntity.class);
        assertNotNull(result);
        assertEquals(2, result.size());
    }

    @Test
    public void testExtractDataFromListNonMapAndNonDocument() {
        // Plain bean list with explicit selectPropNames goes through the else branch
        List<NoIdEntity> beans = new ArrayList<>();
        NoIdEntity e1 = new NoIdEntity();
        e1.setValue("x");
        beans.add(e1);

        Dataset result = MongoDBBase.extractData(Arrays.asList("value"), beans, NoIdEntity.class);

        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    public void testExtractDataWithUnsupportedRowTypeThrows() {
        // checkResultClass should reject non-bean, non-Map types
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.extractData(mockFindIterable, String.class));
    }

    // -- toDocument/toBSONObject/toDBObject varargs branches --

    @Test
    public void testToDocumentEmptyVarargsYieldsEmptyDoc() {
        Document doc = MongoDBBase.toDocument();
        assertTrue(doc.isEmpty());
    }

    @Test
    public void testToBSONObjectEmptyVarargsYieldsEmpty() {
        BasicBSONObject obj = MongoDBBase.toBSONObject();
        assertTrue(obj.isEmpty());
    }

    @Test
    public void testToDBObjectEmptyVarargsYieldsEmpty() {
        BasicDBObject obj = MongoDBBase.toDBObject();
        assertTrue(obj.isEmpty());
    }

    @Test
    public void testToBSONObjectWithOddVarargsThrows() {
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toBSONObject("only-name", 1, "extra"));
    }

    @Test
    public void testToDBObjectWithOddVarargsThrows() {
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toDBObject("only-name", 1, "extra"));
    }

    @Test
    public void testToBSONObjectWithUnsupportedThrows() {
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toBSONObject(new Object()));
    }

    @Test
    public void testToDBObjectWithUnsupportedThrows() {
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toDBObject(new Object()));
    }

    // -- resetObjectId branches via toDocument (Map-based String/Date/byte[] keyed as _id) --

    @Test
    public void testResetObjectIdMapWithStringIdHexConvertedToObjectId() {
        // When the input is a Map with a String _id holding a valid 24-hex ObjectId
        // resetObjectId converts the String to an ObjectId on the result document.
        Map<String, Object> map = new HashMap<>();
        ObjectId expected = new ObjectId();
        map.put("_id", expected.toHexString());
        map.put("name", "n");

        Document doc = MongoDBBase.toDocument(map);

        assertTrue(doc.containsKey("_id"));
        assertTrue(doc.get("_id") instanceof ObjectId);
        assertEquals(expected, doc.get("_id"));
    }

    @Test
    public void testResetObjectIdMapWithDateIdConvertedToObjectId() {
        Map<String, Object> map = new HashMap<>();
        Date when = new Date();
        map.put("_id", when);
        map.put("name", "d");

        Document doc = MongoDBBase.toDocument(map);

        assertTrue(doc.containsKey("_id"));
        assertTrue(doc.get("_id") instanceof ObjectId);
    }

    @Test
    public void testResetObjectIdMapWithByteArrayIdConvertedToObjectId() {
        Map<String, Object> map = new HashMap<>();
        byte[] id12 = new byte[12];
        for (int i = 0; i < 12; i++) {
            id12[i] = (byte) i;
        }
        map.put("_id", id12);
        map.put("name", "b");

        Document doc = MongoDBBase.toDocument(map);

        assertTrue(doc.containsKey("_id"));
        assertTrue(doc.get("_id") instanceof ObjectId);
    }

    // -- registerIdProperty branches --

    @Test
    public void testRegisterIdPropertyWithObjectIdPropertyType() {
        // Should accept an ObjectId-typed setter
        MongoDBBase.registerIdProperty(ObjectIdEntity.class, "id");
    }

    @Test
    public void testRegisterIdPropertyWithUnsupportedTypeThrows() {
        // Wrong setter type -- registerIdProperty should reject it
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.registerIdProperty(IntIdEntity.class, "id"));
    }

    // -- toJson Bson Map branch --

    @Test
    public void testToJsonWithBsonMapBranch() {
        // Document is a Map, so toJson(Bson) takes the Map branch
        Document doc = new Document("k", "v");
        String json = MongoDBBase.toJson((Bson) doc);
        assertNotNull(json);
        assertTrue(json.contains("\"k\""));
    }

    // -- stream wrappers --

    @Test
    public void testStreamFromCursorYieldsValidStream() {
        Stream<Document> s = MongoDBBase.stream(mockCursor);
        assertNotNull(s);
        s.close();
    }

    @Test
    public void testStreamFromCursorWithRowType() {
        Stream<TestEntity> s = MongoDBBase.stream(mockCursor, TestEntity.class);
        assertNotNull(s);
        s.close();
    }

    // -- toList edge cases targeting uncovered branches --

    @Test
    public void testToListLargeDocSingleValueRejected() {
        // A document with more than 2 fields cannot be converted to a primitive type
        List<Document> docs = Arrays.asList(new Document("a", 1).append("b", 2).append("c", 3));
        when(mockFindIterable.into(any())).thenReturn(docs);

        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toList(mockFindIterable, Integer.class));
    }

    @Test
    public void testToListWithNullElementsReturnsEmpty() {
        // No non-null first => returns empty list
        when(mockFindIterable.into(any())).thenReturn(new ArrayList<>());
        List<String> result = MongoDBBase.toList(mockFindIterable, String.class);
        assertEquals(0, result.size());
    }

    // -- toBson convenience method (delegates to toDocument) --

    @Test
    public void testToBsonObjectDelegatesToDocument() {
        Bson result = MongoDBBase.toBson("k", "v");
        assertNotNull(result);
        Document d = (Document) result;
        assertEquals("v", d.getString("k"));
    }

    // -- objectId2Filter with invalid hex string --

    @Test
    public void testObjectId2FilterWithInvalidHexStringThrows() {
        // The string is not a valid 24-hex ObjectId
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.objectId2Filter("not-an-objectid"));
    }

    // -- GeneralCodec tests (package-private inner class) --

    @Test
    public void testGeneralCodecGetEncoderClassForEntity() {
        MongoDBBase.GeneralCodec<TestEntity> codec = new MongoDBBase.GeneralCodec<>(TestEntity.class);
        assertEquals(TestEntity.class, codec.getEncoderClass());
    }

    @Test
    public void testGeneralCodecGetEncoderClassForNonEntity() {
        MongoDBBase.GeneralCodec<String> codec = new MongoDBBase.GeneralCodec<>(String.class);
        assertEquals(String.class, codec.getEncoderClass());
    }

    @Test
    public void testGeneralCodecEncodeDecodeEntity() {
        // Round-trip an entity through the codec via a BSON document
        MongoDBBase.GeneralCodec<TestEntity> codec = new MongoDBBase.GeneralCodec<>(TestEntity.class);

        TestEntity in = new TestEntity();
        in.setName("alice");

        // Encode into a BsonDocument
        org.bson.BsonDocument bsonDoc = new org.bson.BsonDocument();
        org.bson.BsonDocumentWriter writer = new org.bson.BsonDocumentWriter(bsonDoc);
        codec.encode(writer, in, org.bson.codecs.EncoderContext.builder().build());

        // Decode back from the BsonDocument
        org.bson.BsonDocumentReader reader = new org.bson.BsonDocumentReader(bsonDoc);
        TestEntity out = codec.decode(reader, org.bson.codecs.DecoderContext.builder().build());

        assertNotNull(out);
        assertEquals("alice", out.getName());
    }

    @Test
    public void testGeneralCodecEncodeDecodeNonEntityString() {
        // Non-bean classes are written/read as plain strings
        MongoDBBase.GeneralCodec<String> codec = new MongoDBBase.GeneralCodec<>(String.class);

        org.bson.BsonDocument wrap = new org.bson.BsonDocument();
        org.bson.BsonDocumentWriter writer = new org.bson.BsonDocumentWriter(wrap);
        writer.writeStartDocument();
        writer.writeName("v");
        codec.encode(writer, "hello", org.bson.codecs.EncoderContext.builder().build());
        writer.writeEndDocument();

        org.bson.BsonDocumentReader reader = new org.bson.BsonDocumentReader(wrap);
        reader.readStartDocument();
        reader.readName();
        String decoded = codec.decode(reader, org.bson.codecs.DecoderContext.builder().build());
        reader.readEndDocument();

        assertEquals("hello", decoded);
    }

    @Test
    public void testGeneralCodecEncodeDecodeNonEntityInteger() {
        // Integer is encoded as its string form, then valueOf re-parses
        MongoDBBase.GeneralCodec<Integer> codec = new MongoDBBase.GeneralCodec<>(Integer.class);

        org.bson.BsonDocument wrap = new org.bson.BsonDocument();
        org.bson.BsonDocumentWriter writer = new org.bson.BsonDocumentWriter(wrap);
        writer.writeStartDocument();
        writer.writeName("v");
        codec.encode(writer, 42, org.bson.codecs.EncoderContext.builder().build());
        writer.writeEndDocument();

        org.bson.BsonDocumentReader reader = new org.bson.BsonDocumentReader(wrap);
        reader.readStartDocument();
        reader.readName();
        Integer decoded = codec.decode(reader, org.bson.codecs.DecoderContext.builder().build());
        reader.readEndDocument();

        assertEquals(42, decoded);
    }

    // -- GeneralCodecRegistry tests (package-private inner class) --

    @Test
    public void testGeneralCodecRegistryGetReturnsCodec() {
        MongoDBBase.GeneralCodecRegistry registry = new MongoDBBase.GeneralCodecRegistry();
        org.bson.codecs.Codec<TestEntity> codec = registry.get(TestEntity.class);

        assertNotNull(codec);
        assertEquals(TestEntity.class, codec.getEncoderClass());
    }

    @Test
    public void testGeneralCodecRegistryGetCachesPerClass() {
        // Two lookups for the same class should return the same instance (pooled)
        MongoDBBase.GeneralCodecRegistry registry = new MongoDBBase.GeneralCodecRegistry();
        org.bson.codecs.Codec<TestEntity> first = registry.get(TestEntity.class);
        org.bson.codecs.Codec<TestEntity> second = registry.get(TestEntity.class);

        assertNotNull(first);
        assertNotNull(second);
        assertTrue(first == second);
    }

    @Test
    public void testGeneralCodecRegistryGetWithRegistryDelegates() {
        // The two-arg get(Class, CodecRegistry) delegates to the one-arg get
        MongoDBBase.GeneralCodecRegistry registry = new MongoDBBase.GeneralCodecRegistry();
        org.bson.codecs.Codec<String> codec = registry.get(String.class, registry);

        assertNotNull(codec);
        assertEquals(String.class, codec.getEncoderClass());
    }

    @Test
    public void testGeneralCodecRegistryGetForNonEntityType() {
        MongoDBBase.GeneralCodecRegistry registry = new MongoDBBase.GeneralCodecRegistry();
        org.bson.codecs.Codec<Long> codec = registry.get(Long.class);

        assertNotNull(codec);
        assertEquals(Long.class, codec.getEncoderClass());
    }

    // -- Entities used by tests --

    public static class TestEntity {
        private String id;
        private String name;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static class ObjectIdEntity {
        private ObjectId id;
        private String name;

        public ObjectId getId() {
            return id;
        }

        public void setId(ObjectId id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static class NoIdEntity {
        private String value;

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    public static class IntIdEntity {
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
}
