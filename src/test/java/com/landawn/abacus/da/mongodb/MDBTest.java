package com.landawn.abacus.da.mongodb;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.BasicBSONObject;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.stream.Stream;
import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;

public class MDBTest extends TestBase {

    @Mock
    private FindIterable<Document> mockFindIterable;

    @Mock
    private MongoCursor<Document> mockCursor;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testRegisterIdProperty() {
        // Test deprecated method - should not throw exception
        MongoDBBase.registerIdProperty(TestEntity.class, "id");
    }

    @Test
    public void testRegisterIdPropertyWithInvalidProperty() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            MongoDBBase.registerIdProperty(TestEntity.class, "nonExistentProperty");
        });
    }

    @Test
    public void testObjectId2FilterWithString() {
        String objectId = "507f1f77bcf86cd799439011";
        Bson result = MongoDBBase.objectId2Filter(objectId);

        Assertions.assertNotNull(result);
        Document doc = (Document) result;
        Assertions.assertTrue(doc.get("_id") instanceof ObjectId);
    }

    @Test
    public void testObjectId2FilterWithObjectId() {
        ObjectId objectId = new ObjectId();
        Bson result = MongoDBBase.objectId2Filter(objectId);

        Assertions.assertNotNull(result);
        Document doc = (Document) result;
        Assertions.assertEquals(objectId, doc.get("_id"));
    }

    @Test
    public void testObjectId2FilterWithNullString() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            MongoDBBase.objectId2Filter((String) null);
        });
    }

    @Test
    public void testObjectId2FilterWithEmptyString() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            MongoDBBase.objectId2Filter("");
        });
    }

    @Test
    public void testObjectId2FilterWithNullObjectId() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            MongoDBBase.objectId2Filter((ObjectId) null);
        });
    }

    @Test
    public void testFromJsonToDocument() {
        String json = "{\"name\":\"test\",\"value\":123}";
        Document result = MongoDBBase.fromJson(json, Document.class);

        Assertions.assertEquals("test", result.getString("name"));
        Assertions.assertEquals(123, result.getInteger("value"));
    }

    @Test
    public void testFromJsonToBson() {
        String json = "{\"name\":\"test\"}";
        Bson result = MongoDBBase.fromJson(json, Bson.class);

        Assertions.assertNotNull(result);
        Assertions.assertTrue(result instanceof Document);
    }

    @Test
    public void testFromJsonToBasicBSONObject() {
        String json = "{\"name\":\"test\"}";
        BasicBSONObject result = MongoDBBase.fromJson(json, BasicBSONObject.class);

        Assertions.assertEquals("test", result.getString("name"));
    }

    @Test
    public void testFromJsonToBasicDBObject() {
        String json = "{\"name\":\"test\"}";
        BasicDBObject result = MongoDBBase.fromJson(json, BasicDBObject.class);

        Assertions.assertEquals("test", result.getString("name"));
    }

    @Test
    public void testFromJsonWithUnsupportedType() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            MongoDBBase.fromJson("{}", String.class);
        });
    }

    @Test
    public void testToJsonWithBson() {
        Document doc = new Document("name", "test").append("value", 123);
        String result = MongoDBBase.toJson(doc);

        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.contains("\"name\":\"test\""));
        Assertions.assertTrue(result.contains("\"value\":123"));
    }

    @Test
    public void testToJsonWithBasicBSONObject() {
        BasicBSONObject obj = new BasicBSONObject("name", "test");
        String result = MongoDBBase.toJson(obj);

        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.contains("\"name\":\"test\""));
    }

    @Test
    public void testToJsonWithBasicDBObject() {
        BasicDBObject obj = new BasicDBObject("name", "test");
        String result = MongoDBBase.toJson(obj);

        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.contains("\"name\":\"test\""));
    }

    @Test
    public void testToBsonWithObject() {
        TestEntity entity = new TestEntity();
        entity.setId("123");
        entity.setName("test");

        Bson result = MongoDBBase.toBson(entity);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testToBsonWithVarargs() {
        Bson result = MongoDBBase.toBson("name", "test", "value", 123);
        Document doc = (Document) result;

        Assertions.assertEquals("test", doc.getString("name"));
        Assertions.assertEquals(123, doc.getInteger("value"));
    }

    @Test
    public void testToBsonWithEmptyVarargs() {
        Bson result = MongoDBBase.toBson();
        Document doc = (Document) result;

        Assertions.assertTrue(doc.isEmpty());
    }

    @Test
    public void testToDocumentWithMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("name", "test");
        map.put("value", 123);

        Document result = MongoDBBase.toDocument(map);

        Assertions.assertEquals("test", result.getString("name"));
        Assertions.assertEquals(123, result.getInteger("value"));
    }

    @Test
    public void testToDocumentWithEntity() {
        TestEntity entity = new TestEntity();
        entity.setId("123");
        entity.setName("test");

        Document result = MongoDBBase.toDocument(entity);

        Assertions.assertEquals("test", result.getString("name"));
    }

    @Test
    public void testToDocumentWithNullIdDoesNotSetNullObjectId() {
        TestEntity entity = new TestEntity();
        entity.setId(null);
        entity.setName("test");

        Document result = MongoDBBase.toDocument(entity);

        Assertions.assertFalse(result.containsKey("_id"));
    }

    @Test
    public void testToDocumentWithVarargs() {
        Document result = MongoDBBase.toDocument("name", "test", "value", 123);

        Assertions.assertEquals("test", result.getString("name"));
        Assertions.assertEquals(123, result.getInteger("value"));
    }

    @Test
    public void testToDocumentWithOddNumberOfArgs() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            MongoDBBase.toDocument("name", "test", "value");
        });
    }

    @Test
    public void testToDocumentWithInvalidObject() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            MongoDBBase.toDocument(new Object());
        });
    }

    @Test
    public void testToBSONObjectWithMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("name", "test");

        BasicBSONObject result = MongoDBBase.toBSONObject(map);

        Assertions.assertEquals("test", result.getString("name"));
    }

    @Test
    public void testToBSONObjectWithEntity() {
        TestEntity entity = new TestEntity();
        entity.setName("test");

        BasicBSONObject result = MongoDBBase.toBSONObject(entity);

        Assertions.assertEquals("test", result.getString("name"));
    }

    @Test
    public void testToBSONObjectWithVarargs() {
        BasicBSONObject result = MongoDBBase.toBSONObject("name", "test", "value", 123);

        Assertions.assertEquals("test", result.getString("name"));
        Assertions.assertEquals(123, result.get("value"));
    }

    @Test
    public void testToDBObjectWithMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("name", "test");

        BasicDBObject result = MongoDBBase.toDBObject(map);

        Assertions.assertEquals("test", result.getString("name"));
    }

    @Test
    public void testToDBObjectWithEntity() {
        TestEntity entity = new TestEntity();
        entity.setName("test");

        BasicDBObject result = MongoDBBase.toDBObject(entity);

        Assertions.assertEquals("test", result.getString("name"));
    }

    @Test
    public void testToDBObjectWithVarargs() {
        BasicDBObject result = MongoDBBase.toDBObject("name", "test", "value", 123);

        Assertions.assertEquals("test", result.getString("name"));
        Assertions.assertEquals(123, result.get("value"));
    }

    @Test
    public void testToMap() {
        Document doc = new Document("name", "test").append("value", 123);

        Map<String, Object> result = MongoDBBase.toMap(doc);

        Assertions.assertEquals("test", result.get("name"));
        Assertions.assertEquals(123, result.get("value"));
    }

    @Test
    public void testToMapWithSupplier() {
        Document doc = new Document("name", "test");

        Map<String, Object> result = MongoDBBase.toMap(doc, HashMap::new);

        Assertions.assertTrue(result instanceof HashMap);
        Assertions.assertEquals("test", result.get("name"));
    }

    @Test
    public void testToEntity() {
        Document doc = new Document("name", "test").append("_id", new ObjectId());

        TestEntity result = MongoDBBase.toEntity(doc, TestEntity.class);

        Assertions.assertEquals("test", result.getName());
    }

    @Test
    public void testToListWithDocuments() {
        List<Document> docs = Arrays.asList(new Document("id", 1), new Document("id", 2));
        when(mockFindIterable.into(any())).thenReturn(docs);

        List<Document> result = MongoDBBase.toList(mockFindIterable, Document.class);

        Assertions.assertEquals(2, result.size());
    }

    @Test
    public void testToListWithEntityClass() {
        List<Document> docs = Arrays.asList(new Document("name", "test1"), new Document("name", "test2"));
        when(mockFindIterable.into(any())).thenReturn(docs);

        List<TestEntity> result = MongoDBBase.toList(mockFindIterable, TestEntity.class);

        Assertions.assertEquals(2, result.size());
    }

    @Test
    public void testToListWithEmptyResult() {
        when(mockFindIterable.into(any())).thenReturn(Arrays.asList());

        List<TestEntity> result = MongoDBBase.toList(mockFindIterable, TestEntity.class);

        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testExtractDataWithFindIterable() {
        List<Document> docs = Arrays.asList(new Document("id", 1).append("name", "test1"), new Document("id", 2).append("name", "test2"));
        when(mockFindIterable.into(any())).thenReturn(docs);

        Dataset result = MongoDBBase.extractData(mockFindIterable);

        Assertions.assertNotNull(result);
    }

    @Test
    public void testExtractDataWithRowType() {
        List<Document> docs = Arrays.asList(new Document("id", 1).append("name", "test1"));
        when(mockFindIterable.into(any())).thenReturn(docs);

        Dataset result = MongoDBBase.extractData(mockFindIterable, TestEntity.class);

        Assertions.assertNotNull(result);
    }

    @Test
    public void testExtractDataWithSelectPropNames() {
        List<Document> docs = Arrays.asList(new Document("id", 1).append("name", "test1"));
        when(mockFindIterable.into(any())).thenReturn(docs);

        Dataset result = MongoDBBase.extractData(Arrays.asList("id", "name"), mockFindIterable, Map.class);

        Assertions.assertNotNull(result);
    }

    @Test
    public void testExtractDataFromList() {
        List<Document> docs = Arrays.asList(new Document("id", 1).append("name", "test1"));

        Dataset result = MongoDBBase.extractData(docs);

        Assertions.assertNotNull(result);
    }

    @Test
    public void testStreamWithMongoIterable() {
        when(mockFindIterable.iterator()).thenReturn(mockCursor);

        Stream<Document> result = MongoDBBase.stream(mockFindIterable);

        Assertions.assertNotNull(result);
    }

    @Test
    public void testStreamWithMongoIterableAndRowType() {
        when(mockFindIterable.iterator()).thenReturn(mockCursor);

        Stream<TestEntity> result = MongoDBBase.stream(mockFindIterable, TestEntity.class);

        Assertions.assertNotNull(result);
    }

    @Test
    public void testStreamWithMongoCursor() {
        Stream<Document> result = MongoDBBase.stream(mockCursor);

        Assertions.assertNotNull(result);
    }

    @Test
    public void testStreamWithMongoCursorAndRowType() {
        Stream<TestEntity> result = MongoDBBase.stream(mockCursor, TestEntity.class);

        Assertions.assertNotNull(result);
    }

    // Test entity class
    private static class TestEntity {
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

    // Additional test class without proper getter/setter
    private static class InvalidEntity {
        private String field;
    }
}
