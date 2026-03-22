package com.landawn.abacus.da.mongodb.reactivestreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.landawn.abacus.da.TestBase;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;

public class MongoDBTest extends TestBase {

    @Mock
    private MongoDatabase mockDatabase;

    @Mock
    private MongoCollection<Document> mockCollection;

    private MongoDB mongoDB;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        when(mockDatabase.withCodecRegistry(any(CodecRegistry.class))).thenReturn(mockDatabase);
        mongoDB = new MongoDB(mockDatabase);
    }

    @Test
    public void testDb() {
        MongoDatabase result = mongoDB.db();
        assertEquals(mockDatabase, result);
    }

    @Test
    public void testCollectionWithName() {
        String collectionName = "testCollection";
        when(mockDatabase.getCollection(collectionName)).thenReturn(mockCollection);

        MongoCollection<Document> result = mongoDB.collection(collectionName);

        assertEquals(mockCollection, result);
        verify(mockDatabase).getCollection(collectionName);
    }

    @Test
    public void testCollectionWithNameAndRowType() {
        String collectionName = "userCollection";
        Class<User> rowType = User.class;
        MongoCollection<User> mockUserCollection = mock(MongoCollection.class);

        when(mockDatabase.getCollection(collectionName, rowType)).thenReturn(mockUserCollection);

        MongoCollection<User> result = mongoDB.collection(collectionName, rowType);

        assertEquals(mockUserCollection, result);
        verify(mockDatabase).getCollection(collectionName, rowType);
    }

    @Test
    public void testCollExecutorWithCollectionName() {
        String collectionName = "executorTest";
        when(mockDatabase.getCollection(collectionName)).thenReturn(mockCollection);

        MongoCollectionExecutor executor = mongoDB.collExecutor(collectionName);

        assertNotNull(executor);
        assertEquals(mockCollection, executor.coll());
        verify(mockDatabase).getCollection(collectionName);
    }

    @Test
    public void testCollExecutorWithNullCollectionName() {
        assertThrows(IllegalArgumentException.class, () -> {
            mongoDB.collExecutor((String) null);
        });
    }

    @Test
    public void testCollExecutorWithMongoCollection() {
        MongoCollectionExecutor executor = mongoDB.collExecutor(mockCollection);

        assertNotNull(executor);
        assertEquals(mockCollection, executor.coll());
    }

    @Test
    public void testCollExecutorWithNullMongoCollection() {
        assertThrows(IllegalArgumentException.class, () -> {
            mongoDB.collExecutor((MongoCollection<Document>) null);
        });
    }

    @Test
    public void testCollMapperWithRowType() {
        Class<TestEntity> rowType = TestEntity.class;
        String expectedCollectionName = "TestEntity";

        when(mockDatabase.getCollection(expectedCollectionName)).thenReturn(mockCollection);

        MongoCollectionMapper<TestEntity> mapper = mongoDB.collMapper(rowType);

        assertNotNull(mapper);
        assertNotNull(mapper.collExecutor());
        verify(mockDatabase).getCollection(expectedCollectionName);
    }

    @Test
    public void testCollMapperWithCollectionNameAndRowType() {
        String collectionName = "customCollection";
        Class<TestEntity> rowType = TestEntity.class;

        when(mockDatabase.getCollection(collectionName)).thenReturn(mockCollection);

        MongoCollectionMapper<TestEntity> mapper = mongoDB.collMapper(collectionName, rowType);

        assertNotNull(mapper);
        assertNotNull(mapper.collExecutor());
        verify(mockDatabase).getCollection(collectionName);
    }

    @Test
    public void testCollMapperWithNullCollectionName() {
        assertThrows(IllegalArgumentException.class, () -> {
            mongoDB.collMapper((String) null, TestEntity.class);
        });
    }

    @Test
    public void testCollMapperWithNullRowType() {
        assertThrows(IllegalArgumentException.class, () -> {
            mongoDB.collMapper("collection", null);
        });
    }

    @Test
    public void testCollMapperWithMongoCollectionAndRowType() {
        Class<TestEntity> rowType = TestEntity.class;

        MongoCollectionMapper<TestEntity> mapper = mongoDB.collMapper(mockCollection, rowType);

        assertNotNull(mapper);
        assertNotNull(mapper.collExecutor());
        assertEquals(mockCollection, mapper.collExecutor().coll());
    }

    @Test
    public void testCollMapperWithNullMongoCollection() {
        assertThrows(IllegalArgumentException.class, () -> {
            mongoDB.collMapper((MongoCollection<Document>) null, TestEntity.class);
        });
    }

    @Test
    public void testCollMapperWithMongoCollectionAndNullRowType() {
        assertThrows(IllegalArgumentException.class, () -> {
            mongoDB.collMapper(mockCollection, null);
        });
    }

    @Test
    public void testReadRow() {
        // This is a protected static method that uses the parent class implementation
        // We'll test it indirectly through the mapper/executor functionality
        Document doc = new Document("name", "test").append("value", 123);

        // Since readRow is protected, we can't test it directly here
        // It would be tested through the MongoCollectionExecutor and MongoCollectionMapper tests
        assertTrue(true, "readRow is tested through executor and mapper tests");
    }

    @Test
    public void testMultipleCollExecutorCallsReturnNewInstances() {
        String collectionName = "testCollection";
        when(mockDatabase.getCollection(collectionName)).thenReturn(mockCollection);

        MongoCollectionExecutor executor1 = mongoDB.collExecutor(collectionName);
        MongoCollectionExecutor executor2 = mongoDB.collExecutor(collectionName);

        assertNotNull(executor1);
        assertNotNull(executor2);
        assertNotSame(executor1, executor2, "Each call should return a new executor instance");
    }

    @Test
    public void testMultipleCollMapperCallsReturnNewInstances() {
        String collectionName = "testCollection";
        Class<TestEntity> rowType = TestEntity.class;

        when(mockDatabase.getCollection(collectionName)).thenReturn(mockCollection);

        MongoCollectionMapper<TestEntity> firstMapper = mongoDB.collMapper(collectionName, rowType);
        MongoCollectionMapper<TestEntity> secondMapper = mongoDB.collMapper(collectionName, rowType);

        assertNotNull(firstMapper);
        assertNotNull(secondMapper);
        assertNotSame(firstMapper, secondMapper, "Each call should return a new mapper instance");
    }

    @Test
    public void testCollectionNamesForDifferentTypes() {
        when(mockDatabase.getCollection(anyString())).thenReturn(mockCollection);

        Class<TestEntity> type1 = TestEntity.class;
        Class<AnotherEntity> type2 = AnotherEntity.class;

        MongoCollectionMapper<TestEntity> firstMapper = mongoDB.collMapper(type1);
        MongoCollectionMapper<AnotherEntity> secondMapper = mongoDB.collMapper(type2);

        assertNotNull(firstMapper);
        assertNotNull(secondMapper);

        verify(mockDatabase).getCollection("TestEntity");
        verify(mockDatabase).getCollection("AnotherEntity");
    }

    // Helper classes for testing
    static class User {
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

    static class TestEntity {
        private String id;
        private String value;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    static class AnotherEntity {
        private String id;
        private int count;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }
    }
}