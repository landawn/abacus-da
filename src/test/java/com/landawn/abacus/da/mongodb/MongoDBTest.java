package com.landawn.abacus.da.mongodb;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.util.AsyncExecutor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoDBTest extends TestBase {

    @Mock
    private MongoDatabase mockMongoDatabase;

    @Mock
    private MongoCollection<Document> mockCollection;

    @Mock
    private AsyncExecutor mockAsyncExecutor;

    private MongoDB mongoDB;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        when(mockMongoDatabase.withCodecRegistry(any())).thenReturn(mockMongoDatabase);
        mongoDB = new MongoDB(mockMongoDatabase);
    }

    @Test
    public void testConstructorWithDatabase() {
        MongoDB db = new MongoDB(mockMongoDatabase);
        Assertions.assertNotNull(db);
    }

    @Test
    public void testConstructorWithDatabaseAndAsyncExecutor() {
        MongoDB db = new MongoDB(mockMongoDatabase, mockAsyncExecutor);
        Assertions.assertNotNull(db);
    }

    @Test
    public void testDb() {
        MongoDatabase result = mongoDB.db();
        Assertions.assertNotNull(result);
    }

    @Test
    public void testCollectionWithName() {
        String collectionName = "testCollection";
        when(mockMongoDatabase.getCollection(collectionName)).thenReturn(mockCollection);

        MongoCollection<Document> result = mongoDB.collection(collectionName);
        Assertions.assertSame(mockCollection, result);
        verify(mockMongoDatabase).getCollection(collectionName);
    }

    @Test
    public void testCollectionWithNameAndRowType() {
        String collectionName = "testCollection";
        MongoCollection<TestEntity> mockTypedCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection(collectionName, TestEntity.class)).thenReturn(mockTypedCollection);

        MongoCollection<TestEntity> result = mongoDB.collection(collectionName, TestEntity.class);
        Assertions.assertSame(mockTypedCollection, result);
        verify(mockMongoDatabase).getCollection(collectionName, TestEntity.class);
    }

    @Test
    public void testCollExecutorWithCollectionName() {
        String collectionName = "testCollection";
        when(mockMongoDatabase.getCollection(collectionName)).thenReturn(mockCollection);

        MongoCollectionExecutor result = mongoDB.collExecutor(collectionName);
        Assertions.assertNotNull(result);
        verify(mockMongoDatabase).getCollection(collectionName);
    }

    @Test
    public void testCollExecutorWithCollection() {
        MongoCollectionExecutor result = mongoDB.collExecutor(mockCollection);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testCollExecutorWithNullCollectionName() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            mongoDB.collExecutor((String) null);
        });
    }

    @Test
    public void testCollExecutorWithNullCollection() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            mongoDB.collExecutor((MongoCollection<Document>) null);
        });
    }

    @Test
    public void testCollMapperWithRowType() {
        when(mockMongoDatabase.getCollection("TestEntity")).thenReturn(mockCollection);

        MongoCollectionMapper<TestEntity> result = mongoDB.collMapper(TestEntity.class);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testCollMapperWithCollectionNameAndRowType() {
        String collectionName = "customCollection";
        when(mockMongoDatabase.getCollection(collectionName)).thenReturn(mockCollection);

        MongoCollectionMapper<TestEntity> result = mongoDB.collMapper(collectionName, TestEntity.class);
        Assertions.assertNotNull(result);
        verify(mockMongoDatabase).getCollection(collectionName);
    }

    @Test
    public void testCollMapperWithCollectionAndRowType() {
        MongoCollectionMapper<TestEntity> result = mongoDB.collMapper(mockCollection, TestEntity.class);
        Assertions.assertNotNull(result);
    }

    //    @Test
    //    public void testCollMapperWithNullCollectionName() {
    //        Assertions.assertThrows(IllegalArgumentException.class, () -> {
    //            mongoDB.collMapper(null, TestEntity.class);
    //        });
    //    }

    @Test
    public void testCollMapperWithNullRowType() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            mongoDB.collMapper("collection", null);
        });
    }

    @Test
    public void testCollMapperWithNullCollection() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            mongoDB.collMapper((MongoCollection<Document>) null, TestEntity.class);
        });
    }

    @Test
    public void testCollMapperWithCollectionAndNullRowType() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            mongoDB.collMapper(mockCollection, null);
        });
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
}