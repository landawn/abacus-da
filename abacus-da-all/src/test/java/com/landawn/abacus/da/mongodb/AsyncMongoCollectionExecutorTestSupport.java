package com.landawn.abacus.da.mongodb;

import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;

import com.landawn.abacus.da.MockitoTestBase;
import com.landawn.abacus.util.AsyncExecutor;

abstract class AsyncMongoCollectionExecutorTestSupport extends MockitoTestBase {
    @Mock
    protected MongoCollectionExecutor mockCollExecutor;

    protected final AsyncExecutor directExecutor = new AsyncExecutor(Runnable::run);
    protected AsyncMongoCollectionExecutor asyncExecutor;

    @BeforeEach
    void createExecutor() {
        asyncExecutor = new AsyncMongoCollectionExecutor(mockCollExecutor, directExecutor);
    }

    protected static class TestEntity {
        protected String id;
        protected String name;

        // Getters and setters
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
