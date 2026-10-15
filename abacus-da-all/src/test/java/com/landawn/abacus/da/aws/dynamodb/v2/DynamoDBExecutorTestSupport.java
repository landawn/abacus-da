package com.landawn.abacus.da.aws.dynamodb.v2;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;

import com.landawn.abacus.da.MockitoTestBase;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

abstract class DynamoDBExecutorTestSupport extends MockitoTestBase {

    @Mock
    protected DynamoDbClient mockDynamoDbClient;

    protected DynamoDBExecutor executor;

    @BeforeEach
    public void setUp() {
        executor = new DynamoDBExecutor(mockDynamoDbClient);
    }

    @com.landawn.abacus.annotation.Table(name = "TestTable")
    protected static class TestEntity {
        @com.landawn.abacus.annotation.Id
        private String id;
        private String name;
        private String firstName;

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

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(String firstName) {
            this.firstName = firstName;
        }
    }

    // Bean with typed collection/map properties — used by the heap-pollution regression tests.
    protected static class TypedCollectionEntity {
        @com.landawn.abacus.annotation.Id
        private String id;
        private java.util.Set<Long> tags;
        private java.util.List<Integer> scores;
        private Map<String, Long> ratings;
        private Map<String, Object> attrs;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public java.util.Set<Long> getTags() {
            return tags;
        }

        public void setTags(java.util.Set<Long> tags) {
            this.tags = tags;
        }

        public java.util.List<Integer> getScores() {
            return scores;
        }

        public void setScores(java.util.List<Integer> scores) {
            this.scores = scores;
        }

        public Map<String, Long> getRatings() {
            return ratings;
        }

        public void setRatings(Map<String, Long> ratings) {
            this.ratings = ratings;
        }

        public Map<String, Object> getAttrs() {
            return attrs;
        }

        public void setAttrs(Map<String, Object> attrs) {
            this.attrs = attrs;
        }
    }
}
