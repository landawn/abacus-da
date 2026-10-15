package com.landawn.abacus.da.aws.dynamodb;

import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.landawn.abacus.da.MockitoTestBase;

abstract class DynamoDBExecutorTestSupport extends MockitoTestBase {

    @Mock
    protected AmazonDynamoDBClient mockDynamoDBClient;

    protected DynamoDBExecutor executor;

    @BeforeEach
    public void setUp() {
        executor = new DynamoDBExecutor(mockDynamoDBClient);
    }

    @com.landawn.abacus.annotation.Table(name = "TestTable")
    public static class ObjectArrayPropsEntity {
        @com.landawn.abacus.annotation.Id
        private String id;
        private String[] tags;
        private Object[] values;
        private Integer[] nums;
        private String[] emptyTags;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String[] getTags() {
            return tags;
        }

        public void setTags(final String[] tags) {
            this.tags = tags;
        }

        public Object[] getValues() {
            return values;
        }

        public void setValues(final Object[] values) {
            this.values = values;
        }

        public Integer[] getNums() {
            return nums;
        }

        public void setNums(final Integer[] nums) {
            this.nums = nums;
        }

        public String[] getEmptyTags() {
            return emptyTags;
        }

        public void setEmptyTags(final String[] emptyTags) {
            this.emptyTags = emptyTags;
        }
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

    /** A value type whose string form is empty, so it converts to an empty {@code S} key attribute. */
    public static final class BlankKeyValue_coverageDC {
        @Override
        public String toString() {
            return "";
        }
    }
}
