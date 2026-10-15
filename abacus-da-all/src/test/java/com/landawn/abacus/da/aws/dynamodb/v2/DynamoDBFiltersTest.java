package com.landawn.abacus.da.aws.dynamodb.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.ConditionBuilder;
import com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.Filters;

import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.Condition;

public class DynamoDBFiltersTest extends DynamoDBExecutorTestSupport {

    // Queries and results

    @Test
    public void testConditionBuilder_NullOrEmptyAttrNameThrowsIllegalArgument() {
        assertThrows(IllegalArgumentException.class, () -> Filters.builder().eq(null, "v"));
        assertThrows(IllegalArgumentException.class, () -> Filters.builder().eq("", "v"));
        assertThrows(IllegalArgumentException.class, () -> Filters.builder().ne(null, "v"));
        assertThrows(IllegalArgumentException.class, () -> Filters.builder().gt(null, 1));
        assertThrows(IllegalArgumentException.class, () -> Filters.builder().ge(null, 1));
        assertThrows(IllegalArgumentException.class, () -> Filters.builder().lt(null, 1));
        assertThrows(IllegalArgumentException.class, () -> Filters.builder().le(null, 1));
        assertThrows(IllegalArgumentException.class, () -> Filters.builder().bt(null, 1, 2));
        assertThrows(IllegalArgumentException.class, () -> Filters.builder().isNull(null));
        assertThrows(IllegalArgumentException.class, () -> Filters.builder().notNull(null));
        assertThrows(IllegalArgumentException.class, () -> Filters.builder().contains(null, "v"));
        assertThrows(IllegalArgumentException.class, () -> Filters.builder().notContains(null, "v"));
        assertThrows(IllegalArgumentException.class, () -> Filters.builder().beginsWith(null, "v"));
    }

    @Test
    public void testConditionBuilderAllOperations() {
        Map<String, Condition> result = Filters.builder()
                .eq("field1", "value1")
                .ne("field2", "value2")
                .gt("field3", 10)
                .ge("field4", 20)
                .lt("field5", 30)
                .le("field6", 40)
                .bt("field7", 1, 100)
                .isNull("field8")
                .notNull("field9")
                .contains("field10", "substring")
                .notContains("field11", "excluded")
                .beginsWith("field12", "prefix")
                .in("field13", "a", "b", "c")
                .build();

        assertNotNull(result);
        assertEquals(13, result.size());
        assertEquals(ComparisonOperator.EQ, result.get("field1").comparisonOperator());
        assertEquals(ComparisonOperator.NE, result.get("field2").comparisonOperator());
        assertEquals(ComparisonOperator.GT, result.get("field3").comparisonOperator());
        assertEquals(ComparisonOperator.GE, result.get("field4").comparisonOperator());
        assertEquals(ComparisonOperator.LT, result.get("field5").comparisonOperator());
        assertEquals(ComparisonOperator.LE, result.get("field6").comparisonOperator());
        assertEquals(ComparisonOperator.BETWEEN, result.get("field7").comparisonOperator());
        assertEquals(ComparisonOperator.NULL, result.get("field8").comparisonOperator());
        assertEquals(ComparisonOperator.NOT_NULL, result.get("field9").comparisonOperator());
        assertEquals(ComparisonOperator.CONTAINS, result.get("field10").comparisonOperator());
        assertEquals(ComparisonOperator.NOT_CONTAINS, result.get("field11").comparisonOperator());
        assertEquals(ComparisonOperator.BEGINS_WITH, result.get("field12").comparisonOperator());
        assertEquals(ComparisonOperator.IN, result.get("field13").comparisonOperator());
    }

    @Test
    public void testConditionBuilderBeginsWith() {
        Map<String, Condition> result = Filters.builder().beginsWith("name", "John").build();
        assertEquals(ComparisonOperator.BEGINS_WITH, result.get("name").comparisonOperator());
    }

    @Test
    public void testConditionBuilderBt() {
        Map<String, Condition> result = Filters.builder().bt("age", 18, 65).build();
        assertEquals(ComparisonOperator.BETWEEN, result.get("age").comparisonOperator());
        assertEquals(2, result.get("age").attributeValueList().size());
    }

    @Test
    public void testConditionBuilderBuild_NullifiesInternalState() {
        // After build(), the internal map is nulled; calling build() again returns null.
        ConditionBuilder b = Filters.builder().eq("x", 1);
        Map<String, Condition> first = b.build();
        assertNotNull(first);
        assertEquals(1, first.size());
        assertNull(b.build());
    }

    @Test
    public void testConditionBuilderContains() {
        Map<String, Condition> result = Filters.builder().contains("tags", "java").build();
        assertEquals(ComparisonOperator.CONTAINS, result.get("tags").comparisonOperator());
    }

    // Tests for ConditionBuilder class
    @Test
    public void testConditionBuilderEq() {
        Map<String, Condition> result = ConditionBuilder.create().eq("name", "test").build();
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(ComparisonOperator.EQ, result.get("name").comparisonOperator());
    }

    @Test
    public void testConditionBuilderGe() {
        Map<String, Condition> result = Filters.builder().ge("age", 18).build();
        assertEquals(ComparisonOperator.GE, result.get("age").comparisonOperator());
    }

    @Test
    public void testConditionBuilderGt() {
        Map<String, Condition> result = Filters.builder().gt("age", 18).build();
        assertEquals(ComparisonOperator.GT, result.get("age").comparisonOperator());
    }

    @Test
    public void testConditionBuilderInVarargsSingle() {
        Map<String, Condition> result = Filters.builder().in("status", "active").build();
        assertEquals(ComparisonOperator.IN, result.get("status").comparisonOperator());
        assertEquals(1, result.get("status").attributeValueList().size());
    }

    @Test
    public void testConditionBuilderInWithCollection() {
        List<Integer> values = List.of(1, 2, 3, 4, 5);
        Map<String, Condition> result = Filters.builder().in("numbers", values).build();

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(ComparisonOperator.IN, result.get("numbers").comparisonOperator());
        assertEquals(5, result.get("numbers").attributeValueList().size());
    }

    @Test
    public void testConditionBuilderIsNull() {
        Map<String, Condition> result = Filters.builder().isNull("optional").build();
        assertEquals(ComparisonOperator.NULL, result.get("optional").comparisonOperator());
    }

    @Test
    public void testConditionBuilderLe() {
        Map<String, Condition> result = Filters.builder().le("age", 65).build();
        assertEquals(ComparisonOperator.LE, result.get("age").comparisonOperator());
    }

    @Test
    public void testConditionBuilderLt() {
        Map<String, Condition> result = Filters.builder().lt("age", 65).build();
        assertEquals(ComparisonOperator.LT, result.get("age").comparisonOperator());
    }

    @Test
    public void testConditionBuilderMultipleConditions() {
        Map<String, Condition> result = Filters.builder().eq("status", "active").gt("age", 18).lt("score", 65).build();
        assertNotNull(result);
        assertEquals(3, result.size());
    }

    // Individual ConditionBuilder operator coverage tests.
    @Test
    public void testConditionBuilderNe() {
        Map<String, Condition> result = Filters.builder().ne("name", "test").build();
        assertEquals(ComparisonOperator.NE, result.get("name").comparisonOperator());
    }

    @Test
    public void testConditionBuilderNotNull() {
        Map<String, Condition> result = Filters.builder().notNull("required").build();
        assertEquals(ComparisonOperator.NOT_NULL, result.get("required").comparisonOperator());
    }

    @Test
    public void testConditionBuilderNotContains() {
        Map<String, Condition> result = Filters.builder().notContains("tags", "python").build();
        assertEquals(ComparisonOperator.NOT_CONTAINS, result.get("tags").comparisonOperator());
    }

    @Test
    public void testFilters_NullOrEmptyAttrNameThrowsIllegalArgument() {
        assertThrows(IllegalArgumentException.class, () -> Filters.eq(null, "v"));
        assertThrows(IllegalArgumentException.class, () -> Filters.eq("", "v"));
        assertThrows(IllegalArgumentException.class, () -> Filters.ne(null, "v"));
        assertThrows(IllegalArgumentException.class, () -> Filters.gt(null, 1));
        assertThrows(IllegalArgumentException.class, () -> Filters.ge(null, 1));
        assertThrows(IllegalArgumentException.class, () -> Filters.lt(null, 1));
        assertThrows(IllegalArgumentException.class, () -> Filters.le(null, 1));
        assertThrows(IllegalArgumentException.class, () -> Filters.bt(null, 1, 2));
        assertThrows(IllegalArgumentException.class, () -> Filters.isNull(null));
        assertThrows(IllegalArgumentException.class, () -> Filters.notNull(null));
        assertThrows(IllegalArgumentException.class, () -> Filters.contains(null, "v"));
        assertThrows(IllegalArgumentException.class, () -> Filters.notContains(null, "v"));
        assertThrows(IllegalArgumentException.class, () -> Filters.beginsWith(null, "v"));
    }

    @Test
    public void testFiltersBeginsWith() {
        Map<String, Condition> result = Filters.beginsWith("name", "John");
        assertNotNull(result);
        assertEquals(ComparisonOperator.BEGINS_WITH, result.get("name").comparisonOperator());
    }

    @Test
    public void testFiltersBt() {
        Map<String, Condition> result = Filters.bt("age", 18, 65);
        assertNotNull(result);
        assertEquals(ComparisonOperator.BETWEEN, result.get("age").comparisonOperator());
        assertEquals(2, result.get("age").attributeValueList().size());
    }

    @Test
    public void testFiltersBt_AttributeValueContents() {
        Map<String, Condition> result = Filters.bt("age", 18, 65);
        assertEquals("18", result.get("age").attributeValueList().get(0).n());
        assertEquals("65", result.get("age").attributeValueList().get(1).n());
    }

    @Test
    public void testFiltersBuilder() {
        ConditionBuilder builder = Filters.builder();
        assertNotNull(builder);
    }

    @Test
    public void testFiltersContains() {
        Map<String, Condition> result = Filters.contains("tags", "java");
        assertNotNull(result);
        assertEquals(ComparisonOperator.CONTAINS, result.get("tags").comparisonOperator());
    }

    // Tests for Filters class
    @Test
    public void testFiltersEq() {
        Map<String, Condition> result = Filters.eq("name", "test");
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(ComparisonOperator.EQ, result.get("name").comparisonOperator());
    }

    @Test
    public void testFiltersEq_AttributeValueContents() {
        Map<String, Condition> result = Filters.eq("name", "abc");
        assertEquals(1, result.get("name").attributeValueList().size());
        assertEquals("abc", result.get("name").attributeValueList().get(0).s());
    }

    @Test
    public void testFiltersGe() {
        Map<String, Condition> result = Filters.ge("age", 18);
        assertNotNull(result);
        assertEquals(ComparisonOperator.GE, result.get("age").comparisonOperator());
    }

    @Test
    public void testFiltersGt() {
        Map<String, Condition> result = Filters.gt("age", 18);
        assertNotNull(result);
        assertEquals(ComparisonOperator.GT, result.get("age").comparisonOperator());
    }

    @Test
    public void testFiltersInRejectsInvalidVarargsAndAttributeNames() {
        assertThrows(IllegalArgumentException.class, () -> Filters.in("status", new Object[0]));
        assertThrows(IllegalArgumentException.class, () -> Filters.in("status", (Object[]) null));
        assertThrows(IllegalArgumentException.class, () -> Filters.in((String) null, "active"));
        assertThrows(IllegalArgumentException.class, () -> Filters.in("", "active"));
        assertThrows(IllegalArgumentException.class, () -> Filters.in("status", (java.util.Collection<?>) null));
    }

    @Test
    public void testFiltersInCollection() {
        List<String> values = List.of("active", "pending", "approved");
        Map<String, Condition> result = Filters.in("status", values);
        assertNotNull(result);
        assertEquals(ComparisonOperator.IN, result.get("status").comparisonOperator());
        assertEquals(3, result.get("status").attributeValueList().size());
    }

    @Test
    public void testFiltersInCollection_Empty() {
        assertThrows(IllegalArgumentException.class, () -> Filters.in("status", List.of()));
    }

    @Test
    public void testFiltersInVarargs() {
        Map<String, Condition> result = Filters.in("status", "active", "pending", "approved");
        assertNotNull(result);
        assertEquals(ComparisonOperator.IN, result.get("status").comparisonOperator());
        assertEquals(3, result.get("status").attributeValueList().size());
    }

    @Test
    public void testFiltersIsNull() {
        Map<String, Condition> result = Filters.isNull("optional");
        assertNotNull(result);
        assertEquals(ComparisonOperator.NULL, result.get("optional").comparisonOperator());
    }

    @Test
    public void testFiltersLe() {
        Map<String, Condition> result = Filters.le("age", 65);
        assertNotNull(result);
        assertEquals(ComparisonOperator.LE, result.get("age").comparisonOperator());
    }

    @Test
    public void testFiltersLt() {
        Map<String, Condition> result = Filters.lt("age", 65);
        assertNotNull(result);
        assertEquals(ComparisonOperator.LT, result.get("age").comparisonOperator());
    }

    @Test
    public void testFiltersNe() {
        Map<String, Condition> result = Filters.ne("name", "test");
        assertNotNull(result);
        assertEquals(ComparisonOperator.NE, result.get("name").comparisonOperator());
    }

    @Test
    public void testFiltersNotNull() {
        Map<String, Condition> result = Filters.notNull("required");
        assertNotNull(result);
        assertEquals(ComparisonOperator.NOT_NULL, result.get("required").comparisonOperator());
    }

    @Test
    public void testFiltersNotContains() {
        Map<String, Condition> result = Filters.notContains("tags", "python");
        assertNotNull(result);
        assertEquals(ComparisonOperator.NOT_CONTAINS, result.get("tags").comparisonOperator());
    }
}
