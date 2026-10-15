package com.landawn.abacus.da.aws.dynamodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.landawn.abacus.da.aws.dynamodb.DynamoDBExecutor.ConditionBuilder;
import com.landawn.abacus.da.aws.dynamodb.DynamoDBExecutor.Filters;

public class DynamoDBFiltersTest extends DynamoDBExecutorTestSupport {

    // Factories and insert operations

    // Tests for ConditionBuilder class
    @Test
    public void testConditionBuilderCreate() {
        ConditionBuilder builder = ConditionBuilder.create();
        assertNotNull(builder);
    }

    // Queries and results

    @Test
    public void testConditionBuilder_NullOrEmptyAttrNameThrowsIAE() {
        final ConditionBuilder builder = Filters.builder();

        assertThrows(IllegalArgumentException.class, () -> builder.eq(null, "v"));
        assertThrows(IllegalArgumentException.class, () -> builder.eq("", "v"));
        assertThrows(IllegalArgumentException.class, () -> builder.ne(null, "v"));
        assertThrows(IllegalArgumentException.class, () -> builder.gt(null, 1));
        assertThrows(IllegalArgumentException.class, () -> builder.ge(null, 1));
        assertThrows(IllegalArgumentException.class, () -> builder.lt(null, 1));
        assertThrows(IllegalArgumentException.class, () -> builder.le(null, 1));
        assertThrows(IllegalArgumentException.class, () -> builder.bt(null, 1, 2));
        assertThrows(IllegalArgumentException.class, () -> builder.isNull(null));
        assertThrows(IllegalArgumentException.class, () -> builder.notNull(null));
        assertThrows(IllegalArgumentException.class, () -> builder.contains(null, "v"));
        assertThrows(IllegalArgumentException.class, () -> builder.notContains(null, "v"));
        assertThrows(IllegalArgumentException.class, () -> builder.beginsWith(null, "v"));
        assertTrue(builder.build().isEmpty());
    }

    @Test
    public void testConditionBuilderAllOperations() {
        Map<String, Condition> result = new ConditionBuilder().eq("field1", "value1")
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
    }

    @Test
    public void testConditionBuilderBeginsWith() {
        Map<String, Condition> result = new ConditionBuilder().beginsWith("name", "John").build();
        assertEquals(ComparisonOperator.BEGINS_WITH.toString(), result.get("name").getComparisonOperator());
    }

    @Test
    public void testConditionBuilderBt() {
        Map<String, Condition> result = new ConditionBuilder().bt("age", 18, 65).build();
        assertEquals(ComparisonOperator.BETWEEN.toString(), result.get("age").getComparisonOperator());
        assertEquals(2, result.get("age").getAttributeValueList().size());
    }

    @Test
    public void testConditionBuilderBuild_NullifiesInternalState() {
        // After build(), internal map is nulled; calling build() again returns null.
        ConditionBuilder b = new ConditionBuilder().eq("x", 1);
        Map<String, Condition> first = b.build();
        assertNotNull(first);
        assertEquals(1, first.size());
        // Second call returns null per the implementation.
        assertNull(b.build());
    }

    @Test
    public void testConditionBuilderContains() {
        Map<String, Condition> result = new ConditionBuilder().contains("tags", "java").build();
        assertEquals(ComparisonOperator.CONTAINS.toString(), result.get("tags").getComparisonOperator());
    }

    @Test
    public void testConditionBuilderEq() {
        Map<String, Condition> result = new ConditionBuilder().eq("name", "test").build();
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(ComparisonOperator.EQ.toString(), result.get("name").getComparisonOperator());
    }

    @Test
    public void testConditionBuilderGe() {
        Map<String, Condition> result = new ConditionBuilder().ge("age", 18).build();
        assertEquals(ComparisonOperator.GE.toString(), result.get("age").getComparisonOperator());
    }

    @Test
    public void testConditionBuilderGt() {
        Map<String, Condition> result = new ConditionBuilder().gt("age", 18).build();
        assertEquals(ComparisonOperator.GT.toString(), result.get("age").getComparisonOperator());
    }

    @Test
    public void testConditionBuilderInCollection() {
        List<String> values = List.of("active", "pending", "approved");
        Map<String, Condition> result = new ConditionBuilder().in("status", values).build();
        assertEquals(ComparisonOperator.IN.toString(), result.get("status").getComparisonOperator());
        assertEquals(3, result.get("status").getAttributeValueList().size());
    }

    @Test
    public void testConditionBuilderInVarargs() {
        Map<String, Condition> result = new ConditionBuilder().in("status", "active", "pending").build();
        assertEquals(ComparisonOperator.IN.toString(), result.get("status").getComparisonOperator());
        assertEquals(2, result.get("status").getAttributeValueList().size());
    }

    @Test
    public void testConditionBuilderIsNull() {
        Map<String, Condition> result = new ConditionBuilder().isNull("optional").build();
        assertEquals(ComparisonOperator.NULL.toString(), result.get("optional").getComparisonOperator());
    }

    @Test
    public void testConditionBuilderLe() {
        Map<String, Condition> result = new ConditionBuilder().le("age", 65).build();
        assertEquals(ComparisonOperator.LE.toString(), result.get("age").getComparisonOperator());
    }

    @Test
    public void testConditionBuilderLt() {
        Map<String, Condition> result = new ConditionBuilder().lt("age", 65).build();
        assertEquals(ComparisonOperator.LT.toString(), result.get("age").getComparisonOperator());
    }

    // Individual ConditionBuilder operator tests for fine-grained coverage.
    @Test
    public void testConditionBuilderNe() {
        Map<String, Condition> result = new ConditionBuilder().ne("name", "test").build();
        assertEquals(1, result.size());
        assertEquals(ComparisonOperator.NE.toString(), result.get("name").getComparisonOperator());
    }

    @Test
    public void testConditionBuilderNotNull() {
        Map<String, Condition> result = new ConditionBuilder().notNull("required").build();
        assertEquals(ComparisonOperator.NOT_NULL.toString(), result.get("required").getComparisonOperator());
    }

    @Test
    public void testConditionBuilderNotContains() {
        Map<String, Condition> result = new ConditionBuilder().notContains("tags", "python").build();
        assertEquals(ComparisonOperator.NOT_CONTAINS.toString(), result.get("tags").getComparisonOperator());
    }

    @Test
    public void testFilters_NullOrEmptyAttrNameThrowsIAE() {
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
        assertEquals(ComparisonOperator.BEGINS_WITH.toString(), result.get("name").getComparisonOperator());
    }

    @Test
    public void testFiltersBt() {
        Map<String, Condition> result = Filters.bt("age", 18, 65);
        assertNotNull(result);
        assertEquals(ComparisonOperator.BETWEEN.toString(), result.get("age").getComparisonOperator());
        assertEquals(2, result.get("age").getAttributeValueList().size());
    }

    @Test
    public void testFiltersBt_AttributeValueContents() {
        Map<String, Condition> result = Filters.bt("age", 18, 65);
        assertEquals("18", result.get("age").getAttributeValueList().get(0).getN());
        assertEquals("65", result.get("age").getAttributeValueList().get(1).getN());
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
        assertEquals(ComparisonOperator.CONTAINS.toString(), result.get("tags").getComparisonOperator());
    }

    // Tests for Filters class
    @Test
    public void testFiltersEq() {
        Map<String, Condition> result = Filters.eq("name", "test");
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(ComparisonOperator.EQ.toString(), result.get("name").getComparisonOperator());
    }

    @Test
    public void testFiltersEq_AttributeValueContents() {
        Map<String, Condition> result = Filters.eq("name", "abc");
        assertEquals(1, result.get("name").getAttributeValueList().size());
        assertEquals("abc", result.get("name").getAttributeValueList().get(0).getS());
    }

    @Test
    public void testFiltersGe() {
        Map<String, Condition> result = Filters.ge("age", 18);
        assertNotNull(result);
        assertEquals(ComparisonOperator.GE.toString(), result.get("age").getComparisonOperator());
    }

    @Test
    public void testFiltersGt() {
        Map<String, Condition> result = Filters.gt("age", 18);
        assertNotNull(result);
        assertEquals(ComparisonOperator.GT.toString(), result.get("age").getComparisonOperator());
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
        assertEquals(ComparisonOperator.IN.toString(), result.get("status").getComparisonOperator());
        assertEquals(3, result.get("status").getAttributeValueList().size());
    }

    // Filters in() with empty collection edge case.
    @Test
    public void testFiltersInCollection_Empty() {
        assertThrows(IllegalArgumentException.class, () -> Filters.in("status", List.of()));
    }

    @Test
    public void testFiltersInVarargs() {
        Map<String, Condition> result = Filters.in("status", "active", "pending", "approved");
        assertNotNull(result);
        assertEquals(ComparisonOperator.IN.toString(), result.get("status").getComparisonOperator());
        assertEquals(3, result.get("status").getAttributeValueList().size());
    }

    @Test
    public void testFiltersInVarargs_SingleValue() {
        Map<String, Condition> result = Filters.in("status", "active");
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(1, result.get("status").getAttributeValueList().size());
    }

    @Test
    public void testFiltersIsNull() {
        Map<String, Condition> result = Filters.isNull("optional");
        assertNotNull(result);
        assertEquals(ComparisonOperator.NULL.toString(), result.get("optional").getComparisonOperator());
    }

    @Test
    public void testFiltersLe() {
        Map<String, Condition> result = Filters.le("age", 65);
        assertNotNull(result);
        assertEquals(ComparisonOperator.LE.toString(), result.get("age").getComparisonOperator());
    }

    @Test
    public void testFiltersLt() {
        Map<String, Condition> result = Filters.lt("age", 65);
        assertNotNull(result);
        assertEquals(ComparisonOperator.LT.toString(), result.get("age").getComparisonOperator());
    }

    @Test
    public void testFiltersNe() {
        Map<String, Condition> result = Filters.ne("name", "test");
        assertNotNull(result);
        assertEquals(ComparisonOperator.NE.toString(), result.get("name").getComparisonOperator());
    }

    @Test
    public void testFiltersNotNull() {
        Map<String, Condition> result = Filters.notNull("required");
        assertNotNull(result);
        assertEquals(ComparisonOperator.NOT_NULL.toString(), result.get("required").getComparisonOperator());
    }

    @Test
    public void testFiltersNotContains() {
        Map<String, Condition> result = Filters.notContains("tags", "python");
        assertNotNull(result);
        assertEquals(ComparisonOperator.NOT_CONTAINS.toString(), result.get("tags").getComparisonOperator());
    }
}
