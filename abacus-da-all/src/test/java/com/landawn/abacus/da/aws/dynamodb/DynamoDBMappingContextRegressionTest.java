package com.landawn.abacus.da.aws.dynamodb;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.landawn.abacus.annotation.JsonXmlField;

public class DynamoDBMappingContextRegressionTest {
    @Test
    public void legacyNativeBeansAndArraysRetainTheirOriginalMappingContexts() {
        final AttributeValue direct = legacyLeaf(true);
        final AttributeValue json = legacyLeaf(false);
        final ContextHolder holder = DynamoDBExecutor.toEntity(Map.of("direct", direct, "array", new AttributeValue().withL(direct), "list",
                new AttributeValue().withL(json), "arrays", new AttributeValue().withL(new AttributeValue().withL(json))), ContextHolder.class);

        assertContexts(holder);
    }

    @Test
    public void v2NativeBeansAndArraysRetainTheirOriginalMappingContexts() {
        final var direct = v2Leaf(true);
        final var json = v2Leaf(false);
        final ContextHolder holder = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor
                .toEntity(
                        Map.of("direct", direct, "array", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL(List.of(direct)), "list",
                                software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL(List.of(json)), "arrays",
                                software.amazon.awssdk.services.dynamodb.model.AttributeValue
                                        .fromL(List.of(software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL(List.of(json))))),
                        ContextHolder.class);

        assertContexts(holder);
    }

    private static AttributeValue legacyLeaf(final boolean databaseMapping) {
        final AttributeValue date = new AttributeValue().withS(databaseMapping ? "2026-12-31" : "31/12/2026");
        final AttributeValue readOnly = new AttributeValue().withS("from-database");
        final AttributeValue child = new AttributeValue().withM(Map.of("date", date, "readOnly", readOnly));
        return new AttributeValue()
                .withM(Map.of("date", date, "readOnly", readOnly, "child", child, "path.value", new AttributeValue().withS("from-database")));
    }

    private static software.amazon.awssdk.services.dynamodb.model.AttributeValue v2Leaf(final boolean databaseMapping) {
        final var date = software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS(databaseMapping ? "2026-12-31" : "31/12/2026");
        final var readOnly = software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS("from-database");
        final var child = software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(Map.of("date", date, "readOnly", readOnly));
        return software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(Map.of("date", date, "readOnly", readOnly, "child", child, "path.value",
                software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS("from-database")));
    }

    private static void assertContexts(final ContextHolder holder) {
        assertLeaf(holder.getDirect(), true);
        assertLeaf(holder.getArray()[0], true);
        assertLeaf(holder.getList().get(0), false);
        assertLeaf(holder.getArrays().get(0)[0], false);
    }

    private static void assertLeaf(final ContextLeaf leaf, final boolean databaseMapping) {
        final String expected = databaseMapping ? "from-database" : "default";
        assertEquals(LocalDate.of(2026, 12, 31), leaf.getDate());
        assertEquals(LocalDate.of(2026, 12, 31), leaf.getChild().getDate());
        assertEquals(expected, leaf.getReadOnly());
        assertEquals(expected, leaf.getChild().getReadOnly());
        assertEquals(expected, leaf.getPath().getValue());
    }

    public static class ContextHolder {
        private ContextLeaf direct;
        private ContextLeaf[] array;
        private List<ContextLeaf> list;
        private List<ContextLeaf[]> arrays;

        public ContextLeaf getDirect() {
            return direct;
        }

        public void setDirect(final ContextLeaf value) {
            direct = value;
        }

        public ContextLeaf[] getArray() {
            return array;
        }

        public void setArray(final ContextLeaf[] value) {
            array = value;
        }

        public List<ContextLeaf> getList() {
            return list;
        }

        public void setList(final List<ContextLeaf> value) {
            list = value;
        }

        public List<ContextLeaf[]> getArrays() {
            return arrays;
        }

        public void setArrays(final List<ContextLeaf[]> value) {
            arrays = value;
        }
    }

    public static class ContextLeaf {
        @JsonXmlField(direction = JsonXmlField.Direction.SERIALIZE_ONLY)
        private String readOnly = "default";
        @JsonXmlField(dateFormat = "dd/MM/uuuu")
        private LocalDate date;
        private ContextLeaf child;
        private PathLeaf path = new PathLeaf();

        public String getReadOnly() {
            return readOnly;
        }

        public void setReadOnly(final String value) {
            readOnly = value;
        }

        public LocalDate getDate() {
            return date;
        }

        public void setDate(final LocalDate value) {
            date = value;
        }

        public ContextLeaf getChild() {
            return child;
        }

        public void setChild(final ContextLeaf value) {
            child = value;
        }

        public PathLeaf getPath() {
            return path;
        }

        public void setPath(final PathLeaf value) {
            path = value;
        }
    }

    public static class PathLeaf {
        private String value = "default";

        public String getValue() {
            return value;
        }

        public void setValue(final String value) {
            this.value = value;
        }
    }
}
