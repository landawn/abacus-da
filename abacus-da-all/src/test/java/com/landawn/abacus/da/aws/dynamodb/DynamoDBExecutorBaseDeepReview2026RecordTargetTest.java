package com.landawn.abacus.da.aws.dynamodb;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryResult;

import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

/**
 * An entity row type needs a no-args constructor, a builder, or a constructor taking all its properties in declaration order
 * (a record's canonical constructor): records, builder-based beans and other immutable beans are built by item mapping, by a
 * nested {@code M} attribute, and by both SDKs' row readers, while a bean with none of the three is rejected. A dotted attribute
 * name, however, is assigned through the dependency's path setter, which creates a missing intermediate bean with its no-args
 * constructor and assigns into an existing one in place: a path through a nested record fails, a resolvable path into a
 * builder-based target fails, and a path through a nested mutable bean (also inside a record) is assigned. Pins the documented
 * contract of {@code DynamoDBExecutorBase.toEntity} and the async {@code targetClass} params.
 */
public class DynamoDBExecutorBaseDeepReview2026RecordTargetTest {

    // ---- 2026-10-27 sliceB ----

    public record Inner(int x, String y) {
    }

    public record Outer(String id, Inner inner) {
    }

    public static class Nested {
        private int x;

        public int getX() {
            return x;
        }

        public void setX(final int x) {
            this.x = x;
        }
    }

    public record OuterWithBean(String id, Nested nested) {
    }

    public static class BeanWithRecord {
        private Inner inner;

        public Inner getInner() {
            return inner;
        }

        public void setInner(final Inner inner) {
            this.inner = inner;
        }
    }

    /** Immutable bean built through its builder (no no-args constructor, no all-properties constructor). */
    public static final class BuiltBean {
        private final String id;
        private final int count;
        private final Nested nested;

        private BuiltBean(final String id, final int count, final Nested nested) {
            this.id = id;
            this.count = count;
            this.nested = nested;
        }

        public String getId() {
            return id;
        }

        public int getCount() {
            return count;
        }

        public Nested getNested() {
            return nested;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private String id;
            private int count;
            private Nested nested;

            public Builder id(final String id) {
                this.id = id;
                return this;
            }

            public Builder count(final int count) {
                this.count = count;
                return this;
            }

            public Builder nested(final Nested nested) {
                this.nested = nested;
                return this;
            }

            public BuiltBean build() {
                return new BuiltBean(id, count, nested);
            }
        }
    }

    /** Built through the constructor that takes all its properties in declaration order. */
    public static final class AllPropertiesBean {
        private final String id;
        private final int count;

        public AllPropertiesBean(final String id, final int count) {
            this.id = id;
            this.count = count;
        }

        public String getId() {
            return id;
        }

        public int getCount() {
            return count;
        }
    }

    /** Neither a no-args constructor, a builder, nor an all-properties constructor. */
    public static class PartialConstructorBean {
        private String id;
        private int count;

        public PartialConstructorBean(final String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public int getCount() {
            return count;
        }

        public void setCount(final int count) {
            this.count = count;
        }
    }

    private static AttributeValue s(final String value) {
        return new AttributeValue(value);
    }

    private static AttributeValue n(final String value) {
        return new AttributeValue().withN(value);
    }

    private static software.amazon.awssdk.services.dynamodb.model.AttributeValue v2(final AttributeValue v1) {
        return v1.getN() != null ? software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromN(v1.getN())
                : software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS(v1.getS());
    }

    @Test
    public void recordRowTypesAreBuiltThroughTheirConstructorUnderBothSdks() {
        final Map<String, AttributeValue> inner = Map.of("x", n("5"), "y", s("s"));
        final Map<String, AttributeValue> item = Map.of("id", s("i"), "inner", new AttributeValue().withM(inner));
        final Outer expected = new Outer("i", new Inner(5, "s"));

        final Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> v2Item = Map.of("id", v2(s("i")), "inner",
                software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(Map.of("x", v2(n("5")), "y", v2(s("s")))));

        assertAll(() -> assertEquals(expected, DynamoDBExecutor.toEntity(item, Outer.class)),
                () -> assertEquals(List.of(expected), DynamoDBExecutor.toList(new QueryResult().withItems(List.of(item)), Outer.class)),
                () -> assertEquals(expected, com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(v2Item, Outer.class)),
                () -> assertEquals(List.of(expected),
                        com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toList(QueryResponse.builder().items(List.of(v2Item)).build(), Outer.class)),
                // A record inside an ordinary bean, from an M attribute.
                () -> assertEquals(new Inner(5, "s"),
                        DynamoDBExecutor.toEntity(Map.of("inner", new AttributeValue().withM(inner)), BeanWithRecord.class).getInner()));
    }

    @Test
    public void builderAndAllPropertiesConstructorBeansNeedNoNoArgsConstructor() {
        final Map<String, AttributeValue> item = Map.of("id", s("i"), "count", n("3"), "nested", new AttributeValue().withM(Map.of("x", n("9"))));
        final Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> v2Item = Map.of("id", v2(s("i")), "count", v2(n("3")));

        assertAll(() -> {
            final BuiltBean built = DynamoDBExecutor.toEntity(item, BuiltBean.class);
            assertEquals("i", built.getId());
            assertEquals(3, built.getCount());
            assertEquals(9, built.getNested().getX());
        }, () -> assertEquals(3, com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(v2Item, BuiltBean.class).getCount()), () -> {
            final AllPropertiesBean bean = DynamoDBExecutor.toEntity(Map.of("id", s("i"), "count", n("3")), AllPropertiesBean.class);
            assertEquals("i", bean.getId());
            assertEquals(3, bean.getCount());
        }, () -> assertEquals(3, com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(v2Item, AllPropertiesBean.class).getCount()),
                // None of the three ways to build it: rejected by the dependency.
                () -> assertThrows(RuntimeException.class, () -> DynamoDBExecutor.toEntity(Map.of("id", s("i")), PartialConstructorBean.class)));
    }

    @Test
    public void dottedPathThroughANestedRecordIsRejectedWhileANestedMutableBeanIsAssigned() {
        assertAll(
                // The path setter would need a no-args Inner; the exception names the class it cannot create.
                () -> assertTrue(
                        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toEntity(Map.of("id", s("i"), "inner.x", n("5")), Outer.class))
                                .getMessage()
                                .contains("Inner")),
                () -> assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toEntity(Map.of("inner.x", n("5")), BeanWithRecord.class)),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(Map.of("id", v2(s("i")), "inner.x", v2(n("5"))), Outer.class)),
                // A mutable nested bean is created and assigned, also when the outer type is a record.
                () -> assertEquals(7, DynamoDBExecutor.toEntity(Map.of("id", s("i"), "nested.x", n("7")), OuterWithBean.class).nested().getX()),
                () -> assertEquals("i", DynamoDBExecutor.toEntity(Map.of("id", s("i"), "nested.x", n("7")), OuterWithBean.class).id()));
    }

    @Test
    public void dottedPathIntoAnExistingRecordOrABuilderTargetFailsWhileAnUnresolvablePathIsIgnored() {
        // An earlier M attribute creates the nested record; the path setter then assigns into that record in place.
        final Map<String, AttributeValue> recordThenPath = new LinkedHashMap<>();
        recordThenPath.put("inner", new AttributeValue().withM(Map.of("x", n("1"), "y", s("a"))));
        recordThenPath.put("inner.x", n("5"));

        // Both failures are the ClassCastException the toEntity Javadoc of both SDKs and of DynamoDBExecutorBase names (r2D).
        assertAll(() -> assertThrows(ClassCastException.class, () -> DynamoDBExecutor.toEntity(recordThenPath, BeanWithRecord.class)),
                // A builder-based target: the setter reads the intermediate "nested" value from the builder, so the failure is a
                // ClassCastException even though no earlier attribute set it (the SDK v1 Javadoc named IllegalArgumentException
                // for that case; outside review). Both SDKs.
                () -> assertThrows(ClassCastException.class, () -> DynamoDBExecutor.toEntity(Map.of("id", s("i"), "nested.x", n("7")), BuiltBean.class)),
                () -> assertThrows(ClassCastException.class,
                        () -> com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(Map.of("id", v2(s("i")), "nested.x", v2(n("7"))),
                                BuiltBean.class)),
                // A dotted name that resolves to no property chain is ignored, as for any other target.
                () -> {
                    final BuiltBean built = DynamoDBExecutor.toEntity(Map.of("id", s("i"), "missing.x", n("7")), BuiltBean.class);
                    assertEquals("i", built.getId());
                    assertNull(built.getNested());
                });
    }

    // ---- 2026-10-27 verify7 ----

    /** A record with a getter-like helper method: the helper is a bean property of the record (Beans scans record levels). */
    public record RecordWithGetterHelper(String id, int n) {
        public String getLabel() {
            return id + "#" + n;
        }
    }

    /**
     * Pins the toEntity Javadoc of both SDKs: a getter-only accessor is no property of a plain mutable bean, but in a record a
     * no-argument getX()/isX()/hasX() helper IS a property, so toItem writes it and, because the canonical constructor does not
     * take it, an item cannot be read into that record (UnsupportedOperationException). Doc-only: passes on HEAD too.
     */
    @Test
    public void recordGetterHelperIsWrittenAndMakesTheRecordUnreadable_verify7() {
        final Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("id", s("i"));
        item.put("n", n("3"));
        final Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> v2Item = new LinkedHashMap<>();
        v2Item.put("id", v2(s("i")));
        v2Item.put("n", v2(n("3")));
        final RecordWithGetterHelper record = new RecordWithGetterHelper("i", 3);

        assertAll(() -> assertEquals(java.util.Set.of("id", "n", "label"), DynamoDBExecutor.toItem(record).keySet()),
                () -> assertEquals(java.util.Set.of("id", "n", "label"), com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toItem(record).keySet()),
                () -> assertThrows(UnsupportedOperationException.class, () -> DynamoDBExecutor.toEntity(item, RecordWithGetterHelper.class)),
                () -> assertThrows(UnsupportedOperationException.class,
                        () -> com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(v2Item, RecordWithGetterHelper.class)),
                // A record without such a helper reads fine (its canonical constructor takes all its properties).
                () -> assertEquals(new Inner(3, "s"), DynamoDBExecutor.toEntity(Map.of("x", n("3"), "y", s("s")), Inner.class)));
    }

    // ---- 2026-10-27 r2D ----

    /** An @Entity bean whose getter without a setter is backed by a field of the same name. */
    @com.landawn.abacus.annotation.Entity
    public static class EntityWithFieldBackedGetter {
        private String id;
        private String code = "default";

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getCode() {
            return code;
        }
    }

    /** An @Entity bean whose getter without a setter computes its value (no backing field). */
    @com.landawn.abacus.annotation.Entity
    public static class EntityWithComputedGetter {
        private String id;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getLabel() {
            return "L-" + id;
        }
    }

    /** A plain bean (no @Entity) whose getter without a setter is backed by a field of the same name. */
    public static class PlainWithFieldBackedGetter {
        private String id;
        private String code = "default";

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getCode() {
            return code;
        }
    }

    /**
     * Pins the toEntity Javadoc of both SDKs (doc-only, passes on HEAD too): in an @Entity class a getter without a setter is a
     * property that toItem writes; backed by a field of the same name it is assigned through that field when an item is read,
     * while a computed one (no backing field) is read-only and its attribute is ignored. In a plain bean such a getter is no
     * property at all: neither written nor read.
     */
    @Test
    public void entityGetterWithoutSetterIsAssignedThroughItsFieldUnlessComputed_r2D() {
        final Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("id", s("i"));
        item.put("code", s("stored"));
        item.put("label", s("stored"));
        final Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> v2Item = new LinkedHashMap<>();
        v2Item.put("id", v2(s("i")));
        v2Item.put("code", v2(s("stored")));
        v2Item.put("label", v2(s("stored")));
        final EntityWithFieldBackedGetter fieldBacked = new EntityWithFieldBackedGetter();
        fieldBacked.setId("i");
        final EntityWithComputedGetter computed = new EntityWithComputedGetter();
        computed.setId("i");
        final PlainWithFieldBackedGetter plain = new PlainWithFieldBackedGetter();
        plain.setId("i");

        assertAll(() -> assertEquals(java.util.Set.of("id", "code"), DynamoDBExecutor.toItem(fieldBacked).keySet()),
                () -> assertEquals(java.util.Set.of("id", "code"), com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toItem(fieldBacked).keySet()),
                () -> assertEquals("stored", DynamoDBExecutor.toEntity(item, EntityWithFieldBackedGetter.class).getCode()),
                () -> assertEquals("stored",
                        com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(v2Item, EntityWithFieldBackedGetter.class).getCode()),
                () -> assertEquals(java.util.Set.of("id", "label"), DynamoDBExecutor.toItem(computed).keySet()),
                () -> assertEquals("L-i", DynamoDBExecutor.toEntity(item, EntityWithComputedGetter.class).getLabel()),
                () -> assertEquals("L-i", com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(v2Item, EntityWithComputedGetter.class).getLabel()),
                () -> assertEquals(java.util.Set.of("id"), DynamoDBExecutor.toItem(plain).keySet()),
                () -> assertEquals(java.util.Set.of("id"), com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toItem(plain).keySet()),
                () -> assertEquals("default", DynamoDBExecutor.toEntity(item, PlainWithFieldBackedGetter.class).getCode()), () -> assertEquals("default",
                        com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(v2Item, PlainWithFieldBackedGetter.class).getCode()));
    }
}
