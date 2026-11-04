package com.landawn.abacus.da.aws.dynamodb;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryResult;

import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

/**
 * Tests for the 2026-11-03 review of {@link DynamoDBExecutorBase}: the shared collection-row reader's report of a {@code NULL}
 * attribute that a null-rejecting row collection refuses (both SDK executors), and the documented failure of a dotted path
 * through a collection property of a builder-based target bean.
 */
public class DynamoDBExecutorBaseReview20261103Test {

    /** The rows of {@code v1Item} read by both executors' public {@code toList}: SDK v1 first, then the same item through SDK v2. */
    private static List<Object> readBoth(final Map<String, AttributeValue> v1Item,
            final Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> v2Item, final Class<?> rowClass) {
        return List.of(DynamoDBExecutor.toList(new QueryResult().withItems(List.of(v1Item)), rowClass).get(0),
                com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toList(QueryResponse.builder().items(List.of(v2Item)).build(), rowClass).get(0));
    }

    private static Executable rejected(final Runnable read, final String expectedMessage, final Class<? extends Throwable> expectedCause) {
        return () -> {
            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, read::run);
            assertEquals(expectedMessage, e.getMessage());
            assertInstanceOf(expectedCause, e.getCause());
        };
    }

    // Pins the 2026-11-02 fix (passes on HEAD) through the public row readers of BOTH executors, for every null-rejecting or
    // sorted row container of the 2026-11-03 cross-cut: a NULL attribute (or, for a sorted collection, a value that cannot be
    // compared with the others) is an IllegalArgumentException naming the attribute, with the JDK exception as its cause, never
    // a bare NullPointerException/ClassCastException. Containers that accept the value (TreeMap values, ArrayList) keep it.
    @Test
    public void testRowContainers_RejectedAttributeNamedInBothSdks() {
        final Map<String, AttributeValue> v1Null = new LinkedHashMap<>();
        v1Null.put("id", new AttributeValue("1"));
        v1Null.put("gone", new AttributeValue().withNULL(true));
        final Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> v2Null = new LinkedHashMap<>();
        v2Null.put("id", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS("1"));
        v2Null.put("gone", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromNul(true));

        final Map<String, AttributeValue> v1Mixed = new LinkedHashMap<>();
        v1Mixed.put("id", new AttributeValue("1"));
        v1Mixed.put("flag", new AttributeValue().withBOOL(true));
        final Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> v2Mixed = new LinkedHashMap<>();
        v2Mixed.put("id", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS("1"));
        v2Mixed.put("flag", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromBool(true));

        final List<Executable> checks = new ArrayList<>();

        for (final Class<?> rowClass : List.of(ConcurrentHashMap.class, Hashtable.class, ConcurrentSkipListMap.class)) {
            final String message = "Attribute (gone, null) is not accepted by the row map " + rowClass.getName();
            checks.add(rejected(() -> DynamoDBExecutor.toList(new QueryResult().withItems(List.of(v1Null)), rowClass), message, NullPointerException.class));
            checks.add(rejected(
                    () -> com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toList(QueryResponse.builder().items(List.of(v2Null)).build(), rowClass),
                    message, NullPointerException.class));
        }

        for (final Class<?> rowClass : List.of(ArrayBlockingQueue.class, ArrayDeque.class, ConcurrentLinkedQueue.class, TreeSet.class)) {
            final String message = "Attribute (gone, null) is not accepted by the row collection " + rowClass.getName();
            checks.add(rejected(() -> DynamoDBExecutor.toList(new QueryResult().withItems(List.of(v1Null)), rowClass), message, NullPointerException.class));
            checks.add(rejected(
                    () -> com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toList(QueryResponse.builder().items(List.of(v2Null)).build(), rowClass),
                    message, NullPointerException.class));
        }

        // A String and a Boolean cannot be compared: a sorted collection row rejects the second one.
        final String sortedMessage = "Attribute (flag, true) is not accepted by the row collection " + TreeSet.class.getName();
        checks.add(
                rejected(() -> DynamoDBExecutor.toList(new QueryResult().withItems(List.of(v1Mixed)), TreeSet.class), sortedMessage, ClassCastException.class));
        checks.add(rejected(
                () -> com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toList(QueryResponse.builder().items(List.of(v2Mixed)).build(), TreeSet.class),
                sortedMessage, ClassCastException.class));

        // Null-accepting containers are unchanged: the NULL attribute is kept.
        final Map<String, Object> expectedMap = new TreeMap<>();
        expectedMap.put("gone", null);
        expectedMap.put("id", "1");
        checks.add(() -> assertEquals(List.of(expectedMap, expectedMap), readBoth(v1Null, v2Null, TreeMap.class)));
        checks.add(() -> assertEquals(List.of(Arrays.asList("1", null), Arrays.asList("1", null)), readBoth(v1Null, v2Null, ArrayList.class)));

        assertAll(checks);
    }

    public static class Address {
        private String city;

        public String getCity() {
            return city;
        }

        public void setCity(final String city) {
            this.city = city;
        }
    }

    /** An immutable bean created through its builder, with a collection of nested beans. */
    public static final class BuiltBook {
        private final List<Address> addresses;

        BuiltBook(final List<Address> addresses) {
            this.addresses = addresses;
        }

        public List<Address> getAddresses() {
            return addresses;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private List<Address> addresses;

            public Builder addresses(final List<Address> addresses) {
                this.addresses = addresses;
                return this;
            }

            public BuiltBook build() {
                return new BuiltBook(addresses);
            }
        }
    }

    public record RecordBook(List<Address> addresses) {
    }

    // Pins the corrected toEntity Javadoc (passes on HEAD): a dotted path through a collection property is left to the
    // dependency's path setter, which reads that property from the builder of a builder-based target and so fails the item
    // with ClassCastException; a record target (whose values are its constructor arguments) gets a one-element list holding the
    // new nested bean.
    @Test
    public void testDottedPath_ThroughCollection_BuilderTargetFailsRecordTargetWorks() {
        final Map<String, AttributeValue> item = Map.of("addresses.city", new AttributeValue("Paris"));
        final Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> v2Item = Map.of("addresses.city",
                software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS("Paris"));

        assertAll(() -> assertThrows(ClassCastException.class, () -> DynamoDBExecutor.toEntity(item, BuiltBook.class)),
                () -> assertThrows(ClassCastException.class, () -> com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(v2Item, BuiltBook.class)),
                () -> {
                    final List<Address> addresses = DynamoDBExecutor.toEntity(item, RecordBook.class).addresses();
                    assertEquals(1, addresses.size());
                    assertEquals("Paris", addresses.get(0).getCity());
                });
    }

    public static class Tag {
        private String name;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    public static class Shelf {
        private List<Tag> tags;

        public List<Tag> getTags() {
            return tags;
        }

        public void setTags(final List<Tag> tags) {
            this.tags = tags;
        }
    }

    public static class MutableLibrary {
        private List<Address> addresses;

        public List<Address> getAddresses() {
            return addresses;
        }

        public void setAddresses(final List<Address> addresses) {
            this.addresses = addresses;
        }
    }

    /** A builder-based bean whose collection property sits behind a plain nested bean. */
    public static final class BuiltLibrary {
        private final Shelf shelf;

        BuiltLibrary(final Shelf shelf) {
            this.shelf = shelf;
        }

        public Shelf getShelf() {
            return shelf;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private Shelf shelf;

            public Builder shelf(final Shelf shelf) {
                this.shelf = shelf;
                return this;
            }

            public BuiltLibrary build() {
                return new BuiltLibrary(shelf);
            }
        }
    }

    // Pins the exact toEntity Javadoc wording (verifier v6, passes on HEAD): the path setter writes a dotted attribute through a
    // collection into its FIRST element (an L attribute read earlier keeps one element, now overwritten; no element is
    // appended), and in a builder-based target it fails on the first property of the path, also when the collection is further
    // down the path ("shelf.tags.name", whose first property is a plain nested bean).
    @Test
    public void testDottedPath_ThroughCollection_WritesFirstElement_BuilderFailsOnFirstProperty() {
        final Map<String, AttributeValue> v1Item = new LinkedHashMap<>();
        v1Item.put("addresses", new AttributeValue().withL(new AttributeValue().withM(Map.of("city", new AttributeValue("Rome")))));
        v1Item.put("addresses.city", new AttributeValue("Paris"));
        final Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> v2Item = new LinkedHashMap<>();
        v2Item.put("addresses",
                software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL(List.of(software.amazon.awssdk.services.dynamodb.model.AttributeValue
                        .fromM(Map.of("city", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS("Rome"))))));
        v2Item.put("addresses.city", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS("Paris"));

        final Map<String, AttributeValue> v1Deep = Map.of("shelf.tags.name", new AttributeValue("t1"));
        final Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> v2Deep = Map.of("shelf.tags.name",
                software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS("t1"));

        assertAll(() -> {
            final List<Address> addresses = DynamoDBExecutor.toEntity(v1Item, MutableLibrary.class).getAddresses();
            assertEquals(1, addresses.size());
            assertEquals("Paris", addresses.get(0).getCity());
        }, () -> {
            final List<Address> addresses = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(v2Item, MutableLibrary.class).getAddresses();
            assertEquals(1, addresses.size());
            assertEquals("Paris", addresses.get(0).getCity());
        }, () -> {
            final ClassCastException e = assertThrows(ClassCastException.class, () -> DynamoDBExecutor.toEntity(v1Deep, BuiltLibrary.class));
            assertTrue(e.getMessage().contains("Builder"), e.getMessage());
        }, () -> {
            final ClassCastException e = assertThrows(ClassCastException.class,
                    () -> com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(v2Deep, BuiltLibrary.class));
            assertTrue(e.getMessage().contains("Builder"), e.getMessage());
        });
    }
}
