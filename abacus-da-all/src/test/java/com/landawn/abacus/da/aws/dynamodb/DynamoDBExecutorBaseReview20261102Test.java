package com.landawn.abacus.da.aws.dynamodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.landawn.abacus.annotation.AccessFieldByMethod;
import com.landawn.abacus.util.Fn;

/**
 * Regression tests for the 2026-11-02 review of {@link DynamoDBExecutorBase}: dotted attribute paths through a property whose
 * setter copies its argument, and the shared Map-row filler {@link DynamoDBExecutorBase#fillMapRow(Map, Map, com.landawn.abacus.util.function.Function)}.
 */
public class DynamoDBExecutorBaseReview20261102Test {

    public static class Address {
        private String city;
        private String zip;

        public Address() {
        }

        public Address(final Address other) {
            city = other.city;
            zip = other.zip;
        }

        public String getCity() {
            return city;
        }

        public void setCity(final String city) {
            this.city = city;
        }

        public String getZip() {
            return zip;
        }

        public void setZip(final String zip) {
            this.zip = zip;
        }
    }

    /** PropInfo writes an {@code @AccessFieldByMethod} bean through its setters, and this setter stores a copy. */
    @AccessFieldByMethod
    public static class CopyingPerson {
        private String id;
        private Address address;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public Address getAddress() {
            return address;
        }

        public void setAddress(final Address address) {
            this.address = address == null ? null : new Address(address);
        }
    }

    @AccessFieldByMethod
    public static class CopyingCompany {
        private CopyingPerson owner;

        public CopyingPerson getOwner() {
            return owner;
        }

        public void setOwner(final CopyingPerson owner) {
            if (owner == null) {
                this.owner = null;
            } else {
                this.owner = new CopyingPerson();
                this.owner.setId(owner.getId());
                this.owner.setAddress(owner.getAddress());
            }
        }
    }

    /** The getter returns a defensive copy; the setter stores its argument. */
    @AccessFieldByMethod
    public static class CopyingGetterPerson {
        private Address address;

        public Address getAddress() {
            return address == null ? null : new Address(address);
        }

        public void setAddress(final Address address) {
            this.address = address;
        }

        Address storedAddress() {
            return address;
        }
    }

    @AccessFieldByMethod
    public static class CopyingGetterCompany {
        private CopyingGetterPerson owner;

        public CopyingGetterPerson getOwner() {
            if (owner == null) {
                return null;
            }

            final CopyingGetterPerson copy = new CopyingGetterPerson();
            copy.setAddress(owner.storedAddress());
            return copy;
        }

        public void setOwner(final CopyingGetterPerson owner) {
            this.owner = owner;
        }

        CopyingGetterPerson storedOwner() {
            return owner;
        }
    }

    public record RecordPerson(String id, Address address) {
    }

    /** An immutable bean created through its builder: the item is filled into the builder, which has no getters. */
    public static final class BuiltPerson {
        private final String id;
        private final Address address;

        BuiltPerson(final String id, final Address address) {
            this.id = id;
            this.address = address;
        }

        public String getId() {
            return id;
        }

        public Address getAddress() {
            return address;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private String id;
            private Address address;

            public Builder id(final String id) {
                this.id = id;
                return this;
            }

            public Builder address(final Address address) {
                this.address = address;
                return this;
            }

            public BuiltPerson build() {
                return new BuiltPerson(id, address);
            }
        }
    }

    @Test
    public void testDottedPath_BuilderTarget_WritesIntoOneNestedBean() {
        // Before this fix (and on HEAD): ClassCastException "Cannot cast BuiltPerson$Builder to BuiltPerson", from the bean's
        // getter invoked on the builder.
        final Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("id", s("1"));
        item.put("address.city", s("Paris"));
        item.put("address.zip", s("75001"));

        final BuiltPerson person = DynamoDBExecutor.toEntity(item, BuiltPerson.class);

        assertEquals("1", person.getId());
        assertEquals("Paris", person.getAddress().getCity());
        assertEquals("75001", person.getAddress().getZip());

        final software.amazon.awssdk.services.dynamodb.model.AttributeValue city = software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS("Oslo");
        assertEquals("Oslo",
                com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(Map.of("address.city", city), BuiltPerson.class).getAddress().getCity());
    }

    public static class PlainPerson {
        private Address address;

        public Address getAddress() {
            return address;
        }

        public void setAddress(final Address address) {
            this.address = address;
        }
    }

    private static AttributeValue s(final String value) {
        return new AttributeValue().withS(value);
    }

    @Test
    public void testDottedPath_CopyingSetter_KeepsFirstValue() {
        final Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("id", s("1"));
        item.put("address.city", s("Paris"));
        item.put("address.zip", s("75001"));

        final CopyingPerson person = DynamoDBExecutor.toEntity(item, CopyingPerson.class);

        assertEquals("1", person.getId());
        // HEAD: the path setter wrote "Paris" into the Address it created, not into the copy the setter stored.
        assertEquals("Paris", person.getAddress().getCity());
        assertEquals("75001", person.getAddress().getZip());
    }

    @Test
    public void testDottedPath_CopyingSetter_SingleAttribute() {
        final CopyingPerson person = DynamoDBExecutor.toEntity(Map.of("address.city", s("Paris")), CopyingPerson.class);

        assertEquals("Paris", person.getAddress().getCity());
        assertNull(person.getAddress().getZip());
    }

    @Test
    public void testDottedPath_CopyingSetters_NestedTwoLevels() {
        final Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("owner.address.city", s("Oslo"));
        item.put("owner.id", s("7"));

        final CopyingCompany company = DynamoDBExecutor.toEntity(item, CopyingCompany.class);

        assertEquals("Oslo", company.getOwner().getAddress().getCity());
        assertEquals("7", company.getOwner().getId());
    }

    @Test
    public void testDottedPath_CopyingSetter_SdkV2() {
        final Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> item = new LinkedHashMap<>();
        item.put("address.city", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS("Paris"));
        item.put("address.zip", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS("75001"));

        final CopyingPerson person = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(item, CopyingPerson.class);

        assertEquals("Paris", person.getAddress().getCity());
        assertEquals("75001", person.getAddress().getZip());
    }

    @Test
    public void testDottedPath_PlainSetter_AssignsIntoOneInstance() {
        final Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("address.city", s("Rome"));
        item.put("address.zip", s("00100"));

        final PlainPerson person = DynamoDBExecutor.toEntity(item, PlainPerson.class);

        assertEquals("Rome", person.getAddress().getCity());
        assertEquals("00100", person.getAddress().getZip());
    }

    @Test
    public void testDottedPath_CopyingGetter_KeepsValueOfCreatedBean() {
        final Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("address.city", s("Paris"));
        item.put("address.zip", s("75001"));

        final CopyingGetterPerson person = DynamoDBExecutor.toEntity(item, CopyingGetterPerson.class);

        // The value written while the bean is created is kept, as by the dependency's path setter. A later attribute reaches
        // only the getter's copy: HEAD (and the previous fix) wrote into that detached copy and lost "75001"; the copy is now
        // set on the owner again after the write.
        assertEquals("Paris", person.storedAddress().getCity());
        assertEquals("75001", person.storedAddress().getZip());
    }

    /** Both accessors copy: no instance the path could write into is reachable; only the setter can store the value. */
    @AccessFieldByMethod
    public static class CopyingAccessorsPerson {
        private Address address;

        public CopyingAccessorsPerson() {
        }

        CopyingAccessorsPerson(final CopyingAccessorsPerson other) {
            address = other.address;
        }

        public Address getAddress() {
            return address == null ? null : new Address(address);
        }

        public void setAddress(final Address address) {
            this.address = address == null ? null : new Address(address);
        }

        Address storedAddress() {
            return address;
        }
    }

    @Test
    public void testDottedPath_CopyingAccessors_KeepEveryValue() {
        // HEAD (and the previous fix) wrote into a detached instance and kept an empty intermediate; the written instance is now
        // set again, and the setter stores a copy holding the value. Both SDKs share this path.
        final Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("address.city", s("Paris"));
        item.put("address.zip", s("75001"));

        final CopyingAccessorsPerson person = DynamoDBExecutor.toEntity(item, CopyingAccessorsPerson.class);

        assertEquals("Paris", person.storedAddress().getCity());
        assertEquals("75001", person.storedAddress().getZip());
        assertEquals("Oslo", DynamoDBExecutor.toEntity(Map.of("address.city", s("Oslo")), CopyingAccessorsPerson.class).storedAddress().getCity());
    }

    /** Copies its owner on both accessors, whose address accessors copy too: two levels to set again, innermost first. */
    @AccessFieldByMethod
    public static class CopyingAccessorsCompany {
        private CopyingAccessorsPerson owner;

        public CopyingAccessorsPerson getOwner() {
            return owner == null ? null : new CopyingAccessorsPerson(owner);
        }

        public void setOwner(final CopyingAccessorsPerson owner) {
            this.owner = owner == null ? null : new CopyingAccessorsPerson(owner);
        }

        CopyingAccessorsPerson storedOwner() {
            return owner;
        }
    }

    @Test
    public void testDottedPath_CopyingAccessors_NestedTwoLevels() {
        final Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("owner.address.city", s("Oslo"));
        item.put("owner.address.zip", s("0150"));

        final CopyingAccessorsCompany company = DynamoDBExecutor.toEntity(item, CopyingAccessorsCompany.class);

        assertEquals("Oslo", company.storedOwner().storedAddress().getCity());
        assertEquals("0150", company.storedOwner().storedAddress().getZip());
    }

    @Test
    public void testDottedPath_CopyingGetters_NestedTwoLevels() {
        final CopyingGetterCompany company = DynamoDBExecutor.toEntity(Map.of("owner.address.city", s("Oslo")), CopyingGetterCompany.class);

        assertEquals("Oslo", company.storedOwner().storedAddress().getCity());
    }

    @Test
    public void testDottedPath_RecordTarget_WritesIntoNestedBean() {
        final Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("id", s("1"));
        item.put("address.city", s("Paris"));
        item.put("address.zip", s("75001"));

        final RecordPerson person = DynamoDBExecutor.toEntity(item, RecordPerson.class);

        assertEquals("1", person.id());
        assertEquals("Paris", person.address().getCity());
        assertEquals("75001", person.address().getZip());
    }

    @Test
    public void testDottedPath_UnresolvableIntermediateIsIgnored() {
        final PlainPerson person = DynamoDBExecutor.toEntity(Map.of("missing.city", s("Rome")), PlainPerson.class);

        assertNull(person.getAddress());
    }

    public static class AddressBook {
        private List<Address> addresses;

        public List<Address> getAddresses() {
            return addresses;
        }

        public void setAddresses(final List<Address> addresses) {
            this.addresses = addresses;
        }
    }

    public static class PlainBookOwner {
        private AddressBook book;

        public AddressBook getBook() {
            return book;
        }

        public void setBook(final AddressBook book) {
            this.book = book;
        }
    }

    /** The getter returns a copy that shares the stored book's address list; the setter stores its argument. */
    @AccessFieldByMethod
    public static class CopyingGetterBookOwner {
        private AddressBook book;

        public AddressBook getBook() {
            if (book == null) {
                return null;
            }

            final AddressBook copy = new AddressBook();
            copy.setAddresses(book.getAddresses());
            return copy;
        }

        public void setBook(final AddressBook book) {
            this.book = book;
        }
    }

    // Guard (passes on HEAD): a path through a collection is left whole to the dependency's path setter, which creates the book
    // and a list holding one Address that takes the value. Creating the book here first and leaving only the collection to the
    // path setter made it walk again through the copying getter, so the list went into a detached copy and the value was lost.
    @Test
    public void testDottedPath_CollectionIntermediate_LeftWholeToPathSetter() {
        final PlainBookOwner plain = DynamoDBExecutor.toEntity(Map.of("book.addresses.city", s("Paris")), PlainBookOwner.class);

        assertEquals(1, plain.getBook().getAddresses().size());
        assertEquals("Paris", plain.getBook().getAddresses().get(0).getCity());

        final List<Address> addresses = DynamoDBExecutor.toEntity(Map.of("book.addresses.city", s("Paris")), CopyingGetterBookOwner.class)
                .getBook()
                .getAddresses();

        assertEquals(1, addresses == null ? 0 : addresses.size());
        assertEquals("Paris", addresses.get(0).getCity());
    }

    @Test
    public void testFillMapRow_NullValueRejectedByConcurrentHashMap() {
        final Map<String, Object> item = new LinkedHashMap<>();
        item.put("id", "1");
        item.put("missing", null);

        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> DynamoDBExecutorBase.fillMapRow(item, new ConcurrentHashMap<>(), Fn.identity()));

        assertTrue(e.getMessage().contains("missing"), e.getMessage());
        assertTrue(e.getMessage().contains(ConcurrentHashMap.class.getName()), e.getMessage());
        assertInstanceOf(NullPointerException.class, e.getCause());
    }

    @Test
    public void testReadCollectionRow_NullValueRejectedByQueue() {
        // Before this fix: a bare NullPointerException from Collection.add, for every null-rejecting collection row class.
        final Map<String, Object> item = new LinkedHashMap<>();
        item.put("id", "1");
        item.put("missing", null);

        for (final Class<? extends java.util.Collection> rowClass : List.of(ArrayBlockingQueue.class, ArrayDeque.class, ConcurrentLinkedQueue.class)) {
            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> DynamoDBExecutorBase.readCollectionRow(item, rowClass, Fn.identity()));

            assertEquals("Attribute (missing, null) is not accepted by the row collection " + rowClass.getName(), e.getMessage());
            assertInstanceOf(NullPointerException.class, e.getCause());
        }

        // A collection that accepts null keeps the value, in item order; non-null values still fill a queue.
        assertEquals(Arrays.asList("1", null), DynamoDBExecutorBase.readCollectionRow(item, ArrayList.class, Fn.identity()));
        final Map<String, Object> twoValues = new LinkedHashMap<>();
        twoValues.put("a", "1");
        twoValues.put("b", "2");
        assertEquals(List.of("1", "2"), new ArrayList<>(DynamoDBExecutorBase.readCollectionRow(twoValues, ArrayBlockingQueue.class, Fn.identity())));
    }

    @Test
    public void testFillMapRow_KeepsOrderAndNullValues() {
        final Map<String, Object> item = new LinkedHashMap<>();
        item.put("b", "2");
        item.put("a", null);
        item.put("c", 3);
        final Map<String, Object> row = new LinkedHashMap<>();

        final Map<String, Object> result = DynamoDBExecutorBase.fillMapRow(item, row, v -> v instanceof String ? "s:" + v : v);

        assertSame(row, result);
        assertEquals(Arrays.asList("b", "a", "c"), new ArrayList<>(result.keySet()));
        assertEquals(Arrays.asList("s:2", null, 3), new ArrayList<>(result.values()));
    }

    @Test
    public void testFillMapRow_NullRow() {
        final Map<String, Object> item = new HashMap<>();
        assertNull(DynamoDBExecutorBase.fillMapRow(item, null, Fn.identity()));

        item.put("id", "1");
        assertThrows(NullPointerException.class, () -> DynamoDBExecutorBase.fillMapRow(item, null, Fn.identity()));
    }

    @Test
    public void testFillMapRow_DecoderFailurePropagates() {
        final List<String> decoded = new ArrayList<>();
        final IllegalStateException failure = new IllegalStateException("bad attribute");

        final IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> DynamoDBExecutorBase.fillMapRow(Map.of("id", "1"), new ConcurrentHashMap<>(), v -> {
                    decoded.add((String) v);
                    throw failure;
                }));

        assertSame(failure, e);
        assertEquals(List.of("1"), decoded);
    }
}
