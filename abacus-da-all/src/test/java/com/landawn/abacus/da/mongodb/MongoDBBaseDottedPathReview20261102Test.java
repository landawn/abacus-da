package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.annotation.AccessFieldByMethod;

/**
 * Regression tests for the 2026-11-02 review of {@link MongoDBBase}: dotted document keys whose property path passes through a
 * property whose setter stores a copy of its argument.
 */
public class MongoDBBaseDottedPathReview20261102Test {

    public static class Address {
        private String city;
        private Long zip;

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

        public Long getZip() {
            return zip;
        }

        public void setZip(final Long zip) {
            this.zip = zip;
        }

        public String getLabel() {
            return city + "-" + zip;
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
        private String name;
        private CopyingPerson owner;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

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

    public static class PlainPerson {
        private Address address;

        public Address getAddress() {
            return address;
        }

        public void setAddress(final Address address) {
            this.address = address;
        }
    }

    public record AddressHolder(String name, Address address) {
    }

    /** An immutable bean created through its builder: the document is filled into the builder, which has no getters. */
    public static final class BuiltHolder {
        private final String name;
        private final Address address;

        BuiltHolder(final String name, final Address address) {
            this.name = name;
            this.address = address;
        }

        public String getName() {
            return name;
        }

        public Address getAddress() {
            return address;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private String name;
            private Address address;

            public Builder name(final String name) {
                this.name = name;
                return this;
            }

            public Builder address(final Address address) {
                this.address = address;
                return this;
            }

            public BuiltHolder build() {
                return new BuiltHolder(name, address);
            }
        }
    }

    @Test
    public void testDottedKeys_BuilderTarget_WriteIntoOneNestedBean() {
        // Before this fix (and on HEAD): ClassCastException "Cannot cast BuiltHolder$Builder to BuiltHolder", from the bean's
        // getter invoked on the builder.
        final BuiltHolder holder = MongoDB.toEntity(new Document("name", "n").append("address.city", "Paris").append("address.zip", 75001L), BuiltHolder.class);

        assertEquals("n", holder.getName());
        assertEquals("Paris", holder.getAddress().getCity());
        assertEquals(75001L, holder.getAddress().getZip());
        assertNull(MongoDB.toEntity(new Document("name", "n"), BuiltHolder.class).getAddress());
    }

    /** The getter returns a defensive copy; the setter stores its argument. */
    @AccessFieldByMethod
    public static class CopyingGetterPerson {
        private String name;
        private Address address;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public Address getAddress() {
            return address == null ? null : new Address(address);
        }

        public void setAddress(final Address address) {
            this.address = address;
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
            copy.setName(owner.name);
            copy.setAddress(owner.address);
            return copy;
        }

        public void setOwner(final CopyingGetterPerson owner) {
            this.owner = owner;
        }
    }

    /** Both accessors copy: no instance the path could write into is reachable; only the setter can store the value. */
    @AccessFieldByMethod
    public static class CopyingAccessorsPerson {
        private Address address;

        public Address getAddress() {
            return address == null ? null : new Address(address);
        }

        public void setAddress(final Address address) {
            this.address = address == null ? null : new Address(address);
        }
    }

    public record CopyingGetterHolder(String name, CopyingGetterPerson person) {
    }

    public record Point(String x, String y) {
    }

    public static class PointHolder {
        private Point point;

        public Point getPoint() {
            return point;
        }

        public void setPoint(final Point point) {
            this.point = point;
        }
    }

    @Test
    public void testDottedKeys_CopyingSetter_KeepsEveryValue() {
        final ObjectId oid = new ObjectId();
        final Document doc = new Document("_id", oid).append("address.city", "Paris").append("address.zip", 75001);

        final CopyingPerson person = MongoDB.toEntity(doc, CopyingPerson.class);

        assertEquals(oid.toHexString(), person.getId());
        // HEAD: the path setter wrote "Paris" into the Address it created, not into the copy the setter stored.
        assertEquals("Paris", person.getAddress().getCity());
        assertEquals(75001L, person.getAddress().getZip());
    }

    @Test
    public void testDottedKey_CopyingSetter_SingleKey() {
        final CopyingPerson person = MongoDB.toEntity(new Document("address.city", "Paris"), CopyingPerson.class);

        assertEquals("Paris", person.getAddress().getCity());
        assertNull(person.getAddress().getZip());
    }

    @Test
    public void testDottedKeys_CopyingSetters_NestedTwoLevels() {
        final Document doc = new Document("owner.address.city", "Oslo").append("owner.id", "7").append("name", "acme");

        final CopyingCompany company = MongoDB.toEntity(doc, CopyingCompany.class);

        assertEquals("acme", company.getName());
        assertEquals("Oslo", company.getOwner().getAddress().getCity());
        assertEquals("7", company.getOwner().getId());
    }

    @Test
    public void testDottedKeys_InEmbeddedDocument() {
        final Document doc = new Document("owner", new Document("address.city", "Oslo").append("address.zip", 150));

        final CopyingCompany company = MongoDB.toEntity(doc, CopyingCompany.class);

        assertEquals("Oslo", company.getOwner().getAddress().getCity());
        assertEquals(150L, company.getOwner().getAddress().getZip());
    }

    @Test
    public void testDottedKeys_PlainSetter_AssignsIntoOneInstance() {
        final PlainPerson person = MongoDB.toEntity(new Document("address.city", "Rome").append("address.zip", 100), PlainPerson.class);

        assertEquals("Rome", person.getAddress().getCity());
        assertEquals(100L, person.getAddress().getZip());
    }

    @Test
    public void testDottedKey_KeyOrderIsKept() {
        // A later whole sub-document still replaces what an earlier dotted key set; a later dotted key assigns into it.
        final CopyingPerson replaced = MongoDB.toEntity(new Document("address.city", "Paris").append("address", new Document("zip", 1)), CopyingPerson.class);

        assertNull(replaced.getAddress().getCity());
        assertEquals(1L, replaced.getAddress().getZip());

        final PlainPerson merged = MongoDB.toEntity(new Document("address", new Document("zip", 1)).append("address.city", "Paris"), PlainPerson.class);

        assertEquals("Paris", merged.getAddress().getCity());
        assertEquals(1L, merged.getAddress().getZip());
    }

    @Test
    public void testDottedKey_UnresolvablePathIsIgnored() {
        final PlainPerson person = MongoDB.toEntity(new Document("missing.city", "Rome"), PlainPerson.class);

        assertNull(person.getAddress());
    }

    @Test
    public void testDottedKey_ReadOnlyLeafCreatesNoIntermediate() {
        final CopyingPerson person = MongoDB.toEntity(new Document("address.label", "x"), CopyingPerson.class);

        assertNull(person.getAddress());
    }

    @Test
    public void testDottedKey_RecordTargetWithMutableNestedBean() {
        final AddressHolder holder = MongoDB.toEntity(new Document("name", "n").append("address.city", "Paris"), AddressHolder.class);

        assertEquals("n", holder.name());
        assertEquals("Paris", holder.address().getCity());
    }

    // A copying GETTER (plain setter): the bean created for a missing intermediate is the stored one and must receive the
    // value. HEAD kept it (the dependency wrote into the bean it created); continuing with the getter's copy lost it.

    @Test
    public void testDottedKey_CopyingGetter_KeepsValueOfCreatedBean() {
        final CopyingGetterPerson person = MongoDB.toEntity(new Document("name", "n").append("address.city", "Paris"), CopyingGetterPerson.class);

        assertEquals("n", person.getName());
        assertEquals("Paris", person.getAddress().getCity());
    }

    @Test
    public void testDottedKeys_CopyingGetters_NestedTwoLevels() {
        final CopyingGetterCompany company = MongoDB.toEntity(new Document("owner.address.city", "Oslo"), CopyingGetterCompany.class);

        assertEquals("Oslo", company.getOwner().getAddress().getCity());

        final CopyingGetterCompany named = MongoDB.toEntity(new Document("owner.name", "o"), CopyingGetterCompany.class);

        assertEquals("o", named.getOwner().getName());
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
    public void testDottedKey_CollectionIntermediate_LeftWholeToPathSetter() {
        final PlainBookOwner plain = MongoDB.toEntity(new Document("book.addresses.city", "Paris"), PlainBookOwner.class);

        assertEquals(1, plain.getBook().getAddresses().size());
        assertEquals("Paris", plain.getBook().getAddresses().get(0).getCity());

        final List<Address> addresses = MongoDB.toEntity(new Document("book.addresses.city", "Paris"), CopyingGetterBookOwner.class).getBook().getAddresses();

        assertEquals(1, addresses == null ? 0 : addresses.size());
        assertEquals("Paris", addresses.get(0).getCity());
    }

    @Test
    public void testDottedKey_RecordTarget_CopyingGetterNestedBean() {
        final CopyingGetterHolder holder = MongoDB.toEntity(new Document("name", "x").append("person.address.city", "R"), CopyingGetterHolder.class);

        assertEquals("x", holder.name());
        assertEquals("R", holder.person().getAddress().getCity());
    }

    @Test
    public void testDottedKeys_CopyingAccessors_KeepEveryValue() {
        // Neither accessor exposes the stored instance. HEAD (and the previous fix) wrote into a detached instance and kept an
        // empty intermediate; the written instance is now set again, and the setter stores a copy holding the value.
        final CopyingAccessorsPerson person = MongoDB.toEntity(new Document("address.city", "Paris"), CopyingAccessorsPerson.class);

        assertEquals("Paris", person.getAddress().getCity());

        final CopyingAccessorsPerson both = MongoDB.toEntity(new Document("address.city", "Paris").append("address.zip", 75001L), CopyingAccessorsPerson.class);

        assertEquals("Paris", both.getAddress().getCity());
        assertEquals(75001L, both.getAddress().getZip());
    }

    @Test
    public void testDottedKeys_CopyingGetter_KeepsLaterKeys() {
        // HEAD (and the previous fix) wrote the second key into the getter's detached copy and lost it.
        final CopyingGetterPerson person = MongoDB.toEntity(new Document("address.city", "Paris").append("address.zip", 75001L), CopyingGetterPerson.class);

        assertEquals("Paris", person.getAddress().getCity());
        assertEquals(75001L, person.getAddress().getZip());
    }

    @Test
    public void testDottedKey_NestedRecordStillRejected() {
        assertThrows(IllegalArgumentException.class, () -> MongoDB.toEntity(new Document("point.x", "1"), PointHolder.class));
    }
}
