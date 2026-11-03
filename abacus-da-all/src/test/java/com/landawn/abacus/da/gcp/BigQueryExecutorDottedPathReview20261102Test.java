package com.landawn.abacus.da.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.landawn.abacus.annotation.AccessFieldByMethod;

/**
 * Regression tests for the 2026-11-02 review of {@link BigQueryExecutor}: dotted column names whose property path passes
 * through a property whose setter stores a copy of its argument.
 */
public class BigQueryExecutorDottedPathReview20261102Test {

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

    public static class PlainPerson {
        private Address address;

        public Address getAddress() {
            return address;
        }

        public void setAddress(final Address address) {
            this.address = address;
        }
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

    /** The opposite of {@link CopyingPerson}: a plain setter, but a getter that returns a new copy on every read. */
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

    /** An immutable bean created through its builder: the row is filled into the builder, which has no getters. */
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
    public void testDottedColumns_BuilderTarget_WriteIntoOneNestedBean() {
        // Before this fix (and on HEAD): ClassCastException "Cannot cast BuiltPerson$Builder to BuiltPerson", from the bean's
        // getter invoked on the builder.
        final BuiltPerson person = read(BuiltPerson.class, "id", "1", "address.city", "Paris", "address.zip", 75001L);

        assertEquals("1", person.getId());
        assertEquals("Paris", person.getAddress().getCity());
        assertEquals(75001L, person.getAddress().getZip());

        assertEquals("1", read(BuiltPerson.class, "id", "1").getId());
        assertNull(read(BuiltPerson.class, "id", "1").getAddress());
    }

    private static <T> T read(final Class<T> targetClass, final Object... namesAndValues) {
        final List<Field> fields = new ArrayList<>();
        final List<FieldValue> cells = new ArrayList<>();

        for (int i = 0; i < namesAndValues.length; i += 2) {
            final Object value = namesAndValues[i + 1];
            fields.add(Field.of((String) namesAndValues[i], value instanceof Long ? StandardSQLTypeName.INT64 : StandardSQLTypeName.STRING));
            cells.add(FieldValue.of(FieldValue.Attribute.PRIMITIVE, value == null ? null : value.toString()));
        }

        final Schema schema = Schema.of(fields);

        return BigQueryExecutor.toEntity(schema, FieldValueList.of(cells, schema.getFields()), targetClass);
    }

    @Test
    public void testDottedColumns_CopyingSetter_KeepsEveryValue() {
        final CopyingPerson person = read(CopyingPerson.class, "id", "1", "address.city", "Paris", "address.zip", 75001L);

        assertEquals("1", person.getId());
        // HEAD: the path setter wrote "Paris" into the Address it created, not into the copy the setter stored.
        assertEquals("Paris", person.getAddress().getCity());
        assertEquals(75001L, person.getAddress().getZip());
    }

    @Test
    public void testDottedColumn_CopyingSetter_SingleColumn() {
        final CopyingPerson person = read(CopyingPerson.class, "address.city", "Paris");

        assertEquals("Paris", person.getAddress().getCity());
        assertNull(person.getAddress().getZip());
    }

    @Test
    public void testDottedColumns_CopyingSetters_NestedTwoLevels() {
        final CopyingCompany company = read(CopyingCompany.class, "owner.address.city", "Oslo", "owner.id", "7");

        assertEquals("Oslo", company.getOwner().getAddress().getCity());
        assertEquals("7", company.getOwner().getId());
    }

    @Test
    public void testDottedColumns_PlainSetter_AssignsIntoOneInstance() {
        final PlainPerson person = read(PlainPerson.class, "address.city", "Rome", "address.zip", 100L);

        assertEquals("Rome", person.getAddress().getCity());
        assertEquals(100L, person.getAddress().getZip());
    }

    @Test
    public void testDottedColumn_UnresolvablePathIsIgnored() {
        final PlainPerson person = read(PlainPerson.class, "missing.city", "Rome");

        assertNull(person.getAddress());
    }

    @Test
    public void testDottedColumn_ReadOnlyLeafCreatesNoIntermediate() {
        final CopyingPerson person = read(CopyingPerson.class, "address.label", "x");

        assertNull(person.getAddress());
    }

    @Test
    public void testDottedColumns_CopyingGetter_KeepsValueOfCreatedBean() {
        final CopyingGetterPerson person = read(CopyingGetterPerson.class, "address.city", "Paris", "address.zip", 75001L);

        // The value written while the bean is created is kept, as by the dependency's path setter. A later column reaches only
        // the getter's copy: HEAD (and the previous fix) wrote into that detached copy and lost 75001; the copy is now set on
        // the owner again after the write.
        assertEquals("Paris", person.storedAddress().getCity());
        assertEquals(75001L, person.storedAddress().getZip());
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

        Address storedAddress() {
            return address;
        }
    }

    @Test
    public void testDottedColumns_CopyingAccessors_KeepEveryValue() {
        // HEAD (and the previous fix) wrote into a detached instance and kept an empty intermediate; the written instance is now
        // set again, and the setter stores a copy holding the value.
        final CopyingAccessorsPerson person = read(CopyingAccessorsPerson.class, "address.city", "Paris", "address.zip", 75001L);

        assertEquals("Paris", person.storedAddress().getCity());
        assertEquals(75001L, person.storedAddress().getZip());
    }

    @Test
    public void testDottedColumn_CopyingGetters_NestedTwoLevels() {
        final CopyingGetterCompany company = read(CopyingGetterCompany.class, "owner.address.city", "Oslo");

        assertEquals("Oslo", company.storedOwner().storedAddress().getCity());
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
    public void testDottedColumn_CollectionIntermediate_LeftWholeToPathSetter() {
        final PlainBookOwner plain = read(PlainBookOwner.class, "book.addresses.city", "Paris");

        assertEquals(1, plain.getBook().getAddresses().size());
        assertEquals("Paris", plain.getBook().getAddresses().get(0).getCity());

        final List<Address> addresses = read(CopyingGetterBookOwner.class, "book.addresses.city", "Paris").getBook().getAddresses();

        assertEquals(1, addresses == null ? 0 : addresses.size());
        assertEquals("Paris", addresses.get(0).getCity());
    }

    @Test
    public void testDottedColumns_RecordTarget_WritesIntoNestedBean() {
        final RecordPerson person = read(RecordPerson.class, "id", "1", "address.city", "Paris", "address.zip", 75001L);

        assertEquals("1", person.id());
        assertEquals("Paris", person.address().getCity());
        assertEquals(75001L, person.address().getZip());
    }

    @Test
    public void testDottedColumn_NestedRecordStillRejected() {
        // Documented: a nested bean the path has to create needs a no-argument constructor; the exception type is unchanged.
        assertThrows(IllegalArgumentException.class, () -> read(PointHolder.class, "point.x", "1"));
    }
}
