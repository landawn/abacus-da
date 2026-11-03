package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.landawn.abacus.annotation.AccessFieldByMethod;
import com.landawn.abacus.util.ImmutableList;

class CassandraExecutorReview20261102Test {

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

    public static class PlainPerson {
        private Address address;

        public Address getAddress() {
            return address;
        }

        public void setAddress(final Address address) {
            this.address = address;
        }
    }

    /** A plain setter, and a getter that returns a defensive copy on every read. */
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
    void dottedColumns_builderTarget_writeIntoOneNestedBean() {
        // Before this fix (and on HEAD): ClassCastException "Cannot cast BuiltPerson$Builder to BuiltPerson", from the bean's
        // getter invoked on the builder.
        final BuiltPerson person = CassandraExecutor.toEntity(row(new String[] { "id", "address.city", "address.zip" }, "1", "Paris", "75001"),
                BuiltPerson.class);

        assertEquals("1", person.getId());
        assertEquals("Paris", person.getAddress().getCity());
        assertEquals("75001", person.getAddress().getZip());
    }

    // The value written while the intermediate bean is created is kept, as by the dependency's path setter. A later column
    // reaches only the getter's copy: HEAD (and the previous fix) wrote into that detached copy and lost "75001"; the copy is
    // now set on the owner again after the write.
    @Test
    void dottedColumns_copyingGetter_keepValueOfCreatedBean() {
        final Row row = row(new String[] { "address.city", "address.zip" }, "Paris", "75001");

        final CopyingGetterPerson person = CassandraExecutor.toEntity(row, CopyingGetterPerson.class);

        assertEquals("Paris", person.storedAddress().getCity());
        assertEquals("75001", person.storedAddress().getZip());
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

    // HEAD (and the previous fix) wrote into a detached instance and kept an empty intermediate; the written instance is now set
    // again, and the setter stores a copy holding the value.
    @Test
    void dottedColumns_copyingAccessors_keepEveryValue() {
        final CopyingAccessorsPerson person = CassandraExecutor.toEntity(row(new String[] { "address.city", "address.zip" }, "Paris", "75001"),
                CopyingAccessorsPerson.class);

        assertEquals("Paris", person.storedAddress().getCity());
        assertEquals("75001", person.storedAddress().getZip());
    }

    @Test
    void dottedColumn_copyingGetters_twoLevels() {
        final CopyingGetterCompany company = CassandraExecutor.toEntity(row(new String[] { "owner.address.city" }, "Oslo"), CopyingGetterCompany.class);

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
    void dottedColumn_collectionIntermediate_leftWholeToPathSetter() {
        final PlainBookOwner plain = CassandraExecutor.toEntity(row(new String[] { "book.addresses.city" }, "Paris"), PlainBookOwner.class);

        assertEquals(1, plain.getBook().getAddresses().size());
        assertEquals("Paris", plain.getBook().getAddresses().get(0).getCity());

        final List<Address> addresses = CassandraExecutor.toEntity(row(new String[] { "book.addresses.city" }, "Paris"), CopyingGetterBookOwner.class)
                .getBook()
                .getAddresses();

        assertNotNull(addresses);
        assertEquals(1, addresses.size());
        assertEquals("Paris", addresses.get(0).getCity());
    }

    @Test
    void dottedColumns_recordTarget_writeIntoNestedBean() {
        final RecordPerson person = CassandraExecutor.toEntity(row(new String[] { "id", "address.city", "address.zip" }, "1", "Paris", "75001"),
                RecordPerson.class);

        assertEquals("1", person.id());
        assertEquals("Paris", person.address().getCity());
        assertEquals("75001", person.address().getZip());
    }

    @Test
    void dottedColumns_copyingSetter_keepFirstValue() {
        final Row row = row(new String[] { "id", "address.city", "address.zip" }, "p1", "Paris", "75001");

        final CopyingPerson person = CassandraExecutor.toEntity(row, CopyingPerson.class);

        // HEAD: Address(null, 75001) - the path setter wrote "Paris" into the Address it created, not into the copy the setter stored.
        assertEquals("p1", person.getId());
        assertEquals("Paris", person.getAddress().getCity());
        assertEquals("75001", person.getAddress().getZip());
        assertEquals("Paris", CassandraExecutor.toList(result(row), CopyingPerson.class).get(0).getAddress().getCity());
    }

    @Test
    void dottedColumn_copyingSetter_singleColumn() {
        final CopyingPerson person = CassandraExecutor.toEntity(row(new String[] { "address.city" }, "Paris"), CopyingPerson.class);

        assertNotNull(person.getAddress());
        assertEquals("Paris", person.getAddress().getCity());
    }

    @Test
    void dottedColumns_copyingSetters_twoLevels() {
        final Row row = row(new String[] { "owner.id", "owner.address.city", "owner.address.zip" }, "p1", "Paris", "75001");

        final CopyingCompany company = CassandraExecutor.toEntity(row, CopyingCompany.class);

        assertEquals("p1", company.getOwner().getId());
        assertEquals("Paris", company.getOwner().getAddress().getCity());
        assertEquals("75001", company.getOwner().getAddress().getZip());
    }

    @Test
    void dottedColumns_plainSetter_unchanged() {
        final PlainPerson person = CassandraExecutor.toEntity(row(new String[] { "address.city", "address.zip" }, "Paris", "75001"), PlainPerson.class);

        assertEquals("Paris", person.getAddress().getCity());
        assertEquals("75001", person.getAddress().getZip());
    }

    @Test
    void dottedColumn_unresolvedPath_ignoredWithoutCreatingIntermediate() {
        final PlainPerson person = CassandraExecutor.toEntity(row(new String[] { "address.unknown" }, "x"), PlainPerson.class);

        assertNull(person.getAddress());
    }

    @Test
    void arrayBlockingQueueRows_areCreatedWithTheRowWidth() {
        // HEAD: IllegalArgumentException "Not able to create instance for collection: java.util.concurrent.ArrayBlockingQueue".
        final List<ArrayBlockingQueue> rows = CassandraExecutor.toList(result(row(new String[] { "a", "b" }, "x", 7L)), ArrayBlockingQueue.class);

        assertEquals(1, rows.size());
        assertEquals(ArrayBlockingQueue.class, rows.get(0).getClass());
        assertEquals(List.of("x", 7L), new ArrayList<>(rows.get(0)));
    }

    @Test
    void arrayBlockingQueueRow_emptyRow_hasCapacityOne() {
        final ArrayBlockingQueue<?> queue = CassandraExecutor.toList(result(row(new String[0])), ArrayBlockingQueue.class).get(0);

        assertTrue(queue.isEmpty());
        assertEquals(1, queue.remainingCapacity());
    }

    @Test
    void arrayBlockingQueue_nestedRowInTypedArray() {
        // A nested row read as an array component goes through readRow, the other row-collection site.
        final Row outer = row(new String[] { "nested" }, row(new String[] { "v", "w" }, "Ada", "Grace"));

        final ArrayBlockingQueue[] array = CassandraExecutor.toList(result(outer), ArrayBlockingQueue[].class).get(0);

        assertEquals(List.of("Ada", "Grace"), new ArrayList<>(array[0]));
    }

    @Test
    void otherCollectionRows_unchanged() {
        final Row row = row(new String[] { "a", "b" }, "x", "y");

        assertEquals(ArrayList.class, CassandraExecutor.toList(result(row), List.class).get(0).getClass());
        assertEquals(LinkedBlockingQueue.class, CassandraExecutor.toList(result(row), LinkedBlockingQueue.class).get(0).getClass());
        // An unbounded queue stays unbounded (not sized to the row width).
        assertEquals(Integer.MAX_VALUE - 2, CassandraExecutor.toList(result(row), LinkedBlockingQueue.class).get(0).remainingCapacity());
        assertTrue(CassandraExecutor.toList(result(row), ImmutableList.class).get(0) instanceof ImmutableList);
    }

    static Row row(final String[] names, final Object... values) {
        final Row row = mock(Row.class);
        final ColumnDefinitions columns = mock(ColumnDefinitions.class);
        when(row.getColumnDefinitions()).thenReturn(columns);
        when(columns.size()).thenReturn(names.length);

        for (int i = 0; i < names.length; i++) {
            final ColumnDefinition column = mock(ColumnDefinition.class);
            when(columns.get(i)).thenReturn(column);
            when(column.getName()).thenReturn(CqlIdentifier.fromInternal(names[i]));
            when(column.getType()).thenReturn(values[i] instanceof Number ? DataTypes.BIGINT : DataTypes.TEXT);
            when(row.getObject(i)).thenReturn(values[i]);
        }

        return row;
    }

    static ResultSet result(final Row... rows) {
        final ResultSet result = mock(ResultSet.class);
        final ColumnDefinitions metadata = rows[0].getColumnDefinitions();
        when(result.getColumnDefinitions()).thenReturn(metadata);
        when(result.getAvailableWithoutFetching()).thenReturn(rows.length);
        when(result.iterator()).thenAnswer(invocation -> Arrays.asList(rows).iterator());
        when(result.all()).thenReturn(Arrays.asList(rows));
        return result;
    }
}
