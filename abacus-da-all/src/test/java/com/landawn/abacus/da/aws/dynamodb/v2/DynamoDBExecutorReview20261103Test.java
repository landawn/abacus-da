package com.landawn.abacus.da.aws.dynamodb.v2;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Pins the SDK v2 {@link DynamoDBExecutor} behaviors whose Javadoc was corrected or completed in the 2026-11-03 review.
 */
public class DynamoDBExecutorReview20261103Test {

    private static final long MILLIS = 1700000000123L; // 2023-11-14T22:13:20.123Z

    /**
     * The toAttributeValue Javadoc now states that a java.util.Date is written in whole seconds (abacus-common's default
     * java.util.Date text), so a round trip loses its milliseconds, while Timestamp, Calendar and Instant keep them. The text is
     * UTC, so the expected strings do not depend on the JVM time zone.
     */
    @Test
    public void toAttributeValue_utilDateLosesMillisecondsOtherDateTypesKeepThem() {
        final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calendar.setTimeInMillis(MILLIS);

        assertAll(() -> assertEquals(AttributeValue.fromS("2023-11-14T22:13:20Z"), DynamoDBExecutor.toAttributeValue(new Date(MILLIS))),
                () -> assertEquals(MILLIS - 123, DynamoDBExecutor.toValue(DynamoDBExecutor.toAttributeValue(new Date(MILLIS)), Date.class).getTime()),
                () -> assertEquals(AttributeValue.fromS("2023-11-14T22:13:20.123Z"), DynamoDBExecutor.toAttributeValue(new java.sql.Timestamp(MILLIS))),
                () -> assertEquals(MILLIS,
                        DynamoDBExecutor.toValue(DynamoDBExecutor.toAttributeValue(new java.sql.Timestamp(MILLIS)), java.sql.Timestamp.class).getTime()),
                () -> assertEquals(MILLIS, DynamoDBExecutor.toValue(DynamoDBExecutor.toAttributeValue(calendar), Calendar.class).getTimeInMillis()),
                () -> assertEquals(AttributeValue.fromS("2023-11-14T22:13:20.123Z"), DynamoDBExecutor.toAttributeValue(Instant.ofEpochMilli(MILLIS))));
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

    public static class MutableBook {
        private List<Address> addresses;

        public List<Address> getAddresses() {
            return addresses;
        }

        public void setAddresses(final List<Address> addresses) {
            this.addresses = addresses;
        }
    }

    /** An immutable bean created through its builder, with a nested bean and a collection of nested beans. */
    public static final class BuiltBook {
        private final Address main;
        private final List<Address> addresses;

        BuiltBook(final Address main, final List<Address> addresses) {
            this.main = main;
            this.addresses = addresses;
        }

        public Address getMain() {
            return main;
        }

        public List<Address> getAddresses() {
            return addresses;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private Address main;
            private List<Address> addresses;

            public Builder main(final Address main) {
                this.main = main;
                return this;
            }

            public Builder addresses(final List<Address> addresses) {
                this.addresses = addresses;
                return this;
            }

            public BuiltBook build() {
                return new BuiltBook(main, addresses);
            }
        }
    }

    /**
     * The toEntity(Map, Class) Javadoc now also says that a dotted path through a collection property is left to the
     * dependency's path setter, which adds the bean it creates to the collection, and that in a builder-based target it reads
     * that property from the builder and fails the item with ClassCastException. A path through a plain nested bean of a
     * builder-based target still works.
     */
    @Test
    public void toEntity_dottedPathThroughCollectionProperty() {
        final Map<String, AttributeValue> collectionPath = Map.of("addresses.city", AttributeValue.fromS("Paris"));

        assertAll(() -> {
            final List<Address> addresses = DynamoDBExecutor.toEntity(collectionPath, MutableBook.class).getAddresses();
            assertEquals(1, addresses.size());
            assertEquals("Paris", addresses.get(0).getCity());
        }, () -> {
            final ClassCastException e = assertThrows(ClassCastException.class, () -> DynamoDBExecutor.toEntity(collectionPath, BuiltBook.class));
            assertTrue(e.getMessage().contains("Builder"), e.getMessage());
        }, () -> {
            final Map<String, AttributeValue> beanPath = new LinkedHashMap<>();
            beanPath.put("main.city", AttributeValue.fromS("Lyon"));
            assertEquals("Lyon", DynamoDBExecutor.toEntity(beanPath, BuiltBook.class).getMain().getCity());
        }, () -> {
            // The row readers apply the same rule.
            final List<Map<String, AttributeValue>> items = new ArrayList<>();
            items.add(collectionPath);
            assertThrows(ClassCastException.class, () -> DynamoDBExecutor.toList(items, BuiltBook.class));
        });
    }
}
