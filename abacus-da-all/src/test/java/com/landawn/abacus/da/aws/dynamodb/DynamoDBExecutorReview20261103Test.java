package com.landawn.abacus.da.aws.dynamodb;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * Pins the SDK v1 {@link DynamoDBExecutor} Javadoc claims checked by the 2026-11-03 review: the precision of the text written
 * for date types, the JSON text written for an SDK {@code AttributeValue}, and how {@code toEntity} handles a dotted attribute
 * whose path goes through a collection property.
 */
public class DynamoDBExecutorReview20261103Test extends DynamoDBExecutorTestSupport {

    private static final long MILLIS = 1_700_000_000_123L;

    // toAttributeValue writes a java.util.Date through its Type's text, which has whole seconds only; Timestamp, Calendar and
    // Instant keep the milliseconds (same rule as the SDK v2 executor).
    @Test
    public void toAttributeValue_utilDateLosesMillisecondsOtherDateTypesKeepThem() {
        final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calendar.setTimeInMillis(MILLIS);

        assertAll(() -> assertEquals("2023-11-14T22:13:20Z", DynamoDBExecutor.toAttributeValue(new Date(MILLIS)).getS()),
                () -> assertEquals(MILLIS - 123, DynamoDBExecutor.toValue(DynamoDBExecutor.toAttributeValue(new Date(MILLIS)), Date.class).getTime()),
                () -> assertEquals("2023-11-14T22:13:20.123Z", DynamoDBExecutor.toAttributeValue(new java.sql.Timestamp(MILLIS)).getS()),
                () -> assertEquals(MILLIS,
                        DynamoDBExecutor.toValue(DynamoDBExecutor.toAttributeValue(new java.sql.Timestamp(MILLIS)), java.sql.Timestamp.class).getTime()),
                () -> assertEquals(MILLIS, DynamoDBExecutor.toValue(DynamoDBExecutor.toAttributeValue(calendar), Calendar.class).getTimeInMillis()),
                () -> assertEquals("2023-11-14T22:13:20.123Z", DynamoDBExecutor.toAttributeValue(Instant.ofEpochMilli(MILLIS)).getS()));
    }

    // The SDK's own AttributeValue is not passed through by toAttributeValue: it is written as its JSON text.
    @Test
    public void toAttributeValue_sdkAttributeValueIsWrittenAsJsonText() {
        final AttributeValue written = DynamoDBExecutor.toAttributeValue(new AttributeValue().withS("x"));

        assertAll(() -> assertEquals("{\"s\": \"x\"}", written.getS()), () -> assertNull(written.getM()), () -> assertNull(written.getN()));
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

    public static final class BuiltBook {
        private final List<Address> addresses;

        private BuiltBook(final List<Address> addresses) {
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

    // A dotted attribute through a collection property is left to the dependency's path setter: in a mutable target it adds the
    // bean it creates to the collection; in a builder-built target it reads
    // the property from the builder and fails the item with ClassCastException.
    @Test
    public void toEntity_dottedPathThroughCollection_mutableTargetAddsBeanBuilderTargetFails() {
        final Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("addresses.city", new AttributeValue("Paris"));

        assertAll(() -> {
            final List<Address> addresses = DynamoDBExecutor.toEntity(item, MutableBook.class).getAddresses();
            assertEquals(1, addresses.size());
            assertEquals("Paris", addresses.get(0).getCity());
        }, () -> {
            final List<Address> addresses = new ArrayList<>(executorRows(item));
            assertEquals(1, addresses.size());
            assertEquals("Paris", addresses.get(0).getCity());
        }, () -> assertThrows(ClassCastException.class, () -> DynamoDBExecutor.toEntity(item, BuiltBook.class)));
    }

    // The same rule through the row mapper of the list/stream/getItem paths.
    private static List<Address> executorRows(final Map<String, AttributeValue> item) {
        return DynamoDBExecutor.toList(List.of(item), MutableBook.class).get(0).getAddresses();
    }
}
