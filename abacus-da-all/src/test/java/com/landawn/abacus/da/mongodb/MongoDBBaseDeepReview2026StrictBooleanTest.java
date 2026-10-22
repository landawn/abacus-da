package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.bson.Document;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.Test;

/**
 * Pins the "Boolean Conversion" section of the {@link MongoDBBase} class Javadoc for {@code toEntity} with a {@code Boolean}
 * and a {@code boolean} property: only a BSON boolean, an int32 1/0/-1 and the texts 1/0/-1/y/n/true/false (any case,
 * surrounding whitespace ignored) convert; blank text is {@code false} for both targets; any other value fails the whole
 * document with {@link IllegalArgumentException}. A Short or Byte (which BSON cannot store, so it only occurs in an in-memory
 * row) behaves like the int32.
 */
public class MongoDBBaseDeepReview2026StrictBooleanTest {

    public static class Flags {
        private Boolean wrapped;
        private boolean primitive;

        public Boolean getWrapped() {
            return wrapped;
        }

        public void setWrapped(final Boolean wrapped) {
            this.wrapped = wrapped;
        }

        public boolean isPrimitive() {
            return primitive;
        }

        public void setPrimitive(final boolean primitive) {
            this.primitive = primitive;
        }
    }

    /** {stored value, expected Boolean}. */
    static List<Object[]> accepted() {
        return List.of(new Object[] { "", false }, new Object[] { "  ", false }, new Object[] { " Y ", true }, new Object[] { "TRUE", true },
                new Object[] { "n", false }, new Object[] { -1, false }, new Object[] { 1, true }, new Object[] { 0, false }, new Object[] { true, true },
                new Object[] { false, false }, new Object[] { "1", true }, new Object[] { "-1", false }, new Object[] { "FaLsE", false },
                // In-memory rows only: BSON has no int16/int8, but a Short or Byte follows the int32 rule.
                new Object[] { (short) 1, true }, new Object[] { (byte) -1, false }, new Object[] { (short) 0, false });
    }

    static List<Object> rejected() {
        return List.of("yes", 2, 1L, 1.0d, Decimal128.parse("1"), "1.0", "2", (short) 2, (byte) 2);
    }

    @Test
    public void toEntityConvertsBooleanPropertiesOnlyFromTheDocumentedValues() {
        for (final Object[] c : accepted()) {
            final Flags flags = MongoDBBase.toEntity(new Document("wrapped", c[0]).append("primitive", c[0]), Flags.class);

            assertEquals(c[1], flags.getWrapped(), "Boolean property from " + describe(c[0]));
            assertEquals(c[1], flags.isPrimitive(), "boolean property from " + describe(c[0]));
        }

        for (final Object value : rejected()) {
            // Either property fails the whole document.
            assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toEntity(new Document("wrapped", value), Flags.class),
                    "Boolean property from " + describe(value));
            assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toEntity(new Document("primitive", value), Flags.class),
                    "boolean property from " + describe(value));
        }
    }

    static String describe(final Object value) {
        return value == null ? "null" : value.getClass().getSimpleName() + " '" + value + "'";
    }
}
