package example.cassandra;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.Function;

import com.landawn.abacus.type.AbstractType;
import com.landawn.abacus.type.TypeFactory;
import com.landawn.abacus.util.N;

// abacus refuses converters for its own (com.landawn.abacus.*) and JDK classes, so these application-class fixtures for the
// Cassandra blob conversion tests live outside com.landawn.abacus. The registrations are global to the JVM and cannot be undone;
// they only concern these fixture classes, and register() is idempotent.
public final class BlobFixtures {
    private BlobFixtures() {
    }

    /** Registers the BlobBox and EmptyBox -> ByteBuffer converters and the TypedBox and TextBox abacus Types (once per JVM). */
    public static void register() {
        Registration.DONE.hashCode();
    }

    private static final class Registration {
        static final Object DONE = new Object();

        static {
            N.registerConverter(BlobBox.class, (value, targetClass) -> targetClass == ByteBuffer.class ? ByteBuffer.wrap(value.bytes()) : null);
            N.registerConverter(EmptyBox.class, (value, targetClass) -> targetClass == ByteBuffer.class ? ByteBuffer.allocate(0) : null);

            // The from-string overload: abacus builds the Type itself (an anonymous class in com.landawn.abacus.type).
            TypeFactory.registerType(TextBox.class, (Function<TextBox, String>) TextBox::text, (Function<String, TextBox>) TextBox::new);

            TypeFactory.registerType(TypedBox.class, new AbstractType<TypedBox>("CassandraBlobFixturesTypedBox") {
                @Override
                public Class<TypedBox> javaType() {
                    return TypedBox.class;
                }

                @Override
                public String stringOf(final TypedBox x) {
                    return x == null ? null : Arrays.toString(x.bytes());
                }

                @Override
                public TypedBox valueOf(final String str) {
                    throw new UnsupportedOperationException("TypedBox is built from a ByteBuffer only");
                }

                @Override
                public TypedBox valueOf(final Object obj) {
                    if (obj instanceof final ByteBuffer buffer) {
                        final byte[] bytes = new byte[buffer.remaining()];
                        buffer.duplicate().get(bytes);
                        return new TypedBox(bytes);
                    }

                    return obj == null ? null : valueOf(N.stringOf(obj));
                }
            });
        }
    }

    /** An application value with a converter to ByteBuffer (as a protobuf ByteString wrapper would have). */
    public record BlobBox(byte[] bytes) {
    }

    /** An application value whose own abacus Type builds it from a ByteBuffer. */
    public record TypedBox(byte[] bytes) {
    }

    /** An application value whose converter to ByteBuffer yields an empty buffer (an empty blob, position 0). */
    public record EmptyBox() {
    }

    /** An application value whose abacus Type was registered from to/from-string functions (it converts from text only). */
    public record TextBox(String text) {
    }
}
