package com.landawn.abacus.da.aws.dynamodb;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.landawn.abacus.annotation.JsonXmlField;
import com.landawn.abacus.exception.ParsingException;
import com.landawn.abacus.util.EnumType;
import com.landawn.abacus.util.Holder;
import com.landawn.abacus.util.ImmutableBiMap;
import com.landawn.abacus.util.ImmutableCollection;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Pair;
import com.landawn.abacus.util.u.Nullable;

import software.amazon.awssdk.core.SdkBytes;

/**
 * Native attribute shapes that do not match the declared property type, and values whose text form cannot round-trip.
 */
public class DynamoDBShapeAndRoundTripRegressionTest {

    // Construction and configuration

    @Test
    public void emptyQueuesUseTheirNoArgumentConstructors() {
        forEachSdk(sdk -> {
            for (final String context : List.of("top", "direct", "list")) {
                final TypedLeaf leaf = readTyped(sdk, context, Map.of("priorityQueue", List.of(), "untypedQueue", List.of(), "longQueue", List.of()));
                assertAll(context, () -> assertEquals(0, leaf.getPriorityQueue().size()), () -> assertEquals(0, leaf.getUntypedQueue().size()),
                        () -> assertEquals(0, leaf.getLongQueue().size()));
            }

            assertEquals(List.of("x"), List.copyOf(sdk.read(Map.of("untypedQueue", List.of("x")), TypedRoot.class).getUntypedQueue()));
        });
    }

    // Row and entity mapping

    @Test
    public void boolValuesInJsonContextApplyPropertyFormatsAndTypes() {
        forEachSdk(sdk -> assertAll(
                // A format that cannot parse a boolean rejects BOOL as it rejects the same text, instead of storing the Boolean.
                () -> assertThrows(ParsingException.class, () -> readFlags(sdk, Map.of("formattedFlag", true))),
                () -> assertThrows(ParsingException.class, () -> readFlags(sdk, Map.of("formattedFlag", "true"))),
                () -> assertThrows(UnsupportedOperationException.class, () -> readFlags(sdk, Map.of("datedFlag", false))),
                () -> assertThrows(ParsingException.class, () -> readFlags(sdk, Map.of("formattedLong", true))),
                () -> assertThrows(ParsingException.class, () -> readFlags(sdk, Map.of("formattedObject", true))),
                // Unformatted properties read the boolean token by type; supertype slots keep the Boolean.
                () -> assertSame(Boolean.TRUE, readFlags(sdk, Map.of("flag", true)).getFlag()),
                () -> assertEquals(true, readFlags(sdk, Map.of("primitiveFlag", true)).isPrimitiveFlag()),
                () -> assertEquals("true", readFlags(sdk, Map.of("text", true)).getText()),
                () -> assertSame(Boolean.TRUE, readFlags(sdk, Map.of("untyped", true)).getUntyped()),
                () -> assertSame(Boolean.FALSE, readFlags(sdk, Map.of("comparable", false)).getComparable()),
                () -> assertThrows(NumberFormatException.class, () -> readFlags(sdk, Map.of("count", true))),
                // A directly mapped bean keeps map-to-bean rules, which do not apply JSON formats.
                () -> assertSame(Boolean.TRUE, sdk.read(Map.of("direct", Map.of("formattedFlag", true)), FlagRoot.class).getDirect().getFormattedFlag())));
    }

    @Test
    public void jsonContextValuesOfAnotherShapeAreConvertedOrRejected() {
        forEachSdk(sdk -> assertAll(
                () -> assertThrows(IllegalArgumentException.class,
                        () -> sdk.read(Map.of("leaves", List.of(Map.of("nestedLongs", List.of(true)))), ShapeRoot.class)),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> sdk.read(Map.of("leaves", List.of(Map.of("longLists", Map.of("a", Map.of("b", 1))))), ShapeRoot.class)),
                () -> assertEquals(Optional.of(List.of("true")), sdk.read(Map.of("optionalStrings", true), ShapeRoot.class).getOptionalStrings()), () -> {
                    // A binary set read as typed number lists supplies each value's bytes, for both SDKs' binary forms.
                    final ShapeLeaf leaf = sdk
                            .read(Map.of("leaves", List.of(Map.of("nestedLongs", new BinarySet(List.of(buffer(1, 2), buffer(3)))))), ShapeRoot.class)
                            .getLeaves()
                            .get(0);
                    assertEquals(List.of(List.of(1L, 2L), List.of(3L)), leaf.getNestedLongs());
                }, () -> {
                    // Untyped elements receive the whole binary value, in each SDK's representation: v1's ByteBuffer and
                    // v2's byte array (which previously supplied its byte values instead).
                    final List<?> wrapped = sdk.read(Map.of("leaves", List.of(Map.of("nestedObjects", List.of(buffer(4, 5))))), ShapeRoot.class)
                            .getLeaves()
                            .get(0)
                            .getNestedObjects()
                            .get(0);
                    assertEquals(1, wrapped.size());
                    assertArrayEquals(new byte[] { 4, 5 }, bytes(wrapped.get(0)));
                }));
    }

    @Test
    public void mismatchedTopLevelContainersAreConvertedOrRejectedInsteadOfWrapped() {
        forEachSdk(sdk -> assertAll(
                // An M attribute is not a list of numbers: the declared type's codec rejects it instead of storing the map.
                () -> assertThrows(IllegalArgumentException.class, () -> sdk.read(Map.of("longs", Map.of("a", 1)), ShapeRoot.class)),
                () -> assertThrows(IllegalArgumentException.class, () -> sdk.read(Map.of("longSet", Map.of("a", 1)), ShapeRoot.class)),
                () -> assertThrows(IllegalArgumentException.class, () -> sdk.read(Map.of("longs", true), ShapeRoot.class)), () -> {
                    // As before, a String list reads the map through the JSON codec as one element holding its JSON text.
                    final List<?> strings = sdk.read(Map.of("strings", Map.of("a", "1")), ShapeRoot.class).getStrings();
                    assertEquals(1, strings.size());
                    assertEquals(Map.of("a", "1"), N.fromJson(assertInstanceOf(String.class, strings.get(0)), Map.class));
                }));
    }

    @Test
    public void nestedImmutableCollectionAndBiMapPropertiesAreConstructed() {
        forEachSdk(sdk -> {
            for (final String context : List.of("direct", "array", "list")) {
                final ShapeLeaf leaf = readLeaf(sdk, context,
                        Map.of("namedValues", List.of(Map.of("name", "a")), "namedIndex", Map.of("k", Map.of("name", "b"))));
                assertInstanceOf(ImmutableCollection.class, leaf.getNamedValues(), context);
                assertInstanceOf(ImmutableBiMap.class, leaf.getNamedIndex(), context);
                assertEquals("a", leaf.getNamedValues().iterator().next().getName(), context);
                assertEquals("b", leaf.getNamedIndex().get("k").getName(), context);
            }
        });
    }

    // Updates and deletes

    @Test
    public void deleteUpdatesOfEmptyWrappersRemoveTheAttribute() {
        for (final Object empty : java.util.Arrays.asList(null, Optional.empty(), Nullable.empty(), Holder.of(null))) {
            assertNull(DynamoDBExecutor.toAttributeValueUpdate(empty, com.amazonaws.services.dynamodbv2.model.AttributeAction.DELETE).getValue(),
                    String.valueOf(empty));
            assertNull(com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor
                    .toAttributeValueUpdate(empty, software.amazon.awssdk.services.dynamodb.model.AttributeAction.DELETE)
                    .value(), String.valueOf(empty));
            // Other actions still send the NULL value.
            assertSame(Boolean.TRUE,
                    DynamoDBExecutor.toAttributeValueUpdate(empty, com.amazonaws.services.dynamodbv2.model.AttributeAction.PUT).getValue().getNULL());
        }

        assertEquals("x",
                DynamoDBExecutor.toAttributeValueUpdate(Optional.of("x"), com.amazonaws.services.dynamodbv2.model.AttributeAction.DELETE).getValue().getS());
    }

    // Additional contracts

    @Test
    public void arrayBlockingQueuesAreSizedToTheirElementsInEveryContext() {
        forEachSdk(sdk -> {
            final TypedRoot root = sdk.read(Map.of("bufferQueue", buffer(1, 2), "longQueue", List.of(1, 2)), TypedRoot.class);
            assertEquals(1, root.getBufferQueue().size());
            assertArrayEquals(new byte[] { 1, 2 }, bytes(root.getBufferQueue().peek()));
            assertEquals(List.of(1L, 2L), List.copyOf(root.getLongQueue()));
            assertEquals(List.of(3L), List.copyOf(readTyped(sdk, "list", Map.of("longQueue", List.of(3))).getLongQueue()));
            assertEquals(0, readTyped(sdk, "list", Map.of("longQueue", List.of())).getLongQueue().size());
        });
    }

    @Test
    public void base64TextInWrappedBufferPropertiesIsRewrittenWithItsBytes() {
        final String text = Base64.getEncoder().encodeToString(new byte[] { 7, 8, 9 });

        forEachSdk(sdk -> {
            // Items written with the wrapper's text form: each wrapped buffer is readable, so a read-modify-write keeps its
            // bytes instead of storing an empty binary value.
            final WrapperRoot read = sdk.read(Map.of("buffer", text, "abacusBuffer", text, "nullableBuffer", text, "heldBuffer", text), WrapperRoot.class);
            assertAll(() -> assertEquals(0, read.getBuffer().orElseThrow().position()),
                    () -> assertArrayEquals(new byte[] { 7, 8, 9 }, bytes(read.getBuffer().orElseThrow())),
                    () -> assertArrayEquals(new byte[] { 7, 8, 9 }, bytes(read.getAbacusBuffer().get())),
                    () -> assertArrayEquals(new byte[] { 7, 8, 9 }, bytes(read.getNullableBuffer().get())),
                    () -> assertArrayEquals(new byte[] { 7, 8, 9 }, bytes(read.getHeldBuffer().value())));

            for (final String property : List.of("buffer", "abacusBuffer", "nullableBuffer", "heldBuffer")) {
                assertArrayEquals(new byte[] { 7, 8, 9 }, sdk.writtenBinary(read, property), property);
            }
        });
    }

    @Test
    public void base64TextReadIntoByteBufferIsReadableFromPositionZero() {
        final String text = Base64.getEncoder().encodeToString(new byte[] { 7, 8, 9 });

        forEachSdk(sdk -> {
            for (final String context : List.of("top", "direct", "array")) {
                final ByteBuffer buffer = "top".equals(context) ? sdk.read(Map.of("buffer", text), ShapeRoot.class).getBuffer()
                        : readLeaf(sdk, context, Map.of("buffer", text)).getBuffer();
                assertEquals(0, buffer.position(), context);
                assertArrayEquals(new byte[] { 7, 8, 9 }, bytes(buffer), context);
            }
        });
    }

    @Test
    public void base64TextReadWithJsonSemanticsKeepsTheJsonBufferConvention() {
        final String text = Base64.getEncoder().encodeToString(new byte[] { 7, 8, 9 });

        forEachSdk(sdk -> {
            // A bean read through a collection, and the same bean list stored as JSON text, both use the JSON codec,
            // which leaves the decoded bytes before the buffer's position.
            final ByteBuffer nested = readLeaf(sdk, "list", Map.of("buffer", text)).getBuffer();
            final ByteBuffer jsonText = sdk.read(Map.of("leaves", "[{\"buffer\":\"" + text + "\"}]"), ShapeRoot.class).getLeaves().get(0).getBuffer();

            for (final ByteBuffer buffer : List.of(nested, jsonText)) {
                assertEquals(3, buffer.position());
                assertEquals(3, buffer.limit());
                assertArrayEquals(new byte[] { 7, 8, 9 }, bytes(buffer.duplicate().flip()));
            }
        });
    }

    @Test
    public void binaryValuesForTypedCollectionsKeepTheirBytes() {
        forEachSdk(sdk -> assertAll(
                // Number elements receive the byte values, at the top level and in a bean read through JSON context.
                () -> assertEquals(List.of(1L, 2L), sdk.read(Map.of("longs", buffer(1, 2)), ShapeRoot.class).getLongs()),
                () -> assertEquals(List.of(3L, 4L),
                        sdk.read(Map.of("leaves", List.of(Map.of("longValues", buffer(3, 4)))), ShapeRoot.class).getLeaves().get(0).getLongValues()),
                // Binary elements receive the whole value as their single element.
                () -> {
                    final List<ByteBuffer> buffers = sdk.read(Map.of("optionalBuffers", buffer(1, 2)), ShapeRoot.class).getOptionalBuffers().orElseThrow();
                    assertEquals(1, buffers.size());
                    assertArrayEquals(new byte[] { 1, 2 }, bytes(buffers.get(0)));
                }, () -> {
                    final List<byte[]> arrays = sdk.read(Map.of("byteArrayLists", Map.of("k", buffer(5, 6))), ShapeRoot.class).getByteArrayLists().get("k");
                    assertEquals(1, arrays.size());
                    assertArrayEquals(new byte[] { 5, 6 }, arrays.get(0));
                }, () -> {
                    final List<List<ByteBuffer>> nested = sdk.read(Map.of("leaves", List.of(Map.of("bufferLists", List.of(buffer(7, 8))))), ShapeRoot.class)
                            .getLeaves()
                            .get(0)
                            .getBufferLists();
                    assertEquals(1, nested.get(0).size());
                    assertArrayEquals(new byte[] { 7, 8 }, bytes(nested.get(0).get(0)));
                },
                // A binary value has no map form; v1 previously stored null and v2 failed while parsing.
                () -> assertThrows(IllegalArgumentException.class, () -> sdk.read(Map.of("longMap", buffer(1, 2)), ShapeRoot.class))));
    }

    @Test
    public void binaryValuesForTypedContainersInDirectBeansAndArrays() {
        forEachSdk(sdk -> {
            for (final String context : List.of("direct", "list", "top")) {
                final TypedLeaf leaf = readTyped(sdk, context, Map.of("longValues", buffer(1, 2), "longArray", buffer(3, 4), "byteArrays", buffer(5, 6)));
                assertAll(context, () -> assertEquals(List.of(1L, 2L), leaf.getLongValues()),
                        () -> assertArrayEquals(new Long[] { 3L, 4L }, leaf.getLongArray()), () -> {
                            // Binary elements receive the whole value: a direct bean split v2's array into one-byte arrays.
                            assertEquals(1, leaf.getByteArrays().size());
                            assertArrayEquals(new byte[] { 5, 6 }, leaf.getByteArrays().get(0));
                        }, () -> assertThrows(IllegalArgumentException.class, () -> readTyped(sdk, context, Map.of("longMap", buffer(1, 2)))));
            }

            final ByteBuffer[] buffers = sdk.read(Map.of("bufferArray", buffer(7, 8)), TypedRoot.class).getBufferArray();
            assertEquals(1, buffers.length);
            assertArrayEquals(new byte[] { 7, 8 }, bytes(buffers[0]));
        });
    }

    @Test
    public void binaryValuesKeepTheirSlotRulesForTypesABufferCanBe() {
        forEachSdk(sdk -> {
            for (final String context : List.of("top", "direct", "list")) {
                // Comparable<?> can hold the buffer itself: it is not a type without a binary form.
                assertArrayEquals(new byte[] { 1, 2 }, bytes(readTyped(sdk, context, Map.of("comparable", buffer(1, 2))).getComparable()), context);

                // Byte values are read as the JSON reader reads numbers: epoch milliseconds and code points, not their text.
                final TypedLeaf leaf = readTyped(sdk, context, Map.of("dates", buffer(1, 2), "chars", buffer(1, 65)));
                assertEquals(List.of(new Date(1), new Date(2)), leaf.getDates(), context);
                assertEquals(List.of((char) 1, 'A'), leaf.getChars(), context);
            }
        });
    }

    @Test
    public void directBeanNumberMapKeysUseNumericInferenceAndKeepCollisionChecks() {
        forEachSdk(sdk -> {
            for (final String context : List.of("direct", "array")) {
                final Map<String, Object> keys = new LinkedHashMap<>();
                keys.put("1", "one");
                keys.put("2.5", "two and a half");
                final ShapeLeaf leaf = readLeaf(sdk, context, Map.of("numberKeys", keys));
                assertEquals(Set.of(1, 2.5d), leaf.getNumberKeys().keySet(), context);
                assertEquals("one", leaf.getNumberKeys().get(1), context);

                final Map<String, Object> colliding = new LinkedHashMap<>();
                colliding.put("1", "one");
                colliding.put("01", "zero-one");
                final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                        () -> readLeaf(sdk, context, Map.of("numberKeys", colliding)), context);
                assertEquals(IllegalArgumentException.class, failure.getClass(), failure.getMessage());
            }
        });
    }

    @Test
    public void emptyWrappersAreWrittenAsNullAndReadBackEmpty() {
        assertSame(Boolean.TRUE, DynamoDBExecutor.toAttributeValue(Optional.empty()).getNULL());
        assertSame(Boolean.TRUE, DynamoDBExecutor.toAttributeValue(Holder.of(null)).getNULL());
        assertSame(Boolean.TRUE, com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toAttributeValue(Optional.empty()).nul());
        assertSame(Boolean.TRUE, com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toAttributeValue(Nullable.empty()).nul());

        forEachSdk(sdk -> {
            final WrapperRoot original = new WrapperRoot();
            original.setText(Optional.empty());
            original.setHeld(Holder.of(null));
            original.setNullable(Nullable.empty());

            final WrapperRoot result = sdk.roundTrip(original, WrapperRoot.class);
            assertAll(() -> assertEquals(Optional.empty(), result.getText()), () -> assertNull(result.getHeld().value()),
                    () -> assertEquals(Nullable.empty(), result.getNullable()));
        });
    }

    @Test
    public void nullAttributesForHoldersOfWrappersKeepTheInnerWrapper() {
        forEachSdk(sdk -> {
            final Map<String, Object> nullHeld = new LinkedHashMap<>();
            nullHeld.put("heldOptional", null);
            assertEquals(Optional.empty(), sdk.read(nullHeld, WrapperRoot.class).getHeldOptional().value());

            final WrapperRoot original = new WrapperRoot();
            original.setHeldOptional(Holder.of(Optional.empty()));
            assertEquals(Optional.empty(), sdk.roundTrip(original, WrapperRoot.class).getHeldOptional().value());
        });
    }

    @Test
    public void nullValuesRejectedByConcurrentMapsAreReportedInEveryContext() {
        final Map<String, Object> nullValue = new LinkedHashMap<>();
        nullValue.put("k", null);

        forEachSdk(sdk -> assertAll(() -> assertRejectedNull(() -> sdk.read(Map.of("concurrent", nullValue), ShapeRoot.class)),
                () -> assertRejectedNull(() -> sdk.read(Map.of("leaves", List.of(Map.of("concurrent", nullValue))), ShapeRoot.class)),
                () -> assertRejectedNull(() -> sdk.read(Map.of("direct", Map.of("concurrent", nullValue)), ShapeRoot.class))));
    }

    @Test
    public void numberPropertiesUseNumericInferenceInEveryContext() {
        forEachSdk(sdk -> {
            for (final String context : List.of("top", "direct", "list")) {
                assertAll(context, () -> assertEquals(5, readTyped(sdk, context, Map.of("number", 5)).getNumber()),
                        () -> assertEquals(1.5d, readTyped(sdk, context, Map.of("number", 1.5)).getNumber()),
                        () -> assertEquals(new java.math.BigInteger("123456789012345678901234567890"),
                                readTyped(sdk, context, Map.of("number", new java.math.BigInteger("123456789012345678901234567890"))).getNumber()));
            }

            final TypedRoot original = new TypedRoot();
            original.setNumber(42L);
            assertEquals(42, sdk.roundTrip(original, TypedRoot.class).getNumber());

            // A decimal that a Double cannot represent keeps every digit, so a read-modify-write stores the same number.
            final java.math.BigDecimal precise = new java.math.BigDecimal("12345678901234567890.123");
            original.setNumber(precise);
            assertEquals(0, precise.compareTo((java.math.BigDecimal) sdk.roundTrip(original, TypedRoot.class).getNumber()));

            // Values that are not numbers fail as the JSON reader rejects them, not with the abstract type's
            // UnsupportedOperationException; a binary value has no number form.
            assertAll(() -> assertThrows(NumberFormatException.class, () -> sdk.read(Map.of("number", true), TypedRoot.class)),
                    () -> assertThrows(NumberFormatException.class, () -> readTyped(sdk, "list", Map.of("number", List.of(1)))),
                    () -> assertThrows(NumberFormatException.class, () -> sdk.read(Map.of("numbers", List.of(true)), TypedRoot.class)),
                    () -> assertThrows(IllegalArgumentException.class, () -> sdk.read(Map.of("number", buffer(1)), TypedRoot.class)));
        });
    }

    @Test
    public void topLevelCodeEnumsAcceptConstantNamesAndCodesButNotOrdinals() {
        forEachSdk(sdk -> {
            final ShapeRoot original = new ShapeRoot();
            original.setCoded(Status.FIRST);

            assertAll(() -> assertSame(Status.SECOND, sdk.read(Map.of("coded", 20), ShapeRoot.class).getCoded()),
                    () -> assertSame(Status.SECOND, sdk.read(Map.of("coded", "SECOND"), ShapeRoot.class).getCoded()),
                    // 1 is SECOND's ordinal but no constant's code.
                    () -> assertThrows(IllegalArgumentException.class, () -> sdk.read(Map.of("coded", 1), ShapeRoot.class)),
                    () -> assertSame(Status.FIRST, sdk.roundTrip(original, ShapeRoot.class).getCoded()));
        });
    }

    @Test
    public void valuesOfAnotherShapeKeepTheirDeclaredElementTypes() {
        forEachSdk(sdk -> {
            for (final String context : List.of("list", "top")) {
                // A map for a nested list or an array-element list becomes the single element of each level, as the JSON
                // reader reads it; it was stored unconverted as the outer list's element.
                final TypedLeaf leaf = readTyped(sdk, context,
                        Map.of("nestedObjects", Map.of("a", 1), "namedArrays", Map.of("name", "x"), "longMaps", Map.of("a", 1)));
                assertAll(context, () -> assertEquals(List.of(List.of(Map.of("a", "1"))), leaf.getNestedObjects()), () -> {
                    final Named[] named = assertInstanceOf(Named[].class, leaf.getNamedArrays().get(0));
                    assertEquals("x", named[0].getName());
                }, () -> assertEquals(Map.of("a", 1L), leaf.getLongMaps()[0]),
                        // A boolean has no Long form: rejected, instead of stored as the Optional's content.
                        () -> assertThrows(NumberFormatException.class, () -> readTyped(sdk, context, Map.of("optionalLongs", true))),
                        () -> assertThrows(NumberFormatException.class, () -> readTyped(sdk, context, Map.of("optionalLongs", Map.of("a", 1)))));
            }

            // Text for a parameterized type, or an array of one, is read with its complete type.
            assertAll(() -> assertEquals(Pair.of(1L, 2L), sdk.read(Map.of("pair", "[1, 2]"), TypedRoot.class).getPair()),
                    () -> assertEquals(Pair.of(1L, 2L), sdk.read(Map.of("pair", List.of(1, 2)), TypedRoot.class).getPair()),
                    () -> assertEquals(java.util.Optional.of(123L), sdk.read(Map.of("optionalLongs", "123"), TypedRoot.class).getOptionalLongs()[0]),
                    () -> assertThrows(IllegalArgumentException.class, () -> sdk.read(Map.of("pair", buffer(1, 2)), TypedRoot.class)));
        });
    }

    @Test
    public void wrappedBuffersAreWrittenAsBinaryWithoutConsumingTheSource() {
        final ByteBuffer direct = ByteBuffer.allocateDirect(6);
        direct.put(new byte[] { 9, 1, 2, 3, 8, 8 }).position(1).limit(4);
        final ByteBuffer heap = ByteBuffer.wrap(new byte[] { 9, 1, 2, 3, 8 }, 1, 3);

        for (final ByteBuffer source : List.of(heap, direct, heap.asReadOnlyBuffer())) {
            forEachSdk(sdk -> {
                final WrapperRoot original = new WrapperRoot();
                original.setBuffer(Optional.of(source));
                original.setAbacusBuffer(com.landawn.abacus.util.u.Optional.of(source));
                original.setNullableBuffer(Nullable.of(source));
                original.setHeldBuffer(Holder.of(source));
                original.setNestedBuffer(Optional.of(Optional.of(source)));

                for (final String property : List.of("buffer", "abacusBuffer", "nullableBuffer", "heldBuffer", "nestedBuffer")) {
                    assertArrayEquals(new byte[] { 1, 2, 3 }, sdk.writtenBinary(original, property), property);
                }

                final WrapperRoot result = sdk.roundTrip(original, WrapperRoot.class);
                assertAll(() -> assertArrayEquals(new byte[] { 1, 2, 3 }, bytes(result.getBuffer().orElseThrow())),
                        () -> assertArrayEquals(new byte[] { 1, 2, 3 }, bytes(result.getAbacusBuffer().get())),
                        () -> assertArrayEquals(new byte[] { 1, 2, 3 }, bytes(result.getNullableBuffer().get())),
                        () -> assertArrayEquals(new byte[] { 1, 2, 3 }, bytes(result.getHeldBuffer().value())),
                        () -> assertArrayEquals(new byte[] { 1, 2, 3 }, bytes(result.getNestedBuffer().orElseThrow().orElseThrow())),
                        () -> assertEquals(1, source.position()), () -> assertEquals(4, source.limit()));
            });
        }
    }

    @Test
    public void wrappedByteArraysAreWrittenLikeBareByteArrays() {
        forEachSdk(sdk -> {
            final WrapperRoot original = new WrapperRoot();
            original.setBytes(Optional.of(new byte[] { 1, 2, 3 }));

            // v2 writes a bare byte[] as binary, so a wrapped one is binary too and can match a binary key; v1 writes both as text.
            if (sdk == Sdk.V2) {
                assertArrayEquals(new byte[] { 1, 2, 3 }, sdk.writtenBinary(original, "bytes"));
                assertArrayEquals(new byte[] { 1, 2, 3 },
                        com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toAttributeValue(Optional.of(new byte[] { 1, 2, 3 })).b().asByteArray());
            } else {
                assertNull(sdk.writtenBinary(original, "bytes"));
            }

            assertArrayEquals(new byte[] { 1, 2, 3 }, sdk.roundTrip(original, WrapperRoot.class).getBytes().orElseThrow());
        });
    }

    private static void forEachSdk(final Consumer<Sdk> action) {
        assertAll("both DynamoDB SDKs", Arrays.stream(Sdk.values()).map(sdk -> () -> assertAll(sdk.name(), () -> action.accept(sdk))));
    }

    private static ShapeLeaf readLeaf(final Sdk sdk, final String context, final Map<String, Object> leaf) {
        final ShapeRoot root = sdk.read(Map.of("list".equals(context) ? "leaves" : context, "direct".equals(context) ? leaf : List.of(leaf)), ShapeRoot.class);
        return switch (context) {
            case "direct" -> root.getDirect();
            case "array" -> root.getArray()[0];
            default -> root.getLeaves().get(0);
        };
    }

    private static TypedLeaf readTyped(final Sdk sdk, final String context, final Map<String, Object> leaf) {
        return switch (context) {
            case "top" -> sdk.read(leaf, TypedRoot.class);
            case "direct" -> sdk.read(Map.of("direct", leaf), TypedRoot.class).getDirect();
            default -> sdk.read(Map.of("leaves", List.of(leaf)), TypedRoot.class).getLeaves().get(0);
        };
    }

    private static FlagLeaf readFlags(final Sdk sdk, final Map<String, Object> leaf) {
        return sdk.read(Map.of("flags", List.of(leaf)), FlagRoot.class).getFlags().get(0);
    }

    private static void assertRejectedNull(final Executable action) {
        final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, action);
        assertEquals(IllegalArgumentException.class, failure.getClass(), failure.getMessage());
        assertInstanceOf(NullPointerException.class, failure.getCause());
    }

    private static ByteBuffer buffer(final int... values) {
        final byte[] result = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            result[i] = (byte) values[i];
        }
        return ByteBuffer.wrap(result);
    }

    private static byte[] bytes(final Object binary) {
        if (binary instanceof final byte[] array) {
            return array;
        }
        final ByteBuffer buffer = ((ByteBuffer) binary).duplicate();
        final byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    /** A native BS attribute. */
    private record BinarySet(List<ByteBuffer> values) {
    }

    private enum Sdk {
        V1, V2;

        <T> T read(final Map<String, Object> item, final Class<T> targetClass) {
            if (this == V1) {
                return DynamoDBExecutor.toEntity(legacyItem(item), targetClass);
            }
            return com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(v2Item(item), targetClass);
        }

        /** Returns the bytes of the binary attribute that toItem writes for the bean's property, or null if it is not binary. */
        byte[] writtenBinary(final Object bean, final String property) {
            if (this == V1) {
                final ByteBuffer binary = DynamoDBExecutor.toItem(bean).get(property).getB();
                return binary == null ? null : bytes(binary);
            }
            final SdkBytes binary = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toItem(bean).get(property).b();
            return binary == null ? null : binary.asByteArray();
        }

        <T> T roundTrip(final T bean, final Class<T> targetClass) {
            if (this == V1) {
                return DynamoDBExecutor.toEntity(DynamoDBExecutor.toItem(bean), targetClass);
            }
            return com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toItem(bean),
                    targetClass);
        }

        private static Map<String, AttributeValue> legacyItem(final Map<String, ?> item) {
            final Map<String, AttributeValue> result = new LinkedHashMap<>();
            item.forEach((key, value) -> result.put(key, legacyValue(value)));
            return result;
        }

        private static AttributeValue legacyValue(final Object value) {
            if (value == null) {
                return new AttributeValue().withNULL(true);
            } else if (value instanceof final Map<?, ?> map) {
                @SuppressWarnings("unchecked")
                final Map<String, ?> attributes = (Map<String, ?>) map;
                return new AttributeValue().withM(legacyItem(attributes));
            } else if (value instanceof final List<?> list) {
                return new AttributeValue().withL(list.stream().map(Sdk::legacyValue).toList());
            } else if (value instanceof final BinarySet set) {
                return new AttributeValue().withBS(set.values());
            } else if (value instanceof final Boolean bool) {
                return new AttributeValue().withBOOL(bool);
            } else if (value instanceof final Number number) {
                return new AttributeValue().withN(number.toString());
            } else if (value instanceof final ByteBuffer buffer) {
                return new AttributeValue().withB(buffer);
            }
            return new AttributeValue().withS((String) value);
        }

        private static Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> v2Item(final Map<String, ?> item) {
            final Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> result = new LinkedHashMap<>();
            item.forEach((key, value) -> result.put(key, v2Value(value)));
            return result;
        }

        private static software.amazon.awssdk.services.dynamodb.model.AttributeValue v2Value(final Object value) {
            if (value == null) {
                return software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromNul(true);
            } else if (value instanceof final Map<?, ?> map) {
                @SuppressWarnings("unchecked")
                final Map<String, ?> attributes = (Map<String, ?>) map;
                return software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(v2Item(attributes));
            } else if (value instanceof final List<?> list) {
                return software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL(list.stream().map(Sdk::v2Value).toList());
            } else if (value instanceof final BinarySet set) {
                return software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromBs(set.values().stream().map(SdkBytes::fromByteBuffer).toList());
            } else if (value instanceof final Boolean bool) {
                return software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromBool(bool);
            } else if (value instanceof final Number number) {
                return software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromN(number.toString());
            } else if (value instanceof final ByteBuffer buffer) {
                return software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromB(SdkBytes.fromByteBuffer(buffer));
            }
            return software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS((String) value);
        }
    }

    public enum Status {
        FIRST(10), SECOND(20);

        private final int code;

        Status(final int code) {
            this.code = code;
        }

        public int code() {
            return code;
        }
    }

    public static class ShapeRoot {
        private List<Long> longs;
        private List<String> strings;
        private Set<Long> longSet;
        private Optional<List<String>> optionalStrings;
        private Optional<List<ByteBuffer>> optionalBuffers;
        private Map<String, List<byte[]>> byteArrayLists;
        private Map<String, Long> longMap;
        @JsonXmlField(enumerated = EnumType.CODE)
        private Status coded;
        private ConcurrentHashMap<String, Long> concurrent;
        private ByteBuffer buffer;
        private ShapeLeaf direct;
        private ShapeLeaf[] array;
        private List<ShapeLeaf> leaves;

        public List<Long> getLongs() {
            return longs;
        }

        public void setLongs(final List<Long> value) {
            longs = value;
        }

        public List<String> getStrings() {
            return strings;
        }

        public void setStrings(final List<String> value) {
            strings = value;
        }

        public Set<Long> getLongSet() {
            return longSet;
        }

        public void setLongSet(final Set<Long> value) {
            longSet = value;
        }

        public Optional<List<String>> getOptionalStrings() {
            return optionalStrings;
        }

        public void setOptionalStrings(final Optional<List<String>> value) {
            optionalStrings = value;
        }

        public Optional<List<ByteBuffer>> getOptionalBuffers() {
            return optionalBuffers;
        }

        public void setOptionalBuffers(final Optional<List<ByteBuffer>> value) {
            optionalBuffers = value;
        }

        public Map<String, List<byte[]>> getByteArrayLists() {
            return byteArrayLists;
        }

        public void setByteArrayLists(final Map<String, List<byte[]>> value) {
            byteArrayLists = value;
        }

        public Map<String, Long> getLongMap() {
            return longMap;
        }

        public void setLongMap(final Map<String, Long> value) {
            longMap = value;
        }

        public Status getCoded() {
            return coded;
        }

        public void setCoded(final Status value) {
            coded = value;
        }

        public ConcurrentHashMap<String, Long> getConcurrent() {
            return concurrent;
        }

        public void setConcurrent(final ConcurrentHashMap<String, Long> value) {
            concurrent = value;
        }

        public ByteBuffer getBuffer() {
            return buffer;
        }

        public void setBuffer(final ByteBuffer value) {
            buffer = value;
        }

        public ShapeLeaf getDirect() {
            return direct;
        }

        public void setDirect(final ShapeLeaf value) {
            direct = value;
        }

        public ShapeLeaf[] getArray() {
            return array;
        }

        public void setArray(final ShapeLeaf[] value) {
            array = value;
        }

        public List<ShapeLeaf> getLeaves() {
            return leaves;
        }

        public void setLeaves(final List<ShapeLeaf> value) {
            leaves = value;
        }
    }

    public static class ShapeLeaf {
        private List<Long> longValues;
        private List<List<Long>> nestedLongs;
        private List<List<ByteBuffer>> bufferLists;
        private List<List<Object>> nestedObjects;
        private Map<String, List<Long>> longLists;
        private Map<Number, String> numberKeys;
        private ConcurrentHashMap<String, String> concurrent;
        private ByteBuffer buffer;
        private ImmutableCollection<Named> namedValues;
        private ImmutableBiMap<String, Named> namedIndex;

        public List<Long> getLongValues() {
            return longValues;
        }

        public void setLongValues(final List<Long> value) {
            longValues = value;
        }

        public List<List<Long>> getNestedLongs() {
            return nestedLongs;
        }

        public void setNestedLongs(final List<List<Long>> value) {
            nestedLongs = value;
        }

        public List<List<ByteBuffer>> getBufferLists() {
            return bufferLists;
        }

        public void setBufferLists(final List<List<ByteBuffer>> value) {
            bufferLists = value;
        }

        public List<List<Object>> getNestedObjects() {
            return nestedObjects;
        }

        public void setNestedObjects(final List<List<Object>> value) {
            nestedObjects = value;
        }

        public Map<String, List<Long>> getLongLists() {
            return longLists;
        }

        public void setLongLists(final Map<String, List<Long>> value) {
            longLists = value;
        }

        public Map<Number, String> getNumberKeys() {
            return numberKeys;
        }

        public void setNumberKeys(final Map<Number, String> value) {
            numberKeys = value;
        }

        public ConcurrentHashMap<String, String> getConcurrent() {
            return concurrent;
        }

        public void setConcurrent(final ConcurrentHashMap<String, String> value) {
            concurrent = value;
        }

        public ByteBuffer getBuffer() {
            return buffer;
        }

        public void setBuffer(final ByteBuffer value) {
            buffer = value;
        }

        public ImmutableCollection<Named> getNamedValues() {
            return namedValues;
        }

        public void setNamedValues(final ImmutableCollection<Named> value) {
            namedValues = value;
        }

        public ImmutableBiMap<String, Named> getNamedIndex() {
            return namedIndex;
        }

        public void setNamedIndex(final ImmutableBiMap<String, Named> value) {
            namedIndex = value;
        }
    }

    public static class Named {
        private String name;

        public String getName() {
            return name;
        }

        public void setName(final String value) {
            name = value;
        }
    }

    public static class FlagRoot {
        private List<FlagLeaf> flags;
        private FlagLeaf direct;

        public List<FlagLeaf> getFlags() {
            return flags;
        }

        public void setFlags(final List<FlagLeaf> value) {
            flags = value;
        }

        public FlagLeaf getDirect() {
            return direct;
        }

        public void setDirect(final FlagLeaf value) {
            direct = value;
        }
    }

    public static class FlagLeaf {
        @JsonXmlField(numberFormat = "0")
        private Boolean formattedFlag;
        @JsonXmlField(dateFormat = "yyyy")
        private Boolean datedFlag;
        @JsonXmlField(numberFormat = "0")
        private Long formattedLong;
        @JsonXmlField(numberFormat = "0")
        private Object formattedObject;
        private Boolean flag;
        private boolean primitiveFlag;
        private String text;
        private Object untyped;
        private Comparable<?> comparable;
        private Integer count;

        public Boolean getFormattedFlag() {
            return formattedFlag;
        }

        public void setFormattedFlag(final Boolean value) {
            formattedFlag = value;
        }

        public Boolean getDatedFlag() {
            return datedFlag;
        }

        public void setDatedFlag(final Boolean value) {
            datedFlag = value;
        }

        public Long getFormattedLong() {
            return formattedLong;
        }

        public void setFormattedLong(final Long value) {
            formattedLong = value;
        }

        public Object getFormattedObject() {
            return formattedObject;
        }

        public void setFormattedObject(final Object value) {
            formattedObject = value;
        }

        public Boolean getFlag() {
            return flag;
        }

        public void setFlag(final Boolean value) {
            flag = value;
        }

        public boolean isPrimitiveFlag() {
            return primitiveFlag;
        }

        public void setPrimitiveFlag(final boolean value) {
            primitiveFlag = value;
        }

        public String getText() {
            return text;
        }

        public void setText(final String value) {
            text = value;
        }

        public Object getUntyped() {
            return untyped;
        }

        public void setUntyped(final Object value) {
            untyped = value;
        }

        public Comparable<?> getComparable() {
            return comparable;
        }

        public void setComparable(final Comparable<?> value) {
            comparable = value;
        }

        public Integer getCount() {
            return count;
        }

        public void setCount(final Integer value) {
            count = value;
        }
    }

    public static class TypedRoot extends TypedLeaf {
        private Pair<Long, Long> pair;
        private ByteBuffer[] bufferArray;
        private ArrayBlockingQueue<ByteBuffer> bufferQueue;
        private TypedLeaf direct;
        private List<TypedLeaf> leaves;

        public Pair<Long, Long> getPair() {
            return pair;
        }

        public void setPair(final Pair<Long, Long> value) {
            pair = value;
        }

        public ByteBuffer[] getBufferArray() {
            return bufferArray;
        }

        public void setBufferArray(final ByteBuffer[] value) {
            bufferArray = value;
        }

        public ArrayBlockingQueue<ByteBuffer> getBufferQueue() {
            return bufferQueue;
        }

        public void setBufferQueue(final ArrayBlockingQueue<ByteBuffer> value) {
            bufferQueue = value;
        }

        public TypedLeaf getDirect() {
            return direct;
        }

        public void setDirect(final TypedLeaf value) {
            direct = value;
        }

        public List<TypedLeaf> getLeaves() {
            return leaves;
        }

        public void setLeaves(final List<TypedLeaf> value) {
            leaves = value;
        }
    }

    public static class TypedLeaf {
        private Number number;
        private List<Number> numbers;
        private List<List<Object>> nestedObjects;
        private List<Named[]> namedArrays;
        private Optional<Long>[] optionalLongs;
        private Map<String, Long>[] longMaps;
        private List<Long> longValues;
        private Map<String, Long> longMap;
        private List<byte[]> byteArrays;
        private Long[] longArray;
        private ArrayBlockingQueue<Long> longQueue;
        private ArrayBlockingQueue<Object> untypedQueue;
        private java.util.PriorityQueue<Long> priorityQueue;
        private Comparable<?> comparable;
        private List<Date> dates;
        private List<Character> chars;

        public ArrayBlockingQueue<Object> getUntypedQueue() {
            return untypedQueue;
        }

        public void setUntypedQueue(final ArrayBlockingQueue<Object> value) {
            untypedQueue = value;
        }

        public java.util.PriorityQueue<Long> getPriorityQueue() {
            return priorityQueue;
        }

        public void setPriorityQueue(final java.util.PriorityQueue<Long> value) {
            priorityQueue = value;
        }

        public Comparable<?> getComparable() {
            return comparable;
        }

        public void setComparable(final Comparable<?> value) {
            comparable = value;
        }

        public List<Date> getDates() {
            return dates;
        }

        public void setDates(final List<Date> value) {
            dates = value;
        }

        public List<Character> getChars() {
            return chars;
        }

        public void setChars(final List<Character> value) {
            chars = value;
        }

        public Number getNumber() {
            return number;
        }

        public void setNumber(final Number value) {
            number = value;
        }

        public List<Number> getNumbers() {
            return numbers;
        }

        public void setNumbers(final List<Number> value) {
            numbers = value;
        }

        public List<List<Object>> getNestedObjects() {
            return nestedObjects;
        }

        public void setNestedObjects(final List<List<Object>> value) {
            nestedObjects = value;
        }

        public List<Named[]> getNamedArrays() {
            return namedArrays;
        }

        public void setNamedArrays(final List<Named[]> value) {
            namedArrays = value;
        }

        public Optional<Long>[] getOptionalLongs() {
            return optionalLongs;
        }

        public void setOptionalLongs(final Optional<Long>[] value) {
            optionalLongs = value;
        }

        public Map<String, Long>[] getLongMaps() {
            return longMaps;
        }

        public void setLongMaps(final Map<String, Long>[] value) {
            longMaps = value;
        }

        public List<Long> getLongValues() {
            return longValues;
        }

        public void setLongValues(final List<Long> value) {
            longValues = value;
        }

        public Map<String, Long> getLongMap() {
            return longMap;
        }

        public void setLongMap(final Map<String, Long> value) {
            longMap = value;
        }

        public List<byte[]> getByteArrays() {
            return byteArrays;
        }

        public void setByteArrays(final List<byte[]> value) {
            byteArrays = value;
        }

        public Long[] getLongArray() {
            return longArray;
        }

        public void setLongArray(final Long[] value) {
            longArray = value;
        }

        public ArrayBlockingQueue<Long> getLongQueue() {
            return longQueue;
        }

        public void setLongQueue(final ArrayBlockingQueue<Long> value) {
            longQueue = value;
        }
    }

    public static class WrapperRoot {
        private Optional<String> text;
        private Holder<String> held;
        private Nullable<Long> nullable;
        private Optional<ByteBuffer> buffer;
        private com.landawn.abacus.util.u.Optional<ByteBuffer> abacusBuffer;
        private Nullable<ByteBuffer> nullableBuffer;
        private Holder<ByteBuffer> heldBuffer;
        private Optional<Optional<ByteBuffer>> nestedBuffer;
        private Optional<byte[]> bytes;
        private Holder<Optional<String>> heldOptional;

        public Nullable<ByteBuffer> getNullableBuffer() {
            return nullableBuffer;
        }

        public void setNullableBuffer(final Nullable<ByteBuffer> value) {
            nullableBuffer = value;
        }

        public Holder<ByteBuffer> getHeldBuffer() {
            return heldBuffer;
        }

        public void setHeldBuffer(final Holder<ByteBuffer> value) {
            heldBuffer = value;
        }

        public Optional<Optional<ByteBuffer>> getNestedBuffer() {
            return nestedBuffer;
        }

        public void setNestedBuffer(final Optional<Optional<ByteBuffer>> value) {
            nestedBuffer = value;
        }

        public Optional<byte[]> getBytes() {
            return bytes;
        }

        public void setBytes(final Optional<byte[]> value) {
            bytes = value;
        }

        public Holder<Optional<String>> getHeldOptional() {
            return heldOptional;
        }

        public void setHeldOptional(final Holder<Optional<String>> value) {
            heldOptional = value;
        }

        public Optional<String> getText() {
            return text;
        }

        public void setText(final Optional<String> value) {
            text = value;
        }

        public Holder<String> getHeld() {
            return held;
        }

        public void setHeld(final Holder<String> value) {
            held = value;
        }

        public Nullable<Long> getNullable() {
            return nullable;
        }

        public void setNullable(final Nullable<Long> value) {
            nullable = value;
        }

        public Optional<ByteBuffer> getBuffer() {
            return buffer;
        }

        public void setBuffer(final Optional<ByteBuffer> value) {
            buffer = value;
        }

        public com.landawn.abacus.util.u.Optional<ByteBuffer> getAbacusBuffer() {
            return abacusBuffer;
        }

        public void setAbacusBuffer(final com.landawn.abacus.util.u.Optional<ByteBuffer> value) {
            abacusBuffer = value;
        }
    }
}
