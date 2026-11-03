package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWriter;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNumber;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import com.mongodb.client.MongoIterable;

/**
 * Review 2026-11-02 of {@link MongoDBBase}: BsonDocument row types, text-stored {@link Number} values, embedded documents
 * requested as BSON value types, and the in-place normalization of the private row copy in {@code toEntity}.
 */
class MongoDBBaseReview20261102Test {

    public static class NumberHolder {
        private String id;
        private Number num;
        private List<Number> nums;
        private Map<String, Number> byName;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public Number getNum() {
            return num;
        }

        public void setNum(final Number num) {
            this.num = num;
        }

        public List<Number> getNums() {
            return nums;
        }

        public void setNums(final List<Number> nums) {
            this.nums = nums;
        }

        public Map<String, Number> getByName() {
            return byName;
        }

        public void setByName(final Map<String, Number> byName) {
            this.byName = byName;
        }
    }

    public record NumberRecord(Number n, String name) {
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

    public static class Person {
        private String id;
        private List<Address> addresses;
        private Long age;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public List<Address> getAddresses() {
            return addresses;
        }

        public void setAddresses(final List<Address> addresses) {
            this.addresses = addresses;
        }

        public Long getAge() {
            return age;
        }

        public void setAge(final Long age) {
            this.age = age;
        }
    }

    @SuppressWarnings("serial")
    public static class CustomBsonDocument extends BsonDocument {
    }

    private static Document roundTrip(final Document doc) {
        final Codec<Document> codec = MongoDBBase.codecRegistry.get(Document.class);
        final BsonDocument bson = new BsonDocument();
        codec.encode(new BsonDocumentWriter(bson), doc, EncoderContext.builder().build());
        return codec.decode(new BsonDocumentReader(bson), DecoderContext.builder().build());
    }

    // ---- BsonDocument row types (reported by s05) ----

    @Test
    void readRow_bsonDocumentRowType_encodesTheRow() {
        final ObjectId id = new ObjectId();
        final BsonDocument row = MongoDBBase.readRow(new Document("_id", id).append("a", 1).append("s", "x"), BsonDocument.class);

        // HEAD: ClassCastException (ObjectId cannot be cast to BsonValue).
        assertEquals(new BsonObjectId(id), row.get("_id"));
        assertEquals(new BsonInt32(1), row.get("a"));
        assertEquals(new BsonString("x"), row.get("s"));
    }

    @Test
    void readRow_rawBsonDocumentRowType_encodesTheRow() {
        final ObjectId id = new ObjectId();
        final RawBsonDocument row = MongoDBBase.readRow(new Document("_id", id).append("a", 1), RawBsonDocument.class);

        // HEAD: IllegalArgumentException "Not able to create instance for Map".
        assertEquals(new BsonObjectId(id), row.get("_id"));
        assertEquals(new BsonInt32(1), row.get("a"));
    }

    @Test
    void readRow_otherBsonDocumentSubclass_isRejectedClearly() {
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> MongoDBBase.readRow(new Document("a", 1), CustomBsonDocument.class));
        assertEquals(true, e.getMessage().contains("BsonDocument and RawBsonDocument"));
    }

    @Test
    void toList_bsonDocumentRowTypes_encodeEveryRow() {
        final MongoIterable<?> iterable = mock(MongoIterable.class);
        final Map<String, Object> plainMap = new LinkedHashMap<>();
        plainMap.put("b", "y");
        MongoIterableStubs.stubRows(iterable, List.of(new Document("a", 1), plainMap));

        final List<BsonDocument> bson = MongoDBBase.toList(iterable, BsonDocument.class);
        assertEquals(List.of(new BsonDocument("a", new BsonInt32(1)), new BsonDocument("b", new BsonString("y"))), bson);

        final List<RawBsonDocument> raw = MongoDBBase.toList(iterable, RawBsonDocument.class);
        assertEquals(new BsonInt32(1), raw.get(0).get("a"));
        assertEquals(new BsonString("y"), raw.get(1).get("b"));
    }

    // ---- embedded document as a BSON value type ----

    @Test
    void convertBsonValue_embeddedDocumentToBsonValueType_throwsIllegalArgument() {
        // HEAD: NullPointerException ("data" is null) from Binary's constructor.
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.convertBsonValue(new Document("x", 1), Binary.class));
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.readRow(new Document("c", new Document("x", 1)), Code.class));

        final Binary binary = new Binary(new byte[] { 1 });
        assertSame(binary, MongoDBBase.convertBsonValue(binary, Binary.class));

        // Neither an empty document nor a null property value builds an empty value (HEAD: Code{code=null}).
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.convertBsonValue(new Document(), Code.class));
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.convertBsonValue(new Document("code", null), Code.class));
        // Binary's own properties without its data: the conversion failure (HEAD: NullPointerException) becomes an IAE.
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> MongoDBBase.convertBsonValue(new Document("type", (byte) 0), Binary.class));
        assertInstanceOf(NullPointerException.class, e.getCause());
        // A BsonValue class is never filled from an embedded document.
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.convertBsonValue(new BsonDocument("value", new BsonInt32(1)), BsonInt32.class));
    }

    @Test
    void convertBsonValue_embeddedDocumentOfTheClassProperties_stillConverts() {
        // HEAD converted these through N.convert; only documents that do not describe the class are rejected.
        assertEquals(new Code("x"), MongoDBBase.convertBsonValue(new Document("code", "x"), Code.class));
        assertEquals(new Code("x"), MongoDBBase.readRow(new Document("c", new Document("code", "x")), Code.class));

        final Binary binary = MongoDBBase.convertBsonValue(new Document("data", new byte[] { 1, 2 }).append("type", (byte) 5), Binary.class);
        assertEquals(new Binary((byte) 5, new byte[] { 1, 2 }), binary);
        assertEquals(new Binary(new byte[] { 3 }), MongoDBBase.convertBsonValue(new Document("data", new byte[] { 3 }), Binary.class));
    }

    // ---- scalar BsonValue row types (reported by s09) ----

    @Test
    void convertBsonValue_javaValueToMatchingBsonValueClass_isEncoded() {
        // HEAD: ParsingException "Can't parse: a" / IllegalArgumentException "Conversion returned java.lang.String".
        assertEquals(new BsonString("a"), MongoDBBase.convertBsonValue("a", BsonString.class));
        assertEquals(new BsonInt32(7), MongoDBBase.convertBsonValue(7, BsonInt32.class));
        assertEquals(new BsonInt32(7), MongoDBBase.convertBsonValue(7, BsonValue.class));
        assertEquals(new BsonInt64(7L), MongoDBBase.convertBsonValue(7L, BsonNumber.class));
        assertEquals(new BsonDocument("x", new BsonInt32(1)), MongoDBBase.convertBsonValue(new Document("x", 1), BsonValue.class));

        final BsonString same = new BsonString("s");
        assertSame(same, MongoDBBase.convertBsonValue(same, BsonValue.class));
        assertEquals(null, MongoDBBase.convertBsonValue(null, BsonString.class));

        // A value whose BSON form is another type is rejected, not reinterpreted.
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.convertBsonValue(7, BsonString.class));
    }

    @Test
    void convertBsonValue_bsonValueOfAnotherClass_keepsTheRegularConversion() {
        // A BsonDocument row holds BsonValues, and MongoDB mixes int32/int64 for one field: HEAD converted these through
        // N.convert; encoding a BsonValue yields itself, so the encode branch must not reject them.
        assertEquals(new BsonInt64(5L), MongoDBBase.convertBsonValue(new BsonInt32(5), BsonInt64.class));
        assertEquals(new BsonInt32(5), MongoDBBase.convertBsonValue(new BsonInt64(5L), BsonInt32.class));

        final MongoIterable<?> iterable = mock(MongoIterable.class);
        MongoIterableStubs.stubRows(iterable, List.of(new BsonDocument("v", new BsonInt32(1)), new BsonDocument("v", new BsonInt64(2L))));
        assertEquals(List.of(new BsonInt64(1L), new BsonInt64(2L)), MongoDBBase.toList(iterable, BsonInt64.class));
    }

    @Test
    void convertBsonValue_javaNumberOfAnotherNumericBsonClass_converts() {
        // A field of ordinary Document rows mixes Integer and Long values. Before this fix: IllegalArgumentException
        // "it is stored as a BSON INT32" (HEAD: ParsingException), while the same BsonInt32 of a BsonDocument row converted.
        assertEquals(new BsonInt64(1L), MongoDBBase.convertBsonValue(1, BsonInt64.class));
        assertEquals(new BsonInt32(2), MongoDBBase.convertBsonValue(2L, BsonInt32.class));
        assertEquals(new BsonDouble(3.0), MongoDBBase.convertBsonValue(3, BsonDouble.class));
        assertEquals(new BsonDecimal128(Decimal128.parse("4")), MongoDBBase.convertBsonValue(4L, BsonDecimal128.class));
        assertEquals(new BsonInt64(5L), MongoDBBase.readRow(new Document("v", 5), BsonInt64.class));

        final MongoIterable<?> iterable = mock(MongoIterable.class);
        MongoIterableStubs.stubRows(iterable, List.of(new Document("v", 1), new Document("v", 2L)));
        assertEquals(List.of(new BsonInt64(1L), new BsonInt64(2L)), MongoDBBase.toList(iterable, BsonInt64.class));

        // As for a BsonInt64 value, a number out of the target's range overflows, and a non-numeric class is still rejected.
        assertThrows(ArithmeticException.class, () -> MongoDBBase.convertBsonValue(1L << 40, BsonInt32.class));
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.convertBsonValue(1, BsonString.class));
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.convertBsonValue(1L, BsonDateTime.class));
    }

    @Test
    void toList_beanRowsRequestedAsBsonDocument_encodeTheBeanProperties() {
        final Address address = new Address();
        address.setCity("Paris");
        final MongoIterable<?> iterable = mock(MongoIterable.class);
        MongoIterableStubs.stubRows(iterable, List.of(address));

        assertEquals(List.of(new BsonDocument("city", new BsonString("Paris"))), MongoDBBase.toList(iterable, BsonDocument.class));
        assertEquals(new BsonString("Paris"), MongoDBBase.toList(iterable, RawBsonDocument.class).get(0).get("city"));
    }

    @Test
    void readRowAndToList_scalarBsonValueRowTypes_readTheSoleField() {
        assertEquals(new BsonString("a"), MongoDBBase.readRow(new Document("_id", new ObjectId()).append("name", "a"), BsonString.class));
        assertEquals(new BsonInt32(3), MongoDBBase.readRow(new Document("n", 3), BsonValue.class));

        final MongoIterable<?> iterable = mock(MongoIterable.class);
        MongoIterableStubs.stubRows(iterable, List.of(new Document("v", 1), new Document("v", 2)));
        assertEquals(List.of(new BsonInt32(1), new BsonInt32(2)), MongoDBBase.toList(iterable, BsonInt32.class));
    }

    // ---- Number values stored as text ----

    @Test
    void numberProperty_holdingBigInteger_roundTripsThroughTheCodec() {
        final NumberHolder holder = new NumberHolder();
        holder.setNum(new BigInteger("12345678901234567890"));
        holder.setNums(List.of(new BigInteger("5"), 7L));
        holder.setByName(Map.of("k", new BigInteger("9")));

        final Document stored = roundTrip(MongoDBBase.toDocument(holder));
        // GeneralCodec writes a BigInteger (no driver codec) as its text.
        assertEquals("12345678901234567890", stored.get("num"));

        // HEAD: UnsupportedOperationException "No public static method or constructor found in java.lang.Number".
        final NumberHolder back = MongoDBBase.toEntity(stored, NumberHolder.class);
        assertEquals(new BigInteger("12345678901234567890"), back.getNum());
        assertEquals(List.of(5, 7L), back.getNums());
        assertEquals(9, back.getByName().get("k"));
    }

    @Test
    void numberRecordComponent_storedAsTextByEarlierVersions_readsBack() {
        // Earlier versions stored every value of a Number-typed record component as its text.
        final NumberRecord rec = MongoDBBase.toEntity(new Document("n", "11").append("name", "a"), NumberRecord.class);

        assertEquals(11, rec.n());
        assertEquals("a", rec.name());
    }

    @Test
    void numberScalar_fromText_isParsed() {
        assertEquals(5, (Object) MongoDBBase.readRow(new Document("n", "5"), Number.class));
        assertEquals(new BigInteger("98765432109876543210"), MongoDBBase.convertBsonValue("98765432109876543210", Number.class));
        assertEquals(1.5, MongoDBBase.convertBsonValue("1.5", Number.class));
        assertThrows(NumberFormatException.class, () -> MongoDBBase.convertBsonValue("abc", Number.class));
    }

    @Test
    void generalCodecForNumber_decodesStoredText() {
        final Codec<Number> codec = MongoDBBase.codecRegistry.get(Number.class);
        final BsonDocumentReader reader = new BsonDocumentReader(new BsonDocument("v", new BsonString("123456789012345678901")));
        reader.readStartDocument();
        reader.readName();

        // HEAD: UnsupportedOperationException from N.valueOf(String, Number.class).
        assertEquals(new BigInteger("123456789012345678901"), codec.decode(reader, DecoderContext.builder().build()));
    }

    // ---- toEntity converts in its private copy (perf): results and the source stay as before ----

    @Test
    void toEntity_convertedValues_sourceDocumentUnchanged() {
        final ObjectId id = new ObjectId();
        final List<Document> addresses = new ArrayList<>(List.of(new Document("city", "NYC")));
        final Document doc = new Document("_id", id).append("addresses", addresses).append("age", 3);
        final Document snapshot = new Document(new LinkedHashMap<>(doc));

        final Person person = MongoDBBase.toEntity(doc, Person.class);

        assertEquals(id.toHexString(), person.getId());
        assertEquals("NYC", person.getAddresses().get(0).getCity());
        assertEquals(3L, person.getAge());
        assertEquals(snapshot, doc);
        assertSame(addresses, doc.get("addresses"));
        assertInstanceOf(Document.class, addresses.get(0));

        // An embedded document without id mapping is not copied, so it must not be modified either.
        final Document inner = new Document("city", "LA");
        final Document outer = new Document("addresses", List.of(inner));
        assertEquals("LA", MongoDBBase.toEntity(outer, Person.class).getAddresses().get(0).getCity());
        assertEquals(new Document("city", "LA"), inner);
        assertInstanceOf(Document.class, ((List<?>) outer.get("addresses")).get(0));
    }
}
