package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWriter;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWithScope;
import org.bson.types.Symbol;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.util.Holder;
import com.landawn.abacus.util.u;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;

/**
 * Values that {@link MongoDBBase.GeneralCodec} receives through the shared codec registry and must store the way a
 * {@link Document} field holding them is stored.
 */
class MongoDBBaseDeepReview2026CodecValueTest {

    public static class WrapperEntity {
        private u.Optional<String> opt;
        private java.util.Optional<String> jdkOpt;
        private u.Nullable<String> nullable;
        private u.OptionalInt optInt;
        private java.util.OptionalLong jdkOptLong;
        private Holder<String> holder;
        private u.Optional<String> present;

        public u.Optional<String> getOpt() {
            return opt;
        }

        public void setOpt(final u.Optional<String> opt) {
            this.opt = opt;
        }

        public java.util.Optional<String> getJdkOpt() {
            return jdkOpt;
        }

        public void setJdkOpt(final java.util.Optional<String> jdkOpt) {
            this.jdkOpt = jdkOpt;
        }

        public u.Nullable<String> getNullable() {
            return nullable;
        }

        public void setNullable(final u.Nullable<String> nullable) {
            this.nullable = nullable;
        }

        public u.OptionalInt getOptInt() {
            return optInt;
        }

        public void setOptInt(final u.OptionalInt optInt) {
            this.optInt = optInt;
        }

        public java.util.OptionalLong getJdkOptLong() {
            return jdkOptLong;
        }

        public void setJdkOptLong(final java.util.OptionalLong jdkOptLong) {
            this.jdkOptLong = jdkOptLong;
        }

        public Holder<String> getHolder() {
            return holder;
        }

        public void setHolder(final Holder<String> holder) {
            this.holder = holder;
        }

        public u.Optional<String> getPresent() {
            return present;
        }

        public void setPresent(final u.Optional<String> present) {
            this.present = present;
        }
    }

    public record ValueRecord(String name, Object payload, Number num, Map<String, Object> attrs, List<Object> items) {
    }

    public static class RecordHolder {
        private ValueRecord rec;

        public ValueRecord getRec() {
            return rec;
        }

        public void setRec(final ValueRecord rec) {
            this.rec = rec;
        }
    }

    /** Encodes {@code doc} with the registry the MongoDB facades give their databases. */
    private static BsonDocument encode(final Document doc) {
        final BsonDocument out = new BsonDocument();
        MongoDBBase.codecRegistry.get(Document.class).encode(new BsonDocumentWriter(out), doc, EncoderContext.builder().build());
        return out;
    }

    private static Document decode(final BsonDocument bson) {
        return MongoDBBase.codecRegistry.get(Document.class).decode(new BsonDocumentReader(bson), DecoderContext.builder().build());
    }

    // ---- 2026-10-27 sliceT ----

    @Test
    void emptyValueWrappersAreStoredAsBsonNullAndReadBackEmpty() {
        // An empty Optional/Nullable/OptionalInt/... or a Holder of null has no N.stringOf form, and GeneralCodec wrote that null
        // with writeString: an entity with any such property could not be inserted at all ("value can not be null"). Such a value
        // is now stored as BSON null (as DynamoDB stores NULL), which reads back as the empty wrapper.
        final WrapperEntity entity = new WrapperEntity();
        entity.setOpt(u.Optional.empty());
        entity.setJdkOpt(java.util.Optional.empty());
        entity.setNullable(u.Nullable.empty());
        entity.setOptInt(u.OptionalInt.empty());
        entity.setJdkOptLong(java.util.OptionalLong.empty());
        entity.setHolder(Holder.of(null));
        entity.setPresent(u.Optional.of("p"));

        assertAll(() -> assertEquals(BsonNull.VALUE, encode(new Document("v", u.Optional.empty())).get("v")),
                () -> assertEquals(BsonNull.VALUE, encode(new Document("v", java.util.Optional.empty())).get("v")),
                () -> assertEquals(BsonNull.VALUE, encode(new Document("v", u.Nullable.empty())).get("v")),
                () -> assertEquals(BsonNull.VALUE, encode(new Document("v", u.OptionalInt.empty())).get("v")),
                () -> assertEquals(BsonNull.VALUE, encode(new Document("v", java.util.OptionalLong.empty())).get("v")),
                () -> assertEquals(BsonNull.VALUE, encode(new Document("v", Holder.of(null))).get("v")),
                // In a nested Map or a List the empty wrapper is a BSON null as well.
                () -> assertEquals(BsonNull.VALUE, encode(new Document("m", new Document("e", u.Optional.empty()))).getDocument("m").get("e")),
                () -> assertEquals(new BsonArray(Arrays.asList(new BsonString("x"), BsonNull.VALUE)),
                        encode(new Document("l", Arrays.asList("x", u.Optional.empty()))).get("l")),
                // Unchanged: a present wrapper is stored as the text of its value.
                () -> assertEquals(new BsonString("p"), encode(new Document("v", u.Optional.of("p"))).get("v")),
                // The entity is written, and every empty wrapper reads back empty.
                () -> {
                    final BsonDocument stored = encode(MongoDBBase.toDocument(entity));
                    final WrapperEntity read = MongoDBBase.toEntity(decode(stored), WrapperEntity.class);

                    assertAll(() -> assertEquals(BsonNull.VALUE, stored.get("opt")), () -> assertEquals(BsonNull.VALUE, stored.get("holder")),
                            () -> assertFalse(read.getOpt().isPresent()), () -> assertFalse(read.getJdkOpt().isPresent()),
                            () -> assertFalse(read.getNullable().isPresent()), () -> assertFalse(read.getOptInt().isPresent()),
                            () -> assertFalse(read.getJdkOptLong().isPresent()), () -> assertEquals(null, read.getHolder().value()),
                            () -> assertEquals(u.Optional.of("p"), read.getPresent()));
                });
    }

    @Test
    void recordComponentsOfAbstractDeclaredTypesAreStoredByTheirValues() {
        // A record held by an entity is encoded by the driver's record codec, which looks each component's codec up by its DECLARED
        // type. Object, Number, ... have no driver codec, so GeneralCodec rendered the value through N.stringOf: a Number component
        // holding 11 was stored as "11" and could not be read back at all, and the values of a Map<String, Object> or List<Object>
        // component turned into text. They are now stored as a Document field holding the same value is stored.
        final Date day = new Date(1_700_000_000_123L);
        final Map<String, Object> attrs = new LinkedHashMap<>();
        attrs.put("n", 1);
        attrs.put("d", day);
        attrs.put("m", new Document("k", 2));
        final ValueRecord rec = new ValueRecord("r", 5, 11L, attrs, Arrays.asList(1, "a", true));
        final RecordHolder holder = new RecordHolder();
        holder.setRec(rec);

        final BsonDocument stored = encode(MongoDBBase.toDocument(holder)).getDocument("rec");

        assertAll(() -> assertEquals(new BsonString("r"), stored.get("name")), () -> assertEquals(new BsonInt32(5), stored.get("payload")),
                () -> assertEquals(new BsonInt64(11L), stored.get("num")),
                () -> assertEquals(new BsonDocument("n", new BsonInt32(1)).append("d", new BsonDateTime(day.getTime()))
                        .append("m", new BsonDocument("k", new BsonInt32(2))), stored.get("attrs")),
                () -> assertEquals(new BsonArray(Arrays.asList(new BsonInt32(1), new BsonString("a"), BsonBoolean.TRUE)), stored.get("items")),
                () -> assertEquals(rec, MongoDBBase.toEntity(decode(encode(MongoDBBase.toDocument(holder))), RecordHolder.class).getRec()));
    }

    @SuppressWarnings("unchecked")
    @Test
    void codecOfAnAbstractTypeEncodesAValueWithTheCodecOfItsOwnClass() {
        // The codecs the registry returns for Object and Number (GeneralCodec) now write a value as the codec of its own class does.
        final BsonDocument out = new BsonDocument();
        final BsonDocumentWriter writer = new BsonDocumentWriter(out);
        writer.writeStartDocument();
        writer.writeName("o");
        ((Codec<Object>) MongoDBBase.codecRegistry.get(Object.class)).encode(writer, new Document("k", 1), EncoderContext.builder().build());
        writer.writeName("n");
        ((Codec<Number>) MongoDBBase.codecRegistry.get(Number.class)).encode(writer, 2.5, EncoderContext.builder().build());
        writer.writeName("s");
        ((Codec<Object>) MongoDBBase.codecRegistry.get(Object.class)).encode(writer, "text", EncoderContext.builder().build());
        writer.writeEndDocument();

        assertAll(() -> assertEquals(new BsonDocument("k", new BsonInt32(1)), out.get("o")), () -> assertEquals(2.5, out.getDouble("n").getValue()),
                () -> assertEquals(new BsonString("text"), out.get("s")),
                () -> assertTrue(MongoDBBase.codecRegistry.get(Object.class) instanceof MongoDBBase.GeneralCodec));
    }

    public static class PatternEntity {
        private Pattern pattern;
        private List<Pattern> patterns;
        private Map<String, Pattern> byName;
        private PatternRecord rec;

        public Pattern getPattern() {
            return pattern;
        }

        public void setPattern(final Pattern pattern) {
            this.pattern = pattern;
        }

        public List<Pattern> getPatterns() {
            return patterns;
        }

        public void setPatterns(final List<Pattern> patterns) {
            this.patterns = patterns;
        }

        public Map<String, Pattern> getByName() {
            return byName;
        }

        public void setByName(final Map<String, Pattern> byName) {
            this.byName = byName;
        }

        public PatternRecord getRec() {
            return rec;
        }

        public void setRec(final PatternRecord rec) {
            this.rec = rec;
        }
    }

    public record PatternRecord(Pattern p) {
    }

    private static String describe(final Pattern pattern) {
        return pattern == null ? null : pattern.pattern() + "/" + pattern.flags();
    }

    @Test
    void patternValuesReadBackFromTheRegularExpressionTheDriverWrites() {
        // The driver's PatternCodec stores a Pattern as a BSON regular expression, which a Document field decodes as a
        // BsonRegularExpression. N.convert compiled that value's JSON text ({"pattern": ..., "options": ...}), so every read of a
        // Pattern property, element or single value failed with PatternSyntaxException. It is now decoded as the driver's codec
        // decodes it, options included.
        final Pattern pattern = Pattern.compile("a+b", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
        final String expected = describe(pattern);
        final PatternEntity entity = new PatternEntity();
        entity.setPattern(pattern);
        entity.setPatterns(Arrays.asList(pattern));
        final Map<String, Pattern> byName = new LinkedHashMap<>();
        byName.put("k", pattern);
        entity.setByName(byName);
        entity.setRec(new PatternRecord(pattern));
        final Document stored = decode(encode(MongoDBBase.toDocument(entity)));

        assertAll(() -> assertEquals(org.bson.BsonRegularExpression.class, stored.get("pattern").getClass()),
                () -> assertEquals(expected, describe(MongoDBBase.toEntity(stored, PatternEntity.class).getPattern())),
                () -> assertEquals(expected, describe(MongoDBBase.toEntity(stored, PatternEntity.class).getPatterns().get(0))),
                () -> assertEquals(expected, describe(MongoDBBase.toEntity(stored, PatternEntity.class).getByName().get("k"))),
                () -> assertEquals(expected, describe(MongoDBBase.toEntity(stored, PatternEntity.class).getRec().p())),
                () -> assertEquals(expected, describe(MongoDBBase.convertBsonValue(stored.get("pattern"), Pattern.class))),
                () -> assertEquals(expected, describe(MongoDBBase.readRow(new Document("_id", 1).append("p", stored.get("pattern")), Pattern.class))),
                // Unchanged: other targets do not unwrap the value.
                () -> assertEquals(stored.get("pattern"), MongoDBBase.convertBsonValue(stored.get("pattern"), Object.class)));
    }

    @SuppressWarnings("unchecked")
    @Test
    void bsonValueRowTypesAreReadFromTheSoleFieldNotAsBeans() {
        // Beans.isBeanClass (and the Type) report Binary, Code, Symbol and the BsonValue classes as beans, so readRow, toList and
        // stream mapped a document onto such a row type as a bean of the document's fields: a Code came back as Code{code=null}
        // (silently wrong) and a Binary failed with NullPointerException. Such a row type is a single value, as for a scalar.
        final Binary binary = new Binary(new byte[] { 1, 2 });
        final Code code = new Code("function() {}");
        final Symbol symbol = new Symbol("sym");
        final org.bson.BsonRegularExpression regex = new org.bson.BsonRegularExpression("a+", "i");
        final MongoIterable<Document> codes = mock(MongoIterable.class);
        MongoIterableStubs.stubRows(codes, Arrays.asList(new Document("_id", 1).append("c", code), new Document("_id", 2).append("c", code)));
        final MongoCursor<Document> cursor = mock(MongoCursor.class);
        when(cursor.hasNext()).thenReturn(true, false);
        when(cursor.next()).thenReturn(new Document("_id", 1).append("b", binary));

        assertAll(() -> assertSame(binary, MongoDBBase.readRow(new Document("_id", 1).append("b", binary), Binary.class)),
                () -> assertSame(code, MongoDBBase.readRow(new Document("_id", 1).append("c", code), Code.class)),
                () -> assertSame(symbol, MongoDBBase.readRow(new Document("s", symbol), Symbol.class)),
                () -> assertSame(regex, MongoDBBase.readRow(new Document("r", regex), org.bson.BsonRegularExpression.class)),
                () -> assertEquals(Arrays.asList(code, code), MongoDBBase.toList(codes, Code.class)),
                () -> assertSame(binary, MongoDBBase.stream(cursor, Binary.class).first().orElseThrow()),
                // Several fields are rejected as for any single-value row type (it was a NullPointerException for Binary).
                () -> assertThrows(IllegalArgumentException.class, () -> MongoDBBase.readRow(new Document("b", binary).append("c", code), Binary.class)));
    }

    public static class NameBean {
        private String name;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    void immutableRowTypesGetRowsOfThatType() {
        // For an immutable row type the map/collection factories (IntFunctions.ofMap, N.newMap, N.newCollection) create a mutable
        // HashMap, TreeMap, ArrayList or HashSet, which readRow, toList and stream returned as is: a List<ImmutableMap> held
        // HashMaps, failing with ClassCastException only where a row was read. Each filled row is now converted to the row type.
        final Document doc = new Document("_id", 1).append("b", "x");
        final Map<String, Object> expected = new LinkedHashMap<>(doc);
        final MongoIterable<Document> docs = mock(MongoIterable.class);
        MongoIterableStubs.stubRows(docs, Arrays.asList(doc, new Document("_id", 2).append("b", "y")));
        final NameBean bean = new NameBean();
        bean.setName("n");
        final MongoIterable<Object> mixed = mock(MongoIterable.class);
        MongoIterableStubs.stubRows(mixed, Arrays.asList(new java.util.HashMap<>(Map.of("k", 1)), bean));
        final MongoCursor<Document> cursor = mock(MongoCursor.class);
        when(cursor.hasNext()).thenReturn(true, false);
        when(cursor.next()).thenReturn(doc);

        assertAll(() -> assertTrue(MongoDBBase.readRow(doc, com.landawn.abacus.util.ImmutableMap.class) instanceof com.landawn.abacus.util.ImmutableMap),
                () -> assertEquals(expected, MongoDBBase.readRow(doc, com.landawn.abacus.util.ImmutableMap.class)),
                () -> assertTrue(
                        MongoDBBase.readRow(doc, com.landawn.abacus.util.ImmutableSortedMap.class) instanceof com.landawn.abacus.util.ImmutableSortedMap),
                () -> assertTrue(MongoDBBase.readRow(doc, com.landawn.abacus.util.ImmutableList.class) instanceof com.landawn.abacus.util.ImmutableList),
                () -> assertEquals(Arrays.asList(1, "x"), MongoDBBase.readRow(doc, com.landawn.abacus.util.ImmutableList.class)),
                () -> assertTrue(MongoDBBase.readRow(doc, com.landawn.abacus.util.ImmutableSet.class) instanceof com.landawn.abacus.util.ImmutableSet), () -> {
                    final List<com.landawn.abacus.util.ImmutableMap> rows = MongoDBBase.toList(docs, com.landawn.abacus.util.ImmutableMap.class);
                    assertAll(() -> assertTrue(((List) rows).get(0) instanceof com.landawn.abacus.util.ImmutableMap),
                            () -> assertTrue(((List) rows).get(1) instanceof com.landawn.abacus.util.ImmutableMap), () -> assertEquals(expected, rows.get(0)));
                }, () -> {
                    final List<?> rows = MongoDBBase.toList(mixed, com.landawn.abacus.util.ImmutableMap.class);
                    assertAll(() -> assertTrue(rows.get(0) instanceof com.landawn.abacus.util.ImmutableMap),
                            () -> assertTrue(rows.get(1) instanceof com.landawn.abacus.util.ImmutableMap),
                            () -> assertEquals(Map.of("name", "n"), rows.get(1)));
                },
                () -> assertTrue((Object) MongoDBBase.stream(cursor, com.landawn.abacus.util.ImmutableMap.class)
                        .first()
                        .orElseThrow() instanceof com.landawn.abacus.util.ImmutableMap),
                // Unchanged: a mutable row type keeps the factory's map, and a type no factory can create still fails fast.
                () -> assertEquals(LinkedHashMap.class, MongoDBBase.readRow(doc, LinkedHashMap.class).getClass()),
                () -> assertThrows(IllegalArgumentException.class, () -> MongoDBBase.readRow(doc, com.landawn.abacus.util.ImmutableBiMap.class)));
    }

    @Test
    void extractDataReadsMapRowsIntoBsonValueRowTypesAsScalarColumns() {
        // With a BSON value row type, extractData read each Map row with readRow (the value of its sole field) and handed that value to
        // the Dataset, which read it as a bean of its own accessors: [type, data] columns for a Binary, a [code] column for a Code (HEAD:
        // NullPointerException for a Binary, an all-null column for a Code). Like a BSON value row itself, it now fills the single column.
        final Binary b1 = new Binary(new byte[] { 1 });
        final Binary b2 = new Binary(new byte[] { 2 });
        final Code code = new Code("f()");
        final List<Object> binaryDocs = Arrays.asList(new Document("_id", 1).append("b", b1), new Document("_id", 2).append("b", b2));
        final List<Object> codeDocs = Arrays.asList(new Document("_id", 1).append("c", code));

        assertAll(
                () -> assertEquals(Arrays.asList("b"),
                        new java.util.ArrayList<>(MongoDBBase.extractData(Arrays.asList("b"), binaryDocs, Binary.class).columnNames())),
                () -> assertEquals(Arrays.asList(b1, b2), MongoDBBase.extractData(Arrays.asList("b"), binaryDocs, Binary.class).getColumn("b")),
                () -> assertEquals(Arrays.asList(code), MongoDBBase.extractData(Arrays.asList("c"), codeDocs, Code.class).getColumn("c")),
                // A non-Document Map first row takes the bean-target branch, which reads its Map rows the same way.
                () -> assertEquals(Arrays.asList(b1),
                        MongoDBBase.extractData(Arrays.asList(new com.mongodb.BasicDBObject("b", b1)), Binary.class).getColumn("b")),
                // As for a BSON value row: without a single selected column (the first document also has _id) it is rejected.
                () -> assertThrows(IllegalArgumentException.class, () -> MongoDBBase.extractData(codeDocs, Code.class)),
                () -> assertThrows(IllegalArgumentException.class, () -> MongoDBBase.extractData(Arrays.asList("b", "_id"), binaryDocs, Binary.class)));
    }

    @SuppressWarnings("unchecked")
    @Test
    void toListNamesTheRowThatIsNotOfTheRequestedType() {
        // When the first row was already an instance of the row type (kept as is) and a later row was not, toList rejected the result
        // naming the FIRST row ("Cannot convert document: Code{code='f()'} to class: ...Code"), a row of the requested type.
        final Code code = new Code("f()");
        final MongoIterable<Object> codeThenDoc = mock(MongoIterable.class);
        MongoIterableStubs.stubRows(codeThenDoc, Arrays.asList(code, new Document("f", 1)));
        final Binary binary = new Binary(new byte[] { 1 });
        final MongoIterable<Object> codeThenBinary = mock(MongoIterable.class);
        MongoIterableStubs.stubRows(codeThenBinary, Arrays.asList(code, binary));
        final MongoIterable<Object> listThenDoc = mock(MongoIterable.class);
        MongoIterableStubs.stubRows(listThenDoc, Arrays.asList(new java.util.ArrayList<>(Arrays.asList(1)), new Document("g", 2)));

        assertAll(
                () -> assertTrue(assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toList(codeThenDoc, Code.class)).getMessage()
                        .startsWith("Cannot convert document: Document{{f=1}} to class: ")),
                () -> assertTrue(assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toList(codeThenBinary, Code.class)).getMessage()
                        .startsWith("Cannot convert document: " + binary + " to class: ")),
                () -> assertTrue(assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toList(listThenDoc, List.class)).getMessage()
                        .startsWith("Cannot convert document: Document{{g=2}} to class: ")));
    }

    // 2026-10-27 second-pass review (r2T): the branches and positions of the sliceT fixes the tests above did not reach.

    public static class WrapperPositions {
        private u.Nullable<String> nullOfNull;
        private u.OptionalBoolean optBool;
        private u.OptionalDouble optDouble;
        private java.util.OptionalInt jdkOptInt;
        private java.util.OptionalDouble jdkOptDouble;
        private u.Optional<String>[] array;
        private List<u.Optional<String>> list;
        private Map<String, u.Optional<String>> map;

        public u.Nullable<String> getNullOfNull() {
            return nullOfNull;
        }

        public void setNullOfNull(final u.Nullable<String> nullOfNull) {
            this.nullOfNull = nullOfNull;
        }

        public u.OptionalBoolean getOptBool() {
            return optBool;
        }

        public void setOptBool(final u.OptionalBoolean optBool) {
            this.optBool = optBool;
        }

        public u.OptionalDouble getOptDouble() {
            return optDouble;
        }

        public void setOptDouble(final u.OptionalDouble optDouble) {
            this.optDouble = optDouble;
        }

        public java.util.OptionalInt getJdkOptInt() {
            return jdkOptInt;
        }

        public void setJdkOptInt(final java.util.OptionalInt jdkOptInt) {
            this.jdkOptInt = jdkOptInt;
        }

        public java.util.OptionalDouble getJdkOptDouble() {
            return jdkOptDouble;
        }

        public void setJdkOptDouble(final java.util.OptionalDouble jdkOptDouble) {
            this.jdkOptDouble = jdkOptDouble;
        }

        public u.Optional<String>[] getArray() {
            return array;
        }

        public void setArray(final u.Optional<String>[] array) {
            this.array = array;
        }

        public List<u.Optional<String>> getList() {
            return list;
        }

        public void setList(final List<u.Optional<String>> list) {
            this.list = list;
        }

        public Map<String, u.Optional<String>> getMap() {
            return map;
        }

        public void setMap(final Map<String, u.Optional<String>> map) {
            this.map = map;
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    void emptyWrappersOfEveryKindAndPositionAreStoredAsBsonNull() {
        // The class doc's rule for every wrapper kind it names (a Nullable of null, the primitive optionals, java.util ones) and
        // every position: a property reads back as the empty wrapper, an element of an array, collection or map as null.
        final WrapperPositions entity = new WrapperPositions();
        entity.setNullOfNull(u.Nullable.of((String) null));
        entity.setOptBool(u.OptionalBoolean.empty());
        entity.setOptDouble(u.OptionalDouble.empty());
        entity.setJdkOptInt(java.util.OptionalInt.empty());
        entity.setJdkOptDouble(java.util.OptionalDouble.empty());
        entity.setArray(new u.Optional[] { u.Optional.empty(), u.Optional.of("a") });
        entity.setList(Arrays.asList(u.Optional.empty(), u.Optional.of("a")));
        final Map<String, u.Optional<String>> map = new LinkedHashMap<>();
        map.put("e", u.Optional.empty());
        map.put("p", u.Optional.of("a"));
        entity.setMap(map);
        final Map<String, u.Optional<String>> expectedMap = new LinkedHashMap<>();
        expectedMap.put("e", null);
        expectedMap.put("p", u.Optional.of("a"));
        final BsonArray storedElements = new BsonArray(Arrays.asList(BsonNull.VALUE, new BsonString("a")));

        final BsonDocument stored = encode(MongoDBBase.toDocument(entity));
        final WrapperPositions read = MongoDBBase.toEntity(decode(stored), WrapperPositions.class);

        assertAll(() -> assertEquals(BsonNull.VALUE, stored.get("nullOfNull")), () -> assertEquals(BsonNull.VALUE, stored.get("optBool")),
                () -> assertEquals(BsonNull.VALUE, stored.get("optDouble")), () -> assertEquals(BsonNull.VALUE, stored.get("jdkOptInt")),
                () -> assertEquals(BsonNull.VALUE, stored.get("jdkOptDouble")), () -> assertEquals(storedElements, stored.get("array")),
                () -> assertEquals(storedElements, stored.get("list")),
                () -> assertEquals(new BsonDocument("e", BsonNull.VALUE).append("p", new BsonString("a")), stored.get("map")),
                () -> assertFalse(read.getNullOfNull().isPresent()), () -> assertFalse(read.getOptBool().isPresent()),
                () -> assertFalse(read.getOptDouble().isPresent()), () -> assertFalse(read.getJdkOptInt().isPresent()),
                () -> assertFalse(read.getJdkOptDouble().isPresent()),
                () -> assertEquals(Arrays.asList(null, u.Optional.of("a")), Arrays.asList(read.getArray())),
                () -> assertEquals(Arrays.asList(null, u.Optional.of("a")), read.getList()), () -> assertEquals(expectedMap, read.getMap()));
    }

    public record ObjectRecord(Object value) {
    }

    public static class ObjectRecordHolder {
        private ObjectRecord rec;
        private ObjectRecord[] recs;

        public ObjectRecord getRec() {
            return rec;
        }

        public void setRec(final ObjectRecord rec) {
            this.rec = rec;
        }

        public ObjectRecord[] getRecs() {
            return recs;
        }

        public void setRecs(final ObjectRecord[] recs) {
            this.recs = recs;
        }
    }

    /** Stores {@code value} as the {@code Object} component of a record held by an entity, through {@code registry}. */
    private static BsonValue storedComponent(final Object value, final CodecRegistry registry) {
        final ObjectRecordHolder holder = new ObjectRecordHolder();
        holder.setRec(new ObjectRecord(value));
        final BsonDocument out = new BsonDocument();
        registry.get(Document.class).encode(new BsonDocumentWriter(out), MongoDBBase.toDocument(holder), EncoderContext.builder().build());
        return out.getDocument("rec").get("value");
    }

    @Test
    void aValueOfAnotherClassUsesTheCodecOfItsClassFromTheCallingRegistry() {
        // GeneralCodec's branch for a value of another class than its own (here the codec of an Object record component): a bean is a
        // document, an array and a ByteBuffer keep the forms GeneralCodec writes itself (those branches come first), a class without
        // a driver codec stays text, an empty wrapper is BSON null, a record in an array is encoded the same way, and the value's
        // codec comes from the registry that called the record codec: a UUID gets the collection's UUID representation, and a
        // registry without one rejects it as it would a Document field.
        final NameBean bean = new NameBean();
        bean.setName("n");
        final UUID uuid = new UUID(0, 1);
        final CodecRegistry plain = MongoDBBase.codecRegistry;
        final CodecRegistry standard = CodecRegistries.withUuidRepresentation(MongoDBBase.codecRegistry, UuidRepresentation.STANDARD);
        final ObjectRecordHolder arrayHolder = new ObjectRecordHolder();
        arrayHolder.setRecs(new ObjectRecord[] { new ObjectRecord(11) });
        final Map<String, Object> attrs = new LinkedHashMap<>();
        attrs.put("a", null);
        attrs.put("b", u.Optional.empty());
        final RecordHolder containers = new RecordHolder();
        containers.setRec(new ValueRecord("r", null, null, attrs, Arrays.asList(1, null, u.Optional.empty())));

        assertAll(() -> assertEquals(new BsonDocument("name", new BsonString("n")), storedComponent(bean, plain)),
                () -> assertEquals(new BsonArray(Arrays.asList(new BsonInt32(1), new BsonInt32(2))), storedComponent(new int[] { 1, 2 }, plain)),
                () -> assertEquals(new BsonBinary(new byte[] { 7 }), storedComponent(ByteBuffer.wrap(new byte[] { 7 }), plain)),
                () -> assertEquals(new BsonString("10"), storedComponent(BigInteger.TEN, plain)),
                () -> assertEquals(BsonNull.VALUE, storedComponent(u.Optional.empty(), plain)),
                () -> assertEquals(new BsonBinary(uuid, UuidRepresentation.STANDARD), storedComponent(uuid, standard)),
                () -> assertThrows(CodecConfigurationException.class, () -> storedComponent(uuid, plain)),
                () -> assertEquals(new BsonArray(Arrays.asList(new BsonDocument("value", new BsonInt32(11)))),
                        encode(MongoDBBase.toDocument(arrayHolder)).get("recs")),
                // Null and empty-wrapper values of Map<String, Object> / List<Object> components.
                () -> assertEquals(new BsonDocument("a", BsonNull.VALUE).append("b", BsonNull.VALUE),
                        encode(MongoDBBase.toDocument(containers)).getDocument("rec").get("attrs")),
                () -> assertEquals(new BsonArray(Arrays.asList(new BsonInt32(1), BsonNull.VALUE, BsonNull.VALUE)),
                        encode(MongoDBBase.toDocument(containers)).getDocument("rec").get("items")));
    }

    public static class PatternArrayHolder {
        private Pattern[] patterns;

        public Pattern[] getPatterns() {
            return patterns;
        }

        public void setPatterns(final Pattern[] patterns) {
            this.patterns = patterns;
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    void patternArraysAndPatternRowsReadBackFromTheRegularExpression() {
        // The two positions the Pattern test above leaves out: an array element (toDeclaredType's array branch) and toList rows.
        final PatternArrayHolder holder = new PatternArrayHolder();
        holder.setPatterns(new Pattern[] { Pattern.compile("x", Pattern.MULTILINE) });
        final MongoIterable<Document> rows = mock(MongoIterable.class);
        MongoIterableStubs.stubRows(rows, Arrays.asList(new Document("_id", 1).append("p", new BsonRegularExpression("a", "i")),
                new Document("_id", 2).append("p", new BsonRegularExpression("b", "m"))));

        final Pattern[] read = MongoDBBase.toEntity(decode(encode(MongoDBBase.toDocument(holder))), PatternArrayHolder.class).getPatterns();
        final List<Pattern> patterns = MongoDBBase.toList(rows, Pattern.class);

        assertAll(() -> assertEquals("x/" + Pattern.MULTILINE, describe(read[0])),
                () -> assertEquals("a/" + Pattern.CASE_INSENSITIVE, describe(patterns.get(0))),
                () -> assertEquals("b/" + Pattern.MULTILINE, describe(patterns.get(1))));
    }

    @SuppressWarnings("unchecked")
    @Test
    void bsonValueRowTypesKeepNullRowsAndCoverSubclassesAndBsonValue() {
        // Edge rows of the single-value rule for BSON value row types: a null row or an empty document gives null (an empty document was
        // Code{code=null}), a Code subclass is read from the field too, and null rows of a result keep their positions.
        final Code code = new Code("x");
        final CodeWithScope withScope = new CodeWithScope("f", new Document("a", 1));
        final MongoIterable<Object> withNulls = mock(MongoIterable.class);
        MongoIterableStubs.stubRows(withNulls, Arrays.asList(null, new Document("_id", 1).append("c", code), null));

        assertAll(() -> assertEquals(null, MongoDBBase.readRow(null, Code.class)), () -> assertEquals(null, MongoDBBase.readRow(new Document(), Code.class)),
                () -> assertSame(withScope, MongoDBBase.readRow(new Document("c", withScope), CodeWithScope.class)),
                () -> assertEquals(new BsonInt32(3), MongoDBBase.readRow(new Document("v", new BsonInt32(3)), BsonValue.class)),
                () -> assertEquals(Arrays.asList(null, code, null), MongoDBBase.toList(withNulls, Code.class)));
    }

    @Test
    void extractDataReadsABsonValueFieldOfAnObjectRowTypeAsAScalar() {
        // readDatasetRow also covers a single-value row type whose field holds a BSON value: for Object, readRow returns the Binary
        // itself, which the Dataset read as a bean of its own accessors (a null "b" column). A non-BSON value is unchanged.
        final Binary binary = new Binary(new byte[] { 1 });

        assertAll(
                () -> assertEquals(Arrays.asList(binary),
                        MongoDBBase.extractData(Arrays.asList("b"), Arrays.asList(new Document("_id", 1).append("b", binary)), Object.class).getColumn("b")),
                () -> assertEquals(Arrays.asList("s"),
                        MongoDBBase.extractData(Arrays.asList("b"), Arrays.asList(new Document("_id", 1).append("b", "s")), Object.class).getColumn("b")));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    void rowTypesTheStandInCannotBecomeFailWithIllegalArgumentException() {
        // r2X (second-pass cross-check): a row type the factories cannot create and N.convert cannot copy their stand-in into (EnumMap,
        // whose keys must be enums; AbstractQueue) fails with IllegalArgumentException from asRowType, through readRow and toList of
        // Document and plain-Map rows, where HEAD returned the HashMap / LinkedList stand-in typed as that class (as BigQuery/DynamoDB).
        final Document doc = new Document("_id", 1).append("b", "x");
        final MongoIterable<Document> docs = mock(MongoIterable.class);
        MongoIterableStubs.stubRows(docs, Arrays.asList(doc));
        final MongoIterable<Object> maps = mock(MongoIterable.class);
        MongoIterableStubs.stubRows(maps, Arrays.asList(new java.util.HashMap<>(Map.of("k", 1))));

        assertAll(() -> assertThrows(IllegalArgumentException.class, () -> MongoDBBase.readRow(doc, java.util.EnumMap.class)),
                () -> assertThrows(IllegalArgumentException.class, () -> MongoDBBase.readRow(doc, java.util.AbstractQueue.class)),
                () -> assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toList(docs, java.util.EnumMap.class)),
                () -> assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toList(maps, java.util.EnumMap.class)),
                // Pin: a mutable row type keeps the factory's own instance.
                () -> assertEquals(java.util.LinkedList.class, MongoDBBase.readRow(doc, java.util.LinkedList.class).getClass()));
    }
}
