package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWriter;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonNumber;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonArray;
import org.bson.RawBsonDocument;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import com.mongodb.client.MongoIterable;

/**
 * Review 2026-11-03 of {@link MongoDBBase}: entity properties, container elements and record components of {@link BsonValue}
 * classes, {@link RawBsonDocument} values, and collection/map row types that cannot be created without a capacity or that
 * reject a {@code null} field value.
 */
class MongoDBBaseReview20261103Test {

    public static class BsonValueHolder {
        private String id;
        private BsonInt64 count;
        private BsonString label;
        private BsonValue any;
        private BsonBoolean flag;
        private BsonDocument extra;
        private BsonArray items;
        private RawBsonDocument raw;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public BsonInt64 getCount() {
            return count;
        }

        public void setCount(final BsonInt64 count) {
            this.count = count;
        }

        public BsonString getLabel() {
            return label;
        }

        public void setLabel(final BsonString label) {
            this.label = label;
        }

        public BsonValue getAny() {
            return any;
        }

        public void setAny(final BsonValue any) {
            this.any = any;
        }

        public BsonBoolean getFlag() {
            return flag;
        }

        public void setFlag(final BsonBoolean flag) {
            this.flag = flag;
        }

        public BsonDocument getExtra() {
            return extra;
        }

        public void setExtra(final BsonDocument extra) {
            this.extra = extra;
        }

        public BsonArray getItems() {
            return items;
        }

        public void setItems(final BsonArray items) {
            this.items = items;
        }

        public RawBsonDocument getRaw() {
            return raw;
        }

        public void setRaw(final RawBsonDocument raw) {
            this.raw = raw;
        }
    }

    public static class Parent {
        private String name;
        private BsonValueHolder child;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public BsonValueHolder getChild() {
            return child;
        }

        public void setChild(final BsonValueHolder child) {
            this.child = child;
        }
    }

    public static class AbstractBsonContainers {
        private List<BsonValue> values;
        private Map<String, BsonNumber> numbers;

        public List<BsonValue> getValues() {
            return values;
        }

        public void setValues(final List<BsonValue> values) {
            this.values = values;
        }

        public Map<String, BsonNumber> getNumbers() {
            return numbers;
        }

        public void setNumbers(final Map<String, BsonNumber> numbers) {
            this.numbers = numbers;
        }
    }

    public record AbstractBsonRecord(BsonValue value, BsonNumber number) {
    }

    public static class BsonValueMapHolder {
        private Map<String, BsonValue> values;

        public Map<String, BsonValue> getValues() {
            return values;
        }

        public void setValues(final Map<String, BsonValue> values) {
            this.values = values;
        }
    }

    private static Document roundTrip(final Document doc) {
        final Codec<Document> codec = MongoDBBase.codecRegistry.get(Document.class);
        final BsonDocument bson = new BsonDocument();
        codec.encode(new BsonDocumentWriter(bson), doc, EncoderContext.builder().build());
        return codec.decode(new BsonDocumentReader(bson), DecoderContext.builder().build());
    }

    private static BsonValueHolder sampleHolder() {
        final BsonValueHolder holder = new BsonValueHolder();
        holder.setCount(new BsonInt64(5L));
        holder.setLabel(new BsonString("x"));
        holder.setAny(new BsonInt32(7));
        holder.setFlag(BsonBoolean.TRUE);
        holder.setExtra(new BsonDocument("a", new BsonInt32(1)));
        holder.setItems(new BsonArray(List.of(new BsonString("p"), new BsonInt32(2))));
        holder.setRaw(new RawBsonDocument(new BsonDocument("r", new BsonString("q")), MongoDBBase.codecRegistry.get(BsonDocument.class)));
        return holder;
    }

    // ---- BsonValue-typed entity properties ----

    @Test
    void bsonValueProperties_writtenThroughTheCodec_readBack() {
        // The driver stores such a property as the plain BSON value it holds, which a Document holds as a Java value.
        final Document stored = roundTrip(MongoDBBase.toDocument(sampleHolder()));
        assertEquals(5L, stored.get("count"));
        assertEquals("x", stored.get("label"));

        // Before the fix: ParsingException "Can't parse: 5" for every such document (the entity was written but never read).
        final BsonValueHolder back = MongoDBBase.toEntity(stored, BsonValueHolder.class);

        assertAll(() -> assertEquals(new BsonInt64(5L), back.getCount()), //
                () -> assertEquals(new BsonString("x"), back.getLabel()), //
                () -> assertEquals(new BsonInt32(7), back.getAny()), //
                () -> assertEquals(BsonBoolean.TRUE, back.getFlag()), //
                () -> assertEquals(new BsonDocument("a", new BsonInt32(1)), back.getExtra()), //
                () -> assertEquals(new BsonArray(List.of(new BsonString("p"), new BsonInt32(2))), back.getItems()), //
                () -> assertEquals(new BsonString("q"), back.getRaw().get("r")));
    }

    @Test
    void bsonValueEntity_decodedByTheTypedCollectionCodec() {
        // A collection typed BsonValueHolder decodes through GeneralCodec, which maps the decoded Document the same way.
        final Codec<BsonValueHolder> codec = MongoDBBase.codecRegistry.get(BsonValueHolder.class);
        final BsonDocument bson = new BsonDocument();
        codec.encode(new BsonDocumentWriter(bson), sampleHolder(), EncoderContext.builder().build());

        final BsonValueHolder back = codec.decode(new BsonDocumentReader(bson), DecoderContext.builder().build());

        assertAll(() -> assertEquals(new BsonInt64(5L), back.getCount()), //
                () -> assertEquals(new BsonDocument("a", new BsonInt32(1)), back.getExtra()));
    }

    @Test
    void bsonValueProperty_receivesAnotherNumericBsonType_asStoredByOtherWriters() {
        // MongoDB mixes int32 and int64 values of one field: an int32 is read into a BsonInt64 property as a BsonInt64.
        final BsonValueHolder holder = MongoDBBase.toEntity(new Document("count", 5).append("any", "s"), BsonValueHolder.class);

        assertAll(() -> assertEquals(new BsonInt64(5L), holder.getCount()), //
                () -> assertEquals(new BsonString("s"), holder.getAny()));

        // A value whose BSON form is of another type is rejected clearly, as for a row of that type.
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toEntity(new Document("label", 5), BsonValueHolder.class));
    }

    @Test
    void bsonValueProperty_ofAnEmbeddedBeanOrADottedPath_isConverted() {
        final Parent nested = MongoDBBase.toEntity(new Document("name", "p").append("child", new Document("count", 3L)), Parent.class);
        // A dotted key's value of any type reaches the nested property's conversion (before: only String values).
        final Parent dotted = MongoDBBase.toEntity(new Document("name", "p").append("child.count", 4L), Parent.class);

        assertAll(() -> assertEquals(new BsonInt64(3L), nested.getChild().getCount()), //
                () -> assertEquals(new BsonInt64(4L), dotted.getChild().getCount()), //
                () -> assertEquals("p", dotted.getName()));
    }

    @Test
    void bsonValueProperty_alreadyOfItsClass_isKept() {
        final BsonInt64 count = new BsonInt64(9L);
        final BsonDocument extra = new BsonDocument("z", new BsonInt32(0));
        final BsonValueHolder holder = MongoDBBase.toEntity(new Document("count", count).append("extra", extra), BsonValueHolder.class);

        assertAll(() -> assertSame(count, holder.getCount()), () -> assertSame(extra, holder.getExtra()));
    }

    @Test
    void beanLikeBsonValueProperty_fromAnEmbeddedDocument_keepsTheBeanMapping() {
        // Unchanged: BsonInt64 reports a bean type, and an embedded document of its property is read as that bean, as an element
        // of a List<BsonInt64> is.
        assertEquals(new BsonInt64(6L), MongoDBBase.toEntity(new Document("count", new Document("value", 6L)), BsonValueHolder.class).getCount());
    }

    // ---- abstract BsonValue / BsonNumber elements and record components ----

    @Test
    void abstractBsonValueElementsAndComponents_areEncoded() {
        final AbstractBsonContainers containers = MongoDBBase
                .toEntity(new Document("values", new ArrayList<>(List.of(5, "x"))).append("numbers", new Document("n", 5)), AbstractBsonContainers.class);
        // Before the fix: the elements stayed Integer/String (ClassCastException where they were read as BsonValue), and the
        // record failed with IllegalArgumentException "Conversion returned java.lang.String for org.bson.BsonValue".
        final Object first = containers.getValues().get(0);
        final Object number = containers.getNumbers().get("n");
        final AbstractBsonRecord rec = MongoDBBase.toEntity(new Document("value", 5).append("number", 6L), AbstractBsonRecord.class);

        assertAll(() -> assertEquals(new BsonInt32(5), first), //
                () -> assertEquals(new BsonString("x"), containers.getValues().get(1)), //
                () -> assertEquals(new BsonInt32(5), number), //
                () -> assertEquals(new BsonInt32(5), rec.value()), //
                () -> assertEquals(new BsonInt64(6L), rec.number()));
    }

    @Test
    void abstractBsonValueProperty_ofAnEmbeddedDocumentOrArray_isEncodedAsBsonDocumentOrBsonArray() {
        // Before the fix: IllegalArgumentException "Conversion returned java.lang.String for org.bson.BsonValue" for the document.
        final BsonValue fromDocument = MongoDBBase.toEntity(new Document("any", new Document("a", 1)), BsonValueHolder.class).getAny();
        final BsonValue fromList = MongoDBBase.toEntity(new Document("any", new ArrayList<>(List.of(1, "x"))), BsonValueHolder.class).getAny();
        final AbstractBsonRecord rec = MongoDBBase.toEntity(new Document("value", new Document("a", 1)).append("number", 2), AbstractBsonRecord.class);

        assertAll(() -> assertInstanceOf(BsonDocument.class, fromDocument), //
                () -> assertEquals(new BsonDocument("a", new BsonInt32(1)), fromDocument), //
                () -> assertInstanceOf(BsonArray.class, fromList), //
                () -> assertEquals(new BsonArray(List.of(new BsonInt32(1), new BsonString("x"))), fromList), //
                () -> assertInstanceOf(BsonDocument.class, rec.value()), //
                () -> assertEquals(new BsonDocument("a", new BsonInt32(1)), rec.value()));
    }

    @Test
    void abstractBsonValueElementsAndMapValues_ofAnEmbeddedDocumentOrArray_areEncoded() {
        final AbstractBsonContainers containers = MongoDBBase
                .toEntity(new Document("values", new ArrayList<>(List.of(new Document("a", 1), new ArrayList<>(List.of(1))))), AbstractBsonContainers.class);
        final BsonValueMapHolder map = MongoDBBase.toEntity(new Document("values", new Document("d", new Document("x", 1))), BsonValueMapHolder.class);
        // Read as Object: before the fix the List<BsonValue> held a Document and an ArrayList (heap pollution), and the
        // Map<String, BsonValue> value stayed a Document.
        final Object documentElement = containers.getValues().get(0);
        final Object arrayElement = containers.getValues().get(1);
        final Object mapValue = map.getValues().get("d");

        assertAll(() -> assertInstanceOf(BsonDocument.class, documentElement), //
                () -> assertEquals(new BsonDocument("a", new BsonInt32(1)), documentElement), //
                () -> assertInstanceOf(BsonArray.class, arrayElement), //
                () -> assertEquals(new BsonArray(List.of(new BsonInt32(1))), arrayElement), //
                () -> assertInstanceOf(BsonDocument.class, mapValue), //
                () -> assertEquals(new BsonDocument("x", new BsonInt32(1)), mapValue));
    }

    // ---- RawBsonDocument values ----

    @Test
    void convertBsonValue_embeddedDocumentAsRawBsonDocument_isEncoded() {
        // Before the fix: IllegalArgumentException "it is stored as a BSON DOCUMENT", while readRow already returned such rows.
        final RawBsonDocument raw = MongoDBBase.convertBsonValue(new Document("a", 1).append("s", "x"), RawBsonDocument.class);

        assertAll(() -> assertEquals(new BsonInt32(1), raw.get("a")), //
                () -> assertEquals(new BsonString("x"), raw.get("s")), //
                () -> assertThrows(IllegalArgumentException.class, () -> MongoDBBase.convertBsonValue(1, RawBsonDocument.class)));
    }

    // ---- collection and map row types ----

    @Test
    void readRow_arrayBlockingQueueRowType_isSizedToTheRow() {
        // Before the fix: IllegalArgumentException "Not able to create instance for collection: ArrayBlockingQueue".
        final ArrayBlockingQueue<?> row = MongoDBBase.readRow(new Document("_id", 1).append("a", 2), ArrayBlockingQueue.class);
        final ArrayBlockingQueue<?> empty = MongoDBBase.readRow(new Document(), ArrayBlockingQueue.class);

        assertAll(() -> assertEquals(List.of(1, 2), new ArrayList<>(row)), //
                () -> assertEquals(0, row.remainingCapacity()), //
                () -> assertTrue(empty.isEmpty()));
    }

    @Test
    void readRow_nullFieldIntoNullRejectingRowType_namesTheField() {
        final Document row = new Document("_id", 1).append("missing", null);

        // Before the fix: a bare NullPointerException (no message) for each of these row types.
        assertAll(() -> assertRejectedField(() -> MongoDBBase.readRow(row, ConcurrentLinkedQueue.class), "row collection"),
                () -> assertRejectedField(() -> MongoDBBase.readRow(row, ArrayDeque.class), "row collection"),
                () -> assertRejectedField(() -> MongoDBBase.readRow(row, ArrayBlockingQueue.class), "row collection"),
                () -> assertRejectedField(() -> MongoDBBase.readRow(row, ConcurrentHashMap.class), "row map"),
                () -> assertRejectedField(() -> MongoDBBase.readRow(row, Hashtable.class), "row map"));
    }

    @Test
    void readRow_nullFieldIntoOrdinaryRowTypes_isKept() {
        final Document row = new Document("_id", 1).append("missing", null);

        final Map<?, ?> map = MongoDBBase.readRow(row, LinkedHashMap.class);
        final List<?> list = MongoDBBase.readRow(row, ArrayList.class);

        assertAll(() -> assertEquals(new LinkedHashMap<>(row), map), () -> assertEquals(java.util.Arrays.asList(1, null), list));
    }

    @Test
    void toList_nullFieldIntoNullRejectingMapRowType_namesTheField() {
        final MongoIterable<?> iterable = mock(MongoIterable.class);
        MongoIterableStubs.stubRows(iterable, List.of(new Document("a", 1), new Document("a", 2).append("missing", null)));

        // Before the fix: a bare NullPointerException from ConcurrentHashMap.putAll.
        assertRejectedField(() -> MongoDBBase.toList(iterable, ConcurrentHashMap.class), "row map");
    }

    @Test
    void toList_incompatibleKeysOfASortedMapRowType_keepTheDocumentedClassCastException() {
        final Map<Object, Object> mixedKeys = new LinkedHashMap<>();
        mixedKeys.put("a", 1);
        mixedKeys.put(2, "b");
        final MongoIterable<?> iterable = mock(MongoIterable.class);
        MongoIterableStubs.stubRows(iterable, List.of(mixedKeys));

        assertThrows(ClassCastException.class, () -> MongoDBBase.toList(iterable, TreeMap.class));
    }

    // ---- follow-up: BsonArray row types (reported by s12), TreeSet rows (cross-cut from s22) ----

    @Test
    void readRow_bsonArrayRowTypes_encodeTheRowValues() {
        final ObjectId id = new ObjectId();
        final Document row = new Document("_id", id).append("a", "x").append("n", 2).append("missing", null);

        // Before the fix: a bare ClassCastException "Integer cannot be cast to BsonValue" from BsonArray.add (and a RawBsonArray
        // could not be created at all).
        final BsonArray array = MongoDBBase.readRow(row, BsonArray.class);
        final RawBsonArray raw = MongoDBBase.readRow(row, RawBsonArray.class);
        final List<BsonValue> expected = List.of(new BsonObjectId(id), new BsonString("x"), new BsonInt32(2), BsonNull.VALUE);

        assertAll(() -> assertEquals(new BsonArray(expected), array), //
                () -> assertEquals(expected, new ArrayList<>(raw)), //
                () -> assertThrows(IllegalArgumentException.class, () -> MongoDBBase.readRow(row, CustomBsonArray.class)));
    }

    public static class CustomBsonArray extends BsonArray {
        private static final long serialVersionUID = 1L;
    }

    @Test
    void readRow_valuesASortedRowTypeCannotCompare_nameTheField() {
        // Before the fix: a bare ClassCastException from TreeSet.add.
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> MongoDBBase.readRow(new Document("_id", 1).append("name", "x"), TreeSet.class));

        assertAll(() -> assertTrue(e.getMessage().startsWith("Field (name, x) is not accepted by the row collection java.util.TreeSet"), e.getMessage()),
                () -> assertInstanceOf(ClassCastException.class, e.getCause()));
    }

    // ---- follow-up: an id declared by a generic superclass (reported by s01) ----

    public static class GenericIdBase<K> {
        private K id;

        public K getId() {
            return id;
        }

        public void setId(final K id) {
            this.id = id;
        }
    }

    public static class StringIdEntity extends GenericIdBase<String> {
    }

    public static class ObjectIdEntity extends GenericIdBase<ObjectId> {
    }

    public static class LongIdEntity extends GenericIdBase<Long> {
    }

    @Test
    @SuppressWarnings("deprecation")
    void registerIdProperty_idOfAGenericSuperclass_usesTheBoundType() {
        final ObjectId oid = new ObjectId();
        // Before the fix: IllegalArgumentException "must be 'String' or 'ObjectId'" (the erased setter takes Object).
        MongoDBBase.registerIdProperty(StringIdEntity.class, "id");
        MongoDBBase.registerIdProperty(ObjectIdEntity.class, "id");

        final StringIdEntity entity = new StringIdEntity();
        entity.setId(oid.toHexString());
        final StringIdEntity stringBack = MongoDBBase.toEntity(new Document("_id", oid), StringIdEntity.class);
        final ObjectIdEntity objectIdBack = MongoDBBase.toEntity(new Document("_id", oid), ObjectIdEntity.class);
        // The erased Object parameter would also take the ObjectId itself; a String-bound id must receive its hex string.
        final Object stringId = stringBack.getId();

        assertAll(() -> assertEquals(new Document("_id", oid), MongoDBBase.toDocument(entity)), //
                () -> assertEquals(oid.toHexString(), stringId), //
                () -> assertEquals(oid, objectIdBack.getId()), //
                () -> assertThrows(IllegalArgumentException.class, () -> MongoDBBase.registerIdProperty(LongIdEntity.class, "id")));
    }

    // ---- verification v4: a stored _id of another type for a registered ObjectId-bound generic id; map types' own putAll ----

    public static class RegisteredObjectIdEntity extends GenericIdBase<ObjectId> {
    }

    public static class Holder {
        private RegisteredObjectIdEntity child;

        public RegisteredObjectIdEntity getChild() {
            return child;
        }

        public void setChild(final RegisteredObjectIdEntity child) {
            this.child = child;
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    void registerIdProperty_objectIdBoundGenericId_convertsAStoredIdOfAnotherType() {
        final ObjectId oid = new ObjectId();
        MongoDBBase.registerIdProperty(RegisteredObjectIdEntity.class, "id");

        // Before the fix: the erased setter (it takes Object) stored the hex String, or the Integer, itself in the ObjectId-bound id,
        // which failed with ClassCastException only where the id was read. Now converted as the setter of a non-generic ObjectId id
        // converts it: a hex String into its ObjectId, a value that is no ObjectId rejected.
        final Object fromHex = MongoDBBase.toEntity(new Document("_id", oid.toHexString()), RegisteredObjectIdEntity.class).getId();
        final Object embedded = MongoDBBase.toEntity(new Document("child", new Document("_id", oid.toHexString())), Holder.class).getChild().getId();

        assertAll(() -> assertEquals(oid, fromHex), //
                () -> assertEquals(oid, embedded), //
                () -> assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toEntity(new Document("_id", 5), RegisteredObjectIdEntity.class)));
    }

    @Test
    void toList_mapRowType_keepsTheMapTypesOwnCopy() {
        final Map<Object, Object> numericKey = new LinkedHashMap<>();
        numericKey.put(1, "a");
        final MongoIterable<?> iterable = mock(MongoIterable.class);
        MongoIterableStubs.stubRows(iterable, List.of(numericKey));

        // BasicDBObject.putAll stores each key as its String form, as HEAD copied the row. Before the fix (a put per field, to name a
        // rejected null), the Integer key was stored as it is in the BasicDBObject.
        final List<com.mongodb.BasicDBObject> rows = MongoDBBase.toList(iterable, com.mongodb.BasicDBObject.class);
        final Object key = rows.get(0).keySet().iterator().next();

        assertEquals("1", key);
    }

    // ---- follow-up: an embedded document or array requested as a bean or bean array (reported by s12) ----

    public static class Inner {
        private String id;
        private List<Long> nums;
        private LocalDate day;
        private byte[] data;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public List<Long> getNums() {
            return nums;
        }

        public void setNums(final List<Long> nums) {
            this.nums = nums;
        }

        public LocalDate getDay() {
            return day;
        }

        public void setDay(final LocalDate day) {
            this.day = day;
        }

        public byte[] getData() {
            return data;
        }

        public void setData(final byte[] data) {
            this.data = data;
        }
    }

    public record InnerRecord(long count, String name) {
    }

    private static final LocalDate DAY = LocalDate.of(2024, 3, 5);

    private static Document innerDocument(final ObjectId id, final int first) {
        // A BSON date at UTC midnight, as the driver's LocalDate codec writes DAY.
        final Date utcMidnight = new Date(DAY.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());
        return new Document("_id", id).append("nums", new ArrayList<>(List.of(first, first + 1))).append("day", utcMidnight);
    }

    @Test
    void convertBsonValue_embeddedDocumentAsBean_isReadAsAnEmbeddedBean() {
        final ObjectId id = new ObjectId();
        final Document embedded = innerDocument(id, 1).append("data", new Binary(new byte[] { 1, 2 }));

        // Before the fix: NumberFormatException "{"type": 0 is not a valid Byte" (BSON binary into byte[]); without the binary,
        // Integer elements in the List<Long> and the day read in the JVM's zone.
        final Inner inner = MongoDBBase.convertBsonValue(embedded, Inner.class);
        final Object firstNum = inner.getNums().get(0);
        // Before the fix: IllegalArgumentException (argument type mismatch: an Integer for the long component).
        final InnerRecord rec = MongoDBBase.convertBsonValue(new Document("count", 3).append("name", "r"), InnerRecord.class);

        assertAll(() -> assertEquals(id.toHexString(), inner.getId()), //
                () -> assertEquals(1L, firstNum), //
                () -> assertEquals(DAY, inner.getDay()), //
                () -> assertArrayEquals(new byte[] { 1, 2 }, inner.getData()), //
                () -> assertEquals(new InnerRecord(3L, "r"), rec));
    }

    @Test
    void convertBsonValue_arrayOfEmbeddedDocuments_convertsEachElement() {
        final Date utcMidnight = new Date(DAY.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());

        final Inner[] inners = MongoDBBase.convertBsonValue(List.of(innerDocument(new ObjectId(), 5)), Inner[].class);
        final Object firstNum = inners[0].getNums().get(0);
        final LocalDate[] days = MongoDBBase.convertBsonValue(List.of(utcMidnight), LocalDate[].class);

        // A projected array field read through toList into Inner[] rows (the sole non-_id field of each document).
        final MongoIterable<?> iterable = mock(MongoIterable.class);
        MongoIterableStubs.stubRows(iterable, List.of(new Document("_id", 1).append("inners", List.of(innerDocument(new ObjectId(), 7)))));
        final List<Inner[]> rows = MongoDBBase.toList(iterable, Inner[].class);
        final Object rowNum = rows.get(0)[0].getNums().get(0);

        // Before the fix: Integer elements and a zone-shifted day in each case.
        assertAll(() -> assertEquals(5L, firstNum), //
                () -> assertEquals(DAY, inners[0].getDay()), //
                () -> assertArrayEquals(new LocalDate[] { DAY }, days), //
                () -> assertEquals(7L, rowNum), //
                () -> assertEquals(0, MongoDBBase.convertBsonValue(List.of(), Inner[].class).length));
    }

    @Test
    void generalCodec_beanArray_decodesEachDocumentAsABean() {
        // The codec a record component (or distinct) of type Inner[] is decoded with.
        final Codec<Inner[]> codec = MongoDBBase.codecRegistry.get(Inner[].class);
        // An int32 element (as another writer stores it) and the BSON date the driver's LocalDate codec writes for DAY.
        final BsonDocument element = new BsonDocument("nums", new BsonArray(List.of(new BsonInt32(9)))).append("day",
                new BsonDateTime(DAY.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli()));
        final BsonDocument holder = new BsonDocument("v", new BsonArray(List.of(element)));

        final BsonDocumentReader reader = new BsonDocumentReader(holder);
        reader.readStartDocument();
        reader.readName();
        final Inner[] back = codec.decode(reader, DecoderContext.builder().build());
        final Object num = back[0].getNums().get(0);

        assertAll(() -> assertEquals(9L, num), () -> assertEquals(DAY, back[0].getDay()));
    }

    @Test
    void convertBsonValue_mapsNotRequestedAsBeans_keepTheirConversion() {
        final Document doc = new Document("a", 1);

        assertAll(() -> assertSame(doc, MongoDBBase.convertBsonValue(doc, Map.class)), //
                () -> assertEquals(new LinkedHashMap<>(doc), MongoDBBase.convertBsonValue(doc, LinkedHashMap.class)), //
                () -> assertSame(doc, MongoDBBase.convertBsonValue(doc, Object.class)), //
                () -> assertArrayEquals(new Object[] { doc }, MongoDBBase.convertBsonValue(List.of(doc), Object[].class)));
    }

    @Test
    void convertBsonValue_nullElementsOfAnArray_takeTheComponentTypesDefaultAsBefore() {
        final List<Object> values = new ArrayList<>(java.util.Arrays.asList(1, null));
        final List<Object> nullOnly = new ArrayList<>(java.util.Arrays.asList((Object) null));

        // N.convert is what read such arrays before the element-wise branch existed: a null element became the component
        // type's default (Optional.empty, Nullable.empty), not null.
        assertAll(
                () -> assertArrayEquals(com.landawn.abacus.util.N.convert(values, java.util.Optional[].class),
                        MongoDBBase.convertBsonValue(values, java.util.Optional[].class)),
                () -> assertArrayEquals(com.landawn.abacus.util.N.convert(values, com.landawn.abacus.util.u.Nullable[].class),
                        MongoDBBase.convertBsonValue(values, com.landawn.abacus.util.u.Nullable[].class)),
                () -> assertArrayEquals(com.landawn.abacus.util.N.convert(values, Integer[].class), MongoDBBase.convertBsonValue(values, Integer[].class)),
                () -> assertArrayEquals(new Inner[] { null }, MongoDBBase.convertBsonValue(nullOnly, Inner[].class)));
    }

    // ---- fixer round (slot m): RawBsonArray values ----

    public static class RawArrayHolder {
        private RawBsonArray items;
        private RawBsonDocument raw;

        public RawBsonArray getItems() {
            return items;
        }

        public void setItems(final RawBsonArray items) {
            this.items = items;
        }

        public RawBsonDocument getRaw() {
            return raw;
        }

        public void setRaw(final RawBsonDocument raw) {
            this.raw = raw;
        }
    }

    /**
     * Before the fix: IllegalArgumentException "Cannot convert java.util.ArrayList value [1, 2] to org.bson.RawBsonArray: it is
     * stored as a BSON ARRAY" for a RawBsonArray property, while a BsonArray property (and readRow's RawBsonArray rows) worked.
     */
    @Test
    void rawBsonArrayProperty_readsTheStoredArray() {
        final RawArrayHolder holder = MongoDBBase.toEntity(new Document("items", new ArrayList<>(List.of(1, "x"))), RawArrayHolder.class);
        // The value as an entity written with such a property stores it (the codec writes the plain BSON array).
        final RawArrayHolder written = new RawArrayHolder();
        written.setItems(MongoDBBase.convertBsonValue(List.of(3, 4), RawBsonArray.class));
        final RawArrayHolder back = MongoDBBase.toEntity(roundTrip(MongoDBBase.toDocument(written)), RawArrayHolder.class);

        assertAll(() -> assertInstanceOf(RawBsonArray.class, holder.getItems()), //
                () -> assertEquals(List.of(new BsonInt32(1), new BsonString("x")), new ArrayList<>(holder.getItems())), //
                () -> assertEquals(List.of(new BsonInt32(3), new BsonInt32(4)), new ArrayList<>(back.getItems())), //
                () -> assertTrue(MongoDBBase.toEntity(new Document("items", new ArrayList<>()), RawArrayHolder.class).getItems().isEmpty()));
    }

    @Test
    void convertBsonValue_rawBsonArray_fromListsArraysAndBsonValues() {
        final RawBsonArray fromArray = MongoDBBase.convertBsonValue(new Object[] { 1, "x" }, RawBsonArray.class);
        // Before the fix: IllegalArgumentException "Not able to create instance for collection: class org.bson.RawBsonArray" (and
        // "... for Map: class org.bson.RawBsonDocument") from N.convert for a BsonArray or BsonDocument value.
        final RawBsonArray fromBsonArray = MongoDBBase.convertBsonValue(new BsonArray(List.of(new BsonInt32(9))), RawBsonArray.class);
        final RawBsonDocument fromBsonDocument = MongoDBBase.convertBsonValue(new BsonDocument("a", new BsonInt32(1)), RawBsonDocument.class);
        final RawBsonArray same = fromArray;

        assertAll(() -> assertEquals(List.of(new BsonInt32(1), new BsonString("x")), new ArrayList<>(fromArray)), //
                () -> assertEquals(List.of(new BsonInt32(9)), new ArrayList<>(fromBsonArray)), //
                () -> assertEquals(new BsonInt32(1), fromBsonDocument.get("a")), //
                () -> assertSame(same, MongoDBBase.convertBsonValue(same, RawBsonArray.class)), //
                // A value whose BSON form is no array is still rejected, naming what it is stored as.
                () -> assertTrue(assertThrows(IllegalArgumentException.class, () -> MongoDBBase.convertBsonValue(1, RawBsonArray.class)).getMessage()
                        .contains("BSON INT32")), //
                () -> assertTrue(
                        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.convertBsonValue(new Document("a", 1), RawBsonArray.class)).getMessage()
                                .contains("BSON DOCUMENT")));
    }

    @Test
    void rawBsonValueProperties_fromBsonValuesHeldByTheDocument() {
        // A Document built by the application may hold BSON values themselves.
        final RawArrayHolder holder = MongoDBBase.toEntity(
                new Document("items", new BsonArray(List.of(new BsonString("p")))).append("raw", new BsonDocument("r", new BsonInt32(2))),
                RawArrayHolder.class);

        assertAll(() -> assertEquals(List.of(new BsonString("p")), new ArrayList<>(holder.getItems())), //
                () -> assertEquals(new BsonInt32(2), holder.getRaw().get("r")));
    }

    /** toList's single-value path (SCALAR_FROM_MAP) converts each row's sole non-_id field with convertBsonValue. */
    @Test
    void toList_rawBsonArrayRowType_readsTheSoleArrayField() {
        final MongoIterable<?> iterable = mock(MongoIterable.class);
        MongoIterableStubs.stubRows(iterable, List.of(new Document("_id", 1).append("arr", new ArrayList<>(List.of(1, 2)))));

        // Before the fix: IllegalArgumentException "it is stored as a BSON ARRAY".
        final List<RawBsonArray> rows = MongoDBBase.toList(iterable, RawBsonArray.class);

        assertEquals(List.of(new BsonInt32(1), new BsonInt32(2)), new ArrayList<>(rows.get(0)));
    }

    // ---- fixer round (slot m): an @Id / "id" property declared by a generic superclass, not registered ----

    public static class ConventionIdBase<K> {
        private K id;
        private String name;

        public K getId() {
            return id;
        }

        public void setId(final K id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    public static class ConventionStringIdEntity extends ConventionIdBase<String> {
    }

    public static class ConventionObjectIdEntity extends ConventionIdBase<ObjectId> {
    }

    public static class ConventionLongIdEntity extends ConventionIdBase<Long> {
    }

    public static class AnnotatedKeyBase<K> {
        @com.landawn.abacus.annotation.Id
        private K key;
        private String name;

        public K getKey() {
            return key;
        }

        public void setKey(final K key) {
            this.key = key;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    public static class AnnotatedStringKeyEntity extends AnnotatedKeyBase<String> {
    }

    public static class AnnotatedLongKeyEntity extends AnnotatedKeyBase<Long> {
    }

    private static <E extends ConventionIdBase<K>, K> E conventionEntity(final E entity, final K id) {
        entity.setId(id);
        entity.setName("n");
        return entity;
    }

    /**
     * Before the fix the erased {@code Object} parameter of the generic setter was tested, so the id was not mapped: it was written
     * as an ordinary {@code id} field (the server then generates a separate ObjectId {@code _id}), and {@code _id} was not read
     * back into it (the entity read back with a {@code null} id).
     */
    @Test
    void genericSuperclassId_stringOrObjectIdTypeArgument_isMappedToId() {
        final ObjectId oid = new ObjectId();
        final Document stored = new Document("_id", oid).append("name", "n");

        final Document stringDoc = MongoDBBase.toDocument(conventionEntity(new ConventionStringIdEntity(), oid.toHexString()));
        final Document objectIdDoc = MongoDBBase.toDocument(conventionEntity(new ConventionObjectIdEntity(), oid));
        final Object stringBack = MongoDBBase.toEntity(stored, ConventionStringIdEntity.class).getId();
        final Object objectIdBack = MongoDBBase.toEntity(stored, ConventionObjectIdEntity.class).getId();
        // The write and read of the whole round trip, through the codec, as an insert and find do.
        final ConventionStringIdEntity roundTripped = MongoDBBase.toEntity(roundTrip(stringDoc), ConventionStringIdEntity.class);
        // A document stored before the fix (its own "id" field next to the generated _id) keeps its stored id: see
        // genericSuperclassId_legacyDocument_keepsItsStoredId.
        final Object legacy = MongoDBBase.toEntity(new Document("_id", oid).append("id", "own").append("name", "n"), ConventionStringIdEntity.class).getId();

        assertAll(() -> assertEquals("own", legacy), //
                () -> assertEquals(new Document("name", "n").append("_id", oid), stringDoc), //
                () -> assertEquals(new Document("name", "n").append("_id", oid), objectIdDoc), //
                () -> assertEquals(oid.toHexString(), stringBack), // the hex string, not the ObjectId the erased setter would take
                () -> assertEquals(oid, objectIdBack), //
                () -> assertEquals(oid.toHexString(), roundTripped.getId()), //
                () -> assertEquals("n", roundTripped.getName()));
    }

    @Test
    void genericSuperclassId_annotatedWithId_isMappedToId() {
        final ObjectId oid = new ObjectId();
        final AnnotatedStringKeyEntity entity = new AnnotatedStringKeyEntity();
        entity.setKey(oid.toHexString());
        entity.setName("n");

        final Object key = MongoDBBase.toEntity(new Document("_id", oid).append("name", "n"), AnnotatedStringKeyEntity.class).getKey();

        assertAll(() -> assertEquals(new Document("name", "n").append("_id", oid), MongoDBBase.toDocument(entity)), //
                () -> assertEquals(oid.toHexString(), key));
    }

    /**
     * Earlier versions did not map a generic-superclass String/ObjectId id and stored it as an ordinary "id" field next to a
     * generated _id. Such a document read back with the generated _id instead of its stored id after the upgrade (identity
     * silently changed). Its own non-null id field now wins; a null one, or a document without one, still reads the _id, and a
     * non-generic id keeps the outer document's _id precedence.
     */
    @Test
    void genericSuperclassId_legacyDocument_keepsItsStoredId() {
        final ObjectId oid = new ObjectId();
        final ObjectId ownOid = new ObjectId();

        final ConventionStringIdEntity string = MongoDBBase.toEntity(new Document("_id", oid).append("id", "customer-42").append("name", "n"),
                ConventionStringIdEntity.class);
        final Object objectId = MongoDBBase.toEntity(new Document("_id", oid).append("id", ownOid), ConventionObjectIdEntity.class).getId();
        final Object bridge = MongoDBBase.toEntity(new Document("_id", oid).append("id", "customer-42"), BridgeStringIdEntity.class).getId();
        final Object bridgeObjectId = MongoDBBase.toEntity(new Document("_id", oid).append("id", ownOid), BridgeObjectIdEntity.class).getId();
        final Object annotated = MongoDBBase.toEntity(new Document("_id", oid).append("key", "k-1"), AnnotatedStringKeyEntity.class).getKey();
        final Object nullOwn = MongoDBBase.toEntity(new Document("_id", oid).append("id", null), ConventionStringIdEntity.class).getId();
        final Object nonGeneric = MongoDBBase.toEntity(new Document("_id", oid).append("id", "own"), PlainStringIdHolder.class).getId();
        final ConventionStringIdEntity roundTripped = MongoDBBase.toEntity(roundTrip(new Document("_id", oid).append("id", "customer-42").append("name", "n")),
                ConventionStringIdEntity.class);

        assertAll(() -> assertEquals("customer-42", string.getId()), () -> assertEquals("n", string.getName()), //
                () -> assertEquals(ownOid, objectId), //
                () -> assertEquals("customer-42", bridge), //
                () -> assertEquals(ownOid, bridgeObjectId), //
                () -> assertEquals("k-1", annotated), //
                () -> assertEquals(oid.toHexString(), nullOwn), //
                () -> assertEquals(oid.toHexString(), nonGeneric), // unchanged: a non-generic id was always written as _id
                () -> assertEquals("customer-42", roundTripped.getId()));
    }

    public static class PlainStringIdHolder {
        private String id;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }
    }

    /** A type argument other than String or ObjectId keeps the id an ordinary field, as for a non-generic Long id. */
    @Test
    void genericSuperclassId_otherTypeArgument_staysAnOrdinaryField() {
        final ObjectId oid = new ObjectId();
        final AnnotatedLongKeyEntity annotated = new AnnotatedLongKeyEntity();
        annotated.setKey(5L);

        final ConventionLongIdEntity fromId = MongoDBBase.toEntity(new Document("_id", oid).append("name", "n"), ConventionLongIdEntity.class);
        final Object ownField = MongoDBBase.toEntity(new Document("_id", oid).append("id", 7L), ConventionLongIdEntity.class).getId();

        assertAll(() -> assertEquals(new Document("id", 5L).append("name", "n"), MongoDBBase.toDocument(conventionEntity(new ConventionLongIdEntity(), 5L))),
                () -> assertEquals(new Document("key", 5L), MongoDBBase.toDocument(annotated)), //
                () -> assertEquals(null, fromId.getId()), // _id is not read into a Long id
                () -> assertEquals(7L, ownField));
    }

    // ---- verifier follow-up (slot m): a subclass overriding the generic id accessors (the setter found is a bridge) ----

    public static class BridgeStringIdEntity extends ConventionIdBase<String> {
        private String ownId;

        @Override
        public String getId() {
            return ownId;
        }

        @Override
        public void setId(final String id) {
            ownId = id;
        }
    }

    public static class BridgeObjectIdEntity extends ConventionIdBase<ObjectId> {
        private ObjectId ownId;

        @Override
        public ObjectId getId() {
            return ownId;
        }

        @Override
        public void setId(final ObjectId id) {
            ownId = id;
        }
    }

    public static class RegisteredBridgeStringIdEntity extends BridgeStringIdEntity {
    }

    public static class RegisteredBridgeObjectIdEntity extends BridgeObjectIdEntity {
    }

    /** A non-generic id typed Object: neither String nor ObjectId, so it stays an ordinary field (unchanged). */
    public static class PlainObjectIdHolder {
        private Object id;
        private String name;

        public Object getId() {
            return id;
        }

        public void setId(final Object id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    /**
     * Before the fix the setter found for "id" was the compiler's bridge setId(Object), whose generic parameter type is the class
     * Object (not the type variable), so the id was not mapped: written as an ordinary "id" field, and _id not read back (null).
     */
    @Test
    void genericSuperclassId_overriddenAccessors_isMappedToId() {
        final ObjectId oid = new ObjectId();
        final Document stored = new Document("_id", oid).append("name", "n");

        final Document stringDoc = MongoDBBase.toDocument(conventionEntity(new BridgeStringIdEntity(), oid.toHexString()));
        final Document objectIdDoc = MongoDBBase.toDocument(conventionEntity(new BridgeObjectIdEntity(), oid));
        final Object stringBack = MongoDBBase.toEntity(stored, BridgeStringIdEntity.class).getId();
        final Object objectIdBack = MongoDBBase.toEntity(stored, BridgeObjectIdEntity.class).getId();
        final Object roundTripped = MongoDBBase.toEntity(roundTrip(stringDoc), BridgeStringIdEntity.class).getId();

        assertAll(() -> assertEquals(new Document("name", "n").append("_id", oid), stringDoc), //
                () -> assertEquals(new Document("name", "n").append("_id", oid), objectIdDoc), //
                () -> assertEquals(oid.toHexString(), stringBack), //
                () -> assertEquals(oid, objectIdBack), //
                () -> assertEquals(oid.toHexString(), roundTripped), //
                // A hex String _id into the ObjectId-bound id is converted (the bridge would throw ClassCastException on it).
                () -> assertEquals(oid, MongoDBBase.toEntity(new Document("_id", oid.toHexString()), BridgeObjectIdEntity.class).getId()));
    }

    /** Before the fix: IllegalArgumentException "...must be 'String' or 'ObjectId': ...setId(java.lang.Object)". */
    @Test
    @SuppressWarnings("deprecation")
    void registerIdProperty_overriddenGenericAccessors_isAccepted() {
        final ObjectId oid = new ObjectId();
        MongoDBBase.registerIdProperty(RegisteredBridgeStringIdEntity.class, "id");
        MongoDBBase.registerIdProperty(RegisteredBridgeObjectIdEntity.class, "id");

        final RegisteredBridgeStringIdEntity entity = conventionEntity(new RegisteredBridgeStringIdEntity(), oid.toHexString());
        final Object stringBack = MongoDBBase.toEntity(new Document("_id", oid), RegisteredBridgeStringIdEntity.class).getId();
        final Object objectIdBack = MongoDBBase.toEntity(new Document("_id", oid), RegisteredBridgeObjectIdEntity.class).getId();

        assertAll(() -> assertEquals(new Document("name", "n").append("_id", oid), MongoDBBase.toDocument(entity)), //
                () -> assertEquals(oid.toHexString(), stringBack), //
                () -> assertEquals(oid, objectIdBack));
    }

    @Test
    @SuppressWarnings("deprecation")
    void plainObjectTypedId_staysAnOrdinaryField() {
        final ObjectId oid = new ObjectId();
        final PlainObjectIdHolder holder = new PlainObjectIdHolder();
        holder.setId(5);
        holder.setName("n");

        assertAll(() -> assertEquals(new Document("id", 5).append("name", "n"), MongoDBBase.toDocument(holder)), //
                () -> assertEquals(null, MongoDBBase.toEntity(new Document("_id", oid).append("name", "n"), PlainObjectIdHolder.class).getId()), //
                () -> assertThrows(IllegalArgumentException.class, () -> MongoDBBase.registerIdProperty(PlainObjectIdHolder.class, "id")));
    }

    // ---- fixer round (slot m): a dotted key through a collection property ----

    public static class Address {
        private String city;
        private Long zip;

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
    }

    public static class Book {
        private List<Address> addresses;

        public List<Address> getAddresses() {
            return addresses;
        }

        public void setAddresses(final List<Address> addresses) {
            this.addresses = addresses;
        }
    }

    /** An immutable bean built through its builder, which has no getters. */
    public static final class BuiltBook {
        private final String name;
        private final List<Address> addresses;

        BuiltBook(final String name, final List<Address> addresses) {
            this.name = name;
            this.addresses = addresses;
        }

        public String getName() {
            return name;
        }

        public List<Address> getAddresses() {
            return addresses;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private String name;
            private List<Address> addresses;

            public Builder name(final String name) {
                this.name = name;
                return this;
            }

            public Builder addresses(final List<Address> addresses) {
                this.addresses = addresses;
                return this;
            }

            public BuiltBook build() {
                return new BuiltBook(name, addresses);
            }
        }
    }

    /**
     * Pins the toEntity Javadoc (the same rule as DynamoDBExecutorBase's): a dotted path through a collection property is left
     * whole to the dependency's path setter, which writes into the first element of the collection, or into a new bean it adds
     * when the collection is missing or empty; in a builder-based target it fails with ClassCastException.
     */
    @Test
    void dottedKeyThroughACollectionProperty_writesIntoTheFirstElement() {
        final Document existing = new Document("addresses", new ArrayList<>(List.of(new Document("city", "a"), new Document("city", "b"))))
                .append("addresses.zip", 5L);

        final Book first = MongoDBBase.toEntity(existing, Book.class);
        final Book missing = MongoDBBase.toEntity(new Document("addresses.city", "Paris"), Book.class);
        final Book empty = MongoDBBase.toEntity(new Document("addresses", new ArrayList<>()).append("addresses.city", "Rome"), Book.class);

        assertAll(() -> assertEquals(2, first.getAddresses().size()), //
                () -> assertEquals(5L, first.getAddresses().get(0).getZip()), //
                () -> assertEquals(null, first.getAddresses().get(1).getZip()), //
                () -> assertEquals("b", first.getAddresses().get(1).getCity()), //
                () -> assertEquals(1, missing.getAddresses().size()), //
                () -> assertEquals("Paris", missing.getAddresses().get(0).getCity()), //
                () -> assertEquals(1, empty.getAddresses().size()), //
                () -> assertEquals("Rome", empty.getAddresses().get(0).getCity()));
    }

    @Test
    void dottedKeyThroughACollectionProperty_ofABuilderTarget_failsWithClassCastException() {
        assertThrows(ClassCastException.class, () -> MongoDBBase.toEntity(new Document("name", "n").append("addresses.city", "Paris"), BuiltBook.class));
        // A plain key of the collection property is read as usual.
        assertEquals("Paris",
                MongoDBBase.toEntity(new Document("name", "n").append("addresses", List.of(new Document("city", "Paris"))), BuiltBook.class)
                        .getAddresses()
                        .get(0)
                        .getCity());
    }

    // ---- fixer round (slot m): numeric narrowing into BsonValue-typed properties ----

    public static class NumericHolder {
        private BsonInt64 bigBson;
        private BsonInt32 smallBson;
        private Long big;
        private Integer small;

        public BsonInt64 getBigBson() {
            return bigBson;
        }

        public void setBigBson(final BsonInt64 bigBson) {
            this.bigBson = bigBson;
        }

        public BsonInt32 getSmallBson() {
            return smallBson;
        }

        public void setSmallBson(final BsonInt32 smallBson) {
            this.smallBson = smallBson;
        }

        public Long getBig() {
            return big;
        }

        public void setBig(final Long big) {
            this.big = big;
        }

        public Integer getSmall() {
            return small;
        }

        public void setSmall(final Integer small) {
            this.small = small;
        }
    }

    /**
     * Pins the rule the convertBsonValue Javadoc now states: a number read into a numeric BsonValue property follows the rule of
     * the corresponding Java property. A fraction is dropped (5.5 into a Long property is 5, so into a BsonInt64 too), and a value
     * out of range fails with ArithmeticException (as for an Integer property), never wrapping around.
     */
    @Test
    void numericBsonValueProperty_narrowsAsTheJavaPropertyDoes() {
        final Decimal128 decimal = new Decimal128(new java.math.BigDecimal("5.5"));

        final NumericHolder fromDouble = MongoDBBase.toEntity(new Document("bigBson", 5.5).append("big", 5.5), NumericHolder.class);
        final NumericHolder fromDecimal = MongoDBBase.toEntity(new Document("bigBson", decimal).append("big", decimal), NumericHolder.class);
        final NumericHolder inRange = MongoDBBase.toEntity(new Document("smallBson", 7L).append("small", 7L), NumericHolder.class);

        assertAll(() -> assertEquals(new BsonInt64(fromDouble.getBig()), fromDouble.getBigBson()), //
                () -> assertEquals(new BsonInt64(5L), fromDouble.getBigBson()), //
                () -> assertEquals(new BsonInt64(fromDecimal.getBig()), fromDecimal.getBigBson()), //
                () -> assertEquals(new BsonInt32(7), inRange.getSmallBson()), //
                () -> assertEquals(7, inRange.getSmall()), //
                () -> assertThrows(ArithmeticException.class, () -> MongoDBBase.toEntity(new Document("small", Long.MAX_VALUE), NumericHolder.class)), //
                () -> assertThrows(ArithmeticException.class, () -> MongoDBBase.toEntity(new Document("smallBson", Long.MAX_VALUE), NumericHolder.class)), //
                () -> assertThrows(ArithmeticException.class, () -> MongoDBBase.toEntity(new Document("bigBson", 1e300), NumericHolder.class)), //
                () -> assertThrows(ArithmeticException.class, () -> MongoDBBase.convertBsonValue(new BsonInt64(Long.MIN_VALUE), BsonInt32.class)));
    }

    private static void assertRejectedField(final org.junit.jupiter.api.function.Executable call, final String container) {
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, call);

        assertTrue(e.getMessage().startsWith("Field (missing, null) is not accepted by the " + container), e.getMessage());
        assertInstanceOf(NullPointerException.class, e.getCause());
    }
}
