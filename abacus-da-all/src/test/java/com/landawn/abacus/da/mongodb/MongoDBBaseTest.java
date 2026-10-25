package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.bson.BasicBSONObject;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import com.landawn.abacus.da.MockitoTestBase;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.IntFunctions;
import com.landawn.abacus.util.stream.Stream;
import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;

/**
 * Additional tests for MongoDBBase targeting low-coverage branches/methods
 * such as toList variants, readRow, resetObjectId edge cases, type-conversion
 * paths in extractData, and ObjectId reset for Date/byte[]/String inputs.
 */
public class MongoDBBaseTest extends MockitoTestBase {

    @Mock
    private FindIterable<Document> mockFindIterable;

    @Mock
    private MongoCursor<Document> mockCursor;

    @BeforeEach
    public void setUp() {
    }

    // Metadata and validation

    @Test
    public void testRegisterIdPropertyRejectsNullArguments() {
        // Both arguments feed this library's bean reflection, so null is an illegal argument
        // rather than an NPE raised from inside the property-method cache.
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.registerIdProperty(null, "id"));
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.registerIdProperty(ObjectIdEntity.class, null));
    }

    // -- registerIdProperty branches --

    @Test
    public void testRegisterIdPropertyWithObjectIdPropertyType() {
        // Should accept an ObjectId-typed setter
        MongoDBBase.registerIdProperty(ObjectIdEntity.class, "id");
    }

    @Test
    public void testRegisterIdPropertyWithUnsupportedTypeThrows() {
        // Wrong setter type -- registerIdProperty should reject it
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.registerIdProperty(IntIdEntity.class, "id"));
    }

    // Row and entity mapping

    @Test
    public void testBinaryBeanPropertiesDecodeWithoutChangingDocuments() {
        final byte[] expected = { 4, 5, 6 };
        final Binary binary = new Binary(expected);
        final Document child = new Document("buffer", binary).append("bytes", binary);
        final Document row = new Document("buffer", expected).append("bytes", binary).append("child", child);

        final BinaryEntity entity = MongoDBBase.toEntity(row, BinaryEntity.class);

        assertEquals(ByteBuffer.wrap(expected), entity.getBuffer());
        assertArrayEquals(expected, entity.getBytes());
        assertEquals(ByteBuffer.wrap(expected), entity.getChild().getBuffer());
        assertArrayEquals(expected, entity.getChild().getBytes());
        assertSame(expected, row.get("buffer"));
        assertSame(binary, row.get("bytes"));
        assertSame(child, row.get("child"));
        assertSame(binary, child.get("buffer"));

        final BinaryEntity dotted = MongoDBBase.toEntity(new Document("child.buffer", binary), BinaryEntity.class);
        assertEquals(ByteBuffer.wrap(expected), dotted.getChild().getBuffer());
    }

    // -- Entities used by tests --

    @Test
    public void testBinaryRowsDecodeToReadableBuffersAndArrays() {
        final byte[] expected = { 1, 2, 3 };
        final Document decoded = new org.bson.codecs.DocumentCodec().decode(
                new org.bson.BsonDocumentReader(new org.bson.BsonDocument("value", new org.bson.BsonBinary(expected))),
                org.bson.codecs.DecoderContext.builder().build());

        assertTrue(decoded.get("value") instanceof Binary);

        for (final Document row : Arrays.asList(new Document("value", expected), decoded)) {
            assertEquals(ByteBuffer.wrap(expected), MongoDBBase.readRow(row, ByteBuffer.class));
            assertArrayEquals(expected, MongoDBBase.readRow(row, byte[].class));
        }
    }

    @Test
    public void testExtractDataFromNullListYieldsEmptyDataset() {
        // A null row list is treated exactly like an empty one (N.firstNonNull tolerates null).
        final Dataset fromNullList = MongoDBBase.extractData((List<?>) null);
        assertNotNull(fromNullList);
        assertEquals(0, fromNullList.size());

        final Dataset fromNullListTyped = MongoDBBase.extractData((List<?>) null, NoIdEntity.class);
        assertNotNull(fromNullListTyped);
        assertEquals(0, fromNullListTyped.size());

        final Dataset fromNullListWithProps = MongoDBBase.extractData(Arrays.asList("value"), (List<?>) null, NoIdEntity.class);
        assertNotNull(fromNullListWithProps);
        assertEquals(0, fromNullListWithProps.size());
    }

    @Test
    public void testExtractDataFromBeanListWithoutSelectPropNames() {
        // Regression: the else branch passed a null selectPropNames straight to
        // N.newDataset(columnNames, rows), which rejects it with IAE — contradicting the
        // documented "null to include all". It must fall back to the column-deriving overload.
        List<NoIdEntity> beans = new ArrayList<>();
        NoIdEntity e1 = new NoIdEntity();
        e1.setValue("x");
        beans.add(e1);
        NoIdEntity e2 = new NoIdEntity();
        e2.setValue("y");
        beans.add(e2);

        Dataset result = MongoDBBase.extractData(beans);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.containsColumn("value"));
    }

    @Test
    public void testExtractDataFromIterableRejectsNullRowTypeBeforeReading() {
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.extractData(mockFindIterable, null));
    }

    @Test
    public void testExtractDataFromListRejectsNullRowType() {
        // Consistent with the MongoIterable-based overloads, which reject a null rowType via
        // checkResultClass instead of dereferencing it.
        final List<Document> docs = Arrays.asList(new Document("value", "a"));

        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.extractData(docs, (Class<?>) null));
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.extractData(null, docs, (Class<?>) null));
    }

    @Test
    public void testExtractDataFromListNonMapAndNonDocument() {
        // Plain bean list with explicit selectPropNames goes through the else branch
        List<NoIdEntity> beans = new ArrayList<>();
        NoIdEntity e1 = new NoIdEntity();
        e1.setValue("x");
        beans.add(e1);

        Dataset result = MongoDBBase.extractData(Arrays.asList("value"), beans, NoIdEntity.class);

        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    public void testExtractDataFromBasicDBObjectListConvertsToBean() {
        // A Map row that is not a Document (BasicDBObject) must not bypass the requested bean conversion.
        // The extra key is only visible through the old raw-map path; the bean path must derive just "value".
        List<BasicDBObject> rows = Arrays.asList(new BasicDBObject("value", "a").append("extra", "x"), new BasicDBObject("value", "b").append("extra", "y"));

        Dataset result = MongoDBBase.extractData(rows, NoIdEntity.class);

        assertEquals(Arrays.asList("value"), result.columnNames());
        assertEquals(Arrays.asList("a", "b"), result.getColumn("value"));
    }

    @Test
    public void testExtractDataBeanTargetWithSelectedPropertiesAndMapRows() {
        List<BasicDBObject> rows = Arrays.asList(new BasicDBObject("value", "a").append("extra", "x"), new BasicDBObject("value", "b").append("extra", "y"));

        Dataset result = MongoDBBase.extractData(Arrays.asList("value"), rows, NoIdEntity.class);
        List<NoIdEntity> converted = result.toList(NoIdEntity.class);

        assertEquals(Arrays.asList("value"), result.columnNames());
        assertEquals(2, converted.size());
        assertEquals("a", converted.get(0).getValue());
        assertEquals("b", converted.get(1).getValue());
    }

    @Test
    public void testExtractDataBeanTargetPreservesNullAndAlreadyTypedRows() {
        NoIdEntity bean = new NoIdEntity();
        bean.setValue("a");

        Dataset result = MongoDBBase.extractData(Arrays.asList(bean, new BasicDBObject("value", "b"), null), NoIdEntity.class);
        List<NoIdEntity> converted = result.toList(NoIdEntity.class);

        assertEquals(3, converted.size());
        assertEquals("a", converted.get(0).getValue());
        assertEquals("b", converted.get(1).getValue());
        assertNull(converted.get(2).getValue());
    }

    @Test
    public void testExtractDataBeanTargetCopiesAnotherBeanShape() {
        OtherNoIdEntity other = new OtherNoIdEntity();
        other.setValue("c");

        Dataset result = MongoDBBase.extractData(Arrays.asList(other), NoIdEntity.class);
        List<NoIdEntity> converted = result.toList(NoIdEntity.class);

        assertEquals(1, converted.size());
        assertEquals("c", converted.get(0).getValue());
    }

    @Test
    public void testExtractDataBeanTargetMapsObjectIdToIdProperty() {
        // _id -> id is MongoDB-specific mapping performed by readRow, not by Dataset's later bean conversion.
        ObjectId oid = new ObjectId("507f1f77bcf86cd799439011");
        BasicDBObject row = new BasicDBObject("_id", oid).append("name", "alice");

        Dataset result = MongoDBBase.extractData(Arrays.asList(row), TestEntity.class);

        assertEquals(Arrays.asList("id", "name"), result.columnNames());
        assertEquals(Collections.singletonList(oid.toHexString()), result.getColumn("id"));
        assertEquals(Collections.singletonList("alice"), result.getColumn("name"));
    }

    @Test
    public void testExtractDataBeanTargetConvertsPropertyTypesBeforeDatasetCreation() {
        final ObjectId oid = new ObjectId("507f1f77bcf86cd799439011");
        final ObjectIdEntity bean = new ObjectIdEntity();
        bean.setId(oid);
        bean.setName("alice");
        final TestEntity typedBean = new TestEntity();
        typedBean.setId(oid.toHexString());
        typedBean.setName("alice");
        final BasicDBObject map = new BasicDBObject("_id", oid).append("name", "alice");
        final Document document = new Document("_id", oid).append("name", "alice");

        for (final List<?> rows : Arrays.<List<?>> asList(Collections.singletonList(bean), Arrays.asList(map, bean), Arrays.asList(bean, map),
                Arrays.asList(document, bean), Arrays.asList(typedBean, bean))) {
            for (final List<String> selectedProperties : Arrays.<List<String>> asList(null, Collections.emptyList(), Arrays.asList("id", "name"))) {
                final Dataset result = MongoDBBase.extractData(selectedProperties, rows, TestEntity.class);
                final String context = "First row: " + rows.get(0).getClass().getSimpleName() + ", selected properties: " + selectedProperties;

                // Inspect the Dataset itself: toList(TestEntity.class) would convert a retained ObjectId later,
                // hiding a missing bean-to-bean conversion during extractData.
                assertEquals(Arrays.asList("id", "name"), result.columnNames(), context);
                assertEquals(Collections.nCopies(rows.size(), oid.toHexString()), result.getColumn("id"), context);
                assertEquals(Collections.nCopies(rows.size(), "alice"), result.getColumn("name"), context);
            }
        }

        assertSame(oid, bean.getId());
        assertSame(oid, map.get("_id"));
        assertSame(oid, document.get("_id"));
    }

    @Test
    public void testExtractDataDocumentFirstKeepsLaterObjectArrayRow() {
        List<Object> rows = Arrays.asList(new Document("value", "a"), new Object[] { "b" });

        Dataset result = MongoDBBase.extractData(rows, NoIdEntity.class);
        List<NoIdEntity> converted = result.toList(NoIdEntity.class);

        assertEquals(Arrays.asList("value"), result.columnNames());
        assertEquals(2, converted.size());
        assertEquals("a", converted.get(0).getValue());
        assertEquals("b", converted.get(1).getValue());
    }

    @Test
    public void testExtractDataDocumentFirstKeepsLaterListRow() {
        List<Object> rows = Arrays.asList(new Document("value", "a"), Arrays.asList("b"));

        Dataset result = MongoDBBase.extractData(rows, NoIdEntity.class);
        List<NoIdEntity> converted = result.toList(NoIdEntity.class);

        assertEquals(Arrays.asList("value"), result.columnNames());
        assertEquals(2, converted.size());
        assertEquals("a", converted.get(0).getValue());
        assertEquals("b", converted.get(1).getValue());
    }

    @Test
    public void testExtractDataDocumentFirstCopiesLaterBeanRow() {
        OtherNoIdEntity other = new OtherNoIdEntity();
        other.setValue("c");
        List<Object> rows = Arrays.asList(new Document("value", "a"), other);

        Dataset result = MongoDBBase.extractData(rows, NoIdEntity.class);
        List<NoIdEntity> converted = result.toList(NoIdEntity.class);

        assertEquals(2, converted.size());
        assertEquals("a", converted.get(0).getValue());
        assertEquals("c", converted.get(1).getValue());
    }

    @Test
    public void testExtractDataFromListWithDocumentsAndEntityType() {
        // Document rows being converted to an entity row type
        List<Document> docs = Arrays.asList(new Document("value", "a"), new Document("value", "b"));

        Dataset result = MongoDBBase.extractData(docs, NoIdEntity.class);
        assertNotNull(result);
        assertEquals(2, result.size());
    }

    @Test
    public void testExtractDataWithEmptyList() {
        Dataset result = MongoDBBase.extractData(Collections.emptyList());
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    // -- extractData edge cases --

    @Test
    public void testExtractDataWithSelectPropNamesFromList() {
        // When selectPropNames is provided and rows are Maps
        List<Document> rows = Arrays.asList(new Document("a", 1).append("b", 2), new Document("a", 3).append("b", 4));

        Dataset result = MongoDBBase.extractData(Arrays.asList("a"), rows, Map.class);

        assertNotNull(result);
        assertEquals(2, result.size());
    }

    @Test
    public void testExtractDataWithUnsupportedRowTypeThrows() {
        // checkResultClass should reject non-bean, non-Map types
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.extractData(mockFindIterable, String.class));
    }

    @Test
    public void testGeneralCodecDecodesNonStringScalarValues() {
        // Regression: the registry resolves Object/Number (no driver codec) to GeneralCodec, whose decode read every
        // non-bean value with readString, so the driver's distinct(field, Object.class) threw
        // BsonInvalidOperationException ("readString ... not when CurrentBSONType is INT32") for any non-string value.
        final org.bson.BsonArray values = new org.bson.BsonArray(Arrays.asList(new org.bson.BsonInt32(1), new org.bson.BsonString("a"),
                new org.bson.BsonInt64(2L), new org.bson.BsonDocument("x", new org.bson.BsonInt32(3)), org.bson.BsonNull.VALUE,
                new org.bson.BsonArray(Arrays.asList(new org.bson.BsonInt32(4), new org.bson.BsonInt32(5)))));

        final List<Object> objects = decodeArrayWith(values, Object.class);
        assertEquals(Arrays.asList(1, "a", 2L, new Document("x", 3), null, Arrays.asList(4, 5)), objects);
        assertEquals(Document.class, objects.get(3).getClass());

        final List<Number> numbers = decodeArrayWith(new org.bson.BsonArray(Arrays.asList(new org.bson.BsonInt32(7), new org.bson.BsonDouble(1.5))),
                Number.class);
        assertEquals(Arrays.asList(7, 1.5), numbers);

        // A BSON string still goes through the string-parsing path.
        assertEquals(Arrays.asList("s"), decodeArrayWith(new org.bson.BsonArray(Arrays.asList(new org.bson.BsonString("s"))), Object.class));
    }

    @Test
    public void testGeneralCodecEncodeNullValueIsIllegalArgument() {
        final org.bson.BsonDocumentWriter writer = new org.bson.BsonDocumentWriter(new org.bson.BsonDocument());
        final org.bson.codecs.EncoderContext ctx = org.bson.codecs.EncoderContext.builder().build();

        assertThrows(IllegalArgumentException.class, () -> new MongoDBBase.GeneralCodec<>(TestEntity.class).encode(writer, null, ctx));
    }

    @Test
    public void testGeneralCodecEncodeDecodeEntity() {
        // Round-trip an entity through the codec via a BSON document
        MongoDBBase.GeneralCodec<TestEntity> codec = new MongoDBBase.GeneralCodec<>(TestEntity.class);

        TestEntity in = new TestEntity();
        in.setName("alice");

        // Encode into a BsonDocument
        org.bson.BsonDocument bsonDoc = new org.bson.BsonDocument();
        org.bson.BsonDocumentWriter writer = new org.bson.BsonDocumentWriter(bsonDoc);
        codec.encode(writer, in, org.bson.codecs.EncoderContext.builder().build());

        // Decode back from the BsonDocument
        org.bson.BsonDocumentReader reader = new org.bson.BsonDocumentReader(bsonDoc);
        TestEntity out = codec.decode(reader, org.bson.codecs.DecoderContext.builder().build());

        assertNotNull(out);
        assertEquals("alice", out.getName());
    }

    @Test
    public void testGeneralCodecEncodeDecodeNonEntityInteger() {
        // Integer is encoded as its string form, then valueOf re-parses
        MongoDBBase.GeneralCodec<Integer> codec = new MongoDBBase.GeneralCodec<>(Integer.class);

        org.bson.BsonDocument wrap = new org.bson.BsonDocument();
        org.bson.BsonDocumentWriter writer = new org.bson.BsonDocumentWriter(wrap);
        writer.writeStartDocument();
        writer.writeName("v");
        codec.encode(writer, 42, org.bson.codecs.EncoderContext.builder().build());
        writer.writeEndDocument();

        org.bson.BsonDocumentReader reader = new org.bson.BsonDocumentReader(wrap);
        reader.readStartDocument();
        reader.readName();
        Integer decoded = codec.decode(reader, org.bson.codecs.DecoderContext.builder().build());
        reader.readEndDocument();

        assertEquals(42, decoded);
    }

    @Test
    public void testGeneralCodecEncodeDecodeNonEntityString() {
        // Non-bean classes are written/read as plain strings
        MongoDBBase.GeneralCodec<String> codec = new MongoDBBase.GeneralCodec<>(String.class);

        org.bson.BsonDocument wrap = new org.bson.BsonDocument();
        org.bson.BsonDocumentWriter writer = new org.bson.BsonDocumentWriter(wrap);
        writer.writeStartDocument();
        writer.writeName("v");
        codec.encode(writer, "hello", org.bson.codecs.EncoderContext.builder().build());
        writer.writeEndDocument();

        org.bson.BsonDocumentReader reader = new org.bson.BsonDocumentReader(wrap);
        reader.readStartDocument();
        reader.readName();
        String decoded = codec.decode(reader, org.bson.codecs.DecoderContext.builder().build());
        reader.readEndDocument();

        assertEquals("hello", decoded);
    }

    // -- GeneralCodec tests (package-private inner class) --

    @Test
    public void testGeneralCodecGetEncoderClassForEntity() {
        MongoDBBase.GeneralCodec<TestEntity> codec = new MongoDBBase.GeneralCodec<>(TestEntity.class);
        assertEquals(TestEntity.class, codec.getEncoderClass());
    }

    @Test
    public void testGeneralCodecGetEncoderClassForNonEntity() {
        MongoDBBase.GeneralCodec<String> codec = new MongoDBBase.GeneralCodec<>(String.class);
        assertEquals(String.class, codec.getEncoderClass());
    }

    @Test
    public void testInMemoryArraysAreConvertedAndObjectTypedContainersKept_coverageMB() {
        // Pin (bean mapping already typed these before the element pass): a Document built in memory can hold Java arrays where a
        // decoded one holds Lists; the element pass converts their elements too (toDeclaredType's Object[] input). Each case on its own.
        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertEquals("Collection[Long:1, Long:2]",
                        mxDescribe(MongoDBBase.toEntity(new Document("longs", new Object[] { 1, 2L }), ContainerEntity.class).getLongs())),
                () -> assertEquals("Collection[Long:3]",
                        mxDescribe(MongoDBBase.toEntity(new Document("longs", new Integer[] { 3 }), ContainerEntity.class).getLongs())),
                () -> assertEquals("bob",
                        MongoDBBase.toEntity(new Document("children", new Object[] { new Document("name", "bob") }), ContainerEntity.class)
                                .getChildren()
                                .get(0)
                                .getName()),
                () -> assertEquals("bob",
                        MongoDBBase.toEntity(new Document("childArray", new Object[] { new Document("name", "bob") }), ContainerEntity.class).getChildArray()[0]
                                .getName()),
                () -> assertEquals(Arrays.asList(Color.GREEN),
                        MongoDBBase.toEntity(new Document("colors", new String[] { "GREEN" }), ContainerEntity.class).getColors()));

        // Pin: containers declared with Object elements keep the decoded values and the instance.
        final List<Object> raw = list(1, new Document("a", 1));
        final Document rawMap = new Document("k", new Document("b", 2));
        final BinaryEntity entity = MongoDBBase.toEntity(new Document("rawValues", raw).append("rawMap", rawMap), BinaryEntity.class);

        assertSame(raw, entity.getRawValues());
        assertSame(rawMap, entity.getRawMap());
    }

    @Test
    public void testReadRowRejectsExactlyTwoNonIdFieldsForScalarResult() {
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.readRow(new Document("a", 1).append("b", 2), Integer.class));
    }

    @Test
    public void testResetObjectIdMapWithByteArrayIdConvertedToObjectId() {
        Map<String, Object> map = new HashMap<>();
        byte[] id12 = new byte[12];
        for (int i = 0; i < 12; i++) {
            id12[i] = (byte) i;
        }
        map.put("_id", id12);
        map.put("name", "b");

        Document doc = MongoDBBase.toDocument(map);

        assertTrue(doc.containsKey("_id"));
        assertTrue(doc.get("_id") instanceof ObjectId);
    }

    // -- resetObjectId branches via toDocument (Map-based String/Date/byte[] keyed as _id) --

    @Test
    public void testResetObjectIdMapWithStringIdHexConvertedToObjectId() {
        // When the input is a Map with a String _id holding a valid 24-hex ObjectId
        // resetObjectId converts the String to an ObjectId on the result document.
        Map<String, Object> map = new HashMap<>();
        ObjectId expected = new ObjectId();
        map.put("_id", expected.toHexString());
        map.put("name", "n");

        Document doc = MongoDBBase.toDocument(map);

        assertTrue(doc.containsKey("_id"));
        assertTrue(doc.get("_id") instanceof ObjectId);
        assertEquals(expected, doc.get("_id"));
    }

    @Test
    public void testSortedQueueAndTreeContainersReceiveConvertedElements_coverageMB() {
        // Pin (bean mapping already typed these before the element pass): declared containers the List/Set/LinkedHashMap fast paths
        // cannot hold are built by N.convert from the converted elements (toDeclaredType's fallbacks); each property is read on its own.
        org.junit.jupiter.api.Assertions
                .assertAll(
                        () -> assertEquals(
                                "Collection[Long:1, Long:3]", mxDescribe(MongoDBBase
                                        .toEntity(new Document("sortedLongs", list(3, 1L)), SortedContainerEntity.class)
                                        .getSortedLongs())),
                        () -> assertEquals("Collection[Long:1, Long:2]",
                                mxDescribe(MongoDBBase.toEntity(new Document("longDeque", list(1, 2L)), SortedContainerEntity.class).getLongDeque())),
                        () -> assertEquals("q",
                                MongoDBBase.toEntity(new Document("childQueue", list(new Document("name", "q"))), SortedContainerEntity.class)
                                        .getChildQueue()
                                        .peek()
                                        .getName()),
                        () -> assertEquals("Map[String:a=Long:1, String:b=Long:2]",
                                mxDescribe(MongoDBBase.toEntity(new Document("sortedCounts", new Document("b", 2).append("a", 1L)), SortedContainerEntity.class)
                                        .getSortedCounts())),
                        () -> assertEquals("t",
                                MongoDBBase.toEntity(new Document("childTree", new Document("k", new Document("name", "t"))), SortedContainerEntity.class)
                                        .getChildTree()
                                        .get("k")
                                        .getName()));
    }

    @Test
    public void testToEntityRejectsNullRowType() {
        // rowType is consumed by this library's reflection, so it is rejected as an illegal
        // argument rather than surfacing as an NPE from the internal id-setter cache lookup.
        final Document doc = new Document("value", "a");

        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toEntity(doc, null));
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toEntity(null, null));
    }

    @Test
    public void testToEntityConvertsArrayTypedElements_verifyMB() {
        // Regression: the element conversion treated every array element type as uninstantiable (Class.getModifiers reports
        // array classes as abstract). A row of an Integer[][] property whose values already were Integers stayed a List while
        // the other rows became Integer[], so building the matrix failed with ArrayStoreException (it read fine before the
        // element conversion existed); List<String[]>/List<int[]> elements stayed Lists and Map<Integer, byte[]/ByteBuffer>
        // values stayed Binary.
        final Document row = new Document("matrix", new ArrayList<>(Arrays.asList(new ArrayList<>(Arrays.asList(1L, 2L)), new ArrayList<>(Arrays.asList(3)))))
                .append("rows", new ArrayList<>(Arrays.asList(new ArrayList<>(Arrays.asList("a")), new ArrayList<>(Arrays.asList(1)))))
                .append("ints", new ArrayList<>(Arrays.asList(new ArrayList<>(Arrays.asList(1, 2L)))))
                .append("blobsById", new Document("1", new Binary(new byte[] { 1 })))
                .append("buffersById", new Document("2", new Binary(new byte[] { 2, 3 })));

        final ArrayElementEntity entity = MongoDBBase.toEntity(row, ArrayElementEntity.class);

        assertArrayEquals(new Integer[][] { { 1, 2 }, { 3 } }, entity.getMatrix());
        assertArrayEquals(new String[] { "a" }, entity.getRows().get(0));
        assertArrayEquals(new String[] { "1" }, entity.getRows().get(1));
        assertArrayEquals(new int[] { 1, 2 }, entity.getInts().get(0));
        assertArrayEquals(new byte[] { 1 }, entity.getBlobsById().get(1));
        assertEquals(ByteBuffer.wrap(new byte[] { 2, 3 }), entity.getBuffersById().get(2));
    }

    @Test
    public void testToEntityConvertsEveryBsonValueKindAtEveryPosition_coverageMB() {
        final List<MxKind> kinds = mxKinds();
        final List<MxPosition> positions = mxPositions();
        final List<String> failures = new ArrayList<>();
        int cells = 0;

        for (final MxPosition position : positions) {
            for (final MxKind kind : kinds) {
                // Excluded by design: a type argument cannot be primitive, and a primitive container element or array component is a
                // different declaration (long[] is not Long[]), so the primitive kind is only read as a plain, nested-bean or record one.
                // Excluded, pre-existing upstream (abacus-common 8.1.0; identical on HEAD): ParserUtil resolves a java.util.Date[] property by
                // its simple type name "Date[]", i.e. as java.sql.Date[] (its PropInfo.clazz included), so bean mapping fills the array with
                // java.sql.Date elements. Not a MongoDBBase conversion; reported upstream rather than worked around here.
                if (("prim".equals(kind.name()) && !position.primitiveAllowed()) || ("date".equals(kind.name()) && "X[] element".equals(position.name()))) {
                    continue;
                }

                cells++;
                final String expected = mxDescribe(kind.expected());
                String actual;

                try {
                    final Object entity = MongoDBBase.toEntity(position.document().apply(kind.name(), kind.stored().get()),
                            position.rowType().apply(kind.name()));
                    actual = mxDescribe(position.read().apply(entity, kind.name()));
                } catch (final RuntimeException e) {
                    actual = e.getClass().getSimpleName() + ": " + e.getMessage();
                }

                if (!expected.equals(actual)) {
                    failures.add(kind.name() + " @ " + position.name() + ": expected " + expected + " but was " + actual);
                }
            }
        }

        assertEquals(positions.size() * (kinds.size() - 1) + 5 - 1, cells);
        assertTrue(failures.isEmpty(), failures.size() + " of " + cells + " cells failed:\n" + String.join("\n", failures));
    }

    @Test
    public void testToEntityConvertsEveryValueOfARecord_fixMB() {
        // Bean mapping passes a record's values to its constructor unconverted, so a small number stored as int32 failed a Long,
        // short or BigDecimal component (and a String an enum one, ...) with "argument type mismatch". Every value is now
        // converted like a setter's value, by the same rules as for a mutable bean (UTC LocalDate, binary, ObjectId text).
        final Date day = new Date(LocalDate.of(2024, 1, 2).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());
        final ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
        final Document row = new Document("primitiveLong", 1).append("boxedLong", 2)
                .append("ratio", 3)
                .append("small", 4)
                .append("day", day)
                .append("at", day)
                .append("color", "GREEN")
                .append("amount", org.bson.types.Decimal128.parse("1.10"))
                .append("blob", new Binary(new byte[] { 7 }))
                .append("text", id);

        final ScalarRecord record = MongoDBBase.toEntity(row, ScalarRecord.class);

        assertEquals(1L, record.primitiveLong());
        assertEquals(Long.valueOf(2L), record.boxedLong());
        assertEquals(3.0, record.ratio());
        assertEquals((short) 4, record.small());
        assertEquals(LocalDate.of(2024, 1, 2), record.day());
        assertEquals(day.toInstant(), record.at());
        assertEquals(Color.GREEN, record.color());
        assertEquals(new java.math.BigDecimal("1.10"), record.amount());
        assertArrayEquals(new byte[] { 7 }, record.blob());
        assertEquals("507f1f77bcf86cd799439011", record.text());
        assertEquals(Integer.valueOf(2), row.get("boxedLong"));

        // Absent and null values keep the component defaults; an unconvertible value still fails.
        final ScalarRecord sparse = MongoDBBase.toEntity(new Document("boxedLong", null).append("ratio", 2), ScalarRecord.class);

        assertEquals(0L, sparse.primitiveLong());
        assertNull(sparse.boxedLong());
        assertEquals(2.0, sparse.ratio());
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toEntity(new Document("boxedLong", "abc"), ScalarRecord.class));

        // Records nested in a record, in its List and Map, in a mutable bean, and a generic record.
        final Document inner = new Document("name", "i").append("n", 5);
        final OuterRecord outer = MongoDBBase.toEntity(new Document("title", "t").append("inner", inner)
                .append("inners", list(inner, null))
                .append("byName", new Document("k", inner))
                .append("box", new Document("name", "b").append("value", 6))
                .append("boxes", list(new Document("name", "c").append("value", 7))), OuterRecord.class);

        assertEquals(Long.valueOf(5L), outer.inner().n());
        assertEquals(Long.valueOf(5L), outer.inners().get(0).n());
        assertNull(outer.inners().get(1));
        assertEquals(Long.valueOf(5L), outer.byName().get("k").n());
        assertEquals(Long.valueOf(6L), outer.box().value());
        assertEquals(Long.valueOf(7L), outer.boxes().get(0).value());

        final RecordHolderEntity holder = MongoDBBase.toEntity(new Document("inner", inner).append("inners", list(inner)), RecordHolderEntity.class);

        assertEquals(Long.valueOf(5L), holder.getInner().n());
        assertEquals(Long.valueOf(5L), holder.getInners().get(0).n());
    }

    @Test
    public void testToEntityConvertsTypedContainerElementsToDeclaredTypes() {
        // Regression: Beans.mapToBean assigned a decoded List/Map to a typed collection/map property as-is (the container class
        // already matched), so List<Bean> held Documents, List<Long> Integers, List<Enum> Strings and List<BigDecimal> Decimal128s;
        // the entity read back fine but every element access failed with ClassCastException. Embedded arrays of subdocuments
        // written by toDocument/insertOne could not be read back as beans.
        final Document child = new Document("name", "bob").append("_id", "c2");
        final List<Object> children = new ArrayList<>(Arrays.asList(child));
        final Document row = new Document("children", children).append("childSet", Arrays.asList(child))
                .append("childMap", new Document("k", child))
                .append("nested", new ArrayList<>(Arrays.asList(new ArrayList<>(Arrays.asList(child)))))
                .append("childArray", Arrays.asList(child))
                .append("longs", new ArrayList<>(Arrays.asList(1, 2L)))
                .append("colors", new ArrayList<>(Arrays.asList("GREEN")))
                .append("decimals", new ArrayList<>(Arrays.asList(org.bson.types.Decimal128.parse("1.25"))))
                .append("countByName", new Document("a", 1))
                .append("names", new ArrayList<>(Arrays.asList("x", "y")));

        final ContainerEntity entity = MongoDBBase.toEntity(row, ContainerEntity.class);

        assertEquals(1, entity.getChildren().size());
        final ChildEntity first = entity.getChildren().get(0);
        assertEquals("c2", first.getId());
        assertEquals("bob", first.getName());
        assertEquals("bob", entity.getChildSet().iterator().next().getName());
        assertEquals("bob", entity.getChildMap().get("k").getName());
        assertEquals("c2", entity.getNested().get(0).get(0).getId());
        assertEquals("bob", entity.getChildArray()[0].getName());
        assertEquals(Arrays.asList(1L, 2L), entity.getLongs());
        assertEquals(Long.class, entity.getLongs().get(0).getClass());
        assertEquals(Arrays.asList(Color.GREEN), entity.getColors());
        assertEquals(new java.math.BigDecimal("1.25"), entity.getDecimals().get(0));
        assertEquals(Long.valueOf(1L), entity.getCountByName().get("a"));

        // Already-typed containers are kept as-is, and the source document is never modified.
        assertSame(row.get("names"), entity.getNames());
        assertSame(children, row.get("children"));
        assertSame(child, children.get(0));
        assertEquals(Integer.valueOf(1), ((List<?>) row.get("longs")).get(0));
    }

    @Test
    public void testToEntityElementConversionFailuresNullKeysAndReadOnlyProperties_verifyMB() {
        // An element that cannot be converted fails the read like a property value of that type does (before the element
        // conversion the String was left in the List<Long>, failing later with ClassCastException on access).
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toEntity(new Document("id", "abc"), IntIdEntity.class));
        assertThrows(IllegalArgumentException.class,
                () -> MongoDBBase.toEntity(new Document("longs", new ArrayList<>(Arrays.asList("abc"))), ContainerEntity.class));

        // A null key names no property and is skipped whatever its value; resolving its property threw IllegalArgumentException
        // ("'propName' cannot be null") for a Date or container value.
        final Document withNullKey = new Document("names", new ArrayList<>(Arrays.asList("x")));
        withNullKey.put(null, new Date(0));
        assertEquals(Arrays.asList("x"), MongoDBBase.toEntity(withNullKey, ContainerEntity.class).getNames());
        withNullKey.put(null, new ArrayList<>(Arrays.asList(1)));
        assertEquals(Arrays.asList("x"), MongoDBBase.toEntity(withNullKey, ContainerEntity.class).getNames());

        // A read-only (getter-only) container property is skipped by bean mapping, so its stored value is never converted.
        final ReadOnlyContainerEntity readOnly = MongoDBBase.toEntity(new Document("name", "n").append("computed", new ArrayList<>(Arrays.asList("zz"))),
                ReadOnlyContainerEntity.class);

        assertEquals("n", readOnly.getName());
        assertEquals(Arrays.asList(1L), readOnly.getComputed());
    }

    @Test
    public void testToEntityElementConversionFollowsColumnNamesSelfReferencesAndRecords_verifyMB() {
        // The element pass resolves a key to the property Beans.mapToBean sets (here through a @Column name), recurses through a
        // self-referencing bean type only as deep as the data goes, and also serves records, whose components bean mapping
        // assigns without any conversion (a record with a LocalDate or List<Long> component could not be read at all).
        final ColumnNamedEntity named = MongoDBBase.toEntity(new Document("child_nums", new ArrayList<>(Arrays.asList(1))), ColumnNamedEntity.class);

        assertEquals(Long.class, named.getChildNums().get(0).getClass());

        final Document tree = new Document("name", "root")
                .append("children",
                        new ArrayList<>(
                                Arrays.asList(new Document("name", "c").append("children", new ArrayList<>(Arrays.asList(new Document("name", "gc")))))))
                .append("byName", new Document("x", new Document("name", "px")));

        final TreeNode root = MongoDBBase.toEntity(tree, TreeNode.class);

        assertEquals("gc", root.getChildren().get(0).getChildren().get(0).getName());
        assertEquals("px", root.getByName().get("x").getName());

        final Date day = new Date(LocalDate.of(2024, 1, 2).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());
        final DayRecord record = MongoDBBase.toEntity(new Document("name", "r").append("nums", new ArrayList<>(Arrays.asList(1))).append("day", day),
                DayRecord.class);

        assertEquals(Arrays.asList(1L), record.nums());
        assertEquals(LocalDate.of(2024, 1, 2), record.day());
    }

    @Test
    public void testToEntityElementConversionKeepsSpecialContainersAndUninstantiableElements() {
        // Companion pin for the element conversion: declared containers the plain List/Set/Map fast paths cannot build
        // (EnumSet) still come out right, and elements of an abstract bean type, which cannot be instantiated, stay as
        // they were instead of failing the whole read.
        final Document row = new Document("colorSet", new ArrayList<>(Arrays.asList("RED"))).append("shapes",
                new ArrayList<>(Arrays.asList(new Document("name", "x"))));

        final SpecialContainerEntity entity = MongoDBBase.toEntity(row, SpecialContainerEntity.class);

        assertEquals(java.util.EnumSet.of(Color.RED), entity.getColorSet());
        assertEquals(new Document("name", "x"), ((List<?>) entity.getShapes()).get(0));
    }

    @Test
    public void testToEntityGenericBeanBsonValuesFollowTheTypeArgument_fixMB() {
        // BSON values in a T property are converted to the type argument the same way a property of that type converts them
        // (an ObjectId into its hex string, a date into a UTC LocalDate, Decimal128 exactly, binary into byte[]/ByteBuffer),
        // and are kept when they already have it.
        final ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
        final Date day = new Date(LocalDate.of(2024, 1, 2).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());
        final Document row = new Document("strings", list(box(id), box(5))).append("ids", list(box(id)))
                .append("days", list(box(day)))
                .append("decimals", list(box(org.bson.types.Decimal128.parse("1.10"))))
                .append("blobs", list(box(new Binary(new byte[] { 1, 2 }))))
                .append("buffer", box(new Binary(new byte[] { 3 })))
                .append("dates", list(box(day)));

        final GenericBsonEntity entity = MongoDBBase.toEntity(row, GenericBsonEntity.class);

        assertEquals("507f1f77bcf86cd799439011", entity.getStrings().get(0).getValue());
        assertEquals("5", entity.getStrings().get(1).getValue());
        assertSame(id, entity.getIds().get(0).getValue());
        assertEquals(LocalDate.of(2024, 1, 2), entity.getDays().get(0).getValue());
        assertEquals(new java.math.BigDecimal("1.10"), entity.getDecimals().get(0).getValue());
        assertArrayEquals(new byte[] { 1, 2 }, entity.getBlobs().get(0).getValue());
        assertEquals(ByteBuffer.wrap(new byte[] { 3 }), entity.getBuffer().getValue());
        assertSame(day, entity.getDates().get(0).getValue());
    }

    @Test
    public void testToEntityGenericBeanElementsReceiveTheirTypeArguments_fixMB() {
        // Bean elements were mapped by their raw class only, so the T value of a Box<Long> element kept the decoded Integer (or
        // Document, String, ...) and reading it as Long failed with ClassCastException. Every container shape and nesting level
        // now resolves the element's type arguments.
        final Document pageDoc = new Document("items", list(9, 10L)).append("byKey", new Document("k", 11))
                .append("name", "p")
                .append("computed", "read-only, skipped");
        final Document row = new Document("boxes", list(box(1), null, box(2L), box(null))).append("boxByName", new Document("a", box(3)).append("n", null))
                .append("boxArray", list(box(4)))
                .append("boxSet", list(box(5)))
                .append("listBoxes", list(box(list(6, 7L))))
                .append("boxedBoxes", list(box(box(8))))
                .append("childBoxes", list(box(new Document("id", "c1").append("name", "bob"))))
                .append("pages", list(pageDoc))
                .append("pairs", list(new Document("key", 12).append("val", 13)));

        final GenericBoxEntity entity = MongoDBBase.toEntity(row, GenericBoxEntity.class);

        final Long first = entity.getBoxes().get(0).getValue();
        assertEquals(Long.valueOf(1L), first);
        assertNull(entity.getBoxes().get(1));
        assertEquals(Long.valueOf(2L), entity.getBoxes().get(2).getValue());
        assertNull(entity.getBoxes().get(3).getValue());
        assertEquals(Long.valueOf(3L), entity.getBoxByName().get("a").getValue());
        assertTrue(entity.getBoxByName().containsKey("n"));
        assertNull(entity.getBoxByName().get("n"));
        assertEquals(Long.valueOf(4L), entity.getBoxArray()[0].getValue());
        assertEquals(Long.valueOf(5L), entity.getBoxSet().iterator().next().getValue());
        assertEquals(Arrays.asList(6L, 7L), entity.getListBoxes().get(0).getValue());
        assertEquals(Long.valueOf(8L), entity.getBoxedBoxes().get(0).getValue().getValue());
        assertEquals("bob", entity.getChildBoxes().get(0).getValue().getName());

        // A generic bean whose properties are List<T> / Map<String, T>, and one with two type parameters.
        final Page<Long> page = entity.getPages().get(0);
        assertEquals(Arrays.asList(9L, 10L), page.getItems());
        assertEquals(Collections.singletonMap("k", 11L), page.getByKey());
        assertEquals("p", page.getName());
        assertNull(page.getComputed());
        assertEquals("12", entity.getPairs().get(0).getKey());
        assertEquals(Long.valueOf(13L), entity.getPairs().get(0).getVal());

        // The source document is not modified.
        assertEquals(Integer.valueOf(1), ((Document) ((List<?>) row.get("boxes")).get(0)).get("value"));
        assertEquals(Integer.valueOf(11), ((Document) pageDoc.get("byKey")).get("k"));

        // A value that cannot be converted to the type argument fails the read, as a Long property value would.
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toEntity(new Document("boxes", list(box("abc"))), GenericBoxEntity.class));
    }

    @Test
    public void testToEntityGenericBeanPropertiesAndBoundTypeVariablesReceiveTheirTypes_fixMB() {
        // A Box<Long> property (not only an element) was built by its raw class as well, and so was a class that binds the type
        // variable itself (LongBox extends Box<Long>): its erased field accepts any value, so bean mapping kept the Integer.
        final Document row = new Document("box", box(1)).append("childBox", box(new Document("id", "c2").append("name", "ann")))
                .append("longBox", box(2))
                .append("longBoxes", list(box(3)))
                .append("records", list(new Document("name", "r").append("value", 4)));

        final GenericBoxEntity entity = MongoDBBase.toEntity(row, GenericBoxEntity.class);

        final Long value = entity.getBox().getValue();
        assertEquals(Long.valueOf(1L), value);
        assertEquals("ann", entity.getChildBox().getValue().getName());
        assertEquals(Long.valueOf(2L), entity.getLongBox().getValue());
        assertEquals(Long.valueOf(3L), entity.getLongBoxes().get(0).getValue());
        assertEquals(Long.valueOf(4L), entity.getRecords().get(0).value());
        assertEquals(Long.valueOf(5L), MongoDBBase.toEntity(box(5), LongBox.class).getValue());

        // The same applies to a bean decoded by its registry codec (e.g. from a typed collection).
        final org.bson.BsonDocument stored = new org.bson.BsonDocument("boxes",
                new org.bson.BsonArray(Arrays.asList(new org.bson.BsonDocument("value", new org.bson.BsonInt32(7)))));
        final GenericBoxEntity decoded = MongoDBBase.codecRegistry.get(GenericBoxEntity.class)
                .decode(new org.bson.BsonDocumentReader(stored), org.bson.codecs.DecoderContext.builder().build());

        assertEquals(Long.valueOf(7L), decoded.getBoxes().get(0).getValue());
    }

    @Test
    public void testToEntityNeverMutatesSourceDocument() {
        ObjectId oid = new ObjectId();
        Document doc = new Document("_id", oid) {
            @Override
            public Object remove(final Object key) {
                if ("_id".equals(key)) {
                    throw new AssertionError("Source document must not be mutated during conversion");
                }

                return super.remove(key);
            }
        }.append("name", "test");

        TestEntity result = MongoDBBase.toEntity(doc, TestEntity.class);

        assertNotNull(result);
        assertEquals(oid.toHexString(), result.getId());
        assertEquals("test", result.getName());
        assertSame(oid, doc.get("_id"));
    }

    @Test
    public void testToEntityPartialElementConversionKeepsOrderAndReusesUnchangedContainers_verifyMB() {
        // The element pass copies a container only once one of its elements changes; the unchanged elements before it must
        // still be carried over, in order.
        final List<Object> longs = new ArrayList<>(Arrays.asList(1L, 2L, 3, 4L));
        final Document counts = new Document("a", 1L).append("b", 2L).append("c", 3);

        final ContainerEntity converted = MongoDBBase.toEntity(new Document("longs", longs).append("countByName", counts), ContainerEntity.class);

        assertEquals(Arrays.asList(1L, 2L, 3L, 4L), converted.getLongs());
        assertEquals(Long.class, converted.getLongs().get(2).getClass());
        assertEquals(Arrays.asList("a", "b", "c"), new ArrayList<>(converted.getCountByName().keySet()));
        assertEquals(Long.valueOf(3L), converted.getCountByName().get("c"));
        assertEquals(Integer.valueOf(3), longs.get(2));
        assertEquals(Integer.valueOf(3), counts.get("c"));

        // Containers that already hold their declared element types are assigned as-is, without a copy.
        final List<Object> typedLongs = new ArrayList<>(Arrays.asList(1L, 2L));
        final Document typedCounts = new Document("a", 1L);

        final ContainerEntity unchanged = MongoDBBase.toEntity(new Document("longs", typedLongs).append("countByName", typedCounts), ContainerEntity.class);

        assertSame(typedLongs, unchanged.getLongs());
        assertSame(typedCounts, unchanged.getCountByName());
    }

    @Test
    public void testToEntityPassesIdToTheIdComponentOfARecord_fixMB() {
        // A record has no id setter, so _id was dropped and its id component stayed null. _id now fills a String- or
        // ObjectId-typed id component, as it fills a mutable bean's id property (an ObjectId becomes its hex string).
        final ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
        final Document row = new Document("_id", id).append("name", "n");

        assertEquals("507f1f77bcf86cd799439011", MongoDBBase.toEntity(row, IdRecord.class).id());
        assertEquals("abc", MongoDBBase.toEntity(new Document("_id", "abc").append("name", "n"), IdRecord.class).id());
        assertSame(id, MongoDBBase.toEntity(row, ObjectIdRecord.class).id());
        assertEquals(new Document("_id", id).append("name", "n"), row);

        // A record written by this class stores its own id field, which the driver-generated _id does not override.
        final Document written = MongoDBBase.toDocument(new IdRecord("own", "n"));

        assertEquals("own", written.get("id"));
        written.put("_id", new ObjectId());
        assertEquals("own", MongoDBBase.toEntity(written, IdRecord.class).id());
    }

    @Test
    public void testToEntityPreservesNullObjectIdFieldInSourceDocument() {
        Document doc = new Document("_id", null).append("name", "test");

        TestEntity result = MongoDBBase.toEntity(doc, TestEntity.class);

        assertNotNull(result);
        assertEquals("test", result.getName());
        assertTrue(doc.containsKey("_id"));
        assertNull(doc.get("_id"));
    }

    @Test
    public void testToEntityRawWildcardAndPlainBeanElementsKeepTheirMapping_fixMB() {
        // Without type arguments (a raw Box, or Box<?>) there is nothing to resolve: the decoded value is kept, as before.
        final GenericBoxEntity entity = MongoDBBase.toEntity(new Document("rawBoxes", list(box(1))).append("wildcardBoxes", list(box(2))),
                GenericBoxEntity.class);

        assertEquals(Integer.valueOf(1), entity.getRawBoxes().get(0).getValue());
        assertEquals(Integer.valueOf(2), entity.getWildcardBoxes().get(0).getValue());

        // Elements that already are beans are kept, and so is their container.
        final Box<Long> existing = new Box<>();
        existing.setValue(9L);
        final List<Object> boxes = list(existing);

        assertSame(boxes, MongoDBBase.toEntity(new Document("boxes", boxes), GenericBoxEntity.class).getBoxes());

        // Non-generic bean elements and nested beans are mapped as before.
        final ContainerEntity plain = MongoDBBase.toEntity(
                new Document("children", list(new Document("id", "c").append("name", "n"))).append("childMap", new Document("k", new Document("name", "m"))),
                ContainerEntity.class);

        assertEquals("n", plain.getChildren().get(0).getName());
        assertEquals("m", plain.getChildMap().get("k").getName());
    }

    // -- toEntity edge cases --

    @Test
    public void testToEntityWithNullDoc() {
        TestEntity result = MongoDBBase.toEntity(null, TestEntity.class);
        assertNull(result);
    }

    @Test
    public void testToEntityWithObjectIdField() {
        // Entity with ObjectId field type must assign ObjectId directly
        ObjectId oid = new ObjectId();
        Document doc = new Document("_id", oid).append("name", "alpha");

        ObjectIdEntity result = MongoDBBase.toEntity(doc, ObjectIdEntity.class);

        assertNotNull(result);
        assertEquals(oid, result.getId());
        assertEquals("alpha", result.getName());
    }

    @Test
    public void testToEntityWithoutIdFieldOnEntity() {
        // NoIdEntity has no id getter/setter; toEntity must still work
        Document doc = new Document("value", "x");
        NoIdEntity result = MongoDBBase.toEntity(doc, NoIdEntity.class);
        assertNotNull(result);
        assertEquals("x", result.getValue());
    }

    @Test
    public void testToEntityWithStringIdConvertedFromObjectId() {
        // Entity has String id; when document _id is ObjectId, it should be converted to its string form
        ObjectId oid = new ObjectId();
        Document doc = new Document("_id", oid).append("name", "test");

        TestEntity result = MongoDBBase.toEntity(doc, TestEntity.class);

        assertNotNull(result);
        assertEquals("test", result.getName());
        assertEquals(oid.toHexString(), result.getId());
    }

    @Test
    public void testToListRejectsExactlyTwoNonIdFieldsForScalarResult() {
        List<Document> docs = Arrays.asList(new Document("a", 1).append("b", 2));
        MongoIterableStubs.stubRows(mockFindIterable, docs);

        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toList(mockFindIterable, Integer.class));
    }

    @Test
    public void testToListRejectsInconsistentScalarFieldNames() {
        List<Document> docs = Arrays.asList(new Document("a", 1), new Document("b", 2));
        MongoIterableStubs.stubRows(mockFindIterable, docs);

        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toList(mockFindIterable, Integer.class));
    }

    @Test
    public void testToListRejectsLaterWideDocumentForScalarResult() {
        // Regression: only validating the first row allowed a later heterogeneous row to be
        // silently reduced to one value even though it cannot represent a scalar projection.
        List<Document> docs = Arrays.asList(new Document("value", 1), new Document("a", 1).append("b", 2).append("c", 3));
        MongoIterableStubs.stubRows(mockFindIterable, docs);

        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toList(mockFindIterable, Integer.class));
    }

    @Test
    public void testToListBinaryValuesPreservesEachPayload() {
        final byte[] first = { 1, 2 };
        final byte[] second = { 3, 4, 5 };
        MongoIterableStubs.stubRows(mockFindIterable, Arrays.asList(new Document("value", first), new Document("value", new Binary(second))));

        assertEquals(Arrays.asList(ByteBuffer.wrap(first), ByteBuffer.wrap(second)), MongoDBBase.toList(mockFindIterable, ByteBuffer.class));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testToListConvertsScalarRowsToRequestedScalarType() {
        // Regression: scalar rows (e.g. from a typed distinct/aggregate iterable) whose Java type differed from the
        // requested one were rejected as "Cannot convert document: 1 to class: java.lang.Long".
        final MongoIterable<Object> iterable = org.mockito.Mockito.mock(MongoIterable.class);
        MongoIterableStubs.stubRows(iterable, Arrays.asList(1, null, 2L, "3"));

        final List<Long> result = MongoDBBase.toList(iterable, Long.class);

        assertEquals(Arrays.asList(1L, null, 2L, 3L), result);

        // Binary rows are BSON scalars too and follow the same conversion rules as document field values.
        MongoIterableStubs.stubRows(iterable, Arrays.asList(new Binary(new byte[] { 1, 2 })));

        final List<ByteBuffer> buffers = MongoDBBase.toList(iterable, ByteBuffer.class);

        assertEquals(1, buffers.size());
        assertArrayEquals(new byte[] { 1, 2 }, buffers.get(0).array());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testToListCopiesMapEntriesWhenConcreteMapTypeDiffers() {
        // Regression: a HashMap row requested as Document was passed to Beans.beanToMap(...),
        // which introspected HashMap methods instead of copying the actual MongoDB field entries.
        MongoIterable<Map<String, Object>> iterable = org.mockito.Mockito.mock(MongoIterable.class);
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("name", "alice");
        row.put("age", 31);
        MongoIterableStubs.stubRows(iterable, Arrays.asList(row));

        List<Document> result = MongoDBBase.toList(iterable, Document.class);

        assertEquals(1, result.size());
        assertEquals("alice", result.get(0).getString("name"));
        assertEquals(31, result.get(0).getInteger("age"));
    }

    // -- toList edge cases targeting uncovered branches --

    @Test
    public void testToListLargeDocSingleValueRejected() {
        // A document with multiple projected fields cannot be converted to a primitive type.
        List<Document> docs = Arrays.asList(new Document("a", 1).append("b", 2).append("c", 3));
        MongoIterableStubs.stubRows(mockFindIterable, docs);

        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toList(mockFindIterable, Integer.class));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testToListMapsNonDocumentMapRowsToEntityWithIdHandling() {
        // Regression: a non-Document Map row (e.g. BasicDBObject) requested as an entity was passed to
        // Beans.copyAs(...), which can only copy bean-to-bean and rejected the HashMap outright.
        final MongoIterable<Map<String, Object>> iterable = org.mockito.Mockito.mock(MongoIterable.class);
        final Map<String, Object> row = new LinkedHashMap<>();
        row.put("_id", new ObjectId("507f1f77bcf86cd799439011"));
        row.put("name", "alice");
        MongoIterableStubs.stubRows(iterable, Arrays.asList(row, null, new BasicDBObject("name", "bob")));

        final List<TestEntity> result = MongoDBBase.toList(iterable, TestEntity.class);

        assertEquals(3, result.size());
        assertEquals("507f1f77bcf86cd799439011", result.get(0).getId());
        assertEquals("alice", result.get(0).getName());
        assertNull(result.get(1));
        assertEquals("bob", result.get(2).getName());
        assertNull(result.get(2).getId());
        // The source row is left untouched (its _id is not removed while mapping).
        assertTrue(row.containsKey("_id"));
    }

    @Test
    public void testToListPrimitiveExtractionWithConversion() {
        // Documents have integer value; requested as Long -- needs convert path
        List<Document> docs = Arrays.asList(new Document("v", 10), new Document("v", 20));
        MongoIterableStubs.stubRows(mockFindIterable, docs);

        List<Long> result = MongoDBBase.toList(mockFindIterable, Long.class);

        assertEquals(2, result.size());
        assertEquals(10L, result.get(0));
        assertEquals(20L, result.get(1));
    }

    @Test
    public void testToListScalarConvertsEveryRowNotOnlyWhenSampleNeedsIt() {
        // Regression: when the first non-null sample was already an instance of rowType, every row was returned raw.
        // MongoDB freely mixes int32/int64 for the same field, so a later Integer leaked into a List<Long>.
        final List<Document> docs = Arrays.asList(new Document("v", 1L), new Document("v", 2), new Document("v", null));
        MongoIterableStubs.stubRows(mockFindIterable, docs);

        final List<Long> result = MongoDBBase.toList(mockFindIterable, Long.class);

        assertEquals(3, result.size());
        assertEquals(Long.valueOf(1L), result.get(0));
        assertEquals(Long.class, result.get(1).getClass());
        assertEquals(Long.valueOf(2L), result.get(1));
        assertNull(result.get(2));
    }

    @Test
    public void testToListScalarProjectionDerivesPropNameFromFirstRowThatCarriesIt() {
        // Regression: a matched document that lacks the projected field comes back as {_id: ...} only.
        // When such a document happens to be FIRST, the scalar property name must still be derived from
        // a later row that carries the non-_id key — previously every value was silently read from "_id".
        Document idOnly = new Document("_id", new ObjectId());
        Document withName = new Document("_id", new ObjectId()).append("name", "alice");
        MongoIterableStubs.stubRows(mockFindIterable, Arrays.asList(idOnly, withName));

        List<String> result = MongoDBBase.toList(mockFindIterable, String.class);

        assertEquals(2, result.size());
        assertNull(result.get(0)); // first row has no "name" value
        assertEquals("alice", result.get(1)); // read from "name", NOT from "_id"
    }

    // -- 2026-09-27 review (slice H) regressions --

    @SuppressWarnings("unchecked")
    @Test
    public void testToListScalarRowsConvertLaterRowsWhenFirstAlreadyHasRequestedType() {
        // Regression: when the FIRST scalar row was already a Long, the whole raw list was returned, so a later
        // Integer row (MongoDB mixes int32/int64 for one field) leaked into the List<Long>. With an Integer first
        // the same rows were converted, so the result depended on row order.
        final MongoIterable<Object> iterable = org.mockito.Mockito.mock(MongoIterable.class);
        MongoIterableStubs.stubRows(iterable, Arrays.asList(1L, null, 2));

        final List<Long> result = MongoDBBase.toList(iterable, Long.class);

        assertEquals(3, result.size());
        assertEquals(Long.valueOf(1L), result.get(0));
        assertNull(result.get(1));
        assertEquals(Long.class, result.get(2).getClass());
        assertEquals(Long.valueOf(2L), result.get(2));

        // Rows that all have the requested type are still returned as-is (no per-row copy): the result holds the
        // fetched row instances themselves. (Values outside the Long cache, so the identity check is meaningful.)
        final List<Object> sameType = Arrays.asList(1_000_001L, null, 2_000_002L);
        MongoIterableStubs.stubRows(iterable, sameType);

        final List<Long> asIs = MongoDBBase.toList(iterable, Long.class);

        assertEquals(sameType, asIs);
        assertSame(sameType.get(0), asIs.get(0));
        assertSame(sameType.get(2), asIs.get(2));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testToListStillRejectsNonScalarRowsForScalarTarget() {
        final MongoIterable<Object> iterable = org.mockito.Mockito.mock(MongoIterable.class);
        final TestEntity bean = new TestEntity();
        bean.setName("x");
        MongoIterableStubs.stubRows(iterable, Arrays.asList(bean));

        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toList(iterable, Long.class));

        MongoIterableStubs.stubRows(iterable, Arrays.asList(Arrays.asList(1, 2)));

        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toList(iterable, Long.class));

        // A scalar row cannot become an array/collection target either.
        MongoIterableStubs.stubRows(iterable, Arrays.asList(1));

        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toList(iterable, Object[].class));
    }

    @Test
    public void testToListWithNullElementsReturnsEmpty() {
        // No non-null first => returns empty list
        MongoIterableStubs.stubRows(mockFindIterable, new ArrayList<>());
        List<String> result = MongoDBBase.toList(mockFindIterable, String.class);
        assertEquals(0, result.size());
    }

    @Test
    public void testToListWithDocumentsAlreadyMatchingType() {
        // Documents returned exactly match rowType -> fast path returning rowList as-is
        List<Document> docs = Arrays.asList(new Document("a", 1));
        MongoIterableStubs.stubRows(mockFindIterable, docs);

        List<Document> result = MongoDBBase.toList(mockFindIterable, Document.class);

        assertEquals(1, result.size());
        assertEquals(1, result.get(0).getInteger("a"));
    }

    // -- toList variants --

    @Test
    public void testToListWithMapClass() {
        // Documents are Maps already so the cast path returns them unchanged
        List<Document> docs = Arrays.asList(new Document("id", 1), new Document("id", 2));
        MongoIterableStubs.stubRows(mockFindIterable, docs);

        @SuppressWarnings("rawtypes")
        List<Map> result = MongoDBBase.toList(mockFindIterable, Map.class);

        assertEquals(2, result.size());
    }

    @Test
    public void testToListWithSingleValueExtraction() {
        // A doc with one non-_id field, requested as plain type (String) -- readRow takes value path
        List<Document> docs = Arrays.asList(new Document("name", "alice"), new Document("name", "bob"));
        MongoIterableStubs.stubRows(mockFindIterable, docs);

        List<String> result = MongoDBBase.toList(mockFindIterable, String.class);

        assertEquals(2, result.size());
        assertEquals("alice", result.get(0));
    }

    @Test
    public void testToMapWithNullMapSupplier() {
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toMap(new Document(), null));
    }

    @Test
    public void testToMapWithLinkedHashMapSupplier() {
        Document doc = new Document("first", 1).append("second", 2);
        Map<String, Object> result = MongoDBBase.toMap(doc, IntFunctions.ofMap(LinkedHashMap.class));

        assertTrue(result instanceof LinkedHashMap);
        assertEquals(2, result.size());
    }

    // -- toMap edge cases --

    @Test
    public void testToMapWithTreeMapSupplier() {
        Document doc = new Document("b", 1).append("a", 2);
        Map<String, Object> result = MongoDBBase.toMap(doc, IntFunctions.ofMap(TreeMap.class));

        assertTrue(result instanceof TreeMap);
        assertEquals(2, result.size());
        assertEquals(1, result.get("b"));
    }

    @Test
    public void testTypedBinaryArraysConvertElementsWithoutChangingSource() {
        final byte[] bytes = { 7, 8, 9 };
        final Binary binary = new Binary(bytes);
        final Object[] values = { bytes, binary, null };

        for (final Object input : Arrays.asList(values, Arrays.asList(values))) {
            final Document row = new Document("bufferArray", input).append("byteArrayArray", input).append("rawArray", values);
            final BinaryEntity entity = MongoDBBase.toEntity(row, BinaryEntity.class);

            assertEquals(ByteBuffer.wrap(bytes), entity.getBufferArray()[0]);
            assertEquals(ByteBuffer.wrap(bytes), entity.getBufferArray()[1]);
            assertNull(entity.getBufferArray()[2]);
            assertArrayEquals(bytes, entity.getByteArrayArray()[0]);
            assertArrayEquals(bytes, entity.getByteArrayArray()[1]);
            assertNull(entity.getByteArrayArray()[2]);
            assertSame(values, entity.getRawArray());
            assertSame(input, row.get("bufferArray"));
            assertSame(binary, values[1]);
        }

        final ByteBuffer[] buffers = { ByteBuffer.wrap(bytes), null };
        final byte[][] arrays = { bytes, null };
        final BinaryEntity typed = MongoDBBase.toEntity(new Document("bufferArray", buffers).append("byteArrayArray", arrays), BinaryEntity.class);
        assertSame(buffers, typed.getBufferArray());
        assertSame(arrays, typed.getByteArrayArray());
    }

    @Test
    public void testTypedBinaryCollectionsConvertElementsWithoutChangingSource() {
        final byte[] bytes = { 7, 8, 9 };
        final Binary binary = new Binary(bytes);
        final List<Object> values = Arrays.asList(bytes, binary, null);
        final Document row = new Document("buffers", values).append("byteArrays", values).append("rawValues", values);

        final BinaryEntity entity = MongoDBBase.toEntity(row, BinaryEntity.class);

        assertEquals(ByteBuffer.wrap(bytes), entity.getBuffers().get(0));
        assertEquals(ByteBuffer.wrap(bytes), entity.getBuffers().get(1));
        assertNull(entity.getBuffers().get(2));
        assertArrayEquals(bytes, entity.getByteArrays().get(0));
        assertArrayEquals(bytes, entity.getByteArrays().get(1));
        assertNull(entity.getByteArrays().get(2));
        assertSame(values, entity.getRawValues());
        assertSame(values, row.get("buffers"));
        assertSame(bytes, values.get(0));
        assertSame(binary, values.get(1));
    }

    @Test
    public void testTypedBinaryMapsConvertValuesWithoutChangingSource() {
        final byte[] bytes = { 7, 8, 9 };
        final Binary binary = new Binary(bytes);
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put("array", bytes);
        values.put("binary", binary);
        values.put("missing", null);
        final Document row = new Document("bufferMap", values).append("byteArrayMap", values).append("rawMap", values);

        final BinaryEntity entity = MongoDBBase.toEntity(row, BinaryEntity.class);

        assertEquals(ByteBuffer.wrap(bytes), entity.getBufferMap().get("array"));
        assertEquals(ByteBuffer.wrap(bytes), entity.getBufferMap().get("binary"));
        assertNull(entity.getBufferMap().get("missing"));
        assertArrayEquals(bytes, entity.getByteArrayMap().get("array"));
        assertArrayEquals(bytes, entity.getByteArrayMap().get("binary"));
        assertNull(entity.getByteArrayMap().get("missing"));
        assertSame(values, entity.getRawMap());
        assertSame(values, row.get("bufferMap"));
        assertSame(binary, values.get("binary"));
    }

    @Test
    public void testValueTypesWithBeanAccessorsAreNotEncodedAsBeanDocuments() {
        // Regression: GeneralCodec treated every Beans.isBeanClass type as an entity, which includes value types with
        // getters/setters. A GregorianCalendar property was written as {"timeZone": ..., "gregorianChange": ...} (the time
        // itself lost) and a ByteBuffer as {"position": 0, "limit": 3} (the bytes lost); both then failed to read back.
        final java.util.GregorianCalendar calendar = new java.util.GregorianCalendar();
        calendar.setTimeInMillis(1700000000000L);
        final Document source = new Document("buffer", ByteBuffer.wrap(new byte[] { 1, 2, 3 })).append("calendar", calendar);
        final org.bson.BsonDocument written = new org.bson.BsonDocument();

        MongoDBBase.codecRegistry.get(Document.class)
                .encode(new org.bson.BsonDocumentWriter(written), source, org.bson.codecs.EncoderContext.builder().build());

        assertEquals(new org.bson.BsonBinary(new byte[] { 1, 2, 3 }), written.get("buffer"));
        assertTrue(written.get("calendar").isString(), written.toJson());

        final Document read = MongoDBBase.codecRegistry.get(Document.class)
                .decode(new org.bson.BsonDocumentReader(written), org.bson.codecs.DecoderContext.builder().build());
        final ValueTypeEntity entity = MongoDBBase.toEntity(read, ValueTypeEntity.class);

        assertEquals(ByteBuffer.wrap(new byte[] { 1, 2, 3 }), entity.getBuffer());
        assertEquals(1700000000000L, entity.getCalendar().getTimeInMillis());
    }

    // Updates and deletes

    @Test
    public void testResetObjectIdMapWithDateIdKeptAsIs() {
        Map<String, Object> map = new HashMap<>();
        Date when = new Date();
        map.put("_id", when);
        map.put("name", "d");

        Document doc = MongoDBBase.toDocument(map);

        // A Date is a legal MongoDB _id and is kept as-is. (Previously it was silently replaced by a
        // freshly generated ObjectId whose random component made the stored id non-deterministic.)
        assertTrue(doc.containsKey("_id"));
        assertEquals(when, doc.get("_id"));
    }

    @Test
    public void testResetObjectIdMapWithNon12ByteArrayIdKeptAsIs() {
        // A byte[] that is not exactly 12 bytes cannot be an ObjectId and is kept as-is.
        // (Previously new ObjectId(byte[]) threw IllegalArgumentException and the write failed.)
        Map<String, Object> map = new HashMap<>();
        byte[] id = new byte[] { 1, 2, 3 };
        map.put("_id", id);
        map.put("name", "b");

        Document doc = MongoDBBase.toDocument(map);

        assertSame(id, doc.get("_id"));
    }

    @Test
    public void testResetObjectIdMapWithNonHexStringIdKeptAsIs() {
        // A non-24-hex String is a legal MongoDB _id and must be written as-is. (Previously
        // new ObjectId(String) threw IllegalArgumentException and the whole write failed.)
        Map<String, Object> map = new HashMap<>();
        map.put("_id", "user-123");
        map.put("name", "s");

        Document doc = MongoDBBase.toDocument(map);

        assertEquals("user-123", doc.get("_id"));
    }

    // Queries and results

    @Test
    public void testNullArgumentsStillAcceptedWhereDocumented() {
        assertEquals("", MongoDBBase.toJson((BasicDBObject) null));
        assertTrue(MongoDBBase.toDocument((Object[]) null).isEmpty());
        assertEquals(0, MongoDBBase.stream((MongoCursor<Document>) null).count());
        assertNull(MongoDBBase.toEntity(null, TestEntity.class));
    }

    @Test
    public void testDateTargetsOtherThanLocalTypesKeepTheirInstant_verifyMB() {
        // Only LocalDate/LocalDateTime/LocalTime targets are read in UTC; instant-based targets keep the stored instant exactly as
        // before. LocalDate map values are read in UTC too (fails on a JVM whose default zone is west of UTC without the fix).
        final long millis = 1700000000123L;
        final Date date = new Date(millis);
        final Date day = new Date(LocalDate.of(2024, 1, 2).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());

        final InstantEntity entity = MongoDBBase.toEntity(new Document("date", date).append("instant", date)
                .append("zoned", date)
                .append("offset", date)
                .append("timestamp", date)
                .append("dayByName", new Document("a", day)), InstantEntity.class);

        assertEquals(millis, entity.getDate().getTime());
        assertEquals(millis, entity.getInstant().toEpochMilli());
        assertEquals(millis, entity.getZoned().toInstant().toEpochMilli());
        assertEquals(millis, entity.getOffset().toInstant().toEpochMilli());
        assertEquals(millis, entity.getTimestamp().getTime());
        assertEquals(LocalDate.of(2024, 1, 2), entity.getDayByName().get("a"));
        assertEquals(java.time.Instant.ofEpochMilli(millis), MongoDBBase.convertBsonValue(date, java.time.Instant.class));
    }

    @Test
    public void testGeneralCodecRegistryGetCachesPerClass() {
        // Two lookups for the same class should return the same instance (pooled)
        MongoDBBase.GeneralCodecRegistry registry = new MongoDBBase.GeneralCodecRegistry();
        org.bson.codecs.Codec<TestEntity> first = registry.get(TestEntity.class);
        org.bson.codecs.Codec<TestEntity> second = registry.get(TestEntity.class);

        assertNotNull(first);
        assertNotNull(second);
        assertTrue(first == second);
    }

    @Test
    public void testGeneralCodecRegistryGetForNonEntityType() {
        MongoDBBase.GeneralCodecRegistry registry = new MongoDBBase.GeneralCodecRegistry();
        org.bson.codecs.Codec<Long> codec = registry.get(Long.class);

        assertNotNull(codec);
        assertEquals(Long.class, codec.getEncoderClass());
    }

    // -- GeneralCodecRegistry tests (package-private inner class) --

    @Test
    public void testGeneralCodecRegistryGetReturnsCodec() {
        MongoDBBase.GeneralCodecRegistry registry = new MongoDBBase.GeneralCodecRegistry();
        org.bson.codecs.Codec<TestEntity> codec = registry.get(TestEntity.class);

        assertNotNull(codec);
        assertEquals(TestEntity.class, codec.getEncoderClass());
    }

    @Test
    public void testGeneralCodecRegistryGetWithRegistryDelegates() {
        // The two-arg get(Class, CodecRegistry) returns a codec for the class bound to the calling registry
        MongoDBBase.GeneralCodecRegistry registry = new MongoDBBase.GeneralCodecRegistry();
        org.bson.codecs.Codec<String> codec = registry.get(String.class, registry);

        assertNotNull(codec);
        assertEquals(String.class, codec.getEncoderClass());
    }

    // -- objectIdToFilter with invalid hex string --

    @Test
    public void testobjectIdToFilterWithInvalidHexStringThrows() {
        // The string is not a valid 24-hex ObjectId
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.objectIdToFilter("not-an-objectid"));
    }

    @Test
    public void testStreamCursorWithDocumentHoldingRowTypeReturnsDocumentsUnchanged() {
        // Regression: stream(cursor, Object.class/Bson.class) ran every row through readRow's scalar fallback, so a
        // multi-field document threw IllegalArgumentException and a single-field one yielded its sole value, unlike
        // toList(iterable, Object.class) and the executors' stream/list, which return the documents as-is.
        final Document multi = new Document("_id", 5).append("a", 1).append("b", 2);
        final Document single = new Document("_id", 6).append("a", 3);

        for (final Class<?> rowType : Arrays.<Class<?>> asList(Object.class, Bson.class, Map.class, Document.class)) {
            when(mockCursor.hasNext()).thenReturn(true, true, false);
            when(mockCursor.next()).thenReturn(multi, single);

            final List<?> rows = MongoDBBase.stream(mockCursor, rowType).toList();

            assertEquals(2, rows.size(), rowType.getName());
            assertSame(multi, rows.get(0), rowType.getName());
            assertSame(single, rows.get(1), rowType.getName());
        }

        // A scalar row type still extracts the single projected value.
        when(mockCursor.hasNext()).thenReturn(true, false);
        when(mockCursor.next()).thenReturn(single);

        assertEquals(Arrays.asList(3L), MongoDBBase.stream(mockCursor, Long.class).toList());
    }

    @Test
    public void testStreamFromCursorWithRowType() {
        Stream<TestEntity> s = MongoDBBase.stream(mockCursor, TestEntity.class);
        assertNotNull(s);
        s.close();
    }

    // -- stream wrappers --

    @Test
    public void testStreamFromCursorYieldsValidStream() {
        Stream<Document> s = MongoDBBase.stream(mockCursor);
        assertNotNull(s);
        s.close();
    }

    // Resource lifecycle

    @Test
    public void testFromJsonRejectsNullRowTypeWithDocumentedException() {
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.fromJson("{}", null));
    }

    @Test
    public void testScalarConversionPreservesNumericOverflowFailures() {
        final Document row = new Document("value", Long.MAX_VALUE);

        assertThrows(ArithmeticException.class, () -> MongoDBBase.readRow(row, Integer.class));
        assertThrows(ArithmeticException.class, () -> MongoDBBase.convertBsonValue(Long.MAX_VALUE, int.class));
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.convertBsonValue(Long.MAX_VALUE, null));
        assertEquals(Long.MAX_VALUE, MongoDBBase.readRow(row, Long.class));
    }

    // Additional contracts

    @Test
    public void testNullKeyIsSkippedWhateverItsValue_coverageMB() {
        // A null key names no property and bean mapping skips it. Resolving its property threw IllegalArgumentException ("'propName'
        // cannot be null"): on HEAD for a binary, map, collection or array value (the binary pass), and for a Date value once the element
        // pass looked dates up too. Every value kind, and a null key inside a bean element, is checked on its own.
        final List<Object> values = Arrays.asList(new Binary(new byte[] { 1 }), new byte[] { 1 }, ByteBuffer.wrap(new byte[] { 1 }), new Document("a", 1),
                list(1), new Object[] { 1 }, new Date(0), 1, "x");

        org.junit.jupiter.api.Assertions.assertAll(values.stream().map(value -> (org.junit.jupiter.api.function.Executable) () -> {
            final Document row = new Document("names", list("x"));
            row.put(null, value);
            assertEquals(Arrays.asList("x"), MongoDBBase.toEntity(row, ContainerEntity.class).getNames(), value.getClass().getName());
        }));

        final Document child = new Document("name", "a");
        child.put(null, list(1));

        assertEquals("a", MongoDBBase.toEntity(new Document("children", list(child)), ContainerEntity.class).getChildren().get(0).getName());
    }

    @Test
    public void testNullRequiredArgumentsAreIllegalArguments() {
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toMap((Document) null));
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toMap(null, IntFunctions.ofMap()));
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toJson((Bson) null));
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toJson((org.bson.BSONObject) null));
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toBson((Object) null));
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toBson(new Object[] { null }));
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toDocument((Object) null));
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toDocument(new Object[] { null }));
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toDocument(null, false));
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toBSONObject((Object) null));
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toBSONObject(new Object[] { null }));
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toDBObject((Object) null));
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toDBObject(new Object[] { null }));
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toList(null, Document.class));
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toList(mockFindIterable, null));
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.extractData((MongoIterable<?>) null));
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.extractData((MongoIterable<?>) null, Map.class));
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.extractData(null, (MongoIterable<?>) null, Map.class));
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.stream((MongoIterable<Document>) null));
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.stream((MongoIterable<Document>) null, Document.class));

        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toDocument((Object) null));
        assertTrue(e.getMessage().contains("obj"), e.getMessage());
    }

    @Test
    public void testBinaryBufferConversionsDoNotConsumeSource() {
        final ByteBuffer input = ByteBuffer.allocateDirect(5);
        input.put(new byte[] { 0, 1, 2, 3, 4 }).flip();
        input.position(1);
        input.limit(4);
        final ByteBuffer readOnly = input.asReadOnlyBuffer();

        assertArrayEquals(new byte[] { 1, 2, 3 }, MongoDBBase.readRow(new Document("value", readOnly), byte[].class));
        final BinaryEntity entity = MongoDBBase.toEntity(new Document("bytes", readOnly).append("buffer", readOnly), BinaryEntity.class);
        assertArrayEquals(new byte[] { 1, 2, 3 }, entity.getBytes());
        assertSame(readOnly, entity.getBuffer());
        assertEquals(1, readOnly.position());
        assertEquals(4, readOnly.limit());
    }

    @Test
    public void testBoundTypeVariablesWithoutAFieldOrInsideContainersReceiveTheirTypes_coverageMB() {
        // A class binding a type variable whose property has no field (only a setter taking T) is recognized from the setter's declared
        // type; one binding the T of List<T>/Map<String, T> properties (LongPage extends Page<Long>) gets typed elements. Each case on its own.
        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals("Long:1", mxDescribe(MongoDBBase.toEntity(box(1), LongSetterBox.class).getValue())),
                () -> assertEquals("Long:2", mxDescribe(MongoDBBase.toEntity(new Document("setterBox", box(2)), BoundEntity.class).getSetterBox().getValue())),
                () -> assertEquals("Long:3",
                        mxDescribe(MongoDBBase.toEntity(new Document("setterBoxes", list(box(3))), BoundEntity.class).getSetterBoxes().get(0).getValue())),
                () -> {
                    final LongPage page = MongoDBBase.toEntity(
                            new Document("items", list(4)).append("byKey", new Document("k", 5)).append("computed", "read-only, skipped"), LongPage.class);

                    assertEquals("Collection[Long:4]", mxDescribe(page.getItems()));
                    assertEquals("Map[String:k=Long:5]", mxDescribe(page.getByKey()));
                }, () -> assertEquals("Collection[Long:6]", mxDescribe(
                        MongoDBBase.toEntity(new Document("pages", list(new Document("items", list(6)))), BoundEntity.class).getPages().get(0).getItems())));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testElementAndUtcConversionApplyOnEveryReadPath_verifyMB() {
        final Date day = new Date(LocalDate.of(2024, 1, 2).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());
        final MongoIterable<Object> iterable = org.mockito.Mockito.mock(MongoIterable.class);

        // toList: Document rows and non-Document Map rows are both mapped through toEntity.
        MongoIterableStubs.stubRows(iterable, Arrays.asList(new Document("longs", new ArrayList<>(Arrays.asList(1)))));
        assertEquals(Long.class, MongoDBBase.toList(iterable, ContainerEntity.class).get(0).getLongs().get(0).getClass());
        MongoIterableStubs.stubRows(iterable, Arrays.asList(new BasicDBObject("longs", new ArrayList<>(Arrays.asList(2)))));
        assertEquals(Long.class, MongoDBBase.toList(iterable, ContainerEntity.class).get(0).getLongs().get(0).getClass());

        // toList: scalar rows and single-field projections requested as LocalDate are read in UTC.
        MongoIterableStubs.stubRows(iterable, Arrays.asList(day, null));
        assertEquals(Arrays.asList(LocalDate.of(2024, 1, 2), null), MongoDBBase.toList(iterable, LocalDate.class));
        MongoIterableStubs.stubRows(iterable, Arrays.asList(new Document("_id", 1).append("day", day)));
        assertEquals(Arrays.asList(LocalDate.of(2024, 1, 2)), MongoDBBase.toList(iterable, LocalDate.class));

        // A bean decoded by its registry codec (e.g. from a typed collection) gets the same element conversion.
        final org.bson.BsonDocument stored = new org.bson.BsonDocument("longs", new org.bson.BsonArray(Arrays.asList(new org.bson.BsonInt32(7))))
                .append("childMap", new org.bson.BsonDocument("k", new org.bson.BsonDocument("name", new org.bson.BsonString("bob"))));

        final ContainerEntity decoded = MongoDBBase.codecRegistry.get(ContainerEntity.class)
                .decode(new org.bson.BsonDocumentReader(stored), org.bson.codecs.DecoderContext.builder().build());

        assertEquals(Long.class, decoded.getLongs().get(0).getClass());
        assertEquals("bob", decoded.getChildMap().get("k").getName());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testElementAndUtcConversionOnTheRemainingReadPaths_coverageMB() {
        // toEntity's element pass and convertBsonValue's UTC rule are shared by every read path. The verifyMB test covers toList and a
        // bean's registry codec; this one covers readRow, both stream overloads, the bean Dataset of extractData and the codec of a
        // non-bean type, each asserted on its own. All of them read Integers / local-zone dates before (the UTC cells can only fail on a
        // JVM whose default zone is not UTC).
        final long dayMillis = LocalDate.of(2024, 1, 2).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        final Date day = new Date(dayMillis);
        final Date time = new Date(LocalTime.of(10, 30).toSecondOfDay() * 1000L);
        final MongoIterable<Document> iterable = org.mockito.Mockito.mock(MongoIterable.class);
        when(iterable.iterator()).thenAnswer(invocation -> mxCursor(new Document("longs", list(1))));

        org.junit.jupiter.api.Assertions
                .assertAll(
                        () -> assertEquals("Collection[Long:1]",
                                mxDescribe(MongoDBBase.readRow(new Document("longs", list(1)), ContainerEntity.class).getLongs()), "readRow bean"),
                        () -> assertEquals(LocalDate.of(2024, 1, 2), MongoDBBase.readRow(new Document("_id", 1).append("day",
                                day), LocalDate.class), "readRow LocalDate"),
                        () -> assertEquals(LocalTime.of(10, 30), MongoDBBase.readRow(new Document("time", time),
                                LocalTime.class), "readRow LocalTime"),
                        () -> assertEquals("Collection[Long:1]",
                                mxDescribe(MongoDBBase.stream(mxCursor(new Document("longs", list(1))), ContainerEntity.class).first().get().getLongs()),
                                "stream(cursor) bean"),
                        () -> assertEquals(Arrays.asList(LocalDate.of(2024, 1, 2)),
                                MongoDBBase.stream(mxCursor(new Document("day", day)), LocalDate.class).toList(), "stream(cursor) LocalDate"),
                        () -> assertEquals(
                                "Collection[Long:1]", mxDescribe(MongoDBBase.stream(iterable, ContainerEntity.class).first().get().getLongs()),
                                "stream(iterable) bean"),
                        () -> assertEquals("Collection[Long:1]", mxDescribe(
                                MongoDBBase.extractData(Arrays.asList(new Document("longs", list(1))), ContainerEntity.class).getColumn("longs").get(0)),
                                "extractData bean Dataset"),
                        () -> assertEquals(LocalDate.of(2024, 1, 2), mxDecode(LocalDate.class, new org.bson.BsonDateTime(dayMillis)),
                                "GeneralCodec<LocalDate>"),
                        () -> assertEquals(LocalDateTime.of(2024, 1, 2, 0, 0), mxDecode(LocalDateTime.class, new org.bson.BsonDateTime(dayMillis)),
                                "GeneralCodec<LocalDateTime>"));
    }

    @Test
    public void testGeneralCodecFollowsTheRegistryUuidRepresentationAndIsCachedPerRegistry_verifyMB() {
        // Legacy representations work for a UUID nested in a bean as well, in both directions.
        final org.bson.codecs.configuration.CodecRegistry legacy = org.bson.codecs.configuration.CodecRegistries
                .withUuidRepresentation(MongoDBBase.codecRegistry, org.bson.UuidRepresentation.JAVA_LEGACY);
        final java.util.UUID key = java.util.UUID.fromString("01234567-89ab-cdef-0123-456789abcdef");
        final UuidHolder inner = new UuidHolder();
        inner.setKey(key);
        final org.bson.BsonDocument written = new org.bson.BsonDocument();

        legacy.get(Document.class)
                .encode(new org.bson.BsonDocumentWriter(written), new Document("inner", inner), org.bson.codecs.EncoderContext.builder().build());

        assertEquals(new org.bson.BsonBinary(key, org.bson.UuidRepresentation.JAVA_LEGACY), written.getDocument("inner").get("key"));
        assertEquals(key,
                legacy.get(UuidHolder.class)
                        .decode(new org.bson.BsonDocumentReader(written.getDocument("inner")), org.bson.codecs.DecoderContext.builder().build())
                        .getKey());

        // A codec is created per calling registry, which caches it.
        assertSame(legacy.get(UuidHolder.class), legacy.get(UuidHolder.class));
        assertSame(MongoDBBase.codecRegistry.get(UuidHolder.class), MongoDBBase.codecRegistry.get(UuidHolder.class));

        // A non-bean type (Object, as used by distinct(field, Object.class)) decodes a binary UUID with the registry's representation.
        final org.bson.codecs.configuration.CodecRegistry standard = org.bson.codecs.configuration.CodecRegistries
                .withUuidRepresentation(MongoDBBase.codecRegistry, org.bson.UuidRepresentation.STANDARD);
        final org.bson.BsonDocumentReader reader = new org.bson.BsonDocumentReader(
                new org.bson.BsonDocument("v", new org.bson.BsonBinary(key, org.bson.UuidRepresentation.STANDARD)));
        reader.readStartDocument();
        reader.readName();

        assertEquals(key, standard.get(Object.class).decode(reader, org.bson.codecs.DecoderContext.builder().build()));

        // withUuidRepresentation keeps the codec when the representation does not change.
        final org.bson.codecs.OverridableUuidRepresentationCodec<?> codec = (org.bson.codecs.OverridableUuidRepresentationCodec<?>) (Object) new MongoDBBase.GeneralCodec<>(
                UuidHolder.class);

        assertSame(codec, codec.withUuidRepresentation(org.bson.UuidRepresentation.UNSPECIFIED));
        final org.bson.codecs.Codec<?> standardCodec = codec.withUuidRepresentation(org.bson.UuidRepresentation.STANDARD);
        assertTrue(standardCodec != codec);
        assertEquals(UuidHolder.class, standardCodec.getEncoderClass());
    }

    @Test
    public void testGeneralCodecUsesTheCallingRegistryForNestedValues_coverageMB() {
        // Regression (GeneralCodecRegistry.get(Class, CodecRegistry)): a bean's codec resolved its property values through the static
        // registry, ignoring every codec override of the registry performing the lookup, not only its UuidRepresentation. A registry that
        // overrides the enum codec now writes a nested bean's enum property through the override (HEAD wrote "GREEN").
        final org.bson.codecs.Codec<Color> lowerCase = new org.bson.codecs.Codec<>() {
            @Override
            public void encode(final org.bson.BsonWriter writer, final Color value, final org.bson.codecs.EncoderContext encoderContext) {
                writer.writeString(value.name().toLowerCase());
            }

            @Override
            public Color decode(final org.bson.BsonReader reader, final org.bson.codecs.DecoderContext decoderContext) {
                return Color.valueOf(reader.readString().toUpperCase());
            }

            @Override
            public Class<Color> getEncoderClass() {
                return Color.class;
            }
        };
        final org.bson.codecs.configuration.CodecRegistry registry = org.bson.codecs.configuration.CodecRegistries
                .fromRegistries(org.bson.codecs.configuration.CodecRegistries.fromCodecs(lowerCase), MongoDBBase.codecRegistry);
        final MxPlain holder = new MxPlain();
        holder.setColor(Color.GREEN);
        final org.bson.BsonDocument written = new org.bson.BsonDocument();

        registry.get(Document.class)
                .encode(new org.bson.BsonDocumentWriter(written), new Document("holder", holder), org.bson.codecs.EncoderContext.builder().build());

        assertEquals(new org.bson.BsonString("green"), written.getDocument("holder").get("color"));

        // A null registry is the plain lookup (the cached codec); a null class is rejected either way.
        final MongoDBBase.GeneralCodecRegistry general = new MongoDBBase.GeneralCodecRegistry();

        assertSame(general.get(MxPlain.class), general.get(MxPlain.class, (org.bson.codecs.configuration.CodecRegistry) null));
        assertThrows(IllegalArgumentException.class, () -> general.get(null, registry));
        assertThrows(IllegalArgumentException.class, () -> general.get(null, (org.bson.codecs.configuration.CodecRegistry) null));
    }

    @Test
    public void testGeneralCodecWritesTheRemainingBytesOfEveryByteBufferKind_verifyMB() {
        // Read-only, direct and partly consumed buffers are written as BSON binary of their remaining bytes, without being
        // advanced (they were written as {"position": ..., "limit": ...} documents, dropping the bytes).
        final ByteBuffer readOnly = ByteBuffer.wrap(new byte[] { 4, 5 }).asReadOnlyBuffer();
        final ByteBuffer direct = ByteBuffer.allocateDirect(2);
        direct.put((byte) 6).put((byte) 7).flip();
        final ByteBuffer advanced = ByteBuffer.wrap(new byte[] { 1, 2, 3, 4 });
        advanced.position(2);
        final org.bson.BsonDocument written = new org.bson.BsonDocument();

        MongoDBBase.codecRegistry.get(Document.class)
                .encode(new org.bson.BsonDocumentWriter(written), new Document("readOnly", readOnly).append("direct", direct).append("advanced", advanced),
                        org.bson.codecs.EncoderContext.builder().build());

        assertEquals(new org.bson.BsonBinary(new byte[] { 4, 5 }), written.get("readOnly"));
        assertEquals(new org.bson.BsonBinary(new byte[] { 6, 7 }), written.get("direct"));
        assertEquals(new org.bson.BsonBinary(new byte[] { 3, 4 }), written.get("advanced"));
        assertEquals(2, advanced.position());
        assertEquals(0, direct.position());

        // The ByteBuffer codec reads a binary value back as a readable buffer.
        final org.bson.BsonDocumentReader reader = new org.bson.BsonDocumentReader(
                new org.bson.BsonDocument("v", new org.bson.BsonBinary(new byte[] { 9, 8 })));
        reader.readStartDocument();
        reader.readName();

        assertEquals(ByteBuffer.wrap(new byte[] { 9, 8 }),
                MongoDBBase.codecRegistry.get(ByteBuffer.class).decode(reader, org.bson.codecs.DecoderContext.builder().build()));
    }

    @Test
    public void testGregorianCalendarIsWrittenAsTextAndReadBack_coverageMB() {
        // Regression (GeneralCodec isEntityClass), proven on its own (the sliceH test stops at its ByteBuffer assertion on HEAD): a
        // GregorianCalendar has bean accessors, so it was written as a document of them ({"timeZone": ..., "gregorianChange": ...}),
        // losing the time itself.
        final java.util.GregorianCalendar calendar = new java.util.GregorianCalendar();
        calendar.setTimeInMillis(1700000000000L);
        final org.bson.BsonDocument written = new org.bson.BsonDocument();

        MongoDBBase.codecRegistry.get(Document.class)
                .encode(new org.bson.BsonDocumentWriter(written), new Document("calendar", calendar), org.bson.codecs.EncoderContext.builder().build());

        assertTrue(written.get("calendar").isString(), written.toJson());

        // Its codec reads the text back (a typed read such as distinct(field, GregorianCalendar.class)), and so does toEntity.
        final org.bson.BsonDocumentReader reader = new org.bson.BsonDocumentReader(written);
        reader.readStartDocument();
        reader.readName();

        assertEquals(1700000000000L,
                MongoDBBase.codecRegistry.get(java.util.GregorianCalendar.class)
                        .decode(reader, org.bson.codecs.DecoderContext.builder().build())
                        .getTimeInMillis());

        final Document read = MongoDBBase.codecRegistry.get(Document.class)
                .decode(new org.bson.BsonDocumentReader(written), org.bson.codecs.DecoderContext.builder().build());

        assertEquals(1700000000000L, MongoDBBase.toEntity(read, ValueTypeEntity.class).getCalendar().getTimeInMillis());
    }

    @Test
    public void testJavaTimeLocalTypesAreReadInUtcLikeTheDriverCodecsWriteThem() {
        // Regression: the driver's LocalDate/LocalDateTime/LocalTime codecs write a UTC date-time, but the read side converted
        // the decoded java.util.Date with the JVM's default zone, so west of UTC a LocalDate came back as the previous day
        // (and LocalDateTime/LocalTime shifted by the offset). Fails on any JVM whose default zone is not UTC.
        final LocalDate day = LocalDate.of(2024, 1, 2);
        final LocalDateTime dateTime = LocalDateTime.of(2024, 1, 2, 10, 30);
        final LocalTime time = LocalTime.of(10, 30);
        final Date dayDate = new Date(day.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());
        final Date dateTimeDate = new Date(dateTime.toInstant(ZoneOffset.UTC).toEpochMilli());
        final Date timeDate = new Date(time.atDate(LocalDate.ofEpochDay(0)).toInstant(ZoneOffset.UTC).toEpochMilli());

        final TimeEntity entity = MongoDBBase.toEntity(
                new Document("day", dayDate).append("dateTime", dateTimeDate).append("time", timeDate).append("days", new ArrayList<>(Arrays.asList(dayDate))),
                TimeEntity.class);

        assertEquals(day, entity.getDay());
        assertEquals(dateTime, entity.getDateTime());
        assertEquals(time, entity.getTime());
        assertEquals(Arrays.asList(day), entity.getDays());

        // Scalar reads (readRow / queryForSingleValue / toList projections) use the same conversion.
        assertEquals(day, MongoDBBase.convertBsonValue(dayDate, LocalDate.class));
        assertEquals(dateTime, MongoDBBase.readRow(new Document("_id", 1).append("dateTime", dateTimeDate), LocalDateTime.class));
        assertEquals(time, MongoDBBase.convertBsonValue(timeDate, LocalTime.class));
    }

    @Test
    public void testNestedBeanUuidFollowsTheCallingRegistryUuidRepresentation() {
        // Regression: GeneralCodec resolved a bean's property codecs through the static registry, ignoring the UuidRepresentation
        // a collection's registry applies (MongoClientSettings.uuidRepresentation). Inserting an entity whose NESTED bean held a
        // UUID failed with CodecConfigurationException ("The uuidRepresentation has not been specified") although a top-level
        // UUID was written fine (live-probed against mongod with a STANDARD client).
        final org.bson.codecs.configuration.CodecRegistry registry = org.bson.codecs.configuration.CodecRegistries
                .withUuidRepresentation(MongoDBBase.codecRegistry, org.bson.UuidRepresentation.STANDARD);
        final java.util.UUID key = java.util.UUID.fromString("01234567-89ab-cdef-0123-456789abcdef");
        final UuidHolder inner = new UuidHolder();
        inner.setKey(key);
        final org.bson.BsonDocument written = new org.bson.BsonDocument();

        registry.get(Document.class)
                .encode(new org.bson.BsonDocumentWriter(written), new Document("inner", inner), org.bson.codecs.EncoderContext.builder().build());

        assertEquals(new org.bson.BsonBinary(key, org.bson.UuidRepresentation.STANDARD), written.getDocument("inner").get("key"));

        // The bean codec handed out by that registry also reads the binary UUID back as a UUID.
        final UuidHolder decoded = registry.get(UuidHolder.class)
                .decode(new org.bson.BsonDocumentReader(written.getDocument("inner")), org.bson.codecs.DecoderContext.builder().build());

        assertEquals(key, decoded.getKey());
    }

    @Test
    public void testRecordIdComponentFollowsIdAnnotationsColumnNamesAndIdTypes_coverageMB() {
        // _id fills a record's id component by the same rules as a mutable bean's id setter: an @Id-annotated or @Column-named component,
        // a non-ObjectId _id as its text; a document's own id field (also by its column name) takes precedence; a record without a String/
        // ObjectId id component ignores _id, like a mutable bean with an int id (pins). Each case on its own.
        final ObjectId id = new ObjectId("507f1f77bcf86cd799439011");

        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertEquals("507f1f77bcf86cd799439011", MongoDBBase.toEntity(new Document("_id", id).append("name", "n"), KeyRecord.class).key()),
                () -> assertEquals("5", MongoDBBase.toEntity(new Document("_id", 5).append("name", "n"), IdRecord.class).id()),
                () -> assertEquals("5", MongoDBBase.toEntity(new Document("_id", 5).append("name", "n"), TestEntity.class).getId()),
                () -> assertEquals("507f1f77bcf86cd799439011", MongoDBBase.toEntity(new Document("_id", id).append("name", "n"), ColumnIdRecord.class).id()),
                () -> assertEquals("own", MongoDBBase.toEntity(new Document("_id", id).append("doc_id", "own"), ColumnIdRecord.class).id()), () -> {
                    final LongIdRecord record = MongoDBBase.toEntity(new Document("_id", id).append("name", "n"), LongIdRecord.class);

                    assertEquals(0L, record.id());
                    assertEquals("n", record.name());
                }, () -> assertNull(MongoDBBase.toEntity(new Document("_id", null).append("name", "n"), IdRecord.class).id()));
    }

    @Test
    public void testToBSONObjectEmptyVarargsYieldsEmpty() {
        BasicBSONObject obj = MongoDBBase.toBSONObject();
        assertTrue(obj.isEmpty());
    }

    // -- toBson convenience method (delegates to toDocument) --

    @Test
    public void testToBsonObjectDelegatesToDocument() {
        Bson result = MongoDBBase.toBson("k", "v");
        assertNotNull(result);
        Document d = (Document) result;
        assertEquals("v", d.getString("k"));
    }

    @Test
    public void testToBSONObjectWithNonStringNameThrowsIAE() {
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toBSONObject(30, "x"));
    }

    @Test
    public void testToBSONObjectWithOddVarargsThrows() {
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toBSONObject("only-name", 1, "extra"));
    }

    @Test
    public void testToBSONObjectWithUnsupportedThrows() {
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toBSONObject(new Object()));
    }

    @Test
    public void testToDBObjectEmptyVarargsYieldsEmpty() {
        BasicDBObject obj = MongoDBBase.toDBObject();
        assertTrue(obj.isEmpty());
    }

    @Test
    public void testToDBObjectWithNonStringNameThrowsIAE() {
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toDBObject(30, "x"));
    }

    @Test
    public void testToDBObjectWithOddVarargsThrows() {
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toDBObject("only-name", 1, "extra"));
    }

    @Test
    public void testToDBObjectWithUnsupportedThrows() {
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toDBObject(new Object()));
    }

    // -- toDocument/toBSONObject/toDBObject varargs branches --

    @Test
    public void testToDocumentEmptyVarargsYieldsEmptyDoc() {
        Document doc = MongoDBBase.toDocument();
        assertTrue(doc.isEmpty());
    }

    // -- name-value pair varargs with a non-String NAME slot --

    @Test
    public void testToDocumentWithNonStringNameThrowsIAE() {
        // The NAME slot of a name/value pair must be a String. Previously this surfaced as a raw
        // ClassCastException; it is now rejected with the documented IllegalArgumentException.
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toDocument(30, "x"));
    }

    @Test
    public void testToJsonBasicDBObjectAcceptsNull() {
        // N.toJson(null) returns an empty string; this overload performs no null check of its own.
        assertEquals("", MongoDBBase.toJson((BasicDBObject) null));
    }

    // -- 2026-09-22 review (slice H) regressions --

    @Test
    public void testToJsonDriverBuiltBsonRendersPlainValues() {
        // Regression: a non-Map Bson (Filters/Updates/Sorts/Projections) was converted to a BsonDocument and handed to
        // N.toJson as-is, which serialized every BsonValue as a bean: Filters.eq("a", 1) -> {"a": {"value": 1}}.
        assertEquals(MongoDBBase.toJson(new Document("a", 1)), MongoDBBase.toJson(com.mongodb.client.model.Filters.eq("a", 1)));
        assertEquals(MongoDBBase.toJson(new Document("name", "John")), MongoDBBase.toJson(com.mongodb.client.model.Filters.eq("name", "John")));
        assertEquals(MongoDBBase.toJson(new Document("age", new Document("$gt", 5L))), MongoDBBase.toJson(com.mongodb.client.model.Filters.gt("age", 5L)));

        // A BsonDocument is itself a Map (of BsonValues) and must be rendered by value too.
        final org.bson.BsonDocument bsonDoc = new org.bson.BsonDocument("a", new org.bson.BsonInt32(1)).append("s", new org.bson.BsonString("x"));
        assertEquals(MongoDBBase.toJson(new Document("a", 1).append("s", "x")), MongoDBBase.toJson(bsonDoc));
    }

    // -- toJson Bson Map branch --

    @Test
    public void testToJsonWithBsonMapBranch() {
        // Document is a Map, so toJson(Bson) takes the Map branch
        Document doc = new Document("k", "v");
        String json = MongoDBBase.toJson(doc);
        assertNotNull(json);
        assertTrue(json.contains("\"k\""));
    }

    @Test
    public void testTypedBinaryArraySourcesPopulateCollectionsWithoutChangingSource() {
        final byte[] bytes = { 7, 8, 9 };
        final Binary binary = new Binary(bytes);
        final ByteBuffer buffer = ByteBuffer.wrap(new byte[] { 0, 7, 8, 9, 0 }).asReadOnlyBuffer();
        buffer.position(1);
        buffer.limit(4);

        for (final Object[] values : Arrays.asList(new Binary[] { binary, null }, new byte[][] { bytes, null }, new ByteBuffer[] { buffer, null },
                new Object[] { bytes, binary, buffer, null })) {
            final Document row = new Document("buffers", values).append("byteArrays", values).append("rawArray", values);
            final BinaryEntity entity = MongoDBBase.toEntity(row, BinaryEntity.class);

            assertEquals(values.length, entity.getBuffers().size());
            assertEquals(values.length, entity.getByteArrays().size());
            for (int i = 0; i < values.length - 1; i++) {
                assertEquals(ByteBuffer.wrap(bytes), entity.getBuffers().get(i));
                assertArrayEquals(bytes, entity.getByteArrays().get(i));
            }
            assertNull(entity.getBuffers().get(values.length - 1));
            assertNull(entity.getByteArrays().get(values.length - 1));
            assertSame(values, row.get("buffers"));
            assertSame(values, row.get("byteArrays"));
            assertSame(values, entity.getRawArray());
            assertEquals(1, buffer.position());
            assertEquals(4, buffer.limit());
        }
    }

    private static <T> List<T> decodeArrayWith(final org.bson.BsonArray values, final Class<T> cls) {
        final org.bson.codecs.Codec<T> codec = MongoDBBase.codecRegistry.get(cls);
        final org.bson.BsonDocumentReader reader = new org.bson.BsonDocumentReader(new org.bson.BsonDocument("values", values));
        final List<T> result = new ArrayList<>();

        reader.readStartDocument();
        reader.readName("values");
        reader.readStartArray();

        while (reader.readBsonType() != org.bson.BsonType.END_OF_DOCUMENT) {
            result.add(codec.decode(reader, org.bson.codecs.DecoderContext.builder().build()));
        }

        reader.readEndArray();
        reader.readEndDocument();

        return result;
    }

    public static class ValueTypeEntity {
        private ByteBuffer buffer;
        private java.util.GregorianCalendar calendar;

        public ByteBuffer getBuffer() {
            return buffer;
        }

        public void setBuffer(final ByteBuffer buffer) {
            this.buffer = buffer;
        }

        public java.util.GregorianCalendar getCalendar() {
            return calendar;
        }

        public void setCalendar(final java.util.GregorianCalendar calendar) {
            this.calendar = calendar;
        }
    }

    public static class UuidHolder {
        private java.util.UUID key;

        public java.util.UUID getKey() {
            return key;
        }

        public void setKey(final java.util.UUID key) {
            this.key = key;
        }
    }

    public enum Color {
        RED, GREEN
    }

    public abstract static class AbstractShape {
        private String name;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    public static class SpecialContainerEntity {
        private java.util.EnumSet<Color> colorSet;
        private List<AbstractShape> shapes;

        public java.util.EnumSet<Color> getColorSet() {
            return colorSet;
        }

        public void setColorSet(final java.util.EnumSet<Color> colorSet) {
            this.colorSet = colorSet;
        }

        public List<AbstractShape> getShapes() {
            return shapes;
        }

        public void setShapes(final List<AbstractShape> shapes) {
            this.shapes = shapes;
        }
    }

    public static class ChildEntity {
        private String id;
        private String name;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    public static class ContainerEntity {
        private List<ChildEntity> children;
        private java.util.Set<ChildEntity> childSet;
        private Map<String, ChildEntity> childMap;
        private List<List<ChildEntity>> nested;
        private ChildEntity[] childArray;
        private List<Long> longs;
        private List<Color> colors;
        private List<java.math.BigDecimal> decimals;
        private Map<String, Long> countByName;
        private List<String> names;

        public List<ChildEntity> getChildren() {
            return children;
        }

        public void setChildren(final List<ChildEntity> children) {
            this.children = children;
        }

        public java.util.Set<ChildEntity> getChildSet() {
            return childSet;
        }

        public void setChildSet(final java.util.Set<ChildEntity> childSet) {
            this.childSet = childSet;
        }

        public Map<String, ChildEntity> getChildMap() {
            return childMap;
        }

        public void setChildMap(final Map<String, ChildEntity> childMap) {
            this.childMap = childMap;
        }

        public List<List<ChildEntity>> getNested() {
            return nested;
        }

        public void setNested(final List<List<ChildEntity>> nested) {
            this.nested = nested;
        }

        public ChildEntity[] getChildArray() {
            return childArray;
        }

        public void setChildArray(final ChildEntity[] childArray) {
            this.childArray = childArray;
        }

        public List<Long> getLongs() {
            return longs;
        }

        public void setLongs(final List<Long> longs) {
            this.longs = longs;
        }

        public List<Color> getColors() {
            return colors;
        }

        public void setColors(final List<Color> colors) {
            this.colors = colors;
        }

        public List<java.math.BigDecimal> getDecimals() {
            return decimals;
        }

        public void setDecimals(final List<java.math.BigDecimal> decimals) {
            this.decimals = decimals;
        }

        public Map<String, Long> getCountByName() {
            return countByName;
        }

        public void setCountByName(final Map<String, Long> countByName) {
            this.countByName = countByName;
        }

        public List<String> getNames() {
            return names;
        }

        public void setNames(final List<String> names) {
            this.names = names;
        }
    }

    public static class TimeEntity {
        private LocalDate day;
        private LocalDateTime dateTime;
        private LocalTime time;
        private List<LocalDate> days;

        public LocalDate getDay() {
            return day;
        }

        public void setDay(final LocalDate day) {
            this.day = day;
        }

        public LocalDateTime getDateTime() {
            return dateTime;
        }

        public void setDateTime(final LocalDateTime dateTime) {
            this.dateTime = dateTime;
        }

        public LocalTime getTime() {
            return time;
        }

        public void setTime(final LocalTime time) {
            this.time = time;
        }

        public List<LocalDate> getDays() {
            return days;
        }

        public void setDays(final List<LocalDate> days) {
            this.days = days;
        }
    }

    public static class ArrayElementEntity {
        private Integer[][] matrix;
        private List<String[]> rows;
        private List<int[]> ints;
        private Map<Integer, byte[]> blobsById;
        private Map<Integer, ByteBuffer> buffersById;

        public Integer[][] getMatrix() {
            return matrix;
        }

        public void setMatrix(final Integer[][] matrix) {
            this.matrix = matrix;
        }

        public List<String[]> getRows() {
            return rows;
        }

        public void setRows(final List<String[]> rows) {
            this.rows = rows;
        }

        public List<int[]> getInts() {
            return ints;
        }

        public void setInts(final List<int[]> ints) {
            this.ints = ints;
        }

        public Map<Integer, byte[]> getBlobsById() {
            return blobsById;
        }

        public void setBlobsById(final Map<Integer, byte[]> blobsById) {
            this.blobsById = blobsById;
        }

        public Map<Integer, ByteBuffer> getBuffersById() {
            return buffersById;
        }

        public void setBuffersById(final Map<Integer, ByteBuffer> buffersById) {
            this.buffersById = buffersById;
        }
    }

    public static class ReadOnlyContainerEntity {
        private String name;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public List<Long> getComputed() {
            return Arrays.asList(1L);
        }
    }

    public static class ColumnNamedEntity {
        @com.landawn.abacus.annotation.Column("child_nums")
        private List<Long> childNums;

        public List<Long> getChildNums() {
            return childNums;
        }

        public void setChildNums(final List<Long> childNums) {
            this.childNums = childNums;
        }
    }

    public static class TreeNode {
        private String name;
        private List<TreeNode> children;
        private Map<String, TreeNode> byName;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public List<TreeNode> getChildren() {
            return children;
        }

        public void setChildren(final List<TreeNode> children) {
            this.children = children;
        }

        public Map<String, TreeNode> getByName() {
            return byName;
        }

        public void setByName(final Map<String, TreeNode> byName) {
            this.byName = byName;
        }
    }

    public record DayRecord(String name, List<Long> nums, LocalDate day) {
    }

    public static class InstantEntity {
        private Date date;
        private java.time.Instant instant;
        private java.time.ZonedDateTime zoned;
        private java.time.OffsetDateTime offset;
        private java.sql.Timestamp timestamp;
        private Map<String, LocalDate> dayByName;

        public Date getDate() {
            return date;
        }

        public void setDate(final Date date) {
            this.date = date;
        }

        public java.time.Instant getInstant() {
            return instant;
        }

        public void setInstant(final java.time.Instant instant) {
            this.instant = instant;
        }

        public java.time.ZonedDateTime getZoned() {
            return zoned;
        }

        public void setZoned(final java.time.ZonedDateTime zoned) {
            this.zoned = zoned;
        }

        public java.time.OffsetDateTime getOffset() {
            return offset;
        }

        public void setOffset(final java.time.OffsetDateTime offset) {
            this.offset = offset;
        }

        public java.sql.Timestamp getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(final java.sql.Timestamp timestamp) {
            this.timestamp = timestamp;
        }

        public Map<String, LocalDate> getDayByName() {
            return dayByName;
        }

        public void setDayByName(final Map<String, LocalDate> dayByName) {
            this.dayByName = dayByName;
        }
    }

    // ---- end 2026-10-02 verifyMB ----

    // ---- 2026-10-03 fixMB ----

    private static Document box(final Object value) {
        return new Document("value", value);
    }

    private static List<Object> list(final Object... values) {
        return new ArrayList<>(Arrays.asList(values));
    }

    public static class Box<T> {
        private T value;

        public T getValue() {
            return value;
        }

        public void setValue(final T value) {
            this.value = value;
        }
    }

    public static class LongBox extends Box<Long> {
    }

    public static class Pair<K, V> {
        private K key;
        private V val;

        public K getKey() {
            return key;
        }

        public void setKey(final K key) {
            this.key = key;
        }

        public V getVal() {
            return val;
        }

        public void setVal(final V val) {
            this.val = val;
        }
    }

    public static class Page<T> {
        private List<T> items;
        private Map<String, T> byKey;
        private String name;

        public List<T> getItems() {
            return items;
        }

        public void setItems(final List<T> items) {
            this.items = items;
        }

        public Map<String, T> getByKey() {
            return byKey;
        }

        public void setByKey(final Map<String, T> byKey) {
            this.byKey = byKey;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public T getComputed() {
            return null;
        }
    }

    public record BoxRecord<T>(String name, T value) {
    }

    @SuppressWarnings("rawtypes")
    public static class GenericBoxEntity {
        private List<Box<Long>> boxes;
        private Map<String, Box<Long>> boxByName;
        private Box<Long>[] boxArray;
        private java.util.Set<Box<Long>> boxSet;
        private List<Box<List<Long>>> listBoxes;
        private List<Box<Box<Long>>> boxedBoxes;
        private List<Box<ChildEntity>> childBoxes;
        private List<Page<Long>> pages;
        private List<Pair<String, Long>> pairs;
        private Box<Long> box;
        private Box<ChildEntity> childBox;
        private LongBox longBox;
        private List<LongBox> longBoxes;
        private List<BoxRecord<Long>> records;
        private List<Box> rawBoxes;
        private List<Box<?>> wildcardBoxes;

        public List<Box<Long>> getBoxes() {
            return boxes;
        }

        public void setBoxes(final List<Box<Long>> boxes) {
            this.boxes = boxes;
        }

        public Map<String, Box<Long>> getBoxByName() {
            return boxByName;
        }

        public void setBoxByName(final Map<String, Box<Long>> boxByName) {
            this.boxByName = boxByName;
        }

        public Box<Long>[] getBoxArray() {
            return boxArray;
        }

        public void setBoxArray(final Box<Long>[] boxArray) {
            this.boxArray = boxArray;
        }

        public java.util.Set<Box<Long>> getBoxSet() {
            return boxSet;
        }

        public void setBoxSet(final java.util.Set<Box<Long>> boxSet) {
            this.boxSet = boxSet;
        }

        public List<Box<List<Long>>> getListBoxes() {
            return listBoxes;
        }

        public void setListBoxes(final List<Box<List<Long>>> listBoxes) {
            this.listBoxes = listBoxes;
        }

        public List<Box<Box<Long>>> getBoxedBoxes() {
            return boxedBoxes;
        }

        public void setBoxedBoxes(final List<Box<Box<Long>>> boxedBoxes) {
            this.boxedBoxes = boxedBoxes;
        }

        public List<Box<ChildEntity>> getChildBoxes() {
            return childBoxes;
        }

        public void setChildBoxes(final List<Box<ChildEntity>> childBoxes) {
            this.childBoxes = childBoxes;
        }

        public List<Page<Long>> getPages() {
            return pages;
        }

        public void setPages(final List<Page<Long>> pages) {
            this.pages = pages;
        }

        public List<Pair<String, Long>> getPairs() {
            return pairs;
        }

        public void setPairs(final List<Pair<String, Long>> pairs) {
            this.pairs = pairs;
        }

        public Box<Long> getBox() {
            return box;
        }

        public void setBox(final Box<Long> box) {
            this.box = box;
        }

        public Box<ChildEntity> getChildBox() {
            return childBox;
        }

        public void setChildBox(final Box<ChildEntity> childBox) {
            this.childBox = childBox;
        }

        public LongBox getLongBox() {
            return longBox;
        }

        public void setLongBox(final LongBox longBox) {
            this.longBox = longBox;
        }

        public List<LongBox> getLongBoxes() {
            return longBoxes;
        }

        public void setLongBoxes(final List<LongBox> longBoxes) {
            this.longBoxes = longBoxes;
        }

        public List<BoxRecord<Long>> getRecords() {
            return records;
        }

        public void setRecords(final List<BoxRecord<Long>> records) {
            this.records = records;
        }

        public List<Box> getRawBoxes() {
            return rawBoxes;
        }

        public void setRawBoxes(final List<Box> rawBoxes) {
            this.rawBoxes = rawBoxes;
        }

        public List<Box<?>> getWildcardBoxes() {
            return wildcardBoxes;
        }

        public void setWildcardBoxes(final List<Box<?>> wildcardBoxes) {
            this.wildcardBoxes = wildcardBoxes;
        }
    }

    public static class GenericBsonEntity {
        private List<Box<String>> strings;
        private List<Box<ObjectId>> ids;
        private List<Box<LocalDate>> days;
        private List<Box<java.math.BigDecimal>> decimals;
        private List<Box<byte[]>> blobs;
        private Box<ByteBuffer> buffer;
        private List<Box<Date>> dates;

        public List<Box<String>> getStrings() {
            return strings;
        }

        public void setStrings(final List<Box<String>> strings) {
            this.strings = strings;
        }

        public List<Box<ObjectId>> getIds() {
            return ids;
        }

        public void setIds(final List<Box<ObjectId>> ids) {
            this.ids = ids;
        }

        public List<Box<LocalDate>> getDays() {
            return days;
        }

        public void setDays(final List<Box<LocalDate>> days) {
            this.days = days;
        }

        public List<Box<java.math.BigDecimal>> getDecimals() {
            return decimals;
        }

        public void setDecimals(final List<Box<java.math.BigDecimal>> decimals) {
            this.decimals = decimals;
        }

        public List<Box<byte[]>> getBlobs() {
            return blobs;
        }

        public void setBlobs(final List<Box<byte[]>> blobs) {
            this.blobs = blobs;
        }

        public Box<ByteBuffer> getBuffer() {
            return buffer;
        }

        public void setBuffer(final Box<ByteBuffer> buffer) {
            this.buffer = buffer;
        }

        public List<Box<Date>> getDates() {
            return dates;
        }

        public void setDates(final List<Box<Date>> dates) {
            this.dates = dates;
        }
    }

    public record ScalarRecord(long primitiveLong, Long boxedLong, double ratio, short small, LocalDate day, java.time.Instant at, Color color,
            java.math.BigDecimal amount, byte[] blob, String text) {
    }

    public record InnerRecord(String name, Long n) {
    }

    public record OuterRecord(String title, InnerRecord inner, List<InnerRecord> inners, Map<String, InnerRecord> byName, BoxRecord<Long> box,
            List<BoxRecord<Long>> boxes) {
    }

    public record IdRecord(String id, String name) {
    }

    public record ObjectIdRecord(ObjectId id, String name) {
    }

    public static class RecordHolderEntity {
        private InnerRecord inner;
        private List<InnerRecord> inners;

        public InnerRecord getInner() {
            return inner;
        }

        public void setInner(final InnerRecord inner) {
            this.inner = inner;
        }

        public List<InnerRecord> getInners() {
            return inners;
        }

        public void setInners(final List<InnerRecord> inners) {
            this.inners = inners;
        }
    }

    // ---- end 2026-10-03 fixMB ----

    // ---- 2026-10-04 coverageMB ----

    // Conversion matrix: every decoded BSON value kind (int32, int64, double, decimal128, objectId, string, boolean, date, binary,
    // embedded document, array) read by toEntity into a property of its declared type, at every position the decoded-value passes
    // reach (plain / nested bean / container element / generic Box<X> / class binding X / record component). Each cell asserts the
    // exact Java type and value the property receives; all cells run and are reported together, so a failing cell never hides another.
    // (The day/dt/tm cells only show the former JVM-zone reading on a JVM whose default zone is not UTC.)

    /** A decoded BSON value (a fresh instance per read) and the exact Java value a property of the kind's declared type receives. */
    private record MxKind(String name, java.util.function.Supplier<Object> stored, Object expected) {
    }

    /** Where the kind's property sits: the row type read, the document storing the value there, and how to read the value back. */
    private record MxPosition(String name, boolean primitiveAllowed, java.util.function.Function<String, Class<?>> rowType,
            java.util.function.BiFunction<String, Object, Document> document, java.util.function.BiFunction<Object, String, Object> read) {
    }

    private static List<MxKind> mxKinds() {
        final ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
        final long dayMillis = LocalDate.of(2024, 1, 2).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        final long dateTimeMillis = LocalDateTime.of(2024, 1, 2, 10, 30).toInstant(ZoneOffset.UTC).toEpochMilli();
        final long timeMillis = LocalTime.of(10, 30).toSecondOfDay() * 1000L;

        // Field name / declared type: lng Long, intg Integer, dbl Double, flt Float, dec BigDecimal, str String, txt String, oid ObjectId,
        // bool Boolean, day LocalDate, dt LocalDateTime, tm LocalTime, inst Instant, date Date, blob byte[], buf ByteBuffer, color Color,
        // child MxChild, longs List<Long>, counts Map<String, Long>, prim long.
        return Arrays.asList(new MxKind("lng", () -> 5, 5L), // int32
                new MxKind("intg", () -> 7L, 7), // int64
                new MxKind("dbl", () -> 3, 3.0d), // int32
                new MxKind("flt", () -> 2.5d, 2.5f), // double
                new MxKind("dec", () -> org.bson.types.Decimal128.parse("1.10"), new java.math.BigDecimal("1.10")), // decimal128, scale kept
                new MxKind("str", () -> id, "507f1f77bcf86cd799439011"), // objectId as its hex string
                new MxKind("txt", () -> "t", "t"), // string, kept
                new MxKind("oid", () -> id, id), // objectId, kept
                new MxKind("bool", () -> Boolean.TRUE, Boolean.TRUE), // boolean, kept
                new MxKind("day", () -> new Date(dayMillis), LocalDate.of(2024, 1, 2)), // date, read in UTC
                new MxKind("dt", () -> new Date(dateTimeMillis), LocalDateTime.of(2024, 1, 2, 10, 30)), // date, read in UTC
                new MxKind("tm", () -> new Date(timeMillis), LocalTime.of(10, 30)), // date, read in UTC
                new MxKind("inst", () -> new Date(dateTimeMillis), java.time.Instant.ofEpochMilli(dateTimeMillis)), // date, same instant
                new MxKind("date", () -> new Date(dateTimeMillis), new Date(dateTimeMillis)), // date, kept
                new MxKind("blob", () -> new Binary(new byte[] { 1, 2 }), new byte[] { 1, 2 }), // binary
                new MxKind("buf", () -> new Binary(new byte[] { 3 }), ByteBuffer.wrap(new byte[] { 3 })), // binary
                new MxKind("color", () -> "GREEN", Color.GREEN), // string into an enum
                new MxKind("child", () -> new Document("name", "c").append("n", 1), mxChild("c", 1L)), // embedded document into a bean
                new MxKind("longs", () -> list(1, 2L), Arrays.asList(1L, 2L)), // array of int32/int64
                new MxKind("counts", () -> new Document("a", 1), Collections.singletonMap("a", 1L)), // embedded document into a typed map
                new MxKind("prim", () -> 6, 6L)); // int32 into a primitive long
    }

    private static List<MxPosition> mxPositions() {
        return Arrays.asList(new MxPosition("plain property", true, k -> MxPlain.class, (k, v) -> new Document(k, v), MongoDBBaseTest::mxProp),
                new MxPosition("property of a nested bean", true, k -> MxNested.class, (k, v) -> new Document("plain", new Document(k, v)),
                        (e, k) -> mxProp(mxProp(e, "plain"), k)),
                new MxPosition("List<X> element", false, k -> MxLists.class, (k, v) -> new Document(k, list(v)), (e, k) -> ((List<?>) mxProp(e, k)).get(0)),
                new MxPosition("Map<String, X> value", false, k -> MxMaps.class, (k, v) -> new Document(k, new Document("key", v)),
                        (e, k) -> ((Map<?, ?>) mxProp(e, k)).get("key")),
                new MxPosition("X[] element", false, k -> MxArrays.class, (k, v) -> new Document(k, list(v)), (e, k) -> ((Object[]) mxProp(e, k))[0]),
                new MxPosition("Box<X> property", false, k -> MxBoxes.class, (k, v) -> new Document(k, box(v)), (e, k) -> mxProp(mxProp(e, k), "value")),
                new MxPosition("List<Box<X>> element", false, k -> MxBoxLists.class, (k, v) -> new Document(k, list(box(v))),
                        (e, k) -> mxProp(((List<?>) mxProp(e, k)).get(0), "value")),
                new MxPosition("XBox extends Box<X> property", false, k -> MxBounds.class, (k, v) -> new Document(k, box(v)),
                        (e, k) -> mxProp(mxProp(e, k), "value")),
                new MxPosition("XBox extends Box<X> row type", false, k -> mxFieldType(MxBounds.class, k), (k, v) -> box(v), (e, k) -> mxProp(e, "value")),
                new MxPosition("record component", true, k -> MxRecord.class, (k, v) -> new Document(k, v), MongoDBBaseTest::mxProp),
                new MxPosition("record property of a bean", true, k -> MxNested.class, (k, v) -> new Document("record", new Document(k, v)),
                        (e, k) -> mxProp(mxProp(e, "record"), k)),
                new MxPosition("List<record> element", true, k -> MxNested.class, (k, v) -> new Document("records", list(new Document(k, v))),
                        (e, k) -> mxProp(((List<?>) mxProp(e, "records")).get(0), k)));
    }

    private static Object mxProp(final Object bean, final String propName) {
        return bean == null ? null : com.landawn.abacus.util.Beans.getPropValue(bean, propName);
    }

    private static Class<?> mxFieldType(final Class<?> cls, final String fieldName) {
        try {
            return cls.getDeclaredField(fieldName).getType();
        } catch (final NoSuchFieldException e) {
            throw new IllegalStateException(e);
        }
    }

    private static MxChild mxChild(final String name, final Long n) {
        final MxChild child = new MxChild();
        child.setName(name);
        child.setN(n);
        return child;
    }

    /** Renders a value with the exact class of every scalar in it, so a cell compares types as well as values. */
    private static String mxDescribe(final Object value) {
        if (value == null) {
            return "null";
        } else if (value instanceof final byte[] bytes) {
            return "byte[]" + Arrays.toString(bytes);
        } else if (value instanceof final ByteBuffer buffer) {
            final byte[] bytes = new byte[buffer.remaining()];
            buffer.duplicate().get(bytes);
            return "ByteBuffer" + Arrays.toString(bytes);
        } else if (value instanceof final MxChild child) {
            return "MxChild(" + mxDescribe(child.getName()) + ", " + mxDescribe(child.getN()) + ")";
        } else if (value instanceof final java.util.Collection<?> values) {
            return "Collection" + values.stream().map(MongoDBBaseTest::mxDescribe).toList();
        } else if (value instanceof final Map<?, ?> values) {
            final List<String> entries = new ArrayList<>();
            values.forEach((k, v) -> entries.add(mxDescribe(k) + "=" + mxDescribe(v)));
            return "Map" + entries;
        } else if (value instanceof final Object[] values) {
            return "Object[]" + Arrays.stream(values).map(MongoDBBaseTest::mxDescribe).toList();
        }

        return value.getClass().getSimpleName() + ":" + value;
    }

    private static <T> T mxDecode(final Class<T> cls, final org.bson.BsonValue value) {
        final org.bson.BsonDocumentReader reader = new org.bson.BsonDocumentReader(new org.bson.BsonDocument("v", value));
        reader.readStartDocument();
        reader.readName();

        return new MongoDBBase.GeneralCodec<>(cls).decode(reader, org.bson.codecs.DecoderContext.builder().build());
    }

    @SuppressWarnings("unchecked")
    private static MongoCursor<Document> mxCursor(final Document... rows) {
        final MongoCursor<Document> cursor = org.mockito.Mockito.mock(MongoCursor.class);
        final java.util.Iterator<Document> iterator = Arrays.asList(rows).iterator();
        when(cursor.hasNext()).thenAnswer(invocation -> iterator.hasNext());
        when(cursor.next()).thenAnswer(invocation -> iterator.next());

        return cursor;
    }

    @lombok.Data
    public static class SortedContainerEntity {
        private java.util.SortedSet<Long> sortedLongs;
        private java.util.Deque<Long> longDeque;
        private java.util.Queue<ChildEntity> childQueue;
        private java.util.SortedMap<String, Long> sortedCounts;
        private TreeMap<String, ChildEntity> childTree;
    }

    /** A generic bean whose property has no field: only its accessors declare the type variable. */
    public static class SetterBox<T> {
        private Object stored;

        @SuppressWarnings("unchecked")
        public T getValue() {
            return (T) stored;
        }

        public void setValue(final T value) {
            this.stored = value;
        }
    }

    public static class LongSetterBox extends SetterBox<Long> {
    }

    public static class LongPage extends Page<Long> {
    }

    @lombok.Data
    public static class BoundEntity {
        private LongSetterBox setterBox;
        private List<LongSetterBox> setterBoxes;
        private List<LongPage> pages;
    }

    public record KeyRecord(@com.landawn.abacus.annotation.Id String key, String name) {
    }

    public record ColumnIdRecord(@com.landawn.abacus.annotation.Column("doc_id") String id, String name) {
    }

    public record LongIdRecord(long id, String name) {
    }

    @lombok.Data
    public static class MxChild {
        private String name;
        private Long n;
    }

    @lombok.Data
    public static class MxPlain {
        private Long lng;
        private Integer intg;
        private Double dbl;
        private Float flt;
        private java.math.BigDecimal dec;
        private String str;
        private String txt;
        private ObjectId oid;
        private Boolean bool;
        private LocalDate day;
        private LocalDateTime dt;
        private LocalTime tm;
        private java.time.Instant inst;
        private Date date;
        private byte[] blob;
        private ByteBuffer buf;
        private Color color;
        private MxChild child;
        private List<Long> longs;
        private Map<String, Long> counts;
        private long prim;
    }

    @lombok.Data
    public static class MxLists {
        private List<Long> lng;
        private List<Integer> intg;
        private List<Double> dbl;
        private List<Float> flt;
        private List<java.math.BigDecimal> dec;
        private List<String> str;
        private List<String> txt;
        private List<ObjectId> oid;
        private List<Boolean> bool;
        private List<LocalDate> day;
        private List<LocalDateTime> dt;
        private List<LocalTime> tm;
        private List<java.time.Instant> inst;
        private List<Date> date;
        private List<byte[]> blob;
        private List<ByteBuffer> buf;
        private List<Color> color;
        private List<MxChild> child;
        private List<List<Long>> longs;
        private List<Map<String, Long>> counts;
    }

    @lombok.Data
    public static class MxMaps {
        private Map<String, Long> lng;
        private Map<String, Integer> intg;
        private Map<String, Double> dbl;
        private Map<String, Float> flt;
        private Map<String, java.math.BigDecimal> dec;
        private Map<String, String> str;
        private Map<String, String> txt;
        private Map<String, ObjectId> oid;
        private Map<String, Boolean> bool;
        private Map<String, LocalDate> day;
        private Map<String, LocalDateTime> dt;
        private Map<String, LocalTime> tm;
        private Map<String, java.time.Instant> inst;
        private Map<String, Date> date;
        private Map<String, byte[]> blob;
        private Map<String, ByteBuffer> buf;
        private Map<String, Color> color;
        private Map<String, MxChild> child;
        private Map<String, List<Long>> longs;
        private Map<String, Map<String, Long>> counts;
    }

    @lombok.Data
    public static class MxArrays {
        private Long[] lng;
        private Integer[] intg;
        private Double[] dbl;
        private Float[] flt;
        private java.math.BigDecimal[] dec;
        private String[] str;
        private String[] txt;
        private ObjectId[] oid;
        private Boolean[] bool;
        private LocalDate[] day;
        private LocalDateTime[] dt;
        private LocalTime[] tm;
        private java.time.Instant[] inst;
        private Date[] date;
        private byte[][] blob;
        private ByteBuffer[] buf;
        private Color[] color;
        private MxChild[] child;
        private List<Long>[] longs;
        private Map<String, Long>[] counts;
    }

    @lombok.Data
    public static class MxBoxes {
        private Box<Long> lng;
        private Box<Integer> intg;
        private Box<Double> dbl;
        private Box<Float> flt;
        private Box<java.math.BigDecimal> dec;
        private Box<String> str;
        private Box<String> txt;
        private Box<ObjectId> oid;
        private Box<Boolean> bool;
        private Box<LocalDate> day;
        private Box<LocalDateTime> dt;
        private Box<LocalTime> tm;
        private Box<java.time.Instant> inst;
        private Box<Date> date;
        private Box<byte[]> blob;
        private Box<ByteBuffer> buf;
        private Box<Color> color;
        private Box<MxChild> child;
        private Box<List<Long>> longs;
        private Box<Map<String, Long>> counts;
    }

    @lombok.Data
    public static class MxBoxLists {
        private List<Box<Long>> lng;
        private List<Box<Integer>> intg;
        private List<Box<Double>> dbl;
        private List<Box<Float>> flt;
        private List<Box<java.math.BigDecimal>> dec;
        private List<Box<String>> str;
        private List<Box<String>> txt;
        private List<Box<ObjectId>> oid;
        private List<Box<Boolean>> bool;
        private List<Box<LocalDate>> day;
        private List<Box<LocalDateTime>> dt;
        private List<Box<LocalTime>> tm;
        private List<Box<java.time.Instant>> inst;
        private List<Box<Date>> date;
        private List<Box<byte[]>> blob;
        private List<Box<ByteBuffer>> buf;
        private List<Box<Color>> color;
        private List<Box<MxChild>> child;
        private List<Box<List<Long>>> longs;
        private List<Box<Map<String, Long>>> counts;
    }

    // Classes binding Box's type variable themselves (each property's erased field is Object).
    public static class MxIntgBox extends Box<Integer> {
    }

    public static class MxDblBox extends Box<Double> {
    }

    public static class MxFltBox extends Box<Float> {
    }

    public static class MxDecBox extends Box<java.math.BigDecimal> {
    }

    public static class MxStrBox extends Box<String> {
    }

    public static class MxOidBox extends Box<ObjectId> {
    }

    public static class MxBoolBox extends Box<Boolean> {
    }

    public static class MxDayBox extends Box<LocalDate> {
    }

    public static class MxDtBox extends Box<LocalDateTime> {
    }

    public static class MxTmBox extends Box<LocalTime> {
    }

    public static class MxInstBox extends Box<java.time.Instant> {
    }

    public static class MxDateBox extends Box<Date> {
    }

    public static class MxBlobBox extends Box<byte[]> {
    }

    public static class MxBufBox extends Box<ByteBuffer> {
    }

    public static class MxColorBox extends Box<Color> {
    }

    public static class MxChildBox extends Box<MxChild> {
    }

    public static class MxLongsBox extends Box<List<Long>> {
    }

    public static class MxCountsBox extends Box<Map<String, Long>> {
    }

    @lombok.Data
    public static class MxBounds {
        private LongBox lng;
        private MxIntgBox intg;
        private MxDblBox dbl;
        private MxFltBox flt;
        private MxDecBox dec;
        private MxStrBox str;
        private MxStrBox txt;
        private MxOidBox oid;
        private MxBoolBox bool;
        private MxDayBox day;
        private MxDtBox dt;
        private MxTmBox tm;
        private MxInstBox inst;
        private MxDateBox date;
        private MxBlobBox blob;
        private MxBufBox buf;
        private MxColorBox color;
        private MxChildBox child;
        private MxLongsBox longs;
        private MxCountsBox counts;
    }

    public record MxRecord(Long lng, Integer intg, Double dbl, Float flt, java.math.BigDecimal dec, String str, String txt, ObjectId oid, Boolean bool,
            LocalDate day, LocalDateTime dt, LocalTime tm, java.time.Instant inst, Date date, byte[] blob, ByteBuffer buf, Color color, MxChild child,
            List<Long> longs, Map<String, Long> counts, long prim) {
    }

    @lombok.Data
    public static class MxNested {
        private MxPlain plain;
        private MxRecord record;
        private List<MxRecord> records;
    }

    // ---- end 2026-10-04 coverageMB ----

    // ---- 2026-10-19 sliceT ----

    @Test
    public void testExtractDataBeanTargetPassesPositionalRowsThroughLikeTheDocumentBranch_sliceT() {
        // Rows whose first non-null element is not a Document reach the bean-target branch. Positional rows (Object[], List)
        // are interpreted by the Dataset against selectPropNames, as in the Document-first branch; forcing them through
        // Beans.copyAs failed with "Not a bean class".
        final List<String> columns = Arrays.asList("id", "name");

        org.junit.jupiter.api.Assertions.assertAll(() -> {
            final Dataset ds = MongoDBBase.extractData(columns, Arrays.asList(new Object[] { "1", "a" }, new Object[] { "2", "b" }), TestEntity.class);
            assertEquals(columns, ds.columnNames());
            assertEquals(Arrays.asList("a", "b"), ds.getColumn("name"));
        }, () -> {
            final Dataset ds = MongoDBBase.extractData(columns, Collections.singletonList(Arrays.asList("1", "a")), TestEntity.class);
            assertEquals(Collections.singletonList("a"), ds.getColumn("name"));
        }, () -> {
            final Dataset ds = MongoDBBase.extractData(columns, Arrays.asList(new BasicDBObject("name", "x"), new Object[] { "2", "b" }), TestEntity.class);
            assertEquals(Arrays.asList("x", "b"), ds.getColumn("name"));
        }, () -> {
            // A bean row of another class is still copied to the target bean.
            final ObjectIdEntity other = new ObjectIdEntity();
            other.setName("o");
            final Dataset ds = MongoDBBase.extractData(columns, Arrays.asList(new BasicDBObject("name", "x"), other), TestEntity.class);
            assertEquals(Arrays.asList("x", "o"), ds.getColumn("name"));
        });
    }

    @Test
    public void testToEntitySkipsValuesOfAccessFieldByMethodGetterOnlyProperties_sliceT() {
        // abacus-common 8.2 treats a field-backed getter-only property of an @AccessFieldByMethod bean as read-only and
        // Beans.mapToBean skips it, but the decoded-value pass still converted its stored value, so an unconvertible
        // element ("abc" for a List<Long>/Long[] property) failed the whole row with NumberFormatException.
        org.junit.jupiter.api.Assertions.assertAll(() -> {
            final MethodAccessEntity entity = MongoDBBase.toEntity(new Document("name", "x").append("codes", Arrays.asList("abc")), MethodAccessEntity.class);
            assertEquals("x", entity.getName());
            assertEquals(0, entity.getCodes().length);
        }, () -> {
            final MethodAccessEntity entity = MongoDBBase.toEntity(new Document("name", "y").append("nums", Arrays.asList("abc")), MethodAccessEntity.class);
            assertEquals("y", entity.getName());
            assertTrue(entity.getNums().isEmpty());
        });
    }

    @Test
    public void testAppendGroupFieldLeafMapsMissingFieldsToNullKey_sliceT() {
        // A bare "$a" inside a $group _id expression object is omitted for a document without "a" but kept as null for
        // {a: null}, splitting one null-key group into two (live, MongoDB 7). Each leaf must be {$ifNull: ["$a", null]}.
        final Document groupFields = new Document();
        MongoDBBase.appendGroupField(groupFields, "a");
        MongoDBBase.appendGroupField(groupFields, "n.m");
        MongoDBBase.appendGroupField(groupFields, "n.k");
        MongoDBBase.appendGroupField(groupFields, "n.m"); // a repeated identical path stays harmless

        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertEquals(new Document("a", new Document("$ifNull", Arrays.asList("$a", null))).append("n",
                        new Document("m", new Document("$ifNull", Arrays.asList("$n.m", null))).append("k",
                                new Document("$ifNull", Arrays.asList("$n.k", null)))),
                        groupFields),
                () -> assertThrows(IllegalArgumentException.class, () -> MongoDBBase.appendGroupField(groupFields, "n")),
                () -> assertThrows(IllegalArgumentException.class, () -> MongoDBBase.appendGroupField(groupFields, "a.b")),
                () -> assertThrows(IllegalArgumentException.class, () -> MongoDBBase.appendGroupField(groupFields, "n.m.x")));
    }

    @com.landawn.abacus.annotation.Entity
    @com.landawn.abacus.annotation.AccessFieldByMethod
    public static class MethodAccessEntity {
        private String name;
        private final Long[] codes = new Long[0];
        private final List<Long> nums = new ArrayList<>();

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public Long[] getCodes() {
            return codes;
        }

        public List<Long> getNums() {
            return nums;
        }
    }

    // ---- end 2026-10-19 sliceT ----

    // ---- 2026-10-19 mongomem ----
    // toList converts rows as the cursor yields them (it used to fetch every Document with into(...) and then convert,
    // holding the result twice). These pin that the observable result is unchanged.

    @SuppressWarnings("unchecked")
    @Test
    public void testToListStreamsMixedRowsInOrderAndClosesCursor_mongomem() {
        final MongoIterable<Object> iterable = org.mockito.Mockito.mock(MongoIterable.class);
        final TestEntity bean = new TestEntity();
        bean.setName("c");
        final ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
        final List<MongoIterableStubs.ListCursor<?>> cursors = MongoIterableStubs.stubRows(iterable,
                Arrays.asList(new Document("_id", id).append("name", "a"), null, new BasicDBObject("name", "b"), bean, new Document("name", "d")));

        final List<TestEntity> result = MongoDBBase.toList(iterable, TestEntity.class);

        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals(5, result.size()), () -> assertEquals(id.toHexString(), result.get(0).getId()),
                () -> assertEquals("a", result.get(0).getName()), () -> assertNull(result.get(1)), () -> assertEquals("b", result.get(2).getName()),
                () -> assertEquals("c", result.get(3).getName()), () -> assertEquals("d", result.get(4).getName()), () -> assertEquals(1, cursors.size()),
                () -> assertTrue(cursors.get(0).isClosed()), () -> assertEquals(5, cursors.get(0).rowsRead()),
                // The rows are no longer collected with into(...) first.
                () -> org.mockito.Mockito.verify(iterable, org.mockito.Mockito.never()).into(org.mockito.ArgumentMatchers.any()));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testToListKeepsNullRowsEmptyResultsAndMutableLists_mongomem() {
        final MongoIterable<Object> iterable = org.mockito.Mockito.mock(MongoIterable.class);

        org.junit.jupiter.api.Assertions.assertAll(() -> {
            final List<MongoIterableStubs.ListCursor<?>> cursors = MongoIterableStubs.stubRows(iterable, Collections.emptyList());
            final List<String> empty = MongoDBBase.toList(iterable, String.class);
            assertTrue(empty.isEmpty());
            empty.add("mutable");
            assertTrue(cursors.get(0).isClosed());
        }, () -> {
            MongoIterableStubs.stubRows(iterable, Arrays.asList(null, null));
            final List<String> allNull = MongoDBBase.toList(iterable, String.class);
            assertEquals(Arrays.asList(null, null), allNull);
            allNull.add("mutable");
        }, () -> {
            // _id-only rows before the first row carrying the value field read as null (the value field is missing) ...
            MongoIterableStubs.stubRows(iterable, Arrays.asList(new Document("_id", 1), null, new Document("_id", 2).append("a", "x"), new Document("_id", 3)));
            assertEquals(Arrays.asList(null, null, "x", null), MongoDBBase.toList(iterable, String.class));
        }, () -> {
            // ... and by their _id when no row has a value field.
            MongoIterableStubs.stubRows(iterable, Arrays.asList(new Document("_id", 7), null, new Document("_id", 8)));
            assertEquals(Arrays.asList("7", null, "8"), MongoDBBase.toList(iterable, String.class));
        }, () -> {
            MongoIterableStubs.stubRows(iterable, Arrays.asList(null, new Document("name", "a"), null));
            final List<TestEntity> beans = MongoDBBase.toList(iterable, TestEntity.class);
            assertEquals(3, beans.size());
            assertNull(beans.get(0));
            assertEquals("a", beans.get(1).getName());
            assertNull(beans.get(2));
        }, () -> {
            MongoIterableStubs.stubRows(iterable, Arrays.asList(null, new Document("a", 1)));
            final List<HashMap> maps = MongoDBBase.toList(iterable, HashMap.class);
            assertEquals(Arrays.asList(null, new HashMap<>(Map.of("a", 1))), maps);
            assertEquals(HashMap.class, maps.get(1).getClass());
            maps.add(null);
        });
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testToListConvertsRowsKeptBeforeTheFirstRowNeedingConversion_mongomem() {
        // Rows already of the requested type are kept as fetched until a row needs conversion; then they are converted
        // too, exactly as when every row was fetched first.
        final MongoIterable<Object> iterable = org.mockito.Mockito.mock(MongoIterable.class);
        final HashMap<String, Object> hashMap = new HashMap<>(Map.of("name", "bob"));

        org.junit.jupiter.api.Assertions.assertAll(() -> {
            MongoIterableStubs.stubRows(iterable, Arrays.asList(1_000_001L, null, 2, 3_000_003L));
            final List<Long> longs = MongoDBBase.toList(iterable, Long.class);
            assertEquals(Arrays.asList(1_000_001L, null, 2L, 3_000_003L), longs);
            assertEquals(Long.class, longs.get(2).getClass());
        }, () -> {
            MongoIterableStubs.stubRows(iterable, Arrays.asList(hashMap, null, new Document("a", 1)));
            final List<HashMap> maps = MongoDBBase.toList(iterable, HashMap.class);
            assertEquals(Arrays.asList(hashMap, null, new HashMap<>(Map.of("a", 1))), maps);
            // Not every row was a HashMap, so (as before) the HashMap row is copied too.
            org.junit.jupiter.api.Assertions.assertNotSame(hashMap, maps.get(0));
            assertEquals(HashMap.class, maps.get(2).getClass());
        }, () -> {
            // A scalar row kept first selects scalar conversion, so a later document is rejected.
            MongoIterableStubs.stubRows(iterable, Arrays.asList(1, new Document("a", 1)));
            assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toList(iterable, Integer.class));
        });
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testToListConversionFailureClosesCursorAndKeepsFetchThenConvertPrecedence_mongomem() {
        final MongoIterable<Object> iterable = org.mockito.Mockito.mock(MongoIterable.class);

        org.junit.jupiter.api.Assertions.assertAll(() -> {
            final List<MongoIterableStubs.ListCursor<?>> cursors = MongoIterableStubs.stubRows(iterable,
                    Arrays.asList(new Document("a", "x"), new Document("a", 1), new Document("a", 2)));
            assertThrows(NumberFormatException.class, () -> MongoDBBase.toList(iterable, Integer.class));
            assertTrue(cursors.get(0).isClosed());
            // The rest of the cursor is still read, as when every row was fetched before converting.
            assertEquals(3, cursors.get(0).rowsRead());
        }, () -> {
            final List<MongoIterableStubs.ListCursor<?>> cursors = MongoIterableStubs.stubRows(iterable, Arrays.asList(new Document("name", "x"), "str"));
            assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toList(iterable, TestEntity.class));
            assertTrue(cursors.get(0).isClosed());
        }, () -> {
            // A later cursor failure still wins over an earlier conversion failure.
            final List<MongoIterableStubs.ListCursor<?>> cursors = MongoIterableStubs.stubRows(iterable,
                    Arrays.asList(new Document("a", "x"), MongoIterableStubs.CURSOR_FAILURE));
            assertThrows(com.mongodb.MongoException.class, () -> MongoDBBase.toList(iterable, Integer.class));
            assertTrue(cursors.get(0).isClosed());
        }, () -> {
            // For a scalar target read from documents, every row is validated before any value is converted: the
            // inconsistent second row is reported, not the unconvertible first value.
            MongoIterableStubs.stubRows(iterable, Arrays.asList(new Document("a", "x"), new Document("b", 1)));
            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toList(iterable, Integer.class));
            assertTrue(e.getMessage().contains("inconsistent scalar fields 'a' and 'b'"), e.getMessage());
        }, () -> {
            final List<MongoIterableStubs.ListCursor<?>> cursors = MongoIterableStubs.stubRows(iterable,
                    Arrays.asList(new Document("a", 1), MongoIterableStubs.CURSOR_FAILURE));
            assertThrows(com.mongodb.MongoException.class, () -> MongoDBBase.toList(iterable, HashMap.class));
            assertTrue(cursors.get(0).isClosed());
        });
    }

    // ---- end 2026-10-19 mongomem ----

    // ---- 2026-10-20 mongoxd ----

    @Test
    public void testExtractDataMapTargetPassesPositionalAndScalarRowsThroughWithSelectPropNames_mongoxd() {
        // A Map target with a Map first row sent every non-Map row through Beans.beanToMap, so positional (Object[], List)
        // and scalar rows failed with "Not a bean class" instead of being read against selectPropNames like other branches.
        final List<String> columns = Arrays.asList("id", "name");

        org.junit.jupiter.api.Assertions.assertAll(() -> {
            final Dataset ds = MongoDBBase.extractData(columns, Arrays.asList(new Document("name", "x"), new Object[] { "2", "b" }), Map.class);
            assertEquals(columns, ds.columnNames());
            assertEquals(Arrays.asList(null, "2"), ds.getColumn("id"));
            assertEquals(Arrays.asList("x", "b"), ds.getColumn("name"));
        }, () -> {
            final Dataset ds = MongoDBBase.extractData(columns, Arrays.asList(new Document("name", "x"), Arrays.asList("2", "b")), Document.class);
            assertEquals(Arrays.asList(null, "2"), ds.getColumn("id"));
            assertEquals(Arrays.asList("x", "b"), ds.getColumn("name"));
        }, () -> {
            final Map<String, Object> first = new LinkedHashMap<>();
            first.put("name", "x");
            final Dataset ds = MongoDBBase.extractData(columns, Arrays.asList(first, null, new Object[] { "2", "b" }), LinkedHashMap.class);
            assertEquals(Arrays.asList("x", null, "b"), ds.getColumn("name"));
        }, () -> {
            // A scalar row is read as the single selected column.
            final Dataset ds = MongoDBBase.extractData(Collections.singletonList("name"), Arrays.asList(new Document("name", "x"), "y"), Map.class);
            assertEquals(Arrays.asList("x", "y"), ds.getColumn("name"));
        }, () -> {
            // ... and rejected by the Dataset against several columns, not by a bean conversion.
            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> MongoDBBase.extractData(columns, Arrays.asList(new Document("name", "x"), "y"), Map.class));
            assertTrue(!e.getMessage().contains("Not a bean class"), e.getMessage());
        }, () -> {
            // Bean rows are still converted to maps.
            final TestEntity bean = new TestEntity();
            bean.setId("1");
            bean.setName("o");
            final Dataset ds = MongoDBBase.extractData(columns, Arrays.asList(new Document("name", "x"), bean, new Object[] { "2", "b" }), Map.class);
            assertEquals(Arrays.asList(null, "1", "2"), ds.getColumn("id"));
            assertEquals(Arrays.asList("x", "o", "b"), ds.getColumn("name"));
        });
    }

    @Test
    public void testExtractDataMapTargetInferringColumnNamesRejectsNonMapNonBeanRowsClearly_mongoxd() {
        // Without selectPropNames the column names are collected from every row's keys, so a positional or scalar row cannot
        // be read; it is rejected with a message naming the row's class instead of "Not a bean class".
        final Document doc = new Document("name", "x");

        org.junit.jupiter.api.Assertions.assertAll(() -> {
            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> MongoDBBase.extractData(null, Arrays.asList(doc, new Object[] { "2", "b" }), Map.class));
            assertTrue(e.getMessage().contains("java.lang.Object[]") && !e.getMessage().contains("Not a bean class"), e.getMessage());
        }, () -> {
            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> MongoDBBase.extractData(Collections.emptyList(), Arrays.asList(doc, new ArrayList<>(Arrays.asList("2", "b"))), Document.class));
            assertTrue(e.getMessage().contains("java.util.ArrayList") && !e.getMessage().contains("Not a bean class"), e.getMessage());
        }, () -> {
            final Map<String, Object> first = new LinkedHashMap<>();
            first.put("name", "x");
            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> MongoDBBase.extractData(Arrays.asList(first, "y"), LinkedHashMap.class));
            assertTrue(e.getMessage().contains("java.lang.String") && !e.getMessage().contains("Not a bean class"), e.getMessage());
        }, () -> {
            // The null-row check is kept.
            assertThrows(IllegalArgumentException.class, () -> MongoDBBase.extractData(Arrays.asList(doc, null), Map.class));
        }, () -> {
            // Bean rows are still converted, and their keys join the inferred column names.
            final TestEntity bean = new TestEntity();
            bean.setId("1");
            bean.setName("o");
            final Dataset ds = MongoDBBase.extractData(Arrays.asList(doc, bean), Map.class);
            assertEquals(Arrays.asList("name", "id"), ds.columnNames());
            assertEquals(Arrays.asList("x", "o"), ds.getColumn("name"));
            assertEquals(Arrays.asList(null, "1"), ds.getColumn("id"));
        });
    }

    // ---- end 2026-10-20 mongoxd ----

    // ---- 2026-10-24 sliceT ----

    /** Writes {@code doc} through the registry's codecs and decodes it again, as an insert followed by a find does. */
    private static Document sliceTRoundTrip(final Document doc) {
        final org.bson.BsonDocument bson = doc.toBsonDocument(Document.class, MongoDBBase.codecRegistry);

        return new org.bson.codecs.DocumentCodec(MongoDBBase.codecRegistry).decode(new org.bson.BsonDocumentReader(bson),
                org.bson.codecs.DecoderContext.builder().build());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testStringElementArraysReadBackTheJsonTextTheCodecWrites_sliceT() {
        // GeneralCodec wrote an array (the driver has no codec for one) as its N.stringOf JSON text, ["a", "b, c"], but bean
        // mapping converted that text with N.convert, which wraps a String as the single element of a String[]/Object[]: a
        // String[] {"a", "b, c"} written by this class came back as {"[\"a\", \"b, c\"]"} (also proven against live MongoDB).
        // Since 2026-10-24 (apply-mongo) the codec writes a BSON array; the round trip below now reads that array, and the
        // text cases below (plus testLegacyArrayTextStillReadsInEveryShape_applyMongo) keep the stored-text reading pinned.
        final SliceTTextArrayEntity child = new SliceTTextArrayEntity();
        child.setTags(new String[] { "nested" });

        final SliceTTextArrayEntity source = new SliceTTextArrayEntity();
        source.setTags(new String[] { "a", "b, c" });
        source.setObjs(new Object[] { "x", 1 });
        source.setNames(new CharSequence[] { "n" });
        source.setRows(Arrays.asList(new String[] { "r1" }, new String[] { "r2", "r3" }));
        source.setByKey(new LinkedHashMap<>(Collections.singletonMap("k", new String[] { "v" })));
        source.setChildren(Collections.singletonList(child));
        source.setChild(child);

        final Document stored = sliceTRoundTrip(MongoDBBase.toDocument(source));
        // The stored shape: a BSON array (decoded as a List), no longer the JSON text "[\"a\", \"b, c\"]" (apply-mongo).
        assertEquals(Arrays.asList("a", "b, c"), stored.get("tags"));

        final SliceTTextArrayEntity back = MongoDBBase.toEntity(stored, SliceTTextArrayEntity.class);
        final MongoIterable<Object> iterable = org.mockito.Mockito.mock(MongoIterable.class);
        MongoIterableStubs.stubRows(iterable, Arrays.asList(new Document("tags", "[\"p\", \"q\"]"), new Document("tags", "plain")));

        org.junit.jupiter.api.Assertions.assertAll(() -> assertArrayEquals(new String[] { "a", "b, c" }, back.getTags()),
                () -> assertArrayEquals(new Object[] { "x", 1 }, back.getObjs()), () -> assertArrayEquals(new CharSequence[] { "n" }, back.getNames()),
                () -> assertArrayEquals(new String[] { "r2", "r3" }, back.getRows().get(1)),
                () -> assertArrayEquals(new String[] { "v" }, back.getByKey().get("k")),
                () -> assertArrayEquals(new String[] { "nested" }, back.getChildren().get(0).getTags()),
                () -> assertArrayEquals(new String[] { "nested" }, back.getChild().getTags()),
                // A record component and the scalar reads (also used by the executors' queryForSingleValue/distinct) parse it too.
                () -> assertArrayEquals(new String[] { "a", "b" },
                        MongoDBBase.toEntity(new Document("name", "r").append("tags", "[\"a\", \"b\"]"), SliceTTextArrayRecord.class).tags()),
                () -> assertArrayEquals(new String[] { "a" }, MongoDBBase.convertBsonValue(" [\"a\"] ", String[].class)), () -> {
                    final List<String[]> rows = MongoDBBase.toList(iterable, String[].class);
                    assertArrayEquals(new String[] { "p", "q" }, rows.get(0));
                    assertArrayEquals(new String[] { "plain" }, rows.get(1));
                },
                // Other text keeps the single-element reading, and so does text that only looks like an array literal.
                () -> assertArrayEquals(new String[] { "abc" }, MongoDBBase.toEntity(new Document("tags", "abc"), SliceTTextArrayEntity.class).getTags()),
                () -> assertArrayEquals(new String[] { "[x]y]" }, MongoDBBase.toEntity(new Document("tags", "[x]y]"), SliceTTextArrayEntity.class).getTags()),
                // Unchanged: arrays of other element types already parsed their text, and a String property keeps its text.
                () -> assertArrayEquals(new Long[] { 1L, 2L }, MongoDBBase.toEntity(new Document("longs", "[1, 2]"), SliceTTextArrayEntity.class).getLongs()),
                () -> assertEquals("[a]", MongoDBBase.toEntity(new Document("label", "[a]"), SliceTTextArrayEntity.class).getLabel()));
    }

    @Test
    public void testToEntitySkipsBinaryConversionOfReadOnlyProperties_sliceT() {
        // Beans.mapToBean skips a read-only property, and the decoded-value pass already skipped one (2026-10-19), but the binary
        // pass still converted its stored value: a list of text stored for a getter-only byte[]/ByteBuffer/byte[][]/ByteBuffer[]
        // property of an @AccessFieldByMethod bean failed the whole row (NumberFormatException or "Illegal base64 character").
        final List<String> text = Collections.singletonList("x");

        final java.util.stream.Stream<org.junit.jupiter.api.function.Executable> cases = java.util.stream.Stream.of("bytes", "buffer", "chunks", "buffers")
                .map(prop -> () -> {
                    final SliceTReadOnlyBinaryEntity entity = MongoDBBase.toEntity(new Document("name", prop).append(prop, text),
                            SliceTReadOnlyBinaryEntity.class);

                    assertEquals(prop, entity.getName(), prop);
                    assertArrayEquals(new byte[] { 1 }, entity.getBytes(), prop);
                    assertEquals(0, entity.getChunks().length, prop);
                });

        org.junit.jupiter.api.Assertions.assertAll(cases);

        // A writable binary property is still converted.
        final SliceTReadOnlyBinaryEntity entity = MongoDBBase.toEntity(new Document("data", new Binary(new byte[] { 7 })), SliceTReadOnlyBinaryEntity.class);
        assertArrayEquals(new byte[] { 7 }, entity.getData());
    }

    public static class SliceTTextArrayEntity {
        private String label;
        private String[] tags;
        private Object[] objs;
        private CharSequence[] names;
        private Long[] longs;
        private List<String[]> rows;
        private Map<String, String[]> byKey;
        private List<SliceTTextArrayEntity> children;
        private SliceTTextArrayEntity child;

        public String getLabel() {
            return label;
        }

        public void setLabel(final String label) {
            this.label = label;
        }

        public String[] getTags() {
            return tags;
        }

        public void setTags(final String[] tags) {
            this.tags = tags;
        }

        public Object[] getObjs() {
            return objs;
        }

        public void setObjs(final Object[] objs) {
            this.objs = objs;
        }

        public CharSequence[] getNames() {
            return names;
        }

        public void setNames(final CharSequence[] names) {
            this.names = names;
        }

        public Long[] getLongs() {
            return longs;
        }

        public void setLongs(final Long[] longs) {
            this.longs = longs;
        }

        public List<String[]> getRows() {
            return rows;
        }

        public void setRows(final List<String[]> rows) {
            this.rows = rows;
        }

        public Map<String, String[]> getByKey() {
            return byKey;
        }

        public void setByKey(final Map<String, String[]> byKey) {
            this.byKey = byKey;
        }

        public List<SliceTTextArrayEntity> getChildren() {
            return children;
        }

        public void setChildren(final List<SliceTTextArrayEntity> children) {
            this.children = children;
        }

        public SliceTTextArrayEntity getChild() {
            return child;
        }

        public void setChild(final SliceTTextArrayEntity child) {
            this.child = child;
        }
    }

    public record SliceTTextArrayRecord(String name, String[] tags) {
    }

    @com.landawn.abacus.annotation.Entity
    @com.landawn.abacus.annotation.AccessFieldByMethod
    public static class SliceTReadOnlyBinaryEntity {
        private String name;
        private byte[] data;
        private final byte[] bytes = { 1 };
        private final ByteBuffer buffer = ByteBuffer.wrap(new byte[] { 2 });
        private final byte[][] chunks = {};
        private final ByteBuffer[] buffers = {};

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public byte[] getData() {
            return data;
        }

        public void setData(final byte[] data) {
            this.data = data;
        }

        public byte[] getBytes() {
            return bytes;
        }

        public ByteBuffer getBuffer() {
            return buffer;
        }

        public byte[][] getChunks() {
            return chunks;
        }

        public ByteBuffer[] getBuffers() {
            return buffers;
        }
    }

    // ---- end 2026-10-24 sliceT ----

    // ---- 2026-10-24 verify4 ----

    @Test
    public void testDottedKeyArrayTextIsParsedWhateverTheOuterBeanDeclares_verify4() {
        // The sliceT array-text pass was gated by a per-class flag of the OUTER bean, so a dotted key naming a nested bean's
        // String[] property ("child.tags") was parsed only when the outer bean itself had a String[]-like property.
        final String text = "[\"a\", \"b\"]";

        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertArrayEquals(new String[] { "a", "b" },
                        MongoDBBase.toEntity(new Document("child.tags", text), Verify4PlainOuter.class).getChild().getTags()),
                // Control: an outer bean that has a String[] property of its own (already parsed before this fix).
                () -> assertArrayEquals(new String[] { "a", "b" },
                        MongoDBBase.toEntity(new Document("child.tags", text), SliceTTextArrayEntity.class).getChild().getTags()),
                // A dotted String value of a non-array property keeps its text.
                () -> assertEquals("[a]", MongoDBBase.toEntity(new Document("child.label", "[a]"), Verify4PlainOuter.class).getChild().getLabel()));
    }

    /** A bean without a String[]-like property of its own, holding one that has. */
    public static class Verify4PlainOuter {
        private String name;
        private SliceTTextArrayEntity child;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public SliceTTextArrayEntity getChild() {
            return child;
        }

        public void setChild(final SliceTTextArrayEntity child) {
            this.child = child;
        }
    }

    // ---- end 2026-10-24 verify4 ----

    // ---- 2026-10-24 apply-mongo ----

    /** Encodes {@code value} as the only field of a Document through {@code registry}, as an insert does. */
    private static org.bson.BsonValue applyMongoEncodeField(final org.bson.codecs.configuration.CodecRegistry registry, final Object value) {
        final org.bson.BsonDocument written = new org.bson.BsonDocument();
        registry.get(Document.class)
                .encode(new org.bson.BsonDocumentWriter(written), new Document("v", value), org.bson.codecs.EncoderContext.builder().build());

        return written.get("v");
    }

    @Test
    public void testArraysAreWrittenAsBsonArraysLikeTheSameElementsOfAList_applyMongo() {
        // Maintainer decision (2026-10-24 item 6): GeneralCodec wrote every array but byte[] as its N.stringOf JSON text (a BSON
        // string), so eq("tags", "a") matched nothing, $push/$addToSet failed on the field, and a ByteBuffer[] was stored as [""].
        // An array is now a BSON array whose elements are encoded exactly as the same elements of a List.
        final ApplyMongoInner inner = new ApplyMongoInner();
        inner.setName("i");
        inner.setTags(new String[] { "x" });
        inner.setBufs(new ByteBuffer[] { ByteBuffer.wrap(new byte[] { 5, 6 }) });
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("k", 1);
        final Date date = new Date(1700000000000L);

        final Object[][] cases = { { new String[] { "a", "b, c" }, Arrays.asList("a", "b, c") },
                { new Object[] { "x", 1, 2L, 1.5, true, null, new Object[] { "y" }, map },
                        Arrays.asList("x", 1, 2L, 1.5, true, null, Arrays.asList("y"), map) },
                { new CharSequence[] { "n" }, Arrays.asList("n") }, { new Integer[] { 1, null, 3 }, Arrays.asList(1, null, 3) },
                { new int[] { 1, 2 }, Arrays.asList(1, 2) }, { new long[] { 1L, 5000000000L }, Arrays.asList(1L, 5000000000L) },
                { new double[] { 1.5, 2 }, Arrays.asList(1.5, 2.0) }, { new float[] { 1.5f }, Arrays.asList(1.5f) },
                { new short[] { 3 }, Arrays.asList((short) 3) }, { new Byte[] { 1, 2 }, Arrays.asList((byte) 1, (byte) 2) },
                { new boolean[] { true, false }, Arrays.asList(true, false) }, { new char[] { 'a', 'b' }, Arrays.asList('a', 'b') },
                { new Character[] { 'c' }, Arrays.asList('c') },
                { new ByteBuffer[] { ByteBuffer.wrap(new byte[] { 1, 2, 3 }), null }, Arrays.asList(ByteBuffer.wrap(new byte[] { 1, 2, 3 }), null) },
                { new byte[][] { { 9 }, { 8, 7 } }, Arrays.asList(new byte[] { 9 }, new byte[] { 8, 7 }) },
                { new ApplyMongoInner[] { inner }, Arrays.asList(inner) }, { new Map[] { map }, Arrays.asList(map) },
                { new Document[] { new Document("d", 1) }, Arrays.asList(new Document("d", 1)) },
                { new String[][] { { "p" }, { "q", "r" } }, Arrays.asList(Arrays.asList("p"), Arrays.asList("q", "r")) },
                { new int[][] { { 1 }, { 2, 3 } }, Arrays.asList(Arrays.asList(1), Arrays.asList(2, 3)) },
                { new java.util.concurrent.TimeUnit[] { java.util.concurrent.TimeUnit.SECONDS }, Arrays.asList(java.util.concurrent.TimeUnit.SECONDS) },
                { new Date[] { date }, Arrays.asList(date) },
                { new java.math.BigDecimal[] { new java.math.BigDecimal("1.25") }, Arrays.asList(new java.math.BigDecimal("1.25")) },
                { new LocalDate[] { LocalDate.of(2024, 1, 2) }, Arrays.asList(LocalDate.of(2024, 1, 2)) }, { new String[0], new ArrayList<>() } };

        final java.util.stream.Stream<org.junit.jupiter.api.function.Executable> sameAsList = Arrays.stream(cases).map(c -> () -> {
            final org.bson.BsonValue written = applyMongoEncodeField(MongoDBBase.codecRegistry, c[0]);
            final String label = c[0].getClass().getSimpleName();

            assertTrue(written.isArray(), label + ": " + written);
            assertEquals(applyMongoEncodeField(MongoDBBase.codecRegistry, c[1]), written, label);
        });

        // The concrete element shapes, including the char[] decision (one-character strings, as a List<Character> is written).
        final java.util.stream.Stream<org.junit.jupiter.api.function.Executable> shapes = java.util.stream.Stream.of(
                () -> assertEquals(org.bson.BsonDocument.parse("{v: ['a', 'b, c']}").get("v"),
                        applyMongoEncodeField(MongoDBBase.codecRegistry, new String[] { "a", "b, c" })),
                () -> assertEquals(new org.bson.BsonArray(Arrays.asList(new org.bson.BsonInt32(1), new org.bson.BsonInt32(2))),
                        applyMongoEncodeField(MongoDBBase.codecRegistry, new int[] { 1, 2 })),
                () -> assertEquals(new org.bson.BsonArray(Arrays.asList(new org.bson.BsonInt64(5000000000L))),
                        applyMongoEncodeField(MongoDBBase.codecRegistry, new long[] { 5000000000L })),
                () -> assertEquals(new org.bson.BsonArray(Arrays.asList(new org.bson.BsonString("a"), new org.bson.BsonString("b"))),
                        applyMongoEncodeField(MongoDBBase.codecRegistry, new char[] { 'a', 'b' })),
                () -> assertEquals(new org.bson.BsonArray(Arrays.asList(new org.bson.BsonBinary(new byte[] { 1, 2, 3 }), org.bson.BsonNull.VALUE)),
                        applyMongoEncodeField(MongoDBBase.codecRegistry, new ByteBuffer[] { ByteBuffer.wrap(new byte[] { 1, 2, 3 }), null })),
                () -> assertEquals(org.bson.BsonDocument.parse("{v: [{name: 'i', tags: ['x'], bufs: [BinData(0, 'BQY=')]}]}").get("v"),
                        applyMongoEncodeField(MongoDBBase.codecRegistry, new ApplyMongoInner[] { inner })),
                // Unchanged: a byte[] is BSON binary.
                () -> assertEquals(new org.bson.BsonBinary(new byte[] { 1, 2 }), applyMongoEncodeField(MongoDBBase.codecRegistry, new byte[] { 1, 2 })));

        org.junit.jupiter.api.Assertions.assertAll(java.util.stream.Stream.concat(sameAsList, shapes));
    }

    @Test
    public void testArraysRoundTripIntoTheirDeclaredPropertyTypes_applyMongo() {
        // Every array property, also inside nested beans, records and typed containers, is stored as a BSON array and read back
        // into its declared type (a ByteBuffer[] kept its bytes only now: they were stored as [""]).
        final ApplyMongoInner inner = new ApplyMongoInner();
        inner.setName("i");
        inner.setTags(new String[] { "x", "y" });
        inner.setBufs(new ByteBuffer[] { ByteBuffer.wrap(new byte[] { 5, 6 }) });

        final ApplyMongoArrayEntity source = new ApplyMongoArrayEntity();
        source.setTags(new String[] { "a", "b, c" });
        source.setObjs(new Object[] { "x", 1, 2L, 1.5, true, null });
        source.setBoxedInts(new Integer[] { 1, null, 3 });
        source.setInts(new int[] { 1, 2 });
        source.setLongs(new long[] { 1L, 5000000000L });
        source.setDoubles(new double[] { 1.5, 2 });
        source.setFloats(new float[] { 1.5f });
        source.setShorts(new short[] { 3 });
        source.setBytes(new byte[] { 1, 2 });
        source.setBoxedBytes(new Byte[] { 4 });
        source.setBools(new boolean[] { true, false });
        source.setChars(new char[] { 'a', 'b' });
        source.setBoxedChars(new Character[] { 'c' });
        source.setBufs(new ByteBuffer[] { ByteBuffer.wrap(new byte[] { 1, 2, 3 }), null });
        source.setChunks(new byte[][] { { 9 }, { 8, 7 } });
        source.setInners(new ApplyMongoInner[] { inner });
        source.setNested(new String[][] { { "p" }, { "q", "r" } });
        source.setNestedInts(new int[][] { { 1 }, { 2, 3 } });
        source.setRows(Arrays.asList(new String[] { "r1" }, new String[] { "r2", "r3" }));
        source.setByKey(new LinkedHashMap<>(Collections.singletonMap("k", new String[] { "v" })));
        source.setIntRows(Arrays.asList(new int[] { 4 }, new int[] { 5, 6 }));
        source.setInner(inner);
        source.setRec(new ApplyMongoRecord("r", new String[] { "t" }, new int[] { 7 }, new ByteBuffer[] { ByteBuffer.wrap(new byte[] { 8 }) }));

        final Document stored = sliceTRoundTrip(MongoDBBase.toDocument(source));
        final ApplyMongoArrayEntity back = MongoDBBase.toEntity(stored, ApplyMongoArrayEntity.class);
        final ApplyMongoRecord rec = MongoDBBase.toEntity((Document) stored.get("rec"), ApplyMongoRecord.class);

        final java.util.stream.Stream<org.junit.jupiter.api.function.Executable> storedAsArrays = java.util.stream.Stream
                .of("tags", "objs", "boxedInts", "ints", "longs", "doubles", "floats", "shorts", "boxedBytes", "bools", "chars", "boxedChars", "bufs", "chunks",
                        "inners", "nested", "nestedInts")
                .map(prop -> () -> assertTrue(stored.get(prop) instanceof List, prop + ": " + stored.get(prop)));

        final java.util.stream.Stream<org.junit.jupiter.api.function.Executable> values = java.util.stream.Stream.of(
                () -> assertArrayEquals(new String[] { "a", "b, c" }, back.getTags()),
                // An int64 element stays a Long (the JSON text read it back as an Integer).
                () -> assertArrayEquals(new Object[] { "x", 1, 2L, 1.5, true, null }, back.getObjs()),
                () -> assertArrayEquals(new Integer[] { 1, null, 3 }, back.getBoxedInts()), () -> assertArrayEquals(new int[] { 1, 2 }, back.getInts()),
                () -> assertArrayEquals(new long[] { 1L, 5000000000L }, back.getLongs()), () -> assertArrayEquals(new double[] { 1.5, 2 }, back.getDoubles()),
                () -> assertArrayEquals(new float[] { 1.5f }, back.getFloats()), () -> assertArrayEquals(new short[] { 3 }, back.getShorts()),
                () -> assertArrayEquals(new byte[] { 1, 2 }, back.getBytes()), () -> assertArrayEquals(new Byte[] { 4 }, back.getBoxedBytes()),
                () -> assertArrayEquals(new boolean[] { true, false }, back.getBools()), () -> assertArrayEquals(new char[] { 'a', 'b' }, back.getChars()),
                () -> assertArrayEquals(new Character[] { 'c' }, back.getBoxedChars()),
                () -> assertArrayEquals(new ByteBuffer[] { ByteBuffer.wrap(new byte[] { 1, 2, 3 }), null }, back.getBufs()),
                () -> assertArrayEquals(new byte[][] { { 9 }, { 8, 7 } }, back.getChunks()),
                () -> assertArrayEquals(new String[] { "x", "y" }, back.getInners()[0].getTags()),
                () -> assertArrayEquals(new ByteBuffer[] { ByteBuffer.wrap(new byte[] { 5, 6 }) }, back.getInners()[0].getBufs()),
                () -> assertArrayEquals(new String[][] { { "p" }, { "q", "r" } }, back.getNested()),
                () -> assertArrayEquals(new int[][] { { 1 }, { 2, 3 } }, back.getNestedInts()),
                () -> assertArrayEquals(new String[] { "r2", "r3" }, back.getRows().get(1)),
                () -> assertArrayEquals(new String[] { "v" }, back.getByKey().get("k")), () -> assertArrayEquals(new int[] { 5, 6 }, back.getIntRows().get(1)),
                () -> assertArrayEquals(new ByteBuffer[] { ByteBuffer.wrap(new byte[] { 5, 6 }) }, back.getInner().getBufs()),
                () -> assertArrayEquals(new String[] { "t" }, back.getRec().tags()), () -> assertArrayEquals(new int[] { 7 }, back.getRec().nums()),
                () -> assertArrayEquals(new ByteBuffer[] { ByteBuffer.wrap(new byte[] { 8 }) }, back.getRec().bufs()),
                // A record read at the top level too.
                () -> assertArrayEquals(new ByteBuffer[] { ByteBuffer.wrap(new byte[] { 8 }) }, rec.bufs()),
                () -> assertArrayEquals(new int[] { 7 }, rec.nums()));

        org.junit.jupiter.api.Assertions.assertAll(java.util.stream.Stream.concat(storedAsArrays, values));
    }

    @Test
    public void testLegacyArrayTextStillReadsInEveryShape_applyMongo() {
        // Documents written before the codec stored BSON arrays hold each array as its N.stringOf JSON text (the exact strings
        // below are what that codec wrote). They must still read back after the storage change (pinned: passes on both).
        final Document legacy = new Document("tags", "[\"a\", \"b, c\"]").append("objs", "[\"x\", 1]")
                .append("ints", "[1, 2]")
                .append("longs", "[1, 5000000000]")
                .append("chars", "['a', 'b']")
                .append("chunks", "[[9], [8, 7]]")
                .append("nested", "[[\"p\"], [\"q\", \"r\"]]")
                .append("rows", Arrays.asList("[\"r1\"]", "[\"r2\", \"r3\"]"))
                .append("byKey", new Document("k", "[\"v\"]"))
                .append("inners", "[{\"name\": \"i\", \"tags\": [\"x\", \"y\"]}]")
                .append("inner", new Document("name", "i").append("tags", "[\"x\", \"y\"]"))
                .append("rec", new Document("name", "r").append("tags", "[\"t\"]").append("nums", "[7]"));
        final ApplyMongoArrayEntity back = MongoDBBase.toEntity(legacy, ApplyMongoArrayEntity.class);

        org.junit.jupiter.api.Assertions.assertAll(() -> assertArrayEquals(new String[] { "a", "b, c" }, back.getTags()),
                () -> assertArrayEquals(new Object[] { "x", 1 }, back.getObjs()), () -> assertArrayEquals(new int[] { 1, 2 }, back.getInts()),
                () -> assertArrayEquals(new long[] { 1L, 5000000000L }, back.getLongs()), () -> assertArrayEquals(new char[] { 'a', 'b' }, back.getChars()),
                () -> assertArrayEquals(new byte[][] { { 9 }, { 8, 7 } }, back.getChunks()),
                () -> assertArrayEquals(new String[][] { { "p" }, { "q", "r" } }, back.getNested()),
                () -> assertArrayEquals(new String[] { "r2", "r3" }, back.getRows().get(1)),
                () -> assertArrayEquals(new String[] { "v" }, back.getByKey().get("k")),
                () -> assertArrayEquals(new String[] { "x", "y" }, back.getInners()[0].getTags()),
                () -> assertArrayEquals(new String[] { "x", "y" }, back.getInner().getTags()),
                () -> assertArrayEquals(new String[] { "t" }, back.getRec().tags()), () -> assertArrayEquals(new int[] { 7 }, back.getRec().nums()),
                // The codec's own read of a single value (distinct / queryForSingleValue) too.
                () -> assertArrayEquals(new String[] { "a", "b" }, mxDecode(String[].class, new org.bson.BsonString("[\"a\", \"b\"]"))));
    }

    @Test
    public void testFiltersAndUpdatesSendAnArrayValueAsABsonArray_applyMongo() {
        // Filters and update documents encode their values through the same registry, so an array value is now a BSON array
        // there as well (it was the JSON text, which matched only documents that stored the very same text).
        final org.bson.codecs.configuration.CodecRegistry registry = MongoDBBase.codecRegistry;
        final org.bson.BsonDocument rawUpdate = new org.bson.BsonDocument();
        registry.get(Document.class)
                .encode(new org.bson.BsonDocumentWriter(rawUpdate), new Document("$set", new Document("tags", new String[] { "a" })),
                        org.bson.codecs.EncoderContext.builder().build());
        final org.bson.BsonDocument legacyMap = new org.bson.BsonDocument();
        final BasicBSONObject bsonObject = new BasicBSONObject("tags", new String[] { "a" });
        ((org.bson.codecs.Codec<BasicBSONObject>) registry.get(BasicBSONObject.class)).encode(new org.bson.BsonDocumentWriter(legacyMap), bsonObject,
                org.bson.codecs.EncoderContext.builder().build());

        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertEquals(org.bson.BsonDocument.parse("{tags: ['a', 'b']}"),
                        com.mongodb.client.model.Filters.eq("tags", new String[] { "a", "b" }).toBsonDocument(Document.class, registry)),
                () -> assertEquals(org.bson.BsonDocument.parse("{scores: {$in: [[1, 2]]}}"),
                        com.mongodb.client.model.Filters.in("scores", (Object) new int[] { 1, 2 }).toBsonDocument(Document.class, registry)),
                () -> assertEquals(org.bson.BsonDocument.parse("{$set: {tags: ['a']}}"),
                        com.mongodb.client.model.Updates.set("tags", new String[] { "a" }).toBsonDocument(Document.class, registry)),
                () -> assertEquals(org.bson.BsonDocument.parse("{$set: {tags: ['a']}}"), rawUpdate),
                () -> assertEquals(org.bson.BsonDocument.parse("{tags: ['a']}"), legacyMap));
    }

    @Test
    public void testArrayElementsFollowTheRegistryUuidRepresentation_applyMongo() {
        // Each element is encoded through the registry performing the lookup, so a UUID[] follows the client's UuidRepresentation
        // exactly like a List<UUID>; without a representation it fails like a List<UUID> or a UUID property (it was stored as text).
        final org.bson.codecs.configuration.CodecRegistry standard = org.bson.codecs.configuration.CodecRegistries
                .withUuidRepresentation(MongoDBBase.codecRegistry, org.bson.UuidRepresentation.STANDARD);
        final java.util.UUID key = java.util.UUID.fromString("01234567-89ab-cdef-0123-456789abcdef");

        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertEquals(new org.bson.BsonArray(Arrays.asList(new org.bson.BsonBinary(key, org.bson.UuidRepresentation.STANDARD))),
                        applyMongoEncodeField(standard, new java.util.UUID[] { key })),
                () -> assertEquals(applyMongoEncodeField(standard, Arrays.asList(key)), applyMongoEncodeField(standard, new java.util.UUID[] { key })),
                () -> assertThrows(org.bson.codecs.configuration.CodecConfigurationException.class,
                        () -> applyMongoEncodeField(MongoDBBase.codecRegistry, new java.util.UUID[] { key })),
                () -> assertThrows(org.bson.codecs.configuration.CodecConfigurationException.class,
                        () -> applyMongoEncodeField(MongoDBBase.codecRegistry, Arrays.asList(key))));
    }

    @Test
    public void testGeneralCodecWritesByteArraysAsBinaryAndOtherArraysAsBsonArrays_applyMongo() {
        // Used directly (the driver's ByteArrayCodec precedes it in codecRegistry), the codec writes a byte[] as binary, like that
        // codec, not as an array of int32 values (nor as the text "[1, 2]", as before). The encoder context stays optional for an
        // array, as it is for a scalar value.
        final org.bson.BsonDocument written = new org.bson.BsonDocument();
        final org.bson.BsonDocumentWriter writer = new org.bson.BsonDocumentWriter(written);
        writer.writeStartDocument();
        writer.writeName("bytes");
        new MongoDBBase.GeneralCodec<>(byte[].class).encode(writer, new byte[] { 1, 2 }, null);
        writer.writeName("strs");
        new MongoDBBase.GeneralCodec<>(String[].class).encode(writer, new String[] { "a" }, null);
        writer.writeName("objs");
        new MongoDBBase.GeneralCodec<>(Object[].class).encode(writer, new Object[] { null, new Object[] { 1 } }, null);
        writer.writeEndDocument();

        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertEquals(org.bson.BsonDocument.parse("{bytes: BinData(0, 'AQI='), strs: ['a'], objs: [null, [1]]}"), written),
                // The codec reads a BSON array back into the array type (distinct(field, String[].class), queryForSingleValue).
                () -> assertArrayEquals(new String[] { "a" }, mxDecode(String[].class, written.get("strs"))),
                () -> assertArrayEquals(new byte[] { 1, 2 }, mxDecode(byte[].class, written.get("bytes"))),
                () -> assertArrayEquals(new int[] { 1, 2 }, mxDecode(int[].class, org.bson.BsonDocument.parse("{v: [1, 2]}").get("v"))),
                () -> assertArrayEquals(new char[] { 'a', 'b' }, mxDecode(char[].class, org.bson.BsonDocument.parse("{v: ['a', 'b']}").get("v"))));
    }

    public static class ApplyMongoInner {
        private String name;
        private String[] tags;
        private ByteBuffer[] bufs;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public String[] getTags() {
            return tags;
        }

        public void setTags(final String[] tags) {
            this.tags = tags;
        }

        public ByteBuffer[] getBufs() {
            return bufs;
        }

        public void setBufs(final ByteBuffer[] bufs) {
            this.bufs = bufs;
        }
    }

    public record ApplyMongoRecord(String name, String[] tags, int[] nums, ByteBuffer[] bufs) {
    }

    public static class ApplyMongoArrayEntity {
        private String[] tags;
        private Object[] objs;
        private Integer[] boxedInts;
        private int[] ints;
        private long[] longs;
        private double[] doubles;
        private float[] floats;
        private short[] shorts;
        private byte[] bytes;
        private Byte[] boxedBytes;
        private boolean[] bools;
        private char[] chars;
        private Character[] boxedChars;
        private ByteBuffer[] bufs;
        private byte[][] chunks;
        private ApplyMongoInner[] inners;
        private String[][] nested;
        private int[][] nestedInts;
        private List<String[]> rows;
        private Map<String, String[]> byKey;
        private List<int[]> intRows;
        private ApplyMongoInner inner;
        private ApplyMongoRecord rec;

        public String[] getTags() {
            return tags;
        }

        public void setTags(final String[] tags) {
            this.tags = tags;
        }

        public Object[] getObjs() {
            return objs;
        }

        public void setObjs(final Object[] objs) {
            this.objs = objs;
        }

        public Integer[] getBoxedInts() {
            return boxedInts;
        }

        public void setBoxedInts(final Integer[] boxedInts) {
            this.boxedInts = boxedInts;
        }

        public int[] getInts() {
            return ints;
        }

        public void setInts(final int[] ints) {
            this.ints = ints;
        }

        public long[] getLongs() {
            return longs;
        }

        public void setLongs(final long[] longs) {
            this.longs = longs;
        }

        public double[] getDoubles() {
            return doubles;
        }

        public void setDoubles(final double[] doubles) {
            this.doubles = doubles;
        }

        public float[] getFloats() {
            return floats;
        }

        public void setFloats(final float[] floats) {
            this.floats = floats;
        }

        public short[] getShorts() {
            return shorts;
        }

        public void setShorts(final short[] shorts) {
            this.shorts = shorts;
        }

        public byte[] getBytes() {
            return bytes;
        }

        public void setBytes(final byte[] bytes) {
            this.bytes = bytes;
        }

        public Byte[] getBoxedBytes() {
            return boxedBytes;
        }

        public void setBoxedBytes(final Byte[] boxedBytes) {
            this.boxedBytes = boxedBytes;
        }

        public boolean[] getBools() {
            return bools;
        }

        public void setBools(final boolean[] bools) {
            this.bools = bools;
        }

        public char[] getChars() {
            return chars;
        }

        public void setChars(final char[] chars) {
            this.chars = chars;
        }

        public Character[] getBoxedChars() {
            return boxedChars;
        }

        public void setBoxedChars(final Character[] boxedChars) {
            this.boxedChars = boxedChars;
        }

        public ByteBuffer[] getBufs() {
            return bufs;
        }

        public void setBufs(final ByteBuffer[] bufs) {
            this.bufs = bufs;
        }

        public byte[][] getChunks() {
            return chunks;
        }

        public void setChunks(final byte[][] chunks) {
            this.chunks = chunks;
        }

        public ApplyMongoInner[] getInners() {
            return inners;
        }

        public void setInners(final ApplyMongoInner[] inners) {
            this.inners = inners;
        }

        public String[][] getNested() {
            return nested;
        }

        public void setNested(final String[][] nested) {
            this.nested = nested;
        }

        public int[][] getNestedInts() {
            return nestedInts;
        }

        public void setNestedInts(final int[][] nestedInts) {
            this.nestedInts = nestedInts;
        }

        public List<String[]> getRows() {
            return rows;
        }

        public void setRows(final List<String[]> rows) {
            this.rows = rows;
        }

        public Map<String, String[]> getByKey() {
            return byKey;
        }

        public void setByKey(final Map<String, String[]> byKey) {
            this.byKey = byKey;
        }

        public List<int[]> getIntRows() {
            return intRows;
        }

        public void setIntRows(final List<int[]> intRows) {
            this.intRows = intRows;
        }

        public ApplyMongoInner getInner() {
            return inner;
        }

        public void setInner(final ApplyMongoInner inner) {
            this.inner = inner;
        }

        public ApplyMongoRecord getRec() {
            return rec;
        }

        public void setRec(final ApplyMongoRecord rec) {
            this.rec = rec;
        }
    }

    // ---- end 2026-10-24 apply-mongo ----

    // ---- 2026-10-24 r2 sliceT ----

    /** The column names and the rows (as maps) of {@code ds}. */
    private static List<Object> r2sliceTShape(final Dataset ds) {
        return Arrays.asList(new ArrayList<>(ds.columnNames()), ds.toList(Map.class));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testExtractDataReadsAnEntityAnnotatedMapRowTypeLikeMapClass_r2sliceT() {
        // Beans.isBeanClass is also true for an @Entity-annotated Map subclass, so with a first row that is not a Map extractData took
        // its bean-target branch and copied each bean row into the Map subclass with Beans.copyAs: UnsupportedOperationException ("No
        // all arguments constructor found"), or, for a subclass declaring a property of its own, one row of its Map accessors
        // ([keySet, values, entrySet, size, owner, empty], all null) with the bean's values lost. It now reads like HashMap.class.
        final NoIdEntity bean = new NoIdEntity();
        bean.setValue("a");
        final List<Object> rows = Arrays.asList(bean, new Document("value", "b"));
        final MongoIterable<Object> iterable = org.mockito.Mockito.mock(MongoIterable.class);
        MongoIterableStubs.stubRows(iterable, rows);
        final List<Object> expected = Arrays.asList(Arrays.asList("value"),
                Arrays.asList(Collections.singletonMap("value", "a"), Collections.singletonMap("value", "b")));

        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals(expected, r2sliceTShape(MongoDBBase.extractData(rows, HashMap.class))),
                () -> assertEquals(expected, r2sliceTShape(MongoDBBase.extractData(rows, R2SliceTEntityMap.class))),
                () -> assertEquals(expected, r2sliceTShape(MongoDBBase.extractData(rows, R2SliceTOwnedMap.class))),
                () -> assertEquals(expected, r2sliceTShape(MongoDBBase.extractData(Arrays.asList("value"), rows, R2SliceTEntityMap.class))),
                () -> assertEquals(expected, r2sliceTShape(MongoDBBase.extractData(iterable, R2SliceTOwnedMap.class))));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testToEntityRejectsEntityAnnotatedMapSubclassesLikeEveryMapType_r2sliceT() {
        // toEntity mapped an @Entity-annotated Map subclass (Beans.isBeanClass accepts it) as a bean of its Map accessors: it threw a
        // misleading UnsupportedOperationException ("No all arguments constructor found"), or, for a subclass declaring a settable
        // property, silently returned a map without the document's entries ({} with owner = "x"). Every Map type is now rejected
        // alike (a plain HashMap already was, by Beans.mapToBean); readRow/toList/stream read a document into such a map.
        final Document doc = new Document("owner", "x").append("a", 1);
        final MongoIterable<Document> iterable = org.mockito.Mockito.mock(MongoIterable.class);
        MongoIterableStubs.stubRows(iterable, Arrays.asList(doc));

        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertTrue(
                        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toEntity(doc, R2SliceTEntityMap.class)).getMessage().contains("toMap")),
                () -> assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toEntity(doc, R2SliceTOwnedMap.class)),
                () -> assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toEntity(doc, HashMap.class)),
                () -> assertNull(MongoDBBase.toEntity(null, R2SliceTOwnedMap.class)),
                // Unchanged: the row readers copy the document's entries into such a map.
                () -> assertEquals(new LinkedHashMap<>(doc), MongoDBBase.readRow(doc, R2SliceTOwnedMap.class)),
                () -> assertEquals(Arrays.asList(new LinkedHashMap<>(doc)), MongoDBBase.toList(iterable, R2SliceTOwnedMap.class)));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testToListReadsAnArrayOrCollectionRowTypeFromTheSoleFieldUnlikeStream_r2sliceT() {
        // Pins the documented difference (decided not to change, 2026-09-27): for document rows toList treats an Object[]/String[]/List
        // row type as a single-value target, while stream(MongoCursor, Class) (readRow) returns the field values positionally.
        final Document single = new Document("_id", 7).append("tags", Arrays.asList("x", "y"));
        final Document multi = new Document("_id", 7).append("a", 1);
        final MongoIterable<Document> singleRows = org.mockito.Mockito.mock(MongoIterable.class);
        final MongoIterable<Document> multiRows = org.mockito.Mockito.mock(MongoIterable.class);
        MongoIterableStubs.stubRows(singleRows, Arrays.asList(single));
        MongoIterableStubs.stubRows(multiRows, Arrays.asList(new Document("_id", 7).append("a", 1).append("b", 2)));
        final MongoCursor<Document> cursor = org.mockito.Mockito.mock(MongoCursor.class);
        when(cursor.hasNext()).thenReturn(true, false);
        when(cursor.next()).thenReturn(multi);

        org.junit.jupiter.api.Assertions.assertAll(() -> assertArrayEquals(new String[] { "x", "y" }, MongoDBBase.toList(singleRows, String[].class).get(0)),
                () -> assertEquals(Arrays.asList("x", "y"), MongoDBBase.toList(singleRows, List.class).get(0)),
                () -> assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toList(multiRows, Object[].class)),
                () -> assertArrayEquals(new Object[] { 7, 1 }, MongoDBBase.stream(cursor, Object[].class).first().orElseThrow()));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testStreamsCloseTheirCursorWithTheInterruptStatusCleared_r2sliceT() {
        // stream(MongoIterable[, Class]) and stream(MongoCursor[, Class]) closed the cursor with Fn.close(cursor) on the calling thread
        // as is. Closed on an interrupted thread (a task cancelled with cancel(true) while consuming the stream), the sync driver skips
        // killCursors and the server cursor stays open until its idle timeout (proven live for the executors' streams, slice Q). The
        // cursor is now closed once, through closeCursor, and the interrupt status is restored.
        final List<R2SliceTRecordingCursor> opened = new ArrayList<>();
        final MongoIterable<Document> iterable = org.mockito.Mockito.mock(MongoIterable.class);
        org.mockito.Mockito.doAnswer(invocation -> {
            final R2SliceTRecordingCursor cursor = new R2SliceTRecordingCursor(Arrays.asList(new Document("value", "a"), new Document("value", "b")));
            opened.add(cursor);
            return cursor;
        }).when(iterable).iterator();

        final Map<String, java.util.function.Supplier<Stream<?>>> streams = new LinkedHashMap<>();
        streams.put("stream(iterable)", () -> MongoDBBase.stream(iterable));
        streams.put("stream(iterable, Map)", () -> MongoDBBase.stream(iterable, Map.class));
        streams.put("stream(iterable, NoIdEntity)", () -> MongoDBBase.stream(iterable, NoIdEntity.class));
        streams.put("stream(cursor)", () -> MongoDBBase.stream(iterable.iterator()));
        streams.put("stream(cursor, Document)", () -> MongoDBBase.stream(iterable.iterator(), Document.class));
        streams.put("stream(cursor, null)", () -> MongoDBBase.stream(iterable.iterator(), null));

        final java.util.stream.Stream<org.junit.jupiter.api.function.Executable> checks = streams.entrySet().stream().map(e -> () -> {
            final String label = e.getKey();
            opened.clear();
            final Stream<?> stream = e.getValue().get();
            assertNotNull(stream.iterator().next(), label);

            Thread.currentThread().interrupt();

            try {
                stream.close();
                stream.close();
                assertTrue(Thread.currentThread().isInterrupted(), label + ": interrupt status restored");
            } finally {
                Thread.interrupted();
            }

            assertEquals(1, opened.size(), label + ": cursors opened");
            assertEquals(1, opened.get(0).closeCount, label + ": close calls");
            assertTrue(!opened.get(0).interruptedAtClose, label + ": cursor closed on an interrupted thread");
        });

        org.junit.jupiter.api.Assertions.assertAll(checks);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testExtractDataWithoutRowsKeepsTheSelectedColumns_r2sliceT() {
        // Without a (non-null) row, extractData returned a Dataset without any column even when names were selected, so a projected
        // query that matched nothing (sync and reactive query(fields, ...) both end here) failed on getColumn("name") with
        // IllegalArgumentException, and its columns depended on whether a document matched. The selected names are now the columns,
        // named and validated by the same N.newDataset call as with rows.
        final List<String> names = Arrays.asList("name", "address.city");
        final List<String> flatNames = Arrays.asList("value");
        final List<Object> noRows = new ArrayList<>();
        final MongoIterable<Document> none = org.mockito.Mockito.mock(MongoIterable.class);
        MongoIterableStubs.stubRows(none, Collections.emptyList());

        final com.mongodb.client.MongoCollection<Document> collection = org.mockito.Mockito.mock(com.mongodb.client.MongoCollection.class);
        final FindIterable<Document> found = org.mockito.Mockito.mock(FindIterable.class);
        when(collection.find(org.mockito.ArgumentMatchers.any(Bson.class))).thenReturn(found);
        when(found.projection(org.mockito.ArgumentMatchers.any())).thenReturn(found);
        when(found.sort(org.mockito.ArgumentMatchers.any())).thenReturn(found);
        when(found.skip(org.mockito.ArgumentMatchers.anyInt())).thenReturn(found);
        when(found.limit(org.mockito.ArgumentMatchers.anyInt())).thenReturn(found);
        MongoIterableStubs.stubRows(found, Collections.emptyList());
        final MongoCollectionExecutor sync = new MongoDB(org.mockito.Mockito.mock(com.mongodb.client.MongoDatabase.class)).collectionExecutor(collection);

        final com.mongodb.reactivestreams.client.MongoCollection<Document> reactiveCollection = org.mockito.Mockito
                .mock(com.mongodb.reactivestreams.client.MongoCollection.class);
        final com.mongodb.reactivestreams.client.FindPublisher<Document> publisher = org.mockito.Mockito
                .mock(com.mongodb.reactivestreams.client.FindPublisher.class);
        when(reactiveCollection.find(org.mockito.ArgumentMatchers.any(Bson.class))).thenReturn(publisher);
        when(publisher.projection(org.mockito.ArgumentMatchers.any())).thenReturn(publisher);
        when(publisher.sort(org.mockito.ArgumentMatchers.any())).thenReturn(publisher);
        when(publisher.skip(org.mockito.ArgumentMatchers.anyInt())).thenReturn(publisher);
        when(publisher.limit(org.mockito.ArgumentMatchers.anyInt())).thenReturn(publisher);
        org.mockito.Mockito.doAnswer(invocation -> {
            reactor.core.publisher.Flux.<Document> empty().subscribe((org.reactivestreams.Subscriber<? super Document>) invocation.getArgument(0));
            return null;
        }).when(publisher).subscribe(org.mockito.ArgumentMatchers.any());
        final com.landawn.abacus.da.mongodb.reactivestreams.MongoCollectionExecutor reactive = new com.landawn.abacus.da.mongodb.reactivestreams.MongoDB(
                org.mockito.Mockito.mock(com.mongodb.reactivestreams.client.MongoDatabase.class)).collectionExecutor(reactiveCollection);

        final java.util.stream.Stream<org.junit.jupiter.api.function.Executable> perRowType = java.util.stream.Stream
                .of(Map.class, HashMap.class, Document.class, NoIdEntity.class, R2SliceTOwnedMap.class)
                .map(rowType -> () -> {
                    final Dataset ds = MongoDBBase.extractData(names, noRows, rowType);
                    assertEquals(names, ds.columnNames(), rowType.getSimpleName());
                    assertEquals(0, ds.size(), rowType.getSimpleName());
                });

        final java.util.stream.Stream<org.junit.jupiter.api.function.Executable> paths = java.util.stream.Stream.of(
                () -> assertEquals(names, MongoDBBase.extractData(names, (List<?>) null, Map.class).columnNames()),
                () -> assertEquals(names, MongoDBBase.extractData(names, none, NoIdEntity.class).columnNames()),
                () -> assertTrue(MongoDBBase.extractData(names, noRows, Map.class).getColumn("name").isEmpty()),
                // The order of the selected names is kept, as with rows.
                () -> assertEquals(Arrays.asList("b", "a"),
                        MongoDBBase.extractData(new java.util.LinkedHashSet<>(Arrays.asList("b", "a")), noRows, Map.class).columnNames()),
                // The executors (sync: dotted names through fetchAll, flat names through the MongoIterable overload; reactive both).
                () -> assertEquals(names, sync.query(names, new Document(), Map.class).columnNames()),
                () -> assertEquals(flatNames, sync.query(flatNames, new Document(), NoIdEntity.class).columnNames()),
                () -> assertEquals(names, reactive.query(names, new Document(), Map.class).block().columnNames()),
                () -> assertEquals(flatNames, reactive.query(flatNames, new Document(), NoIdEntity.class).block().columnNames()),
                // Names are validated as with rows: duplicates fail whether or not a row exists.
                () -> assertThrows(IllegalArgumentException.class, () -> MongoDBBase.extractData(Arrays.asList("a", "a"), noRows, Map.class)),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> MongoDBBase.extractData(Arrays.asList("a", "a"), Arrays.asList(new Document("a", 1)), Map.class)),
                // Null rows give rows of nulls, as among other rows.
                () -> assertEquals(2, MongoDBBase.extractData(names, Arrays.asList(null, null), Map.class).size()),
                // Unchanged: without selected names the empty Dataset has no columns.
                () -> assertTrue(MongoDBBase.extractData(null, noRows, Map.class).columnNames().isEmpty()),
                () -> assertTrue(MongoDBBase.extractData(Collections.emptyList(), noRows, Map.class).columnNames().isEmpty()),
                () -> assertTrue(MongoDBBase.extractData(none, Map.class).columnNames().isEmpty()));

        org.junit.jupiter.api.Assertions.assertAll(java.util.stream.Stream.concat(perRowType, paths));
    }

    /** A cursor over {@code rows} recording how often it was closed and whether the closing thread was interrupted. */
    private static final class R2SliceTRecordingCursor implements MongoCursor<Document> {
        private final List<Document> rows;
        private int position;
        int closeCount;
        boolean interruptedAtClose;

        R2SliceTRecordingCursor(final List<Document> rows) {
            this.rows = rows;
        }

        @Override
        public void close() {
            closeCount++;
            interruptedAtClose = Thread.currentThread().isInterrupted();
        }

        @Override
        public boolean hasNext() {
            return position < rows.size();
        }

        @Override
        public Document next() {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException();
            }

            return rows.get(position++);
        }

        @Override
        public int available() {
            return rows.size() - position;
        }

        @Override
        public Document tryNext() {
            return hasNext() ? next() : null;
        }

        @Override
        public com.mongodb.ServerCursor getServerCursor() {
            return null;
        }

        @Override
        public com.mongodb.ServerAddress getServerAddress() {
            return null;
        }
    }

    @com.landawn.abacus.annotation.Entity
    public static class R2SliceTEntityMap extends HashMap<String, Object> {
        private static final long serialVersionUID = 1L;
    }

    @com.landawn.abacus.annotation.Entity
    public static class R2SliceTOwnedMap extends LinkedHashMap<String, Object> {
        private static final long serialVersionUID = 1L;
        private String owner;

        public String getOwner() {
            return owner;
        }

        public void setOwner(final String owner) {
            this.owner = owner;
        }
    }

    // ---- end 2026-10-24 r2 sliceT ----

    public static class BinaryEntity {
        private ByteBuffer buffer;
        private byte[] bytes;
        private BinaryEntity child;
        private List<ByteBuffer> buffers;
        private List<byte[]> byteArrays;
        private List<Object> rawValues;
        private Map<String, ByteBuffer> bufferMap;
        private Map<String, byte[]> byteArrayMap;
        private Map<String, Object> rawMap;
        private ByteBuffer[] bufferArray;
        private byte[][] byteArrayArray;
        private Object[] rawArray;

        public ByteBuffer getBuffer() {
            return buffer;
        }

        public void setBuffer(final ByteBuffer buffer) {
            this.buffer = buffer;
        }

        public byte[] getBytes() {
            return bytes;
        }

        public void setBytes(final byte[] bytes) {
            this.bytes = bytes;
        }

        public BinaryEntity getChild() {
            return child;
        }

        public void setChild(final BinaryEntity child) {
            this.child = child;
        }

        public List<ByteBuffer> getBuffers() {
            return buffers;
        }

        public void setBuffers(final List<ByteBuffer> buffers) {
            this.buffers = buffers;
        }

        public List<byte[]> getByteArrays() {
            return byteArrays;
        }

        public void setByteArrays(final List<byte[]> byteArrays) {
            this.byteArrays = byteArrays;
        }

        public List<Object> getRawValues() {
            return rawValues;
        }

        public void setRawValues(final List<Object> rawValues) {
            this.rawValues = rawValues;
        }

        public Map<String, ByteBuffer> getBufferMap() {
            return bufferMap;
        }

        public void setBufferMap(final Map<String, ByteBuffer> bufferMap) {
            this.bufferMap = bufferMap;
        }

        public Map<String, byte[]> getByteArrayMap() {
            return byteArrayMap;
        }

        public void setByteArrayMap(final Map<String, byte[]> byteArrayMap) {
            this.byteArrayMap = byteArrayMap;
        }

        public Map<String, Object> getRawMap() {
            return rawMap;
        }

        public void setRawMap(final Map<String, Object> rawMap) {
            this.rawMap = rawMap;
        }

        public ByteBuffer[] getBufferArray() {
            return bufferArray;
        }

        public void setBufferArray(final ByteBuffer[] bufferArray) {
            this.bufferArray = bufferArray;
        }

        public byte[][] getByteArrayArray() {
            return byteArrayArray;
        }

        public void setByteArrayArray(final byte[][] byteArrayArray) {
            this.byteArrayArray = byteArrayArray;
        }

        public Object[] getRawArray() {
            return rawArray;
        }

        public void setRawArray(final Object[] rawArray) {
            this.rawArray = rawArray;
        }
    }

    public static class TestEntity {
        private String id;
        private String name;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static class ObjectIdEntity {
        private ObjectId id;
        private String name;

        public ObjectId getId() {
            return id;
        }

        public void setId(ObjectId id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static class NoIdEntity {
        private String value;

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    public static class OtherNoIdEntity {
        private String value;

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    public static class IntIdEntity {
        private int id;
        private String name;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}
