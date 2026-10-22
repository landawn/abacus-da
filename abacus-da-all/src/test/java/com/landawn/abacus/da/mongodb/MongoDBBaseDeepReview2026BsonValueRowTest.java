package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.Symbol;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.util.Beans;
import com.landawn.abacus.util.Dataset;
import com.mongodb.client.MongoIterable;

/**
 * {@code Beans.isBeanClass} is true for the BSON value types with getters (Binary, Code, Symbol, BsonBinary, BsonString, ...).
 * {@code extractData} must still treat such a value in a scalar position as a scalar, as its Javadoc says, instead of
 * reading it as a bean of its own properties.
 */
class MongoDBBaseDeepReview2026BsonValueRowTest {

    public static class Holder {
        private Object v;

        public Object getV() {
            return v;
        }

        public void setV(final Object v) {
            this.v = v;
        }
    }

    @Test
    void bsonValueTypesWithGettersAreBeanClasses() {
        // The premise of this test: if the library stops seeing these as beans, the guards below become redundant but stay correct.
        assertTrue(Beans.isBeanClass(Binary.class));
        assertTrue(Beans.isBeanClass(Code.class));
        assertTrue(Beans.isBeanClass(Symbol.class));
        assertTrue(Beans.isBeanClass(BsonString.class));
    }

    @Test
    void mapTargetWithSelectedColumnsReadsBsonValueRowsAsScalars() {
        final Binary binary = new Binary(new byte[] { 1, 2 });
        final Code code = new Code("function() {}");
        final Symbol symbol = new Symbol("sym");
        final BsonString bsonString = new BsonString("text");
        final BsonInt32 bsonInt = new BsonInt32(7);

        // Map first row: previously the Binary row became {v: null} (Beans.beanToMap read its type/data properties).
        final Dataset dataset = MongoDBBase.extractData(List.of("v"), Arrays.asList(new Document("v", 1), binary, code, symbol, bsonString, bsonInt),
                Map.class);
        final List<Map> rows = dataset.toList(Map.class);

        assertEquals(6, rows.size());
        assertEquals(1, rows.get(0).get("v"));
        assertSame(binary, rows.get(1).get("v"));
        assertSame(code, rows.get(2).get("v"));
        assertSame(symbol, rows.get(3).get("v"));
        assertSame(bsonString, rows.get(4).get("v"));
        assertSame(bsonInt, rows.get(5).get("v"));
    }

    @Test
    void mapTargetWithoutSelectedColumnsRejectsBsonValueRowsLikeOtherScalars() {
        final List<?> rows = Arrays.asList(new Document("v", 1), new Binary(new byte[] { 1, 2 }));

        // Previously the Binary row silently contributed "type" and "data" columns.
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> MongoDBBase.extractData(null, rows, Map.class));
        assertTrue(e.getMessage().contains("Binary"), e.getMessage());
    }

    @Test
    void beanTargetPassesBsonValueRowsThroughInsteadOfCopyingThemIntoEmptyBeans() {
        final Binary binary = new Binary(new byte[] { 1, 2 });

        // Document first row, bean target.
        List<Map> rows = MongoDBBase.extractData(List.of("v"), Arrays.asList(new Document("v", binary), binary), Holder.class).toList(Map.class);
        assertEquals(2, rows.size());
        assertSame(binary, rows.get(0).get("v"));
        assertSame(binary, rows.get(1).get("v"));

        // Bean first row, bean target: previously Beans.copyAs(binary, Holder.class) produced a Holder with v == null.
        final Holder holder = new Holder();
        holder.setV(binary);
        rows = MongoDBBase.extractData(List.of("v"), Arrays.asList(holder, binary), Holder.class).toList(Map.class);
        assertEquals(2, rows.size());
        assertSame(binary, rows.get(0).get("v"));
        assertSame(binary, rows.get(1).get("v"));
    }

    @Test
    void bsonContainersAreStillMapsAndCollections() {
        // BsonDocument is a Map and BsonArray a List: they keep their container role.
        final BsonDocument bsonDocument = new BsonDocument("v", new BsonInt32(1));
        final List<Map> rows = MongoDBBase.extractData(List.of("v"), Arrays.asList(new Document("v", 0), bsonDocument), Map.class).toList(Map.class);

        assertEquals(2, rows.size());
        assertEquals(new BsonInt32(1), rows.get(1).get("v"));

        final List<?> positional = Arrays.asList(new Document("v", 0), new BsonArray(List.of(new BsonInt32(2))));
        assertEquals(new BsonInt32(2), MongoDBBase.extractData(List.of("v"), positional, Map.class).toList(Map.class).get(1).get("v"));
    }

    // ---- 2026-10-20 bsonrow ----

    /**
     * The extractData guard must not widen toList's scalar conversion: convertBsonValue does not unwrap Code, Symbol or a
     * BsonValue (a BsonString read as a String became the JSON text {"value": "x"}, a BsonInt32 read as an Integer threw
     * NumberFormatException). Such rows stay rejected with the clear IllegalArgumentException; Binary still converts.
     */
    @Test
    void bsonrow_toListRejectsUnwrappableBsonValueRowsAndStillConvertsBinary() {
        final List<Object[]> rejected = List.of(new Object[] { new BsonString("x"), String.class }, new Object[] { new Code("f()"), String.class },
                new Object[] { new Symbol("s"), String.class }, new Object[] { new BsonInt32(5), Integer.class });

        for (final Object[] c : rejected) {
            final MongoIterable<Object> iterable = mock(MongoIterable.class);
            MongoIterableStubs.stubRows(iterable, List.of(c[0]));

            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toList(iterable, (Class<?>) c[1]),
                    String.valueOf(c[0]));
            assertTrue(e.getMessage().startsWith("Cannot convert document"), e.getMessage());
        }

        final MongoIterable<Object> binaries = mock(MongoIterable.class);
        MongoIterableStubs.stubRows(binaries, List.of(new Binary(new byte[] { 1, 2 })));
        assertArrayEquals(new byte[] { 1, 2 }, MongoDBBase.toList(binaries, byte[].class).get(0));
    }

    /**
     * The last extractData branch (a first row that is neither a Map nor a Document, and a non-bean target) passed BSON value
     * rows to the Dataset as is, which read them as beans: a Binary after a positional row left the column null, and a Binary
     * first row produced {type, data} columns (or a null selected column).
     */
    @Test
    void bsonrow_firstRowOrPositionalFirstRowReadsBsonValueRowsAsScalars() {
        final Binary binary = new Binary(new byte[] { 1, 2 });
        final Code code = new Code("f()");

        final List<Object[]> positional = MongoDBBase.extractData(List.of("v"), Arrays.asList(new Object[] { 1 }, binary, null, code), Object[].class)
                .toList(Object[].class);
        assertEquals(4, positional.size());
        assertEquals(1, positional.get(0)[0]);
        assertSame(binary, positional.get(1)[0]);
        assertSame(code, positional.get(3)[0]);

        // A Binary first row with a Map target (no Map first row, so not the Map branch).
        final List<Map> maps = MongoDBBase.extractData(List.of("v"), Arrays.asList(binary, new Object[] { 2 }), Map.class).toList(Map.class);
        assertSame(binary, maps.get(0).get("v"));
        assertEquals(2, maps.get(1).get("v"));

        // Like any other scalar row: rejected without exactly one selected column.
        final IllegalArgumentException noColumns = assertThrows(IllegalArgumentException.class,
                () -> MongoDBBase.extractData(null, List.of(binary), Map.class));
        assertTrue(noColumns.getMessage().contains("without selectPropNames"), noColumns.getMessage());
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.extractData(List.of("a", "b"), List.of(binary), Object[].class));

        // Rows without a BSON value are handed to the Dataset unchanged.
        assertEquals("str", MongoDBBase.extractData(List.of("v"), List.of("str"), Map.class).toList(Map.class).get(0).get("v"));
    }

    /**
     * The Document-first branch with a target that is neither a bean nor a Map (Object[]): a later BSON value row is not a
     * bean-to-bean copy, so it is read as the scalar of the single selected column, like any other scalar row.
     */
    @Test
    void bsonrow_documentFirstRowWithObjectArrayTargetReadsBsonValueRowAsScalar() {
        final Binary binary = new Binary(new byte[] { 1, 2 });
        final Code code = new Code("f()");

        final List<Object[]> rows = MongoDBBase.extractData(List.of("v"), Arrays.asList(new Document("v", 1), binary, code), Object[].class)
                .toList(Object[].class);

        assertEquals(3, rows.size());
        assertEquals(1, rows.get(0)[0]);
        assertSame(binary, rows.get(1)[0]);
        assertSame(code, rows.get(2)[0]);
    }
}
