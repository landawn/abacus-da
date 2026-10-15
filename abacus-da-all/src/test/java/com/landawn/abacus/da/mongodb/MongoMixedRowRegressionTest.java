package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.da.TestBase;
import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoIterable;

public class MongoMixedRowRegressionTest extends TestBase {

    @Test
    public void documentAndMapRowsConvertToBeansInEitherOrder() {
        final ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
        final Document document = new Document("_id", id).append("name", "document");
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("_id", id);
        map.put("name", "map");
        final BasicDBObject legacyDocument = new BasicDBObject("_id", id).append("name", "legacy");

        for (final List<?> rows : List.of(Arrays.asList(document, null, map, legacyDocument), Arrays.asList(map, null, legacyDocument, document))) {
            final List<Row> converted = MongoDBBase.toList(iterable(rows), Row.class);

            assertEquals(4, converted.size());
            assertNull(converted.get(1));
            for (final int index : new int[] { 0, 2, 3 }) {
                assertEquals(id.toHexString(), converted.get(index).getId());
                assertEquals(((Map<?, ?>) rows.get(index)).get("name"), converted.get(index).getName());
            }
        }

        assertSame(id, document.get("_id"));
        assertSame(id, map.get("_id"));
        assertSame(id, legacyDocument.get("_id"));
    }

    @Test
    public void documentAndMapRowsConvertToConcreteMapsInEitherOrder() {
        final Document document = new Document("name", "document");
        final Map<String, Object> map = Map.of("name", "map");

        for (final List<?> rows : List.of(Arrays.asList(document, null, map), Arrays.asList(map, null, document))) {
            final List<TreeMap> converted = MongoDBBase.toList(iterable(rows), TreeMap.class);

            assertEquals(rows.size(), converted.size());
            assertEquals(TreeMap.class, converted.get(0).getClass());
            assertEquals(((Map<?, ?>) rows.get(0)).get("name"), converted.get(0).get("name"));
            assertNull(converted.get(1));
            assertEquals(TreeMap.class, converted.get(2).getClass());
            assertEquals(((Map<?, ?>) rows.get(2)).get("name"), converted.get(2).get("name"));
        }
    }

    @Test
    public void documentRowsUseCapacityAwareMapConstructors() {
        final Document first = new Document("name", "first").append("count", 2);
        final Document second = new Document("name", "second");

        final List<CapacityMap> converted = MongoDBBase.toList(iterable(Arrays.asList(first, null, second)), CapacityMap.class);

        assertEquals(new LinkedHashMap<>(first), converted.get(0));
        assertEquals(2, converted.get(0).expectedSize);
        assertNull(converted.get(1));
        assertEquals(new LinkedHashMap<>(second), converted.get(2));
        assertEquals(1, converted.get(2).expectedSize);
    }

    @Test
    public void mixedMapRowsUseOneMapFactoryChosenByTheFirstRow() {
        final Row bean = new Row();
        bean.setId("bean-id");
        bean.setName("bean");
        final Document document = new Document("name", "document").append("count", 2);
        final Map<String, Object> map = Map.of("name", "map");

        // A Document result keeps its size-aware creators for every row: other rows were built by a no-argument supplier
        // that this map type lacks.
        final List<CapacityMap> sized = MongoDBBase.toList(iterable(Arrays.asList(document, map, bean)), CapacityMap.class);
        assertEquals(List.of(2, 1), List.of(sized.get(0).expectedSize, sized.get(1).expectedSize));
        assertEquals(new LinkedHashMap<>(map), sized.get(1));
        assertEquals(Map.of("id", "bean-id", "name", "bean"), sized.get(2));

        // A mixed result is not built two ways: a Document first row selects the size-aware factory for every row, and any
        // other first row keeps the no-argument supplier for every row, including a later Document, as before.
        for (final Map.Entry<List<?>, String> example : List.<Map.Entry<List<?>, String>> of(Map.entry(Arrays.asList(document, map, bean), "sized"),
                Map.entry(Arrays.asList(map, document), "supplied"), Map.entry(Arrays.asList(bean, document, map), "supplied"))) {
            for (final TrackingMap converted : MongoDBBase.toList(iterable(example.getKey()), TrackingMap.class)) {
                assertEquals(example.getValue(), converted.creator);
            }
        }
    }

    @Test
    public void extractDataConvertsMixedRowsIndividually() {
        final Row bean = new Row();
        bean.setId("bean-id");
        bean.setName("bean");
        final Document document = new Document("id", "document-id").append("name", "document");
        final Map<String, Object> map = Map.of("id", "map-id", "name", "map");

        // A Document first row cast every later row to Document, and Map rows were iterated as maps: both failed a cast.
        assertEquals(List.of("document", "map", "bean"), MongoDBBase.extractData(Arrays.asList(document, map, bean), Row.class).getColumn("name"));
        assertEquals(List.of("map", "bean"), MongoDBBase.extractData(Arrays.asList(map, bean), Map.class).getColumn("name"));
        assertEquals(List.of("document", "bean"), MongoDBBase.extractData(Arrays.asList(document, bean), Map.class).getColumn("name"));
    }

    @Test
    public void mixedDocumentMapAndBeanRowsConvertWithLeadingNulls() {
        final Row bean = new Row();
        bean.setId("bean-id");
        bean.setName("bean");
        final Document document = new Document("id", "document-id").append("name", "document");
        final Map<String, Object> map = Map.of("id", "map-id", "name", "map");

        for (final List<?> rows : List.of(Arrays.asList(null, document, map, bean), Arrays.asList(null, bean, map, document))) {
            final List<Row> beans = MongoDBBase.toList(iterable(rows), Row.class);
            final List<TreeMap> maps = MongoDBBase.toList(iterable(rows), TreeMap.class);
            assertNull(beans.get(0));
            assertNull(maps.get(0));
            assertEquals(4, beans.size());
            assertEquals(4, maps.size());

            for (int i = 1; i < rows.size(); i++) {
                final Object source = rows.get(i);
                final String expectedId = source == bean ? bean.getId() : (String) ((Map<?, ?>) source).get("id");
                final String expectedName = source == bean ? bean.getName() : (String) ((Map<?, ?>) source).get("name");
                assertEquals(expectedId, beans.get(i).getId());
                assertEquals(expectedName, beans.get(i).getName());
                assertEquals(Map.of("id", expectedId, "name", expectedName), maps.get(i));
            }
        }

        assertEquals("bean-id", bean.getId());
        assertEquals("bean", bean.getName());
    }

    @Test
    public void identityMapKeysUseDocumentIdentifierSemantics() {
        final ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
        final Map<String, Object> source = new IdentityHashMap<>();
        final String idField = new String("_id");
        source.put(idField, id);

        final AnnotatedRow converted = MongoDBBase.toList(iterable(List.of(source)), AnnotatedRow.class).get(0);

        assertEquals(id.toHexString(), converted.getKey());
        assertSame(id, source.get(idField));
        assertEquals(1, source.size());
    }

    @Test
    public void caseInsensitiveMapKeysUseDocumentIdentifierSemantics() {
        final ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
        final Map<String, Object> source = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        source.put("_ID", id);
        source.put("key", "own-key");

        final AnnotatedRow converted = MongoDBBase.toList(iterable(List.of(source)), AnnotatedRow.class).get(0);

        assertEquals("own-key", converted.getKey());
        assertSame(id, source.get("_ID"));
        assertEquals(2, source.size());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static MongoIterable<?> iterable(final List<?> rows) {
        final MongoIterable<Object> result = mock(MongoIterable.class);
        when(result.into(any())).thenAnswer(invocation -> {
            final List<Object> target = invocation.getArgument(0);
            target.addAll(rows);
            return target;
        });
        return result;
    }

    public static class Row {
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

    public static class AnnotatedRow {
        @Id
        private String key;

        public String getKey() {
            return key;
        }

        public void setKey(final String key) {
            this.key = key;
        }
    }

    public static class TrackingMap extends LinkedHashMap<String, Object> {
        private final String creator;

        public TrackingMap() {
            creator = "supplied";
        }

        public TrackingMap(final int expectedSize) {
            super(expectedSize);
            creator = "sized";
        }
    }

    public static class CapacityMap extends LinkedHashMap<String, Object> {
        private final int expectedSize;

        public CapacityMap(final int expectedSize) {
            super(expectedSize);
            this.expectedSize = expectedSize;
        }
    }
}
