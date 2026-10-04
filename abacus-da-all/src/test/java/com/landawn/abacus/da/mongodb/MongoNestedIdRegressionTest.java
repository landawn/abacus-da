package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.TestBase;
import com.mongodb.client.MongoIterable;

public class MongoNestedIdRegressionTest extends TestBase {

    @Test
    public void nestedBeanPreservesExplicitIdMapping() {
        final ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
        final Document child = new Document("_id", id).append("name", "child");
        final Document source = new Document("child", child);

        final Parent result = MongoDBBase.toEntity(source, Parent.class);

        assertEquals(id.toHexString(), result.getChild().getKey());
        assertEquals("child", result.getChild().getName());
        assertSame(id, child.get("_id"));
        assertSame(child, source.get("child"));
    }

    @Test
    public void typedBeanContainersPreserveExplicitIdMapping() {
        final ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
        final Document child = new Document("_id", id).append("name", "child");
        final Document source = new Document("children", List.of(child)).append("byName", Map.of("first", child)).append("array", List.of(child));

        final Parent result = MongoDBBase.toEntity(source, Parent.class);

        assertEquals(id.toHexString(), result.getChildren().get(0).getKey());
        assertEquals(id.toHexString(), result.getByName().get("first").getKey());
        assertEquals(id.toHexString(), result.getArray()[0].getKey());
        assertSame(id, child.get("_id"));
    }

    @Test
    public void nestedRecordsPreserveExplicitIdMapping() {
        final ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
        final Document child = new Document("_id", id).append("name", "child");
        final RecordParent result = MongoDBBase.toEntity(new Document("child", child).append("children", List.of(child)), RecordParent.class);

        assertEquals(id.toHexString(), result.child().key());
        assertEquals(id.toHexString(), result.children().get(0).key());
    }

    @Test
    public void codecRoundTripPreservesNestedId() {
        final Child child = new Child();
        child.setKey("507f1f77bcf86cd799439011");
        child.setName("child");
        final Parent source = new Parent();
        source.setChild(child);
        final Codec<Parent> codec = MongoDBBase.codecRegistry.get(Parent.class);
        final BsonDocument stored = new BsonDocument();
        codec.encode(new BsonDocumentWriter(stored), source, EncoderContext.builder().build());

        assertEquals(new ObjectId(child.getKey()), stored.getDocument("child").getObjectId("_id").getValue());
        final Parent decoded = codec.decode(new BsonDocumentReader(stored), DecoderContext.builder().build());

        assertEquals(child.getKey(), decoded.getChild().getKey());
        assertEquals(child.getName(), decoded.getChild().getName());
    }

    @Test
    public void nestedGenericBeansKeepIdAndDeclaredValueType() {
        final ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
        final Document child = new Document("_id", id).append("value", 7);
        final GenericParent result = MongoDBBase.toEntity(new Document("child", child).append("children", List.of(child)), GenericParent.class);

        assertEquals(id.toHexString(), result.child().getKey());
        assertEquals(Long.valueOf(7), result.child().getValue());
        assertEquals(id.toHexString(), result.children().get(0).getKey());
        assertEquals(Long.valueOf(7), result.children().get(0).getValue());
        assertSame(id, child.get("_id"));
        assertEquals(Integer.valueOf(7), child.get("value"));
    }

    @Test
    public void nestedRecordOwnIdRetainsPrecedenceAndObjectIdType() {
        final ObjectId generatedId = new ObjectId("507f1f77bcf86cd799439011");
        final ObjectId ownId = new ObjectId("507f1f77bcf86cd799439012");
        final Document source = new Document("child", new Document("_id", generatedId).append("key", "own-key").append("name", "child"))
                .append("objectIdChild", new Document("_id", ownId).append("name", "object-id-child"));

        final MixedRecordParent result = MongoDBBase.toEntity(source, MixedRecordParent.class);

        assertEquals("own-key", result.child().key());
        assertSame(ownId, result.objectIdChild().key());
        assertSame(generatedId, ((Document) source.get("child")).get("_id"));
    }

    @Test
    public void nestedStringIdRetainsOwnValueAcrossContainers() {
        final ObjectId generatedId = new ObjectId("507f1f77bcf86cd799439011");
        for (final Document child : List.of(new Document("_id", generatedId).append("id", "sku-1"),
                new Document("id", "sku-1").append("_id", generatedId))) {
            final Document original = new Document(child);
            final StringIdParent result = MongoDBBase.toEntity(withContainers(child), StringIdParent.class);

            assertEquals("sku-1", result.child().getId());
            assertEquals("sku-1", result.children().get(0).getId());
            assertEquals("sku-1", result.byName().get("first").getId());
            assertEquals("sku-1", result.array()[0].getId());
            assertEquals(original, child);
        }
    }

    @Test
    public void nestedObjectIdRetainsOwnValueAcrossContainers() {
        final ObjectId generatedId = new ObjectId("507f1f77bcf86cd799439011");
        final ObjectId ownId = new ObjectId("507f1f77bcf86cd799439012");
        for (final Document child : List.of(new Document("_id", generatedId).append("id", ownId),
                new Document("id", ownId).append("_id", generatedId))) {
            final Document original = new Document(child);
            final ObjectIdParent result = MongoDBBase.toEntity(withContainers(child), ObjectIdParent.class);

            assertEquals(ownId, result.child().getId());
            assertEquals(ownId, result.children().get(0).getId());
            assertEquals(ownId, result.byName().get("first").getId());
            assertEquals(ownId, result.array()[0].getId());
            assertEquals(original, child);
        }
    }

    @Test
    public void nestedAnnotatedIdRecognizesOwnFieldAndAlias() {
        final ObjectId generatedId = new ObjectId("507f1f77bcf86cd799439011");
        for (final String field : List.of("itemId", "item_id")) {
            for (final String ownId : Arrays.asList("sku-1", null)) {
                final Document child = new Document("_id", generatedId).append(field, ownId);
                final AliasedIdParent result = MongoDBBase.toEntity(withContainers(child), AliasedIdParent.class);

                assertEquals(ownId, result.child().getItemId(), field);
                assertEquals(ownId, result.children().get(0).getItemId(), field);
                assertEquals(new Document("_id", generatedId).append(field, ownId), child);
            }
        }
    }

    @Test
    public void nestedExplicitNullIdsRetainPrecedence() {
        final ObjectId generatedId = new ObjectId("507f1f77bcf86cd799439011");
        final Document child = new Document("_id", generatedId).append("id", null);
        final StringIdParent strings = MongoDBBase.toEntity(withContainers(child), StringIdParent.class);
        final ObjectIdParent objects = MongoDBBase.toEntity(withContainers(child), ObjectIdParent.class);

        assertNull(strings.child().getId());
        assertNull(strings.children().get(0).getId());
        assertNull(strings.byName().get("first").getId());
        assertNull(strings.array()[0].getId());
        assertNull(objects.child().getId());
        assertNull(objects.children().get(0).getId());
        assertNull(objects.byName().get("first").getId());
        assertNull(objects.array()[0].getId());
        assertEquals(new Document("_id", generatedId).append("id", null), child);
    }

    @Test
    public void nestedMissingOwnIdsStillUseMongoId() {
        final ObjectId generatedId = new ObjectId("507f1f77bcf86cd799439011");
        final Document child = new Document("_id", generatedId);
        final StringIdParent strings = MongoDBBase.toEntity(withContainers(child), StringIdParent.class);
        final ObjectIdParent objects = MongoDBBase.toEntity(withContainers(child), ObjectIdParent.class);
        final AliasedIdParent annotated = MongoDBBase.toEntity(withContainers(child), AliasedIdParent.class);

        assertEquals(generatedId.toHexString(), strings.child().getId());
        assertEquals(generatedId.toHexString(), strings.children().get(0).getId());
        assertEquals(generatedId.toHexString(), strings.byName().get("first").getId());
        assertEquals(generatedId.toHexString(), strings.array()[0].getId());
        assertEquals(generatedId, objects.child().getId());
        assertEquals(generatedId, objects.children().get(0).getId());
        assertEquals(generatedId, objects.byName().get("first").getId());
        assertEquals(generatedId, objects.array()[0].getId());
        assertEquals(generatedId.toHexString(), annotated.child().getItemId());
        assertEquals(generatedId.toHexString(), annotated.children().get(0).getItemId());
        assertEquals(new Document("_id", generatedId), child);
    }

    @Test
    public void topLevelMongoIdRetainsPrecedenceOverOwnId() {
        final ObjectId generatedId = new ObjectId("507f1f77bcf86cd799439011");
        for (final Object ownId : Arrays.asList(new ObjectId("507f1f77bcf86cd799439012"), null)) {
            final Document child = new Document("_id", generatedId).append("id", ownId);

            assertEquals(generatedId.toHexString(), MongoDBBase.toEntity(child, StringIdChild.class).getId());
            assertEquals(generatedId, MongoDBBase.toEntity(child, ObjectIdChild.class).getId());
            assertEquals(new Document("_id", generatedId).append("id", ownId), child);
        }
        for (final String field : List.of("itemId", "item_id")) {
            assertEquals(generatedId.toHexString(),
                    MongoDBBase.toEntity(new Document("_id", generatedId).append(field, "sku-1"), AliasedIdChild.class).getItemId());
        }
    }

    private static Document withContainers(final Document child) {
        return new Document("child", child).append("children", List.of(child)).append("byName", Map.of("first", child)).append("array", List.of(child));
    }

    @Test
    public void nestedNumericBeanIdsSurvivePropertiesAndContainers() {
        final Document child = new Document("_id", 5);
        final Document source = new Document("child", child).append("children", List.of(child)).append("byName", Map.of("first", child))
                .append("array", List.of(child));

        final NumericParent result = MongoDBBase.toEntity(source, NumericParent.class);

        assertEquals(Long.valueOf(5), result.child().getId());
        assertEquals(Long.valueOf(5), result.children().get(0).getId());
        assertEquals(Long.valueOf(5), result.byName().get("first").getId());
        assertEquals(Long.valueOf(5), result.array()[0].getId());
        assertEquals(new Document("_id", 5), child);
        assertSame(child, source.get("child"));
    }

    @Test
    public void nestedUuidBeanIdsSurvivePropertiesAndContainers() {
        final UUID id = UUID.fromString("b6b6b6b6-1111-2222-3333-444444444444");
        final Document child = new Document("_id", id);
        final UuidParent result = MongoDBBase.toEntity(new Document("child", child).append("children", List.of(child)), UuidParent.class);

        assertEquals(id, result.child().getId());
        assertEquals(id, result.children().get(0).getId());
        assertEquals(new Document("_id", id), child);
    }

    @Test
    public void nestedNumericRecordIdsSurvivePropertiesAndContainers() {
        final Document child = new Document("_id", 5).append("name", "child");
        final NumericRecordParent result = MongoDBBase.toEntity(new Document("child", child).append("children", List.of(child)), NumericRecordParent.class);

        assertEquals(new NumericRecord(5L, "child"), result.child());
        assertEquals(List.of(new NumericRecord(5L, "child")), result.children());
        assertEquals(new Document("_id", 5).append("name", "child"), child);
    }

    @Test
    public void topLevelNumericIdsRetainExistingMappingRules() {
        for (final Object id : List.of(5L, new ObjectId("507f1f77bcf86cd799439011"))) {
            final Document source = new Document("_id", id);
            assertNull(MongoDBBase.toEntity(source, NumericChild.class).getId());
            assertNull(MongoDBBase.toEntity(source, NumericRecord.class).id());
            assertSame(id, source.get("_id"));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void allNullIterablePreservesResultCardinality() {
        final MongoIterable<Object> iterable = (MongoIterable<Object>) Proxy.newProxyInstance(MongoIterable.class.getClassLoader(),
                new Class<?>[] { MongoIterable.class }, (proxy, method, args) -> {
                    if (method.getName().equals("into")) {
                        ((Collection<Object>) args[0]).addAll(Arrays.asList(null, null));
                        return args[0];
                    }

                    throw new UnsupportedOperationException(method.getName());
                });

        assertEquals(Arrays.asList(null, null), MongoDBBase.toList(iterable, String.class));
    }

    public static class StringIdChild {
        private String id;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }
    }

    public static class ObjectIdChild {
        private ObjectId id;

        public ObjectId getId() {
            return id;
        }

        public void setId(final ObjectId id) {
            this.id = id;
        }
    }

    public static class AliasedIdChild {
        @com.landawn.abacus.annotation.Id
        private String itemId;

        public String getItemId() {
            return itemId;
        }

        public void setItemId(final String itemId) {
            this.itemId = itemId;
        }
    }

    public record StringIdParent(StringIdChild child, List<StringIdChild> children, Map<String, StringIdChild> byName, StringIdChild[] array) {
    }

    public record ObjectIdParent(ObjectIdChild child, List<ObjectIdChild> children, Map<String, ObjectIdChild> byName, ObjectIdChild[] array) {
    }

    public record AliasedIdParent(AliasedIdChild child, List<AliasedIdChild> children) {
    }

    public static class NumericChild {
        private Long id;

        public Long getId() {
            return id;
        }

        public void setId(final Long id) {
            this.id = id;
        }
    }

    public static class UuidChild {
        private UUID id;

        public UUID getId() {
            return id;
        }

        public void setId(final UUID id) {
            this.id = id;
        }
    }

    public record NumericParent(NumericChild child, List<NumericChild> children, Map<String, NumericChild> byName, NumericChild[] array) {
    }

    public record UuidParent(UuidChild child, List<UuidChild> children) {
    }

    public record NumericRecord(Long id, String name) {
    }

    public record NumericRecordParent(NumericRecord child, List<NumericRecord> children) {
    }

    public static class Child {
        @com.landawn.abacus.annotation.Id
        private String key;
        private String name;

        public String getKey() {
            return key;
        }

        public void setKey(final String key) {
            this.key = key;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    public static class Parent {
        private Child child;
        private List<Child> children;
        private Map<String, Child> byName;
        private Child[] array;

        public Child getChild() {
            return child;
        }

        public void setChild(final Child child) {
            this.child = child;
        }

        public List<Child> getChildren() {
            return children;
        }

        public void setChildren(final List<Child> children) {
            this.children = children;
        }

        public Map<String, Child> getByName() {
            return byName;
        }

        public void setByName(final Map<String, Child> byName) {
            this.byName = byName;
        }

        public Child[] getArray() {
            return array;
        }

        public void setArray(final Child[] array) {
            this.array = array;
        }
    }

    public static class GenericChild<V> {
        @com.landawn.abacus.annotation.Id
        private String key;
        private V value;

        public String getKey() {
            return key;
        }

        public void setKey(final String key) {
            this.key = key;
        }

        public V getValue() {
            return value;
        }

        public void setValue(final V value) {
            this.value = value;
        }
    }

    public record IdRecord(@com.landawn.abacus.annotation.Id String key, String name) {
    }

    public record RecordParent(IdRecord child, List<IdRecord> children) {
    }

    public record GenericParent(GenericChild<Long> child, List<GenericChild<Long>> children) {
    }

    public record ObjectIdRecord(@com.landawn.abacus.annotation.Id ObjectId key, String name) {
    }

    public record MixedRecordParent(IdRecord child, ObjectIdRecord objectIdChild) {
    }
}
