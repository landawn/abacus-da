package com.landawn.abacus.da.aws.dynamodb;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.landawn.abacus.util.Beans;
import com.landawn.abacus.util.N;

/**
 * A {@code Map} subclass annotated as an entity is a bean class to {@code Beans.isBeanClass}, but its abacus {@code Type} is a
 * {@code MapType} ({@code isBean()} false, {@code isMap()} true). Every dispatch in {@code DynamoDBExecutorBase} tests the
 * {@code Type}, so such a property, collection element or row type is read as a map of the attribute values in item, direct
 * ({@code M}) bean and JSON-context ({@code L} element) mapping, under both SDKs. Pins the existing behaviour.
 */
public class DynamoDBExecutorBaseDeepReview2026EntityMapTest {

    // ---- 2026-10-24 r2 sliceB ----

    @com.landawn.abacus.annotation.Entity
    public static class EntityMap extends LinkedHashMap<String, Object> {
        private static final long serialVersionUID = 1L;
    }

    public static class MapHolder {
        private EntityMap map;
        private List<EntityMap> maps;

        public EntityMap getMap() {
            return map;
        }

        public void setMap(final EntityMap map) {
            this.map = map;
        }

        public List<EntityMap> getMaps() {
            return maps;
        }

        public void setMaps(final List<EntityMap> maps) {
            this.maps = maps;
        }
    }

    public static class Outer {
        private MapHolder direct;
        private List<MapHolder> list;

        public MapHolder getDirect() {
            return direct;
        }

        public void setDirect(final MapHolder direct) {
            this.direct = direct;
        }

        public List<MapHolder> getList() {
            return list;
        }

        public void setList(final List<MapHolder> list) {
            this.list = list;
        }
    }

    private static AttributeValue v1Map() {
        final Map<String, AttributeValue> m = new LinkedHashMap<>();
        m.put("a", new AttributeValue().withS("x"));
        m.put("n", new AttributeValue().withN("1"));
        return new AttributeValue().withM(m);
    }

    private static Map<String, AttributeValue> v1Fields() {
        final Map<String, AttributeValue> fields = new LinkedHashMap<>();
        fields.put("map", v1Map());
        fields.put("maps", new AttributeValue().withL(v1Map()));
        return fields;
    }

    private static software.amazon.awssdk.services.dynamodb.model.AttributeValue v2Map() {
        final Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> m = new LinkedHashMap<>();
        m.put("a", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS("x"));
        m.put("n", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromN("1"));
        return software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(m);
    }

    private static Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> v2Fields() {
        final Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> fields = new LinkedHashMap<>();
        fields.put("map", v2Map());
        fields.put("maps", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL(List.of(v2Map())));
        return fields;
    }

    /** Both maps of a MapHolder are EntityMap instances holding the attribute values ({a=x, n=1}). */
    private static void assertEntityMaps(final MapHolder holder, final String context) {
        final Map<String, Object> expected = new LinkedHashMap<>();
        expected.put("a", "x");
        expected.put("n", "1");

        assertAll(context, () -> assertSame(EntityMap.class, holder.getMap().getClass()), () -> assertEquals(expected, holder.getMap()),
                () -> assertEquals(1, holder.getMaps().size()), () -> assertSame(EntityMap.class, holder.getMaps().get(0).getClass()),
                () -> assertEquals(expected, holder.getMaps().get(0)));
    }

    @Test
    public void entityAnnotatedMapPropertiesAndElementsReadAsMapsInEveryContext_r2sliceB() {
        assertAll(() -> assertTrue(Beans.isBeanClass(EntityMap.class)), () -> assertFalse(N.typeOf(EntityMap.class).isBean()),
                () -> assertTrue(N.typeOf(EntityMap.class).isMap()),
                // SDK v1: item, direct (M attribute) bean, JSON-context bean (element of an L attribute).
                () -> assertEntityMaps(DynamoDBExecutor.toEntity(v1Fields(), MapHolder.class), "v1 item"),
                () -> assertEntityMaps(DynamoDBExecutor.toEntity(Map.of("direct", new AttributeValue().withM(v1Fields())), Outer.class).getDirect(),
                        "v1 direct bean"),
                () -> assertEntityMaps(
                        DynamoDBExecutor.toEntity(Map.of("list", new AttributeValue().withL(new AttributeValue().withM(v1Fields()))), Outer.class)
                                .getList()
                                .get(0),
                        "v1 JSON-context bean"),
                // SDK v2: the same shared rules.
                () -> assertEntityMaps(com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(v2Fields(), MapHolder.class), "v2 item"),
                () -> assertEntityMaps(
                        com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor
                                .toEntity(Map.of("direct", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(v2Fields())), Outer.class)
                                .getDirect(),
                        "v2 direct bean"),
                () -> assertEntityMaps(
                        com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor
                                .toEntity(
                                        Map.of("list",
                                                software.amazon.awssdk.services.dynamodb.model.AttributeValue
                                                        .fromL(List.of(software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(v2Fields())))),
                                        Outer.class)
                                .getList()
                                .get(0),
                        "v2 JSON-context bean"),
                // query(QueryRequest, Class) accepts it as a Map row type (checked before the bean test).
                () -> DynamoDBExecutorBase.checkDatasetRowType(EntityMap.class));
    }
}
