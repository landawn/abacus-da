package com.landawn.abacus.da.aws.dynamodb;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.landawn.abacus.annotation.AccessFieldByMethod;
import com.landawn.abacus.annotation.Entity;
import com.landawn.abacus.annotation.JsonXmlField;

/**
 * Item mapping must skip every property that has no write path. abacus-common 8.2 treats a field-backed getter-only
 * property of an {@code @AccessFieldByMethod} bean as read-only ({@code PropInfo.isReadOnlyProperty}), and its
 * {@code setPropValue} throws {@code UnsupportedOperationException}. Covers both SDK executors, which share
 * {@link DynamoDBExecutorBase#toEntity}, for top-level items, direct nested beans (M) and JSON-context beans (L of M).
 */
public class DynamoDBReadOnlyPropertyMappingTest {

    // ---- 2026-10-19 sliceB ----

    // @Entity makes the getter-only final field a bean property (otherwise it is not introspected at all).
    @Entity
    @AccessFieldByMethod
    public static class MethodAccessBean {
        private String id;
        private final String computed = "default";

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        // field-backed, but getter-only: with @AccessFieldByMethod there is no write path
        public String getComputed() {
            return computed;
        }
    }

    public static class Outer {
        private MethodAccessBean inner;
        private List<MethodAccessBean> list;

        public MethodAccessBean getInner() {
            return inner;
        }

        public void setInner(final MethodAccessBean inner) {
            this.inner = inner;
        }

        public List<MethodAccessBean> getList() {
            return list;
        }

        public void setList(final List<MethodAccessBean> list) {
            this.list = list;
        }
    }

    public static class NamedBean {
        private String customerFirstName;
        @com.landawn.abacus.annotation.Column("explicit_col")
        private String other;

        public String getCustomerFirstName() {
            return customerFirstName;
        }

        public void setCustomerFirstName(final String customerFirstName) {
            this.customerFirstName = customerFirstName;
        }

        public String getOther() {
            return other;
        }

        public void setOther(final String other) {
            this.other = other;
        }
    }

    @Test
    public void getAttrNameCachesNamingPolicyConversionPerPolicy() {
        final com.landawn.abacus.parser.ParserUtil.BeanInfo beanInfo = com.landawn.abacus.parser.ParserUtil.getBeanInfo(NamedBean.class);
        final com.landawn.abacus.parser.ParserUtil.PropInfo name = beanInfo.getPropInfo("customerFirstName");
        final com.landawn.abacus.parser.ParserUtil.PropInfo other = beanInfo.getPropInfo("other");

        assertAll(() -> {
            for (final com.landawn.abacus.util.NamingPolicy policy : com.landawn.abacus.util.NamingPolicy.values()) {
                final String expected = policy == com.landawn.abacus.util.NamingPolicy.CAMEL_CASE ? "customerFirstName" : policy.convert("customerFirstName");
                assertEquals(expected, DynamoDBExecutorBase.getAttrName(name, policy), policy.name());
                assertEquals(expected, DynamoDBExecutorBase.getAttrName(name, policy), policy.name() + " (cached)");
                assertEquals("explicit_col", DynamoDBExecutorBase.getAttrName(other, policy), policy.name());
            }
        }, () -> org.junit.jupiter.api.Assertions.assertSame(DynamoDBExecutorBase.getAttrName(name, com.landawn.abacus.util.NamingPolicy.SNAKE_CASE),
                DynamoDBExecutorBase.getAttrName(name, com.landawn.abacus.util.NamingPolicy.SNAKE_CASE)),
                () -> assertEquals(Map.of("customer_first_name", new AttributeValue("a"), "explicit_col", new AttributeValue("b")),
                        DynamoDBExecutor.toItem(bean("a"), com.landawn.abacus.util.NamingPolicy.SNAKE_CASE)),
                () -> assertEquals(
                        Map.of("CUSTOMER_FIRST_NAME", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS("a"), "explicit_col",
                                software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS("b")),
                        com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toItem(bean("a"), com.landawn.abacus.util.NamingPolicy.SCREAMING_SNAKE_CASE)));
    }

    private static NamedBean bean(final String firstName) {
        final NamedBean bean = new NamedBean();
        bean.setCustomerFirstName(firstName);
        bean.setOther("b");
        return bean;
    }

    @Test
    public void v1ItemMappingSkipsAccessFieldByMethodGetterOnlyProperty() {
        final Map<String, AttributeValue> inner = Map.of("id", new AttributeValue("1"), "computed", new AttributeValue("stored"));

        assertAll(() -> {
            final MethodAccessBean bean = DynamoDBExecutor.toEntity(inner, MethodAccessBean.class);
            assertEquals("1", bean.getId());
            assertEquals("default", bean.getComputed());
        }, () -> {
            final Outer outer = DynamoDBExecutor.toEntity(Map.of("inner", new AttributeValue().withM(inner)), Outer.class);
            assertEquals("1", outer.getInner().getId());
            assertEquals("default", outer.getInner().getComputed());
            assertNull(outer.getList());
        }, () -> {
            final Outer outer = DynamoDBExecutor.toEntity(Map.of("list", new AttributeValue().withL(new AttributeValue().withM(inner))), Outer.class);
            assertEquals("1", outer.getList().get(0).getId());
            assertEquals("default", outer.getList().get(0).getComputed());
        });
    }

    @Test
    public void v2ItemMappingSkipsAccessFieldByMethodGetterOnlyProperty() {
        final Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> inner = Map.of("id",
                software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS("1"), "computed",
                software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS("stored"));

        assertAll(() -> {
            final MethodAccessBean bean = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(inner, MethodAccessBean.class);
            assertEquals("1", bean.getId());
            assertEquals("default", bean.getComputed());
        }, () -> {
            final Outer outer = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor
                    .toEntity(Map.of("inner", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(inner)), Outer.class);
            assertEquals("1", outer.getInner().getId());
            assertEquals("default", outer.getInner().getComputed());
        }, () -> {
            final Outer outer = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor
                    .toEntity(Map.of("list", software.amazon.awssdk.services.dynamodb.model.AttributeValue
                            .fromL(List.of(software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(inner)))), Outer.class);
            assertEquals("1", outer.getList().get(0).getId());
            assertEquals("default", outer.getList().get(0).getComputed());
        });
    }

    // ---- 2026-10-20 ddbfix ----

    private static final String DIRECTION_JSON = "{\"id\":\"1\",\"computed\":\"stored\"}";

    /**
     * Read-only (field restricted by {@code @AccessFieldByMethod}, no setter) with an explicit {@code DESERIALIZE_ONLY}
     * direction: abacus-common keeps the declared direction, but {@code isReadOnlyProperty} is what toEntity tests first.
     */
    @Entity
    public static class DeserializeOnlyReadOnlyBean {
        private String id;
        @AccessFieldByMethod
        @JsonXmlField(direction = JsonXmlField.Direction.DESERIALIZE_ONLY)
        private String computed = "default";

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getComputed() {
            return computed;
        }
    }

    /** {@code SERIALIZE_ONLY} with a setter: not read-only, so item and direct mapping assign it and JSON reads skip it. */
    public static class SerializeOnlyWritableBean {
        private String id;
        @JsonXmlField(direction = JsonXmlField.Direction.SERIALIZE_ONLY)
        private String computed = "default";

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getComputed() {
            return computed;
        }

        public void setComputed(final String computed) {
            this.computed = computed;
        }
    }

    public static class DirectionOuter {
        private DeserializeOnlyReadOnlyBean ro;
        private List<DeserializeOnlyReadOnlyBean> roList;
        private SerializeOnlyWritableBean so;
        private List<SerializeOnlyWritableBean> soList;

        public DeserializeOnlyReadOnlyBean getRo() {
            return ro;
        }

        public void setRo(final DeserializeOnlyReadOnlyBean ro) {
            this.ro = ro;
        }

        public List<DeserializeOnlyReadOnlyBean> getRoList() {
            return roList;
        }

        public void setRoList(final List<DeserializeOnlyReadOnlyBean> roList) {
            this.roList = roList;
        }

        public SerializeOnlyWritableBean getSo() {
            return so;
        }

        public void setSo(final SerializeOnlyWritableBean so) {
            this.so = so;
        }

        public List<SerializeOnlyWritableBean> getSoList() {
            return soList;
        }

        public void setSoList(final List<SerializeOnlyWritableBean> soList) {
            this.soList = soList;
        }
    }

    /** The two beans carry the PropInfo flags the tests below rely on (abacus-common 8.2.1). */
    @Test
    public void directionBeansHaveTheExpectedPropInfoFlags_ddbfix() {
        final com.landawn.abacus.parser.ParserUtil.PropInfo readOnly = com.landawn.abacus.parser.ParserUtil.getBeanInfo(DeserializeOnlyReadOnlyBean.class)
                .getPropInfo("computed");
        final com.landawn.abacus.parser.ParserUtil.PropInfo writable = com.landawn.abacus.parser.ParserUtil.getBeanInfo(SerializeOnlyWritableBean.class)
                .getPropInfo("computed");

        assertAll(() -> assertTrue(readOnly.isReadOnlyProperty), () -> assertEquals(JsonXmlField.Direction.DESERIALIZE_ONLY, readOnly.jsonXmlExpose),
                () -> assertFalse(writable.isReadOnlyProperty), () -> assertEquals(JsonXmlField.Direction.SERIALIZE_ONLY, writable.jsonXmlExpose));
    }

    /**
     * An explicitly DESERIALIZE_ONLY read-only property is skipped like any read-only property by item mapping, direct (M)
     * mapping and the JSON-context element of an L; only a bean parsed from an S attribute's JSON text by abacus-common's
     * JSON parser rejects it with UnsupportedOperationException.
     */
    @Test
    public void v1DeserializeOnlyReadOnlyPropertyIsSkippedInEveryMappingContext_ddbfix() {
        final Map<String, AttributeValue> inner = Map.of("id", new AttributeValue("1"), "computed", new AttributeValue("stored"));

        assertAll(() -> {
            final DeserializeOnlyReadOnlyBean bean = DynamoDBExecutor.toEntity(inner, DeserializeOnlyReadOnlyBean.class);
            assertEquals("1", bean.getId());
            assertEquals("default", bean.getComputed());
        }, () -> {
            final DirectionOuter outer = DynamoDBExecutor.toEntity(Map.of("ro", new AttributeValue().withM(inner)), DirectionOuter.class);
            assertEquals("1", outer.getRo().getId());
            assertEquals("default", outer.getRo().getComputed());
        }, () -> {
            final DirectionOuter outer = DynamoDBExecutor.toEntity(Map.of("roList", new AttributeValue().withL(new AttributeValue().withM(inner))),
                    DirectionOuter.class);
            assertEquals("1", outer.getRoList().get(0).getId());
            assertEquals("default", outer.getRoList().get(0).getComputed());
        }, () -> assertThrows(UnsupportedOperationException.class,
                () -> DynamoDBExecutor.toEntity(Map.of("ro", new AttributeValue(DIRECTION_JSON)), DirectionOuter.class)));
    }

    @Test
    public void v2DeserializeOnlyReadOnlyPropertyIsSkippedInEveryMappingContext_ddbfix() {
        final Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> inner = Map.of("id",
                software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS("1"), "computed",
                software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS("stored"));

        assertAll(() -> {
            final DeserializeOnlyReadOnlyBean bean = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(inner, DeserializeOnlyReadOnlyBean.class);
            assertEquals("1", bean.getId());
            assertEquals("default", bean.getComputed());
        }, () -> {
            final DirectionOuter outer = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor
                    .toEntity(Map.of("ro", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(inner)), DirectionOuter.class);
            assertEquals("1", outer.getRo().getId());
            assertEquals("default", outer.getRo().getComputed());
        }, () -> {
            final DirectionOuter outer = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor
                    .toEntity(Map.of("roList", software.amazon.awssdk.services.dynamodb.model.AttributeValue
                            .fromL(List.of(software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(inner)))), DirectionOuter.class);
            assertEquals("1", outer.getRoList().get(0).getId());
            assertEquals("default", outer.getRoList().get(0).getComputed());
        }, () -> assertThrows(UnsupportedOperationException.class, () -> com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor
                .toEntity(Map.of("ro", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS(DIRECTION_JSON)), DirectionOuter.class)));
    }

    /**
     * A SERIALIZE_ONLY property with a setter is not read-only: item mapping and direct (M) mapping assign it, while the
     * JSON reads (an element of an L, a bean parsed from an S attribute's JSON text) skip it.
     */
    @Test
    public void v1SerializeOnlyPropertyWithSetterIsAssignedByItemMappingAndSkippedByJsonReads_ddbfix() {
        final Map<String, AttributeValue> inner = Map.of("id", new AttributeValue("1"), "computed", new AttributeValue("stored"));

        assertAll(() -> assertEquals("stored", DynamoDBExecutor.toEntity(inner, SerializeOnlyWritableBean.class).getComputed()), () -> assertEquals("stored",
                DynamoDBExecutor.toEntity(Map.of("so", new AttributeValue().withM(inner)), DirectionOuter.class).getSo().getComputed()), () -> {
                    final DirectionOuter outer = DynamoDBExecutor.toEntity(Map.of("soList", new AttributeValue().withL(new AttributeValue().withM(inner))),
                            DirectionOuter.class);
                    assertEquals("1", outer.getSoList().get(0).getId());
                    assertEquals("default", outer.getSoList().get(0).getComputed());
                }, () -> {
                    final DirectionOuter outer = DynamoDBExecutor.toEntity(Map.of("so", new AttributeValue(DIRECTION_JSON)), DirectionOuter.class);
                    assertEquals("1", outer.getSo().getId());
                    assertEquals("default", outer.getSo().getComputed());
                });
    }

    @Test
    public void v2SerializeOnlyPropertyWithSetterIsAssignedByItemMappingAndSkippedByJsonReads_ddbfix() {
        final Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> inner = Map.of("id",
                software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS("1"), "computed",
                software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS("stored"));

        assertAll(
                () -> assertEquals("stored",
                        com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(inner, SerializeOnlyWritableBean.class).getComputed()),
                () -> assertEquals("stored",
                        com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor
                                .toEntity(Map.of("so", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(inner)), DirectionOuter.class)
                                .getSo()
                                .getComputed()),
                () -> {
                    final DirectionOuter outer = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor
                            .toEntity(
                                    Map.of("soList",
                                            software.amazon.awssdk.services.dynamodb.model.AttributeValue
                                                    .fromL(List.of(software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(inner)))),
                                    DirectionOuter.class);
                    assertEquals("1", outer.getSoList().get(0).getId());
                    assertEquals("default", outer.getSoList().get(0).getComputed());
                }, () -> {
                    final DirectionOuter outer = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor
                            .toEntity(Map.of("so", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS(DIRECTION_JSON)), DirectionOuter.class);
                    assertEquals("1", outer.getSo().getId());
                    assertEquals("default", outer.getSo().getComputed());
                });
    }
}
