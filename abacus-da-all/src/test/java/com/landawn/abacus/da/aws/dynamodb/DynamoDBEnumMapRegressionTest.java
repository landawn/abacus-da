package com.landawn.abacus.da.aws.dynamodb;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.landawn.abacus.exception.ParsingException;
import com.landawn.abacus.util.Holder;
import com.landawn.abacus.util.u.Nullable;

public class DynamoDBEnumMapRegressionTest {
    private static final List<String> CONTEXTS = List.of("list", "map", "optional", "abacusOptional", "nullable", "held");

    @Test
    public void legacyRawEnumMapPropertiesRejectMissingEnumKeyMetadata() {
        final List<Executable> checks = new ArrayList<>();

        for (final boolean empty : new boolean[] { true, false }) {
            final Map<String, AttributeValue> entries = empty ? Map.of() : Map.of("MONDAY", new AttributeValue().withS("value"));
            final AttributeValue leaf = new AttributeValue().withM(Map.of("values", new AttributeValue().withM(entries)));

            for (final String context : CONTEXTS) {
                checks.add(() -> assertMissingEnumKey(() -> DynamoDBExecutor.toEntity(Map.of(context, legacyContainer(context, leaf)), ContainerBean.class),
                        context + ", empty=" + empty));
            }
        }

        assertAll(checks);
    }

    @Test
    public void v2RawEnumMapPropertiesRejectMissingEnumKeyMetadata() {
        final List<Executable> checks = new ArrayList<>();

        for (final boolean empty : new boolean[] { true, false }) {
            final Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> entries = empty ? Map.of()
                    : Map.of("MONDAY", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS("value"));
            final var leaf = software.amazon.awssdk.services.dynamodb.model.AttributeValue
                    .fromM(Map.of("values", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(entries)));

            for (final String context : CONTEXTS) {
                checks.add(() -> assertMissingEnumKey(
                        () -> com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(Map.of(context, v2Container(context, leaf)), ContainerBean.class),
                        context + ", empty=" + empty));
            }
        }

        assertAll(checks);
    }

    @Test
    public void legacyNullAndAbsentRawEnumMapPropertiesRemainNull() {
        for (final boolean explicitNull : new boolean[] { true, false }) {
            final AttributeValue leaf = new AttributeValue().withM(explicitNull ? Map.of("values", new AttributeValue().withNULL(true)) : Map.of());
            final Map<String, AttributeValue> item = new HashMap<>();

            for (final String context : CONTEXTS) {
                item.put(context, legacyContainer(context, leaf));
            }

            assertNullMapProperties(DynamoDBExecutor.toEntity(item, ContainerBean.class));
        }
    }

    @Test
    public void v2NullAndAbsentRawEnumMapPropertiesRemainNull() {
        for (final boolean explicitNull : new boolean[] { true, false }) {
            final var leaf = software.amazon.awssdk.services.dynamodb.model.AttributeValue
                    .fromM(explicitNull ? Map.of("values", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromNul(true)) : Map.of());
            final Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> item = new HashMap<>();

            for (final String context : CONTEXTS) {
                item.put(context, v2Container(context, leaf));
            }

            assertNullMapProperties(com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(item, ContainerBean.class));
        }
    }

    private static AttributeValue legacyContainer(final String context, final AttributeValue leaf) {
        return switch (context) {
            case "list" -> new AttributeValue().withL(leaf);
            case "map" -> new AttributeValue().withM(Map.of("entry", leaf));
            default -> leaf;
        };
    }

    private static software.amazon.awssdk.services.dynamodb.model.AttributeValue v2Container(final String context,
            final software.amazon.awssdk.services.dynamodb.model.AttributeValue leaf) {
        return switch (context) {
            case "list" -> software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL(List.of(leaf));
            case "map" -> software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(Map.of("entry", leaf));
            default -> leaf;
        };
    }

    private static void assertMissingEnumKey(final Executable conversion, final String context) {
        final ParsingException failure = assertThrows(ParsingException.class, conversion, context);
        assertTrue(failure.getMessage().contains("enum key type"), context + ": " + failure.getMessage());
        assertTrue(failure.getMessage().contains("resolved key type: Object"), context + ": " + failure.getMessage());
    }

    private static void assertNullMapProperties(final ContainerBean result) {
        assertEquals(1, result.getList().size());
        assertEquals(1, result.getMap().size());

        // Typed access also checks that native wrappers contain the bean, rather than serialized JSON text.
        final RawEnumMapBean[] beans = { result.getList().get(0), result.getMap().get("entry"), result.getOptional().get(), result.getAbacusOptional().get(),
                result.getNullable().get(), result.getHeld().value() };

        for (final RawEnumMapBean bean : beans) {
            assertNotNull(bean);
            assertNull(bean.getValues());
        }
    }

    public static final class ContainerBean {
        private List<RawEnumMapBean> list;
        private Map<String, RawEnumMapBean> map;
        private Optional<RawEnumMapBean> optional;
        private com.landawn.abacus.util.u.Optional<RawEnumMapBean> abacusOptional;
        private Nullable<RawEnumMapBean> nullable;
        private Holder<RawEnumMapBean> held;

        public List<RawEnumMapBean> getList() {
            return list;
        }

        public void setList(final List<RawEnumMapBean> value) {
            list = value;
        }

        public Map<String, RawEnumMapBean> getMap() {
            return map;
        }

        public void setMap(final Map<String, RawEnumMapBean> value) {
            map = value;
        }

        public Optional<RawEnumMapBean> getOptional() {
            return optional;
        }

        public void setOptional(final Optional<RawEnumMapBean> value) {
            optional = value;
        }

        public com.landawn.abacus.util.u.Optional<RawEnumMapBean> getAbacusOptional() {
            return abacusOptional;
        }

        public void setAbacusOptional(final com.landawn.abacus.util.u.Optional<RawEnumMapBean> value) {
            abacusOptional = value;
        }

        public Nullable<RawEnumMapBean> getNullable() {
            return nullable;
        }

        public void setNullable(final Nullable<RawEnumMapBean> value) {
            nullable = value;
        }

        public Holder<RawEnumMapBean> getHeld() {
            return held;
        }

        public void setHeld(final Holder<RawEnumMapBean> value) {
            held = value;
        }
    }

    @SuppressWarnings("rawtypes")
    public static final class RawEnumMapBean {
        private EnumMap values;

        public EnumMap getValues() {
            return values;
        }

        public void setValues(final EnumMap value) {
            values = value;
        }
    }
}
