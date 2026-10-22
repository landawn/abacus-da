package com.landawn.abacus.da.aws.dynamodb.v2;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.landawn.abacus.da.aws.dynamodb.DynamoDBExecutorBase;
import com.landawn.abacus.util.Fn;
import com.landawn.abacus.util.N;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Pins every claim of the "Boolean targets" paragraph of the {@link DynamoDBExecutorBase} class Javadoc for the SDK v2
 * executor: strict text conversion of {@code S}/{@code N} attributes, the blank-text and {@code NULL} special cases per
 * mapping context, JSON number tokens, and native Java numbers reaching the direct bean mapper.
 */
public class DynamoDBV2ExecutorDeepReview2026BooleanTargetTest {

    @ParameterizedTest
    @CsvSource(delimiter = '|', ignoreLeadingAndTrailingWhitespace = false, value = { "1|true", "y|true", "Y|true", "true|true", "TRUE|true", "tRuE|true",
            " Y|true", " true |true", "0|false", "-1|false", "n|false", "N|false", "false|false", "FALSE|false", " n |false" })
    public void acceptedTextConvertsInEveryContext(final String text, final boolean expected) {
        assertReadsEverywhereAs(AttributeValue.fromS(text), expected);
        assertReadsEverywhereAs(AttributeValue.fromN(text), expected);
        // The same text inside the JSON text of an S attribute.
        final Flags json = json("{\"boxed\":\"" + text + "\",\"primitive\":\"" + text + "\"}");
        assertAll(() -> assertEquals(expected, json.getBoxed()), () -> assertEquals(expected, json.isPrimitive()));
    }

    @Test
    public void boolAttributeMapsDirectly() {
        assertReadsEverywhereAs(AttributeValue.fromBool(true), true);
        assertReadsEverywhereAs(AttributeValue.fromBool(false), false);
    }

    @ParameterizedTest
    @ValueSource(strings = { "yes", "T", "2", "1.0", "01", "abc" })
    public void otherTextIsRejectedInEveryContext(final String text) {
        assertRejectedEverywhere(AttributeValue.fromS(text));
        assertRejectedEverywhere(AttributeValue.fromN(text));
        assertThrows(IllegalArgumentException.class, () -> json("{\"boxed\":\"" + text + "\"}"));
        assertThrows(IllegalArgumentException.class, () -> json("{\"primitive\":\"" + text + "\"}"));
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "  " })
    public void blankTextIsFalseForItemMappingDirectBeanMappingAndSingleValueReads(final String text) {
        final AttributeValue blank = AttributeValue.fromS(text);
        final Flags item = DynamoDBExecutor.toEntity(Map.of("boxed", blank, "primitive", blank), Flags.class);
        final Flags nested = DynamoDBExecutor.toEntity(Map.of("nested", m(blank, blank)), Root.class).getNested();

        assertAll(() -> assertEquals(Boolean.FALSE, item.getBoxed()), () -> assertFalse(item.isPrimitive()), //
                () -> assertEquals(Boolean.FALSE, nested.getBoxed()), () -> assertFalse(nested.isPrimitive()), //
                () -> assertEquals(Boolean.FALSE, DynamoDBExecutor.toValue(blank, Boolean.class)), //
                () -> assertEquals(Boolean.FALSE, DynamoDBExecutor.toValue(blank, boolean.class)), //
                () -> assertEquals(List.of(Boolean.FALSE), DynamoDBExecutor.toList(List.of(Map.of("flag", blank)), Boolean.class)), //
                () -> assertEquals(List.of(Boolean.FALSE), DynamoDBExecutor.toList(List.of(Map.of("flag", blank)), boolean.class)));
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "  " })
    public void blankTextIsNullForBoxedAndFalseForPrimitiveInJsonContext(final String text) {
        final AttributeValue blank = AttributeValue.fromS(text);
        final Flags element = DynamoDBExecutor.toEntity(Map.of("list", l(m(blank, blank))), Root.class).getList().get(0);
        final Flags json = json("{\"boxed\":\"" + text + "\",\"primitive\":\"" + text + "\"}");

        assertAll(() -> assertNull(element.getBoxed()), () -> assertFalse(element.isPrimitive()), //
                () -> assertNull(json.getBoxed()), () -> assertFalse(json.isPrimitive()));
    }

    @Test
    public void nullAttributeIsNullForBoxedAndFalseForPrimitive() {
        final AttributeValue nul = AttributeValue.fromNul(true);
        final Flags item = DynamoDBExecutor.toEntity(Map.of("boxed", nul, "primitive", nul), Flags.class);
        final Root root = DynamoDBExecutor.toEntity(
                Map.of("nested", m(nul, nul), "list", l(m(nul, nul)), "json", AttributeValue.fromS("{\"boxed\":null,\"primitive\":null}")), Root.class);

        assertAll(() -> assertNull(item.getBoxed()), () -> assertFalse(item.isPrimitive()), //
                () -> assertNull(root.getNested().getBoxed()), () -> assertFalse(root.getNested().isPrimitive()), //
                () -> assertNull(root.getList().get(0).getBoxed()), () -> assertFalse(root.getList().get(0).isPrimitive()), //
                () -> assertNull(root.getJson().getBoxed()), () -> assertFalse(root.getJson().isPrimitive()), //
                () -> assertNull(DynamoDBExecutor.toValue(nul, Boolean.class)), //
                () -> assertEquals(Boolean.FALSE, DynamoDBExecutor.toValue(nul, boolean.class)), //
                () -> assertEquals(Collections.singletonList(null), DynamoDBExecutor.toList(List.of(Map.of("flag", nul)), Boolean.class)), //
                () -> assertEquals(List.of(Boolean.FALSE), DynamoDBExecutor.toList(List.of(Map.of("flag", nul)), boolean.class)));
    }

    /** A JSON number token in the JSON text of an S attribute follows the Integer 1/0/-1 rule. */
    @Test
    public void jsonNumberTokenFollowsTheIntegerRule() {
        assertAll(() -> assertEquals(Boolean.TRUE, json("{\"boxed\":1,\"primitive\":1}").getBoxed()), //
                () -> assertTrue(json("{\"boxed\":1,\"primitive\":1}").isPrimitive()), //
                () -> assertEquals(Boolean.FALSE, json("{\"boxed\":0}").getBoxed()), //
                () -> assertEquals(Boolean.FALSE, json("{\"boxed\":-1}").getBoxed()), //
                () -> assertFalse(json("{\"primitive\":-1}").isPrimitive()), //
                () -> assertThrows(IllegalArgumentException.class, () -> json("{\"boxed\":1.0}")), //
                () -> assertThrows(IllegalArgumentException.class, () -> json("{\"primitive\":1.0}")), //
                () -> assertThrows(IllegalArgumentException.class, () -> json("{\"boxed\":2}")), //
                () -> assertThrows(IllegalArgumentException.class, () -> json("{\"boxed\":10000000000}")));
    }

    /**
     * An item never carries a native Java number (an N attribute decodes to its number text), so the rule for a decoded Map
     * value converted to a bean is pinned on the shared base mapper, which both executors use, in direct (MAPPED_BEAN) context.
     */
    @Test
    public void nativeNumbersConvertOnlyAsIntegerShortOrByteOneZeroMinusOne() {
        assertAll(() -> assertEquals(Boolean.TRUE, BaseAccess.mappedBean(Map.of("boxed", 1, "primitive", 1)).getBoxed()), //
                () -> assertTrue(BaseAccess.mappedBean(Map.of("boxed", 1, "primitive", 1)).isPrimitive()), //
                () -> assertEquals(Boolean.FALSE, BaseAccess.mappedBean(Map.of("boxed", 0)).getBoxed()), //
                () -> assertEquals(Boolean.FALSE, BaseAccess.mappedBean(Map.of("boxed", -1)).getBoxed()), //
                () -> assertFalse(BaseAccess.mappedBean(Map.of("primitive", -1)).isPrimitive()), //
                () -> assertEquals(Boolean.TRUE, BaseAccess.mappedBean(Map.of("boxed", (short) 1)).getBoxed()), //
                () -> assertEquals(Boolean.TRUE, BaseAccess.mappedBean(Map.of("boxed", (byte) 1)).getBoxed()), //
                () -> assertEquals(Boolean.FALSE, BaseAccess.mappedBean(Map.of("boxed", (short) -1)).getBoxed()), //
                () -> assertThrows(IllegalArgumentException.class, () -> BaseAccess.mappedBean(Map.of("boxed", 2))), //
                () -> assertThrows(IllegalArgumentException.class, () -> BaseAccess.mappedBean(Map.of("primitive", 2))), //
                () -> assertThrows(IllegalArgumentException.class, () -> BaseAccess.mappedBean(Map.of("boxed", 1L))), //
                () -> assertThrows(IllegalArgumentException.class, () -> BaseAccess.mappedBean(Map.of("boxed", 1.0d))), //
                () -> assertThrows(IllegalArgumentException.class, () -> BaseAccess.mappedBean(Map.of("boxed", 1.0f))), //
                () -> assertThrows(IllegalArgumentException.class, () -> BaseAccess.mappedBean(Map.of("boxed", BigDecimal.ONE))));
    }

    // ---- helpers ----

    /** Item mapping, direct (M) bean mapping, a JSON-context element of L, and the single-value reads must all agree. */
    private static void assertReadsEverywhereAs(final AttributeValue value, final boolean expected) {
        final Flags item = DynamoDBExecutor.toEntity(Map.of("boxed", value, "primitive", value), Flags.class);
        final Root root = DynamoDBExecutor.toEntity(Map.of("nested", m(value, value), "list", l(m(value, value))), Root.class);
        final Flags nested = root.getNested();
        final Flags element = root.getList().get(0);
        final List<Map<String, AttributeValue>> rows = List.of(Map.of("flag", value));

        assertAll(() -> assertEquals(expected, item.getBoxed()), () -> assertEquals(expected, item.isPrimitive()), //
                () -> assertEquals(expected, nested.getBoxed()), () -> assertEquals(expected, nested.isPrimitive()), //
                () -> assertEquals(expected, element.getBoxed()), () -> assertEquals(expected, element.isPrimitive()), //
                () -> assertEquals(expected, DynamoDBExecutor.toValue(value, Boolean.class)), //
                () -> assertEquals(expected, DynamoDBExecutor.toValue(value, boolean.class)), //
                () -> assertEquals(List.of(expected), DynamoDBExecutor.toList(rows, Boolean.class)), //
                () -> assertEquals(List.of(expected), DynamoDBExecutor.toList(rows, boolean.class)));
    }

    private static void assertRejectedEverywhere(final AttributeValue value) {
        final List<Map<String, AttributeValue>> rows = List.of(Map.of("flag", value));

        assertAll(() -> assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toEntity(Map.of("boxed", value), Flags.class)), //
                () -> assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toEntity(Map.of("primitive", value), Flags.class)), //
                () -> assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toEntity(Map.of("nested", m(value, value)), Root.class)), //
                () -> assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toEntity(Map.of("list", l(m(value, value))), Root.class)), //
                () -> assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toValue(value, Boolean.class)), //
                () -> assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toValue(value, boolean.class)), //
                () -> assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toList(rows, Boolean.class)), //
                () -> assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toList(rows, boolean.class)));
    }

    private static Flags json(final String jsonText) {
        return DynamoDBExecutor.toEntity(Map.of("json", AttributeValue.fromS(jsonText)), Root.class).getJson();
    }

    private static AttributeValue m(final AttributeValue boxed, final AttributeValue primitive) {
        return AttributeValue.fromM(Map.of("boxed", boxed, "primitive", primitive));
    }

    private static AttributeValue l(final AttributeValue... elements) {
        return AttributeValue.fromL(List.of(elements));
    }

    /** Reaches the shared base mapper from this package; the native-number rule lives in {@link DynamoDBExecutorBase}. */
    private static final class BaseAccess extends DynamoDBExecutorBase {
        static Flags mappedBean(final Map<String, Object> values) {
            return toEntity(values, N.typeOf(Flags.class), Fn.identity(), MappingContext.MAPPED_BEAN);
        }
    }

    public static class Flags {
        private Boolean boxed;
        private boolean primitive;

        public Boolean getBoxed() {
            return boxed;
        }

        public void setBoxed(final Boolean boxed) {
            this.boxed = boxed;
        }

        public boolean isPrimitive() {
            return primitive;
        }

        public void setPrimitive(final boolean primitive) {
            this.primitive = primitive;
        }
    }

    public static class Root {
        private Flags nested; // an M attribute: direct bean mapping
        private List<Flags> list; // an L of M: JSON-context elements
        private Flags json; // an S attribute holding JSON text: abacus-common's JSON parser

        public Flags getNested() {
            return nested;
        }

        public void setNested(final Flags nested) {
            this.nested = nested;
        }

        public List<Flags> getList() {
            return list;
        }

        public void setList(final List<Flags> list) {
            this.list = list;
        }

        public Flags getJson() {
            return json;
        }

        public void setJson(final Flags json) {
            this.json = json;
        }
    }
}
