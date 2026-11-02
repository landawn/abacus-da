/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.aws.dynamodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.landawn.abacus.annotation.Column;
import com.landawn.abacus.annotation.JsonXmlField;
import com.landawn.abacus.da.MockitoTestBase;
import com.landawn.abacus.parser.ParserUtil;
import com.landawn.abacus.parser.ParserUtil.BeanInfo;
import com.landawn.abacus.parser.ParserUtil.PropInfo;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.TypeReference;

class DynamoDBExecutorBaseLineCoverageTest extends MockitoTestBase {

    @Test
    void columnAliasFillsTheBeanProperty() {
        final Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("user_name", new AttributeValue().withS("Ada"));

        final ColumnAliasedUser user = DynamoDBExecutor.toEntity(item, ColumnAliasedUser.class);

        assertEquals("Ada", user.getUserName());
    }

    @Test
    void nestedBeanTextUsesTheFormattedJsonReader() {
        final BeanInfo beanInfo = ParserUtil.getBeanInfo(AmountEnvelope.class);
        final PropInfo body = beanInfo.getPropInfo("body");

        final FormattedAmount amount = (FormattedAmount) DynamoDBExecutorBase.readNestedPropValue(beanInfo, body, "{\"amount\":\"7\"}");

        assertEquals(Optional.of(7L), amount.amount);
    }

    @Test
    void numericWrapperDateFormatIsReadByTheProperty() {
        final BeanInfo beanInfo = ParserUtil.getBeanInfo(DatedOptional.class);
        final PropInfo when = beanInfo.getPropInfo("when");

        final UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class,
                () -> DynamoDBExecutorBase.readNestedPropValue(beanInfo, when, "1"));

        assertTrue(ex.getMessage().contains("DateFormat"));
    }

    @Test
    void reachableNumberFormatsIncludeArraysOwnersAndSerializableProperties() throws Exception {
        assertTrue(DynamoDBExecutorBase.hasReachableJsonNumberFormat(N.typeOf(ReachableFormatBean[].class), new HashSet<>()));

        final Type<?> formattedOwner = Type.of(FormatCarrier.class.getDeclaredField("nested").getGenericType());
        final java.lang.reflect.ParameterizedType formattedReflect = (java.lang.reflect.ParameterizedType) formattedOwner.reflectType();
        assertTrue(formattedReflect.getOwnerType() instanceof java.lang.reflect.ParameterizedType);
        assertTrue(DynamoDBExecutorBase.hasReachableJsonNumberFormat(formattedOwner, new HashSet<>()));

        final Type<?> plainOwner = Type.of(PlainCarrier.class.getDeclaredField("nested").getGenericType());
        final java.lang.reflect.ParameterizedType plainReflect = (java.lang.reflect.ParameterizedType) plainOwner.reflectType();
        assertTrue(plainReflect.getOwnerType() instanceof java.lang.reflect.ParameterizedType);
        assertFalse(DynamoDBExecutorBase.hasReachableJsonNumberFormat(plainOwner, new HashSet<>()));

        assertTrue(DynamoDBExecutorBase.hasReachableJsonNumberFormat(N.typeOf(SerializeThenFormat.class), new HashSet<>()));
    }

    @Test
    void toNumberRoundTripsANumberAndKeepsASuffixedDouble() {
        final Number whole = DynamoDBExecutorBase.toNumber(42);
        final Number suffixed = DynamoDBExecutorBase.toNumber("1.0d");

        assertEquals(Integer.class, whole.getClass());
        assertEquals(42, whole.intValue());
        assertEquals(Double.class, suffixed.getClass());
        assertEquals(1.0d, suffixed.doubleValue());
    }

    @Test
    void decimalLiteralCheckRejectsABareDotAndAnEmptyExponent() {
        assertFalse(DynamoDBExecutorBase.isDecimalNumberText("."));
        assertFalse(DynamoDBExecutorBase.isDecimalNumberText("+"));
        assertFalse(DynamoDBExecutorBase.isDecimalNumberText("1e"));
        assertFalse(DynamoDBExecutorBase.isDecimalNumberText("1E+"));

        final AttributeValue dot = DynamoDBExecutor.toAttributeValue(new TextNumber("."));
        final AttributeValue exponent = DynamoDBExecutor.toAttributeValue(new TextNumber("1e"));

        assertEquals(".", dot.getS());
        assertNull(dot.getN());
        assertEquals("1e", exponent.getS());
        assertNull(exponent.getN());
    }

    @Test
    void mismatchedNumberAndWrappedBinaryUseTheirDeclaredSlots() {
        final Object number = DynamoDBExecutorBase.convertMismatchedValue(7, N.typeOf(Number.class), DynamoDBExecutorBase.MappingContext.ITEM);

        assertEquals(Integer.class, number.getClass());
        assertEquals(7, ((Number) number).intValue());
        assertTrue(DynamoDBExecutorBase.acceptsWholeBinary(createType(new TypeReference<Optional<byte[]>>() {
        })));
        assertFalse(DynamoDBExecutorBase.acceptsWholeBinary(createType(new TypeReference<Optional<String>>() {
        })));
    }

    public static class ColumnAliasedUser {
        @Column("user_name")
        private String userName;

        public String getUserName() {
            return userName;
        }

        public void setUserName(final String userName) {
            this.userName = userName;
        }
    }

    public static class FormattedAmount {
        @JsonXmlField(numberFormat = "#")
        public Optional<Long> amount;
    }

    public static class AmountEnvelope {
        public FormattedAmount body;
    }

    public static class DatedOptional {
        @JsonXmlField(dateFormat = "long")
        public Optional<Long> when;
    }

    public static class ReachableFormatBean {
        @JsonXmlField(numberFormat = "#")
        public Long amount;
    }

    public static class FormatOwner<T> {
        public T value;

        public class Inner {
            public String label;
        }
    }

    public static class FormatCarrier {
        public FormatOwner<ReachableFormatBean>.Inner nested;
    }

    public static class PlainCarrier {
        public FormatOwner<String>.Inner nested;
    }

    public static class SerializeThenFormat {
        @JsonXmlField(direction = JsonXmlField.Direction.SERIALIZE_ONLY, numberFormat = "#")
        public Long hidden;

        @JsonXmlField(numberFormat = "#")
        public Long shown;
    }

    public static final class TextNumber extends Number {
        private final String text;

        TextNumber(final String text) {
            this.text = text;
        }

        @Override
        public String toString() {
            return text;
        }

        @Override
        public int intValue() {
            return 0;
        }

        @Override
        public long longValue() {
            return 0L;
        }

        @Override
        public float floatValue() {
            return 0f;
        }

        @Override
        public double doubleValue() {
            return 0d;
        }
    }
}
