package com.landawn.abacus.da.aws.dynamodb;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.landawn.abacus.util.Fraction;

import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

/**
 * A finite number whose text is not a decimal literal (an abacus {@link Fraction}, {@code "3/4"}) is stored as {@code S}. A
 * {@code Number} slot reads that text back as the {@code Fraction} (the JSON reader rejected it, so the item could not be read);
 * {@code Object} and {@code Fraction} slots and every other text are read as before. Shared by the SDK v1 and v2 executors.
 */
public class DynamoDBExecutorBaseDeepReview2026FractionTest {

    // ---- 2026-10-24 r2 sliceB ----

    public static class Numbers {
        private String id;
        private Number number;
        private Object object;
        private Fraction fraction;
        private Optional<Number> optionalNumber;
        private List<Number> numbers;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public Number getNumber() {
            return number;
        }

        public void setNumber(final Number number) {
            this.number = number;
        }

        public Object getObject() {
            return object;
        }

        public void setObject(final Object object) {
            this.object = object;
        }

        public Fraction getFraction() {
            return fraction;
        }

        public void setFraction(final Fraction fraction) {
            this.fraction = fraction;
        }

        public Optional<Number> getOptionalNumber() {
            return optionalNumber;
        }

        public void setOptionalNumber(final Optional<Number> optionalNumber) {
            this.optionalNumber = optionalNumber;
        }

        public List<Number> getNumbers() {
            return numbers;
        }

        public void setNumbers(final List<Number> numbers) {
            this.numbers = numbers;
        }
    }

    public static class Outer {
        private Numbers direct;
        private List<Numbers> list;

        public Numbers getDirect() {
            return direct;
        }

        public void setDirect(final Numbers direct) {
            this.direct = direct;
        }

        public List<Numbers> getList() {
            return list;
        }

        public void setList(final List<Numbers> list) {
            this.list = list;
        }
    }

    private static Numbers written() {
        final Numbers numbers = new Numbers();
        numbers.setId("1");
        numbers.setNumber(Fraction.of(3, 4));
        numbers.setObject(Fraction.of(1, 2));
        numbers.setFraction(Fraction.of(-5, 6));
        numbers.setOptionalNumber(Optional.of(Fraction.of(2, 3)));
        return numbers;
    }

    private static AttributeValue v1S(final String text) {
        return new AttributeValue().withS(text);
    }

    private static software.amazon.awssdk.services.dynamodb.model.AttributeValue v2S(final String text) {
        return software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS(text);
    }

    private static Map<String, AttributeValue> v1Item(final String name, final AttributeValue value) {
        final Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put(name, value);
        return item;
    }

    private static Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> v2Item(final String name,
            final software.amazon.awssdk.services.dynamodb.model.AttributeValue value) {
        final Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> item = new LinkedHashMap<>();
        item.put(name, value);
        return item;
    }

    private static void assertReadBack(final Numbers read, final String sdk) {
        assertAll(sdk, () -> assertSame(Fraction.class, read.getNumber().getClass()), () -> assertEquals(Fraction.of(3, 4), read.getNumber()),
                // Unchanged: an Object slot keeps the text, a Fraction slot reads the Fraction through its own type.
                () -> assertEquals("1/2", read.getObject()), () -> assertEquals(Fraction.of(-5, 6), read.getFraction()),
                () -> assertEquals(Optional.of(Fraction.of(2, 3)), read.getOptionalNumber()));
    }

    @Test
    public void numberSlotsReadTheFractionTheItemWasWrittenFrom_r2sliceB() {
        final Map<String, AttributeValue> v1Written = DynamoDBExecutor.toItem(written());
        final Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> v2Written = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor
                .toItem(written());

        final Map<String, AttributeValue> v1NumberMap = v1Item("number", v1S("1/4"));
        final Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> v2NumberMap = v2Item("number", v2S("1/4"));

        assertAll(() -> assertEquals("3/4", v1Written.get("number").getS()), () -> assertEquals("3/4", v2Written.get("number").s()),
                // Item mapping (toItem -> toEntity round trip).
                () -> assertReadBack(DynamoDBExecutor.toEntity(v1Written, Numbers.class), "v1"),
                () -> assertReadBack(com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(v2Written, Numbers.class), "v2"),
                // A native L attribute for a List<Number>: elements read with JSON rules.
                () -> assertEquals(
                        List.of(Fraction.of(1,
                                3), 5),
                        DynamoDBExecutor.toEntity(v1Item("numbers", new AttributeValue().withL(v1S("1/3"), new AttributeValue().withN("5"))), Numbers.class)
                                .getNumbers()),
                () -> assertEquals(List.of(Fraction.of(1, 3), 5),
                        com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(
                                v2Item("numbers",
                                        software.amazon.awssdk.services.dynamodb.model.AttributeValue
                                                .fromL(List.of(v2S("1/3"), software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromN("5")))),
                                Numbers.class).getNumbers()),
                // A direct (M attribute) bean and a JSON-context bean (element of an L attribute).
                () -> assertEquals(Fraction.of(1, 4),
                        DynamoDBExecutor.toEntity(v1Item("direct", new AttributeValue().withM(v1NumberMap)), Outer.class).getDirect().getNumber()),
                () -> assertEquals(Fraction.of(1, 4), DynamoDBExecutor
                        .toEntity(v1Item("list", new AttributeValue().withL(new AttributeValue().withM(v1NumberMap))), Outer.class)
                        .getList()
                        .get(0)
                        .getNumber()),
                () -> assertEquals(Fraction.of(1, 4),
                        com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor
                                .toEntity(v2Item("direct", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(v2NumberMap)), Outer.class)
                                .getDirect()
                                .getNumber()),
                () -> assertEquals(Fraction.of(1, 4),
                        com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor
                                .toEntity(
                                        v2Item("list",
                                                software.amazon.awssdk.services.dynamodb.model.AttributeValue
                                                        .fromL(List.of(software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(v2NumberMap)))),
                                        Outer.class)
                                .getList()
                                .get(0)
                                .getNumber()),
                // Single-value reads.
                () -> assertEquals(Fraction.of(3, 4),
                        DynamoDBExecutor.toList(new QueryResult().withItems(List.of(v1Item("v", v1S("3/4")))), Number.class).get(0)),
                () -> assertEquals(Fraction.of(3, 4),
                        com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor
                                .toList(QueryResponse.builder().items(List.of(v2Item("v", v2S("3/4")))).build(), Number.class)
                                .get(0)));
    }

    @Test
    public void otherNumberTextIsReadAsBefore_r2sliceB() {
        assertAll(
                // Not a number and not a Fraction's text: still rejected.
                () -> assertThrows(NumberFormatException.class, () -> DynamoDBExecutor.toEntity(v1Item("number", v1S("abc")), Numbers.class)),
                () -> assertThrows(NumberFormatException.class,
                        () -> com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(v2Item("number", v2S("abc")), Numbers.class)),
                // Fraction-like text that no Fraction is written as (zero denominator, padding, a sign on the denominator).
                () -> assertThrows(NumberFormatException.class, () -> DynamoDBExecutor.toEntity(v1Item("number", v1S("1/0")), Numbers.class)),
                () -> assertThrows(NumberFormatException.class, () -> DynamoDBExecutor.toEntity(v1Item("number", v1S(" 3/4")), Numbers.class)),
                () -> assertThrows(NumberFormatException.class, () -> DynamoDBExecutor.toEntity(v1Item("number", v1S("3/-4")), Numbers.class)),
                () -> assertThrows(NumberFormatException.class,
                        () -> DynamoDBExecutor.toEntity(v1Item("numbers", new AttributeValue().withL(v1S("1/0"))), Numbers.class)),
                // Decimal text keeps the JSON reader's numeric inference.
                () -> assertEquals(5, DynamoDBExecutor.toEntity(v1Item("number", new AttributeValue().withN("5")), Numbers.class).getNumber()),
                () -> assertEquals(1.5, DynamoDBExecutor.toEntity(v1Item("number", new AttributeValue().withN("1.5")), Numbers.class).getNumber()),
                () -> assertEquals(Optional.of(7),
                        DynamoDBExecutor.toEntity(v1Item("optionalNumber", new AttributeValue().withN("7")), Numbers.class).getOptionalNumber()));
    }
}
