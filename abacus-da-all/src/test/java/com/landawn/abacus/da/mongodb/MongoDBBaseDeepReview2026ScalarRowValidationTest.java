package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import com.mongodb.client.MongoIterable;

class MongoDBBaseDeepReview2026ScalarRowValidationTest {

    @Test
    void scalarTargetRejectsNonScalarRowsRegardlessOfPosition() {
        final List<Object> nonScalarRows = Arrays.asList(new BeanRow(), new Document("value", 2), List.of(2), new Object[] { 2 });

        for (final Object nonScalarRow : nonScalarRows) {
            assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toList(iterable(Arrays.asList(nonScalarRow, "scalar")), String.class),
                    nonScalarRow.getClass().getName());
            assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toList(iterable(Arrays.asList("scalar", nonScalarRow)), String.class),
                    nonScalarRow.getClass().getName());
        }
    }

    @Test
    void scalarTargetStillConvertsEveryScalarRowAndRetainsNulls() {
        assertEquals(Arrays.asList("1", null, "2"), MongoDBBase.toList(iterable(Arrays.asList(1, null, 2L)), String.class));
    }

    @SuppressWarnings("unchecked")
    private static MongoIterable<?> iterable(final List<?> rows) {
        final MongoIterable<Object> result = mock(MongoIterable.class);
        when(result.into(any())).thenAnswer(invocation -> {
            final List<Object> target = invocation.getArgument(0);
            target.addAll(rows);
            return target;
        });
        return result;
    }

    public static class BeanRow {
        private String value;

        public String getValue() {
            return value;
        }

        public void setValue(final String value) {
            this.value = value;
        }
    }
}
