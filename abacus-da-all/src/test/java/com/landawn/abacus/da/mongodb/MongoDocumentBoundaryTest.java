package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Projections;

class MongoDocumentBoundaryTest {

    @Test
    void rowConversionPreservesFieldOrderAndRequestedContainerTypes() {
        final Document row = new Document("first", "Ada").append("last", "Lovelace");
        final Object[] expected = { "Ada", "Lovelace" };
        assertArrayEquals(expected, MongoDBBase.readRow(row, null));
        assertArrayEquals(expected, MongoDBBase.readRow(row, Object[].class));
        assertArrayEquals(expected, MongoDBBase.readRow(row, String[].class));
        assertEquals(List.of("Ada", "Lovelace"), MongoDBBase.readRow(row, List.class));
        final LinkedHashMap<?, ?> copy = MongoDBBase.readRow(row, LinkedHashMap.class);
        assertEquals(new LinkedHashMap<>(row), copy);
        assertNotSame(row, copy);
        assertEquals(List.of("first", "last"), List.copyOf(copy.keySet()));
        assertThrows(ArrayStoreException.class, () -> MongoDBBase.readRow(new Document("value", 7), String[].class));
    }

    @Test
    void nullRowsUsePrimitiveDefaultsOrNullReferences() {
        assertNull(MongoDBBase.readRow(null, null));
        assertNull(MongoDBBase.readRow(null, String.class));
        assertEquals(0, MongoDBBase.readRow(null, int.class));
    }

    @Test
    void scalarListsUseIdsWhenNoProjectedValueExistsAndPreserveNullRows() {
        final List<?> rows = Arrays.asList(null, new Document("_id", 1), new Document("_id", 2L));
        assertEquals(Arrays.asList(null, 1L, 2L), MongoDBBase.toList(iterable(rows), Long.class));
        assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toList(iterable(List.of(new Document("value", 1), 2)), Long.class));
    }

    @SuppressWarnings("unchecked")
    @Test
    void bsonProjectionConvertsTheFirstValueAndDistinguishesAbsentResults() {
        final MongoCollection<Document> collection = mock(MongoCollection.class);
        final FindIterable<Document> found = mock(FindIterable.class, RETURNS_SELF);
        when(collection.find(any(Bson.class))).thenReturn(found);
        when(found.first()).thenReturn(new Document("_id", "key").append("value", 7), null, new Document());
        final MongoCollectionExecutor executor = new MongoCollectionExecutor(collection, new com.landawn.abacus.util.AsyncExecutor(Runnable::run));
        final Bson projection = Projections.include("value");
        final Bson filter = new Document("active", true);
        assertEquals(7L, executor.findFirst(projection, filter, null, Long.class).get());
        assertFalse(executor.findFirst(projection, filter, null, Long.class).isPresent());
        assertFalse(executor.findFirst(projection, filter, null, Long.class).isPresent());
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
}
