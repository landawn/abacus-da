/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.annotation.Entity;
import com.landawn.abacus.da.MockitoTestBase;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.TypeReference;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;

class MongoDBBaseLineCoverageTest extends MockitoTestBase {

    @Test
    void toDocumentRejectsABeanThatHasNoGetters() {
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toDocument(new PublicFieldOnlyBean()));

        assertEquals("No property getter/setter method found in the specified entity: " + PublicFieldOnlyBean.class.getCanonicalName(), ex.getMessage());
    }

    @Test
    void parameterizedNonBeanHasNoPropertyConversions() throws Exception {
        final Method propConversions = MongoDBBase.class.getDeclaredMethod("propConversions", Type.class);
        propConversions.setAccessible(true);

        final Object conversions = propConversions.invoke(null, createType(new TypeReference<List<String>>() {
        }));

        assertNull(conversions);
    }

    @Test
    void getterOnlyPropertyIsResolvedWhileReadingTheEntity() {
        final UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class,
                () -> MongoDBBase.toEntity(new Document("code", 1L), GetterOnlyEntity.class));

        assertEquals("No all arguments constructor found in class: " + GetterOnlyEntity.class.getCanonicalName(), ex.getMessage());
    }

    @Test
    void abstractMapTargetKeepsTheFactoryFailure() {
        final MongoIterable<?> iterable = iterableOf(new Document("a", 1));

        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toList(iterable, AbstractCustomMap.class));

        assertEquals("Not able to create instance for abstract Map: " + AbstractCustomMap.class, ex.getMessage());
    }

    @Test
    void scalarMapValidationStopsAfterTheFirstNonMapRow() {
        final MongoIterable<?> iterable = iterableOf(new Document("n", 1), "text", new Object());

        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> MongoDBBase.toList(iterable, Serializable.class));

        assertEquals("Cannot convert document: text to class: " + ClassUtil.getCanonicalClassName(Serializable.class), ex.getMessage());
    }

    @Test
    void nullIdIsRemovedFromTheDocument() {
        final Map<String, Object> source = new HashMap<>();
        source.put("_id", null);
        source.put("name", "Ada");

        final Document document = MongoDBBase.toDocument(source);

        assertFalse(document.containsKey("_id"));
        assertEquals("Ada", document.get("name"));
    }

    @SuppressWarnings("unchecked")
    private static MongoIterable<?> iterableOf(final Object... rows) {
        final MongoIterable<Object> iterable = mock(MongoIterable.class);
        final MongoCursor<Object> cursor = mock(MongoCursor.class);
        final int[] index = { 0 };
        when(iterable.iterator()).thenReturn(cursor);
        when(cursor.hasNext()).thenAnswer(invocation -> index[0] < rows.length);
        when(cursor.next()).thenAnswer(invocation -> rows[index[0]++]);
        return iterable;
    }

    public static class PublicFieldOnlyBean {
        public String name;
    }

    @Entity
    public static class GetterOnlyEntity {
        public Long getCode() {
            return 1L;
        }
    }

    public abstract static class AbstractCustomMap<K, V> implements Map<K, V> {
    }
}
