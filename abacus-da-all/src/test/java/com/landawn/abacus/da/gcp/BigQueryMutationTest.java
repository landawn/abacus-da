package com.landawn.abacus.da.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.NamingPolicy;

public class BigQueryMutationTest extends BigQueryExecutorTestSupport {

    // Factories and insert operations

    @Test
    public void testInsertClassPropsGeneratesPositionalSql() throws Exception {
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("id", 1);
        props.put("name", "Test");
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        executor.insert(TestEntity.class, props);

        final String sql = captureGeneratedSql();
        assertTrue(sql.contains("?"), sql);
        assertFalse(sql.contains(":"), sql);
    }

    @Test
    public void testInsertEntity() throws Exception {
        TestEntity entity = new TestEntity();
        entity.setId(1);
        entity.setName("Test");

        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        TableResult result = executor.insert(entity);

        assertEquals(mockTableResult, result);
        verify(mockBigQuery).query(any(QueryJobConfiguration.class));
    }

    @Test
    public void testInsertEntityGeneratesPositionalSql() throws Exception {
        TestEntity entity = new TestEntity();
        entity.setId(1);
        entity.setName("Test");
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        executor.insert(entity);

        final String sql = captureGeneratedSql();
        assertTrue(sql.contains("?"), sql);
        assertFalse(sql.contains(":"), sql);
    }

    @Test
    public void testInsertWithClassAndProps() throws Exception {
        Map<String, Object> props = new HashMap<>();
        props.put("id", 1);
        props.put("name", "Test");

        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        TableResult result = executor.insert(TestEntity.class, props);

        assertEquals(mockTableResult, result);
        verify(mockBigQuery).query(any(QueryJobConfiguration.class));
    }

    @Test
    public void testInsertWithMap_CamelCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(mockBigQuery, NamingPolicy.CAMEL_CASE);
        Map<String, Object> props = new HashMap<>();
        props.put("id", 1);
        props.put("name", "X");

        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        assertSame(mockTableResult, exec.insert(TestEntity.class, props));
    }

    // Drives the SCREAMING_SNAKE_CASE and CAMEL_CASE branches of prepareInsert(Class,Map),
    // prepareUpdate(Class,Map,Condition) and prepareDelete(Class,Condition).

    @Test
    public void testInsertWithMap_ScreamingSnakeCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(mockBigQuery, NamingPolicy.SCREAMING_SNAKE_CASE);
        Map<String, Object> props = new HashMap<>();
        props.put("id", 1);
        props.put("name", "X");

        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        TableResult result = exec.insert(TestEntity.class, props);
        assertSame(mockTableResult, result);
    }

    // Updates and deletes

    @Test
    public void testDelete_CamelCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(mockBigQuery, NamingPolicy.CAMEL_CASE);
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);
        assertSame(mockTableResult, exec.delete(TestEntity.class, Filters.eq("id", 1)));
    }

    @Test
    public void testDelete_EntityNullKeyValue() {
        TestEntityWithStringKey entity = new TestEntityWithStringKey();
        entity.setId(null);
        assertThrows(IllegalArgumentException.class, () -> executor.delete(entity));
    }

    @Test
    public void testDelete_EntityNoKeyValue() {
        // Entity with the @Id-equivalent key but a default (zero) primitive int is still considered present;
        // however an entity whose only key is null/empty string should fail entityToCondition.
        TestEntityWithStringKey entity = new TestEntityWithStringKey();
        entity.setId(""); // empty string is treated as missing
        assertThrows(IllegalArgumentException.class, () -> executor.delete(entity));
    }

    @Test
    public void testDelete_ScreamingSnakeCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(mockBigQuery, NamingPolicy.SCREAMING_SNAKE_CASE);
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);
        assertSame(mockTableResult, exec.delete(TestEntity.class, Filters.eq("id", 1)));
    }

    @Test
    public void testDelete_WithIds_EmptyIds() {
        assertThrows(IllegalArgumentException.class, () -> executor.delete(TestEntity.class, new Object[0]));
    }

    @Test
    public void testDelete_WithIds_NullIdValue() {
        // idsToCondition's @NotEmpty check rejects a null-only varargs array.
        assertThrows(IllegalArgumentException.class, () -> executor.delete(TestEntity.class, (Object[]) null));
    }

    @Test
    public void testDelete_WithIds_MismatchCount() {
        // Single-key entity should reject more than one ID.
        assertThrows(IllegalArgumentException.class, () -> executor.delete(TestEntity.class, 1, 2));
    }

    @Test
    public void testDeleteClassConditionGeneratesPositionalSql() throws Exception {
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        executor.delete(TestEntity.class, Filters.eq("id", 1));

        final String sql = captureGeneratedSql();
        assertTrue(sql.contains("?"), sql);
        assertFalse(sql.contains(":"), sql);
    }

    @Test
    public void testDeleteEntity() throws Exception {
        TestEntity entity = new TestEntity();
        entity.setId(1);

        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        TableResult result = executor.delete(entity);

        assertEquals(mockTableResult, result);
        verify(mockBigQuery).query(any(QueryJobConfiguration.class));
    }

    // delete(Object)/update(Object): a null entity is rejected eagerly with IAE (matches insert(Object)).
    @Test
    public void testDeleteObject_NullEntityThrows() {
        assertThrows(IllegalArgumentException.class, () -> executor.delete((Object) null));
    }

    @Test
    public void testDeleteWithClassAndCondition() throws Exception {
        Condition condition = Filters.eq("id", 1);

        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        TableResult result = executor.delete(TestEntity.class, condition);

        assertEquals(mockTableResult, result);
        verify(mockBigQuery).query(any(QueryJobConfiguration.class));
    }

    @Test
    public void testDeleteWithClassAndIds() throws Exception {
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        TableResult result = executor.delete(TestEntity.class, 1);

        assertEquals(mockTableResult, result);
        verify(mockBigQuery).query(any(QueryJobConfiguration.class));
    }

    @Test
    public void testUpdate_EntityWithCompositeKey() throws Exception {
        CompositeKeyEntity e = new CompositeKeyEntity();
        e.setIdA("1");
        e.setIdB("2");
        e.setValue("v");
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);
        // Should not throw - composite key has both values set
        assertDoesNotThrowExt(() -> executor.update(e));
    }

    @Test
    public void testUpdateClassPropsConditionGeneratesPositionalSql() throws Exception {
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("name", "UpdatedTest");
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        executor.update(TestEntity.class, props, Filters.eq("id", 1));

        final String sql = captureGeneratedSql();
        assertTrue(sql.contains("?"), sql);
        assertFalse(sql.contains(":"), sql);
    }

    @Test
    public void testUpdateEntity() throws Exception {
        TestEntity entity = new TestEntity();
        entity.setId(1);
        entity.setName("UpdatedTest");

        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        TableResult result = executor.update(entity);

        assertEquals(mockTableResult, result);
        verify(mockBigQuery).query(any(QueryJobConfiguration.class));
    }

    @Test
    public void testUpdateEntity_NullPropertiesExcludedFromSetClause() throws Exception {
        // Regression: a null non-key property used to flow into the parameter list as a raw null,
        // which buildQueryParameterValue rejects with IAE — entity update was unusable for entities
        // with null fields. Null props are now excluded from SET (mirroring insert semantics).
        TestEntityWithEmail entity = new TestEntityWithEmail();
        entity.setId(1);
        entity.setName("OnlyName");
        // email left null
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        executor.update(entity, N.asSet("id"));

        final String sql = captureGeneratedSql();
        assertTrue(sql.contains("SET name = ? WHERE"), sql);
        assertFalse(sql.contains("email = ?"), sql);
    }

    @Test
    public void testUpdateEntity_AllNonKeyPropsNullThrows() {
        TestEntityWithEmail entity = new TestEntityWithEmail();
        entity.setId(1);

        assertThrows(IllegalArgumentException.class, () -> executor.update(entity, N.asSet("id")));
    }

    @Test
    public void testUpdateEntity_CamelCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(mockBigQuery, NamingPolicy.CAMEL_CASE);
        TestEntity entity = new TestEntity();
        entity.setId(1);
        entity.setName("X");

        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);
        assertSame(mockTableResult, exec.update(entity));
    }

    @Test
    public void testUpdateEntity_ScreamingSnakeCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(mockBigQuery, NamingPolicy.SCREAMING_SNAKE_CASE);
        TestEntity entity = new TestEntity();
        entity.setId(1);
        entity.setName("X");

        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);
        assertSame(mockTableResult, exec.update(entity));
    }

    // prepareUpdate: a null key value fails fast with a clear message
    // (was a confusing generic error from buildQueryParameterValue)
    @Test
    public void testUpdateEntityWithNullKeyValueThrowsDescriptiveIAE() {
        TestEntityWithStringKey entity = new TestEntityWithStringKey();
        entity.setValue("v"); // id left null

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> executor.update(entity, N.asSet("id")));
        assertTrue(ex.getMessage().contains("No property value specified in entity for key names"));
    }

    // update(entity, keys) rejects a null or empty key value (the same "null or empty" rule as entityToCondition
    // and the sibling executors); a whitespace-only key value is not blank-checked and is bound as a parameter.
    @Test
    public void testUpdateEntityWithKeys_EmptyKeyValueRejectedButWhitespaceKeyValueBound() throws Exception {
        TestEntityWithStringKey emptyKey = new TestEntityWithStringKey();
        emptyKey.setId("");
        emptyKey.setValue("v");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> executor.update(emptyKey, N.asSet("id")));
        assertTrue(ex.getMessage().contains("No property value specified in entity for key names"));
        verify(mockBigQuery, times(0)).query(any(QueryJobConfiguration.class));

        TestEntityWithStringKey whitespaceKey = new TestEntityWithStringKey();
        whitespaceKey.setId(" ");
        whitespaceKey.setValue("v");
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        executor.update(whitespaceKey, N.asSet("id"));

        final ArgumentCaptor<QueryJobConfiguration> captor = ArgumentCaptor.forClass(QueryJobConfiguration.class);
        verify(mockBigQuery).query(captor.capture());
        assertEquals("UPDATE test_entity_with_string_key SET value = ? WHERE id = ?", captor.getValue().getQuery());
        assertEquals(2, captor.getValue().getPositionalParameters().size());
        assertEquals(" ", captor.getValue().getPositionalParameters().get(1).getValue());
    }

    @Test
    public void testUpdateEntityWithKeysGeneratesPositionalSql() throws Exception {
        TestEntity entity = new TestEntity();
        entity.setId(1);
        entity.setName("UpdatedTest");
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        executor.update(entity, N.asSet("id"));

        final String sql = captureGeneratedSql();
        assertTrue(sql.contains("?"), sql);
        assertFalse(sql.contains(":"), sql);
    }

    // update(entity) on a class without key properties reported "primaryKeyNames cannot be null or empty", naming a
    // parameter the caller never passed; it now reports the missing keys like delete(entity).
    @Test
    public void testUpdateEntityWithoutKeyPropertiesReportsMissingKeyNames() throws Exception {
        final KeylessEntity entity = new KeylessEntity();
        entity.setName("n");

        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> executor.update(entity));

        assertTrue(ex.getMessage().contains("No key names defined for entity class: KeylessEntity"), ex.getMessage());
        assertEquals(ex.getMessage(), assertThrows(IllegalArgumentException.class, () -> executor.delete(entity)).getMessage());
        verify(mockBigQuery, times(0)).query(any(QueryJobConfiguration.class));
    }

    @Test
    public void testUpdateEntityWithPrimaryKeyNames() throws Exception {
        TestEntity entity = new TestEntity();
        entity.setId(1);
        entity.setName("UpdatedTest");
        Set<String> primaryKeyNames = N.asSet("id");

        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        TableResult result = executor.update(entity, primaryKeyNames);

        assertEquals(mockTableResult, result);
        verify(mockBigQuery).query(any(QueryJobConfiguration.class));
    }

    @Test
    public void testUpdateObject_NullEntityThrows() {
        assertThrows(IllegalArgumentException.class, () -> executor.update((Object) null));
    }

    @Test
    public void testUpdateObjectWithKeys_NullEntityThrows() {
        // The 2-arg update(Object, Set) overload must reject a null entity the same way (IAE), not NPE.
        assertThrows(IllegalArgumentException.class, () -> executor.update((Object) null, N.asSet("id")));
    }

    @Test
    public void testUpdateWithEmptyPrimaryKeyNames() {
        TestEntity entity = new TestEntity();
        entity.setId(1);
        Set<String> emptyKeyNames = N.toSet();

        assertThrows(IllegalArgumentException.class, () -> {
            executor.update(entity, emptyKeyNames);
        });
    }

    @Test
    public void testUpdateWithEmptyProps() {
        Map<String, Object> emptyProps = new HashMap<>();

        assertThrows(IllegalArgumentException.class, () -> {
            executor.update(TestEntity.class, emptyProps, Filters.eq("id", 1));
        });
    }

    @Test
    public void testUpdateWithClassPropsAndCondition() throws Exception {
        Map<String, Object> props = new HashMap<>();
        props.put("name", "UpdatedTest");
        Condition condition = Filters.eq("id", 1);

        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        TableResult result = executor.update(TestEntity.class, props, condition);

        assertEquals(mockTableResult, result);
        verify(mockBigQuery).query(any(QueryJobConfiguration.class));
    }

    @Test
    public void testUpdateWithMap_CamelCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(mockBigQuery, NamingPolicy.CAMEL_CASE);
        Map<String, Object> props = new HashMap<>();
        props.put("name", "Y");

        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);
        assertSame(mockTableResult, exec.update(TestEntity.class, props, Filters.eq("id", 1)));
    }

    @Test
    public void testUpdateWithMap_ScreamingSnakeCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(mockBigQuery, NamingPolicy.SCREAMING_SNAKE_CASE);
        Map<String, Object> props = new HashMap<>();
        props.put("name", "Y");

        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);
        assertSame(mockTableResult, exec.update(TestEntity.class, props, Filters.eq("id", 1)));
    }

    // Queries and results

    @Test
    public void testEntityToCondition_CompositeKey() throws Exception {
        CompositeKeyEntity e = new CompositeKeyEntity();
        e.setIdA("1");
        e.setIdB("2");
        e.setValue("v");
        // delete(entity) invokes entityToCondition internally
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);
        assertDoesNotThrowExt(() -> executor.delete(e));
    }

    @Test
    public void testEntityToCondition_CompositeKey_NullValueThrows() {
        CompositeKeyEntity e = new CompositeKeyEntity();
        e.setIdA("1");
        // idB is null
        e.setValue("v");
        assertThrows(IllegalArgumentException.class, () -> executor.delete(e));
    }

    // entityToCondition: empty key throws

    // idsToCondition: empty ids throws
    @Test
    public void testIdsToCondition_EmptyIdsThrows() {
        assertThrows(IllegalArgumentException.class, () -> executor.exists(TestEntity.class, new Object[0]));
    }

    // idsToCondition: a null targetClass is rejected with IAE (was a bare NPE from the key-name cache)
    @Test
    public void testIdsToCondition_NullTargetClassThrowsIAE() {
        assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.idsToCondition(null, "x"));
        assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.idsToCondition(null, new Object[0]));
    }

    @Test
    public void testIdsToCondition_CompositeKey() throws Exception {
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);
        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockTableResult.iterateAll()).thenReturn(new ArrayList<>());
        // exists(class, id1, id2) drives idsToCondition; non-matching id count throws else proceeds
        assertDoesNotThrowExt(() -> executor.exists(CompositeKeyEntity.class, "1", "2"));
    }

    @Test
    public void testIdsToCondition_CompositeKeyMismatchThrows() {
        assertThrows(IllegalArgumentException.class, () -> executor.exists(CompositeKeyEntity.class, "1"));
    }

    // idsToCondition: a class with no @Id/id-named property gets a descriptive message
    // (was a self-contradictory "provided 1 IDs but expected 1 key [id]")
    @Test
    public void testIdsToCondition_KeylessClassThrowsDescriptiveIAE() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.idsToCondition(KeylessEntity.class, "x"));
        assertTrue(ex.getMessage().contains("No @Id-annotated or id-named property"));
    }

    // idsToCondition: more ids than keys throws
    @Test
    public void testIdsToCondition_TooManyIdsThrows() {
        assertThrows(IllegalArgumentException.class, () -> executor.exists(TestEntity.class, 1, 2, 3));
    }

    /**
     * {@code targetClass} feeds the key-name cache (a ConcurrentHashMap), so an unchecked null used to
     * surface as a bare NullPointerException from the map rather than as an IllegalArgumentException.
     */
    @Test
    public void testKeyBasedMethodsRejectNullTargetClass() {
        assertThrows(IllegalArgumentException.class, () -> executor.delete((Class<?>) null, "id1"));
        assertThrows(IllegalArgumentException.class, () -> executor.exists((Class<?>) null, "id1"));
        assertThrows(IllegalArgumentException.class, () -> executor.exists((Class<?>) null, Filters.eq("id", 1)));
    }

    // Helper: assertDoesNotThrow returns Object; here we wrap to handle checked exceptions cleanly
    private static void assertDoesNotThrowExt(Runnable r) {
        try {
            r.run();
        } catch (Throwable t) {
            throw new AssertionError("Expected no exception, got: " + t, t);
        }
    }

    // Composite-key entity (2 @Id fields) used to exercise composite-key branches
    public static class CompositeKeyEntity {
        @com.landawn.abacus.annotation.Id
        private String idA;
        @com.landawn.abacus.annotation.Id
        private String idB;
        private String value;

        public String getIdA() {
            return idA;
        }

        public void setIdA(String idA) {
            this.idA = idA;
        }

        public String getIdB() {
            return idB;
        }

        public void setIdB(String idB) {
            this.idB = idB;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    // Entity with a String id used to exercise entityToCondition's empty/null-key error paths.
    // Entity with no @Id annotation and no id-named property (keyless)
    public static class KeylessEntity {
        private String name;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static class TestEntityWithStringKey {
        private String id;
        private String value;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    // Test entity classes
    public static class TestEntityWithEmail {
        private int id;
        private String name;
        private String email;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getEmail() {
            return email;
        }

        public void setEmail(String email) {
            this.email = email;
        }
    }
}
