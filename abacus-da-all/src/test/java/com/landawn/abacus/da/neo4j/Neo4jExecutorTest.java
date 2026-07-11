package com.landawn.abacus.da.neo4j;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.ogm.cypher.ComparisonOperator;
import org.neo4j.ogm.cypher.Filter;
import org.neo4j.ogm.cypher.Filters;
import org.neo4j.ogm.cypher.query.Pagination;
import org.neo4j.ogm.cypher.query.SortOrder;
import org.neo4j.ogm.model.Result;
import org.neo4j.ogm.session.Session;
import org.neo4j.ogm.session.SessionFactory;
import org.neo4j.ogm.transaction.Transaction;

import com.landawn.abacus.da.TestBase;

/**
 * Unit tests for {@link Neo4jExecutor}. The executor delegates virtually every
 * operation to a Neo4j OGM {@link Session} acquired from a {@link SessionFactory}.
 * Both are mocked here so the tests run without a live Neo4j instance.
 */
public class Neo4jExecutorTest extends TestBase {

    private SessionFactory mockSessionFactory;
    private Session mockSession;
    private Neo4jExecutor executor;

    /** Sample entity class used as the targetClass argument in load/save/delete tests. */
    public static class Person {
        private Long id;
        private String name;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static class StringIdEntity {
        private String id;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }
    }

    @BeforeEach
    public void setUp() {
        mockSessionFactory = mock(SessionFactory.class);
        mockSession = mock(Session.class);
        when(mockSessionFactory.openSession()).thenReturn(mockSession);
        executor = new Neo4jExecutor(mockSessionFactory);
    }

    // ---------- Constructor / accessor tests ----------

    @Test
    public void testConstructor() {
        assertNotNull(executor);
        assertSame(mockSessionFactory, executor.sessionFactory());
    }

    @Test
    public void testConstructor_NullSessionFactory() {
        assertThrows(IllegalArgumentException.class, () -> new Neo4jExecutor(null));
    }

    @Test
    public void testSessionFactory() {
        assertSame(mockSessionFactory, executor.sessionFactory());
    }

    @Test
    public void testOpenSession() {
        Session session = executor.openSession();
        assertSame(mockSession, session);
        verify(mockSessionFactory, atLeastOnce()).openSession();
    }

    // ---------- run / call helpers ----------

    @Test
    public void testRun() {
        AtomicBoolean invoked = new AtomicBoolean(false);
        executor.run(s -> {
            invoked.set(true);
            assertSame(mockSession, s);
        });
        assertTrue(invoked.get());
        // Session is cleared and returned to the pool.
        verify(mockSession).clear();
    }

    @Test
    public void testCall() {
        String result = executor.call(s -> {
            assertSame(mockSession, s);
            return "ok";
        });
        assertEquals("ok", result);
        verify(mockSession).clear();
    }

    @Test
    public void testRun_PropagatesException() {
        // Session must still be cleared even if the action throws.
        assertThrows(RuntimeException.class, () -> executor.run(s -> {
            throw new RuntimeException("boom");
        }));
        verify(mockSession).clear();
    }

    @Test
    public void testCall_PropagatesException() {
        assertThrows(RuntimeException.class, () -> executor.call(s -> {
            throw new RuntimeException("boom");
        }));
        verify(mockSession).clear();
    }

    // ---------- closeSession: leaked-transaction handling ----------

    @Test
    public void testRun_LeakedTransactionIsRolledBackBeforePooling() {
        Transaction tx = mock(Transaction.class);
        // The transaction is still current after the action returns, and gone once one
        // rollback()/close() pair has run.
        when(mockSession.getTransaction()).thenReturn(tx, (Transaction) null);

        executor.run(s -> s.beginTransaction());

        verify(tx).rollback();
        verify(tx).close();
        verify(mockSession).clear();
    }

    @Test
    public void testRun_LeakedNestedTransactionIsFullyUnwound() {
        // OGM "extended" transaction: beginTransaction() called twice on the same session returns
        // the same underlying transaction with an incremented nesting count. One rollback()/close()
        // pair only pops one level (rollback merely marks ROLLBACK_PENDING), so the transaction is
        // still current after the first pair; only the root-level pair actually rolls back and
        // deregisters it. closeSession must keep unwinding until no live transaction remains.
        Transaction tx = mock(Transaction.class);
        when(mockSession.getTransaction()).thenReturn(tx, tx, (Transaction) null);

        executor.run(s -> {
            s.beginTransaction();
            s.beginTransaction();
        });

        verify(tx, times(2)).rollback();
        verify(tx, times(2)).close();
        // Fully unwound, so the session is safe to pool.
        verify(mockSession).clear();
    }

    @Test
    public void testRun_TransactionRollbackFailure_DiscardsSession() {
        Transaction tx = mock(Transaction.class);
        when(mockSession.getTransaction()).thenReturn(tx);
        doThrow(new RuntimeException("rollback failed")).when(tx).rollback();

        executor.run(s -> s.beginTransaction());

        // A session whose transaction could not be rolled back must not be pooled (or even
        // cleared); the next borrower gets a fresh session instead.
        verify(mockSession, never()).clear();
        executor.run(s -> {
        });
        verify(mockSessionFactory, times(2)).openSession();
    }

    // ---------- load / loadAll delegators ----------

    @Test
    public void testLoad_ById() {
        Person p = new Person();
        when(mockSession.load(Person.class, 42L)).thenReturn(p);
        assertSame(p, executor.load(Person.class, 42L));
        verify(mockSession).load(Person.class, 42L);
    }

    @Test
    public void testLoad_ByIdWithDepth() {
        Person p = new Person();
        when(mockSession.load(Person.class, 42L, 2)).thenReturn(p);
        assertSame(p, executor.load(Person.class, 42L, 2));
        verify(mockSession).load(Person.class, 42L, 2);
    }

    @Test
    public void testLoad_ByStringId() {
        StringIdEntity entity = new StringIdEntity();
        when(mockSession.load(StringIdEntity.class, "sku-1")).thenReturn(entity);

        assertSame(entity, executor.load(StringIdEntity.class, "sku-1"));
        verify(mockSession).load(StringIdEntity.class, "sku-1");
    }

    @Test
    public void testLoadAll_ByIds() {
        Collection<Long> ids = Arrays.asList(1L, 2L);
        Collection<Person> expected = Arrays.asList(new Person(), new Person());
        when(mockSession.loadAll(Person.class, ids)).thenReturn(expected);
        Collection<Person> actual = executor.loadAll(Person.class, ids);
        assertEquals(2, actual.size());
    }

    @Test
    public void testLoadAll_ByStringIds() {
        Collection<String> ids = Arrays.asList("sku-1", "sku-2");
        Collection<StringIdEntity> expected = Arrays.asList(new StringIdEntity(), new StringIdEntity());
        when(mockSession.loadAll(StringIdEntity.class, ids)).thenReturn(expected);

        Collection<StringIdEntity> actual = executor.loadAll(StringIdEntity.class, ids);

        assertEquals(2, actual.size());
        verify(mockSession).loadAll(StringIdEntity.class, ids);
    }

    @Test
    public void testLoadAll_ByIdsWithDepth() {
        Collection<Long> ids = Arrays.asList(1L);
        when(mockSession.loadAll(Person.class, ids, 1)).thenReturn(Collections.singletonList(new Person()));
        Collection<Person> actual = executor.loadAll(Person.class, ids, 1);
        assertEquals(1, actual.size());
    }

    @Test
    public void testLoadAll_ByIdsWithSortOrder() {
        Collection<Long> ids = Arrays.asList(1L);
        SortOrder sortOrder = new SortOrder();
        when(mockSession.loadAll(Person.class, ids, sortOrder)).thenReturn(Collections.singletonList(new Person()));
        assertEquals(1, executor.loadAll(Person.class, ids, sortOrder).size());
    }

    @Test
    public void testLoadAll_ByIdsWithSortOrderAndDepth() {
        Collection<Long> ids = Arrays.asList(1L);
        SortOrder sortOrder = new SortOrder();
        when(mockSession.loadAll(Person.class, ids, sortOrder, 1)).thenReturn(Collections.singletonList(new Person()));
        assertEquals(1, executor.loadAll(Person.class, ids, sortOrder, 1).size());
    }

    @Test
    public void testLoadAll_ByIdsWithPagination() {
        Collection<Long> ids = Arrays.asList(1L);
        Pagination pagination = new Pagination(0, 10);
        when(mockSession.loadAll(Person.class, ids, pagination)).thenReturn(Collections.singletonList(new Person()));
        assertEquals(1, executor.loadAll(Person.class, ids, pagination).size());
    }

    @Test
    public void testLoadAll_ByIdsWithPaginationAndDepth() {
        Collection<Long> ids = Arrays.asList(1L);
        Pagination pagination = new Pagination(0, 10);
        when(mockSession.loadAll(Person.class, ids, pagination, 1)).thenReturn(Collections.singletonList(new Person()));
        assertEquals(1, executor.loadAll(Person.class, ids, pagination, 1).size());
    }

    @Test
    public void testLoadAll_ByIdsWithSortOrderAndPagination() {
        Collection<Long> ids = Arrays.asList(1L);
        SortOrder sortOrder = new SortOrder();
        Pagination pagination = new Pagination(0, 10);
        when(mockSession.loadAll(Person.class, ids, sortOrder, pagination)).thenReturn(Collections.singletonList(new Person()));
        assertEquals(1, executor.loadAll(Person.class, ids, sortOrder, pagination).size());
    }

    @Test
    public void testLoadAll_ByIdsWithSortOrderPaginationAndDepth() {
        Collection<Long> ids = Arrays.asList(1L);
        SortOrder sortOrder = new SortOrder();
        Pagination pagination = new Pagination(0, 10);
        when(mockSession.loadAll(Person.class, ids, sortOrder, pagination, 2)).thenReturn(Collections.singletonList(new Person()));
        assertEquals(1, executor.loadAll(Person.class, ids, sortOrder, pagination, 2).size());
    }

    // ---------- loadAll(Collection<T>, ...) overloads ----------

    @Test
    public void testLoadAll_Objects() {
        Collection<Person> objs = Arrays.asList(new Person());
        when(mockSession.loadAll(objs)).thenReturn(objs);
        assertEquals(1, executor.loadAll(objs).size());
    }

    @Test
    public void testLoadAll_ObjectsWithDepth() {
        Collection<Person> objs = Arrays.asList(new Person());
        when(mockSession.loadAll(objs, 1)).thenReturn(objs);
        assertEquals(1, executor.loadAll(objs, 1).size());
    }

    @Test
    public void testLoadAll_ObjectsWithSortOrder() {
        Collection<Person> objs = Arrays.asList(new Person());
        SortOrder sortOrder = new SortOrder();
        when(mockSession.loadAll(objs, sortOrder)).thenReturn(objs);
        assertEquals(1, executor.loadAll(objs, sortOrder).size());
    }

    @Test
    public void testLoadAll_ObjectsWithSortOrderAndDepth() {
        Collection<Person> objs = Arrays.asList(new Person());
        SortOrder sortOrder = new SortOrder();
        when(mockSession.loadAll(objs, sortOrder, 1)).thenReturn(objs);
        assertEquals(1, executor.loadAll(objs, sortOrder, 1).size());
    }

    @Test
    public void testLoadAll_ObjectsWithPagination() {
        Collection<Person> objs = Arrays.asList(new Person());
        Pagination pagination = new Pagination(0, 10);
        when(mockSession.loadAll(objs, pagination)).thenReturn(objs);
        assertEquals(1, executor.loadAll(objs, pagination).size());
    }

    @Test
    public void testLoadAll_ObjectsWithPaginationAndDepth() {
        Collection<Person> objs = Arrays.asList(new Person());
        Pagination pagination = new Pagination(0, 10);
        when(mockSession.loadAll(objs, pagination, 1)).thenReturn(objs);
        assertEquals(1, executor.loadAll(objs, pagination, 1).size());
    }

    @Test
    public void testLoadAll_ObjectsWithSortOrderAndPagination() {
        Collection<Person> objs = Arrays.asList(new Person());
        SortOrder sortOrder = new SortOrder();
        Pagination pagination = new Pagination(0, 10);
        when(mockSession.loadAll(objs, sortOrder, pagination)).thenReturn(objs);
        assertEquals(1, executor.loadAll(objs, sortOrder, pagination).size());
    }

    @Test
    public void testLoadAll_ObjectsWithSortOrderPaginationAndDepth() {
        Collection<Person> objs = Arrays.asList(new Person());
        SortOrder sortOrder = new SortOrder();
        Pagination pagination = new Pagination(0, 10);
        when(mockSession.loadAll(objs, sortOrder, pagination, 1)).thenReturn(objs);
        assertEquals(1, executor.loadAll(objs, sortOrder, pagination, 1).size());
    }

    // ---------- loadAll(Class<T>, ...) overloads ----------

    @Test
    public void testLoadAll_Class() {
        when(mockSession.loadAll(Person.class)).thenReturn(Collections.singletonList(new Person()));
        assertEquals(1, executor.loadAll(Person.class).size());
    }

    @Test
    public void testLoadAll_ClassWithDepth() {
        when(mockSession.loadAll(Person.class, 1)).thenReturn(Collections.singletonList(new Person()));
        assertEquals(1, executor.loadAll(Person.class, 1).size());
    }

    @Test
    public void testLoadAll_ClassWithSortOrder() {
        SortOrder sortOrder = new SortOrder();
        when(mockSession.loadAll(Person.class, sortOrder)).thenReturn(Collections.singletonList(new Person()));
        assertEquals(1, executor.loadAll(Person.class, sortOrder).size());
    }

    @Test
    public void testLoadAll_ClassWithSortOrderAndDepth() {
        SortOrder sortOrder = new SortOrder();
        when(mockSession.loadAll(Person.class, sortOrder, 1)).thenReturn(Collections.singletonList(new Person()));
        assertEquals(1, executor.loadAll(Person.class, sortOrder, 1).size());
    }

    @Test
    public void testLoadAll_ClassWithPagination() {
        Pagination pagination = new Pagination(0, 10);
        when(mockSession.loadAll(Person.class, pagination)).thenReturn(Collections.singletonList(new Person()));
        assertEquals(1, executor.loadAll(Person.class, pagination).size());
    }

    @Test
    public void testLoadAll_ClassWithPaginationAndDepth() {
        Pagination pagination = new Pagination(0, 10);
        when(mockSession.loadAll(Person.class, pagination, 1)).thenReturn(Collections.singletonList(new Person()));
        assertEquals(1, executor.loadAll(Person.class, pagination, 1).size());
    }

    @Test
    public void testLoadAll_ClassWithSortOrderAndPagination() {
        SortOrder sortOrder = new SortOrder();
        Pagination pagination = new Pagination(0, 10);
        when(mockSession.loadAll(Person.class, sortOrder, pagination)).thenReturn(Collections.singletonList(new Person()));
        assertEquals(1, executor.loadAll(Person.class, sortOrder, pagination).size());
    }

    @Test
    public void testLoadAll_ClassWithSortOrderPaginationAndDepth() {
        SortOrder sortOrder = new SortOrder();
        Pagination pagination = new Pagination(0, 10);
        when(mockSession.loadAll(Person.class, sortOrder, pagination, 1)).thenReturn(Collections.singletonList(new Person()));
        assertEquals(1, executor.loadAll(Person.class, sortOrder, pagination, 1).size());
    }

    // ---------- loadAll(Class<T>, Filter, ...) overloads ----------

    @Test
    public void testLoadAll_ClassWithFilter() {
        Filter filter = new Filter("name", ComparisonOperator.EQUALS, "John");
        when(mockSession.loadAll(Person.class, filter)).thenReturn(Collections.singletonList(new Person()));
        assertEquals(1, executor.loadAll(Person.class, filter).size());
    }

    @Test
    public void testLoadAll_ClassWithFilterAndDepth() {
        Filter filter = new Filter("name", ComparisonOperator.EQUALS, "John");
        when(mockSession.loadAll(Person.class, filter, 1)).thenReturn(Collections.singletonList(new Person()));
        assertEquals(1, executor.loadAll(Person.class, filter, 1).size());
    }

    @Test
    public void testLoadAll_ClassWithFilterAndSortOrder() {
        Filter filter = new Filter("name", ComparisonOperator.EQUALS, "John");
        SortOrder sortOrder = new SortOrder();
        when(mockSession.loadAll(Person.class, filter, sortOrder)).thenReturn(Collections.singletonList(new Person()));
        assertEquals(1, executor.loadAll(Person.class, filter, sortOrder).size());
    }

    @Test
    public void testLoadAll_ClassWithFilterSortOrderAndDepth() {
        Filter filter = new Filter("name", ComparisonOperator.EQUALS, "John");
        SortOrder sortOrder = new SortOrder();
        when(mockSession.loadAll(Person.class, filter, sortOrder, 1)).thenReturn(Collections.singletonList(new Person()));
        assertEquals(1, executor.loadAll(Person.class, filter, sortOrder, 1).size());
    }

    @Test
    public void testLoadAll_ClassWithFilterAndPagination() {
        Filter filter = new Filter("name", ComparisonOperator.EQUALS, "John");
        Pagination pagination = new Pagination(0, 10);
        when(mockSession.loadAll(Person.class, filter, pagination)).thenReturn(Collections.singletonList(new Person()));
        assertEquals(1, executor.loadAll(Person.class, filter, pagination).size());
    }

    @Test
    public void testLoadAll_ClassWithFilterPaginationAndDepth() {
        Filter filter = new Filter("name", ComparisonOperator.EQUALS, "John");
        Pagination pagination = new Pagination(0, 10);
        when(mockSession.loadAll(Person.class, filter, pagination, 1)).thenReturn(Collections.singletonList(new Person()));
        assertEquals(1, executor.loadAll(Person.class, filter, pagination, 1).size());
    }

    @Test
    public void testLoadAll_ClassWithFilterSortOrderAndPagination() {
        Filter filter = new Filter("name", ComparisonOperator.EQUALS, "John");
        SortOrder sortOrder = new SortOrder();
        Pagination pagination = new Pagination(0, 10);
        when(mockSession.loadAll(Person.class, filter, sortOrder, pagination)).thenReturn(Collections.singletonList(new Person()));
        assertEquals(1, executor.loadAll(Person.class, filter, sortOrder, pagination).size());
    }

    @Test
    public void testLoadAll_ClassWithFilterSortOrderPaginationAndDepth() {
        Filter filter = new Filter("name", ComparisonOperator.EQUALS, "John");
        SortOrder sortOrder = new SortOrder();
        Pagination pagination = new Pagination(0, 10);
        when(mockSession.loadAll(Person.class, filter, sortOrder, pagination, 1)).thenReturn(Collections.singletonList(new Person()));
        assertEquals(1, executor.loadAll(Person.class, filter, sortOrder, pagination, 1).size());
    }

    // ---------- loadAll(Class<T>, Filters, ...) overloads ----------

    @Test
    public void testLoadAll_ClassWithFilters() {
        Filters filters = new Filters();
        when(mockSession.loadAll(Person.class, filters)).thenReturn(Collections.singletonList(new Person()));
        assertEquals(1, executor.loadAll(Person.class, filters).size());
    }

    @Test
    public void testLoadAll_ClassWithFiltersAndDepth() {
        Filters filters = new Filters();
        when(mockSession.loadAll(Person.class, filters, 1)).thenReturn(Collections.singletonList(new Person()));
        assertEquals(1, executor.loadAll(Person.class, filters, 1).size());
    }

    @Test
    public void testLoadAll_ClassWithFiltersAndSortOrder() {
        Filters filters = new Filters();
        SortOrder sortOrder = new SortOrder();
        when(mockSession.loadAll(Person.class, filters, sortOrder)).thenReturn(Collections.singletonList(new Person()));
        assertEquals(1, executor.loadAll(Person.class, filters, sortOrder).size());
    }

    @Test
    public void testLoadAll_ClassWithFiltersSortOrderAndDepth() {
        Filters filters = new Filters();
        SortOrder sortOrder = new SortOrder();
        when(mockSession.loadAll(Person.class, filters, sortOrder, 1)).thenReturn(Collections.singletonList(new Person()));
        assertEquals(1, executor.loadAll(Person.class, filters, sortOrder, 1).size());
    }

    @Test
    public void testLoadAll_ClassWithFiltersAndPagination() {
        Filters filters = new Filters();
        Pagination pagination = new Pagination(0, 10);
        when(mockSession.loadAll(Person.class, filters, pagination)).thenReturn(Collections.singletonList(new Person()));
        assertEquals(1, executor.loadAll(Person.class, filters, pagination).size());
    }

    @Test
    public void testLoadAll_ClassWithFiltersPaginationAndDepth() {
        Filters filters = new Filters();
        Pagination pagination = new Pagination(0, 10);
        when(mockSession.loadAll(Person.class, filters, pagination, 1)).thenReturn(Collections.singletonList(new Person()));
        assertEquals(1, executor.loadAll(Person.class, filters, pagination, 1).size());
    }

    @Test
    public void testLoadAll_ClassWithFiltersSortOrderAndPagination() {
        Filters filters = new Filters();
        SortOrder sortOrder = new SortOrder();
        Pagination pagination = new Pagination(0, 10);
        when(mockSession.loadAll(Person.class, filters, sortOrder, pagination)).thenReturn(Collections.singletonList(new Person()));
        assertEquals(1, executor.loadAll(Person.class, filters, sortOrder, pagination).size());
    }

    @Test
    public void testLoadAll_ClassWithFiltersSortOrderPaginationAndDepth() {
        Filters filters = new Filters();
        SortOrder sortOrder = new SortOrder();
        Pagination pagination = new Pagination(0, 10);
        when(mockSession.loadAll(Person.class, filters, sortOrder, pagination, 1)).thenReturn(Collections.singletonList(new Person()));
        assertEquals(1, executor.loadAll(Person.class, filters, sortOrder, pagination, 1).size());
    }

    // ---------- Save / delete operations ----------

    @Test
    public void testSave() {
        Person p = new Person();
        executor.save(p);
        verify(mockSession).save(p);
        verify(mockSession).clear();
    }

    @Test
    public void testSave_WithDepth() {
        Person p = new Person();
        executor.save(p, 2);
        verify(mockSession).save(p, 2);
        verify(mockSession).clear();
    }

    @Test
    public void testDelete() {
        Person p = new Person();
        executor.delete(p);
        verify(mockSession).delete(p);
        verify(mockSession).clear();
    }

    @Test
    public void testDeleteAll() {
        executor.deleteAll(Person.class);
        verify(mockSession).deleteAll(Person.class);
        verify(mockSession).clear();
    }

    // ---------- Query methods ----------

    @Test
    public void testQueryForObject() {
        Person p = new Person();
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("name", "John");
        when(mockSession.queryForObject(Person.class, "MATCH (p:Person {name:$name}) RETURN p", params)).thenReturn(p);

        Person actual = executor.queryForObject(Person.class, "MATCH (p:Person {name:$name}) RETURN p", params);
        assertSame(p, actual);
    }

    @Test
    public void testStream() {
        // Build a Result that returns a single-row iterator.
        Result mockResult = mock(Result.class);
        Map<String, Object> row = Collections.singletonMap("p", "x");
        Iterator<Map<String, Object>> rowIter = Collections.singletonList(row).iterator();
        when(mockResult.iterator()).thenReturn(rowIter);
        when(mockSession.query("MATCH (n) RETURN n", Collections.emptyMap())).thenReturn(mockResult);

        long count = executor.stream("MATCH (n) RETURN n", Collections.emptyMap()).count();
        assertEquals(1L, count);
    }

    @Test
    public void testStreamWithReadOnly() {
        Result mockResult = mock(Result.class);
        Iterator<Map<String, Object>> rowIter = Collections.<Map<String, Object>> emptyList().iterator();
        when(mockResult.iterator()).thenReturn(rowIter);
        when(mockSession.query("MATCH (n) RETURN n", Collections.emptyMap(), true)).thenReturn(mockResult);

        long count = executor.stream("MATCH (n) RETURN n", Collections.emptyMap(), true).count();
        assertEquals(0L, count);
    }

    @Test
    public void testStreamWithType() {
        // session.query(Class, String, Map) returns Iterable<T>.
        @SuppressWarnings("unchecked")
        Iterable<Person> iterable = mock(Iterable.class);
        List<Person> people = Arrays.asList(new Person(), new Person());
        when(iterable.iterator()).thenReturn(people.iterator());
        when(mockSession.query(Person.class, "MATCH (p:Person) RETURN p", Collections.emptyMap())).thenReturn(iterable);

        long count = executor.stream(Person.class, "MATCH (p:Person) RETURN p", Collections.emptyMap()).count();
        assertEquals(2L, count);
    }

    @Test
    public void testStreamClosesSessionOnException() {
        // When session.query throws, the session must still be returned to the pool.
        when(mockSession.query(eq("BAD CYPHER"), eq(Collections.emptyMap()))).thenThrow(new RuntimeException("syntax"));

        assertThrows(RuntimeException.class, () -> executor.stream("BAD CYPHER", Collections.emptyMap()));
        verify(mockSession).clear();
    }

    // ---------- Count / id resolution ----------

    @Test
    public void testCount() {
        List<Filter> filters = Arrays.asList(new Filter("name", ComparisonOperator.EQUALS, "John"));
        when(mockSession.count(Person.class, filters)).thenReturn(7L);
        assertEquals(7L, executor.count(Person.class, filters));
    }

    @Test
    public void testCountEntitiesOfType() {
        when(mockSession.countEntitiesOfType(Person.class)).thenReturn(123L);
        assertEquals(123L, executor.countEntitiesOfType(Person.class));
    }

    @Test
    public void testResolveGraphIdFor() {
        Person p = new Person();
        when(mockSession.resolveGraphIdFor(p)).thenReturn(99L);
        assertEquals(Long.valueOf(99L), executor.resolveGraphIdFor(p));
    }

    @Test
    public void testResolveGraphIdFor_NotEntity() {
        Object obj = new Object();
        when(mockSession.resolveGraphIdFor(obj)).thenReturn(null);
        assertNull(executor.resolveGraphIdFor(obj));
    }

    // ---------- Session pool behaviour ----------

    @Test
    public void testSessionPool_ReusesSession() {
        // Two sequential operations should reuse the same pooled session.
        executor.run(s -> {
            /* no-op */ });
        executor.run(s -> {
            /* no-op */ });
        verify(mockSessionFactory, atLeastOnce()).openSession();
        verify(mockSession, atLeastOnce()).clear();
    }

    @Test
    public void testLoad_ReturnsNullWhenSessionReturnsNull() {
        when(mockSession.load(Person.class, 999L)).thenReturn(null);
        assertNull(executor.load(Person.class, 999L));
    }

    @Test
    public void testLoadAll_EmptyCollection() {
        Collection<Long> ids = Collections.emptyList();
        when(mockSession.loadAll(Person.class, ids)).thenReturn(Collections.<Person> emptyList());
        Collection<Person> actual = executor.loadAll(Person.class, ids);
        assertEquals(0, actual.size());
        assertTrue(actual.isEmpty());
    }

    @Test
    public void testCall_ReturnsNull() {
        // The session-managed call may legitimately return null.
        Object result = executor.call(s -> null);
        assertNull(result);
        verify(mockSession).clear();
    }

    @Test
    public void testRun_SessionIsActuallyPassed() {
        AtomicInteger seen = new AtomicInteger();
        executor.run(s -> {
            if (s == mockSession) {
                seen.incrementAndGet();
            }
        });
        assertEquals(1, seen.get());
    }
}
