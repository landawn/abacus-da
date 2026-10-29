package com.landawn.abacus.da.neo4j;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.Invocation;
import org.neo4j.ogm.annotation.EndNode;
import org.neo4j.ogm.annotation.GeneratedValue;
import org.neo4j.ogm.annotation.Id;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Property;
import org.neo4j.ogm.annotation.Relationship;
import org.neo4j.ogm.annotation.RelationshipEntity;
import org.neo4j.ogm.annotation.StartNode;
import org.neo4j.ogm.cypher.BooleanOperator;
import org.neo4j.ogm.cypher.ComparisonOperator;
import org.neo4j.ogm.cypher.Filter;
import org.neo4j.ogm.cypher.Filters;
import org.neo4j.ogm.cypher.query.Pagination;
import org.neo4j.ogm.cypher.query.SortOrder;
import org.neo4j.ogm.driver.Driver;
import org.neo4j.ogm.exception.core.MappingException;
import org.neo4j.ogm.model.GraphModel;
import org.neo4j.ogm.model.RowModel;
import org.neo4j.ogm.request.GraphModelRequest;
import org.neo4j.ogm.request.Request;
import org.neo4j.ogm.request.RestModelRequest;
import org.neo4j.ogm.request.RowModelRequest;
import org.neo4j.ogm.response.Response;
import org.neo4j.ogm.response.model.DefaultGraphModel;
import org.neo4j.ogm.response.model.DefaultRowModel;
import org.neo4j.ogm.response.model.NodeModel;
import org.neo4j.ogm.session.Session;
import org.neo4j.ogm.session.SessionFactory;
import org.neo4j.ogm.session.event.Event;
import org.neo4j.ogm.session.event.EventListener;
import org.neo4j.ogm.session.event.EventListenerAdapter;
import org.neo4j.ogm.transaction.AbstractTransaction;

/**
 * Pins the Neo4j-OGM behaviors that the {@link Neo4jExecutor} Javadoc documents, against a real OGM
 * {@link SessionFactory}/{@code Neo4jSession} whose {@link Driver} is mocked (no database): negative load depth for a
 * relationship entity, {@code getGraphId} for a primary-index-only entity, {@code save} of unmapped objects and
 * elements, the per-entity transactions of a collection {@code delete}, how {@code count} and the {@code Filter}/
 * {@code Filters} {@code loadAll} overloads send copies of the caller's {@link Filter} objects (distinct parameter names,
 * nothing resolved or re-indexed in place) and when they pass them as they are, which results {@code findOnly} and
 * {@code stream} count, {@code loadAll} by IDs of another Java type, the element checks of {@code delete}, and the
 * factory configuration a pooled session keeps.
 */
public class Neo4jExecutorDeepReview2026OgmContractTest {

    // ---- 2026-10-24 sliceW ----

    @NodeEntity
    public static class NativeIdNode {
        @Id
        @GeneratedValue
        Long id;
        Integer age;

        public NativeIdNode() {
        }

        NativeIdNode(final Long id) {
            this.id = id;
        }
    }

    @NodeEntity
    public static class PrimaryIndexNode {
        @Id
        String uuid;

        public PrimaryIndexNode() {
        }

        PrimaryIndexNode(final String uuid) {
            this.uuid = uuid;
        }
    }

    @RelationshipEntity(type = "LINKS")
    public static class Link {
        @Id
        @GeneratedValue
        Long id;
        @StartNode
        NativeIdNode from;
        @EndNode
        NativeIdNode to;
    }

    private static final class StopException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        StopException(final String message) {
            super(message);
        }
    }

    private final AtomicInteger commits = new AtomicInteger();
    private final AtomicInteger rollbacks = new AtomicInteger();
    private Driver driver;
    private Request request;
    private Neo4jExecutor executor;

    @BeforeEach
    public void setUp() {
        driver = mock(Driver.class);
        when(driver.getTypeSystem()).thenCallRealMethod();
        when(driver.requiresTransaction()).thenReturn(true);
        when(driver.getTransactionFactorySupplier()).thenReturn(manager -> (type, bookmarks) -> new AbstractTransaction(manager) {
            @Override
            protected void rollback0() {
                rollbacks.incrementAndGet();
            }

            @Override
            protected void commit0() {
                commits.incrementAndGet();
            }
        });
        when(driver.getExceptionTranslator()).thenReturn(e -> e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e));
        request = mock(Request.class);
        when(driver.request(any())).thenReturn(request);

        final String prefix = Neo4jExecutorDeepReview2026OgmContractTest.class.getName() + "$";
        executor = new Neo4jExecutor(new SessionFactory(driver, prefix + "NativeIdNode", prefix + "PrimaryIndexNode", prefix + "Link"));
    }

    @Test
    public void testNegativeDepthIsRejectedForRelationshipEntityBeforeAnyQuery() {
        assertAll(() -> assertThrows(IllegalArgumentException.class, () -> executor.load(Link.class, 1L, -1)),
                () -> assertThrows(IllegalArgumentException.class, () -> executor.loadAll(Link.class, -1)),
                () -> assertThrows(IllegalArgumentException.class, () -> executor.loadAll(Link.class, List.of(1L), -1)), () -> verifyNoInteractions(request));
    }

    @Test
    public void testGetGraphIdOfEntityWithoutNativeIdFieldIsAlwaysNull() {
        final NativeIdNode saved = new NativeIdNode(7L);
        final NativeIdNode transientNode = new NativeIdNode();

        assertAll(() -> assertNull(executor.getGraphId(new PrimaryIndexNode("u-1"))), () -> assertEquals(Long.valueOf(7L), executor.getGraphId(saved)),
                () -> assertNull(executor.getGraphId(transientNode)),
                // documented side effect: OGM writes a temporary negative placeholder into the native-ID field
                () -> assertTrue(transientNode.id != null && transientNode.id < 0, "placeholder id: " + transientNode.id));
    }

    @Test
    public void testSaveRejectsUnmappedSingleObjectSkipsUnmappedElementAndFailsOnNullElement() {
        assertAll(() -> assertThrows(IllegalArgumentException.class, () -> executor.save(new Object())),
                () -> assertThrows(IllegalArgumentException.class, () -> executor.save(new Object(), 0)), () -> {
                    executor.save(List.of(new Object()));
                    // nothing was sent to the database for the skipped element
                    verify(request, never()).execute(any(RowModelRequest.class));
                }, () -> assertThrows(NullPointerException.class, () -> executor.save(Arrays.asList(new NativeIdNode(), null))));
    }

    @Test
    public void testDeleteOfIterableUsesOneTransactionPerEntity() {
        final List<String> statements = new ArrayList<>();
        final StopException secondDeleteFailure = new StopException("second DELETE failed");
        when(request.execute(any(RowModelRequest.class))).thenAnswer(invocation -> {
            statements.add(invocation.<RowModelRequest> getArgument(0).getStatement());

            if (statements.size() == 2) {
                throw secondDeleteFailure;
            }

            return null;
        });

        final StopException thrown = assertThrows(StopException.class,
                () -> executor.delete(List.of(new NativeIdNode(10L), new NativeIdNode(11L), new NativeIdNode(12L))));

        assertAll(() -> assertSame(secondDeleteFailure, thrown),
                // the first entity's DELETE was committed in a transaction of its own, the second one's was rolled back,
                // and the third was never attempted
                () -> assertEquals(2, statements.size()), () -> assertEquals(1, commits.get()), () -> assertEquals(1, rollbacks.get()));
    }

    @Test
    public void testCountOfUnmappedClass() {
        final RuntimeException thrown = assertThrows(RuntimeException.class, () -> executor.count(Object.class, Collections.emptyList()));

        assertAll(() -> assertEquals(RuntimeException.class, thrown.getClass()), () -> assertTrue(thrown.getMessage().contains("is not a persistable class")),
                () -> assertEquals(0L, executor.count(Object.class)));
    }

    @Test
    public void testCountFiltersInPlainListGetDistinctParameterNamesLikeFiltersObject() {
        final List<Map<String, Object>> parameters = new ArrayList<>();
        when(request.execute(any(RestModelRequest.class))).thenAnswer(invocation -> {
            parameters.add(new LinkedHashMap<>(invocation.<RestModelRequest> getArgument(0).getParameters()));

            throw new StopException("captured");
        });

        final Filter plainMin = new Filter("age", ComparisonOperator.GREATER_THAN, 18);
        final Filter plainMax = new Filter("age", ComparisonOperator.LESS_THAN, 65);
        plainMax.setBooleanOperator(BooleanOperator.AND);
        assertThrows(StopException.class, () -> executor.count(NativeIdNode.class, Arrays.asList(plainMin, plainMax)));

        final Filters filters = new Filters().add(new Filter("age", ComparisonOperator.GREATER_THAN, 18))
                .and(new Filter("age", ComparisonOperator.LESS_THAN, 65));
        assertThrows(StopException.class, () -> executor.count(NativeIdNode.class, filters));

        assertAll(() -> assertEquals(2, parameters.size()),
                // plain list (2026-10-24 apply-neo4j): count sends copies indexed by position, so the lower bound 18 is no
                // longer lost to a shared age_0 parameter
                () -> assertEquals(Map.of("age_0", 18, "age_1", 65), parameters.get(0)),
                () -> assertEquals(Map.of("age_0", 18, "age_1", 65), parameters.get(1)));
    }

    // ---- 2026-10-24 apply-neo4j ----

    @NodeEntity
    public static class Owner {
        @Id
        @GeneratedValue
        Long id;
        Integer age;
        String name;
        @Relationship(type = "OWNS")
        List<Pet> pets;
    }

    @NodeEntity
    public static class Pet {
        @Id
        @GeneratedValue
        Long id;
        Integer age;
        String name;
        @Relationship(type = "TREATED_BY")
        Vet vet;
    }

    @NodeEntity
    public static class Vet {
        @Id
        @GeneratedValue
        Long id;
        Integer age;
    }

    @RelationshipEntity(type = "ADOPTED")
    public static class Adoption {
        @Id
        @GeneratedValue
        Long id;
        @StartNode
        Owner owner;
        @EndNode
        Pet pet;
        Integer years;
    }

    /** A {@code Filter} subclass, which {@code count} cannot copy faithfully. */
    private static final class CustomFilter extends Filter {
        CustomFilter(final String propertyName, final ComparisonOperator operator, final Object value) {
            super(propertyName, operator, value);
        }
    }

    private final List<String> countStatements = new ArrayList<>();
    private final List<Map<String, Object>> countParameters = new ArrayList<>();

    /** Captures the statement and parameters of every count query and aborts the query before a result is read. */
    private void captureCountQueries() {
        when(request.execute(any(RestModelRequest.class))).thenAnswer(invocation -> {
            final RestModelRequest countRequest = invocation.getArgument(0);
            countStatements.add(countRequest.getStatement());
            countParameters.add(new LinkedHashMap<>(countRequest.getParameters()));

            throw new StopException("captured");
        });
    }

    private static void countCaptured(final Neo4jExecutor target, final Class<?> targetClass, final Iterable<Filter> filters) {
        assertThrows(StopException.class, () -> target.count(targetClass, filters));
    }

    private Neo4jExecutor petExecutor() {
        final String prefix = Neo4jExecutorDeepReview2026OgmContractTest.class.getName() + "$";

        return new Neo4jExecutor(new SessionFactory(driver, prefix + "Owner", prefix + "Pet", prefix + "Vet", prefix + "Adoption"));
    }

    private static Filter joined(final BooleanOperator booleanOperator, final Filter filter) {
        filter.setBooleanOperator(booleanOperator);

        return filter;
    }

    private static Filter nested(final String nestedPropertyName, final Class<?> nestedPropertyType, final Filter filter) {
        filter.setNestedPropertyName(nestedPropertyName);
        filter.setNestedPropertyType(nestedPropertyType);

        return filter;
    }

    private static Filter deepNested(final Filter filter) {
        filter.setNestedPath(new Filter.NestedPathSegment("pets", Pet.class), new Filter.NestedPathSegment("vet", Vet.class));

        return filter;
    }

    @Test
    public void testCountPlainListLeavesCallerFiltersUnmodified() {
        captureCountQueries();

        final Filter min = new Filter("age", ComparisonOperator.GREATER_THAN, 18);
        final Filter max = joined(BooleanOperator.AND, new Filter("age", ComparisonOperator.LESS_THAN, 65));

        countCaptured(executor, NativeIdNode.class, Arrays.asList(min, max));

        assertAll(() -> assertEquals(Map.of("age_0", 18, "age_1", 65), countParameters.get(0)),
                // copies were sent: the caller's filters keep their index 0 and are not resolved against the counted class
                () -> assertEquals(Map.of("age_0", 18), min.parameters()), () -> assertEquals(Map.of("age_0", 65), max.parameters()),
                () -> assertNull(min.getOwnerEntityType()), () -> assertNull(max.getOwnerEntityType()),
                () -> assertEquals(BooleanOperator.NONE, min.getBooleanOperator()), () -> assertEquals(BooleanOperator.AND, max.getBooleanOperator()));
    }

    @Test
    public void testCountPlainListKeepsIndexesGivenByAnotherFiltersObject() {
        captureCountQueries();

        final Filter excluded = new Filter("age", ComparisonOperator.EQUALS, 40);
        excluded.setNegated(true);
        final Filter min = new Filter("age", ComparisonOperator.GREATER_THAN, 18);
        final Filter max = new Filter("age", ComparisonOperator.LESS_THAN, 65);
        final Filters owning = new Filters().add(excluded).and(min).and(max); // indexes 0, 1, 2

        countCaptured(executor, NativeIdNode.class, Arrays.asList(min, max));
        countCaptured(executor, NativeIdNode.class, owning);

        assertAll(
                // the copies are indexed by their position in the list ...
                () -> assertEquals(Map.of("age_0", 18, "age_1", 65), countParameters.get(0)),
                // ... while the caller's filters keep the indexes their Filters object gave them (re-indexing them to 0 and 1
                // would make that Filters object send age_0 twice)
                () -> assertEquals(Map.of("age_1", 18), min.parameters()), () -> assertEquals(Map.of("age_2", 65), max.parameters()),
                () -> assertEquals(Map.of("age_0", 40, "age_1", 18, "age_2", 65), countParameters.get(1)));
    }

    @Test
    public void testCountSendsCopiesOfFiltersObjectAndLeavesItUnmodified() {
        captureCountQueries();

        final Filter min = new Filter("age", ComparisonOperator.GREATER_THAN, 18);
        final Filter max = new Filter("age", ComparisonOperator.LESS_THAN, 65);

        countCaptured(executor, NativeIdNode.class, new Filters().add(min).and(max));

        assertAll(() -> assertEquals(Map.of("age_0", 18, "age_1", 65), countParameters.get(0)),
                // 2026-10-27 sliceW: a Filters object is copied too, so OGM no longer resolves the caller's own filters
                // (until then it was passed as it is and min/max got NativeIdNode as owner type)
                () -> assertNull(min.getOwnerEntityType()), () -> assertNull(max.getOwnerEntityType()));
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testCountHandsSessionCopiesAndUncopyableIterablesAsTheyAre() {
        final SessionFactory mockFactory = mock(SessionFactory.class);
        final Session session = mock(Session.class);
        when(mockFactory.openSession()).thenReturn(session);
        final Neo4jExecutor mockExecutor = new Neo4jExecutor(mockFactory);

        final Filters filters = new Filters().add(new Filter("age", ComparisonOperator.GREATER_THAN, 18));
        final List<Filter> empty = Collections.emptyList();
        final List<Filter> withNull = Arrays.asList(new Filter("age", ComparisonOperator.GREATER_THAN, 18), null);
        final List<Filter> withSubclass = Arrays.asList(new Filter("age", ComparisonOperator.GREATER_THAN, 18),
                joined(BooleanOperator.AND, new CustomFilter("age", ComparisonOperator.LESS_THAN, 65)));
        final Filter withSegmentSubclass = new Filter("age", ComparisonOperator.GREATER_THAN, 1);
        withSegmentSubclass.setNestedPath(new Filter.NestedPathSegment("pets", Pet.class) {
        });
        final List<Filter> withCustomSegment = List.of(withSegmentSubclass);
        final Filter min = new Filter("age", ComparisonOperator.GREATER_THAN, 18);
        final Filter max = joined(BooleanOperator.AND, new Filter("age", ComparisonOperator.LESS_THAN, 65));
        final List<Filter> plain = Arrays.asList(min, max);

        for (final Iterable<Filter> argument : Arrays.<Iterable<Filter>> asList(filters, empty, withNull, withSubclass, withCustomSegment, plain)) {
            mockExecutor.count(NativeIdNode.class, argument);
        }

        final ArgumentCaptor<Iterable> captor = ArgumentCaptor.forClass(Iterable.class);
        verify(session, times(6)).count(eq(NativeIdNode.class), captor.capture());
        final List<Iterable> passed = captor.getAllValues();

        assertAll(() -> {
            // 2026-10-27 sliceW: a Filters object is copied like any other iterable (it used to be passed as it is)
            assertInstanceOf(Filters.class, passed.get(0));
            assertNotSame(filters, passed.get(0));
            final Filter original = filters.iterator().next();
            final Filter copy = ((Filters) passed.get(0)).iterator().next();
            assertNotSame(original, copy);
            assertEquals(original.parameters(), copy.parameters());
        }, () -> assertSame(empty, passed.get(1)),
                // a filter that cannot be copied faithfully: the iterable is passed on as it is (today's behavior)
                () -> assertSame(withNull, passed.get(2)), () -> assertSame(withSubclass, passed.get(3)), () -> assertSame(withCustomSegment, passed.get(4)),
                () -> {
                    final Iterable<Filter> copies = passed.get(5);
                    assertInstanceOf(Filters.class, copies);

                    final List<Filter> copied = new ArrayList<>();
                    copies.forEach(copied::add);

                    assertEquals(2, copied.size());
                    assertNotSame(min, copied.get(0));
                    assertNotSame(max, copied.get(1));
                    assertEquals(Map.of("age_0", 18), copied.get(0).parameters());
                    assertEquals(Map.of("age_1", 65), copied.get(1).parameters());
                    assertEquals(BooleanOperator.AND, copied.get(1).getBooleanOperator());
                });
    }

    @Test
    public void testCountPlainListRendersNestedAndBooleanOperatorFiltersLikeFiltersObject() {
        captureCountQueries();

        final Neo4jExecutor petExecutor = petExecutor();
        final Map<String, Supplier<List<Filter>>> nodeCases = new LinkedHashMap<>();
        nodeCases.put("nested property", () -> List.of(nested("pets", Pet.class, new Filter("age", ComparisonOperator.GREATER_THAN, 1)),
                nested("pets", Pet.class, joined(BooleanOperator.AND, new Filter("age", ComparisonOperator.LESS_THAN, 10)))));
        nodeCases.put("OR, negation, ignore case", () -> {
            final Filter notBob = joined(BooleanOperator.AND, new Filter("name", ComparisonOperator.EQUALS, "bob"));
            notBob.setNegated(true);

            return List.of(new Filter("age", ComparisonOperator.LESS_THAN, 18),
                    joined(BooleanOperator.OR, new Filter("age", ComparisonOperator.GREATER_THAN, 65)), notBob,
                    joined(BooleanOperator.AND, new Filter("name", ComparisonOperator.EQUALS, "REX").ignoreCase()));
        });
        nodeCases.put("deep nested path", () -> List.of(deepNested(new Filter("age", ComparisonOperator.GREATER_THAN, 30)),
                deepNested(joined(BooleanOperator.AND, new Filter("age", ComparisonOperator.LESS_THAN, 60)))));
        nodeCases.put("principal and nested", () -> List.of(new Filter("name", ComparisonOperator.EQUALS, "ann"),
                nested("pets", Pet.class, joined(BooleanOperator.AND, new Filter("name", ComparisonOperator.EQUALS, "rex")))));

        final List<Executable> checks = new ArrayList<>();
        nodeCases.forEach((name, filters) -> checks.add(() -> assertRendersLikeFiltersObject(petExecutor, Owner.class, name, filters)));
        checks.add(() -> assertRendersLikeFiltersObject(petExecutor, Adoption.class, "relationship entity",
                () -> List.of(new Filter("years", ComparisonOperator.GREATER_THAN, 1),
                        joined(BooleanOperator.AND, new Filter("years", ComparisonOperator.LESS_THAN, 5)))));

        assertAll(checks);
    }

    private void assertRendersLikeFiltersObject(final Neo4jExecutor target, final Class<?> targetClass, final String name,
            final Supplier<List<Filter>> filters) {
        final int first = countStatements.size();
        final List<Filter> plain = filters.get();

        countCaptured(target, targetClass, plain);
        countCaptured(target, targetClass, new Filters(filters.get()));

        assertEquals(countStatements.get(first + 1), countStatements.get(first), name);
        assertEquals(countParameters.get(first + 1), countParameters.get(first), name);
        // the reference Filters object really gives every filter its own parameter
        assertEquals(plain.size(), countParameters.get(first + 1).size(), name + ": " + countParameters.get(first + 1));
    }

    @Test
    public void testCountPlainListLeavesCallerNestedFiltersUnresolvedAndReusable() {
        captureCountQueries();

        final Neo4jExecutor petExecutor = petExecutor();
        final Filter.NestedPathSegment petsSegment = new Filter.NestedPathSegment("pets", Pet.class);
        final Filter.NestedPathSegment vetSegment = new Filter.NestedPathSegment("vet", Vet.class);
        final Filter deep = new Filter("age", ComparisonOperator.GREATER_THAN, 30);
        deep.setNestedPath(petsSegment, vetSegment);
        final Filter nestedName = nested("pets", Pet.class, joined(BooleanOperator.AND, new Filter("name", ComparisonOperator.EQUALS, "rex")));

        countCaptured(petExecutor, Owner.class, Arrays.asList(deep, nestedName));

        // OGM shortens the nested path of a filter it counts relationship entities with; the caller's filter must survive it
        final Filter.NestedPathSegment petSegment = new Filter.NestedPathSegment("pet", Pet.class);
        final Filter adoptedPet = new Filter("age", ComparisonOperator.GREATER_THAN, 2);
        adoptedPet.setNestedPath(petSegment);
        final List<Filter> adoptionFilters = List.of(adoptedPet);

        countCaptured(petExecutor, Adoption.class, adoptionFilters);
        countCaptured(petExecutor, Adoption.class, adoptionFilters);

        assertAll(() -> assertEquals(3, countStatements.size()), () -> assertNull(petsSegment.getRelationshipType()),
                () -> assertNull(vetSegment.getNestedEntityTypeLabel()), () -> assertEquals(List.of(petsSegment, vetSegment), deep.getNestedPath()),
                () -> assertNull(deep.getOwnerEntityType()), () -> assertNull(nestedName.getRelationshipType()),
                () -> assertNull(nestedName.getNestedEntityTypeLabel()), () -> assertEquals(List.of(petSegment), adoptedPet.getNestedPath()),
                () -> assertNull(adoptedPet.getOwnerEntityType()),
                // the same plain list counted twice renders the same query
                () -> assertEquals(countStatements.get(1), countStatements.get(2)), () -> assertEquals(countParameters.get(1), countParameters.get(2)));
    }

    // ---- 2026-10-27 sliceW ----

    /** The label Neo4j-OGM derives for {@link NativeIdNode}. */
    private static final String NATIVE_ID_NODE_LABEL = Neo4jExecutorDeepReview2026OgmContractTest.class.getSimpleName() + "$NativeIdNode";

    private static final Map<String, Object> NO_PARAMETERS = Collections.emptyMap();

    private static NodeModel nativeIdNodeModel(final long id, final int age) {
        final NodeModel node = new NodeModel(id);
        node.setLabels(new String[] { NATIVE_ID_NODE_LABEL });
        node.setProperties(Map.of("age", age));

        return node;
    }

    /** One result row as the driver hands it to Neo4j-OGM for an entity query: the nodes the row returns. */
    private static GraphModel graphRow(final NodeModel... nodes) {
        final DefaultGraphModel row = new DefaultGraphModel();

        for (final NodeModel node : nodes) {
            row.addNode(node);
        }

        return row;
    }

    /** One single-column result row as the driver hands it to Neo4j-OGM for a scalar query. */
    private static RowModel scalarRow(final Object value) {
        return new DefaultRowModel(new Object[] { value }, new String[] { "x" });
    }

    @SafeVarargs
    private static <T> Response<T> responseOf(final T... rows) {
        final Deque<T> remaining = new ArrayDeque<>(Arrays.asList(rows));

        return new Response<>() {
            @Override
            public T next() {
                return remaining.poll();
            }

            @Override
            public void close() {
            }

            @Override
            public String[] columns() {
                return new String[] { "x" };
            }
        };
    }

    private static List<Long> idsOf(final Iterable<NativeIdNode> nodes) {
        final List<Long> ids = new ArrayList<>();
        nodes.forEach(node -> ids.add(node.id));

        return ids;
    }

    @Test
    public void testEntityQueryResultsAreTheDistinctEntitiesOfTheResultNotItsRows() {
        final String cypher = "MATCH (p:NativeIdNode) RETURN p";

        // one node returned by two rows (e.g. one row per relationship of p)
        when(request.execute(any(GraphModelRequest.class)))
                .thenAnswer(invocation -> responseOf(graphRow(nativeIdNodeModel(1, 30)), graphRow(nativeIdNodeModel(1, 30))));
        final List<Long> repeatedNode = idsOf(executor.stream(NativeIdNode.class, cypher, NO_PARAMETERS).toList());
        final NativeIdNode onlyOfRepeatedNode = executor.findOnly(NativeIdNode.class, cypher, NO_PARAMETERS).get();

        // one row returning two such nodes (RETURN p, friend)
        when(request.execute(any(GraphModelRequest.class))).thenAnswer(invocation -> responseOf(graphRow(nativeIdNodeModel(1, 30), nativeIdNodeModel(2, 40))));
        final List<Long> oneRowTwoNodes = idsOf(executor.stream(NativeIdNode.class, cypher, NO_PARAMETERS).toList());
        final RuntimeException twoResults = assertThrows(RuntimeException.class, () -> executor.findOnly(NativeIdNode.class, cypher, NO_PARAMETERS));

        // rows b, a, b: each entity once, in the order of first appearance
        when(request.execute(any(GraphModelRequest.class))).thenAnswer(
                invocation -> responseOf(graphRow(nativeIdNodeModel(2, 40)), graphRow(nativeIdNodeModel(1, 30)), graphRow(nativeIdNodeModel(2, 40))));
        final List<Long> firstAppearanceOrder = idsOf(executor.stream(NativeIdNode.class, cypher, NO_PARAMETERS).toList());

        assertAll(() -> assertEquals(List.of(1L), repeatedNode), () -> assertEquals(Long.valueOf(1L), onlyOfRepeatedNode.id),
                () -> assertEquals(List.of(1L, 2L), oneRowTwoNodes), () -> assertTrue(twoResults.getMessage().contains("found 2"), twoResults.getMessage()),
                () -> assertEquals(List.of(2L, 1L), firstAppearanceOrder));
    }

    @Test
    public void testScalarQueryMapsRowByRowAndFindOnlyRejectsPrimitiveTargetClass() {
        when(request.execute(any(RowModelRequest.class))).thenAnswer(invocation -> responseOf(scalarRow(5L), scalarRow(5L)));
        final List<Long> twoEqualRows = executor.stream(Long.class, "UNWIND [5, 5] AS x RETURN x", NO_PARAMETERS).toList();
        final RuntimeException twoResults = assertThrows(RuntimeException.class,
                () -> executor.findOnly(Long.class, "UNWIND [5, 5] AS x RETURN x", NO_PARAMETERS));

        when(request.execute(any(RowModelRequest.class))).thenAnswer(invocation -> responseOf(scalarRow(5L)));
        final Long wrapper = executor.findOnly(Long.class, "RETURN 5", NO_PARAMETERS).get();
        final List<Long> primitiveStream = executor.stream(long.class, "RETURN 5", NO_PARAMETERS).toList();
        final MappingException primitive = assertThrows(MappingException.class, () -> executor.findOnly(long.class, "RETURN 5", NO_PARAMETERS));

        when(request.execute(any(RowModelRequest.class))).thenAnswer(invocation -> responseOf(scalarRow(null)));
        final boolean wrapperNullIsEmpty = executor.findOnly(Long.class, "RETURN null", NO_PARAMETERS).isEmpty();
        // a null value is read as the primitive default 0L, which fails the same way
        assertThrows(MappingException.class, () -> executor.findOnly(long.class, "RETURN null", NO_PARAMETERS));
        final List<Long> wrapperNullStream = executor.stream(Long.class, "RETURN null", NO_PARAMETERS).toList();
        final List<Long> primitiveNullStream = executor.stream(long.class, "RETURN null", NO_PARAMETERS).toList();
        // a non-numeric primitive class keeps a null value
        final boolean booleanNullIsEmpty = executor.findOnly(boolean.class, "RETURN null", NO_PARAMETERS).isEmpty();

        when(request.execute(any(RowModelRequest.class))).thenAnswer(invocation -> responseOf(scalarRow(true)));
        assertThrows(MappingException.class, () -> executor.findOnly(boolean.class, "RETURN true", NO_PARAMETERS));

        when(request.execute(any(RowModelRequest.class))).thenAnswer(invocation -> responseOf());
        final boolean primitiveNoRowIsEmpty = executor.findOnly(long.class, "MATCH (n:None) RETURN count(n)", NO_PARAMETERS).isEmpty();

        assertAll(() -> assertEquals(List.of(5L, 5L), twoEqualRows), () -> assertTrue(twoResults.getMessage().contains("found 2"), twoResults.getMessage()),
                () -> assertEquals(Long.valueOf(5L), wrapper), () -> assertEquals(List.of(5L), primitiveStream),
                () -> assertTrue(primitive.getMessage().contains("to long"), primitive.getMessage()), () -> assertTrue(wrapperNullIsEmpty),
                () -> assertEquals(Collections.singletonList(null), wrapperNullStream), () -> assertEquals(List.of(0L), primitiveNullStream),
                () -> assertTrue(booleanNullIsEmpty), () -> assertTrue(primitiveNoRowIsEmpty));
    }

    @Test
    public void testLoadAllByIdsSilentlyMissesIdsOfAnotherJavaType() {
        when(request.execute(any(GraphModelRequest.class)))
                .thenAnswer(invocation -> responseOf(graphRow(nativeIdNodeModel(1, 30)), graphRow(nativeIdNodeModel(2, 40))));

        final Collection<NativeIdNode> byIntegers = executor.loadAll(NativeIdNode.class, Arrays.asList(1, 2));
        final Collection<NativeIdNode> byIntegersSorted = executor.loadAll(NativeIdNode.class, Arrays.asList(1, 2), new SortOrder().add("age"));
        final Collection<NativeIdNode> byLongs = executor.loadAll(NativeIdNode.class, Arrays.asList(2L, 1L));

        assertAll(() -> assertTrue(byIntegers.isEmpty(), byIntegers::toString), () -> assertTrue(byIntegersSorted.isEmpty(), byIntegersSorted::toString),
                // the same rows are found by Long ids, in the order of the ids supplied
                () -> assertEquals(List.of(2L, 1L), idsOf(byLongs)),
                () -> assertThrows(IllegalArgumentException.class, () -> executor.load(NativeIdNode.class, 1)));
    }

    @Test
    public void testDeleteRejectsNullOrUnmappedElementBeforeDeletingAnything() {
        final NativeIdNode saved = new NativeIdNode(5L);

        assertAll(() -> assertThrows(NullPointerException.class, () -> executor.delete(Arrays.asList(saved, null))),
                () -> assertThrows(NullPointerException.class, () -> executor.delete(new Object[] { saved, null })),
                () -> assertThrows(IllegalArgumentException.class, () -> executor.delete(Arrays.asList(saved, new Object()))),
                // nothing was sent: the element checks run before the first DELETE
                () -> verifyNoInteractions(request));
    }

    @Test
    public void testPooledSessionKeepsTheFactoryConfigurationItWasOpenedWith() {
        final SessionFactory factory = new SessionFactory(driver, Neo4jExecutorDeepReview2026OgmContractTest.class.getName() + "$NativeIdNode");
        final AtomicInteger preDeletes = new AtomicInteger();
        final EventListener listener = new EventListenerAdapter() {
            @Override
            public void onPreDelete(final Event event) {
                preDeletes.incrementAndGet();
            }
        };

        final Neo4jExecutor usedBefore = new Neo4jExecutor(factory);
        usedBefore.deleteAll(NativeIdNode.class); // opens the session it pools afterwards
        factory.register(listener);
        usedBefore.deleteAll(NativeIdNode.class); // reuses that session, opened before the registration
        final int afterPooledSessionBeforeRegistration = preDeletes.get();

        final Neo4jExecutor usedAfter = new Neo4jExecutor(factory);
        usedAfter.deleteAll(NativeIdNode.class); // opens a session after the registration
        final int afterNewSession = preDeletes.get();
        factory.deregister(listener);
        usedAfter.deleteAll(NativeIdNode.class); // its pooled session still calls the listener
        final int afterDeregistration = preDeletes.get();

        assertAll(() -> assertEquals(0, afterPooledSessionBeforeRegistration), () -> assertEquals(1, afterNewSession),
                () -> assertEquals(2, afterDeregistration));
    }

    @NodeEntity
    public static class RenamedNode {
        @Id
        @GeneratedValue
        Long id;
        @Property(name = "full_name")
        String name;
    }

    @NodeEntity
    public static class PlainNameNode {
        @Id
        @GeneratedValue
        Long id;
        String name;
    }

    private final List<String> loadStatements = new ArrayList<>();
    private final List<Map<String, Object>> loadParameters = new ArrayList<>();

    /** An executor over RenamedNode, PlainNameNode and NativeIdNode whose depth-0 loads capture their statement and parameters. */
    private Neo4jExecutor capturingLoadExecutor() {
        when(request.execute(any(GraphModelRequest.class))).thenAnswer(invocation -> {
            final GraphModelRequest graphRequest = invocation.getArgument(0);
            loadStatements.add(graphRequest.getStatement());
            loadParameters.add(new LinkedHashMap<>(graphRequest.getParameters()));

            return responseOf();
        });

        final String prefix = Neo4jExecutorDeepReview2026OgmContractTest.class.getName() + "$";

        return new Neo4jExecutor(new SessionFactory(driver, prefix + "RenamedNode", prefix + "PlainNameNode", prefix + "NativeIdNode"));
    }

    @Test
    public void testLoadAllLeavesTheCallersFilterAndFiltersUnmodified() {
        final Neo4jExecutor target = capturingLoadExecutor();

        final Filter byName = new Filter("name", ComparisonOperator.EQUALS, "ann");
        target.loadAll(RenamedNode.class, byName, 0);

        final Filter one = new Filter("age", ComparisonOperator.EQUALS, 1);
        final Filter two = new Filter("age", ComparisonOperator.EQUALS, 2);
        new Filters().add(one).or(two); // gives two the index 1
        target.loadAll(NativeIdNode.class, two, 0);

        final Filter inFilters = new Filter("name", ComparisonOperator.EQUALS, "bob");
        target.loadAll(RenamedNode.class, new Filters().add(inFilters), 0);

        assertAll(() -> assertEquals(3, loadStatements.size()),
                // the copies were resolved against RenamedNode ...
                () -> assertTrue(loadStatements.get(0).contains("n.`full_name`"), loadStatements.get(0)),
                () -> assertTrue(loadStatements.get(2).contains("n.`full_name`"), loadStatements.get(2)),
                () -> assertEquals(Map.of("age_0", 2), loadParameters.get(1)),
                // ... while the caller's filters keep their own state: not resolved, not re-indexed
                () -> assertEquals("name", byName.getPropertyName()), () -> assertNull(byName.getOwnerEntityType()),
                () -> assertEquals(Map.of("age_1", 2), two.parameters()), () -> assertNull(two.getOwnerEntityType()),
                () -> assertEquals("name", inFilters.getPropertyName()), () -> assertNull(inFilters.getOwnerEntityType()));
    }

    @Test
    public void testFiltersObjectReusedAfterSingleFilterLoadAllStillSendsBothParameters() {
        final Neo4jExecutor target = capturingLoadExecutor();
        captureCountQueries();

        final Filter one = new Filter("age", ComparisonOperator.EQUALS, 1);
        final Filter two = new Filter("age", ComparisonOperator.EQUALS, 2);
        final Filters either = new Filters().add(one).or(two);

        target.loadAll(NativeIdNode.class, two, 0);
        target.loadAll(NativeIdNode.class, either, 0);
        countCaptured(target, NativeIdNode.class, either);

        // HEAD re-indexed two to 0, so either sent age_0 twice and "age = 1 OR age = 2" ran as "age = 2 OR age = 2"
        assertAll(() -> assertEquals(Map.of("age_0", 1, "age_1", 2), loadParameters.get(1)),
                () -> assertEquals(Map.of("age_0", 1, "age_1", 2), countParameters.get(0)));
    }

    @Test
    public void testFilterReusedForASecondClassResolvesAgainstThatClass() {
        final Neo4jExecutor target = capturingLoadExecutor();
        captureCountQueries();

        final Filter byName = new Filter("name", ComparisonOperator.EQUALS, "ann");
        target.loadAll(RenamedNode.class, byName, 0);
        target.loadAll(PlainNameNode.class, byName, 0);

        final Filters byNameFilters = new Filters().add(new Filter("name", ComparisonOperator.EQUALS, "bob"));
        target.loadAll(RenamedNode.class, byNameFilters, 0);
        target.loadAll(PlainNameNode.class, byNameFilters, 0);

        countCaptured(target, RenamedNode.class, byNameFilters);
        countCaptured(target, PlainNameNode.class, byNameFilters);

        assertAll(() -> assertEquals(4, loadStatements.size()), () -> assertTrue(loadStatements.get(0).contains("n.`full_name`"), loadStatements.get(0)),
                // HEAD kept the first class's resolution: PlainNameNode was queried on full_name and matched nothing
                () -> assertTrue(loadStatements.get(1).contains("n.`name`"), loadStatements.get(1)),
                () -> assertTrue(loadStatements.get(2).contains("n.`full_name`"), loadStatements.get(2)),
                () -> assertTrue(loadStatements.get(3).contains("n.`name`"), loadStatements.get(3)),
                () -> assertTrue(countStatements.get(0).contains("n.`full_name`"), countStatements.get(0)),
                () -> assertTrue(countStatements.get(1).contains("n.`name`"), countStatements.get(1)));
    }

    @Test
    public void testEveryFilterLoadAllOverloadHandsOgmCopiesAndOtherArgumentsUnchanged() {
        final SessionFactory mockFactory = mock(SessionFactory.class);
        final Session session = mock(Session.class);
        when(mockFactory.openSession()).thenReturn(session);
        final Neo4jExecutor mockExecutor = new Neo4jExecutor(mockFactory);

        final SortOrder sortOrder = new SortOrder().add("age");
        final Pagination pagination = new Pagination(0, 10);
        final List<List<Object>> tails = List.of(List.of(), List.of(1), List.of(sortOrder), List.of(sortOrder, 1), List.of(pagination), List.of(pagination, 1),
                List.of(sortOrder, pagination), List.of(sortOrder, pagination, 1));

        final Filter filter = new Filter("age", ComparisonOperator.GREATER_THAN, 18);
        final Filter first = new Filter("age", ComparisonOperator.GREATER_THAN, 18);
        final Filters filters = new Filters().add(first).and(new Filter("age", ComparisonOperator.LESS_THAN, 65));

        mockExecutor.loadAll(NativeIdNode.class, filter);
        mockExecutor.loadAll(NativeIdNode.class, filter, 1);
        mockExecutor.loadAll(NativeIdNode.class, filter, sortOrder);
        mockExecutor.loadAll(NativeIdNode.class, filter, sortOrder, 1);
        mockExecutor.loadAll(NativeIdNode.class, filter, pagination);
        mockExecutor.loadAll(NativeIdNode.class, filter, pagination, 1);
        mockExecutor.loadAll(NativeIdNode.class, filter, sortOrder, pagination);
        mockExecutor.loadAll(NativeIdNode.class, filter, sortOrder, pagination, 1);
        mockExecutor.loadAll(NativeIdNode.class, filters);
        mockExecutor.loadAll(NativeIdNode.class, filters, 1);
        mockExecutor.loadAll(NativeIdNode.class, filters, sortOrder);
        mockExecutor.loadAll(NativeIdNode.class, filters, sortOrder, 1);
        mockExecutor.loadAll(NativeIdNode.class, filters, pagination);
        mockExecutor.loadAll(NativeIdNode.class, filters, pagination, 1);
        mockExecutor.loadAll(NativeIdNode.class, filters, sortOrder, pagination);
        mockExecutor.loadAll(NativeIdNode.class, filters, sortOrder, pagination, 1);

        // arguments that cannot be copied faithfully, or need no copy, are passed as they are
        final CustomFilter custom = new CustomFilter("age", ComparisonOperator.GREATER_THAN, 18);
        final Filters withCustom = new Filters().add(custom);
        final Filters empty = new Filters();
        mockExecutor.loadAll(NativeIdNode.class, custom);
        mockExecutor.loadAll(NativeIdNode.class, withCustom);
        mockExecutor.loadAll(NativeIdNode.class, empty);
        mockExecutor.loadAll(NativeIdNode.class, (Filters) null);

        final List<Invocation> loads = new ArrayList<>();
        mockingDetails(session).getInvocations().stream().filter(invocation -> invocation.getMethod().getName().equals("loadAll")).forEach(loads::add);

        final List<Executable> checks = new ArrayList<>();
        checks.add(() -> assertEquals(20, loads.size()));

        for (int i = 0; i < 16; i++) {
            final Invocation load = loads.get(i);
            final boolean single = i < 8;
            final List<Object> tail = tails.get(i % 8);

            checks.add(() -> {
                final Object[] arguments = load.getArguments();
                assertSame(NativeIdNode.class, arguments[0]);
                // the same OGM overload as before, with every other argument passed as it is
                assertSame(single ? Filter.class : Filters.class, load.getMethod().getParameterTypes()[1]);
                assertEquals(tail.size(), arguments.length - 2, load.toString());

                for (int j = 0; j < tail.size(); j++) {
                    assertEquals(tail.get(j), arguments[j + 2], load.toString());
                }

                if (single) {
                    assertNotSame(filter, arguments[1]);
                    assertEquals(Map.of("age_0", 18), ((Filter) arguments[1]).parameters());
                } else {
                    assertNotSame(filters, arguments[1]);
                    final List<Filter> copies = new ArrayList<>();
                    ((Filters) arguments[1]).forEach(copies::add);
                    assertEquals(2, copies.size());
                    assertNotSame(first, copies.get(0));
                    assertEquals(Map.of("age_0", 18), copies.get(0).parameters());
                    assertEquals(Map.of("age_1", 65), copies.get(1).parameters());
                    assertEquals(BooleanOperator.AND, copies.get(1).getBooleanOperator());
                }
            });
        }

        checks.add(() -> assertSame(custom, loads.get(16).getArguments()[1]));
        checks.add(() -> assertSame(withCustom, loads.get(17).getArguments()[1]));
        checks.add(() -> assertSame(empty, loads.get(18).getArguments()[1]));
        checks.add(() -> assertNull(loads.get(19).getArguments()[1]));

        assertAll(checks);
    }

    // 2026-10-27 second-pass review (r2W): the loadAll fallbacks and nested paths

    @NodeEntity
    public static class FriendNode {
        @Id
        @GeneratedValue
        Long id;
        Integer age;
        @Relationship(type = "KNOWS")
        List<FriendNode> friends;
    }

    @Test
    public void testLoadAllPassesAnUncopyableArgumentAsItIsSoOgmResolvesItInPlace() {
        final Neo4jExecutor target = capturingLoadExecutor();
        final Neo4jExecutor pets = petExecutor();

        final CustomFilter custom = new CustomFilter("name", ComparisonOperator.EQUALS, "ann");
        target.loadAll(RenamedNode.class, custom, 0);

        // all or nothing: the plain filter of a Filters object that also holds a Filter subclass is not copied either
        final Filter plain = new Filter("name", ComparisonOperator.EQUALS, "bob");
        target.loadAll(RenamedNode.class, new Filters().add(plain).and(new CustomFilter("name", ComparisonOperator.EQUALS, "cy")), 0);

        final Filter.NestedPathSegment customSegment = new Filter.NestedPathSegment("pets", Pet.class) {
        };
        final Filter withCustomSegment = new Filter("age", ComparisonOperator.GREATER_THAN, 1);
        withCustomSegment.setNestedPath(customSegment);
        pets.loadAll(Owner.class, withCustomSegment, 0);

        assertAll(() -> assertEquals(3, loadStatements.size()),
                // Neo4j-OGM resolved the caller's own objects ("Filter Arguments" in the Neo4jExecutor class documentation)
                () -> assertEquals("full_name", custom.getPropertyName()), () -> assertSame(RenamedNode.class, custom.getOwnerEntityType()),
                () -> assertEquals("full_name", plain.getPropertyName()), () -> assertSame(RenamedNode.class, plain.getOwnerEntityType()),
                () -> assertSame(Owner.class, withCustomSegment.getOwnerEntityType()), () -> assertEquals("OWNS", customSegment.getRelationshipType()));
    }

    @Test
    public void testLoadAllHandsSessionFiltersWithAnUncopyableNestedPathAsTheyAre() {
        final SessionFactory mockFactory = mock(SessionFactory.class);
        final Session session = mock(Session.class);
        when(mockFactory.openSession()).thenReturn(session);
        final Neo4jExecutor mockExecutor = new Neo4jExecutor(mockFactory);

        final Filter withCustomSegment = new Filter("age", ComparisonOperator.GREATER_THAN, 1);
        withCustomSegment.setNestedPath(new Filter.NestedPathSegment("pets", Pet.class) {
        });
        final Filter withNullSegment = new Filter("age", ComparisonOperator.GREATER_THAN, 1);
        withNullSegment.setNestedPath((Filter.NestedPathSegment) null);
        final Filters filtersWithCustomSegment = new Filters().add(new Filter("age", ComparisonOperator.LESS_THAN, 9)).and(withCustomSegment);
        final Filters filtersWithNullSegment = new Filters().add(withNullSegment);

        mockExecutor.loadAll(Owner.class, withCustomSegment);
        mockExecutor.loadAll(Owner.class, withNullSegment);
        mockExecutor.loadAll(Owner.class, filtersWithCustomSegment);
        mockExecutor.loadAll(Owner.class, filtersWithNullSegment);

        final ArgumentCaptor<Filter> filterCaptor = ArgumentCaptor.forClass(Filter.class);
        verify(session, times(2)).loadAll(eq(Owner.class), filterCaptor.capture());
        final ArgumentCaptor<Filters> filtersCaptor = ArgumentCaptor.forClass(Filters.class);
        verify(session, times(2)).loadAll(eq(Owner.class), filtersCaptor.capture());

        assertAll(() -> assertSame(withCustomSegment, filterCaptor.getAllValues().get(0)),
                () -> assertSame(withNullSegment, filterCaptor.getAllValues().get(1)),
                () -> assertSame(filtersWithCustomSegment, filtersCaptor.getAllValues().get(0)),
                () -> assertSame(filtersWithNullSegment, filtersCaptor.getAllValues().get(1)));
    }

    @Test
    public void testLoadAllCopiesNestedPathsSegmentBySegment() {
        capturingLoadExecutor(); // stubs the load requests of every executor over the mocked driver
        final Neo4jExecutor pets = petExecutor();
        final Neo4jExecutor friends = new Neo4jExecutor(new SessionFactory(driver, Neo4jExecutorDeepReview2026OgmContractTest.class.getName() + "$FriendNode"));

        final Filter.NestedPathSegment petsSegment = new Filter.NestedPathSegment("pets", Pet.class);
        final Filter.NestedPathSegment vetSegment = new Filter.NestedPathSegment("vet", Vet.class);
        final Filter deep = new Filter("age", ComparisonOperator.GREATER_THAN, 30);
        deep.setNestedPath(petsSegment, vetSegment);
        pets.loadAll(Owner.class, new Filters().add(deep), 0);

        // one segment instance at two positions of a path (friends of friends)
        final Filter.NestedPathSegment friendsSegment = new Filter.NestedPathSegment("friends", FriendNode.class);
        final Filter repeated = new Filter("age", ComparisonOperator.EQUALS, 7);
        repeated.setNestedPath(friendsSegment, friendsSegment);
        friends.loadAll(FriendNode.class, repeated, 0);

        final Filter distinct = new Filter("age", ComparisonOperator.EQUALS, 7);
        distinct.setNestedPath(new Filter.NestedPathSegment("friends", FriendNode.class), new Filter.NestedPathSegment("friends", FriendNode.class));
        friends.loadAll(FriendNode.class, distinct, 0);

        assertAll(() -> assertEquals(3, loadStatements.size()),
                // the caller's path and segments are neither resolved nor replaced
                () -> assertEquals(List.of(petsSegment, vetSegment), deep.getNestedPath()), () -> assertNull(deep.getOwnerEntityType()),
                () -> assertNull(petsSegment.getRelationshipType()), () -> assertNull(vetSegment.getNestedEntityTypeLabel()),
                () -> assertNull(friendsSegment.getRelationshipType()),
                // every position gets a segment copy of its own, so Neo4j-OGM's identity-based last-segment check renders a
                // repeated instance like distinct segments (the original renders "(:`Label`)(m0)", which is not valid Cypher)
                () -> assertEquals(loadStatements.get(2), loadStatements.get(1)));
    }
}
