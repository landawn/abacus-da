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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.ArgumentCaptor;
import org.neo4j.ogm.annotation.EndNode;
import org.neo4j.ogm.annotation.GeneratedValue;
import org.neo4j.ogm.annotation.Id;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Relationship;
import org.neo4j.ogm.annotation.RelationshipEntity;
import org.neo4j.ogm.annotation.StartNode;
import org.neo4j.ogm.cypher.BooleanOperator;
import org.neo4j.ogm.cypher.ComparisonOperator;
import org.neo4j.ogm.cypher.Filter;
import org.neo4j.ogm.cypher.Filters;
import org.neo4j.ogm.driver.Driver;
import org.neo4j.ogm.request.Request;
import org.neo4j.ogm.request.RestModelRequest;
import org.neo4j.ogm.request.RowModelRequest;
import org.neo4j.ogm.session.Session;
import org.neo4j.ogm.session.SessionFactory;
import org.neo4j.ogm.transaction.AbstractTransaction;

/**
 * Pins the Neo4j-OGM behaviors that the {@link Neo4jExecutor} Javadoc documents, against a real OGM
 * {@link SessionFactory}/{@code Neo4jSession} whose {@link Driver} is mocked (no database): negative load depth for a
 * relationship entity, {@code getGraphId} for a primary-index-only entity, {@code save} of unmapped objects and
 * elements, the per-entity transactions of a collection {@code delete}, and how {@code count} gives filters passed in a
 * plain list distinct parameter names without modifying the caller's {@link Filter} objects.
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
    public void testCountPassesFiltersObjectToOgmAsItIs() {
        captureCountQueries();

        final Filter min = new Filter("age", ComparisonOperator.GREATER_THAN, 18);
        final Filter max = new Filter("age", ComparisonOperator.LESS_THAN, 65);

        countCaptured(executor, NativeIdNode.class, new Filters().add(min).and(max));

        assertAll(() -> assertEquals(Map.of("age_0", 18, "age_1", 65), countParameters.get(0)),
                // OGM resolved the caller's own filters against the counted class, as it always has
                () -> assertSame(NativeIdNode.class, min.getOwnerEntityType()), () -> assertSame(NativeIdNode.class, max.getOwnerEntityType()));
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testCountHandsSessionFiltersObjectAndUncopyableIterablesAsTheyAreAndPlainListAsCopies() {
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

        assertAll(() -> assertSame(filters, passed.get(0)), () -> assertSame(empty, passed.get(1)),
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
}
